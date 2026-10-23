//! Pairing: a member's own Claude, on their machine, works for a task's
//! session. The member runs `forge pair <n> <folder>...`, which waits on the
//! API for asks; the session runs `$FORGE_BIN local "<ask>"`, which reaches
//! Forge through the proxy with its key and waits for the answer. An ask
//! carries the session's code as a patch on a commit GitHub has, so the
//! member's machine runs what the session has now. Their files stay there:
//! only answers come back. Nothing is stored; a restart drops what waits.

use std::collections::HashMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use axum::body::Bytes;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::Engine as _;
use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::sync::oneshot;

use crate::cli::{cache_root, error_text, pick_repo, Client, ClientConfig};

/// How long a machine counts as paired after it last asked for work.
const PAIRED_FOR: TimeDelta = TimeDelta::minutes(2);
/// How long a session waits for an answer: under its Bash tool's 30 minutes.
const ANSWER_WAIT: Duration = Duration::from_secs(25 * 60);
/// How long the paired machine's request waits for an ask: under the CLI's
/// 30-second HTTP timeout.
pub const POLL_WAIT: Duration = Duration::from_secs(25);
/// The largest patch an ask carries, before base64.
pub const PATCH_LIMIT: usize = 8 << 20;
/// The longest answer sent back: the API takes 64 KB bodies.
const ANSWER_LIMIT: usize = 60_000;

/// One ask, as the paired machine gets it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Ask {
    pub id: u64,
    pub text: String,
    /// A commit GitHub has; `patch` applies on it.
    pub base: String,
    /// `git diff --binary` from `base` to the session's files, base64.
    pub patch: String,
}

/// What `forge local` sends.
#[derive(Debug, Serialize, Deserialize)]
pub struct AskBody {
    pub text: String,
    pub base: String,
    pub patch: String,
}

/// Each task's pairing.
#[derive(Clone, Default)]
pub struct Pairs(Arc<Mutex<HashMap<(String, u64), Pair>>>);

#[derive(Default)]
struct Pair {
    /// The paired member (GitHub login, name) and when their machine last asked for work.
    by: Option<(String, String, DateTime<Utc>)>,
    next_id: u64,
    /// Unanswered asks, oldest first, each with the session waiting on it.
    waiting: Vec<(Ask, oneshot::Sender<String>)>,
}

impl Pairs {
    /// The name of the member paired with the task now.
    pub fn paired(&self, repo: &str, issue: u64, now: DateTime<Utc>) -> Option<String> {
        let map = self.0.lock().unwrap();
        let (_, name, seen) = map.get(&(repo.to_string(), issue))?.by.as_ref()?;
        (now - *seen < PAIRED_FOR).then(|| name.clone())
    }

    /// Queues an ask for the paired machine: the answer's receiver and who
    /// will answer, or None when no machine is paired.
    pub fn ask(&self, repo: &str, issue: u64, text: String, base: String, patch: String, now: DateTime<Utc>) -> Option<(oneshot::Receiver<String>, String)> {
        let name = self.paired(repo, issue, now)?;
        let mut map = self.0.lock().unwrap();
        let pair = map.entry((repo.to_string(), issue)).or_default();
        pair.next_id += 1;
        let (tx, rx) = oneshot::channel();
        pair.waiting.push((Ask { id: pair.next_id, text, base, patch }, tx));
        Some((rx, name))
    }

    /// Marks `github` paired and gives the oldest ask a session still waits
    /// on, again until it's answered (a machine that restarts gets it again).
    /// Err with the other member's name when someone else is paired.
    fn take(&self, repo: &str, issue: u64, github: &str, name: &str, now: DateTime<Utc>) -> Result<Option<Ask>, String> {
        let mut map = self.0.lock().unwrap();
        let pair = map.entry((repo.to_string(), issue)).or_default();
        if let Some((other, other_name, seen)) = &pair.by {
            if !other.eq_ignore_ascii_case(github) && now - *seen < PAIRED_FOR {
                return Err(other_name.clone());
            }
        }
        pair.by = Some((github.to_string(), name.to_string(), now));
        pair.waiting.retain(|(_, tx)| !tx.is_closed());
        Ok(pair.waiting.first().map(|(ask, _)| ask.clone()))
    }

    /// Waits up to `wait` for an ask (see `take`).
    pub async fn next(&self, repo: &str, issue: u64, github: &str, name: &str, wait: Duration) -> Result<Option<Ask>, String> {
        let until = tokio::time::Instant::now() + wait;
        loop {
            if let Some(ask) = self.take(repo, issue, github, name, Utc::now())? {
                return Ok(Some(ask));
            }
            if tokio::time::Instant::now() >= until {
                return Ok(None);
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Hands the paired member's answer to the session waiting on ask `id`;
    /// false when none waits.
    pub fn answer(&self, repo: &str, issue: u64, github: &str, id: u64, text: String) -> bool {
        let mut map = self.0.lock().unwrap();
        let Some(pair) = map.get_mut(&(repo.to_string(), issue)) else { return false };
        match &mut pair.by {
            Some((by, _, seen)) if by.eq_ignore_ascii_case(github) => *seen = Utc::now(),
            _ => return false,
        }
        let Some(i) = pair.waiting.iter().position(|(ask, _)| ask.id == id) else { return false };
        pair.waiting.remove(i).1.send(text).is_ok()
    }
}

/// `POST /forge/local` on the proxy: a session's ask, from the task its key
/// (tagged "<repo>#<issue> ...") belongs to, answered by the paired machine.
pub async fn session_ask(pairs: &Pairs, tag: Option<String>, body: Bytes) -> Response {
    let reply = |code: StatusCode, v: Value| (code, Json(v)).into_response();
    let refuse = |code: StatusCode, msg: String| reply(code, json!({ "error": msg }));
    let Some((repo, issue)) = tag.as_deref().and_then(task_of) else {
        return refuse(StatusCode::UNAUTHORIZED, "a live Forge session key is needed".into());
    };
    let b: AskBody = match serde_json::from_slice(&body) {
        Ok(b) => b,
        Err(e) => return refuse(StatusCode::BAD_REQUEST, format!("bad ask: {e}")),
    };
    if b.text.trim().is_empty() {
        return refuse(StatusCode::BAD_REQUEST, "the ask is empty".into());
    }
    let Some((answer, by)) = pairs.ask(&repo, issue, b.text, b.base, b.patch, Utc::now()) else {
        return refuse(
            StatusCode::CONFLICT,
            format!(
                "Nobody has paired a machine with #{issue}. Ask for it in your forge block: someone runs \
                 `forge pair {issue} <folder>` on their machine."
            ),
        );
    };
    match tokio::time::timeout(ANSWER_WAIT, answer).await {
        Ok(Ok(text)) => reply(StatusCode::OK, json!({ "answer": text, "by": by })),
        Ok(Err(_)) => refuse(StatusCode::SERVICE_UNAVAILABLE, "the ask was dropped; send it again".into()),
        Err(_) => refuse(StatusCode::GATEWAY_TIMEOUT, format!("{by}'s machine didn't answer within 25 minutes")),
    }
}

/// The task a proxy key's tag names: "<owner/repo>#<issue>", then anything.
fn task_of(tag: &str) -> Option<(String, u64)> {
    let (repo, issue) = tag.split(' ').next()?.rsplit_once('#')?;
    Some((repo.to_string(), issue.parse().ok()?))
}

/// `forge local "<ask>"` in a session: sends the ask with the worktree's code
/// to the paired machine and prints its answer. "-" reads the ask from stdin.
pub async fn cmd_local(text: String) -> Result<()> {
    let env = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
    let (Some(proxy), Some(key)) = (env("ANTHROPIC_BASE_URL"), env("ANTHROPIC_AUTH_TOKEN")) else {
        bail!("`forge local` runs in a Forge session");
    };
    let text = if text == "-" { std::io::read_to_string(std::io::stdin())? } else { text };
    if text.trim().is_empty() {
        bail!("say what to ask");
    }
    let (base, patch) = snapshot(&std::env::current_dir()?)?;
    if patch.len() > PATCH_LIMIT {
        bail!("your changes come to {} MB as a patch; at most 8 MB can be sent", patch.len() >> 20);
    }
    let patch = base64::engine::general_purpose::STANDARD.encode(patch);
    let http = reqwest::Client::builder().timeout(ANSWER_WAIT + Duration::from_secs(60)).build()?;
    let resp = http
        .post(format!("{}/forge/local", proxy.trim_end_matches('/')))
        .bearer_auth(key)
        .json(&AskBody { text, base, patch })
        .send()
        .await
        .context("can't reach Forge")?;
    let status = resp.status();
    let body = resp.text().await.context("reading Forge's answer")?;
    if !status.is_success() {
        bail!("{}", error_text(status, &body));
    }
    let v: Value = serde_json::from_str(&body).context("unexpected answer from Forge")?;
    println!("{}'s machine answers:\n\n{}", v["by"].as_str().unwrap_or("The paired"), v["answer"].as_str().unwrap_or(""));
    Ok(())
}

/// The worktree's files as a patch on the newest commit of HEAD that GitHub
/// has (one on an `origin/*` ref): commits not pushed, staged, changed and
/// new files. Ignored files aren't sent.
fn snapshot(dir: &Path) -> Result<(String, Vec<u8>)> {
    let local = String::from_utf8(git(dir, &["rev-list", "--first-parent", "HEAD", "--not", "--remotes=origin"], None)?)?;
    let base = match local.lines().last() {
        Some(oldest) => git(dir, &["rev-parse", &format!("{oldest}^")], None)?,
        None => git(dir, &["rev-parse", "HEAD"], None)?,
    };
    let base = String::from_utf8(base)?.trim().to_string();
    // New files go in through a copy of the index: the session's own is untouched.
    let index = String::from_utf8(git(dir, &["rev-parse", "--path-format=absolute", "--git-path", "index"], None)?)?;
    let copy = std::env::temp_dir().join(format!("forge-local-{}.index", std::process::id()));
    std::fs::copy(index.trim(), &copy).context("copying the git index")?;
    let patch = git(dir, &["add", "-A"], Some(&copy)).and_then(|_| git(dir, &["diff", "--cached", "--binary", &base], Some(&copy)));
    let _ = std::fs::remove_file(&copy);
    Ok((base, patch?))
}

/// Runs git in `dir`, with `index` as its index file if given; its stdout.
fn git(dir: &Path, args: &[&str], index: Option<&Path>) -> Result<Vec<u8>> {
    let mut cmd = std::process::Command::new("git");
    cmd.arg("-C").arg(dir).args(args).stdin(Stdio::null());
    if let Some(index) = index {
        cmd.env("GIT_INDEX_FILE", index);
    }
    let out = cmd.output().context("running git")?;
    if !out.status.success() {
        bail!("`git {}` failed: {}", args.join(" "), String::from_utf8_lossy(&out.stderr).trim());
    }
    Ok(out.stdout)
}

/// `forge pair <n> <folder>...`: until Ctrl-C, the task's session can ask a
/// Claude on this machine to work with the files in `folders`.
pub async fn cmd_pair(repo: Option<String>, issue: u64, folders: Vec<PathBuf>) -> Result<()> {
    let cfg = ClientConfig::load()?;
    let repo = pick_repo(repo, &cfg)?;
    let client = Client::from_config(&cfg)?;
    let view = client.task(&repo, issue).await?;
    let folders = folders
        .iter()
        .map(|f| f.canonicalize().with_context(|| format!("no folder {}", f.display())))
        .collect::<Result<Vec<_>>>()?;
    if let Some(f) = folders.iter().find(|f| !f.is_dir()) {
        bail!("{} isn't a folder", f.display());
    }
    let cache = cache_root(std::env::var_os("XDG_CACHE_HOME"), std::env::var_os("HOME"))?;
    let worktree = pair_worktree(&cache, &repo, issue)?;
    let settings = worktree.with_extension("settings.json");
    let login = ClientConfig::path()?.parent().context("no config folder")?.to_path_buf();
    std::fs::write(&settings, serde_json::to_vec_pretty(&sandbox_settings(&folders, &login))?)?;
    let shown: Vec<String> = folders.iter().map(|f| f.display().to_string()).collect();
    println!(
        "Paired with #{issue} {}.\nIts session can ask a Claude here to work on {}.\nThat Claude runs sandboxed: only \
         those folders and the task's code, no network beyond package registries. Ctrl-C ends the pairing.",
        view.title,
        shown.join(", ")
    );
    let mut session: Option<String> = None;
    loop {
        let ask = match client.get::<Value>(&format!("/api/tasks/{repo}/{issue}/pair")).await {
            Ok(v) => serde_json::from_value::<Option<Ask>>(v["ask"].clone())?,
            Err(e) => {
                eprintln!("· {e:#}; trying again in 10 s");
                tokio::time::sleep(Duration::from_secs(10)).await;
                continue;
            }
        };
        let Some(ask) = ask else { continue };
        println!("\n▶ Forge's Claude asks:\n{}\n", indent(&ask.text));
        let answer = match put_code(&worktree, &ask) {
            Err(e) => format!("Couldn't put the task's code on this machine: {e:#}"),
            Ok(()) => match run_claude(&worktree, &settings, &folders, &prompt(&repo, &view.title, &shown, &ask.text), session.as_deref()).await {
                Ok((text, id)) => {
                    session = id.or(session);
                    text
                }
                Err(e) => format!("The Claude on this machine failed: {e:#}"),
            },
        };
        let answer = cap(&answer, ANSWER_LIMIT);
        println!("\n◀ Answer:\n{}\n", indent(&answer));
        if let Err(e) = client.post::<Value>(&format!("/api/tasks/{repo}/{issue}/pair/{}", ask.id), json!({ "answer": answer })).await {
            eprintln!("· {e:#}");
        }
    }
}

/// `<cache>/pair/<repo name>-<issue>`, a worktree of Forge's clone of the
/// repo in the cache (the one `forge try` uses), made on first use.
fn pair_worktree(cache: &Path, repo: &str, issue: u64) -> Result<PathBuf> {
    let mirror = cache.join("repos").join(format!("{repo}.git"));
    if !mirror.exists() {
        std::fs::create_dir_all(mirror.parent().context("no cache folder")?)?;
        eprintln!("$ git clone --bare git@github.com:{repo}.git {}", mirror.display());
        git(cache, &["clone", "--bare", "--quiet", &format!("git@github.com:{repo}.git"), &mirror.to_string_lossy()], None)?;
    }
    let name = repo.rsplit('/').next().unwrap_or(repo);
    let worktree = cache.join("pair").join(format!("{name}-{issue}"));
    if !worktree.join(".git").exists() {
        std::fs::create_dir_all(cache.join("pair"))?;
        git(&mirror, &["worktree", "add", "--detach", "--force", &worktree.to_string_lossy()], None)?;
    }
    Ok(worktree)
}

/// Puts the ask's code in the worktree: its base, fetched if missing, then its patch.
fn put_code(worktree: &Path, ask: &Ask) -> Result<()> {
    let have = git(worktree, &["cat-file", "-e", &format!("{}^{{commit}}", ask.base)], None).is_ok();
    if !have {
        git(worktree, &["fetch", "--quiet", "origin", &ask.base], None)?;
    }
    git(worktree, &["checkout", "--quiet", "--detach", "--force", &ask.base], None)?;
    git(worktree, &["clean", "-fdq"], None)?;
    let patch = base64::engine::general_purpose::STANDARD.decode(&ask.patch).context("the patch isn't base64")?;
    if patch.is_empty() {
        return Ok(());
    }
    let mut apply = std::process::Command::new("git")
        .arg("-C")
        .arg(worktree)
        .args(["apply", "--binary", "--whitespace=nowarn", "-"])
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("running git apply")?;
    apply.stdin.take().context("git apply's stdin")?.write_all(&patch)?;
    let out = apply.wait_with_output()?;
    if !out.status.success() {
        bail!("`git apply` failed: {}", String::from_utf8_lossy(&out.stderr).trim());
    }
    Ok(())
}

/// Claude Code's settings for the paired Claude: Bash in the sandbox and
/// never out of it, writes in the folders, the code and package caches only,
/// no network but package registries and local ports, no secrets read, and
/// no web tools (anything fetched could carry the user's files out).
/// `login` is the folder that holds this CLI's token, wherever it is.
fn sandbox_settings(folders: &[PathBuf], login: &Path) -> Value {
    let mut writable: Vec<String> = folders.iter().map(|f| format!("/{}", f.display())).collect();
    writable.extend(["~/.cargo", "~/.npm", "~/.cache"].map(String::from));
    let secrets = ["~/.ssh", "~/.aws", "~/.gnupg", "~/.kube", "~/.docker", "~/.config/gh", "~/.config/forge", "~/.config/gcloud", "~/Library/Keychains"];
    let mut deny: Vec<String> = secrets.iter().map(|s| format!("Read({s}/**)")).collect();
    deny.push(format!("Read(/{}/**)", login.display()));
    deny.extend(["Read(~/.netrc)", "WebFetch", "WebSearch"].map(String::from));
    json!({
        "sandbox": {
            "enabled": true,
            "autoAllowBashIfSandboxed": true,
            "allowUnsandboxedCommands": false,
            "filesystem": { "allowWrite": writable },
            "network": {
                "allowLocalBinding": true,
                "allowedDomains": ["crates.io", "*.crates.io", "registry.npmjs.org", "pypi.org", "files.pythonhosted.org"]
            }
        },
        "permissions": { "deny": deny }
    })
}

/// What the paired Claude is told with each ask.
fn prompt(repo: &str, title: &str, folders: &[String], ask: &str) -> String {
    format!(
        "Forge's Claude is working on {repo} (\"{title}\") on Forge's server, and asks you, the Claude on this \
         machine, for help. Your final message goes back to it as the answer.\n\n\
         This folder holds the task's code as Forge's Claude has it now. The user's files are in {}. Work with \
         them as asked: run the code on them, check what comes out, sort or change them if asked.\n\n\
         Rules:\n\
         - The user's files never leave this machine; your answer does. Give results: what you ran, what passed or \
         failed, counts. Describe problems in your own words or with made-up examples. Never copy text, names or \
         numbers out of the user's files into your answer.\n\
         - Don't fix the code here: it's replaced before each ask. Say what's wrong and where; Forge's Claude fixes it.\n\
         - Keep the answer short.\n\n\
         The ask:\n\n{ask}\n",
        folders.join(", ")
    )
}

/// Runs the paired Claude on `prompt` in `dir`, resuming `resume`, printing a
/// line per tool it uses; its final message and session id.
async fn run_claude(dir: &Path, settings: &Path, folders: &[PathBuf], prompt: &str, resume: Option<&str>) -> Result<(String, Option<String>)> {
    let mut cmd = tokio::process::Command::new("claude");
    cmd.args(["-p", "--output-format", "stream-json", "--verbose", "--permission-mode", "acceptEdits", "--strict-mcp-config", "--settings"])
        .arg(settings);
    for f in folders {
        cmd.arg("--add-dir").arg(f);
    }
    if let Some(id) = resume {
        cmd.args(["--resume", id]);
    }
    let mut child = cmd
        .current_dir(dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .context("running claude: is Claude Code installed?")?;
    let mut stdin = child.stdin.take().context("claude's stdin")?;
    stdin.write_all(prompt.as_bytes()).await?;
    drop(stdin);
    let mut lines = BufReader::new(child.stdout.take().context("claude's stdout")?).lines();
    let (mut result, mut session) = (None, None);
    while let Some(line) = lines.next_line().await? {
        let Ok(event) = serde_json::from_str::<Value>(&line) else { continue };
        if let Some(id) = event["session_id"].as_str() {
            session = Some(id.to_string());
        }
        match event["type"].as_str() {
            Some("assistant") => {
                for item in event["message"]["content"].as_array().into_iter().flatten().filter(|i| i["type"] == "tool_use") {
                    println!("  · {}", tool_line(item["name"].as_str().unwrap_or("?"), &item["input"]));
                }
            }
            Some("result") => result = event["result"].as_str().map(String::from),
            _ => {}
        }
    }
    let status = child.wait().await?;
    match result {
        Some(text) => Ok((text, session)),
        None => bail!("claude ended ({status}) without an answer"),
    }
}

/// One line for a tool the paired Claude used: its name and what it's on.
fn tool_line(name: &str, input: &Value) -> String {
    let on = ["command", "file_path", "pattern", "path"].iter().find_map(|k| input[*k].as_str()).unwrap_or("");
    let on = on.lines().next().unwrap_or("");
    let on = if on.chars().count() > 100 { format!("{}…", on.chars().take(100).collect::<String>()) } else { on.to_string() };
    format!("{name} {on}").trim_end().to_string()
}

fn indent(text: &str) -> String {
    text.lines().map(|l| format!("  {l}")).collect::<Vec<_>>().join("\n")
}

/// `text` cut to at most `limit` bytes, on a character boundary, saying so.
fn cap(text: &str, limit: usize) -> String {
    if text.len() <= limit {
        return text.to_string();
    }
    let mut end = limit - 20;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}\n[answer cut short]", &text[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    const REPO: &str = "example-team/shop";

    fn t0() -> DateTime<Utc> {
        Utc::now()
    }

    #[test]
    fn a_key_tag_names_its_task() {
        assert_eq!(task_of("example-team/shop#412 tmux"), Some((REPO.into(), 412)));
        assert_eq!(task_of("example-team/shop#412"), Some((REPO.into(), 412)));
        assert_eq!(task_of("example-team/shop#412 attach ann-gh"), Some((REPO.into(), 412)));
        assert_eq!(task_of("nothing"), None);
    }

    #[tokio::test]
    async fn an_ask_waits_for_a_paired_machine_and_gets_its_answer() {
        let pairs = Pairs::default();
        assert!(pairs.ask(REPO, 1, "run it".into(), "abc".into(), String::new(), t0()).is_none(), "nobody is paired");
        assert_eq!(pairs.next(REPO, 1, "ann-gh", "Ann", Duration::ZERO).await, Ok(None));
        assert_eq!(pairs.paired(REPO, 1, t0()).as_deref(), Some("Ann"));
        assert_eq!(pairs.paired(REPO, 2, t0()), None);

        let (rx, by) = pairs.ask(REPO, 1, "run it".into(), "abc".into(), "cGF0Y2g=".into(), t0()).unwrap();
        assert_eq!(by, "Ann");
        let ask = pairs.next(REPO, 1, "ann-gh", "Ann", Duration::ZERO).await.unwrap().unwrap();
        assert_eq!((ask.text.as_str(), ask.base.as_str(), ask.patch.as_str()), ("run it", "abc", "cGF0Y2g="));
        // Until it's answered, the same ask comes again (a restarted machine).
        assert_eq!(pairs.next(REPO, 1, "ann-gh", "Ann", Duration::ZERO).await.unwrap(), Some(ask.clone()));
        assert!(!pairs.answer(REPO, 1, "bob-gh", ask.id, "no".into()), "only the paired member answers");
        assert!(pairs.answer(REPO, 1, "ann-gh", ask.id, "2 of 3 pass".into()));
        assert_eq!(rx.await.unwrap(), "2 of 3 pass");
        assert!(!pairs.answer(REPO, 1, "ann-gh", ask.id, "again".into()), "answered once");
        assert_eq!(pairs.next(REPO, 1, "ann-gh", "Ann", Duration::ZERO).await, Ok(None));
    }

    #[tokio::test]
    async fn one_member_pairs_at_a_time_until_theirs_goes_quiet() {
        let pairs = Pairs::default();
        pairs.take(REPO, 1, "ann-gh", "Ann", t0()).unwrap();
        assert_eq!(pairs.take(REPO, 1, "bob-gh", "Bob", t0()), Err("Ann".into()));
        assert_eq!(pairs.paired(REPO, 1, t0() + TimeDelta::minutes(3)), None);
        assert_eq!(pairs.take(REPO, 1, "bob-gh", "Bob", t0() + TimeDelta::minutes(3)), Ok(None));
        assert_eq!(pairs.paired(REPO, 1, t0() + TimeDelta::minutes(3)).as_deref(), Some("Bob"));
    }

    #[tokio::test]
    async fn an_ask_the_session_gave_up_on_is_dropped() {
        let pairs = Pairs::default();
        pairs.take(REPO, 1, "ann-gh", "Ann", t0()).unwrap();
        let (rx, _) = pairs.ask(REPO, 1, "first".into(), "abc".into(), String::new(), t0()).unwrap();
        let (_rx2, _) = pairs.ask(REPO, 1, "second".into(), "abc".into(), String::new(), t0()).unwrap();
        drop(rx);
        let ask = pairs.next(REPO, 1, "ann-gh", "Ann", Duration::ZERO).await.unwrap().unwrap();
        assert_eq!(ask.text, "second");
    }

    #[tokio::test]
    async fn a_session_ask_is_answered_or_refused() {
        let pairs = Pairs::default();
        let body = |text: &str| Bytes::from(json!({ "text": text, "base": "abc", "patch": "" }).to_string());
        let status = |r: Response| r.status();
        assert_eq!(status(session_ask(&pairs, None, body("x")).await), StatusCode::UNAUTHORIZED);
        let tag = || Some(format!("{REPO}#7 tmux"));
        assert_eq!(status(session_ask(&pairs, tag(), body("x")).await), StatusCode::CONFLICT);
        pairs.take(REPO, 7, "ann-gh", "Ann", t0()).unwrap();
        assert_eq!(status(session_ask(&pairs, tag(), body("  ")).await), StatusCode::BAD_REQUEST);
        let machine = pairs.clone();
        tokio::spawn(async move {
            let ask = machine.next(REPO, 7, "ann-gh", "Ann", Duration::from_secs(5)).await.unwrap().unwrap();
            machine.answer(REPO, 7, "ann-gh", ask.id, format!("did: {}", ask.text));
        });
        let r = session_ask(&pairs, tag(), body("sort them")).await;
        assert_eq!(r.status(), StatusCode::OK);
        let v: Value = serde_json::from_slice(&axum::body::to_bytes(r.into_body(), usize::MAX).await.unwrap()).unwrap();
        assert_eq!(v, json!({ "answer": "did: sort them", "by": "Ann" }));
    }

    #[test]
    fn a_snapshot_is_the_unpushed_work_on_a_pushed_commit() {
        let tmp = tempfile::tempdir().unwrap();
        let (origin, dir) = (tmp.path().join("origin"), tmp.path().join("work"));
        let run = |dir: &Path, args: &[&str]| String::from_utf8(git(dir, args, None).unwrap()).unwrap().trim().to_string();
        std::fs::create_dir_all(&origin).unwrap();
        run(&origin, &["init", "-q", "-b", "main"]);
        run(&origin, &["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "root"]);
        run(tmp.path(), &["clone", "-q", &origin.to_string_lossy(), &dir.to_string_lossy()]);
        let pushed = run(&dir, &["rev-parse", "HEAD"]);
        std::fs::write(dir.join("a.txt"), "committed\n").unwrap();
        run(&dir, &["add", "a.txt"]);
        run(&dir, &["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "a"]);
        std::fs::write(dir.join("b.txt"), "new, never added\n").unwrap();
        std::fs::write(dir.join(".gitignore"), "target/\n").unwrap();
        std::fs::create_dir_all(dir.join("target")).unwrap();
        std::fs::write(dir.join("target/big"), "ignored\n").unwrap();

        let (base, patch) = snapshot(&dir).unwrap();
        assert_eq!(base, pushed);
        let patch = String::from_utf8(patch).unwrap();
        for part in ["+committed", "+new, never added", "+target/"] {
            assert!(patch.contains(part), "missing {part:?} in:\n{patch}");
        }
        assert!(!patch.contains("ignored"), "{patch}");
        assert_eq!(run(&dir, &["status", "--porcelain"]), "?? .gitignore\n?? b.txt", "the session's index is untouched");

        // And it applies on the base in another worktree.
        let ask = Ask { id: 1, text: String::new(), base, patch: base64::engine::general_purpose::STANDARD.encode(patch) };
        let other = tmp.path().join("other");
        run(&dir, &["worktree", "add", "-q", "--detach", &other.to_string_lossy()]);
        put_code(&other, &ask).unwrap();
        assert_eq!(std::fs::read_to_string(other.join("b.txt")).unwrap(), "new, never added\n");
        assert_eq!(std::fs::read_to_string(other.join("a.txt")).unwrap(), "committed\n");
    }

    #[test]
    fn the_paired_claude_is_sandboxed_to_its_folders() {
        let s = sandbox_settings(&[PathBuf::from("/Users/ann/reports")], Path::new("/Users/ann/conf/forge"));
        assert_eq!(s["sandbox"]["allowUnsandboxedCommands"], json!(false));
        assert_eq!(s["sandbox"]["filesystem"]["allowWrite"][0], json!("//Users/ann/reports"));
        let deny = s["permissions"]["deny"].to_string();
        for part in ["Read(~/.ssh/**)", "Read(~/.config/forge/**)", "Read(//Users/ann/conf/forge/**)", "WebFetch", "WebSearch"] {
            assert!(deny.contains(part), "missing {part} in {deny}");
        }
    }

    #[test]
    fn answers_are_capped_on_a_character_boundary() {
        assert_eq!(cap("short", 100), "short");
        let long = "é".repeat(100);
        let c = cap(&long, 51);
        assert!(c.ends_with("\n[answer cut short]") && c.len() <= 51, "{c}");
        assert_eq!(tool_line("Bash", &json!({ "command": "npm test\nmore" })), "Bash npm test");
        assert_eq!(tool_line("TodoWrite", &json!({})), "TodoWrite");
    }
}
