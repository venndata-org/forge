//! Claude sessions: one per task, run headless in bursts with `claude -p`,
//! resumed with `--resume`. A session ends each burst with a ```forge block
//! saying what happened; Forge does every GitHub action itself.

use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use tokio::io::AsyncWriteExt;

use crate::config::{Config, RepoConfig};
use crate::gitops::{read_tail, sudo_args, supervise, unless_timed_out, BACKSTOP};
use crate::types::{branch_for, BurstReason, BurstRequest, BurstResult, IssueThread, Outcome, Task};

#[async_trait]
pub trait Runner: Send + Sync {
    async fn burst(&self, req: BurstRequest) -> Result<BurstResult>;
    /// The task is done with its session (it landed): end whatever keeps it.
    async fn finish(&self, _repo: &str, _issue: u64) {}
}

/// Claude Code settings for Forge's sessions. No background commands: a turn
/// that ends while one runs reads as finished, so tests run in the foreground,
/// with time for a full suite that waits its turn on the build slot. No
/// self-updates: root keeps the binary current (forge-claude-update).
pub const CLAUDE_ENV: [(&str, &str); 4] = [
    ("DISABLE_AUTOUPDATER", "1"),
    ("CLAUDE_CODE_DISABLE_BACKGROUND_TASKS", "1"),
    ("BASH_DEFAULT_TIMEOUT_MS", "1800000"),
    ("BASH_MAX_TIMEOUT_MS", "7200000"),
];

/// Runs `claude -p` as the session user, pointed at Forge's local proxy.
pub struct ClaudeRunner {
    pub bin: PathBuf,
    pub session_user: Option<String>,
    /// e.g. "http://127.0.0.1:8471"
    pub proxy_url: String,
    /// How long one burst may run.
    pub timeout: Duration,
    /// The build-slot lock `forge slot` takes.
    pub slot_path: PathBuf,
    pub forge_bin: PathBuf,
}

/// Forge's own variables a session also gets, when set.
const PASSED_ENV: [&str; 3] = ["HOME", "PATH", "LANG"];
/// How much of a failed check's output, or of review notes, a prompt carries.
const REASON_TAIL: usize = 8 * 1024;
const STDOUT_MAX: usize = 16 << 20;
const STDERR_MAX: usize = 64 << 10;
/// How many lines of stderr a failed burst's reason carries.
const ERROR_LINES: usize = 20;
/// Seconds `timeout` gives claude after SIGTERM before SIGKILL.
const KILL_AFTER: u64 = 60;

impl ClaudeRunner {
    pub fn new(cfg: &Config) -> ClaudeRunner {
        ClaudeRunner {
            bin: cfg.claude.bin.clone(),
            session_user: cfg.claude.session_user.clone(),
            proxy_url: format!("http://{}", cfg.claude.proxy_listen),
            timeout: Duration::from_secs(cfg.claude.burst_timeout_minutes * 60),
            slot_path: cfg.slot_path(),
            forge_bin: cfg.claude.forge_bin.clone(),
        }
    }

    /// The command a burst runs: program, arguments and environment, without
    /// running it. The prompt goes on stdin: one argument can't hold a long one.
    /// With a session user, it is wrapped in `sudo -n -H -u <user>` and
    /// `timeout`, which ends the session at its limit since Forge can't signal
    /// another user's processes.
    /// Environment: ANTHROPIC_BASE_URL, ANTHROPIC_AUTH_TOKEN (the session
    /// key), FORGE_SLOT, FORGE_BIN, FORGE_ISSUE and [`CLAUDE_ENV`]; nothing
    /// else of Forge's.
    /// HOME is Forge's own only when there is no session user; a session user
    /// keeps its own home, where its Claude sessions and git config live.
    pub fn command_line(&self, req: &BurstRequest) -> (String, Vec<String>, Vec<(String, String)>) {
        let mut env: Vec<(String, String)> = vec![
            ("ANTHROPIC_BASE_URL".into(), self.proxy_url.clone()),
            ("ANTHROPIC_AUTH_TOKEN".into(), req.session_key.clone()),
            ("FORGE_SLOT".into(), self.slot_path.display().to_string()),
            ("FORGE_BIN".into(), self.forge_bin.display().to_string()),
            ("FORGE_ISSUE".into(), req.issue.to_string()),
        ];
        env.extend(CLAUDE_ENV.iter().map(|(k, v)| (k.to_string(), v.to_string())));
        for name in PASSED_ENV {
            if name == "HOME" && self.session_user.is_some() {
                continue;
            }
            if let Ok(value) = std::env::var(name) {
                env.push((name.into(), value));
            }
        }
        let mut args: Vec<String> = vec![
            "-p".into(),
            "--output-format".into(),
            "json".into(),
            "--model".into(),
            req.model.clone(),
            "--effort".into(),
            req.effort.clone(),
            "--dangerously-skip-permissions".into(),
        ];
        if let Some(id) = &req.resume {
            args.extend(["--resume".into(), id.clone()]);
        }
        let bin = self.bin.display().to_string();
        let Some(user) = &self.session_user else {
            return (bin, args, env);
        };
        let names: Vec<&str> = env.iter().map(|(k, _)| k.as_str()).collect();
        let mut sudo = sudo_args(user, &names, KILL_AFTER, self.timeout);
        sudo.push(bin);
        sudo.extend(args);
        ("sudo".into(), sudo, env)
    }
}

#[async_trait]
impl Runner for ClaudeRunner {
    async fn burst(&self, req: BurstRequest) -> Result<BurstResult> {
        let (program, args, env) = self.command_line(&req);
        let mut child = tokio::process::Command::new(&program)
            .args(&args)
            .env_clear()
            .envs(env)
            .current_dir(&req.worktree)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0)
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("starting {program} in {}", req.worktree.display()))?;
        let mut stdin = child.stdin.take().context("no stdin")?;
        let mut stdout = child.stdout.take().context("no stdout")?;
        let mut stderr = child.stderr.take().context("no stderr")?;
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let prompt = req.prompt.as_bytes();
        let feeding = async move {
            let _ = stdin.write_all(prompt).await;
        };
        let reading = async {
            tokio::join!(
                feeding,
                read_tail(&mut stdout, &mut out, STDOUT_MAX),
                read_tail(&mut stderr, &mut err, STDERR_MAX)
            );
        };
        let wrapped = self.session_user.is_some();
        let limit = if wrapped { self.timeout + BACKSTOP } else { self.timeout };
        let status = unless_timed_out(supervise(&mut child, reading, limit).await?, wrapped);
        let (out, err) = (String::from_utf8_lossy(&out), String::from_utf8_lossy(&err));
        Ok(burst_result(status, &out, &err, req.resume))
    }
}

/// What a finished (or, with no status, timed-out) `claude -p` run comes to.
/// A run that tells no session id keeps the one it resumed.
fn burst_result(status: Option<ExitStatus>, stdout: &str, stderr: &str, resume: Option<String>) -> BurstResult {
    let failed = |session_id, why: String, detail: &str, cost_usd| {
        let detail = last_lines(detail, ERROR_LINES);
        let reason = if detail.is_empty() { why } else { format!("{why}:\n{detail}") };
        BurstResult { session_id, outcome: Outcome::Blocked { reason }, cost_usd, is_error: true }
    };
    let Some(status) = status else {
        return failed(resume, "the session ran past its time limit".into(), "", None);
    };
    match parse_claude_output(stdout) {
        Ok((id, text, false, cost)) if status.success() => {
            BurstResult { session_id: id.or(resume), outcome: parse_outcome(&text), cost_usd: cost, is_error: false }
        }
        Ok((id, text, _, cost)) => {
            let detail = if stderr.trim().is_empty() { text.as_str() } else { stderr };
            failed(id.or(resume), format!("claude failed ({status})"), detail, cost)
        }
        Err(e) => failed(resume, format!("claude's output couldn't be read ({status}, {e})"), stderr, None),
    }
}

/// The prompt for a build burst: the role's prompt, the repo's notes, the
/// issue thread (comments newer than `task.last_seen_comment` marked new),
/// why this burst runs, and the rules every session follows (see BUILD.md).
pub fn build_prompt(repo: &RepoConfig, role_prompt: &str, thread: &IssueThread, reason: &BurstReason, task: &Task) -> String {
    let n = thread.number;
    let task_text = format!(
        "You are working on issue #{n} of {} in this git worktree, on branch `{}`. Make the change the issue \
         asks for, with as little code as does the job well. Lines added are a cost.",
        repo.name, task.branch
    );
    let rules = format!("- Commit your work on this branch with clear messages.\n{RULES}");
    let mut p = head(role_prompt, &task_text, &rules, repo);
    p.push_str(&issue(thread, Some(task.last_seen_comment)));
    p.push_str(&format!("## Why this burst runs\n\n{}\n\n", why(repo, reason)));
    p.push_str(BUILD_ENDING);
    p
}

/// The prompt for a review burst: review `git diff origin/<base>...HEAD`
/// against the issue, and end with a review ```forge block.
pub fn review_prompt(repo: &RepoConfig, role_prompt: &str, thread: &IssueThread, sha: &str) -> String {
    let n = thread.number;
    let task_text = format!(
        "You are reviewing the change for issue #{n} of {} in this git worktree, on branch `{}`. The change is \
         commit {sha}, which Forge pushed: read it with `git diff origin/{}...{sha}`, not HEAD. Check that it \
         does what the issue asks, with no needless code.",
        repo.name,
        branch_for(n),
        repo.default_branch
    );
    let rules = format!("- Change nothing: no edits, no commits.\n{RULES}");
    let mut p = head(role_prompt, &task_text, &rules, repo);
    p.push_str(&issue(thread, None));
    p.push_str(REVIEW_ENDING);
    p
}

const RULES: &str = "\
- Never push, never open PRs, never comment on GitHub: Forge does that.
- Run builds through the build slot: `$FORGE_BIN slot -- <build command>`. Only one build fits on the box at a time.
- Never put real customer data anywhere; use synthetic data only.
";

const BUILD_ENDING: &str = r#"## How to end

End your final message with exactly one block:

```forge
{"outcome": "pr", "title": "...", "body": "..."}
```

or `{"outcome": "question", "text": "..."}` when you need a person, or `{"outcome": "blocked", "reason": "..."}` when you can't go on.
"#;

const REVIEW_ENDING: &str = r#"## How to end

End your final message with exactly one block:

```forge
{"outcome": "review", "pass": true, "notes": "..."}
```

Use `"pass": false` when the change needs work, and say what in the notes.
"#;

/// The role's prompt, the task, the rules and the repo's notes.
fn head(role_prompt: &str, task_text: &str, rules: &str, repo: &RepoConfig) -> String {
    let mut p = String::new();
    if !role_prompt.trim().is_empty() {
        p.push_str(&format!("{}\n\n", role_prompt.trim()));
    }
    p.push_str(&format!("## Your task\n\n{task_text}\n\n## Rules\n\n{rules}\n"));
    if let Some(notes) = repo.notes.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        p.push_str(&format!("## Notes for this repo\n\n{notes}\n\n"));
    }
    p
}

/// The issue and its comments, oldest first; with `seen`, comments newer
/// than it are marked new.
fn issue(thread: &IssueThread, seen: Option<u64>) -> String {
    let body = if thread.body.trim().is_empty() { "(no description)" } else { thread.body.trim() };
    let mut p = format!("## Issue #{}: {}\n\nOpened by @{}.\n\n{body}\n\n", thread.number, thread.title, thread.author);
    if !thread.comments.is_empty() {
        p.push_str("## Comments\n\n");
    }
    for c in &thread.comments {
        let new = if seen.is_some_and(|s| c.id > s) { " (new)" } else { "" };
        let at = c.created_at.format("%Y-%m-%d %H:%M UTC");
        p.push_str(&format!("### @{}, {at}{new}\n\n{}\n\n", c.author, c.body.trim()));
    }
    p
}

fn why(repo: &RepoConfig, reason: &BurstReason) -> String {
    let main = &repo.default_branch;
    match reason {
        BurstReason::Start => "The task just started. Read the issue and make the change.".into(),
        BurstReason::Answer => "Someone answered on the issue: see the comments marked new. Carry on.".into(),
        BurstReason::ReviewNotes { notes } => format!(
            "A review asked for changes. Make them and commit, then end with a pr block again. The review's notes:\n\n{}",
            block(notes)
        ),
        BurstReason::MergeCheckFailed { output } => format!(
            "Forge rebased this branch on current {main} and the merge check `{}` failed. `git fetch origin` brings \
             current {main}; fix it and commit. Its output:\n\n{}",
            repo.merge_check.as_deref().unwrap_or("(none set)"),
            block(output)
        ),
        BurstReason::RebaseConflict { output } => format!(
            "This branch no longer rebases cleanly on current {main}. Run `git fetch origin`, rebase it on \
             `origin/{main}` yourself, resolve the conflicts, and commit. Git said:\n\n{}",
            block(output)
        ),
        BurstReason::AfterAttach => {
            "A person stepped into this session with `forge attach` and has left. Carry on from where things stand.".into()
        }
        BurstReason::Review => "Review the change on this branch.".into(),
    }
}

/// The last 8 KB of `text` in a fenced block whose fence no line inside can close.
fn block(text: &str) -> String {
    let text = text.trim_end();
    let kept = tail(text, REASON_TAIL);
    let kept = if kept.len() < text.len() { format!("[...]\n{kept}") } else { kept.to_string() };
    let longest = kept.split(|c| c != '`').map(str::len).max().unwrap_or(0);
    let fence = "`".repeat(longest.max(2) + 1);
    format!("{fence}\n{kept}\n{fence}")
}

/// The last `max` bytes of `text`, cut at a character boundary.
fn tail(text: &str, max: usize) -> &str {
    let mut start = text.len().saturating_sub(max);
    while !text.is_char_boundary(start) {
        start += 1;
    }
    &text[start..]
}

fn last_lines(text: &str, n: usize) -> String {
    let lines: Vec<&str> = text.trim_end().lines().collect();
    tail(&lines[lines.len().saturating_sub(n)..].join("\n"), REASON_TAIL).to_string()
}

/// Reads the last ```forge block in a session's final message. A missing or
/// unreadable block is `Outcome::Blocked` saying so.
pub fn parse_outcome(text: &str) -> Outcome {
    let Some(body) = last_forge_block(text) else {
        return Outcome::Blocked { reason: "no forge block in the session's answer".into() };
    };
    serde_json::from_str(body.trim()).unwrap_or_else(|e| Outcome::Blocked {
        reason: format!("the session's forge block isn't a valid outcome ({e})"),
    })
}

/// Whether the text has a ```forge block.
pub fn has_forge_block(text: &str) -> bool {
    last_forge_block(text).is_some()
}

/// The body of the last ```forge block; an unclosed last block runs to the end.
fn last_forge_block(text: &str) -> Option<String> {
    let mut last = None;
    let mut open: Option<Vec<&str>> = None;
    for line in text.lines() {
        let t = line.trim();
        let fence = t.starts_with("```");
        match open.as_mut() {
            Some(_) if fence && t.trim_start_matches('`').is_empty() => last = open.take().map(|b| b.join("\n")),
            Some(body) => body.push(line),
            None if fence && t.trim_start_matches('`').trim() == "forge" => open = Some(Vec::new()),
            None => {}
        }
    }
    open.map(|b| b.join("\n")).or(last)
}

/// The fields Forge reads from `claude -p --output-format json`.
#[derive(Deserialize)]
struct ClaudeOutput {
    session_id: Option<String>,
    result: Option<String>,
    #[serde(default)]
    is_error: bool,
    subtype: Option<String>,
    total_cost_usd: Option<f64>,
}

/// Reads `claude -p --output-format json` output: (session_id, result text,
/// is_error, total_cost_usd).
pub fn parse_claude_output(stdout: &str) -> Result<(Option<String>, String, bool, Option<f64>)> {
    let value: serde_json::Value = serde_json::from_str(stdout.trim()).context("claude's output isn't JSON")?;
    let value = match value {
        serde_json::Value::Array(messages) => messages
            .into_iter()
            .rev()
            .find(|m| m["type"] == "result")
            .context("claude's output has no result message")?,
        other => other,
    };
    let out: ClaudeOutput = serde_json::from_value(value).context("claude's output isn't a result")?;
    let is_error = out.is_error || out.subtype.as_deref().is_some_and(|s| s.starts_with("error"));
    Ok((out.session_id, out.result.unwrap_or_default(), is_error, out.total_cost_usd))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::time::Instant;

    use chrono::TimeZone;

    use crate::types::{Comment, Status};

    fn runner(bin: &Path, user: Option<&str>) -> ClaudeRunner {
        ClaudeRunner {
            bin: bin.to_path_buf(),
            session_user: user.map(String::from),
            proxy_url: "http://127.0.0.1:8471".into(),
            timeout: Duration::from_secs(20),
            slot_path: "/var/lib/forge/build.lock".into(),
            forge_bin: "/usr/local/bin/forge".into(),
        }
    }

    fn request(worktree: &Path, resume: Option<&str>) -> BurstRequest {
        BurstRequest {
            repo: "example-team/shop".into(),
            issue: 412,
            role: "backend".into(),
            resume: resume.map(String::from),
            worktree: worktree.to_path_buf(),
            prompt: "Do the thing.\nCarefully.".into(),
            model: "claude-opus-5-5".into(),
            effort: "xhigh".into(),
            session_key: "fsk-test".into(),
        }
    }

    fn claude_args(resume: Option<&str>) -> Vec<String> {
        let mut args = ["-p", "--output-format", "json", "--model", "claude-opus-5-5"]
            .map(String::from)
            .to_vec();
        args.extend(["--effort", "xhigh", "--dangerously-skip-permissions"].map(String::from));
        if let Some(id) = resume {
            args.extend(["--resume".into(), id.into()]);
        }
        args
    }

    fn expected_env(with_home: bool) -> BTreeMap<String, String> {
        let mut env: BTreeMap<String, String> = [
            ("ANTHROPIC_BASE_URL", "http://127.0.0.1:8471"),
            ("ANTHROPIC_AUTH_TOKEN", "fsk-test"),
            ("FORGE_SLOT", "/var/lib/forge/build.lock"),
            ("FORGE_BIN", "/usr/local/bin/forge"),
            ("FORGE_ISSUE", "412"),
            ("DISABLE_AUTOUPDATER", "1"),
            ("CLAUDE_CODE_DISABLE_BACKGROUND_TASKS", "1"),
            ("BASH_DEFAULT_TIMEOUT_MS", "1800000"),
            ("BASH_MAX_TIMEOUT_MS", "7200000"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        for name in ["HOME", "PATH", "LANG"] {
            if let (Ok(v), true) = (std::env::var(name), with_home || name != "HOME") {
                env.insert(name.into(), v);
            }
        }
        env
    }

    #[test]
    fn new_reads_the_config() {
        let r = ClaudeRunner::new(&crate::config::sample());
        assert_eq!(r.proxy_url, "http://127.0.0.1:8471");
        assert_eq!(r.timeout, Duration::from_secs(240 * 60));
        assert_eq!(r.slot_path, PathBuf::from("/var/lib/forge/build.lock"));
        assert_eq!(r.session_user.as_deref(), Some("forge-run"));
        assert_eq!(r.bin, PathBuf::from("claude"));
        assert_eq!(r.forge_bin, PathBuf::from("/usr/local/bin/forge"));
    }

    #[test]
    fn command_line_as_forge_itself() {
        let r = runner(Path::new("/usr/bin/claude"), None);
        let (program, args, env) = r.command_line(&request(Path::new("/w"), Some("sess-1")));
        assert_eq!(program, "/usr/bin/claude");
        assert_eq!(args, claude_args(Some("sess-1")));
        assert_eq!(env.into_iter().collect::<BTreeMap<_, _>>(), expected_env(true));
    }

    #[test]
    fn command_line_as_the_session_user() {
        let r = runner(Path::new("/usr/bin/claude"), Some("forge-run"));
        let (program, args, env) = r.command_line(&request(Path::new("/w"), None));
        let names: Vec<&str> = env.iter().map(|(k, _)| k.as_str()).collect();
        let mut expected: Vec<String> = vec![
            "-n".into(),
            "-H".into(),
            "-u".into(),
            "forge-run".into(),
            format!("--preserve-env={}", names.join(",")),
            "--".into(),
            "timeout".into(),
            "-k".into(),
            "60".into(),
            "20".into(),
            "/usr/bin/claude".into(),
        ];
        expected.extend(claude_args(None));
        assert_eq!(program, "sudo");
        assert_eq!(args, expected);
        assert_eq!(env.into_iter().collect::<BTreeMap<_, _>>(), expected_env(false));
    }

    /// A fake `claude` that records its arguments, stdin, environment and
    /// directory next to itself, then runs `body`.
    fn fake_claude(dir: &Path, body: &str) -> PathBuf {
        let bin = dir.join("claude");
        let d = dir.display();
        let script =
            format!("#!/bin/sh\nprintf '%s\\0' \"$@\" > {d}/args\ncat > {d}/stdin\nenv > {d}/env\npwd > {d}/pwd\n{body}\n");
        std::fs::write(&bin, script).unwrap();
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o700)).unwrap();
        bin
    }

    const PR_JSON: &str = r#"{"type":"result","subtype":"success","is_error":false,"duration_ms":10,"result":"Done.\n\n```forge\n{\"outcome\":\"pr\",\"title\":\"Add x\",\"body\":\"Adds x.\"}\n```\n","session_id":"s-123","total_cost_usd":0.25,"usage":{"input_tokens":1}}"#;

    #[tokio::test]
    async fn burst_runs_claude_with_only_its_env() {
        let dir = tempfile::tempdir().unwrap();
        let worktree = tempfile::tempdir().unwrap();
        let bin = fake_claude(dir.path(), &format!("cat <<'EOF'\n{PR_JSON}\nEOF"));
        let mut req = request(worktree.path(), Some("s-122"));
        req.prompt = format!("{}\nThe end.", "A long prompt. ".repeat(20_000));
        let prompt = req.prompt.clone();
        let result = runner(&bin, None).burst(req).await.unwrap();
        assert_eq!(
            result,
            BurstResult {
                session_id: Some("s-123".into()),
                outcome: Outcome::Pr { title: "Add x".into(), body: "Adds x.".into() },
                cost_usd: Some(0.25),
                is_error: false,
            }
        );
        let args = std::fs::read_to_string(dir.path().join("args")).unwrap();
        let args: Vec<&str> = args.trim_end_matches('\0').split('\0').collect();
        assert_eq!(args, claude_args(Some("s-122")));
        assert!(prompt.len() > 128 * 1024);
        assert_eq!(std::fs::read_to_string(dir.path().join("stdin")).unwrap(), prompt);
        let env: BTreeMap<String, String> = std::fs::read_to_string(dir.path().join("env"))
            .unwrap()
            .lines()
            .filter_map(|l| l.split_once('='))
            .filter(|(k, _)| !["PWD", "OLDPWD", "SHLVL", "_"].contains(k))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(env, expected_env(true));
        let pwd = std::fs::read_to_string(dir.path().join("pwd")).unwrap();
        assert_eq!(Path::new(pwd.trim()).canonicalize().unwrap(), worktree.path().canonicalize().unwrap());
    }

    #[tokio::test]
    async fn burst_past_its_time_is_stopped() {
        let dir = tempfile::tempdir().unwrap();
        let bin = fake_claude(dir.path(), "echo working >&2\nsleep 30\necho '{}'");
        let mut r = runner(&bin, None);
        r.timeout = Duration::from_millis(300);
        let started = Instant::now();
        let result = r.burst(request(dir.path(), Some("s-1"))).await.unwrap();
        assert!(started.elapsed() < Duration::from_secs(4), "took {:?}", started.elapsed());
        assert_eq!(
            result,
            BurstResult {
                session_id: Some("s-1".into()),
                outcome: Outcome::Blocked { reason: "the session ran past its time limit".into() },
                cost_usd: None,
                is_error: true,
            }
        );
    }

    #[tokio::test]
    async fn burst_that_fails_says_why() {
        let dir = tempfile::tempdir().unwrap();
        let noise: String = (0..50).map(|i| format!("echo noise-{i} >&2\n")).collect();
        let bin = fake_claude(dir.path(), &format!("{noise}echo 'boom: no plan' >&2\nexit 2"));
        let result = runner(&bin, None).burst(request(dir.path(), None)).await.unwrap();
        assert!(result.is_error);
        assert_eq!(result.session_id, None);
        let Outcome::Blocked { reason } = result.outcome else { panic!("{:?}", result.outcome) };
        assert!(reason.contains("boom: no plan") && reason.contains("noise-49"), "{reason}");
        assert!(!reason.contains("noise-1\n"), "{reason}");
    }

    #[tokio::test]
    async fn burst_with_an_error_result_keeps_its_session() {
        let dir = tempfile::tempdir().unwrap();
        let json = r#"{"type":"result","subtype":"error_during_execution","is_error":true,"session_id":"s-9","total_cost_usd":0.1}"#;
        let bin = fake_claude(dir.path(), &format!("echo '{json}'\necho 'API error: overloaded' >&2\nexit 1"));
        let result = runner(&bin, None).burst(request(dir.path(), None)).await.unwrap();
        assert!(result.is_error);
        assert_eq!(result.session_id.as_deref(), Some("s-9"));
        assert_eq!(result.cost_usd, Some(0.1));
        let Outcome::Blocked { reason } = result.outcome else { panic!("{:?}", result.outcome) };
        assert!(reason.contains("API error: overloaded"), "{reason}");
    }

    fn thread() -> IssueThread {
        let at = |h| chrono::Utc.with_ymd_and_hms(2026, 10, 19, h, 0, 0).unwrap();
        IssueThread {
            number: 412,
            title: "Export invoices as CSV".into(),
            body: "Add a CSV export to the invoices page.".into(),
            author: "bob-gh".into(),
            labels: vec![],
            url: "https://github.com/example-team/shop/issues/412".into(),
            comments: vec![
                Comment { id: 5, author: "ann-gh".into(), body: "Keep it to one button.".into(), created_at: at(9) },
                Comment { id: 9, author: "bob-gh".into(), body: "Use ISO dates.".into(), created_at: at(10) },
            ],
        }
    }

    fn task() -> Task {
        let mut t = Task::new("example-team/shop", 412, "Export invoices as CSV", "bob-gh", Status::Building, "ui");
        t.last_seen_comment = 5;
        t
    }

    fn repo() -> RepoConfig {
        let mut r = crate::config::sample().repos[0].clone();
        r.notes = Some("Migrations are numbered; never reuse a number.".into());
        r
    }

    #[test]
    fn build_prompt_says_what_every_session_is_told() {
        let p = build_prompt(&repo(), "You build UI.", &thread(), &BurstReason::Start, &task());
        for part in [
            "You build UI.",
            "issue #412 of example-team/shop in this git worktree, on branch `forge/412`",
            "Lines added are a cost.",
            "Commit your work on this branch",
            "Never push, never open PRs, never comment on GitHub",
            "`$FORGE_BIN slot -- <build command>`",
            "use synthetic data only",
            "Migrations are numbered",
            "## Issue #412: Export invoices as CSV",
            "Opened by @bob-gh.",
            "Add a CSV export to the invoices page.",
            "### @ann-gh, 2026-10-19 09:00 UTC\n\nKeep it to one button.",
            "### @bob-gh, 2026-10-19 10:00 UTC (new)\n\nUse ISO dates.",
            "The task just started.",
            "```forge\n{\"outcome\": \"pr\", \"title\": \"...\", \"body\": \"...\"}\n```",
            "{\"outcome\": \"question\", \"text\": \"...\"}",
            "{\"outcome\": \"blocked\", \"reason\": \"...\"}",
        ] {
            assert!(p.contains(part), "missing {part:?} in:\n{p}");
        }
        assert_eq!(p.matches("(new)").count(), 1);
    }

    #[test]
    fn build_prompt_carries_the_reason_in_a_block() {
        let notes = "Drop the helper.\n```rust\nfn helper() {}\n```";
        let reason = BurstReason::ReviewNotes { notes: notes.into() };
        let p = build_prompt(&repo(), "", &thread(), &reason, &task());
        assert!(p.contains(&format!("````\n{notes}\n````")), "{p}");
        assert!(p.starts_with("## Your task"));

        let output = format!("HEAD-OF-OUTPUT\n{}\nmigration 0042 is used twice\n", "x".repeat(20_000));
        let reason = BurstReason::MergeCheckFailed { output: output.clone() };
        let p = build_prompt(&repo(), "", &thread(), &reason, &task());
        assert!(p.contains("`./scripts/check-migration-numbers.sh` failed"));
        assert!(p.contains("[...]\n") && p.contains("migration 0042 is used twice\n```"));
        assert!(!p.contains("HEAD-OF-OUTPUT"));
        assert!(p.len() < output.len());

        let reason = BurstReason::RebaseConflict { output: "CONFLICT (content): Merge conflict in a.rs".into() };
        let p = build_prompt(&repo(), "", &thread(), &reason, &task());
        assert!(p.contains("Run `git fetch origin`, rebase it on `origin/main` yourself"));
        assert!(p.contains("```\nCONFLICT (content): Merge conflict in a.rs\n```"));
    }

    #[test]
    fn review_prompt_asks_for_a_verdict() {
        let p = review_prompt(&repo(), "You review.", &thread(), "abc123");
        for part in [
            "You review.",
            "reviewing the change for issue #412 of example-team/shop",
            "on branch `forge/412`",
            "`git diff origin/main...abc123`, not HEAD",
            "no needless code",
            "Change nothing",
            "Never push, never open PRs, never comment on GitHub",
            "`$FORGE_BIN slot -- <build command>`",
            "use synthetic data only",
            "Use ISO dates.",
            "```forge\n{\"outcome\": \"review\", \"pass\": true, \"notes\": \"...\"}\n```",
        ] {
            assert!(p.contains(part), "missing {part:?} in:\n{p}");
        }
        assert!(!p.contains("(new)"));
    }

    #[test]
    fn parse_outcome_takes_the_last_forge_block() {
        let text = "First:\n```forge\n{\"outcome\":\"question\",\"text\":\"a?\"}\n```\nThen:\n```json\n{}\n```\n\n```forge\n{\"outcome\": \"pr\", \"title\": \"T\",\n \"body\": \"B\"}\n```\nBye.";
        assert_eq!(parse_outcome(text), Outcome::Pr { title: "T".into(), body: "B".into() });
        let review = "```forge\n{\"outcome\":\"review\",\"pass\":false,\"notes\":\"n\"}\n```";
        assert_eq!(parse_outcome(review), Outcome::Review { pass: false, notes: "n".into() });
        let unclosed = "```forge\n{\"outcome\":\"blocked\",\"reason\":\"no db\"}";
        assert_eq!(parse_outcome(unclosed), Outcome::Blocked { reason: "no db".into() });
    }

    #[test]
    fn parse_outcome_blocks_on_a_missing_or_bad_block() {
        let none = parse_outcome("All done, see the commits.\n```json\n{\"outcome\":\"pr\"}\n```");
        assert_eq!(none, Outcome::Blocked { reason: "no forge block in the session's answer".into() });
        let Outcome::Blocked { reason } = parse_outcome("```forge\n{\"outcome\": \"pr\", \"title\": }\n```") else {
            panic!("bad JSON should block");
        };
        assert!(reason.starts_with("the session's forge block isn't a valid outcome"), "{reason}");
        let Outcome::Blocked { reason } = parse_outcome("```forge\n{\"outcome\": \"shipped\"}\n```") else {
            panic!("an unknown outcome should block");
        };
        assert!(reason.contains("isn't a valid outcome"), "{reason}");
    }

    #[test]
    fn parse_claude_output_reads_the_result() {
        let parsed = parse_claude_output(&format!("{PR_JSON}\n")).unwrap();
        assert_eq!(parsed.0.as_deref(), Some("s-123"));
        assert!(parsed.1.starts_with("Done.\n\n```forge\n"));
        assert!(!parsed.2);
        assert_eq!(parsed.3, Some(0.25));

        let error = r#"{"type":"result","subtype":"error_max_turns","session_id":"s-2","is_error":false}"#;
        assert_eq!(parse_claude_output(error).unwrap(), (Some("s-2".into()), String::new(), true, None));

        let verbose = r#"[{"type":"system","session_id":"s-3"},{"type":"result","result":"ok","session_id":"s-3"}]"#;
        assert_eq!(parse_claude_output(verbose).unwrap(), (Some("s-3".into()), "ok".into(), false, None));

        assert!(parse_claude_output("").is_err());
        assert!(parse_claude_output("Error: not logged in").is_err());
        assert!(parse_claude_output("[1, 2]").is_err());
    }
}
