//! Each task's Claude runs interactively in its own tmux session, so people can
//! watch it (`forge watch`: `tmux attach -r`) or join it (`forge attach`:
//! `tmux attach`) at any time, mid-turn included.
//!
//! A build burst types its prompt into the task's session, starting Claude
//! there first if it isn't running, and waits for the turn to end: Claude Code's
//! Stop hook runs `forge turn-done`, which writes the turn's last message to the
//! task's turn file. A turn that ends without a ```forge block is nudged once.
//! Claude stays up between bursts with a per-task proxy key, so a person who
//! joins can keep talking to it. Review bursts stay headless, in their own
//! checkout.

use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::{bail, ensure, Context, Result};
use async_trait::async_trait;
use serde_json::{json, Value};

use crate::config::{Config, REVIEW_ROLE};
use crate::proxy::{tmux_tag, SessionKeys};
use crate::session::{has_forge_block, parse_outcome, ClaudeRunner, Runner, CLAUDE_ENV};
use crate::types::{BurstRequest, BurstResult, Outcome};

/// What a turn that ended without a forge block is sent once.
const NUDGE: &str = "End your turn with your ```forge block, as the rules above say.";
/// How often a burst looks for its turn's end, and how many looks go by
/// between checks that Claude is still running (each one a sudo).
const POLL: Duration = Duration::from_secs(2);
const ALIVE_EVERY: u32 = 5;
/// How long after a start a burst watches the screen for Claude Code's
/// folder-trust question, and what that question says.
const SETTLE: Duration = Duration::from_secs(60);
const TRUST_QUESTION: &str = "trust this folder";
/// Every session's size, whoever attaches: narrow enough for the dashboard
/// to show a line whole. And the scrollback each pane keeps.
const WIDTH: &str = "120";
const HEIGHT: &str = "50";
const HISTORY: &str = "10000";
/// A session whose screen hasn't changed for this long is stuck: a working
/// Claude's spinner and tool timers tick every second.
const STALL: Duration = Duration::from_secs(15 * 60);
/// The most scrollback the dashboard may ask for at once.
pub const MAX_HISTORY: u32 = 10_000;

/// The tmux session of a task: `<owner>-<repo>-<issue>`, in tmux's characters.
/// Commands take it as `=<name>` (exactly that session) or, for its window and
/// pane, `=<name>:`.
pub fn session_name(repo: &str, issue: u64) -> String {
    let safe: String = repo.chars().map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '-' }).collect();
    format!("{safe}-{issue}")
}

/// `tmux -S <socket>` as the session user (or as Forge itself when there is none).
fn tmux_command(user: Option<&str>, socket: &Path) -> tokio::process::Command {
    let mut cmd = match user {
        Some(user) => {
            let mut c = tokio::process::Command::new("sudo");
            c.args(["-n", "-H", "-u", user, "--", "tmux"]);
            c
        }
        None => tokio::process::Command::new("tmux"),
    };
    cmd.arg("-S").arg(socket).kill_on_drop(true);
    cmd
}

/// `'text'`, safe to put in a shell command.
fn sh_quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', r"'\''"))
}

pub struct TmuxRunner {
    headless: ClaudeRunner,
    socket: PathBuf,
    /// Prompt and turn files, and the Claude settings with the Stop hook.
    dir: PathBuf,
    keys: SessionKeys,
    /// Each running session's proxy key, revoked when its Claude is replaced
    /// or the session ends.
    live: Mutex<HashMap<String, String>>,
    /// How long a screen may stay the same before the burst calls it stuck.
    stall: Duration,
}

enum Turn {
    Done { session_id: Option<String>, text: String },
    Exited,
    TimedOut,
    Stalled,
}

impl TmuxRunner {
    pub fn new(cfg: &Config, keys: SessionKeys) -> TmuxRunner {
        TmuxRunner {
            headless: ClaudeRunner::new(cfg),
            socket: cfg.tmux_socket(),
            dir: cfg.sessions_dir(),
            keys,
            live: Mutex::new(HashMap::new()),
            stall: STALL,
        }
    }

    async fn tmux(&self, args: &[&str]) -> Result<std::process::Output> {
        let out = tmux_command(self.headless.session_user.as_deref(), &self.socket)
            .args(args)
            .output()
            .await
            .context("running tmux")?;
        Ok(out)
    }

    async fn tmux_ok(&self, args: &[&str]) -> Result<String> {
        let out = self.tmux(args).await?;
        if !out.status.success() {
            bail!("tmux {}: {}", args.first().unwrap_or(&""), String::from_utf8_lossy(&out.stderr).trim());
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    /// Whether the session exists and its Claude is still running.
    async fn running(&self, name: &str) -> bool {
        let target = format!("={name}:");
        match self.tmux(&["display-message", "-p", "-t", &target, "#{pane_dead}"]).await {
            Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout).trim() == "0",
            _ => false,
        }
    }

    /// Starts Claude in the task's session (making the session if needed) with
    /// the prompt file as its first message and a fresh per-task proxy key.
    async fn start(&self, name: &str, req: &BurstRequest, prompt: &Path, turn: &Path) -> Result<()> {
        self.trust(&req.worktree).await;
        let settings = self.dir.join("claude-settings.json");
        // StopFailure: a turn that ends on an API error ends all the same.
        let hook = format!("{} turn-done", self.headless.forge_bin.display());
        let done = json!([ { "hooks": [ { "type": "command", "command": hook } ] } ]);
        let body = json!({ "hooks": { "Stop": done, "StopFailure": done } });
        std::fs::write(&settings, serde_json::to_vec_pretty(&body)?).context("writing Claude's settings")?;
        // By tag, not just `live`: a key from before a restart is saved, not in `live`.
        let tag = tmux_tag(&req.repo, req.issue);
        self.keys.revoke_tagged(&tag);
        let key = self.keys.issue(&tag);
        self.live.lock().unwrap().insert(name.to_string(), key.clone());
        let mut claude = format!(
            "exec {} --model {} --effort {} --dangerously-skip-permissions --settings {}",
            sh_quote(&self.headless.bin.display().to_string()),
            sh_quote(&req.model),
            sh_quote(&req.effort),
            sh_quote(&settings.display().to_string()),
        );
        if let Some(id) = &req.resume {
            claude.push_str(&format!(" --resume {}", sh_quote(id)));
        }
        claude.push_str(&format!(" \"$(cat {})\"", sh_quote(&prompt.display().to_string())));
        let env = [
            ("ANTHROPIC_BASE_URL", self.headless.proxy_url.clone()),
            ("ANTHROPIC_AUTH_TOKEN", key),
            ("FORGE_SLOT", self.headless.slot_path.display().to_string()),
            ("FORGE_BIN", self.headless.forge_bin.display().to_string()),
            ("FORGE_ISSUE", req.issue.to_string()),
            ("FORGE_TURN_FILE", turn.display().to_string()),
            ("LANG", "C.UTF-8".into()),
        ];
        let env: Vec<String> = env.iter().map(|(k, v)| format!("{k}={v}")).chain(CLAUDE_ENV.iter().map(|(k, v)| format!("{k}={v}"))).collect();
        let dir = req.worktree.display().to_string();
        let (session, target) = (format!("={name}"), format!("={name}:"));
        let mut args: Vec<&str> = Vec::new();
        let exists = self.tmux(&["has-session", "-t", &session]).await.is_ok_and(|o| o.status.success());
        if exists {
            args.extend(["respawn-pane", "-k", "-t", &target, "-c", &dir]);
        } else {
            args.extend(["start-server", ";", "set-option", "-g", "history-limit", HISTORY, ";"]);
            args.extend(["new-session", "-d", "-s", name, "-x", WIDTH, "-y", HEIGHT, "-c", &dir]);
        }
        for e in &env {
            args.extend(["-e", e]);
        }
        args.push(&claude);
        if !exists {
            // Keep a pane whose Claude exits, so its last screen stays to read,
            // and fix the size (resize-window makes it manual): an attaching
            // terminal no longer reflows it.
            args.extend([";", "set-option", "-w", "-t", &target, "remain-on-exit", "on"]);
            args.extend([";", "resize-window", "-t", &target, "-x", WIDTH, "-y", HEIGHT]);
        }
        self.tmux_ok(&args).await.map(drop)
    }

    /// Runs `forge trust <dir>` as the session user, so its Claude doesn't stop
    /// on the folder-trust question. A failure only logs: `wait` still answers
    /// the question if it shows.
    async fn trust(&self, dir: &Path) {
        let mut cmd = match self.headless.session_user.as_deref() {
            Some(user) => {
                let mut c = tokio::process::Command::new("sudo");
                c.args(["-n", "-H", "-u", user, "--"]).arg(&self.headless.forge_bin);
                c
            }
            None => tokio::process::Command::new(&self.headless.forge_bin),
        };
        match cmd.arg("trust").arg(dir).output().await {
            Ok(out) if out.status.success() => {}
            Ok(out) => tracing::warn!(dir = %dir.display(), "forge trust: {}", String::from_utf8_lossy(&out.stderr).trim()),
            Err(e) => tracing::warn!(dir = %dir.display(), "forge trust: {e}"),
        }
    }

    /// Whether the session's Claude is older than the one installed: root
    /// updates the binary in place (forge-claude-update), but a running Claude
    /// stays the version it started as. Such a session is restarted with
    /// --resume at its next burst, keeping its conversation.
    async fn outdated(&self, name: &str) -> bool {
        let target = format!("={name}:");
        let Ok(pid) = self.tmux_ok(&["display-message", "-p", "-t", &target, "#{pane_pid}"]).await else { return false };
        if pid.is_empty() || !pid.bytes().all(|b| b.is_ascii_digit()) {
            return false;
        }
        // No answer from /proc (not Linux, or the process is gone) is not outdated.
        let check = format!(
            "running=$(readlink /proc/{pid}/exe) && [ -n \"$running\" ] && [ \"$(readlink -f {})\" != \"$running\" ]",
            sh_quote(&self.headless.bin.display().to_string())
        );
        let mut cmd = match self.headless.session_user.as_deref() {
            Some(user) => {
                let mut c = tokio::process::Command::new("sudo");
                c.args(["-n", "-H", "-u", user, "--", "sh", "-c", &check]);
                c
            }
            None => {
                let mut c = tokio::process::Command::new("sh");
                c.args(["-c", &check]);
                c
            }
        };
        let outdated = cmd.output().await.is_ok_and(|o| o.status.success());
        if outdated {
            tracing::info!(session = name, "restarting on the newer Claude Code");
        }
        outdated
    }

    /// Types the file into the running Claude as one paste, then Enter.
    async fn paste(&self, name: &str, file: &Path) -> Result<()> {
        let target = format!("={name}:");
        let buffer = format!("forge-{name}");
        self.tmux_ok(&["load-buffer", "-b", &buffer, &file.display().to_string()]).await?;
        self.tmux_ok(&["paste-buffer", "-p", "-d", "-b", &buffer, "-t", &target]).await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
        self.tmux_ok(&["send-keys", "-t", &target, "Enter"]).await.map(drop)
    }

    /// Waits for the turn file, Claude exiting, the screen standing still for
    /// `stall`, or the deadline. Until `settle`, it also answers Claude Code's
    /// folder-trust question if it shows.
    async fn wait(&self, name: &str, turn: &Path, deadline: Instant, settle: Option<Instant>) -> Turn {
        let target = format!("={name}:");
        // The screen is compared about 15 times per stall (each look is a sudo).
        let check_every = (self.stall / 15).max(POLL);
        let (mut last_screen, mut checked, mut still_since) = (String::new(), Instant::now(), Instant::now());
        for look in 0u32.. {
            if checked.elapsed() >= check_every {
                checked = Instant::now();
                let screen = self.tmux_ok(&["capture-pane", "-p", "-t", &target]).await.unwrap_or_default();
                if screen != last_screen {
                    (last_screen, still_since) = (screen, Instant::now());
                } else if still_since.elapsed() >= self.stall {
                    return Turn::Stalled;
                }
            }
            if settle.is_some_and(|until| Instant::now() < until) {
                let screen = self.tmux_ok(&["capture-pane", "-p", "-t", &target]).await.unwrap_or_default();
                if let Some(key) = trust_key(&screen) {
                    tracing::info!(session = name, key, "answering Claude's folder-trust question");
                    let _ = self.tmux(&["send-keys", "-t", &target, key]).await;
                }
            }
            if let Ok(bytes) = std::fs::read(turn) {
                if let Ok(v) = serde_json::from_slice::<Value>(&bytes) {
                    let _ = std::fs::remove_file(turn);
                    return Turn::Done {
                        session_id: v["session_id"].as_str().map(String::from),
                        text: v["text"].as_str().unwrap_or_default().to_string(),
                    };
                }
            }
            if look % ALIVE_EVERY == ALIVE_EVERY - 1 && !self.running(name).await {
                return Turn::Exited;
            }
            if Instant::now() >= deadline {
                return Turn::TimedOut;
            }
            tokio::time::sleep(POLL).await;
        }
        unreachable!("the loop only returns")
    }

    /// The last lines on the session's screen, for a failed burst's reason.
    async fn screen(&self, name: &str) -> String {
        let target = format!("={name}:");
        let out = self.tmux_ok(&["capture-pane", "-p", "-t", &target]).await.unwrap_or_default();
        let lines: Vec<&str> = out.lines().filter(|l| !l.trim().is_empty()).collect();
        lines[lines.len().saturating_sub(15)..].join("\n")
    }

    /// Waits out the burst's turn, nudging once if it ends without a forge block.
    async fn finish_turn(&self, name: &str, req: &BurstRequest, prompt: &Path, turn: &Path, settle: Option<Instant>) -> Result<BurstResult> {
        let deadline = Instant::now() + self.headless.timeout;
        let mut nudged = false;
        loop {
            match self.wait(name, turn, deadline, settle).await {
                Turn::Done { session_id, text } => {
                    // Nudged once, and only a Claude that's still there.
                    if !has_forge_block(&text) && !nudged && self.running(name).await {
                        nudged = true;
                        std::fs::write(prompt, NUDGE).context("writing the nudge")?;
                        self.paste(name, prompt).await?;
                        continue;
                    }
                    return Ok(BurstResult {
                        session_id: session_id.or_else(|| req.resume.clone()),
                        outcome: parse_outcome(&text),
                        cost_usd: None,
                        is_error: false,
                    });
                }
                Turn::Exited => {
                    let screen = self.screen(name).await;
                    return Ok(failed(req.resume.clone(), format!("claude exited in its tmux session:\n{screen}")));
                }
                Turn::Stalled => {
                    // Claude stays up, so a person can look, or answer on the issue.
                    let screen = self.screen(name).await;
                    let minutes = self.stall.as_secs() / 60;
                    return Ok(failed(req.resume.clone(), format!("the session's screen hasn't changed for {minutes} minutes:\n{screen}")));
                }
                Turn::TimedOut => {
                    let target = format!("={name}:");
                    let _ = self.tmux(&["send-keys", "-t", &target, "Escape"]).await;
                    return Ok(failed(req.resume.clone(), "the session ran past its time limit".into()));
                }
            }
        }
    }
}

/// The key that moves Claude Code's folder-trust question towards "Yes": Enter
/// when "Yes" is chosen, Down when it isn't, None when the question isn't shown.
fn trust_key(screen: &str) -> Option<&'static str> {
    if !screen.contains(TRUST_QUESTION) {
        return None;
    }
    let chosen = screen.lines().find(|l| l.trim_start().starts_with('❯')).unwrap_or("");
    Some(if chosen.contains("Yes") { "Enter" } else { "Down" })
}

/// `forge trust <dir>`, run as the session user before its Claude starts: marks
/// the folder trusted in ~/.claude.json. Trusting the worktrees folder once
/// covered every task's folder under it, until a Claude Code update stopped
/// honouring that and a task waited on the question.
pub fn trust(dir: &Path) -> Result<()> {
    let home = std::env::var_os("HOME").context("HOME is not set")?;
    trust_in(&Path::new(&home).join(".claude.json"), dir)
}

fn trust_in(config: &Path, dir: &Path) -> Result<()> {
    let mut v: Value = match std::fs::read(config) {
        Ok(bytes) => serde_json::from_slice(&bytes).context("reading ~/.claude.json")?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => json!({}),
        Err(e) => return Err(e).context("reading ~/.claude.json"),
    };
    ensure!(v.is_object(), "~/.claude.json isn't a JSON object");
    let key = dir.display().to_string();
    if v["projects"][&key]["hasTrustDialogAccepted"] == true {
        return Ok(());
    }
    v["projects"][&key]["hasTrustDialogAccepted"] = json!(true);
    let tmp = config.with_extension("json.forge");
    {
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;
        let mut f = std::fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(&tmp)?;
        f.write_all(&serde_json::to_vec_pretty(&v)?)?;
    }
    std::fs::rename(&tmp, config).context("writing ~/.claude.json")
}

fn failed(session_id: Option<String>, reason: String) -> BurstResult {
    BurstResult { session_id, outcome: Outcome::Blocked { reason }, cost_usd: None, is_error: true }
}

#[async_trait]
impl Runner for TmuxRunner {
    async fn burst(&self, req: BurstRequest) -> Result<BurstResult> {
        if req.role == REVIEW_ROLE {
            return self.headless.burst(req).await;
        }
        let name = session_name(&req.repo, req.issue);
        let turn = self.dir.join(format!("{name}.turn.json"));
        let prompt = self.dir.join(format!("{name}.prompt.md"));
        // Each burst removes its prompt file when it ends, so one still there
        // means Forge restarted mid-burst: if that turn has ended or Claude is
        // still on it, take its answer rather than send the prompt again.
        let in_flight = prompt.exists() && (turn.exists() || self.running(&name).await);
        let mut settle = None;
        if in_flight {
            tracing::info!(session = name, "carrying on with the turn already running");
        } else {
            std::fs::write(&prompt, &req.prompt).context("writing the prompt")?;
            let _ = std::fs::remove_file(&turn);
            if self.running(&name).await && !(req.resume.is_some() && self.outdated(&name).await) {
                self.paste(&name, &prompt).await?;
            } else {
                self.start(&name, &req, &prompt, &turn).await?;
                settle = Some(Instant::now() + SETTLE);
            }
        }
        let result = self.finish_turn(&name, &req, &prompt, &turn, settle).await;
        let _ = std::fs::remove_file(&prompt);
        result
    }

    async fn finish(&self, repo: &str, issue: u64) {
        let name = session_name(repo, issue);
        let target = format!("={name}");
        let _ = self.tmux(&["kill-session", "-t", &target]).await;
        self.live.lock().unwrap().remove(&name);
        self.keys.revoke_tagged(&tmux_tag(repo, issue));
        for file in [format!("{name}.turn.json"), format!("{name}.prompt.md")] {
            let _ = std::fs::remove_file(self.dir.join(file));
        }
    }
}

/// Keys a person may send to a session from the dashboard, besides text.
pub const KEYS: [&str; 10] = ["Enter", "Escape", "C-c", "Up", "Down", "Left", "Right", "Tab", "BTab", "BSpace"];

/// What a person sends from the dashboard: text (and Enter after it if
/// `submit`), or one of [`KEYS`].
pub enum Input<'a> {
    Text { text: &'a str, submit: bool },
    Key(&'a str),
}

/// The task's tmux screen with its colours (ANSI escapes) and up to
/// `history` lines of scrollback above it, or None when the task has no live
/// session. Lines the terminal wrapped come back whole.
pub async fn screen(cfg: &Config, repo: &str, issue: u64, history: u32) -> Result<Option<String>> {
    let target = format!("={}:", session_name(repo, issue));
    let start = format!("-{}", history.min(MAX_HISTORY));
    let out = tmux_command(cfg.claude.session_user.as_deref(), &cfg.tmux_socket())
        .args(["capture-pane", "-p", "-e", "-J", "-S", &start, "-t", &target])
        .output()
        .await
        .context("running tmux")?;
    Ok(out.status.success().then(|| String::from_utf8_lossy(&out.stdout).into_owned()))
}

/// Types into the task's live session; false when it has none.
pub async fn type_into(cfg: &Config, repo: &str, issue: u64, input: &Input<'_>) -> Result<bool> {
    let target = format!("={}:", session_name(repo, issue));
    let send = |args: Vec<&str>| {
        let mut cmd = tmux_command(cfg.claude.session_user.as_deref(), &cfg.tmux_socket());
        cmd.arg("send-keys").arg("-t").arg(&target).args(args);
        cmd
    };
    let ran = match input {
        Input::Key(key) => {
            ensure!(KEYS.contains(key), "unknown key {key:?}");
            send(vec![key]).output().await?
        }
        Input::Text { text, submit } => {
            let typed = send(vec!["-l", "--", text]).output().await?;
            if typed.status.success() && *submit {
                tokio::time::sleep(Duration::from_millis(150)).await;
                send(vec!["Enter"]).output().await?
            } else {
                typed
            }
        }
    };
    Ok(ran.status.success())
}

/// `forge turn-done`, Claude Code's Stop hook: writes the session id and the
/// turn's last message (read from the transcript the hook names) to
/// $FORGE_TURN_FILE, whole or not at all. Outside Forge's sessions it does nothing.
pub fn turn_done(hook_input: &str) -> Result<()> {
    let Ok(path) = std::env::var("FORGE_TURN_FILE") else { return Ok(()) };
    write_turn(hook_input, Path::new(&path))
}

fn write_turn(hook_input: &str, out: &Path) -> Result<()> {
    let hook: Value = serde_json::from_str(hook_input).context("reading the hook's input")?;
    let transcript = hook["transcript_path"].as_str().unwrap_or_default();
    // A turn that failed says so, rather than repeat an older message.
    let failed = (hook["hook_event_name"] == "StopFailure").then(|| {
        let why = hook["error"].as_str().or(hook["error"]["message"].as_str()).unwrap_or("no reason given");
        format!("The turn stopped on an error: {why}")
    });
    let mut text = failed.clone();
    // The transcript can lag the hook, and then its last message is an older
    // one. Claude Code names the last message in the hook (clipping a long
    // one), so read the transcript until it ends on that message, or else use
    // the hook's copy.
    let said = hook["last_assistant_message"].as_str().filter(|t| !t.trim().is_empty());
    if failed.is_none() {
        for _ in 0..20 {
            text = last_assistant_text(&std::fs::read_to_string(transcript).unwrap_or_default());
            match (said, text.as_deref()) {
                (Some(said), Some(read)) if same_message(said, read) => break,
                (None, Some(_)) => break,
                _ => std::thread::sleep(Duration::from_millis(300)),
            }
        }
        if let Some(said) = said.filter(|said| !text.as_deref().is_some_and(|read| same_message(said, read))) {
            text = Some(said.to_string());
        }
    }
    let body = json!({ "session_id": hook["session_id"], "text": text.unwrap_or_default() });
    // Readable by Forge, which doesn't share the session user's group.
    let tmp = out.with_extension("tmp");
    std::fs::write(&tmp, serde_json::to_vec(&body)?).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o644))?;
    std::fs::rename(&tmp, out).with_context(|| format!("writing {}", out.display()))
}

/// The text of the transcript's last assistant message that has any.
/// Whether the transcript's message is the one the hook named, which Claude
/// Code may have clipped ("… [+N chars]").
fn same_message(said: &str, read: &str) -> bool {
    let head = said.rsplit_once("\u{2026} [+").map_or(said, |(head, _)| head);
    read.trim_start().starts_with(head.trim())
}

fn last_assistant_text(transcript: &str) -> Option<String> {
    transcript.lines().rev().find_map(|line| {
        let entry: Value = serde_json::from_str(line).ok()?;
        if entry["type"] != "assistant" {
            return None;
        }
        let texts: Vec<&str> = entry["message"]["content"]
            .as_array()?
            .iter()
            .filter(|b| b["type"] == "text")
            .filter_map(|b| b["text"].as_str())
            .collect();
        (!texts.is_empty()).then(|| texts.join("\n"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_names_are_safe_for_tmux() {
        assert_eq!(session_name("example-team/shop", 1676), "example-team-shop-1676");
        assert_eq!(session_name("o/my.repo", 2), "o-my-repo-2");
    }

    #[test]
    fn quoting_survives_quotes() {
        assert_eq!(sh_quote("it's"), r"'it'\''s'");
    }

    #[test]
    fn the_hook_writes_the_last_message() {
        let tmp = tempfile::tempdir().unwrap();
        let transcript = tmp.path().join("t.jsonl");
        let lines = [
            json!({ "type": "assistant", "message": { "content": [{ "type": "text", "text": "earlier" }] } }),
            json!({ "type": "user", "message": { "content": [{ "type": "tool_result", "content": "ok" }] } }),
            json!({ "type": "assistant", "message": { "content": [
                { "type": "text", "text": "Done.\n```forge\n{\"outcome\":\"pr\",\"title\":\"t\",\"body\":\"b\"}\n```" }
            ] } }),
            json!({ "type": "assistant", "message": { "content": [{ "type": "tool_use", "name": "Bash" }] } }),
            json!({ "type": "system" }),
        ];
        std::fs::write(&transcript, lines.iter().map(|l| l.to_string() + "\n").collect::<String>()).unwrap();
        let out = tmp.path().join("turn.json");
        let hook = json!({ "session_id": "s-1", "transcript_path": transcript });
        write_turn(&hook.to_string(), &out).unwrap();
        let v: Value = serde_json::from_slice(&std::fs::read(&out).unwrap()).unwrap();
        assert_eq!(v["session_id"], "s-1");
        assert!(has_forge_block(v["text"].as_str().unwrap()));
        assert!(!out.with_extension("tmp").exists());
        assert_eq!(std::fs::metadata(&out).unwrap().permissions().mode() & 0o777, 0o644);
    }

    #[test]
    fn the_trust_question_is_answered_yes() {
        let asked = "Quick safety check: Is this a project you created or one you trust?\n\n ❯ No, exit\n   Yes, I trust this folder\n";
        assert_eq!(trust_key(asked), Some("Down"));
        assert_eq!(trust_key(&asked.replace(" ❯ No", "   No").replace("   Yes", " ❯ Yes")), Some("Enter"));
        assert_eq!(trust_key("❯ \n ⏵⏵ bypass permissions on"), None);
    }

    #[test]
    fn trusting_a_folder_keeps_the_rest_of_claude_json() {
        let tmp = tempfile::tempdir().unwrap();
        let config = tmp.path().join(".claude.json");
        std::fs::write(&config, r#"{"hasCompletedOnboarding":true,"projects":{"/a":{"hasTrustDialogAccepted":true,"x":1}}}"#).unwrap();
        trust_in(&config, Path::new("/w/forge-7")).unwrap();
        let v: Value = serde_json::from_slice(&std::fs::read(&config).unwrap()).unwrap();
        assert_eq!(v["hasCompletedOnboarding"], true);
        assert_eq!(v["projects"]["/a"]["x"], 1);
        assert_eq!(v["projects"]["/w/forge-7"]["hasTrustDialogAccepted"], true);
        assert_eq!(std::fs::metadata(&config).unwrap().permissions().mode() & 0o777, 0o600);

        let fresh = tmp.path().join("new.json");
        trust_in(&fresh, Path::new("/w/forge-8")).unwrap();
        let v: Value = serde_json::from_slice(&std::fs::read(&fresh).unwrap()).unwrap();
        assert_eq!(v["projects"]["/w/forge-8"]["hasTrustDialogAccepted"], true);
    }

    #[test]
    fn the_hook_waits_for_the_message_it_was_told_of() {
        let tmp = tempfile::tempdir().unwrap();
        let transcript = tmp.path().join("t.jsonl");
        let say = |text: &str| json!({ "type": "assistant", "message": { "content": [{ "type": "text", "text": text }] } }).to_string() + "\n";
        let last = "Done.\n```forge\n{\"outcome\":\"pr\",\"title\":\"t\",\"body\":\"b\"}\n```";
        std::fs::write(&transcript, say("Status: still compiling.")).unwrap();
        let out = tmp.path().join("turn.json");
        let hook = json!({ "session_id": "s-1", "transcript_path": transcript, "last_assistant_message": "Done.\n```forge\n{\"outcome\" \u{2026} [+40 chars]" });
        // The final message lands in the transcript a moment after the hook runs.
        let (path, late) = (transcript.clone(), say(last));
        let writer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(700));
            let mut f = std::fs::OpenOptions::new().append(true).open(path).unwrap();
            std::io::Write::write_all(&mut f, late.as_bytes()).unwrap();
        });
        write_turn(&hook.to_string(), &out).unwrap();
        writer.join().unwrap();
        let v: Value = serde_json::from_slice(&std::fs::read(&out).unwrap()).unwrap();
        assert_eq!(v["text"], last, "the stale status line is not taken for the turn's end");

        // Never in the transcript: the hook's own copy is used.
        std::fs::write(&transcript, say("Older.")).unwrap();
        let hook = json!({ "session_id": "s-1", "transcript_path": transcript, "last_assistant_message": "Whole message." });
        write_turn(&hook.to_string(), &out).unwrap();
        let v: Value = serde_json::from_slice(&std::fs::read(&out).unwrap()).unwrap();
        assert_eq!(v["text"], "Whole message.");
    }

    #[test]
    fn a_failed_turn_reports_its_error_not_an_older_message() {
        let tmp = tempfile::tempdir().unwrap();
        let transcript = tmp.path().join("t.jsonl");
        let old = json!({ "type": "assistant", "message": { "content": [{ "type": "text", "text": "```forge\n{\"outcome\":\"pr\"}\n```" }] } });
        std::fs::write(&transcript, old.to_string() + "\n").unwrap();
        let out = tmp.path().join("turn.json");
        let hook = json!({ "hook_event_name": "StopFailure", "session_id": "s-1", "transcript_path": transcript, "error": "401 a live Forge session key is needed" });
        write_turn(&hook.to_string(), &out).unwrap();
        let v: Value = serde_json::from_slice(&std::fs::read(&out).unwrap()).unwrap();
        assert_eq!(v["text"], "The turn stopped on an error: 401 a live Forge session key is needed");
        assert!(!has_forge_block(v["text"].as_str().unwrap()));
    }

    /// A real tmux server and a stand-in `claude` that answers its first
    /// message at start and each pasted message after, as the Stop hook would.
    #[tokio::test]
    async fn bursts_run_in_a_tmux_session_that_stays_up() {
        if std::process::Command::new("tmux").arg("-V").output().is_err() {
            eprintln!("no tmux here; skipping");
            return;
        }
        // A short path: a unix socket's path is limited to about 100 bytes.
        let tmp = tempfile::Builder::new().prefix("ft").tempdir_in("/tmp").unwrap();
        let claude = tmp.path().join("claude");
        std::fs::write(
            &claude,
            r#"#!/bin/sh
for last; do :; done
answer() { printf '{"session_id":"s-%s","text":"%s"}' "$1" "$2" > "$FORGE_TURN_FILE.tmp" && mv "$FORGE_TURN_FILE.tmp" "$FORGE_TURN_FILE"; }
case "$last" in *FIRST*) answer 1 '```forge\n{\"outcome\":\"question\",\"text\":\"which colour?\"}\n```' ;; esac
while IFS= read -r line; do
  case "$line" in
    *SECOND*) answer 2 'no block yet' ;;
    *"forge block"*) answer 2 '```forge\n{\"outcome\":\"blocked\",\"reason\":\"stuck\"}\n```' ;;
  esac
done
"#,
        )
        .unwrap();
        // A stand-in `forge` that records what it was asked to trust.
        let forge = tmp.path().join("forge");
        std::fs::write(&forge, "#!/bin/sh\necho \"$@\" >> \"$(dirname \"$0\")/trusted\"\n").unwrap();
        use std::os::unix::fs::PermissionsExt as _;
        for bin in [&claude, &forge] {
            std::fs::set_permissions(bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let mut cfg = crate::config::sample();
        cfg.data_dir = tmp.path().join("data");
        cfg.claude.bin = claude;
        cfg.claude.forge_bin = forge;
        cfg.claude.session_user = None;
        std::fs::create_dir_all(cfg.sessions_dir()).unwrap();
        let keys = SessionKeys::new();
        let mut runner = TmuxRunner::new(&cfg, keys.clone());
        let worktree = tmp.path().to_path_buf();
        let req = |prompt: &str, resume: Option<&str>| BurstRequest {
            repo: "o/r".into(),
            issue: 7,
            role: "backend".into(),
            resume: resume.map(String::from),
            worktree: worktree.clone(),
            prompt: prompt.into(),
            model: "m".into(),
            effort: "high".into(),
            session_key: "unused".into(),
        };

        let first = runner.burst(req("FIRST burst", None)).await.unwrap();
        assert!(!first.is_error, "{first:?}");
        assert_eq!(first.session_id.as_deref(), Some("s-1"));
        assert_eq!(first.outcome, Outcome::Question { text: "which colour?".into() });
        let trusted = std::fs::read_to_string(tmp.path().join("trusted")).unwrap();
        assert_eq!(trusted.trim(), format!("trust {}", worktree.display()), "the folder is trusted before Claude starts");
        let key = runner.live.lock().unwrap().get("o-r-7").cloned().unwrap();
        assert!(keys.tag(&key).is_some(), "the session keeps a live key between bursts");

        // The same Claude gets the next prompt; a turn with no block is nudged once.
        let second = runner.burst(req("SECOND burst", Some("s-1"))).await.unwrap();
        assert_eq!(second.outcome, Outcome::Blocked { reason: "stuck".into() });
        assert_eq!(second.session_id.as_deref(), Some("s-2"));
        assert!(!cfg.sessions_dir().join("o-r-7.prompt.md").exists(), "a burst that ends removes its prompt");

        // A Claude that goes quiet, its screen standing still, is called stuck.
        runner.stall = Duration::from_secs(3);
        let quiet = runner.burst(req("QUIET burst", Some("s-2"))).await.unwrap();
        let Outcome::Blocked { reason } = &quiet.outcome else { panic!("{quiet:?}") };
        assert!(reason.starts_with("the session's screen hasn't changed for 0 minutes"), "{reason}");
        assert!(runner.running("o-r-7").await, "a stuck Claude is left up for a person to look at");

        // Forge restarted mid-turn: the prompt file is still there and Claude is
        // up, so the burst waits for that turn instead of sending its prompt.
        std::fs::write(cfg.sessions_dir().join("o-r-7.prompt.md"), "in flight").unwrap();
        let turn = cfg.sessions_dir().join("o-r-7.turn.json");
        let done = json!({ "session_id": "s-3", "text": "```forge\n{\"outcome\":\"blocked\",\"reason\":\"carried on\"}\n```" });
        std::fs::write(&turn, done.to_string()).unwrap();
        let third = runner.burst(req("THIRD burst", Some("s-2"))).await.unwrap();
        assert_eq!(third.outcome, Outcome::Blocked { reason: "carried on".into() });
        assert!(!cfg.sessions_dir().join("o-r-7.prompt.md").exists());

        runner.finish("o/r", 7).await;
        assert!(!runner.running("o-r-7").await);
        assert_eq!(keys.tag(&key), None, "finishing revokes the key");

        // A turn that ended while Forge was down is the answer, Claude gone or not.
        std::fs::write(cfg.sessions_dir().join("o-r-7.prompt.md"), "in flight").unwrap();
        std::fs::write(&turn, done.to_string()).unwrap();
        let fourth = runner.burst(req("FOURTH burst", Some("s-3"))).await.unwrap();
        assert_eq!(fourth.outcome, Outcome::Blocked { reason: "carried on".into() });
        assert!(!runner.running("o-r-7").await, "no Claude was started for it");
        let _ = tmux_command(None, &cfg.tmux_socket()).arg("kill-server").output().await;
    }
}
