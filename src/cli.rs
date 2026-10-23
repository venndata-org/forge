//! The `forge` commands people run on their own machines: login, status, try,
//! attach. They talk to the daemon's API with the token `forge login` saved
//! in `~/.config/forge/client.toml` (mode 600).

use std::ffi::OsString;
use std::io::Write as _;
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::attach::valid_repo;
use crate::types::{E2eRun, PlanView, StatusView, TaskLine, TaskView};

/// `~/.config/forge/client.toml`. FORGE_SERVER and FORGE_TOKEN override it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ClientConfig {
    pub server: Option<String>,
    pub token: Option<String>,
    /// The repo commands default to, "owner/repo".
    pub repo: Option<String>,
    /// Where `forge attach` SSHes, e.g. "forge-run@forge.example.com".
    pub ssh_target: Option<String>,
}

impl ClientConfig {
    /// Where the file lives on this machine.
    pub fn path() -> Result<PathBuf> {
        config_path(std::env::var_os("XDG_CONFIG_HOME"), std::env::var_os("HOME"))
    }

    /// The file (empty if missing) with FORGE_SERVER and FORGE_TOKEN over it.
    pub fn load() -> Result<ClientConfig> {
        let cfg = ClientConfig::read(&ClientConfig::path()?)?;
        Ok(cfg.with_env(env_var("FORGE_SERVER"), env_var("FORGE_TOKEN")))
    }

    /// Writes the file with mode 600.
    pub fn save(&self) -> Result<()> {
        self.save_to(&ClientConfig::path()?)
    }

    /// The file alone, without the environment; a missing file is an empty config.
    fn read(path: &Path) -> Result<ClientConfig> {
        match std::fs::read_to_string(path) {
            Ok(text) => toml::from_str(&text).with_context(|| format!("reading {}", path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(ClientConfig::default()),
            Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
        }
    }

    fn with_env(mut self, server: Option<String>, token: Option<String>) -> ClientConfig {
        self.server = server.or(self.server);
        self.token = token.or(self.token);
        self
    }

    fn save_to(&self, path: &Path) -> Result<()> {
        if let Some(dir) = path.parent() {
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(dir)
                .with_context(|| format!("creating {}", dir.display()))?;
        }
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
            .with_context(|| format!("writing {}", path.display()))?;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        file.write_all(toml::to_string(self)?.as_bytes())?;
        Ok(())
    }
}

/// `$XDG_CONFIG_HOME/forge/client.toml` when that is an absolute path, else
/// `$HOME/.config/forge/client.toml`.
fn config_path(xdg: Option<OsString>, home: Option<OsString>) -> Result<PathBuf> {
    let base = match xdg.map(PathBuf::from).filter(|p| p.is_absolute()) {
        Some(dir) => dir,
        None => PathBuf::from(home.filter(|h| !h.is_empty()).context("HOME isn't set")?).join(".config"),
    };
    Ok(base.join("forge").join("client.toml"))
}

fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

pub struct Client {
    pub server: String,
    pub token: Option<String>,
    http: reqwest::Client,
}

#[derive(Deserialize)]
struct ChallengeReply {
    challenge: String,
}

#[derive(Deserialize)]
struct TokenReply {
    token: String,
}

impl Client {
    pub fn new(server: &str, token: Option<String>) -> Client {
        let http = reqwest::Client::builder().timeout(Duration::from_secs(30)).build().unwrap_or_default();
        Client { server: server.trim_end_matches('/').to_string(), token, http }
    }

    pub fn from_config(cfg: &ClientConfig) -> Result<Client> {
        let server = cfg
            .server
            .as_deref()
            .filter(|s| !s.is_empty())
            .context("no Forge server set: run `forge login --server <url> --email <you>`")?;
        Ok(Client::new(server, cfg.token.clone()))
    }

    pub async fn begin_signin(&self, email: &str) -> Result<String> {
        let reply: ChallengeReply = self.post("/api/signin", json!({ "email": email })).await?;
        Ok(reply.challenge)
    }

    pub async fn finish_signin(&self, challenge: &str, code: &str) -> Result<String> {
        let reply: TokenReply = self.post("/api/signin/verify", json!({ "challenge": challenge, "code": code })).await?;
        Ok(reply.token)
    }

    /// Asks for a task: a new issue on the repo, Asked on its Project.
    pub async fn new_task(&self, repo: &str, title: &str, body: &str) -> Result<NewTask> {
        if !valid_repo(repo) {
            bail!("{repo:?} isn't owner/repo");
        }
        self.post(&format!("/api/tasks/{repo}"), json!({ "title": title, "body": body })).await
    }

    /// Moves a task to Ready or Accepted on its Project.
    pub async fn move_task(&self, repo: &str, issue: u64, status: &str) -> Result<Value> {
        if !valid_repo(repo) {
            bail!("{repo:?} isn't owner/repo");
        }
        self.post(&format!("/api/tasks/{repo}/{issue}/status"), json!({ "status": status })).await
    }

    pub async fn status(&self) -> Result<StatusView> {
        self.get("/api/status").await
    }

    /// The Forge skill as this server renders it, with its repos and people.
    pub async fn skill(&self) -> Result<String> {
        let v: Value = self.get("/api/skill").await?;
        v["skill"].as_str().map(String::from).context("the server sent no skill")
    }

    pub async fn task(&self, repo: &str, issue: u64) -> Result<TaskView> {
        if !valid_repo(repo) {
            bail!("{repo:?} isn't owner/repo");
        }
        self.get(&format!("/api/tasks/{repo}/{issue}")).await
    }

    pub(crate) async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        self.send(self.http.get(format!("{}{path}", self.server))).await
    }

    pub(crate) async fn post<T: DeserializeOwned>(&self, path: &str, body: Value) -> Result<T> {
        self.send(self.http.post(format!("{}{path}", self.server)).json(&body)).await
    }

    async fn send<T: DeserializeOwned>(&self, req: reqwest::RequestBuilder) -> Result<T> {
        let req = match &self.token {
            Some(token) => req.bearer_auth(token),
            None => req,
        };
        let resp = req.send().await.with_context(|| format!("can't reach {}", self.server))?;
        let status = resp.status();
        let body = resp.text().await.context("reading the server's answer")?;
        if !status.is_success() {
            bail!("{}", error_text(status, &body));
        }
        serde_json::from_str(&body).with_context(|| format!("unexpected answer from {}", self.server))
    }
}

/// A task Forge just opened.
#[derive(Debug, Serialize, Deserialize)]
pub struct NewTask {
    pub issue: u64,
    pub url: String,
}

/// The `{"error": ...}` text of a failed API answer, else its HTTP status.
pub(crate) fn error_text(status: reqwest::StatusCode, body: &str) -> String {
    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|v| v.get("error")?.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("the server answered {status}"))
}

/// Asks for a code by email, reads it from stdin, saves the token.
pub async fn cmd_login(server: Option<String>, email: String) -> Result<()> {
    let path = ClientConfig::path()?;
    let mut cfg = ClientConfig::read(&path)?;
    let server = server
        .or_else(|| env_var("FORGE_SERVER"))
        .or(cfg.server.clone())
        .context("which Forge? pass --server https://<forge host>")?;
    sign_in(&path, &mut cfg, &server, &email).await
}

/// `forge setup`, which the installer runs: signs in (asking for the email if
/// needed), adds Forge's MCP server to Claude Code, installs the Forge skill,
/// and checks that SSH reaches Forge for watch and attach. Safe to run again.
pub async fn cmd_setup(server: Option<String>, email: Option<String>) -> Result<()> {
    let path = ClientConfig::path()?;
    let mut cfg = ClientConfig::read(&path)?;
    let server = server
        .or_else(|| env_var("FORGE_SERVER"))
        .or(cfg.server.clone())
        .context("which Forge? pass --server https://<forge host>")?;
    let signed_in = cfg.server.as_deref() == Some(server.trim_end_matches('/'))
        && cfg.token.is_some()
        && Client::from_config(&cfg)?.status().await.is_ok();
    if signed_in {
        println!("✓ Signed in to {server}");
    } else {
        let email = match email {
            Some(email) => email,
            None => {
                eprint!("Your work email: ");
                std::io::stderr().flush()?;
                let mut line = String::new();
                std::io::stdin().read_line(&mut line)?;
                line.trim().to_string()
            }
        };
        sign_in(&path, &mut cfg, &server, &email).await?;
    }
    let exe = std::env::current_exe().context("finding this forge binary")?;
    match add_mcp(&exe) {
        Ok(true) => println!("✓ Added Forge's MCP tools to Claude Code"),
        Ok(false) => println!("✓ Claude Code already has Forge's MCP tools"),
        Err(e) => println!("· Claude Code's MCP: {e}. Add it yourself: claude mcp add --scope user forge -- {} mcp", exe.display()),
    }
    // The server's skill names its repos and people; the built-in one is generic.
    let skill = Client::from_config(&cfg)?.skill().await.unwrap_or_else(|_| SKILL.to_string());
    match install_skill(&skill, std::env::var_os("CLAUDE_CONFIG_DIR"), std::env::var_os("HOME")) {
        Ok(file) => println!("✓ Installed the Forge skill at {}", file.display()),
        Err(e) => println!("· The Forge skill: {e:#}"),
    }
    match default_ssh_target(&server) {
        Some(target) if ssh_reaches(&target) => println!("✓ SSH reaches Forge: forge watch and forge attach work"),
        Some(_) => println!(
            "· SSH can't reach Forge yet. Watch and attach use an SSH key on your GitHub account (github.com/settings/keys); \
             Forge picks new keys up within the hour. If yours isn't one ssh tries by default, add `Host {}` with its \
             IdentityFile to ~/.ssh/config.",
            server.trim_start_matches("https://").trim_end_matches('/')
        ),
        None => {}
    }
    println!("\nNext: `forge status`, or ask your Claude what Forge is doing.");
    Ok(())
}

/// The skill that teaches Claude Code Forge, before a server adds its own repos.
pub const SKILL: &str = include_str!("../skills/forge/SKILL.md");

/// Writes the skill to `<claude config>/skills/forge/SKILL.md`, replacing an older one.
fn install_skill(text: &str, claude_dir: Option<OsString>, home: Option<OsString>) -> Result<PathBuf> {
    let base = match claude_dir.map(PathBuf::from).filter(|p| p.is_absolute()) {
        Some(dir) => dir,
        None => PathBuf::from(home.filter(|h| !h.is_empty()).context("HOME isn't set")?).join(".claude"),
    };
    let file = base.join("skills/forge/SKILL.md");
    std::fs::create_dir_all(file.parent().context("no skill folder")?)?;
    std::fs::write(&file, text).with_context(|| format!("writing {}", file.display()))?;
    Ok(file)
}

/// Adds `forge mcp` to Claude Code for this user; false if it was there already.
fn add_mcp(exe: &Path) -> Result<bool> {
    let has = std::process::Command::new("claude").args(["mcp", "get", "forge"]).output();
    let has = has.map_err(|_| anyhow!("Claude Code (`claude`) isn't on your PATH"))?;
    if has.status.success() {
        return Ok(false);
    }
    let added = std::process::Command::new("claude")
        .args(["mcp", "add", "--scope", "user", "forge", "--"])
        .arg(exe)
        .arg("mcp")
        .output()?;
    if !added.status.success() {
        bail!("`claude mcp add` failed: {}", String::from_utf8_lossy(&added.stderr).trim());
    }
    Ok(true)
}

/// Whether SSH signs in to Forge's gate: it answers an unknown command with
/// exit code 2, while a refused key is ssh's own 255.
fn ssh_reaches(target: &str) -> bool {
    let status = std::process::Command::new("ssh")
        .args(["-o", "BatchMode=yes", "-o", "ConnectTimeout=8", "-o", "StrictHostKeyChecking=accept-new", "-T", target, "hello"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    status.is_ok_and(|s| s.code() == Some(2))
}

/// Emails a code, reads it from stdin, saves the token, and fills in what the
/// other commands need.
async fn sign_in(path: &Path, cfg: &mut ClientConfig, server: &str, email: &str) -> Result<()> {
    let client = Client::new(server, None);
    let challenge = client.begin_signin(email).await?;
    eprint!("Code from your email: ");
    std::io::stderr().flush()?;
    let mut code = String::new();
    std::io::stdin().read_line(&mut code)?;
    if code.trim().is_empty() {
        bail!("no code given");
    }
    let token = client.finish_signin(&challenge, code.trim()).await?;
    // Fill in what the other commands need, so nothing is left to set by hand.
    if cfg.repo.is_none() {
        let signed_in = Client::new(&client.server, Some(token.clone()));
        if let Ok(status) = signed_in.status().await {
            if let [only] = status.repos.as_slice() {
                cfg.repo = Some(only.repo.clone());
            }
        }
    }
    cfg.token = Some(token);
    cfg.server = Some(client.server);
    cfg.save_to(path)?;
    println!("✓ Signed in. Token saved in {}", path.display());
    Ok(())
}

pub async fn cmd_status(json: bool) -> Result<()> {
    let status = Client::from_config(&ClientConfig::load()?)?.status().await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&status)?);
    } else {
        print!("{}", render_status(&status));
    }
    Ok(())
}

/// Fetches the task's branch into a worktree next to the local clone at
/// `dir` (default: the current directory), then runs the repo's build, seed
/// and run commands there, unless `no_run`.
pub async fn cmd_try(repo: Option<String>, issue: u64, dir: Option<PathBuf>, no_run: bool) -> Result<()> {
    let cfg = ClientConfig::load()?;
    let repo = pick_repo(repo, &cfg)?;
    let view = Client::from_config(&cfg)?.task(&repo, issue).await?;
    if !no_run && view.try_commands.stop.is_some() {
        eprintln!("When you're done: forge stop {issue}");
    }
    match dir {
        Some(dir) => try_in(&dir, &repo, &view, no_run).await,
        None => {
            let cache = cache_root(std::env::var_os("XDG_CACHE_HOME"), std::env::var_os("HOME"))?;
            try_cached(&cache, &format!("git@github.com:{repo}.git"), &repo, &view, no_run).await
        }
    }
}

/// Runs the repo's stop command in the task's try folder: what `forge try` started stops.
pub async fn cmd_stop(repo: Option<String>, issue: u64, dir: Option<PathBuf>) -> Result<()> {
    let cfg = ClientConfig::load()?;
    let repo = pick_repo(repo, &cfg)?;
    let view = Client::from_config(&cfg)?.task(&repo, issue).await?;
    let worktree = match dir {
        Some(dir) => try_dir(&clone_root(&dir)?, &repo, issue)?,
        None => cached_try_dir(&cache_root(std::env::var_os("XDG_CACHE_HOME"), std::env::var_os("HOME"))?, &repo, issue),
    };
    stop_in(&worktree, &repo, &view).await
}

async fn stop_in(worktree: &Path, repo: &str, view: &TaskView) -> Result<()> {
    if !worktree.is_dir() {
        bail!("nothing to stop: #{} has no try folder at {}", view.issue, worktree.display());
    }
    Running::end(worktree).await;
    let cmd = view.try_commands.stop.as_deref().with_context(|| format!("{repo} has no stop command in Forge's config"))?;
    run_steps(&[Step::sh(worktree, cmd)]).await
}

/// A running `forge try`, recorded in `<try folder>.pid` while its commands run,
/// so `forge stop` can end it (and the dev servers it started) before stopping
/// the rest. Removed when the try ends.
struct Running(PathBuf);

impl Running {
    fn file(worktree: &Path) -> PathBuf {
        worktree.with_extension("pid")
    }

    fn record(worktree: &Path) -> Running {
        let file = Running::file(worktree);
        let _ = std::fs::write(&file, std::process::id().to_string());
        Running(file)
    }

    /// Interrupts the recorded try's whole process group, as Ctrl-C in its
    /// terminal would; terminates it if it's still there 15 seconds later.
    async fn end(worktree: &Path) {
        let file = Running::file(worktree);
        let Some(pid) = std::fs::read_to_string(&file).ok().and_then(|s| s.trim().parse::<u32>().ok()) else { return };
        let alive = |pid: u32| async move {
            let status = tokio::process::Command::new("kill").args(["-0", &pid.to_string()]).stderr(Stdio::null()).status().await;
            status.is_ok_and(|s| s.success())
        };
        let group = tokio::process::Command::new("ps").args(["-o", "pgid=", "-p", &pid.to_string()]).output().await;
        let group = group.ok().map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string()).filter(|g| !g.is_empty());
        if let Some(group) = group.filter(|_| pid != std::process::id()) {
            eprintln!("$ kill -INT -{group}   # the running forge try");
            for signal in ["-INT", "-TERM"] {
                let _ = tokio::process::Command::new("kill").args([signal, "--", &format!("-{group}")]).status().await;
                for _ in 0..30 {
                    if !alive(pid).await {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                if !alive(pid).await {
                    break;
                }
            }
        }
        let _ = std::fs::remove_file(&file);
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// `<cache>/try/<repo name>-<issue>`.
fn cached_try_dir(cache: &Path, repo: &str, issue: u64) -> PathBuf {
    let name = repo.rsplit('/').next().unwrap_or(repo);
    cache.join("try").join(format!("{name}-{issue}"))
}

/// `$XDG_CACHE_HOME/forge` when that is an absolute path, else `$HOME/.cache/forge`.
pub(crate) fn cache_root(xdg: Option<OsString>, home: Option<OsString>) -> Result<PathBuf> {
    let base = match xdg.map(PathBuf::from).filter(|p| p.is_absolute()) {
        Some(dir) => dir,
        None => PathBuf::from(home.filter(|h| !h.is_empty()).context("HOME isn't set")?).join(".cache"),
    };
    Ok(base.join("forge"))
}

/// `forge try` without --dir: Forge's own bare clone of the repo in the cache
/// (made on first use, with your GitHub access), and the task's branch checked
/// out at `<cache>/try/<repo name>-<issue>`.
async fn try_cached(cache: &Path, url: &str, repo: &str, view: &TaskView, no_run: bool) -> Result<()> {
    let mirror = cache.join("repos").join(format!("{repo}.git"));
    let worktree = cached_try_dir(cache, repo, view.issue);
    let parent = mirror.parent().context("no cache folder")?;
    std::fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    std::fs::create_dir_all(cache.join("try"))?;
    let mut steps = Vec::new();
    if !mirror.exists() {
        steps.push(Step::git(parent, &["clone", "--bare", "--quiet", url, &mirror.to_string_lossy()]));
    }
    steps.extend(try_steps(&mirror, &worktree, worktree.join(".git").exists(), view, no_run));
    let _running = (!no_run).then(|| Running::record(&worktree));
    run_steps(&steps).await?;
    if no_run {
        println!("{} is in {}", view.branch, worktree.display());
    }
    Ok(())
}

/// `forge new`: asks for a task. A body of "-" is read from stdin.
pub async fn cmd_new(repo: Option<String>, title: String, body: Option<String>) -> Result<()> {
    let cfg = ClientConfig::load()?;
    let repo = pick_repo(repo, &cfg)?;
    let body = match body.as_deref() {
        Some("-") => {
            let mut text = String::new();
            std::io::Read::read_to_string(&mut std::io::stdin(), &mut text)?;
            text
        }
        Some(text) => text.to_string(),
        None => String::new(),
    };
    let task = Client::from_config(&cfg)?.new_task(&repo, &title, &body).await?;
    println!("Asked: #{} {}", task.issue, task.url);
    Ok(())
}

/// `forge ready` and `forge accept`: moves a task on the Project.
pub async fn cmd_move(repo: Option<String>, issue: u64, status: &str) -> Result<()> {
    let cfg = ClientConfig::load()?;
    let repo = pick_repo(repo, &cfg)?;
    Client::from_config(&cfg)?.move_task(&repo, issue, status).await?;
    match status {
        "Accepted" => println!("#{issue} is Accepted. Forge rebases, checks and merges it within a minute or two."),
        "Backlog" => println!("#{issue} is in the Backlog. Forge leaves it until it's moved to Ready."),
        _ => println!("#{issue} is {status}. Forge starts it when a slot is free."),
    }
    Ok(())
}

/// `ssh -t <ssh_target> attach <owner/repo> <issue>`.
pub async fn cmd_attach(repo: Option<String>, issue: u64) -> Result<()> {
    run_ssh(gate_command(&ClientConfig::load()?, "attach", repo, issue)?).await
}

/// `ssh -t <ssh_target> watch <owner/repo> <issue>`: follow the session, read-only.
pub async fn cmd_watch(repo: Option<String>, issue: u64) -> Result<()> {
    run_ssh(gate_command(&ClientConfig::load()?, "watch", repo, issue)?).await
}

async fn run_ssh(argv: Vec<String>) -> Result<()> {
    let status = tokio::process::Command::new(&argv[0])
        .args(&argv[1..])
        .status()
        .await
        .context("running ssh")?;
    if !status.success() {
        bail!("ssh ended with {status}");
    }
    Ok(())
}

/// The repo named on the command line, else the client config's.
pub(crate) fn pick_repo(arg: Option<String>, cfg: &ClientConfig) -> Result<String> {
    let repo = arg
        .or_else(|| cfg.repo.clone())
        .context("which repo? pass --repo owner/repo, or set repo in client.toml")?;
    if !valid_repo(&repo) {
        bail!("{repo:?} isn't owner/repo");
    }
    Ok(repo)
}

/// The ssh command line `forge attach` or `forge watch` runs.
fn gate_command(cfg: &ClientConfig, verb: &str, repo: Option<String>, issue: u64) -> Result<Vec<String>> {
    let repo = pick_repo(repo, cfg)?;
    let target = cfg
        .ssh_target
        .clone()
        .filter(|t| !t.is_empty())
        .or_else(|| cfg.server.as_deref().and_then(default_ssh_target))
        .ok_or_else(|| anyhow!("which Forge? sign in first: forge login --server https://<forge host> --email <you>"))?;
    let target = target.as_str();
    if target.starts_with('-') {
        bail!("ssh_target {target:?} isn't a host");
    }
    Ok(["ssh", "-t", target, verb, &repo, &issue.to_string()].map(String::from).to_vec())
}

/// `forge-run@<host>` for the Forge at `server`: sessions run as forge-run there.
fn default_ssh_target(server: &str) -> Option<String> {
    let url = reqwest::Url::parse(server).ok()?;
    Some(format!("forge-run@{}", url.host_str()?))
}

/// One command `forge try` runs, and the directory it runs in.
#[derive(Debug, Clone, PartialEq)]
struct Step {
    dir: PathBuf,
    argv: Vec<String>,
}

impl Step {
    fn git(dir: &Path, args: &[&str]) -> Step {
        let argv = std::iter::once("git").chain(args.iter().copied()).map(String::from).collect();
        Step { dir: dir.to_path_buf(), argv }
    }

    fn sh(dir: &Path, command: &str) -> Step {
        Step { dir: dir.to_path_buf(), argv: ["sh", "-c", command].map(String::from).to_vec() }
    }

    /// How the step is printed before it runs.
    fn shown(&self) -> String {
        match self.argv.as_slice() {
            [sh, c, command] if sh == "sh" && c == "-c" => command.clone(),
            argv => argv.join(" "),
        }
    }
}

/// Puts the task's branch in the try worktree beside the clone that holds
/// `dir`, then runs the repo's commands there.
async fn try_in(dir: &Path, repo: &str, view: &TaskView, no_run: bool) -> Result<()> {
    let clone = clone_root(dir)?;
    let worktree = try_dir(&clone, repo, view.issue)?;
    let steps = try_steps(&clone, &worktree, worktree.join(".git").exists(), view, no_run);
    let _running = (!no_run).then(|| Running::record(&worktree));
    run_steps(&steps).await?;
    if no_run {
        println!("{} is in {}", view.branch, worktree.display());
    }
    Ok(())
}

/// The top of the git clone that holds `dir`.
fn clone_root(dir: &Path) -> Result<PathBuf> {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .context("running git")?;
    if !out.status.success() {
        bail!("{} isn't in a git clone: run this in your clone, or pass --dir", dir.display());
    }
    Ok(PathBuf::from(String::from_utf8_lossy(&out.stdout).trim()))
}

/// `<clone>/../<repo name>-try-<issue>`.
fn try_dir(clone: &Path, repo: &str, issue: u64) -> Result<PathBuf> {
    let name = repo.rsplit('/').next().unwrap_or(repo);
    let parent = clone.parent().context("the clone has no parent directory")?;
    Ok(parent.join(format!("{name}-try-{issue}")))
}

/// Fetch the branch and check it out in the try worktree (each worktree has
/// its own FETCH_HEAD, so an existing one fetches for itself), then build,
/// seed and run unless `no_run`.
fn try_steps(clone: &Path, worktree: &Path, exists: bool, view: &TaskView, no_run: bool) -> Vec<Step> {
    let mut steps = if exists {
        vec![
            Step::git(worktree, &["fetch", "origin", &view.branch]),
            Step::git(worktree, &["reset", "--hard", "FETCH_HEAD"]),
        ]
    } else {
        vec![
            Step::git(clone, &["fetch", "origin", &view.branch]),
            Step::git(clone, &["worktree", "add", "--force", &worktree.to_string_lossy(), "FETCH_HEAD"]),
        ]
    };
    if !no_run {
        let c = &view.try_commands;
        steps.extend([&c.build, &c.seed, &c.run].into_iter().flatten().map(|cmd| Step::sh(worktree, cmd)));
    }
    steps
}

/// Runs each step with inherited stdio, printing it first; stops at the first failure.
async fn run_steps(steps: &[Step]) -> Result<()> {
    let mut here = std::env::current_dir().ok();
    for step in steps {
        if here.as_deref() != Some(step.dir.as_path()) {
            eprintln!("$ cd {}", step.dir.display());
            here = Some(step.dir.clone());
        }
        eprintln!("$ {}", step.shown());
        let status = tokio::process::Command::new(&step.argv[0])
            .args(&step.argv[1..])
            .current_dir(&step.dir)
            .status()
            .await
            .with_context(|| format!("running {}", step.argv[0]))?;
        if !status.success() {
            bail!("`{}` failed ({status})", step.shown());
        }
    }
    Ok(())
}

/// The status as plain text for a terminal.
pub fn render_status(v: &StatusView) -> String {
    let mut lines = Vec::new();
    if v.waiting_on_you.is_empty() {
        lines.push("Nothing waits on you.".to_string());
    } else {
        lines.push("Waiting on you".to_string());
        for w in &v.waiting_on_you {
            lines.push(join(&[format!("  {}#{} {}", w.repo, w.issue, w.title), format!("[{}]", w.why), first_line(&w.detail)]));
            lines.push(format!("    {}", w.url));
        }
    }
    for r in &v.repos {
        lines.push(String::new());
        lines.push(if r.concurrency == 0 { format!("{} (paused)", r.repo) } else { r.repo.clone() });
        let stages = [
            ("Asked", &r.asked),
            ("Ready", &r.ready),
            ("Building", &r.building),
            ("Reviewed", &r.reviewed),
            ("Accepted", &r.accepted),
            ("On main", &r.on_main),
            ("Backlog", &r.backlog),
        ];
        for (name, tasks) in stages.into_iter().filter(|(_, tasks)| !tasks.is_empty()) {
            lines.push(format!("  {name}"));
            lines.extend(tasks.iter().map(|t| format!("    {}", task_line(t))));
        }
        if let Some(e2e) = &r.last_e2e {
            lines.push(format!("  Last e2e: {}", e2e_line(e2e)));
        }
    }
    if !v.pool.is_empty() {
        lines.push(String::new());
        lines.push("Plans".to_string());
        let width = v.pool.iter().map(|p| p.label.len()).max().unwrap_or(0);
        lines.extend(v.pool.iter().map(|p| format!("  {:<width$}  {}", p.label, plan_line(p))));
    }
    lines.join("\n") + "\n"
}

/// The non-empty parts, two spaces apart.
fn join(parts: &[String]) -> String {
    parts.iter().filter(|p| !p.is_empty()).cloned().collect::<Vec<_>>().join("  ")
}

fn first_line(text: &str) -> String {
    text.lines().next().unwrap_or("").trim().chars().take(100).collect()
}

fn when(t: DateTime<Utc>) -> String {
    t.format("%d %b %H:%M UTC").to_string()
}

fn task_line(t: &TaskLine) -> String {
    join(&[
        format!("#{} {}", t.issue, t.title),
        format!("[{}]", t.role),
        t.waiting_on.as_ref().map(|w| format!("waiting on {w}")).unwrap_or_default(),
        t.attached_by.as_ref().map(|a| format!("attached by {a}")).unwrap_or_default(),
    ])
}

fn e2e_line(e: &E2eRun) -> String {
    let ended = when(e.finished_at.unwrap_or(e.started_at));
    match e.passed {
        Some(true) => format!("passed {ended}"),
        Some(false) => match e.issue {
            Some(n) => format!("failed {ended}, see #{n}"),
            None => format!("failed {ended}"),
        },
        None => format!("running since {}", when(e.started_at)),
    }
}

fn plan_line(p: &PlanView) -> String {
    let pct = |v: Option<f64>| v.map(|v| format!("{v:.0}%")).unwrap_or_else(|| "?".into());
    join(&[
        match &p.limits {
            Some(l) => format!("5h {}  week {}", pct(l.five_hour_pct), pct(l.week_pct)),
            None => "no reading yet".into(),
        },
        if p.at_ceiling { "at ceiling".into() } else { String::new() },
        match (p.out_until, &p.out_reason) {
            (Some(t), Some(why)) => format!("out until {} ({why})", when(t)),
            (Some(t), None) => format!("out until {}", when(t)),
            (None, _) => String::new(),
        },
    ])
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::types::{Limits, RepoView, Status, TryCommands, WaitingItem};
    use axum::extract::Path as UrlPath;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use chrono::TimeZone as _;

    fn at(h: u32, m: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 20, h, m, 0).unwrap()
    }

    fn line(issue: u64, title: &str, status: Status, role: &str) -> TaskLine {
        TaskLine {
            issue,
            title: title.into(),
            status,
            role: role.into(),
            waiting_on: None,
            attached_by: None,
            pr_url: None,
            url: format!("https://github.com/example-team/shop/issues/{issue}"),
        }
    }

    pub(crate) fn sample_status() -> StatusView {
        let mut asking = line(412, "Add GST field", Status::Building, "backend");
        asking.waiting_on = Some("bob-gh".into());
        let mut attached = line(413, "Fix nav", Status::Building, "ui");
        attached.attached_by = Some("ann-gh".into());
        StatusView {
            generated_at: at(9, 0),
            repos: vec![RepoView {
                repo: "example-team/shop".into(),
                concurrency: 2,
                orderer: "bob-gh".into(),
                backlog: vec![],
                asked: vec![],
                ready: vec![],
                building: vec![asking, attached],
                reviewed: vec![line(410, "Invoice PDF", Status::Reviewed, "backend")],
                accepted: vec![],
                on_main: vec![line(400, "Login page", Status::OnMain, "ui")],
                last_e2e: Some(E2eRun {
                    repo: "example-team/shop".into(),
                    started_at: at(8, 0),
                    finished_at: Some(at(8, 12)),
                    passed: Some(false),
                    issue: Some(455),
                }),
            }],
            pool: vec![
                PlanView {
                    label: "blue".into(),
                    limits: Some(Limits {
                        five_hour_pct: Some(42.4),
                        five_hour_resets: None,
                        week_pct: Some(61.0),
                        week_resets: None,
                        read_at: at(9, 0),
                    }),
                    at_ceiling: false,
                    out_until: None,
                    out_reason: None,
                },
                PlanView {
                    label: "tan".into(),
                    limits: None,
                    at_ceiling: true,
                    out_until: Some(at(14, 0)),
                    out_reason: Some("429".into()),
                },
            ],
            waiting_on_you: vec![WaitingItem {
                repo: "example-team/shop".into(),
                issue: 412,
                title: "Add GST field".into(),
                why: "question".into(),
                detail: "Which table holds GST?\nMore context here.".into(),
                url: "https://github.com/example-team/shop/issues/412".into(),
            }],
            you: Some("ann-gh".into()),
        }
    }

    pub(crate) fn sample_task() -> TaskView {
        TaskView {
            repo: "example-team/shop".into(),
            issue: 412,
            title: "Add GST field".into(),
            status: Status::Reviewed,
            branch: "forge/412".into(),
            pr_url: None,
            attached_by: None,
            try_commands: TryCommands {
                build: Some("cargo build".into()),
                seed: None,
                run: Some("./scripts/dev.sh".into()),
                stop: None,
            },
        }
    }

    fn signed_in(headers: &HeaderMap) -> bool {
        headers.get("authorization").and_then(|v| v.to_str().ok()) == Some("Bearer good-token")
    }

    fn api_error(status: StatusCode, text: &str) -> (StatusCode, Json<Value>) {
        (status, Json(json!({ "error": text })))
    }

    /// A local stand-in for the daemon's API; the token "good-token" is signed in.
    pub(crate) async fn fake_api() -> String {
        let app = Router::new()
            .route("/api/signin", post(|| async { Json(json!({ "challenge": "ch-1" })) }))
            .route(
                "/api/signin/verify",
                post(|Json(body): Json<Value>| async move {
                    if body["challenge"] == "ch-1" && body["code"] == "123456" {
                        Ok(Json(json!({ "token": "good-token" })))
                    } else {
                        Err(api_error(StatusCode::UNAUTHORIZED, "wrong or expired code"))
                    }
                }),
            )
            .route(
                "/api/status",
                get(|headers: HeaderMap| async move {
                    if signed_in(&headers) {
                        Ok(Json(sample_status()))
                    } else {
                        Err(api_error(StatusCode::UNAUTHORIZED, "not signed in"))
                    }
                }),
            )
            .route(
                "/api/tasks/:owner/:repo/:n",
                get(|headers: HeaderMap, UrlPath((owner, repo, n)): UrlPath<(String, String, u64)>| async move {
                    if !signed_in(&headers) {
                        Err(api_error(StatusCode::UNAUTHORIZED, "not signed in"))
                    } else if format!("{owner}/{repo}") == "example-team/shop" && n == 412 {
                        Ok(Json(sample_task()))
                    } else {
                        Err(api_error(StatusCode::NOT_FOUND, "no such task"))
                    }
                }),
            )
            .route(
                "/api/tasks/:owner/:repo",
                post(|UrlPath((owner, repo)): UrlPath<(String, String)>, Json(body): Json<Value>| async move {
                    assert_eq!((owner.as_str(), repo.as_str(), body["title"].as_str()), ("example-team", "shop", Some("Fix the dates")));
                    Json(json!({ "issue": 77, "url": "https://github.com/example-team/shop/issues/77" }))
                }),
            )
            .route(
                "/api/tasks/:owner/:repo/:n/status",
                post(|UrlPath((_, _, n)): UrlPath<(String, String, u64)>, Json(body): Json<Value>| async move {
                    Json(json!({ "issue": n, "status": body["status"] }))
                }),
            )
            .route("/api/broken", get(|| async { (StatusCode::BAD_GATEWAY, "upstream down") }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    #[test]
    fn config_path_prefers_absolute_xdg() {
        let p = |xdg: Option<&str>, home: Option<&str>| config_path(xdg.map(Into::into), home.map(Into::into));
        assert_eq!(p(Some("/x"), Some("/h")).unwrap(), PathBuf::from("/x/forge/client.toml"));
        assert_eq!(p(None, Some("/h")).unwrap(), PathBuf::from("/h/.config/forge/client.toml"));
        assert_eq!(p(Some(""), Some("/h")).unwrap(), PathBuf::from("/h/.config/forge/client.toml"));
        assert_eq!(p(Some("rel"), Some("/h")).unwrap(), PathBuf::from("/h/.config/forge/client.toml"));
        assert!(p(None, None).is_err());
    }

    #[test]
    fn save_writes_private_file_and_reads_back() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("cfg/forge/client.toml");
        let cfg = ClientConfig {
            server: Some("https://forge.example.com".into()),
            token: Some("t".into()),
            repo: Some("example-team/shop".into()),
            ssh_target: None,
        };
        cfg.save_to(&path).unwrap();
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&path), 0o600);
        assert_eq!(mode(path.parent().unwrap()), 0o700);
        assert_eq!(ClientConfig::read(&path).unwrap(), cfg);

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        cfg.save_to(&path).unwrap();
        assert_eq!(mode(&path), 0o600);
        assert_eq!(ClientConfig::read(&tmp.path().join("missing.toml")).unwrap(), ClientConfig::default());
    }

    #[test]
    fn env_overrides_file() {
        let file = ClientConfig { server: Some("a".into()), token: Some("b".into()), ..Default::default() };
        let cfg = file.clone().with_env(Some("x".into()), None);
        assert_eq!((cfg.server.as_deref(), cfg.token.as_deref()), (Some("x"), Some("b")));
        let cfg = file.with_env(None, Some("y".into()));
        assert_eq!((cfg.server.as_deref(), cfg.token.as_deref()), (Some("a"), Some("y")));
        assert!(Client::from_config(&ClientConfig::default()).is_err());
        assert_eq!(Client::new("https://f.example.com/", None).server, "https://f.example.com");
    }

    #[test]
    fn status_reads_top_down() {
        let text = render_status(&sample_status());
        let expected = "\
Waiting on you
  example-team/shop#412 Add GST field  [question]  Which table holds GST?
    https://github.com/example-team/shop/issues/412

example-team/shop
  Building
    #412 Add GST field  [backend]  waiting on bob-gh
    #413 Fix nav  [ui]  attached by ann-gh
  Reviewed
    #410 Invoice PDF  [backend]
  On main
    #400 Login page  [ui]
  Last e2e: failed 20 Oct 08:12 UTC, see #455

Plans
  blue  5h 42%  week 61%
  tan   no reading yet  at ceiling  out until 20 Oct 14:00 UTC (429)
";
        assert_eq!(text, expected);

        let mut quiet = sample_status();
        quiet.waiting_on_you.clear();
        quiet.repos[0].concurrency = 0;
        let text = render_status(&quiet);
        assert!(text.starts_with("Nothing waits on you.\n\nexample-team/shop (paused)\n"));
    }

    #[test]
    fn try_steps_new_and_existing() {
        let clone = Path::new("/w/shop");
        let wt = try_dir(clone, "example-team/shop", 412).unwrap();
        assert_eq!(wt, PathBuf::from("/w/shop-try-412"));
        let view = sample_task();
        let shown = |steps: Vec<Step>| steps.iter().map(|s| (s.dir.clone(), s.shown())).collect::<Vec<_>>();

        let fresh = try_steps(clone, &wt, false, &view, false);
        assert_eq!(
            shown(fresh.clone()),
            vec![
                (clone.into(), "git fetch origin forge/412".into()),
                (clone.into(), "git worktree add --force /w/shop-try-412 FETCH_HEAD".into()),
                (wt.clone(), "cargo build".into()),
                (wt.clone(), "./scripts/dev.sh".into()),
            ]
        );
        assert_eq!(fresh[2].argv, vec!["sh", "-c", "cargo build"]);

        let again = try_steps(clone, &wt, true, &view, true);
        assert_eq!(
            shown(again),
            vec![(wt.clone(), "git fetch origin forge/412".into()), (wt, "git reset --hard FETCH_HEAD".into())]
        );
    }

    fn git(dir: &Path, args: &[&str]) -> String {
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(["-c", "user.name=t", "-c", "user.email=t@example.com"])
            .args(args)
            .output()
            .unwrap();
        assert!(out.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&out.stderr));
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    #[tokio::test]
    async fn try_checks_out_runs_and_stops_at_failure() {
        let tmp = tempfile::tempdir().unwrap();
        let origin = tmp.path().join("origin");
        std::fs::create_dir(&origin).unwrap();
        git(&origin, &["init", "-q", "-b", "main"]);
        git(&origin, &["commit", "-q", "--allow-empty", "-m", "one"]);
        git(&origin, &["checkout", "-q", "-b", "forge/412"]);
        std::fs::write(origin.join("one.txt"), "1").unwrap();
        git(&origin, &["add", "one.txt"]);
        git(&origin, &["commit", "-q", "-m", "change"]);
        git(tmp.path(), &["clone", "-q", "-b", "main", origin.to_str().unwrap(), "shop"]);
        let clone = tmp.path().join("shop");
        let wt = tmp.path().join("shop-try-412");

        let mut view = sample_task();
        view.try_commands = TryCommands {
            build: Some("echo built > built.txt".into()),
            seed: Some("false".into()),
            run: Some("touch ran.txt".into()),
            stop: Some("touch stopped.txt".into()),
        };
        let err = try_in(&clone, "example-team/shop", &view, false).await.unwrap_err();
        assert!(err.to_string().contains("`false` failed"), "{err}");
        assert!(wt.join("one.txt").exists() && wt.join("built.txt").exists());
        assert!(!wt.join("ran.txt").exists());

        std::fs::write(origin.join("two.txt"), "2").unwrap();
        git(&origin, &["add", "two.txt"]);
        git(&origin, &["commit", "-q", "-m", "more"]);
        try_in(&clone, "example-team/shop", &view, true).await.unwrap();
        assert_eq!(git(&wt, &["rev-parse", "HEAD"]), git(&origin, &["rev-parse", "forge/412"]));
        assert!(wt.join("two.txt").exists());

        let not_git = tempfile::tempdir().unwrap();
        assert!(try_in(not_git.path(), "example-team/shop", &view, true).await.is_err());

        // Without --dir: Forge's own clone in the cache, from anywhere.
        let cache = tmp.path().join("cache");
        let url = origin.to_str().unwrap();
        try_cached(&cache, url, "example-team/shop", &view, true).await.unwrap();
        let cached = cache.join("try/shop-412");
        assert_eq!(git(&cached, &["rev-parse", "HEAD"]), git(&origin, &["rev-parse", "forge/412"]));
        assert!(cache.join("repos/example-team/shop.git").is_dir());
        std::fs::write(origin.join("three.txt"), "3").unwrap();
        git(&origin, &["add", "three.txt"]);
        git(&origin, &["commit", "-q", "-m", "again"]);
        try_cached(&cache, url, "example-team/shop", &view, true).await.unwrap();
        assert!(cached.join("three.txt").exists());

        // forge stop runs the stop command there, and says so when there's nothing to stop.
        stop_in(&cached, "example-team/shop", &view).await.unwrap();
        assert!(cached.join("stopped.txt").exists());
        assert!(stop_in(&cache.join("try/shop-9"), "example-team/shop", &view).await.is_err());
        view.try_commands.stop = None;
        assert!(stop_in(&cached, "example-team/shop", &view).await.is_err());
    }

    #[tokio::test]
    async fn stop_ends_the_running_try_and_its_group() {
        let tmp = tempfile::tempdir().unwrap();
        let worktree = tmp.path().join("shop-412");
        std::fs::create_dir(&worktree).unwrap();
        // A stand-in for a running `forge try`: its own process group, with a child.
        let mut running = tokio::process::Command::new("sh")
            .args(["-c", "sleep 60 & wait"])
            .process_group(0)
            .spawn()
            .unwrap();
        std::fs::write(Running::file(&worktree), running.id().unwrap().to_string()).unwrap();
        Running::end(&worktree).await;
        let status = tokio::time::timeout(Duration::from_secs(5), running.wait()).await.unwrap().unwrap();
        assert!(!status.success());
        assert!(!Running::file(&worktree).exists());
        Running::end(&worktree).await; // nothing recorded: nothing to do
    }

    #[test]
    fn setup_installs_the_skill_where_claude_looks() {
        let tmp = tempfile::tempdir().unwrap();
        let file = install_skill(SKILL, None, Some(tmp.path().into())).unwrap();
        assert_eq!(file, tmp.path().join(".claude/skills/forge/SKILL.md"));
        assert!(std::fs::read_to_string(&file).unwrap().starts_with("---\nname: forge\n"));
        let own = tmp.path().join("cc");
        assert_eq!(install_skill(SKILL, Some(own.clone().into()), None).unwrap(), own.join("skills/forge/SKILL.md"));
    }

    #[test]
    fn cache_root_follows_xdg() {
        assert_eq!(cache_root(Some("/x".into()), Some("/h".into())).unwrap(), PathBuf::from("/x/forge"));
        assert_eq!(cache_root(Some("rel".into()), Some("/h".into())).unwrap(), PathBuf::from("/h/.cache/forge"));
        assert!(cache_root(None, None).is_err());
    }

    #[test]
    fn attach_command_needs_target_and_repo() {
        let mut cfg = ClientConfig { repo: Some("example-team/shop".into()), ..Default::default() };
        let err = gate_command(&cfg, "attach", None, 412).unwrap_err().to_string();
        assert!(err.contains("forge login"), "{err}");
        cfg.server = Some("https://forge.example.com".into());
        assert_eq!(gate_command(&cfg, "attach", None, 412).unwrap()[2], "forge-run@forge.example.com");
        cfg.ssh_target = Some("forge-run@forge.example.com".into());
        assert_eq!(
            gate_command(&cfg, "attach", None, 412).unwrap(),
            vec!["ssh", "-t", "forge-run@forge.example.com", "attach", "example-team/shop", "412"]
        );
        assert_eq!(gate_command(&cfg, "attach", Some("o/r".into()), 7).unwrap()[4], "o/r");
        assert!(gate_command(&cfg, "attach", Some("o/r; rm -rf ~".into()), 7).is_err());
        cfg.ssh_target = Some("-oProxyCommand=x".into());
        assert!(gate_command(&cfg, "attach", None, 412).is_err());
        assert!(gate_command(&ClientConfig::default(), "attach", None, 1).is_err());
        cfg.ssh_target = None;
        cfg.server = Some("https://forge.example.com".into());
        assert_eq!(gate_command(&cfg, "watch", None, 9).unwrap()[3..], ["watch", "example-team/shop", "9"]);
    }

    #[tokio::test]
    async fn client_calls_the_api() {
        let base = fake_api().await;
        let anon = Client::new(&base, None);
        assert_eq!(anon.begin_signin("a@example.com").await.unwrap(), "ch-1");
        assert_eq!(anon.finish_signin("ch-1", "123456").await.unwrap(), "good-token");
        let err = anon.finish_signin("ch-1", "000000").await.unwrap_err();
        assert_eq!(err.to_string(), "wrong or expired code");
        assert_eq!(anon.status().await.unwrap_err().to_string(), "not signed in");

        let client = Client::new(&base, Some("good-token".into()));
        assert_eq!(client.status().await.unwrap(), sample_status());
        assert_eq!(client.task("example-team/shop", 412).await.unwrap(), sample_task());
        assert_eq!(client.task("example-team/shop", 9).await.unwrap_err().to_string(), "no such task");
        let err: Result<Value> = client.get("/api/broken").await;
        assert_eq!(err.unwrap_err().to_string(), "the server answered 502 Bad Gateway");
    }
}
