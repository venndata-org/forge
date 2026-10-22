//! Git on the box. Forge keeps one mirror per repo that only Forge writes, and
//! makes each task its own clone for its session. Once a clone is made, Forge
//! never runs git in it, or in anything else a session can write: it takes a
//! session's commits by fetching from the clone with upload-pack running as
//! the session user, has a review read exactly what it pushed in a fresh clone,
//! and lands exactly that, rebased in a checkout only Forge can open and
//! checked as the session user in a fresh export.
//! Tokens go in a per-command header, never into a remote URL or git config.

use std::future::Future;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Output, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use base64::Engine as _;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command};

use crate::config::RepoConfig;
use crate::types::CmdOutput;

/// What landing a branch came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Landing {
    /// Rebasing onto origin/<default> hit conflicts; git's output.
    Conflict(String),
    /// The check failed on the rebased commit; its output.
    CheckFailed(String),
    /// The rebased commit, now on the remote branch; merge the PR with it.
    Ready { sha: String },
}

#[async_trait]
pub trait GitOps: Send + Sync {
    /// The task's own clone for its session, made on first use.
    async fn worktree(&self, repo: &RepoConfig, branch: &str, token: &str) -> Result<PathBuf>;
    /// Takes the clone's branch tip into Forge's mirror; returns how many commits origin/<default> lacks.
    async fn collect(&self, repo: &RepoConfig, branch: &str, token: &str) -> Result<u32>;
    /// Pushes what the last `collect` took and records it as pushed; returns
    /// its sha. The lease is what Forge last pushed, or, before its first push,
    /// that the branch doesn't exist, so a person's push is never overwritten.
    async fn push(&self, repo: &RepoConfig, branch: &str, token: &str) -> Result<String>;
    /// A fresh clone of exactly what was last pushed, for one review session.
    async fn review_checkout(&self, repo: &RepoConfig, branch: &str, token: &str) -> Result<PathBuf>;
    /// Removes the branch's review checkout, whatever the session left in it.
    async fn remove_review_checkout(&self, repo: &RepoConfig, branch: &str) -> Result<()>;
    /// Rebases what was last pushed onto fresh origin/<default> in Forge's
    /// landing checkout, never reading the clone, and runs `check` as the
    /// session user on an export of the result. When it passes, pushes the
    /// rebased commit under a lease against what was pushed, records it as
    /// pushed, and returns it as Ready.
    async fn land(
        &self,
        repo: &RepoConfig,
        branch: &str,
        check: Option<&str>,
        env: &[(String, String)],
        timeout: Duration,
        token: &str,
    ) -> Result<Landing>;
    /// Runs `shell` as the session user in a fresh export of origin/<default> (e2e).
    async fn run_on_main(
        &self,
        repo: &RepoConfig,
        shell: &str,
        env: &[(String, String)],
        timeout: Duration,
        token: &str,
    ) -> Result<CmdOutput>;
    /// Removes the task's clone, its landing checkout and Forge's refs for it.
    async fn remove_worktree(&self, repo: &RepoConfig, branch: &str) -> Result<()>;
}

/// Git through the `git` command line.
pub struct CliGit {
    /// Mirrors live in `<data_dir>/mirrors`, clones and exports in
    /// `<data_dir>/worktrees`, landing checkouts in `<data_dir>/state/landing`.
    pub data_dir: PathBuf,
    /// "https://github.com", or a file:// base in tests.
    pub remote_base: String,
    /// The name and email Forge's commits carry.
    pub author_name: String,
    pub author_email: String,
    /// The user sessions' git, checks and e2e run as, through sudo. None runs
    /// them as Forge's own user (tests, development).
    pub session_user: Option<String>,
}

/// How much of a command's output `run_as_session` keeps.
const OUTPUT_TAIL: usize = 64 * 1024;
/// How long a stopped command gets between SIGTERM and SIGKILL.
const STOP_GRACE: Duration = Duration::from_secs(5);
/// How long output may keep coming after a command exits.
const DRAIN_GRACE: Duration = Duration::from_secs(1);
/// How long Forge waits past a session-user command's own `timeout` before it
/// gives up on the command itself.
pub(crate) const BACKSTOP: Duration = Duration::from_secs(120);
/// What coreutils' `timeout` exits with when it stopped its command.
const TIMED_OUT: i32 = 124;
/// Seconds `timeout` gives a stopped command before SIGKILL.
const KILL_AFTER: u64 = 30;
/// How long Forge's own chores as the session user (upload-pack, cleanup) may take.
const CHORE_LIMIT: Duration = Duration::from_secs(600);
/// The mirror's remote-tracking refs: `origin/<branch>` everywhere.
const FETCH_REFSPEC: &str = "+refs/heads/*:refs/remotes/origin/*";
/// What a task's clone fetches from the mirror, so `origin/*` means the same in both.
const CLONE_REFSPEC: &str = "+refs/remotes/origin/*:refs/remotes/origin/*";
/// Every git Forge runs ignores hooks and fsmonitor.
const HARDENING: [&str; 4] = ["-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false"];
/// Forge's git runs with umask 027, so nothing it makes is group-writable.
const GIT_SCRIPT: &str = "umask 027 && exec git \"$@\"";

/// Writes to a mirror's refs take turns: two at once fight over its ref locks.
static MIRROR: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

impl CliGit {
    fn mirror_path(&self, repo: &RepoConfig) -> PathBuf {
        let (owner, name) = repo.owner_and_name();
        self.data_dir.join("mirrors").join(owner).join(format!("{name}.git"))
    }

    fn clone_path(&self, repo: &RepoConfig, branch: &str) -> PathBuf {
        let (owner, name) = repo.owner_and_name();
        self.data_dir.join("worktrees").join(owner).join(name).join(branch.replace('/', "-"))
    }

    fn review_path(&self, repo: &RepoConfig, branch: &str) -> PathBuf {
        self.clone_path(repo, &format!("{branch}-review"))
    }

    fn landing_path(&self, repo: &RepoConfig, branch: &str) -> PathBuf {
        let (owner, name) = repo.owner_and_name();
        self.data_dir.join("state/landing").join(owner).join(name).join(branch.replace('/', "-"))
    }

    fn remote_url(&self, repo: &RepoConfig) -> String {
        format!("{}/{}.git", self.remote_base.trim_end_matches('/'), repo.name)
    }

    /// The token as an auth header for one git command, passed in git's
    /// environment so it never shows in a process list. file:// needs none.
    fn auth_env(&self, token: &str) -> Vec<(String, String)> {
        let http = self.remote_base.starts_with("https://") || self.remote_base.starts_with("http://");
        if !http || token.is_empty() {
            return Vec::new();
        }
        let basic = base64::engine::general_purpose::STANDARD.encode(format!("x-access-token:{token}"));
        vec![
            ("GIT_CONFIG_COUNT".into(), "1".into()),
            ("GIT_CONFIG_KEY_0".into(), "http.extraHeader".into()),
            ("GIT_CONFIG_VALUE_0".into(), format!("Authorization: Basic {basic}")),
        ]
    }

    /// Makes the repo's mirror if it is missing, keeps its settings right, and
    /// fetches every branch into `origin/*`. Returns the mirror's path.
    async fn refresh(&self, repo: &RepoConfig, token: &str) -> Result<PathBuf> {
        let mirror = self.mirror_path(repo);
        let _turn = MIRROR.lock().await;
        if !mirror.join("HEAD").exists() {
            let parent = mirror.parent().context("mirror path has no parent")?;
            make_dirs(parent, 0o750)?;
            git(parent, &["init", "--bare", "-q", utf8(&mirror)?], &[]).await?;
        }
        let url = self.remote_url(repo);
        let settings = [
            ("remote.origin.url", url.as_str()),
            ("remote.origin.fetch", FETCH_REFSPEC),
            ("user.name", self.author_name.as_str()),
            ("user.email", self.author_email.as_str()),
        ];
        for (key, value) in settings {
            git(&mirror, &["config", key, value], &[]).await?;
        }
        git(&mirror, &["fetch", "-q", "--prune", "origin"], &self.auth_env(token)).await?;
        Ok(mirror)
    }

    /// Makes a clone on `branch` at the mirror's ref `start` in a staging
    /// folder no session can open, then opens it to the session user's group
    /// and moves it into place, so a session never sees a half-made clone or
    /// touches one Forge is working in.
    async fn make_clone(&self, mirror: &Path, path: &Path, branch: &str, start: &str) -> Result<()> {
        let parent = path.parent().context("clone path has no parent")?;
        let name = path.file_name().and_then(|n| n.to_str()).context("clone path has no name")?;
        make_dirs(parent, 0o750)?;
        let staging = parent.join(format!(".{name}.new"));
        if staging.exists() {
            std::fs::remove_dir_all(&staging).with_context(|| format!("removing {}", staging.display()))?;
        }
        std::fs::DirBuilder::new().mode(0o700).create(&staging)?;
        git(parent, &["clone", "-q", "--no-hardlinks", "--no-checkout", utf8(mirror)?, utf8(&staging)?], &[]).await?;
        git(&staging, &["config", "remote.origin.fetch", CLONE_REFSPEC], &[]).await?;
        git(&staging, &["fetch", "-q", "origin"], &[]).await?;
        git(&staging, &["fetch", "-q", "origin", start], &[]).await?;
        git(&staging, &["checkout", "-q", "--no-track", "-B", branch, "FETCH_HEAD"], &[]).await?;
        git(&staging, &["config", "user.name", &self.author_name], &[]).await?;
        git(&staging, &["config", "user.email", &self.author_email], &[]).await?;
        open_to_group(&staging).await?;
        std::fs::rename(&staging, path).with_context(|| format!("moving the clone to {}", path.display()))
    }

    /// Takes the clone's branch tip into `refs/forge/incoming/<branch>`; returns its sha.
    async fn take(&self, repo: &RepoConfig, branch: &str) -> Result<String> {
        let mirror = self.mirror_path(repo);
        let clone = self.clone_path(repo, branch);
        let incoming = incoming_ref(branch);
        let upload_pack = format!("--upload-pack={}", self.upload_pack());
        let refspec = format!("+refs/heads/{branch}:{incoming}");
        let hardened = ["-c", "protocol.file.allow=always", "-c", "fetch.fsckObjects=true"];
        let fetch = ["fetch", "-q", "--no-tags", &upload_pack, utf8(&clone)?, &refspec];
        let _turn = MIRROR.lock().await;
        git(&mirror, &[&hardened[..], &fetch[..]].concat(), &[]).await?;
        rev_parse(&mirror, &incoming).await?.context("the fetch from the clone left no incoming ref")
    }

    /// The landing checkout for the branch, detached at `sha` with no rebase left over.
    async fn landing_checkout(&self, mirror: &Path, path: &Path, sha: &str) -> Result<()> {
        if path.join(".git").exists() {
            let _ = git_output(path, &["rebase", "--abort"], &[]).await;
            git(path, &["checkout", "-q", "-f", "--detach", sha], &[]).await?;
            return Ok(());
        }
        if path.exists() {
            std::fs::remove_dir_all(path).with_context(|| format!("removing {}", path.display()))?;
        }
        make_dirs(path.parent().context("landing path has no parent")?, 0o700)?;
        git(mirror, &["worktree", "prune"], &[]).await?;
        git(mirror, &["worktree", "add", "-q", "-f", "--detach", utf8(path)?, sha], &[]).await?;
        Ok(())
    }

    /// Runs `shell` as the session user in a fresh export of `sha`, then removes the export.
    async fn run_export(
        &self,
        mirror: &Path,
        sha: &str,
        shell: &str,
        env: &[(String, String)],
        timeout: Duration,
    ) -> Result<CmdOutput> {
        let dir = self.export(mirror, sha).await?;
        let out = self.run_as_session(&dir, shell, env, timeout).await;
        if let Err(e) = self.remove_tree(&dir).await {
            tracing::warn!("leaving {}: {e:#}", dir.display());
        }
        out
    }

    /// `git archive <sha> | tar -x` into a new folder under `worktrees/.runs`,
    /// filled while only Forge can open it, then opened to the session user's group.
    async fn export(&self, mirror: &Path, sha: &str) -> Result<PathBuf> {
        let runs = self.data_dir.join("worktrees/.runs");
        make_dirs(&runs, 0o750)?;
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or_default();
        let dir = runs.join(format!("{}-{nanos}", &sha[..sha.len().min(12)]));
        std::fs::DirBuilder::new().mode(0o700).create(&dir).with_context(|| format!("creating {}", dir.display()))?;
        let mut archive = git_command(mirror, &["archive", "--format=tar", sha], &[])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("running git archive")?;
        let tar_in: Stdio = archive.stdout.take().context("git archive has no stdout")?.try_into()?;
        let tar = Command::new("tar").args(["-xf", "-", "-C"]).arg(&dir).stdin(tar_in).output();
        let (archived, untarred) = tokio::join!(archive.wait_with_output(), tar);
        let (archived, untarred) = (archived?, untarred.context("running tar")?);
        if !archived.status.success() || !untarred.status.success() {
            bail!("exporting {sha}: {}{}", combined(&archived).trim(), combined(&untarred).trim());
        }
        open_to_group(&dir).await?;
        Ok(dir)
    }

    /// Runs a shell command in `dir` as the session user with a timeout;
    /// output is the last 64 KB of stdout and stderr together.
    async fn run_as_session(&self, dir: &Path, shell: &str, env: &[(String, String)], timeout: Duration) -> Result<CmdOutput> {
        let names: Vec<&str> = env.iter().map(|(k, _)| k.as_str()).collect();
        let (program, args) = self.session_command(shell, &names, timeout);
        let wrapped = self.session_user.is_some();
        let (writer, mut reader) = tokio::net::unix::pipe::pipe()?;
        let writer = writer.into_blocking_fd()?;
        let mut cmd = Command::new(&program);
        cmd.args(&args)
            .current_dir(dir)
            .envs(env.iter().map(|(k, v)| (k, v)))
            .stdin(Stdio::null())
            .stdout(Stdio::from(writer.try_clone()?))
            .stderr(Stdio::from(writer))
            .process_group(0)
            .kill_on_drop(true);
        let mut child = cmd.spawn().with_context(|| format!("running {shell:?} in {}", dir.display()))?;
        drop(cmd);
        let mut out = Vec::new();
        let limit = if wrapped { timeout + BACKSTOP } else { timeout };
        let status = supervise(&mut child, read_tail(&mut reader, &mut out, OUTPUT_TAIL), limit).await?;
        let status = unless_timed_out(status, wrapped);
        let mut output = String::from_utf8_lossy(&out).into_owned();
        if status.is_none() {
            output.push_str(&format!("\n[forge: stopped after {}s]\n", timeout.as_secs_f32()));
        }
        Ok(CmdOutput { ok: status.is_some_and(|s| s.success()), code: status.and_then(|s| s.code()), output })
    }

    /// The program and arguments that run `shell` as the session user: through
    /// sudo and coreutils' `timeout`, since Forge can't signal another user's
    /// processes. With no session user, just `sh -c`.
    fn session_command(&self, shell: &str, env_names: &[&str], timeout: Duration) -> (String, Vec<String>) {
        let Some(user) = &self.session_user else {
            return ("sh".into(), vec!["-c".into(), shell.into()]);
        };
        let mut args = sudo_args(user, env_names, KILL_AFTER, timeout);
        args.extend(["sh".into(), "-c".into(), shell.into()]);
        ("sudo".into(), args)
    }

    /// The upload-pack command for the clone's side of a fetch from it: as the
    /// session user when there is one, so the clone is only read with the
    /// session's own rights, and never with its hooks.
    fn upload_pack(&self) -> String {
        let git = format!("git {} upload-pack", HARDENING.join(" "));
        match &self.session_user {
            None => git,
            Some(user) => format!("sudo {} {git}", sudo_args(user, &[], KILL_AFTER, CHORE_LIMIT).join(" ")),
        }
    }

    /// Removes a tree a session may have written in. What Forge can't remove
    /// itself, the session user removes first.
    async fn remove_tree(&self, dir: &Path) -> Result<()> {
        if !dir.exists() || std::fs::remove_dir_all(dir).is_ok() {
            return Ok(());
        }
        if self.session_user.is_some() {
            let parent = dir.parent().context("no parent")?;
            let env = [("FORGE_RM".to_string(), utf8(dir)?.to_string())];
            self.run_as_session(parent, "rm -rf -- \"$FORGE_RM\"", &env, CHORE_LIMIT).await?;
        }
        std::fs::remove_dir_all(dir).with_context(|| format!("removing {}", dir.display()))
    }
}

#[async_trait]
impl GitOps for CliGit {
    async fn worktree(&self, repo: &RepoConfig, branch: &str, token: &str) -> Result<PathBuf> {
        let path = self.clone_path(repo, branch);
        if !path.exists() {
            let mirror = self.refresh(repo, token).await?;
            let remote = format!("refs/remotes/origin/{branch}");
            let start = match rev_parse(&mirror, &remote).await? {
                Some(_) => remote,
                None => format!("refs/remotes/origin/{}", repo.default_branch),
            };
            self.make_clone(&mirror, &path, branch, &start).await?;
        }
        Ok(path)
    }

    async fn collect(&self, repo: &RepoConfig, branch: &str, _token: &str) -> Result<u32> {
        let sha = self.take(repo, branch).await?;
        let range = format!("refs/remotes/origin/{}..{sha}", repo.default_branch);
        let count = git(&self.mirror_path(repo), &["rev-list", "--count", &range], &[]).await?;
        count.trim().parse().context("reading git rev-list --count")
    }

    async fn push(&self, repo: &RepoConfig, branch: &str, token: &str) -> Result<String> {
        let mirror = self.mirror_path(repo);
        let _turn = MIRROR.lock().await;
        let sha = rev_parse(&mirror, &incoming_ref(branch)).await?.with_context(|| format!("nothing collected for {branch}"))?;
        let expected = rev_parse(&mirror, &pushed_ref(branch)).await?.unwrap_or_default();
        let lease = format!("--force-with-lease=refs/heads/{branch}:{expected}");
        let refspec = format!("{sha}:refs/heads/{branch}");
        git(&mirror, &["push", "-q", &lease, "origin", &refspec], &self.auth_env(token)).await?;
        git(&mirror, &["update-ref", &pushed_ref(branch), &sha], &[]).await?;
        Ok(sha)
    }

    async fn review_checkout(&self, repo: &RepoConfig, branch: &str, _token: &str) -> Result<PathBuf> {
        let path = self.review_path(repo, branch);
        self.remove_tree(&path).await?;
        self.make_clone(&self.mirror_path(repo), &path, branch, &pushed_ref(branch)).await?;
        Ok(path)
    }

    async fn remove_review_checkout(&self, repo: &RepoConfig, branch: &str) -> Result<()> {
        self.remove_tree(&self.review_path(repo, branch)).await
    }

    async fn land(
        &self,
        repo: &RepoConfig,
        branch: &str,
        check: Option<&str>,
        env: &[(String, String)],
        timeout: Duration,
        token: &str,
    ) -> Result<Landing> {
        let mirror = self.refresh(repo, token).await?;
        let pushed = rev_parse(&mirror, &pushed_ref(branch)).await?.with_context(|| format!("nothing pushed for {branch}"))?;
        let landing = self.landing_path(repo, branch);
        self.landing_checkout(&mirror, &landing, &pushed).await?;
        let onto = format!("refs/remotes/origin/{}", repo.default_branch);
        let out = git_output(&landing, &["rebase", &onto], &[]).await?;
        if !out.status.success() {
            let _ = git_output(&landing, &["rebase", "--abort"], &[]).await;
            return Ok(Landing::Conflict(combined(&out)));
        }
        let sha = rev_parse(&landing, "HEAD").await?.context("the landing checkout has no HEAD")?;
        if let Some(check) = check {
            let out = self.run_export(&mirror, &sha, check, env, timeout).await?;
            if !out.ok {
                return Ok(Landing::CheckFailed(out.output));
            }
        }
        let lease = format!("--force-with-lease=refs/heads/{branch}:{pushed}");
        let refspec = format!("{sha}:refs/heads/{branch}");
        let _turn = MIRROR.lock().await;
        git(&mirror, &["push", "-q", &lease, "origin", &refspec], &self.auth_env(token)).await?;
        git(&mirror, &["update-ref", &pushed_ref(branch), &sha, &pushed], &[]).await?;
        Ok(Landing::Ready { sha })
    }

    async fn run_on_main(
        &self,
        repo: &RepoConfig,
        shell: &str,
        env: &[(String, String)],
        timeout: Duration,
        token: &str,
    ) -> Result<CmdOutput> {
        let mirror = self.refresh(repo, token).await?;
        let main = format!("refs/remotes/origin/{}", repo.default_branch);
        let sha = rev_parse(&mirror, &main).await?.with_context(|| format!("{} has no {main}", repo.name))?;
        self.run_export(&mirror, &sha, shell, env, timeout).await
    }

    async fn remove_worktree(&self, repo: &RepoConfig, branch: &str) -> Result<()> {
        self.remove_tree(&self.clone_path(repo, branch)).await?;
        let landing = self.landing_path(repo, branch);
        if landing.exists() {
            std::fs::remove_dir_all(&landing).with_context(|| format!("removing {}", landing.display()))?;
        }
        let mirror = self.mirror_path(repo);
        if !mirror.exists() {
            return Ok(());
        }
        git(&mirror, &["worktree", "prune"], &[]).await?;
        let _turn = MIRROR.lock().await;
        for name in [incoming_ref(branch), pushed_ref(branch)] {
            if rev_parse(&mirror, &name).await?.is_some() {
                git(&mirror, &["update-ref", "-d", &name], &[]).await?;
            }
        }
        Ok(())
    }
}

/// The clone's tip when Forge last took it.
fn incoming_ref(branch: &str) -> String {
    format!("refs/forge/incoming/{branch}")
}

/// What Forge last pushed for a branch: what was reviewed, or Forge's rebase of it.
fn pushed_ref(branch: &str) -> String {
    format!("refs/forge/pushed/{branch}")
}

/// `sudo` arguments that run a command as `user`, keeping the named
/// variables, under `timeout -k <kill_after> <limit>`; the command follows.
pub(crate) fn sudo_args(user: &str, env_names: &[&str], kill_after: u64, limit: Duration) -> Vec<String> {
    let mut args: Vec<String> = vec!["-n".into(), "-H".into(), "-u".into(), user.into()];
    if !env_names.is_empty() {
        args.push(format!("--preserve-env={}", env_names.join(",")));
    }
    let secs = limit.as_secs().max(1).to_string();
    args.extend(["--".into(), "timeout".into(), "-k".into(), kill_after.to_string(), secs]);
    args
}

/// A finished command's status, or None if it timed out: Forge's own limit,
/// or (when `wrapped` in `timeout`) the wrapper's.
pub(crate) fn unless_timed_out(status: Option<ExitStatus>, wrapped: bool) -> Option<ExitStatus> {
    status.filter(|s| !(wrapped && s.code() == Some(TIMED_OUT)))
}

/// Makes `dir` and its missing parents with `mode`, less the umask. Inside a
/// setgid folder they keep its group.
fn make_dirs(dir: &Path, mode: u32) -> Result<()> {
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(mode)
        .create(dir)
        .with_context(|| format!("creating {}", dir.display()))
}

/// Lets the folder's group read and write everything in it.
async fn open_to_group(dir: &Path) -> Result<()> {
    let out = Command::new("chmod").args(["-R", "g+rwX"]).arg(dir).output().await.context("running chmod")?;
    if !out.status.success() {
        bail!("opening {} to its group: {}", dir.display(), combined(&out).trim());
    }
    Ok(())
}

/// Runs git in `dir`; returns its stdout, or an error carrying its output.
async fn git(dir: &Path, args: &[&str], env: &[(String, String)]) -> Result<String> {
    let out = git_output(dir, args, env).await?;
    if !out.status.success() {
        bail!("git {} failed in {}: {}", args.join(" "), dir.display(), combined(&out).trim());
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

async fn git_output(dir: &Path, args: &[&str], env: &[(String, String)]) -> Result<Output> {
    git_command(dir, args, env).output().await.context("running git")
}

fn git_command(dir: &Path, args: &[&str], env: &[(String, String)]) -> Command {
    let mut cmd = Command::new("sh");
    cmd.args(["-c", GIT_SCRIPT, "git", "-C"])
        .arg(dir)
        .args(HARDENING)
        .args(args)
        .envs(env.iter().map(|(k, v)| (k, v)))
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        .kill_on_drop(true);
    cmd
}

/// The sha `name` points at, if it exists.
async fn rev_parse(dir: &Path, name: &str) -> Result<Option<String>> {
    let out = git_output(dir, &["rev-parse", "--verify", "--quiet", name], &[]).await?;
    Ok(out.status.success().then(|| String::from_utf8_lossy(&out.stdout).trim().to_string()))
}

fn combined(out: &Output) -> String {
    format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr))
}

fn utf8(path: &Path) -> Result<&str> {
    path.to_str().with_context(|| format!("{} isn't UTF-8", path.display()))
}

/// Reads `from` to its end, keeping the last `max` bytes in `into`.
pub(crate) async fn read_tail(mut from: impl AsyncRead + Unpin, into: &mut Vec<u8>, max: usize) {
    let mut chunk = vec![0u8; 16 * 1024];
    while let Ok(n @ 1..) = from.read(&mut chunk).await {
        into.extend_from_slice(&chunk[..n]);
        if into.len() > 2 * max {
            into.drain(..into.len() - max);
        }
    }
    if into.len() > max {
        into.drain(..into.len() - max);
    }
}

/// Waits for `child`, which leads its own process group, while `reading`
/// drains its output. Past `limit` the group is stopped and the result is
/// None. Anything it left running that still holds its output is stopped
/// too, so a stray background process can't hold Forge up.
pub(crate) async fn supervise(
    child: &mut Child,
    reading: impl Future<Output = ()>,
    limit: Duration,
) -> Result<Option<ExitStatus>> {
    let pid = child.id().context("the command has no pid")?;
    tokio::pin!(reading);
    let mut drained = false;
    let waited = tokio::time::timeout(limit, async {
        loop {
            tokio::select! {
                status = child.wait() => break status,
                () = &mut reading, if !drained => drained = true,
            }
        }
    })
    .await;
    let status = match waited {
        Ok(status) => Some(status?),
        Err(_) => {
            stop_group(child, pid).await;
            None
        }
    };
    if !drained && tokio::time::timeout(DRAIN_GRACE, &mut reading).await.is_err() {
        stop_group(child, pid).await;
        let _ = tokio::time::timeout(DRAIN_GRACE, &mut reading).await;
    }
    Ok(status)
}

/// Sends the group SIGTERM, gives its leader a moment, then SIGKILL.
async fn stop_group(child: &mut Child, pid: u32) {
    signal_group(pid, "TERM").await;
    let _ = tokio::time::timeout(STOP_GRACE, child.wait()).await;
    signal_group(pid, "KILL").await;
    let _ = tokio::time::timeout(STOP_GRACE, child.wait()).await;
}

async fn signal_group(pid: u32, signal: &str) {
    let _ = Command::new("kill")
        .args(["-s", signal, "--", &format!("-{pid}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::process::ExitStatusExt;
    use std::time::Instant;

    use tokio::io::AsyncWriteExt;

    const TOKEN: &str = "test-token-not-real";
    const BRANCH: &str = "forge/7";

    /// A file:// remote with `main` holding one commit, a clone to change it
    /// from as a person would, and Forge's data dir.
    struct Fixture {
        dir: tempfile::TempDir,
        git: CliGit,
        repo: RepoConfig,
    }

    impl Fixture {
        fn new() -> Fixture {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().canonicalize().unwrap();
            std::fs::create_dir_all(root.join("remote/acme")).unwrap();
            sh(&root, "git init -q --bare -b main remote/acme/app.git");
            sh(&root, "git clone -q remote/acme/app.git upstream 2>/dev/null");
            sh(&root.join("upstream"), "echo one > a.txt && git add . && git commit -qm one && git push -q origin HEAD:main");
            let git = CliGit {
                data_dir: root.join("data"),
                remote_base: format!("file://{}", root.join("remote").display()),
                author_name: "Forge".into(),
                author_email: "forge@example.com".into(),
                session_user: None,
            };
            let mut repo = crate::config::sample().repos[0].clone();
            repo.name = "acme/app".into();
            Fixture { dir, git, repo }
        }

        fn root(&self) -> PathBuf {
            self.dir.path().canonicalize().unwrap()
        }

        fn mirror(&self) -> PathBuf {
            self.root().join("data/mirrors/acme/app.git")
        }

        fn landing(&self) -> PathBuf {
            self.root().join("data/state/landing/acme/app/forge-7")
        }

        /// Commits a change on the remote's main (or `branch`), as a person would.
        fn upstream_commit(&self, branch: &str, file: &str, text: &str) {
            let up = self.root().join("upstream");
            sh(&up, &format!("git fetch -q origin && git checkout -q -B {branch} origin/{branch} 2>/dev/null || git checkout -q -B {branch} origin/main"));
            sh(&up, &format!("echo {text} > {file} && git add . && git commit -qm {text} && git push -q -f origin HEAD:{branch}"));
        }

        fn remote_sha(&self, branch: &str) -> String {
            sh(&self.root().join("remote/acme/app.git"), &format!("git rev-parse {branch}")).trim().to_string()
        }

        fn mirror_sha(&self, name: &str) -> String {
            sh(&self.mirror(), &format!("git rev-parse {name}")).trim().to_string()
        }

        async fn try_land(&self, check: Option<&str>, env: &[(String, String)]) -> Result<Landing> {
            self.git.land(&self.repo, BRANCH, check, env, Duration::from_secs(20), TOKEN).await
        }

        async fn land(&self, check: Option<&str>, env: &[(String, String)]) -> Landing {
            self.try_land(check, env).await.unwrap()
        }

        /// Collects and pushes the session's work, as a PR outcome does; returns the sha pushed.
        async fn collect_and_push(&self) -> String {
            self.git.collect(&self.repo, BRANCH, TOKEN).await.unwrap();
            self.git.push(&self.repo, BRANCH, TOKEN).await.unwrap()
        }
    }

    /// Runs a setup script with a fixed identity and no user or system git config.
    fn sh(dir: &Path, script: &str) -> String {
        let out = std::process::Command::new("sh")
            .arg("-c")
            .arg(script)
            .current_dir(dir)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_AUTHOR_NAME", "Person")
            .env("GIT_AUTHOR_EMAIL", "person@example.com")
            .env("GIT_COMMITTER_NAME", "Person")
            .env("GIT_COMMITTER_EMAIL", "person@example.com")
            .output()
            .unwrap();
        assert!(out.status.success(), "{script}: {}", String::from_utf8_lossy(&out.stderr));
        String::from_utf8(out.stdout).unwrap()
    }

    /// Commits in a clone the way a session does: with the clone's own config.
    fn session_commit(wt: &Path, file: &str, text: &str) -> String {
        let out = std::process::Command::new("sh")
            .arg("-c")
            .arg(format!("echo {text} > {file} && git add . && git commit -qm {text} && git rev-parse HEAD"))
            .current_dir(wt)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .unwrap();
        assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
        String::from_utf8(out.stdout).unwrap().trim().to_string()
    }

    /// Every path under `dir` whose mode lets its group write.
    fn group_writable(dir: &Path) -> Vec<PathBuf> {
        let mut found = Vec::new();
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            let meta = std::fs::symlink_metadata(&path).unwrap();
            if meta.permissions().mode() & 0o020 != 0 {
                found.push(path.clone());
            }
            if meta.is_dir() {
                found.extend(group_writable(&path));
            }
        }
        found
    }

    #[tokio::test]
    async fn clone_starts_from_main_as_its_own_repo() {
        let f = Fixture::new();
        let wt = f.git.worktree(&f.repo, BRANCH, TOKEN).await.unwrap();
        assert_eq!(wt, f.root().join("data/worktrees/acme/app/forge-7"));
        assert_eq!(sh(&wt, "cat a.txt"), "one\n");
        assert_eq!(sh(&wt, "git rev-parse --abbrev-ref HEAD"), "forge/7\n");
        assert!(wt.join(".git").is_dir(), "the clone should be its own repo, not a worktree of the mirror");
        assert_eq!(sh(&wt, "git config remote.origin.url").trim(), f.mirror().display().to_string());
        assert_eq!(sh(&wt, "git rev-parse origin/main").trim(), f.remote_sha("main"));
        assert!(!wt.parent().unwrap().join(".forge-7.new").exists());

        session_commit(&wt, "b.txt", "two");
        assert_eq!(sh(&wt, "git log -1 --format='%an <%ae>'"), "Forge <forge@example.com>\n");
        std::fs::write(wt.join("scratch.txt"), "kept").unwrap();
        assert_eq!(f.git.worktree(&f.repo, BRANCH, TOKEN).await.unwrap(), wt);
        assert!(wt.join("scratch.txt").exists());

        assert_eq!(std::fs::metadata(&wt).unwrap().permissions().mode() & 0o070, 0o070);
        assert_eq!(std::fs::metadata(wt.join(".git/config")).unwrap().permissions().mode() & 0o060, 0o060);
        assert_eq!(group_writable(&f.root().join("data/mirrors")), Vec::<PathBuf>::new());
        assert_eq!(std::fs::metadata(wt.parent().unwrap()).unwrap().permissions().mode() & 0o020, 0);
    }

    #[tokio::test]
    async fn clone_starts_from_an_existing_remote_branch() {
        let f = Fixture::new();
        f.upstream_commit("forge/7", "x.txt", "pushed");
        let wt = f.git.worktree(&f.repo, BRANCH, TOKEN).await.unwrap();
        assert_eq!(sh(&wt, "cat x.txt"), "pushed\n");
        assert_eq!(f.git.collect(&f.repo, BRANCH, TOKEN).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn collect_takes_and_counts_the_session_commits() {
        let f = Fixture::new();
        let wt = f.git.worktree(&f.repo, BRANCH, TOKEN).await.unwrap();
        assert_eq!(f.git.collect(&f.repo, BRANCH, TOKEN).await.unwrap(), 0);
        session_commit(&wt, "b.txt", "two");
        let tip = session_commit(&wt, "c.txt", "three");
        assert_eq!(f.git.collect(&f.repo, BRANCH, TOKEN).await.unwrap(), 2);
        assert_eq!(f.mirror_sha("refs/forge/incoming/forge/7"), tip);
        assert!(!f.mirror().join("refs/heads/forge").exists());
        let newer = session_commit(&wt, "d.txt", "four");
        assert_eq!(f.git.collect(&f.repo, BRANCH, TOKEN).await.unwrap(), 3);
        assert_eq!(f.mirror_sha("refs/forge/incoming/forge/7"), newer);
    }

    #[tokio::test]
    async fn push_sends_what_collect_took_under_a_lease() {
        let f = Fixture::new();
        let wt = f.git.worktree(&f.repo, BRANCH, TOKEN).await.unwrap();
        assert!(f.git.push(&f.repo, BRANCH, TOKEN).await.is_err(), "nothing collected yet");
        let tip = session_commit(&wt, "b.txt", "two");
        f.git.collect(&f.repo, BRANCH, TOKEN).await.unwrap();
        session_commit(&wt, "late.txt", "after-collect");
        assert_eq!(f.git.push(&f.repo, BRANCH, TOKEN).await.unwrap(), tip);
        assert_eq!(f.remote_sha("forge/7"), tip);
        assert_eq!(f.mirror_sha("refs/forge/pushed/forge/7"), tip);
        assert!(!std::fs::read_to_string(f.mirror().join("config")).unwrap().contains(TOKEN));

        f.upstream_commit("forge/7", "d.txt", "someone-else");
        session_commit(&wt, "e.txt", "more");
        f.git.collect(&f.repo, BRANCH, TOKEN).await.unwrap();
        assert!(f.git.push(&f.repo, BRANCH, TOKEN).await.is_err(), "the lease should stop a blind overwrite");
    }

    #[tokio::test]
    async fn push_never_overwrites_a_branch_forge_did_not_push() {
        let f = Fixture::new();
        f.upstream_commit("forge/7", "x.txt", "theirs");
        let theirs = f.remote_sha("forge/7");
        let wt = f.git.worktree(&f.repo, BRANCH, TOKEN).await.unwrap();
        session_commit(&wt, "b.txt", "two");
        f.git.collect(&f.repo, BRANCH, TOKEN).await.unwrap();
        assert!(f.git.push(&f.repo, BRANCH, TOKEN).await.is_err(), "before its first push, the branch must not exist");
        assert_eq!(f.remote_sha("forge/7"), theirs);

        let f = Fixture::new();
        let wt = f.git.worktree(&f.repo, BRANCH, TOKEN).await.unwrap();
        session_commit(&wt, "b.txt", "two");
        f.collect_and_push().await;
        f.upstream_commit("forge/7", "d.txt", "someone-else");
        let theirs = f.remote_sha("forge/7");
        f.git.refresh(&f.repo, TOKEN).await.unwrap();
        session_commit(&wt, "e.txt", "more");
        f.git.collect(&f.repo, BRANCH, TOKEN).await.unwrap();
        assert!(f.git.push(&f.repo, BRANCH, TOKEN).await.is_err(), "the lease is what Forge pushed, not what it fetched");
        assert_eq!(f.remote_sha("forge/7"), theirs);
    }

    #[tokio::test]
    async fn review_checkout_is_a_fresh_clone_of_exactly_what_was_pushed() {
        let f = Fixture::new();
        let wt = f.git.worktree(&f.repo, BRANCH, TOKEN).await.unwrap();
        session_commit(&wt, "b.txt", "two");
        assert!(f.git.review_checkout(&f.repo, BRANCH, TOKEN).await.is_err(), "nothing pushed yet");
        let pushed = f.collect_and_push().await;
        session_commit(&wt, "late.txt", "unpushed");
        sh(&wt, "git config diff.external /bin/false && echo pass > PLANTED.md");

        let dir = f.git.review_checkout(&f.repo, BRANCH, TOKEN).await.unwrap();
        assert_eq!(dir, f.root().join("data/worktrees/acme/app/forge-7-review"));
        assert_eq!(sh(&dir, "git rev-parse HEAD").trim(), pushed);
        assert_eq!(sh(&dir, "git rev-parse --abbrev-ref HEAD"), "forge/7\n");
        assert_eq!(sh(&dir, "git diff --name-only origin/main...HEAD"), "b.txt\n");
        assert_eq!(sh(&dir, "git status --porcelain"), "");
        assert_eq!(sh(&dir, "git config --get diff.external || true"), "");
        assert_eq!(std::fs::metadata(&dir).unwrap().permissions().mode() & 0o070, 0o070);
        assert!(!dir.parent().unwrap().join(".forge-7-review.new").exists());

        std::fs::write(dir.join("left.txt"), "x").unwrap();
        assert_eq!(f.git.review_checkout(&f.repo, BRANCH, TOKEN).await.unwrap(), dir);
        assert!(!dir.join("left.txt").exists(), "each review starts clean");
        f.git.remove_review_checkout(&f.repo, BRANCH).await.unwrap();
        assert!(!dir.exists() && wt.exists());
        f.git.remove_review_checkout(&f.repo, BRANCH).await.unwrap();
    }

    #[tokio::test]
    async fn land_rebases_what_was_pushed_and_pushes_the_result() {
        let f = Fixture::new();
        let wt = f.git.worktree(&f.repo, BRANCH, TOKEN).await.unwrap();
        session_commit(&wt, "b.txt", "two");
        assert!(f.try_land(None, &[]).await.is_err(), "nothing pushed yet");
        let pushed = f.collect_and_push().await;
        f.upstream_commit("main", "c.txt", "three");
        let hostile = session_commit(&wt, "hostile.txt", "unreviewed");

        let Landing::Ready { sha } = f.land(None, &[]).await else { panic!("expected Ready") };
        assert_ne!(sha, pushed);
        assert_eq!(f.mirror_sha(&format!("{sha}^")), f.remote_sha("main"));
        assert_eq!(sh(&f.mirror(), &format!("git show {sha}:b.txt")), "two\n");
        assert_eq!(sh(&f.mirror(), &format!("git ls-tree --name-only {sha}")), "a.txt\nb.txt\nc.txt\n");
        assert_eq!(f.remote_sha("forge/7"), sha);
        assert_eq!(f.mirror_sha("refs/forge/pushed/forge/7"), sha);
        assert_eq!(sh(&wt, "git rev-parse HEAD").trim(), hostile, "Forge never touches the clone");
        assert_eq!(group_writable(&f.root().join("data/mirrors")), Vec::<PathBuf>::new());

        let newer = session_commit(&wt, "d.txt", "four");
        assert_eq!(f.collect_and_push().await, newer);
        assert_eq!(f.remote_sha("forge/7"), newer);
    }

    #[tokio::test]
    async fn land_never_overwrites_a_branch_someone_else_pushed() {
        let f = Fixture::new();
        let wt = f.git.worktree(&f.repo, BRANCH, TOKEN).await.unwrap();
        session_commit(&wt, "b.txt", "two");
        f.collect_and_push().await;
        f.upstream_commit("main", "c.txt", "three");
        f.upstream_commit("forge/7", "d.txt", "someone-else");
        let theirs = f.remote_sha("forge/7");
        assert!(f.try_land(None, &[]).await.is_err());
        assert_eq!(f.remote_sha("forge/7"), theirs);
    }

    #[tokio::test]
    async fn land_conflict_aborts_and_says_why() {
        let f = Fixture::new();
        let wt = f.git.worktree(&f.repo, BRANCH, TOKEN).await.unwrap();
        f.upstream_commit("main", "a.txt", "theirs");
        let tip = session_commit(&wt, "a.txt", "mine");
        f.collect_and_push().await;
        let Landing::Conflict(output) = f.land(None, &[]).await else { panic!("expected Conflict") };
        assert!(output.contains("CONFLICT"), "{output}");
        let landing = f.landing();
        assert_eq!(sh(&landing, "git status --porcelain"), "");
        let gitdir = PathBuf::from(sh(&landing, "git rev-parse --absolute-git-dir").trim());
        assert!(!gitdir.join("rebase-merge").exists() && !gitdir.join("rebase-apply").exists());
        assert_eq!(f.mirror_sha("refs/forge/pushed/forge/7"), tip);
        assert_eq!(f.remote_sha("forge/7"), tip);
        assert!(matches!(f.land(None, &[]).await, Landing::Conflict(_)), "a second try starts clean");
    }

    #[tokio::test]
    async fn land_check_failure_carries_its_output() {
        let f = Fixture::new();
        let wt = f.git.worktree(&f.repo, BRANCH, TOKEN).await.unwrap();
        session_commit(&wt, "b.txt", "two");
        let pushed = f.collect_and_push().await;
        f.upstream_commit("main", "c.txt", "three");
        let check = "echo migration 0042 is used twice; exit 1";
        let Landing::CheckFailed(output) = f.land(Some(check), &[]).await else { panic!("expected CheckFailed") };
        assert_eq!(output, "migration 0042 is used twice\n");
        assert_eq!(f.remote_sha("forge/7"), pushed);
        assert_eq!(f.mirror_sha("refs/forge/pushed/forge/7"), pushed);
    }

    #[tokio::test]
    async fn check_runs_in_a_fresh_export_that_is_removed() {
        let f = Fixture::new();
        let wt = f.git.worktree(&f.repo, BRANCH, TOKEN).await.unwrap();
        session_commit(&wt, "b.txt", "two");
        f.collect_and_push().await;
        f.upstream_commit("main", "c.txt", "three");
        let seen = f.root().join("seen");
        let env = [("SEEN".to_string(), seen.display().to_string())];
        let check = "test ! -e .git && { pwd; cat a.txt b.txt c.txt; } > \"$SEEN\"";
        let Landing::Ready { .. } = f.land(Some(check), &env).await else { panic!("expected Ready") };
        let seen = std::fs::read_to_string(seen).unwrap();
        let (dir, files) = seen.split_once('\n').unwrap();
        assert!(dir.starts_with(&f.root().join("data/worktrees/.runs/").display().to_string()), "{dir}");
        assert_eq!(files, "one\ntwo\nthree\n");
        assert!(!Path::new(dir).exists(), "the export should be removed");
        assert_eq!(std::fs::read_dir(f.root().join("data/worktrees/.runs")).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn run_on_main_sees_fresh_main() {
        let f = Fixture::new();
        let limit = Duration::from_secs(20);
        let out = f.git.run_on_main(&f.repo, "cat a.txt", &[], limit, TOKEN).await.unwrap();
        assert_eq!(out.output, "one\n");
        f.upstream_commit("main", "n.txt", "newer");
        let out = f.git.run_on_main(&f.repo, "test ! -e .git && cat a.txt n.txt", &[], limit, TOKEN).await.unwrap();
        assert!(out.ok, "{}", out.output);
        assert_eq!(out.output, "one\nnewer\n");
    }

    #[tokio::test]
    async fn hostile_clone_config_has_no_effect() {
        let f = Fixture::new();
        let root = f.root();
        let wt = f.git.worktree(&f.repo, BRANCH, TOKEN).await.unwrap();
        let tip = session_commit(&wt, "b.txt", "two");

        sh(&root, "mkdir -p evil/acme && git init -q --bare evil/acme/app.git");
        let marker = root.join("hostile-ran");
        let hooks = root.join("hooks");
        std::fs::create_dir(&hooks).unwrap();
        for hook in ["pre-push", "post-checkout", "reference-transaction", "post-rewrite", "fsmonitor", "pack"] {
            let path = hooks.join(hook);
            std::fs::write(&path, format!("#!/bin/sh\necho {hook} >> {}\n", marker.display())).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let evil = format!("file://{}", root.join("evil").display());
        let config = [
            format!("url.{evil}.insteadOf {}", f.git.remote_base),
            format!("url.{evil}/acme/app.git.insteadOf {}", f.mirror().display()),
            format!("core.hooksPath {}", hooks.display()),
            format!("core.fsmonitor {}", hooks.join("fsmonitor").display()),
            format!("uploadpack.packObjectsHook {}", hooks.join("pack").display()),
        ];
        for setting in config {
            sh(&wt, &format!("git config {setting}"));
        }

        assert_eq!(f.git.collect(&f.repo, BRANCH, TOKEN).await.unwrap(), 1);
        assert_eq!(f.git.push(&f.repo, BRANCH, TOKEN).await.unwrap(), tip);
        f.upstream_commit("main", "c.txt", "three");
        let Landing::Ready { sha } = f.land(Some("true"), &[]).await else { panic!("expected Ready") };
        assert_eq!(f.remote_sha("forge/7"), sha);
        assert!(sh(&root.join("evil/acme/app.git"), "git for-each-ref").is_empty());
        assert!(!marker.exists(), "Forge's git ran: {}", std::fs::read_to_string(&marker).unwrap_or_default());
    }

    #[tokio::test]
    async fn remove_worktree_drops_the_clone_landing_and_refs() {
        let f = Fixture::new();
        let wt = f.git.worktree(&f.repo, BRANCH, TOKEN).await.unwrap();
        session_commit(&wt, "b.txt", "two");
        f.collect_and_push().await;
        f.land(None, &[]).await;
        std::fs::write(wt.join("untracked.txt"), "x").unwrap();
        f.git.remove_worktree(&f.repo, BRANCH).await.unwrap();
        assert!(!wt.exists() && !f.landing().exists());
        assert_eq!(sh(&f.mirror(), "git for-each-ref refs/forge"), "");
        assert_eq!(sh(&f.mirror(), "git worktree list").lines().count(), 1);
        f.git.remove_worktree(&f.repo, BRANCH).await.unwrap();
    }

    #[test]
    fn session_user_commands_go_through_sudo_and_timeout() {
        let mut git = Fixture::new().git;
        let limit = Duration::from_secs(90);
        assert_eq!(git.session_command("make check", &["A"], limit), ("sh".into(), vec!["-c".into(), "make check".into()]));
        assert_eq!(git.upload_pack(), "git -c core.hooksPath=/dev/null -c core.fsmonitor=false upload-pack");

        git.session_user = Some("forge-run".into());
        let (program, args) = git.session_command("make check", &["STAGING_URL", "B"], limit);
        assert_eq!(program, "sudo");
        let expected = "-n -H -u forge-run --preserve-env=STAGING_URL,B -- timeout -k 30 90 sh -c";
        assert_eq!(args[..args.len() - 1].join(" "), expected);
        assert_eq!(args.last().unwrap(), "make check");
        let (_, args) = git.session_command("true", &[], limit);
        assert_eq!(args.join(" "), "-n -H -u forge-run -- timeout -k 30 90 sh -c true");
        assert_eq!(
            git.upload_pack(),
            "sudo -n -H -u forge-run -- timeout -k 30 600 git -c core.hooksPath=/dev/null -c core.fsmonitor=false upload-pack"
        );
    }

    #[test]
    fn only_the_wrapper_exit_counts_as_timed_out() {
        let timed_out = Some(ExitStatus::from_raw(124 << 8));
        assert_eq!(unless_timed_out(timed_out, true), None);
        assert_eq!(unless_timed_out(timed_out, false), timed_out);
        let failed = Some(ExitStatus::from_raw(1 << 8));
        assert_eq!(unless_timed_out(failed, true), failed);
    }

    #[test]
    fn token_goes_in_a_header_for_http_only() {
        let mut git = Fixture::new().git;
        assert!(git.auth_env(TOKEN).is_empty());
        git.remote_base = "https://github.com".into();
        let env = git.auth_env(TOKEN);
        let basic = base64::engine::general_purpose::STANDARD.encode(format!("x-access-token:{TOKEN}"));
        assert_eq!(env[1].1, "http.extraHeader");
        assert_eq!(env[2].1, format!("Authorization: Basic {basic}"));
    }

    #[tokio::test]
    async fn https_fetch_sends_the_header_and_stores_no_token() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut conn, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            let mut buf = [0u8; 4096];
            while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = conn.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                head.extend_from_slice(&buf[..n]);
            }
            conn.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
            String::from_utf8_lossy(&head).into_owned()
        });
        let mut f = Fixture::new();
        f.git.remote_base = format!("http://{addr}");
        assert!(f.git.worktree(&f.repo, BRANCH, TOKEN).await.is_err());
        let head = server.await.unwrap();
        let basic = base64::engine::general_purpose::STANDARD.encode(format!("x-access-token:{TOKEN}"));
        assert!(head.contains(&format!("Authorization: Basic {basic}")), "{head}");
        let config = std::fs::read_to_string(f.mirror().join("config")).unwrap();
        assert!(config.contains(&format!("url = http://{addr}/acme/app.git")));
        assert!(!config.contains(TOKEN) && !config.contains(&basic));
    }

    #[tokio::test]
    async fn run_combines_output_and_passes_env() {
        let git = Fixture::new().git;
        let dir = tempfile::tempdir().unwrap();
        let env = [("FORGE_TEST_VALUE".to_string(), "bar".to_string())];
        let out = git
            .run_as_session(dir.path(), "echo out; echo err >&2; echo $FORGE_TEST_VALUE; pwd; exit 3", &env, Duration::from_secs(10))
            .await
            .unwrap();
        assert!(!out.ok);
        assert_eq!(out.code, Some(3));
        let pwd = dir.path().canonicalize().unwrap();
        assert_eq!(out.output, format!("out\nerr\nbar\n{}\n", pwd.display()));
    }

    #[tokio::test]
    async fn run_keeps_the_tail_of_long_output() {
        let git = Fixture::new().git;
        let dir = tempfile::tempdir().unwrap();
        let out = git
            .run_as_session(dir.path(), "yes 0123456789 | head -c 200000; echo; echo LAST", &[], Duration::from_secs(10))
            .await
            .unwrap();
        assert!(out.ok);
        assert_eq!(out.output.len(), OUTPUT_TAIL);
        assert!(out.output.ends_with("LAST\n"));
    }

    #[tokio::test]
    async fn run_stops_the_whole_group_on_timeout() {
        let git = Fixture::new().git;
        let dir = tempfile::tempdir().unwrap();
        let started = Instant::now();
        let out = git
            .run_as_session(dir.path(), "echo started; sleep 30; echo never", &[], Duration::from_millis(300))
            .await
            .unwrap();
        assert!(started.elapsed() < Duration::from_secs(4), "took {:?}", started.elapsed());
        assert!(!out.ok);
        assert_eq!(out.code, None);
        assert!(out.output.starts_with("started\n") && !out.output.contains("never"), "{}", out.output);
    }

    #[tokio::test]
    async fn run_does_not_wait_for_background_leftovers() {
        let git = Fixture::new().git;
        let dir = tempfile::tempdir().unwrap();
        let started = Instant::now();
        let out = git.run_as_session(dir.path(), "sleep 30 & echo done", &[], Duration::from_secs(20)).await.unwrap();
        assert!(started.elapsed() < Duration::from_secs(5), "took {:?}", started.elapsed());
        assert!(out.ok);
        assert_eq!(out.output, "done\n");
    }
}
