//! One task end to end on this machine: the real engine, SQLite, git (against
//! a file:// "GitHub") and session runner (with a fake `claude`), a fake GitHub
//! API. Ready → Building → PR → review (in a fresh checkout of the pushed
//! commit) → Reviewed → Accepted → rebased → merged, landing what was pushed
//! and reviewed, never what the clone holds later.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;

use forge::config::RepoConfig;
use forge::db::Db;
use forge::engine::Engine;
use forge::github::GitHubApi;
use forge::gitops::CliGit;
use forge::notify::RecordingNotifier;
use forge::proxy::SessionKeys;
use forge::session::ClaudeRunner;
use forge::types::{Comment, IssueThread, ProjectItem, PullRequest, Status};

const REPO: &str = "example-team/shop";

#[derive(Default)]
struct Hub {
    items: Mutex<Vec<ProjectItem>>,
    comments: Mutex<Vec<String>>,
    merges: Mutex<Vec<(u64, String)>>,
}

impl Hub {
    fn status(&self) -> Option<Status> {
        self.items.lock().unwrap()[0].status
    }
    fn set(&self, status: Status) {
        self.items.lock().unwrap()[0].status = Some(status);
    }
}

#[async_trait]
impl GitHubApi for Hub {
    async fn project_items(&self, _: &RepoConfig) -> Result<Vec<ProjectItem>> {
        Ok(self.items.lock().unwrap().clone())
    }
    async fn set_status(&self, _: &RepoConfig, _: &str, status: Status) -> Result<()> {
        self.set(status);
        Ok(())
    }
    async fn issue_thread(&self, repo: &str, number: u64) -> Result<IssueThread> {
        let comments: Vec<Comment> = self
            .comments
            .lock()
            .unwrap()
            .iter()
            .enumerate()
            .map(|(i, body)| Comment {
                id: i as u64 + 1,
                author: "example-forge[bot]".into(),
                body: body.clone(),
                created_at: Utc::now(),
            })
            .collect();
        Ok(IssueThread {
            number,
            title: "Add a greeting".into(),
            body: "Add a greeting line to the README.".into(),
            author: "bob-gh".into(),
            labels: vec![],
            url: format!("https://github.com/{repo}/issues/{number}"),
            comments,
        })
    }
    async fn comment(&self, _: &str, _: u64, body: &str) -> Result<u64> {
        let mut c = self.comments.lock().unwrap();
        c.push(body.to_string());
        Ok(c.len() as u64)
    }
    async fn create_issue(&self, _: &str, _: &str, _: &str, _: &[String]) -> Result<u64> {
        Ok(99)
    }
    async fn open_or_update_pr(&self, repo: &str, _: &str, _: &str, _: &str, _: &str) -> Result<PullRequest> {
        Ok(PullRequest { number: 10, url: format!("https://github.com/{repo}/pull/10"), head_sha: String::new() })
    }
    async fn merge_pr(&self, _: &str, number: u64, sha: &str, _: &str) -> Result<()> {
        self.merges.lock().unwrap().push((number, sha.to_string()));
        Ok(())
    }
    async fn git_token(&self) -> Result<String> {
        Ok("unused-for-file-remotes".into())
    }
    async fn collaborators(&self, _: &str) -> Result<Vec<String>> {
        Ok(vec![])
    }
}

/// A `claude` that commits a line, sets git config to steer a reviewer, and
/// asks for a PR; or reviews, passing only in a clean checkout of exactly the
/// pushed commit that adds the line.
const FAKE_CLAUDE: &str = r#"#!/bin/sh
prompt=$(cat)
case "$prompt" in
  *REVIEW-ROLE*)
    sha=$(printf '%s\n' "$prompt" | sed -n 's/.*origin\/main\.\.\.\([0-9a-f]\{40\}\).*/\1/p' | head -n 1)
    pass=false
    case "$PWD" in
      */forge-1-review)
        if [ "$(git rev-parse HEAD)" = "$sha" ] && [ -z "$(git config diff.external)" ] &&
           git diff origin/main..."$sha" -- README.md | grep -q '^+hello from forge$'; then
          pass=true
        fi
        ;;
    esac
    printf '%s' '{"session_id":"rev-1","result":"Looks right.\n```forge\n{\"outcome\":\"review\",\"pass\":'"$pass"',\"notes\":\"Read the README.\"}\n```","is_error":false,"total_cost_usd":0.01}'
    ;;
  *)
    echo "hello from forge" >> README.md
    git add README.md && git commit -q -m "Add a greeting"
    git config diff.external "$PWD/always-empty"
    printf '%s' '{"session_id":"build-1","result":"Done.\n```forge\n{\"outcome\":\"pr\",\"title\":\"Add a greeting\",\"body\":\"Adds one line.\"}\n```","is_error":false,"total_cost_usd":0.02}'
    ;;
esac
"#;

fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git").arg("-C").arg(dir).args(args).output().unwrap();
    assert!(out.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

fn write(path: &Path, text: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, text).unwrap();
}

/// A bare "GitHub" repo with a README and a merge check, and a clone to push from.
fn remote(root: &Path) -> (PathBuf, PathBuf) {
    let bare = root.join("remote").join(REPO).with_extension("git");
    std::fs::create_dir_all(&bare).unwrap();
    git(&bare, &["init", "-q", "--bare", "-b", "main"]);
    let seed = root.join("seed");
    std::fs::create_dir_all(&seed).unwrap();
    git(&seed, &["init", "-q", "-b", "main"]);
    git(&seed, &["config", "user.name", "Bob"]);
    git(&seed, &["config", "user.email", "bob@example.com"]);
    write(&seed.join("README.md"), "SHOP\n");
    write(&seed.join("scripts/check.sh"), "test -f README.md\n");
    git(&seed, &["add", "."]);
    git(&seed, &["commit", "-q", "-m", "Start"]);
    git(&seed, &["remote", "add", "origin", bare.to_str().unwrap()]);
    git(&seed, &["push", "-q", "origin", "main"]);
    (bare, seed)
}

#[tokio::test]
async fn a_task_goes_from_ready_to_main() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let (bare, seed) = remote(root);

    let claude = root.join("bin/claude");
    write(&claude, FAKE_CLAUDE);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&claude, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    let mut cfg = forge::config::sample();
    cfg.data_dir = root.join("data");
    cfg.claude.bin = claude;
    cfg.claude.session_user = None;
    cfg.github.remote_base = format!("file://{}", root.join("remote").display());
    let repo = &mut cfg.repos[0];
    repo.merge_check = Some("sh scripts/check.sh".into());
    repo.e2e = None;
    for (name, role) in repo.roles.iter_mut() {
        role.prompt_file = root.join(format!("prompts/{name}.md"));
        let text = if name == "review" { "REVIEW-ROLE" } else { "BUILD-ROLE" };
        write(&role.prompt_file, text);
    }
    std::fs::create_dir_all(cfg.state_dir()).unwrap();

    let db = Arc::new(Db::open(&cfg.db_path()).unwrap());
    let hub = Arc::new(Hub::default());
    hub.items.lock().unwrap().push(ProjectItem {
        item_id: "I1".into(),
        issue_number: 1,
        title: "Add a greeting".into(),
        status: Some(Status::Ready),
        labels: vec![],
        author: "bob-gh".into(),
    });
    let notes = Arc::new(RecordingNotifier::default());
    let git_ops = Arc::new(CliGit {
        data_dir: cfg.data_dir.clone(),
        remote_base: cfg.github.remote_base.clone(),
        author_name: cfg.github.bot_login(),
        author_email: "forge@example.com".into(),
        session_user: None,
    });
    let runner = Arc::new(ClaudeRunner::new(&cfg));
    let cfg = Arc::new(cfg);
    let engine = Arc::new(Engine::new(cfg.clone(), db.clone(), hub.clone(), runner, git_ops, notes.clone(), SessionKeys::new()));

    // Ready → Building: the session commits, Forge pushes, opens the PR, and
    // the review passes in its own checkout, removed after.
    engine.tick().await.unwrap();
    engine.wait_idle().await;
    let task = db.task(REPO, 1).unwrap().unwrap();
    assert_eq!(task.status, Status::Reviewed, "comments: {:?}", hub.comments.lock().unwrap());
    let worktrees = cfg.data_dir.join("worktrees").join(REPO);
    assert!(!worktrees.join("forge-1-review").exists(), "the review checkout is removed");
    assert_eq!(hub.status(), Some(Status::Reviewed));
    assert_eq!(task.session_id.as_deref(), Some("build-1"));
    assert_eq!(task.pr_number, Some(10));
    let pushed = git(&bare, &["rev-parse", "refs/heads/forge/1"]);
    assert_eq!(git(&bare, &["log", "-1", "--format=%s", &pushed]), "Add a greeting");
    assert!(hub.comments.lock().unwrap().iter().any(|c| c.contains("forge try 1")));
    assert!(notes.pings.lock().unwrap().iter().any(|p| p.contains("ready to verify")));

    // Main moves on meanwhile; a person verifies and accepts.
    write(&seed.join("CHANGELOG.md"), "Started.\n");
    git(&seed, &["add", "."]);
    git(&seed, &["commit", "-q", "-m", "Changelog"]);
    git(&seed, &["push", "-q", "origin", "main"]);
    let main = git(&bare, &["rev-parse", "main"]);
    hub.set(Status::Accepted);

    // Something else running as the session user commits in the clone after acceptance.
    let clone = worktrees.join("forge-1");
    write(&clone.join("hostile.txt"), "never reviewed\n");
    git(&clone, &["add", "hostile.txt"]);
    git(&clone, &["commit", "-q", "-m", "Unreviewed"]);

    // Accepted → what was pushed, rebased on the new main, checked, pushed and merged with that sha.
    engine.tick().await.unwrap();
    engine.wait_idle().await;
    let landed = git(&bare, &["rev-parse", "refs/heads/forge/1"]);
    assert_ne!(landed, pushed, "the branch was rebased");
    assert_eq!(git(&bare, &["rev-parse", &format!("{landed}^")]), main);
    assert_eq!(*hub.merges.lock().unwrap(), vec![(10, landed.clone())]);
    let files = git(&bare, &["ls-tree", "-r", "--name-only", &landed]);
    assert_eq!(files.lines().collect::<Vec<_>>(), ["CHANGELOG.md", "README.md", "scripts/check.sh"]);
    assert_eq!(git(&bare, &["show", &format!("{landed}:README.md")]), "SHOP\nhello from forge");
    assert_eq!(hub.status(), Some(Status::OnMain));
    assert_eq!(db.task(REPO, 1).unwrap().unwrap().status, Status::OnMain);
    assert!(!clone.exists(), "the clone is removed");
}
