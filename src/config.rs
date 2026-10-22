//! Forge's settings: one TOML file on the box. Each repo Forge serves has its
//! settings here, not in the repo. Secrets are never inline: every one is a
//! path to a file only Forge's own user can read.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Where the dashboard and API listen, behind a TLS proxy.
    pub listen: String,
    /// Where `forge attach-gate` reaches the daemon. Loopback only.
    #[serde(default = "default_internal_listen")]
    pub internal_listen: String,
    /// The dashboard's public address, used in links.
    pub public_url: String,
    /// Forge's SQLite file, mirrors, worktrees and build-slot lock.
    pub data_dir: PathBuf,
    #[serde(default = "default_poll_seconds")]
    pub poll_seconds: u64,
    pub claude: ClaudeConfig,
    pub github: GitHubConfig,
    pub pool: PoolConfig,
    #[serde(default)]
    pub telegram: Option<TelegramConfig>,
    #[serde(default)]
    pub mail: Option<MailConfig>,
    pub auth: AuthConfig,
    #[serde(default)]
    pub repos: Vec<RepoConfig>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaudeConfig {
    #[serde(default = "default_claude_bin")]
    pub bin: PathBuf,
    /// The unprivileged user sessions run as, so they can't read the plan
    /// logins. None runs them as Forge's own user (development only).
    #[serde(default)]
    pub session_user: Option<String>,
    /// Where the local Claude proxy listens. Loopback only.
    #[serde(default = "default_proxy_listen")]
    pub proxy_listen: String,
    #[serde(default = "default_burst_timeout_minutes")]
    pub burst_timeout_minutes: u64,
    /// The `forge` binary sessions call for `forge slot`.
    #[serde(default = "default_forge_bin")]
    pub forge_bin: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitHubConfig {
    pub app_id: u64,
    /// The App's slug: its comments come from `<slug>[bot]`.
    pub app_slug: String,
    pub private_key_file: PathBuf,
    pub installation_id: u64,
    #[serde(default = "default_github_api")]
    pub api_url: String,
    /// Where repos are cloned from: `<remote_base>/<owner>/<repo>.git`.
    #[serde(default = "default_github_remote")]
    pub remote_base: String,
}

impl GitHubConfig {
    /// The login Forge's own comments and commits carry.
    pub fn bot_login(&self) -> String {
        format!("{}[bot]", self.app_slug)
    }

    /// Whether a login is Forge's own: GitHub's REST API names it
    /// `<slug>[bot]`, its GraphQL API `<slug>`.
    pub fn is_bot(&self, login: &str) -> bool {
        login.eq_ignore_ascii_case(&self.app_slug) || login.eq_ignore_ascii_case(&self.bot_login())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolConfig {
    pub plans: Vec<PlanConfig>,
    /// No new request goes to a plan past this percent of its 5-hour window.
    #[serde(default = "default_five_hour_cap")]
    pub five_hour_cap: f64,
    /// No new request goes to a plan past this percent of its week.
    #[serde(default = "default_week_cap")]
    pub week_cap: f64,
    #[serde(default = "default_upstream")]
    pub upstream: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanConfig {
    pub label: String,
    /// The plan's long-lived Claude login token, readable only by Forge's user.
    pub token_file: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelegramConfig {
    pub bot_token_file: PathBuf,
    pub chat_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailConfig {
    pub resend_api_key_file: PathBuf,
    /// e.g. "Forge <forge@example.com>"
    pub from: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    /// A random secret for hashing sign-in codes and tokens.
    pub secret_file: PathBuf,
    #[serde(default = "default_session_days")]
    pub session_days: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepoConfig {
    /// "owner/repo"
    pub name: String,
    /// The Project's owner (an org or user login) and number.
    pub project_owner: String,
    pub project_number: u64,
    #[serde(default = "default_branch")]
    pub default_branch: String,
    /// Tasks building at once. 0 pauses the repo.
    #[serde(default = "default_concurrency")]
    pub concurrency: u32,
    /// The GitHub login who orders the Ready column.
    pub orderer: String,
    pub members: Vec<Member>,
    pub roles: BTreeMap<String, RoleConfig>,
    /// The role a task gets when none of its labels names one.
    pub default_role: String,
    #[serde(default)]
    pub build: Option<String>,
    #[serde(default)]
    pub run: Option<String>,
    /// Stops what `run` started, for `forge stop`.
    #[serde(default)]
    pub stop: Option<String>,
    /// Loads synthetic data for `forge try`. Never real data.
    #[serde(default)]
    pub seed: Option<String>,
    /// Runs in the task's worktree after rebasing on main, before merging.
    #[serde(default)]
    pub merge_check: Option<String>,
    #[serde(default)]
    pub e2e: Option<E2eConfig>,
    /// Extra instructions every session in this repo gets.
    #[serde(default)]
    pub notes: Option<String>,
    /// What members' own Claude should know about this repo, added to the
    /// Forge skill that `forge setup` installs (GET /api/skill).
    #[serde(default)]
    pub skill_notes: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Member {
    pub github: String,
    pub email: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleConfig {
    pub prompt_file: PathBuf,
    pub model: String,
    pub effort: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct E2eConfig {
    pub command: String,
    #[serde(default = "default_e2e_hours")]
    pub every_hours: u64,
    pub staging_url: String,
}

/// The role every repo must have for review runs.
pub const REVIEW_ROLE: &str = "review";

const EFFORTS: [&str; 5] = ["low", "medium", "high", "xhigh", "max"];

fn default_internal_listen() -> String {
    "127.0.0.1:8472".into()
}
fn default_poll_seconds() -> u64 {
    30
}
fn default_claude_bin() -> PathBuf {
    "claude".into()
}
fn default_proxy_listen() -> String {
    "127.0.0.1:8471".into()
}
fn default_burst_timeout_minutes() -> u64 {
    240
}
fn default_forge_bin() -> PathBuf {
    "/usr/local/bin/forge".into()
}
fn default_github_api() -> String {
    "https://api.github.com".into()
}
fn default_github_remote() -> String {
    "https://github.com".into()
}
fn default_five_hour_cap() -> f64 {
    85.0
}
fn default_week_cap() -> f64 {
    90.0
}
fn default_upstream() -> String {
    "https://api.anthropic.com".into()
}
fn default_session_days() -> u32 {
    14
}
fn default_branch() -> String {
    "main".into()
}
fn default_concurrency() -> u32 {
    1
}
fn default_e2e_hours() -> u64 {
    4
}

impl Config {
    pub fn load(path: &Path) -> Result<Config> {
        let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let config = Config::parse(&text).with_context(|| format!("in {}", path.display()))?;
        Ok(config)
    }

    pub fn parse(text: &str) -> Result<Config> {
        let config: Config = toml::from_str(text)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.pool.plans.is_empty() {
            bail!("pool.plans is empty: Forge needs at least one Claude plan");
        }
        let mut labels = std::collections::BTreeSet::new();
        for plan in &self.pool.plans {
            if plan.label.is_empty() || !plan.label.chars().all(|c| c.is_ascii_alphanumeric() || "_.-".contains(c)) {
                bail!("plan label {:?} must be letters, digits, '_', '.' or '-'", plan.label);
            }
            if !labels.insert(plan.label.as_str()) {
                bail!("plan label {:?} appears twice", plan.label);
            }
        }
        for cap in [self.pool.five_hour_cap, self.pool.week_cap] {
            if !(0.0..=100.0).contains(&cap) {
                bail!("pool caps are percents between 0 and 100");
            }
        }
        if self.poll_seconds == 0 {
            bail!("poll_seconds must be at least 1");
        }
        let mut names = std::collections::BTreeSet::new();
        for repo in &self.repos {
            repo.validate()?;
            if !names.insert(repo.name.as_str()) {
                bail!("repo {} appears twice", repo.name);
            }
        }
        Ok(())
    }

    pub fn repo(&self, name: &str) -> Option<&RepoConfig> {
        self.repos.iter().find(|r| r.name.eq_ignore_ascii_case(name))
    }

    /// A member of any repo, by email (case-insensitive).
    pub fn member_by_email(&self, email: &str) -> Option<&Member> {
        self.repos
            .iter()
            .flat_map(|r| &r.members)
            .find(|m| m.email.eq_ignore_ascii_case(email.trim()))
    }

    /// A member of any repo, by GitHub login (case-insensitive).
    pub fn member_by_github(&self, login: &str) -> Option<&Member> {
        self.repos
            .iter()
            .flat_map(|r| &r.members)
            .find(|m| m.github.eq_ignore_ascii_case(login.trim()))
    }

    pub fn worktrees_dir(&self) -> PathBuf {
        self.data_dir.join("worktrees")
    }

    /// Forge's own state, in a folder only Forge's user can open.
    pub fn state_dir(&self) -> PathBuf {
        self.data_dir.join("state")
    }

    pub fn db_path(&self) -> PathBuf {
        self.state_dir().join("forge.db")
    }

    /// The attach keys `forge keys` last wrote: who may step into a session.
    pub fn attach_keys_path(&self) -> PathBuf {
        self.state_dir().join("forge-run.keys")
    }

    pub fn slot_path(&self) -> PathBuf {
        self.data_dir.join("build.lock")
    }

    /// Each task's tmux prompt and turn files, Claude's settings, and the
    /// session user's tmux server socket (see tmux.rs).
    pub fn sessions_dir(&self) -> PathBuf {
        self.data_dir.join("sessions")
    }

    pub fn tmux_socket(&self) -> PathBuf {
        self.sessions_dir().join("tmux.sock")
    }
}

impl RepoConfig {
    pub fn validate(&self) -> Result<()> {
        let parts: Vec<&str> = self.name.split('/').collect();
        if parts.len() != 2 || parts.iter().any(|p| p.is_empty()) {
            bail!("repo name {:?} must be owner/repo", self.name);
        }
        if self.members.is_empty() {
            bail!("repo {} has no members", self.name);
        }
        if !self.is_member(&self.orderer) {
            bail!("repo {}: orderer {} must be a member", self.name, self.orderer);
        }
        if !self.roles.contains_key(&self.default_role) {
            bail!("repo {}: default_role {} isn't one of its roles", self.name, self.default_role);
        }
        if !self.roles.contains_key(REVIEW_ROLE) {
            bail!("repo {} needs a {:?} role for review runs", self.name, REVIEW_ROLE);
        }
        for (name, role) in &self.roles {
            if role.model.trim().is_empty() {
                bail!("repo {}: role {} has no model", self.name, name);
            }
            if !EFFORTS.contains(&role.effort.as_str()) {
                bail!("repo {}: role {} effort must be one of {:?}", self.name, name, EFFORTS);
            }
        }
        if let Some(e2e) = &self.e2e {
            if e2e.every_hours == 0 {
                bail!("repo {}: e2e.every_hours must be at least 1", self.name);
            }
        }
        Ok(())
    }

    pub fn is_member(&self, login: &str) -> bool {
        self.members.iter().any(|m| m.github.eq_ignore_ascii_case(login))
    }

    /// The first label that names a role (not the review role), else the default.
    pub fn role_for(&self, labels: &[String]) -> String {
        labels
            .iter()
            .find(|l| l.as_str() != REVIEW_ROLE && self.roles.contains_key(l.as_str()))
            .cloned()
            .unwrap_or_else(|| self.default_role.clone())
    }

    pub fn owner_and_name(&self) -> (&str, &str) {
        self.name.split_once('/').unwrap_or((&self.name, ""))
    }
}

/// Reads a secret file, trimmed. Refuses a file anyone but its owner can read.
pub fn read_secret(path: &Path) -> Result<String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path)
            .with_context(|| format!("reading {}", path.display()))?
            .permissions()
            .mode();
        if mode & 0o077 != 0 {
            bail!("{} is readable by others (mode {:o}); chmod 600 it", path.display(), mode & 0o777);
        }
    }
    let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let text = text.trim().to_string();
    if text.is_empty() {
        bail!("{} is empty", path.display());
    }
    Ok(text)
}

/// A config for tests: one repo with UI, backend and review roles.
pub fn sample() -> Config {
    Config::parse(SAMPLE).expect("sample config is valid")
}

pub const SAMPLE: &str = r#"
listen = "127.0.0.1:8470"
public_url = "https://forge.example.com"
data_dir = "/var/lib/forge"

[claude]
session_user = "forge-run"

[github]
app_id = 1
app_slug = "example-forge"
private_key_file = "/etc/forge/github-app.pem"
installation_id = 2

[pool]
plans = [
  { label = "blue", token_file = "/etc/forge/plans/blue" },
  { label = "red", token_file = "/etc/forge/plans/red" },
  { label = "tan", token_file = "/etc/forge/plans/tan" },
]

[telegram]
bot_token_file = "/etc/forge/telegram-token"
chat_id = "-100123"

[mail]
resend_api_key_file = "/etc/forge/resend-key"
from = "Forge <forge@example.com>"

[auth]
secret_file = "/etc/forge/auth-secret"

[[repos]]
name = "example-team/shop"
project_owner = "example-team"
project_number = 7
concurrency = 1
orderer = "bob-gh"
default_role = "backend"
build = "cargo build"
run = "./scripts/dev.sh"
stop = "./scripts/dev.sh stop"
seed = "./scripts/seed-synthetic.sh"
merge_check = "./scripts/check-migration-numbers.sh"
members = [
  { github = "ann-gh", email = "ann@example.com", name = "Ann" },
  { github = "bob-gh", email = "bob@example.com", name = "Bob" },
]

[repos.e2e]
command = "npm run e2e"
staging_url = "https://staging.example.com"

[repos.roles.ui]
prompt_file = "/etc/forge/prompts/ui.md"
model = "claude-opus-5-5"
effort = "xhigh"

[repos.roles.backend]
prompt_file = "/etc/forge/prompts/backend.md"
model = "claude-opus-5-5"
effort = "xhigh"

[repos.roles.review]
prompt_file = "/etc/forge/prompts/review.md"
model = "claude-opus-5-5"
effort = "xhigh"
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_parses_with_defaults() {
        let c = sample();
        assert_eq!(c.poll_seconds, 30);
        assert_eq!(c.claude.proxy_listen, "127.0.0.1:8471");
        assert_eq!(c.pool.five_hour_cap, 85.0);
        assert_eq!(c.pool.week_cap, 90.0);
        let repo = c.repo("EXAMPLE-TEAM/shop").unwrap();
        assert_eq!(repo.default_branch, "main");
        assert_eq!(repo.e2e.as_ref().unwrap().every_hours, 4);
        assert_eq!(c.github.bot_login(), "example-forge[bot]");
        assert!(c.github.is_bot("example-forge") && c.github.is_bot("Example-Forge[bot]"));
        assert!(!c.github.is_bot("bob-gh"));
        assert_eq!(c.member_by_email(" BOB@example.com").unwrap().github, "bob-gh");
    }

    #[test]
    fn role_comes_from_labels() {
        let c = sample();
        let repo = &c.repos[0];
        assert_eq!(repo.role_for(&["bug".into(), "ui".into()]), "ui");
        assert_eq!(repo.role_for(&["review".into()]), "backend");
        assert_eq!(repo.role_for(&[]), "backend");
    }

    #[test]
    fn rejects_bad_settings() {
        let no_review = SAMPLE.replace("[repos.roles.review]", "[repos.roles.other]");
        assert!(Config::parse(&no_review).is_err());
        let bad_effort = SAMPLE.replacen("effort = \"xhigh\"", "effort = \"huge\"", 1);
        assert!(Config::parse(&bad_effort).is_err());
        let outsider = SAMPLE.replace("orderer = \"bob-gh\"", "orderer = \"someone\"");
        assert!(Config::parse(&outsider).is_err());
        let inline_secret = SAMPLE.replace("[auth]", "[auth]\ntoken = \"x\"");
        assert!(Config::parse(&inline_secret).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn secret_files_must_be_private() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s");
        std::fs::write(&path, " abc \n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(read_secret(&path).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(read_secret(&path).unwrap(), "abc");
    }
}
