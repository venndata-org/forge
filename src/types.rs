//! Domain types every part of Forge shares. This file is the contract between
//! modules: change it only with every user of a type in view.

use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Where a task stands. Members set Ready, Accepted and Live on the GitHub
/// Project; Forge sets Building, Reviewed and OnMain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// Parked: Forge never picks it up until it moves to Ready.
    Backlog,
    Asked,
    Ready,
    Building,
    Reviewed,
    Accepted,
    OnMain,
    Live,
}

impl Status {
    pub const ALL: [Status; 8] = [
        Status::Backlog,
        Status::Asked,
        Status::Ready,
        Status::Building,
        Status::Reviewed,
        Status::Accepted,
        Status::OnMain,
        Status::Live,
    ];

    /// The option's name on the Project's Status field.
    pub fn project_name(self) -> &'static str {
        match self {
            Status::Backlog => "Backlog",
            Status::Asked => "Asked",
            Status::Ready => "Ready",
            Status::Building => "Building",
            Status::Reviewed => "Reviewed",
            Status::Accepted => "Accepted",
            Status::OnMain => "On main",
            Status::Live => "Live",
        }
    }

    /// Reads a Project option name, ignoring case and surrounding space.
    pub fn from_project_name(name: &str) -> Option<Status> {
        let name = name.trim();
        Status::ALL
            .into_iter()
            .find(|s| s.project_name().eq_ignore_ascii_case(name))
    }

    /// The name stored in SQLite and sent over the API.
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Backlog => "backlog",
            Status::Asked => "asked",
            Status::Ready => "ready",
            Status::Building => "building",
            Status::Reviewed => "reviewed",
            Status::Accepted => "accepted",
            Status::OnMain => "on_main",
            Status::Live => "live",
        }
    }

    pub fn parse(s: &str) -> Option<Status> {
        Status::ALL.into_iter().find(|x| x.as_str() == s)
    }
}

/// One issue on a repo's GitHub Project, in the Project's own order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectItem {
    /// The Project item's node id, which `set_status` takes.
    pub item_id: String,
    pub issue_number: u64,
    pub title: String,
    /// None when the item has no Status, or an option Forge doesn't know.
    pub status: Option<Status>,
    pub labels: Vec<String>,
    pub author: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Comment {
    pub id: u64,
    pub author: String,
    pub body: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IssueThread {
    pub number: u64,
    pub title: String,
    pub body: String,
    pub author: String,
    pub labels: Vec<String>,
    pub url: String,
    /// Oldest first.
    pub comments: Vec<Comment>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PullRequest {
    pub number: u64,
    pub url: String,
    pub head_sha: String,
}

/// Why a session burst runs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BurstReason {
    /// The task just moved to Building.
    Start,
    /// Someone answered the session's question on the issue.
    Answer,
    /// The review run asked for changes.
    ReviewNotes { notes: String },
    /// The repo's merge check failed after rebasing on current main.
    MergeCheckFailed { output: String },
    /// Rebasing on current main hit conflicts.
    RebaseConflict { output: String },
    /// A person stepped into the session with `forge attach` and left.
    AfterAttach,
    /// A fresh review session for the task's PR.
    Review,
}

impl BurstReason {
    pub fn kind(&self) -> &'static str {
        match self {
            BurstReason::Start => "start",
            BurstReason::Answer => "answer",
            BurstReason::ReviewNotes { .. } => "review_notes",
            BurstReason::MergeCheckFailed { .. } => "merge_check_failed",
            BurstReason::RebaseConflict { .. } => "rebase_conflict",
            BurstReason::AfterAttach => "after_attach",
            BurstReason::Review => "review",
        }
    }
}

/// Everything a runner needs to run one burst of a Claude session.
#[derive(Debug, Clone, PartialEq)]
pub struct BurstRequest {
    pub repo: String,
    pub issue: u64,
    pub role: String,
    /// The session to resume; None starts a fresh one.
    pub resume: Option<String>,
    pub worktree: PathBuf,
    pub prompt: String,
    pub model: String,
    pub effort: String,
    /// The per-burst key the session uses with Forge's local Claude proxy.
    pub session_key: String,
}

/// What a burst ended in, read from the session's final ```forge block.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum Outcome {
    /// Work is committed on the branch and ready for a PR.
    Pr { title: String, body: String },
    /// The session needs a person to answer before it can go on.
    Question { text: String },
    /// The session can't go on, and says why.
    Blocked { reason: String },
    /// A review session's verdict.
    Review { pass: bool, notes: String },
}

impl Outcome {
    pub fn kind(&self) -> &'static str {
        match self {
            Outcome::Pr { .. } => "pr",
            Outcome::Question { .. } => "question",
            Outcome::Blocked { .. } => "blocked",
            Outcome::Review { .. } => "review",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BurstResult {
    /// The Claude session id, to resume next time.
    pub session_id: Option<String>,
    pub outcome: Outcome,
    pub cost_usd: Option<f64>,
    /// Claude reported an error, or the process failed or timed out.
    pub is_error: bool,
}

/// A task as Forge keeps it in SQLite. GitHub stays the source of truth for
/// the issue and its Project status; this holds what GitHub can't.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Task {
    pub repo: String,
    pub issue: u64,
    pub title: String,
    pub author: String,
    pub status: Status,
    pub role: String,
    /// The build session, resumed on every burst.
    pub session_id: Option<String>,
    /// Always `forge/<issue>`.
    pub branch: String,
    pub pr_number: Option<u64>,
    pub pr_url: Option<String>,
    /// The newest issue comment the session has seen.
    pub last_seen_comment: u64,
    /// The GitHub login a question or block waits on.
    pub waiting_on: Option<String>,
    /// The GitHub login stepped into the session right now.
    pub attached_by: Option<String>,
    /// A burst owed to this task, run on the next tick it can.
    pub pending: Option<BurstReason>,
    pub review_rounds: u32,
    pub bursts: u32,
    pub updated_at: DateTime<Utc>,
}

impl Task {
    pub fn new(repo: &str, issue: u64, title: &str, author: &str, status: Status, role: &str) -> Task {
        Task {
            repo: repo.to_string(),
            issue,
            title: title.to_string(),
            author: author.to_string(),
            status,
            role: role.to_string(),
            session_id: None,
            branch: branch_for(issue),
            pr_number: None,
            pr_url: None,
            last_seen_comment: 0,
            waiting_on: None,
            attached_by: None,
            pending: None,
            review_rounds: 0,
            bursts: 0,
            updated_at: Utc::now(),
        }
    }
}

pub fn branch_for(issue: u64) -> String {
    format!("forge/{issue}")
}

/// One burst, for the record and the dashboard.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BurstLog {
    pub repo: String,
    pub issue: u64,
    pub role: String,
    /// `BurstReason::kind`.
    pub reason: String,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
    /// `Outcome::kind`, or "error".
    pub outcome: String,
    pub session_id: Option<String>,
    pub cost_usd: Option<f64>,
}

/// A plan's limits as last read: percent used of its 5-hour and weekly windows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Limits {
    pub five_hour_pct: Option<f64>,
    pub five_hour_resets: Option<DateTime<Utc>>,
    pub week_pct: Option<f64>,
    pub week_resets: Option<DateTime<Utc>>,
    pub read_at: DateTime<Utc>,
}

/// A Claude plan as anyone may see it. Never carries its login.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanView {
    pub label: String,
    pub limits: Option<Limits>,
    pub at_ceiling: bool,
    pub out_until: Option<DateTime<Utc>>,
    pub out_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct E2eRun {
    pub repo: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub passed: Option<bool>,
    /// The issue Forge filed when it failed.
    pub issue: Option<u64>,
}

/// A shell command's result: exit and combined output, trimmed to a tail.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CmdOutput {
    pub ok: bool,
    pub code: Option<i32>,
    pub output: String,
}

// ---- Views the API serves and the CLI, MCP and dashboard read ----

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatusView {
    pub generated_at: DateTime<Utc>,
    pub repos: Vec<RepoView>,
    pub pool: Vec<PlanView>,
    /// What waits on the signed-in member.
    pub waiting_on_you: Vec<WaitingItem>,
    /// The signed-in member's GitHub login.
    #[serde(default)]
    pub you: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RepoView {
    pub repo: String,
    pub concurrency: u32,
    /// Who moves tasks to Ready.
    #[serde(default)]
    pub orderer: String,
    #[serde(default)]
    pub backlog: Vec<TaskLine>,
    #[serde(default)]
    pub asked: Vec<TaskLine>,
    #[serde(default)]
    pub ready: Vec<TaskLine>,
    pub building: Vec<TaskLine>,
    pub reviewed: Vec<TaskLine>,
    pub accepted: Vec<TaskLine>,
    /// The latest tasks merged, newest first, at most 10.
    pub on_main: Vec<TaskLine>,
    pub last_e2e: Option<E2eRun>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskLine {
    pub issue: u64,
    pub title: String,
    pub status: Status,
    pub role: String,
    pub waiting_on: Option<String>,
    pub attached_by: Option<String>,
    pub pr_url: Option<String>,
    pub url: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WaitingItem {
    pub repo: String,
    pub issue: u64,
    pub title: String,
    /// "question", "blocked" or "verify".
    pub why: String,
    pub detail: String,
    pub url: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskView {
    pub repo: String,
    pub issue: u64,
    pub title: String,
    pub status: Status,
    pub branch: String,
    pub pr_url: Option<String>,
    pub attached_by: Option<String>,
    pub try_commands: TryCommands,
}

/// The repo's own commands `forge try` runs on a laptop.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TryCommands {
    pub build: Option<String>,
    pub seed: Option<String>,
    pub run: Option<String>,
    /// Stops what `run` started (`forge stop`).
    #[serde(default)]
    pub stop: Option<String>,
}

/// What `forge attach-gate` gets from the daemon to step into a session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AttachGrant {
    pub session_id: String,
    pub worktree: PathBuf,
    pub model: String,
    pub proxy_url: String,
    pub session_key: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_names_round_trip() {
        for s in Status::ALL {
            assert_eq!(Status::from_project_name(s.project_name()), Some(s));
            assert_eq!(Status::parse(s.as_str()), Some(s));
        }
        assert_eq!(Status::from_project_name("  on MAIN "), Some(Status::OnMain));
        assert_eq!(Status::from_project_name("Todo"), None);
    }

    #[test]
    fn outcome_reads_its_tag() {
        let o: Outcome = serde_json::from_str(r#"{"outcome":"question","text":"Which table?"}"#).unwrap();
        assert_eq!(o, Outcome::Question { text: "Which table?".into() });
        let r: BurstReason = serde_json::from_str(r#"{"kind":"review_notes","notes":"x"}"#).unwrap();
        assert_eq!(r.kind(), "review_notes");
    }
}
