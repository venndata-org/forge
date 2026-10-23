//! The loop: read each repo's Project, start Ready tasks up to the repo's
//! concurrency, resume sessions when someone answers, review PRs, and land
//! Accepted tasks on main one at a time. Also the 4-hourly e2e run.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use tracing::{info, warn};

use crate::config::{Config, E2eConfig, RepoConfig, RoleConfig, REVIEW_ROLE};
use crate::db::Db;
use crate::github::{GitHubApi, GitHubError};
use crate::gitops::{GitOps, Landing};
use crate::notify::Notifier;
use crate::proxy::SessionKeys;
use crate::session::Runner;
use crate::status::issue_url;
use crate::types::{BurstLog, BurstReason, BurstRequest, BurstResult, CmdOutput, IssueThread, Outcome, ProjectItem, Status, Task};

use crate::session::{build_prompt, review_prompt};

/// Failed review runs in a row before the task is blocked on a person.
const MAX_REVIEW_ROUNDS: u32 = 3;
/// Tries at merging a PR while GitHub isn't ready to merge it yet.
const MERGE_TRIES: u32 = 5;
/// The wait before the first retry of a merge; it doubles each time.
const MERGE_BACKOFF: Duration = Duration::from_secs(2);
/// How long an attach lasts before Forge ends it, in case its person left
/// without detaching.
const ATTACH_LIMIT: chrono::Duration = chrono::Duration::hours(8);
/// The merge check runs inside the tick, so it must be quick.
const MERGE_CHECK_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const E2E_TIMEOUT: Duration = Duration::from_secs(60 * 60);
const E2E_CHECK_EVERY: Duration = Duration::from_secs(60);
/// How much of a failed e2e run's output its issue quotes.
const E2E_TAIL_LINES: usize = 200;
/// The last sha Forge pushed for review, and the last whose review passed;
/// a task lands only while they are the same.
const PUSHED: &str = "pushed";
const REVIEWED: &str = "reviewed";

pub struct Engine {
    pub cfg: Arc<Config>,
    pub db: Arc<Db>,
    pub gh: Arc<dyn GitHubApi>,
    pub runner: Arc<dyn Runner>,
    pub git: Arc<dyn GitOps>,
    pub notify: Arc<dyn Notifier>,
    pub keys: SessionKeys,
    /// (repo, issue) of every burst and landing running now.
    in_flight: Busy,
    /// `MERGE_BACKOFF`, shorter in tests.
    merge_backoff: Duration,
}

/// The tasks Forge is working in right now, shared with the API so nobody
/// attaches to a session while a burst runs in it.
#[derive(Clone, Default)]
pub struct Busy(Arc<Mutex<HashSet<(String, u64)>>>);

impl Busy {
    pub fn contains(&self, repo: &str, issue: u64) -> bool {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).contains(&(repo.to_string(), issue))
    }

    /// Marks a task busy by hand, for other modules' tests.
    #[cfg(test)]
    pub fn hold(&self, repo: &str, issue: u64) {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).insert((repo.to_string(), issue));
    }
}

/// A task held in flight; dropping it lets the task go, however the work ended.
struct Flight {
    engine: Arc<Engine>,
    key: (String, u64),
}

impl Drop for Flight {
    fn drop(&mut self) {
        self.engine.flights().remove(&self.key);
    }
}

/// A burst's proxy key, revoked when dropped, however the burst ended.
struct LiveKey<'a> {
    keys: &'a SessionKeys,
    key: String,
}

impl<'a> LiveKey<'a> {
    fn issue(keys: &'a SessionKeys, tag: &str) -> LiveKey<'a> {
        LiveKey { keys, key: keys.issue(tag) }
    }
}

impl Drop for LiveKey<'_> {
    fn drop(&mut self) {
        self.keys.revoke(&self.key);
    }
}

/// What a build burst's outcome is acted on with.
struct Job<'a> {
    repo: &'a RepoConfig,
    item_id: &'a str,
    task: Task,
    /// The thread as the session was shown it.
    thread: IssueThread,
}

impl Engine {
    pub fn new(
        cfg: Arc<Config>,
        db: Arc<Db>,
        gh: Arc<dyn GitHubApi>,
        runner: Arc<dyn Runner>,
        git: Arc<dyn GitOps>,
        notify: Arc<dyn Notifier>,
        keys: SessionKeys,
    ) -> Engine {
        Engine { cfg, db, gh, runner, git, notify, keys, in_flight: Busy::default(), merge_backoff: MERGE_BACKOFF }
    }

    /// One pass over every repo. Bursts are spawned, not awaited.
    pub async fn tick(self: &Arc<Self>) -> Result<()> {
        let mut failed = Vec::new();
        for repo in &self.cfg.repos {
            if let Err(e) = self.tick_repo(repo).await {
                failed.push(format!("{}: {e:#}", repo.name));
            }
        }
        if !failed.is_empty() {
            bail!(failed.join("; "));
        }
        Ok(())
    }

    /// Runs any repo's e2e command that is due.
    pub async fn e2e_tick(self: &Arc<Self>) -> Result<()> {
        for repo in &self.cfg.repos {
            let Some(e2e) = &repo.e2e else { continue };
            let every = chrono::Duration::hours(e2e.every_hours as i64);
            if self.db.last_e2e(&repo.name)?.is_none_or(|run| Utc::now() - run.started_at >= every) {
                self.run_e2e(repo, e2e).await?;
            }
        }
        Ok(())
    }

    /// Ticks every `poll_seconds` and checks e2e every minute until `stop`.
    pub async fn run(self: Arc<Self>, mut stop: tokio::sync::watch::Receiver<bool>) {
        let mut ticks = every(Duration::from_secs(self.cfg.poll_seconds));
        let mut e2e = every(E2E_CHECK_EVERY);
        while !*stop.borrow() {
            tokio::select! {
                _ = ticks.tick() => {
                    if let Err(e) = self.tick().await {
                        warn!("tick failed: {e:#}");
                    }
                }
                _ = e2e.tick() => {
                    // A run takes up to an hour; it records its start, so it isn't due twice.
                    let engine = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = engine.e2e_tick().await {
                            warn!("e2e check failed: {e:#}");
                        }
                    });
                }
                changed = stop.changed() => if changed.is_err() { break },
            }
        }
        info!("loop stopped");
    }

    /// Waits until no burst is running (tests).
    pub async fn wait_idle(&self) {
        while !self.flights().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    // ---- The tick ----

    /// Sync, answers and owed bursts, starts, then at most one landing.
    /// Concurrency 0 pauses the repo: it is synced and nothing else.
    async fn tick_repo(self: &Arc<Self>, repo: &RepoConfig) -> Result<()> {
        let read_at = Utc::now();
        let items = self.gh.project_items(repo).await.context("reading the Project")?;
        let mut tasks = Vec::with_capacity(items.len());
        for item in items {
            let task = self.sync(repo, &item, read_at)?;
            tasks.push((item, task));
        }
        if repo.concurrency == 0 {
            return Ok(());
        }
        for (item, task) in &tasks {
            if task.status == Status::Building {
                if let Err(e) = self.resume(repo, &item.item_id, task).await {
                    warn!(task = %tag(task), "can't resume: {e:#}");
                }
            }
        }
        let started = self.start(repo, &tasks).await;
        let landed = self.land_next(repo, &tasks).await;
        started.and(landed)
    }

    /// Brings the item's task in SQLite in line with the Project, creating it
    /// if new. A task moved back to Ready, Asked or Backlog stops being owed bursts.
    /// A task changed in SQLite after `read_at` keeps its status, since the
    /// Project's may predate the change. A stale attach is ended.
    fn sync(&self, repo: &RepoConfig, item: &ProjectItem, read_at: DateTime<Utc>) -> Result<Task> {
        let status = item.status.unwrap_or(Status::Asked);
        let role = repo.role_for(&item.labels);
        let Some(old) = self.db.task(&repo.name, item.issue_number)? else {
            let task = Task::new(&repo.name, item.issue_number, &item.title, &item.author, status, &role);
            self.db.upsert_task(&task)?;
            info!(task = %tag(&task), "new task, {}", status.project_name());
            return Ok(task);
        };
        let status = if old.updated_at > read_at { old.status } else { status };
        let mut task = Task { title: item.title.clone(), author: item.author.clone(), status, role, ..old.clone() };
        if matches!(status, Status::Ready | Status::Asked | Status::Backlog) {
            (task.pending, task.waiting_on) = (None, None);
        }
        self.end_stale_attach(&mut task)?;
        if task != old {
            if task.status != old.status {
                info!(task = %tag(&task), "moved from {} to {}", old.status.project_name(), status.project_name());
            }
            task.updated_at = Utc::now();
            self.db.upsert_task(&task)?;
        }
        Ok(task)
    }

    /// Detaches the task if its attach is older than ATTACH_LIMIT, or has no
    /// time on record (`attach:<repo>#<issue>` in kv, written by the API).
    fn end_stale_attach(&self, task: &mut Task) -> Result<()> {
        let Some(who) = task.attached_by.clone() else { return Ok(()) };
        let key = format!("attach:{}", tag(task));
        let since = self.db.kv_get(&key)?.and_then(|t| DateTime::parse_from_rfc3339(&t).ok());
        if since.is_some_and(|t| Utc::now() - t.with_timezone(&Utc) < ATTACH_LIMIT) {
            return Ok(());
        }
        warn!(task = %tag(task), "ending {who}'s attach: it is stale");
        self.keys.revoke_tagged(&format!("{} attach ", tag(task)));
        self.db.detach(&task.repo, task.issue)?;
        self.db.kv_set(&key, "")?;
        task.attached_by = None;
        Ok(())
    }

    /// Runs a Building task's owed burst, when the repo has room. A task owing
    /// nothing is owed one if a person answered it or, waiting on nobody (moved
    /// back to Building, or left there by a crash), one that resumes its
    /// session or starts it.
    async fn resume(self: &Arc<Self>, repo: &RepoConfig, item_id: &str, task: &Task) -> Result<()> {
        let Some(flight) = self.claim_burst(repo, task) else { return Ok(()) };
        // Read under the claim: a burst that just ended may have changed the task.
        let Some(mut task) = self.db.task(&task.repo, task.issue)? else { return Ok(()) };
        if task.status != Status::Building || task.attached_by.is_some() {
            return Ok(());
        }
        if task.pending.is_none() {
            let owed = match &task.waiting_on {
                Some(_) => self.answered(&task).await?.then_some(BurstReason::Answer),
                None if task.session_id.is_some() => Some(BurstReason::Answer),
                None => Some(BurstReason::Start),
            };
            if let Some(reason) = owed {
                info!(task = %tag(&task), reason = reason.kind(), "owed a burst");
                task = self.update(&task, |t| {
                    t.waiting_on = None;
                    t.pending = Some(reason);
                })?;
            }
        }
        if let Some(reason) = task.pending.clone() {
            self.spawn(flight, repo, item_id, task, reason);
        }
        Ok(())
    }

    /// Whether anyone but Forge has commented since the session last looked.
    async fn answered(&self, task: &Task) -> Result<bool> {
        let thread = self.gh.issue_thread(&task.repo, task.issue).await?;
        Ok(thread.comments.iter().any(|c| c.id > task.last_seen_comment && !self.cfg.github.is_bot(&c.author)))
    }

    /// Starts Ready tasks, in Project order, while the repo has fewer than
    /// `concurrency` tasks Building and room for another burst.
    async fn start(self: &Arc<Self>, repo: &RepoConfig, tasks: &[(ProjectItem, Task)]) -> Result<()> {
        let mut building = tasks.iter().filter(|(_, t)| t.status == Status::Building).count();
        for (item, task) in tasks {
            if building >= repo.concurrency as usize {
                break;
            }
            if task.status != Status::Ready || task.attached_by.is_some() {
                continue;
            }
            let Some(flight) = self.claim_burst(repo, task) else { continue };
            let Some(task) = self.fresh(task, Status::Ready)? else { continue };
            let task = self
                .move_to(repo, &item.item_id, &task, Status::Building, |t| {
                    t.pending = Some(BurstReason::Start);
                    t.waiting_on = None;
                    t.review_rounds = 0;
                })
                .await?;
            self.spawn(flight, repo, &item.item_id, task, BurstReason::Start);
            building += 1;
        }
        Ok(())
    }

    /// Runs a burst in its own tokio task, which holds the flight until it ends.
    fn spawn(self: &Arc<Self>, flight: Flight, repo: &RepoConfig, item_id: &str, task: Task, reason: BurstReason) {
        let (engine, repo, item_id) = (self.clone(), repo.clone(), item_id.to_string());
        tokio::spawn(async move {
            let _flight = flight;
            engine.burst(&repo, &item_id, task, reason).await;
        });
    }

    // ---- Bursts ----

    /// One build burst: the session, then whatever its outcome asks for.
    /// Until a session has run, a failure leaves the burst owed for the next tick.
    async fn burst(&self, repo: &RepoConfig, item_id: &str, task: Task, reason: BurstReason) {
        let (role, role_prompt) = match role(repo, &task.role) {
            Ok(found) => found,
            Err(e) => return self.block_or_log(repo, &task, &format!("{e:#}")).await,
        };
        let (worktree, thread) = match self.workspace(repo, &task).await {
            Ok(found) => found,
            Err(e) => {
                warn!(task = %tag(&task), "can't run its burst yet: {e:#}");
                return;
            }
        };
        let prompt = build_prompt(repo, &role_prompt, &thread, &reason, &task);
        let req = request(&task, &task.role, role, &worktree, prompt, task.session_id.clone());
        let result = self.session(&task, req, &reason).await;
        let job = Job { repo, item_id, task, thread };
        if let Err(e) = self.settle(&job, &result).await {
            self.block_or_log(repo, &job.task, &format!("{e:#}")).await;
        }
    }

    /// The task's worktree, made on first use, and its issue thread.
    async fn workspace(&self, repo: &RepoConfig, task: &Task) -> Result<(PathBuf, IssueThread)> {
        let token = self.gh.git_token().await.context("getting a git token")?;
        let worktree = self.git.worktree(repo, &task.branch, &token).await.context("making the worktree")?;
        let thread = self.gh.issue_thread(&repo.name, task.issue).await.context("reading the issue")?;
        Ok((worktree, thread))
    }

    /// Runs one session burst under a fresh proxy key, then counts and logs it.
    async fn session(&self, task: &Task, mut req: BurstRequest, reason: &BurstReason) -> BurstResult {
        let key = LiveKey::issue(&self.keys, &tag(task));
        req.session_key = key.key.clone();
        let started_at = Utc::now();
        let role = req.role.clone();
        info!(task = %tag(task), role = %role, reason = reason.kind(), "burst started");
        let result = self.runner.burst(req).await.unwrap_or_else(|e| BurstResult {
            session_id: None,
            outcome: Outcome::Blocked { reason: format!("the session failed to run: {e:#}") },
            cost_usd: None,
            is_error: true,
        });
        drop(key);
        let outcome = if result.is_error { "error" } else { result.outcome.kind() };
        info!(task = %tag(task), role = %role, outcome, "burst ended");
        let log = BurstLog {
            repo: task.repo.clone(),
            issue: task.issue,
            role,
            reason: reason.kind().to_string(),
            started_at,
            ended_at: Utc::now(),
            outcome: outcome.to_string(),
            session_id: result.session_id.clone(),
            cost_usd: result.cost_usd,
        };
        if let Err(e) = self.db.record_burst(&log).and_then(|()| self.update(task, |t| t.bursts += 1)) {
            warn!(task = %tag(task), "can't record the burst: {e:#}");
        }
        result
    }

    /// Saves what the build session left, then acts on its outcome.
    async fn settle(&self, job: &Job<'_>, result: &BurstResult) -> Result<()> {
        let newest = job.thread.comments.iter().map(|c| c.id).max().unwrap_or(0);
        self.update(&job.task, |t| {
            if let Some(id) = &result.session_id {
                t.session_id = Some(id.clone());
            }
            t.last_seen_comment = t.last_seen_comment.max(newest);
            t.waiting_on = None;
        })?;
        match verdict(result) {
            Outcome::Pr { title, body } => self.open_pr(job, &title, &body).await,
            Outcome::Question { text } => self.ask(job.repo, &job.task, &text).await,
            Outcome::Blocked { reason } => self.block(job.repo, &job.task, &reason).await,
            Outcome::Review { .. } => self.block(job.repo, &job.task, "the build session ended with a review, not a PR").await,
        }
    }

    /// Pushes the branch, opens or updates its PR, and has it reviewed.
    async fn open_pr(&self, job: &Job<'_>, title: &str, body: &str) -> Result<()> {
        let (repo, task) = (job.repo, &job.task);
        let token = self.gh.git_token().await.context("getting a git token")?;
        let ahead = self.git.collect(repo, &task.branch, &token).await.context("taking the session's commits")?;
        if ahead == 0 {
            return self.block(repo, task, "no commits").await;
        }
        self.db.kv_set(&sha_key(PUSHED, task), "")?;
        let sha = self.git.push(repo, &task.branch, &token).await.context("pushing the branch")?;
        self.db.kv_set(&sha_key(PUSHED, task), &sha)?;
        let body = format!("{body}\n\nCloses #{}", task.issue);
        let pr = self
            .gh
            .open_or_update_pr(&repo.name, &task.branch, &repo.default_branch, title, &body)
            .await
            .context("opening the PR")?;
        info!(task = %tag(task), pr = %pr.url, "PR open");
        self.update(task, |t| {
            t.pr_number = Some(pr.number);
            t.pr_url = Some(pr.url.clone());
        })?;
        self.review(job, &sha, &token).await
    }

    /// Runs a fresh review session on the pushed commit, in a fresh checkout
    /// of it that is removed after, and acts on its verdict.
    async fn review(&self, job: &Job<'_>, sha: &str, token: &str) -> Result<()> {
        let (repo, task) = (job.repo, &job.task);
        let (role, role_prompt) = role(repo, REVIEW_ROLE)?;
        let checkout = self.git.review_checkout(repo, &task.branch, token).await.context("making the review checkout")?;
        let prompt = review_prompt(repo, &role_prompt, &job.thread, sha);
        let req = request(task, REVIEW_ROLE, role, &checkout, prompt, None);
        let result = self.session(task, req, &BurstReason::Review).await;
        if let Err(e) = self.git.remove_review_checkout(repo, &task.branch).await {
            warn!(task = %tag(task), "can't remove the review checkout: {e:#}");
        }
        match verdict(&result) {
            review @ Outcome::Review { pass: true, .. } => self.passed(job, sha, &review).await,
            Outcome::Review { pass: false, notes, .. } => self.failed_review(job, notes).await,
            Outcome::Blocked { reason } => bail!("the review run failed: {reason}"),
            other => bail!("the review run ended with a {} block, not a verdict", other.kind()),
        }
    }

    /// The review of `sha` passed: the task waits for a person to verify it.
    async fn passed(&self, job: &Job<'_>, sha: &str, review: &Outcome) -> Result<()> {
        let (repo, task, n) = (job.repo, &job.task, job.task.issue);
        self.db.kv_set(&sha_key(REVIEWED, task), sha)?;
        self.move_to(repo, job.item_id, task, Status::Reviewed, |t| {
            t.review_rounds = 0;
            t.pending = None;
        })
        .await?;
        let head = format!("@{} ready to verify: `forge try {n}`", self.person(repo, task).await);
        self.comment(task, &verify_comment(&head, review)).await;
        self.ping(&format!("{} ready to verify: forge try {n} {}", tag(task), issue_url(&task.repo, n))).await;
        Ok(())
    }

    /// The review asked for changes: owe a burst with its notes, or block on
    /// a person once it has failed MAX_REVIEW_ROUNDS times in a row.
    async fn failed_review(&self, job: &Job<'_>, notes: String) -> Result<()> {
        let task = self.update(&job.task, |t| {
            t.review_rounds += 1;
            t.pending = Some(BurstReason::ReviewNotes { notes: notes.clone() });
        })?;
        info!(task = %tag(&task), round = task.review_rounds, "review asked for changes");
        if task.review_rounds < MAX_REVIEW_ROUNDS {
            return Ok(());
        }
        // A person steps in now; once they answer, the rounds start over.
        self.update(&task, |t| t.review_rounds = 0)?;
        let head = format!("review failed {MAX_REVIEW_ROUNDS} times");
        let reason = match folded("The last review's notes", &notes) {
            Some(notes) => format!("{head}\n\n{notes}"),
            None => head,
        };
        self.block(job.repo, &task, &reason).await
    }

    // ---- Landing ----

    /// Lands the first Accepted task with a PR, in Project order, if what
    /// Forge last pushed for it passed review; if not, it goes back to
    /// Building for the burst it owes. A failure sends it back to Building,
    /// blocked on a person, so the queue moves on.
    async fn land_next(self: &Arc<Self>, repo: &RepoConfig, tasks: &[(ProjectItem, Task)]) -> Result<()> {
        let next = tasks.iter().find(|(_, t)| {
            t.status == Status::Accepted && t.pr_number.is_some() && t.attached_by.is_none() && !self.in_flight(t)
        });
        let Some((item, task)) = next else { return Ok(()) };
        let Some(_flight) = self.claim(task) else { return Ok(()) };
        let Some(task) = self.fresh(task, Status::Accepted)? else { return Ok(()) };
        if !self.reviewed_is_pushed(&task)? {
            warn!(task = %tag(&task), "accepted, but what Forge last pushed hasn't passed review");
            self.move_to(repo, &item.item_id, &task, Status::Building, |_| {}).await?;
            return Ok(());
        }
        let Err(e) = self.land(repo, &item.item_id, &task).await else { return Ok(()) };
        warn!(task = %tag(&task), "landing failed: {e:#}");
        self.move_to(repo, &item.item_id, &task, Status::Building, |_| {}).await?;
        self.block(repo, &task, &format!("couldn't land it: {e:#}")).await
    }

    /// Rebases what was pushed on main, runs the merge check, pushes the
    /// result and squash-merges the PR with it.
    async fn land(&self, repo: &RepoConfig, item_id: &str, task: &Task) -> Result<()> {
        let pr = task.pr_number.context("it has no PR")?;
        let token = self.gh.git_token().await.context("getting a git token")?;
        let check = repo.merge_check.as_deref();
        let landing = self
            .git
            .land(repo, &task.branch, check, &[], MERGE_CHECK_TIMEOUT, &token)
            .await
            .context("rebasing on main")?;
        let sha = match landing {
            Landing::Conflict(output) => return self.owe(repo, item_id, task, BurstReason::RebaseConflict { output }).await,
            Landing::CheckFailed(output) => {
                return self.owe(repo, item_id, task, BurstReason::MergeCheckFailed { output }).await
            }
            Landing::Ready { sha } => sha,
        };
        for what in [PUSHED, REVIEWED] {
            self.db.kv_set(&sha_key(what, task), &sha)?;
        }
        let title = format!("{} (#{pr})", task.title);
        self.merge(&repo.name, pr, &sha, &title).await.context("merging the PR")?;
        self.landed(repo, item_id, task).await;
        Ok(())
    }

    /// Squash-merges the PR, trying again while GitHub answers 405 or 409
    /// because it hasn't caught up with the push that came just before.
    async fn merge(&self, repo: &str, pr: u64, sha: &str, title: &str) -> Result<()> {
        let mut wait = self.merge_backoff;
        for _ in 1..MERGE_TRIES {
            match self.gh.merge_pr(repo, pr, sha, title).await {
                Err(e) if not_ready_to_merge(&e) => {
                    info!(repo, pr, "GitHub isn't ready to merge, trying again in {wait:?}: {e:#}");
                    tokio::time::sleep(wait).await;
                    wait *= 2;
                }
                merged => return merged,
            }
        }
        self.gh.merge_pr(repo, pr, sha, title).await
    }

    /// Sends a task back to Building with a burst owed for what went wrong.
    async fn owe(&self, repo: &RepoConfig, item_id: &str, task: &Task, reason: BurstReason) -> Result<()> {
        info!(task = %tag(task), reason = reason.kind(), "can't land yet");
        self.move_to(repo, item_id, task, Status::Building, |t| {
            t.pending = Some(reason);
            t.waiting_on = None;
        })
        .await?;
        Ok(())
    }

    /// Wraps up after a merge. Each step is best effort: the code is on main.
    async fn landed(&self, repo: &RepoConfig, item_id: &str, task: &Task) {
        info!(task = %tag(task), "merged to main");
        if let Err(e) = self.move_to(repo, item_id, task, Status::OnMain, |t| t.pending = None).await {
            warn!(task = %tag(task), "can't mark it On main: {e:#}");
        }
        self.comment(task, "Merged to main.").await;
        self.runner.finish(&task.repo, task.issue).await;
        if let Err(e) = self.git.remove_worktree(repo, &task.branch).await {
            warn!(task = %tag(task), "can't remove its worktree: {e:#}");
        }
        self.ping(&format!("{} merged to main", tag(task))).await;
    }

    // ---- E2E ----

    /// Runs the e2e command on a fresh main checkout against staging, and
    /// files an issue when it fails.
    async fn run_e2e(&self, repo: &RepoConfig, e2e: &E2eConfig) -> Result<()> {
        let started = Utc::now();
        let id = self.db.start_e2e(&repo.name, started)?;
        info!(repo = %repo.name, "e2e started");
        let output = match self.e2e_output(repo, e2e).await {
            Ok(out) if out.ok => {
                info!(repo = %repo.name, "e2e passed");
                return self.db.finish_e2e(id, Utc::now(), true, None);
            }
            Ok(out) => out.output,
            Err(e) => format!("{e:#}"),
        };
        warn!(repo = %repo.name, "e2e failed on staging");
        let issue = self.file_e2e_failure(repo, e2e, started, &output).await;
        self.db.finish_e2e(id, Utc::now(), false, issue)
    }

    async fn e2e_output(&self, repo: &RepoConfig, e2e: &E2eConfig) -> Result<CmdOutput> {
        let token = self.gh.git_token().await.context("getting a git token")?;
        let env = [("STAGING_URL".to_string(), e2e.staging_url.clone())];
        self.git.run_on_main(repo, &e2e.command, &env, E2E_TIMEOUT, &token).await
    }

    /// Opens an issue for a failed run and pings; the issue's number, if it opened.
    async fn file_e2e_failure(&self, repo: &RepoConfig, e2e: &E2eConfig, started: DateTime<Utc>, output: &str) -> Option<u64> {
        let title = format!("E2E failed on staging, {}", started.format("%Y-%m-%d %H:%M UTC"));
        let body = format!(
            "`{}` failed against {}.\n\n```\n{}\n```\n",
            e2e.command,
            e2e.staging_url,
            tail(output, E2E_TAIL_LINES)
        );
        match self.gh.create_issue(&repo.name, &title, &body, &["e2e".to_string()]).await {
            Ok(n) => {
                self.ping(&format!("E2E failed on staging for {}: {}", repo.name, issue_url(&repo.name, n))).await;
                Some(n)
            }
            Err(e) => {
                warn!(repo = %repo.name, "can't file the e2e failure: {e:#}");
                self.ping(&format!("E2E failed on staging for {} (no issue filed)", repo.name)).await;
                None
            }
        }
    }

    // ---- Small parts ----

    /// Puts a question to the task's person on the issue and waits for an answer.
    async fn ask(&self, repo: &RepoConfig, task: &Task, text: &str) -> Result<()> {
        let who = self.person(repo, task).await;
        self.update(task, |t| {
            t.waiting_on = Some(who.clone());
            t.pending = None;
        })?;
        info!(task = %tag(task), waiting_on = %who, "asked: {}", first_line(text));
        if let Some(id) = self.comment(task, &format!("@{who} {text}")).await {
            self.update(task, |t| t.last_seen_comment = t.last_seen_comment.max(id))?;
        }
        let url = issue_url(&task.repo, task.issue);
        self.ping(&format!("{} needs {who}: {} {url}", tag(task), first_line(text))).await;
        Ok(())
    }

    async fn block(&self, repo: &RepoConfig, task: &Task, reason: &str) -> Result<()> {
        self.ask(repo, task, &format!("Blocked: {reason}")).await
    }

    /// Blocks a task whose burst went wrong; if even that fails, logs it.
    async fn block_or_log(&self, repo: &RepoConfig, task: &Task, reason: &str) {
        if let Err(e) = self.block(repo, task, reason).await {
            warn!(task = %tag(task), "can't block it ({reason}): {e:#}");
        }
    }

    /// Who a task's questions go to: its author; on an issue Forge filed, the
    /// member who asked for it through Forge, or else the orderer.
    async fn person(&self, repo: &RepoConfig, task: &Task) -> String {
        if !self.cfg.github.is_bot(&task.author) {
            return task.author.clone();
        }
        let body = self.gh.issue_thread(&task.repo, task.issue).await.map(|t| t.body).unwrap_or_default();
        asker(repo, &body).unwrap_or(&repo.orderer).to_string()
    }

    /// Posts on the task's issue. A failure is logged, not raised: the state
    /// the comment reports is saved already.
    async fn comment(&self, task: &Task, body: &str) -> Option<u64> {
        match self.gh.comment(&task.repo, task.issue, body).await {
            Ok(id) => Some(id),
            Err(e) => {
                warn!(task = %tag(task), "can't comment: {e:#}");
                None
            }
        }
    }

    async fn ping(&self, text: &str) {
        if let Err(e) = self.notify.ping(text).await {
            warn!("ping failed: {e:#}");
        }
    }

    /// Sets the task's Project status and mirrors it, with `change`, in SQLite.
    async fn move_to(&self, repo: &RepoConfig, item_id: &str, task: &Task, status: Status, change: impl FnOnce(&mut Task)) -> Result<Task> {
        self.gh
            .set_status(repo, item_id, status)
            .await
            .with_context(|| format!("moving it to {}", status.project_name()))?;
        info!(task = %tag(task), "now {}", status.project_name());
        self.update(task, |t| {
            t.status = status;
            change(t);
        })
    }

    /// The task as SQLite has it now, if it is still `status` and nobody is attached.
    fn fresh(&self, task: &Task, status: Status) -> Result<Option<Task>> {
        let fresh = self.db.task(&task.repo, task.issue)?;
        Ok(fresh.filter(|t| t.status == status && t.attached_by.is_none()))
    }

    /// Changes the task as SQLite has it now, so others' changes aren't lost.
    fn update(&self, task: &Task, change: impl FnOnce(&mut Task)) -> Result<Task> {
        let mut fresh = self
            .db
            .task(&task.repo, task.issue)?
            .with_context(|| format!("{} isn't in the database", tag(task)))?;
        change(&mut fresh);
        fresh.updated_at = Utc::now();
        self.db.upsert_task(&fresh)?;
        Ok(fresh)
    }

    /// A handle on the tasks in flight, for the API.
    pub fn busy(&self) -> Busy {
        self.in_flight.clone()
    }

    fn flights(&self) -> MutexGuard<'_, HashSet<(String, u64)>> {
        self.in_flight.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Marks the task in flight; None when something already holds it.
    fn claim(self: &Arc<Self>, task: &Task) -> Option<Flight> {
        let key = (task.repo.clone(), task.issue);
        let claimed = self.flights().insert(key.clone());
        claimed.then(|| Flight { engine: self.clone(), key })
    }

    /// Claims the task for a burst; None when something already holds it or
    /// the repo has `concurrency` claims in flight, landings included.
    fn claim_burst(self: &Arc<Self>, repo: &RepoConfig, task: &Task) -> Option<Flight> {
        let key = (task.repo.clone(), task.issue);
        let mut flights = self.flights();
        let room = flights.iter().filter(|(r, _)| *r == repo.name).count() < repo.concurrency as usize;
        let claimed = room && flights.insert(key.clone());
        drop(flights);
        claimed.then(|| Flight { engine: self.clone(), key })
    }

    /// Whether the last commit Forge pushed for review is the one whose review passed.
    fn reviewed_is_pushed(&self, task: &Task) -> Result<bool> {
        let pushed = self.db.kv_get(&sha_key(PUSHED, task))?.unwrap_or_default();
        let reviewed = self.db.kv_get(&sha_key(REVIEWED, task))?;
        Ok(!pushed.is_empty() && reviewed.as_deref() == Some(pushed.as_str()))
    }

    fn in_flight(&self, task: &Task) -> bool {
        self.flights().contains(&(task.repo.clone(), task.issue))
    }
}

/// A role's settings and its prompt, read from the file at burst time.
fn role<'a>(repo: &'a RepoConfig, name: &str) -> Result<(&'a RoleConfig, String)> {
    let role = repo.roles.get(name).with_context(|| format!("{} has no {name:?} role", repo.name))?;
    let prompt = std::fs::read_to_string(&role.prompt_file)
        .with_context(|| format!("can't read the {name} role's prompt file {}", role.prompt_file.display()))?;
    Ok((role, prompt))
}

/// A burst request; `Engine::session` fills in its proxy key.
/// The member an issue asked for through Forge names: "_Asked by <name> through Forge._".
fn asker<'a>(repo: &'a RepoConfig, body: &str) -> Option<&'a str> {
    let name = body.rsplit_once("_Asked by ")?.1.split_once(" through Forge._")?.0;
    repo.members.iter().find(|m| m.name == name).map(|m| m.github.as_str())
}

fn request(task: &Task, role_name: &str, role: &RoleConfig, worktree: &Path, prompt: String, resume: Option<String>) -> BurstRequest {
    BurstRequest {
        repo: task.repo.clone(),
        issue: task.issue,
        role: role_name.to_string(),
        resume,
        worktree: worktree.to_path_buf(),
        prompt,
        model: role.model.clone(),
        effort: role.effort.clone(),
        session_key: String::new(),
    }
}

/// What a session said, with a run that failed read as Blocked.
fn verdict(result: &BurstResult) -> Outcome {
    if result.is_error && !matches!(result.outcome, Outcome::Blocked { .. }) {
        return Outcome::Blocked { reason: "the session ended in an error".into() };
    }
    result.outcome.clone()
}

/// GitHub's answer to a merge while it is still taking in the PR's new head.
fn not_ready_to_merge(e: &anyhow::Error) -> bool {
    e.downcast_ref::<GitHubError>().is_some_and(|g| matches!(g.status, 405 | 409))
}

/// An interval that, after a slow tick, waits a full period instead of catching up.
fn every(period: Duration) -> tokio::time::Interval {
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval
}

fn tag(task: &Task) -> String {
    format!("{}#{}", task.repo, task.issue)
}

/// The kv key holding a task's `PUSHED` or `REVIEWED` sha.
fn sha_key(what: &str, task: &Task) -> String {
    format!("{what}:{}", tag(task))
}

/// A ping's one line from a longer text.
fn first_line(text: &str) -> String {
    text.lines().next().unwrap_or("").trim().chars().take(200).collect()
}

/// The comment a passed review leaves: who verifies and how, then the
/// reviewer's fields in one layout, each item on one line, its notes folded away.
fn verify_comment(head: &str, review: &Outcome) -> String {
    let Outcome::Review { notes, summary, checked, needs_you, try_steps, .. } = review else { return head.to_string() };
    let line = |s: &String| s.split_whitespace().collect::<Vec<_>>().join(" ");
    let items = |list: &[String]| list.iter().map(line).filter(|s| !s.is_empty()).collect::<Vec<_>>();
    let mut parts = vec![head.to_string()];
    parts.extend(Some(line(summary)).filter(|s| !s.is_empty()));
    parts.extend(Some(line(checked)).filter(|s| !s.is_empty()).map(|s| format!("**Checked:** {s}")));
    let needs = items(needs_you);
    if !needs.is_empty() {
        parts.push(format!("**Needs you**\n{}", needs.iter().map(|s| format!("- {s}")).collect::<Vec<_>>().join("\n")));
    }
    let steps = items(try_steps);
    if !steps.is_empty() {
        let steps: Vec<String> = steps.iter().enumerate().map(|(i, s)| format!("{}. {s}", i + 1)).collect();
        parts.push(format!("**To try**\n{}", steps.join("\n")));
    }
    parts.extend(folded("Review notes", notes));
    parts.join("\n\n")
}

/// `text` folded under `summary`: detail people open only if they want it.
fn folded(summary: &str, text: &str) -> Option<String> {
    let text = text.trim();
    (!text.is_empty()).then(|| format!("<details><summary>{summary}</summary>\n\n{text}\n\n</details>"))
}

fn tail(text: &str, lines: usize) -> String {
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};

    use async_trait::async_trait;
    use tokio::sync::Semaphore;

    use super::*;
    use crate::config;
    use crate::notify::RecordingNotifier;
    use crate::types::{Comment, PullRequest};

    const REPO: &str = "example-team/shop";
    const BOT: &str = "example-forge[bot]";
    const URL: &str = "https://github.com/example-team/shop/issues/1";

    // ---- GitHub ----

    struct FakeGh(Mutex<Gh>, Semaphore);

    impl Default for FakeGh {
        fn default() -> FakeGh {
            FakeGh(Mutex::default(), Semaphore::new(1000))
        }
    }

    #[derive(Default)]
    struct Gh {
        items: Vec<ProjectItem>,
        comments: HashMap<u64, Vec<Comment>>,
        last_comment: u64,
        /// (head, body) of every PR opened or updated.
        prs: Vec<(String, String)>,
        /// (PR, sha)
        merges: Vec<(u64, String)>,
        /// Statuses Forge set: (issue, status).
        moves: Vec<(u64, Status)>,
        /// (title, body, labels)
        issues: Vec<(String, String, Vec<String>)>,
        reads: usize,
        fail_reads: bool,
        fail_merges: bool,
        /// Statuses GitHub answers the next merges with, before it merges.
        refusals: VecDeque<u16>,
        merge_tries: u32,
    }

    impl Gh {
        fn post(&mut self, issue: u64, author: &str, body: &str) -> u64 {
            self.last_comment += 1;
            let c = Comment { id: self.last_comment, author: author.into(), body: body.into(), created_at: Utc::now() };
            self.comments.entry(issue).or_default().push(c);
            self.last_comment
        }

        fn item(&mut self, issue: u64) -> &mut ProjectItem {
            self.items.iter_mut().find(|i| i.issue_number == issue).expect("no such item")
        }
    }

    impl FakeGh {
        fn gh(&self) -> MutexGuard<'_, Gh> {
            self.0.lock().unwrap()
        }

        fn add(&self, issue: u64, status: Status, labels: &[&str]) {
            self.gh().items.push(ProjectItem {
                item_id: format!("item-{issue}"),
                issue_number: issue,
                title: format!("Task {issue}"),
                status: Some(status),
                labels: labels.iter().map(|l| l.to_string()).collect(),
                author: "alice".into(),
            });
        }

        /// A person moves the item on the Project.
        fn drag(&self, issue: u64, status: Status) {
            self.gh().item(issue).status = Some(status);
        }

        fn say(&self, issue: u64, who: &str, body: &str) -> u64 {
            self.gh().post(issue, who, body)
        }

        fn bodies(&self, issue: u64) -> Vec<String> {
            self.gh().comments.get(&issue).into_iter().flatten().map(|c| c.body.clone()).collect()
        }
    }

    #[async_trait]
    impl GitHubApi for FakeGh {
        async fn project_items(&self, _: &RepoConfig) -> Result<Vec<ProjectItem>> {
            let items = {
                let mut gh = self.gh();
                gh.reads += 1;
                if gh.fail_reads {
                    bail!("GitHub is down");
                }
                gh.items.clone()
            };
            // Each read takes a permit before it answers; tests drain them to hold a read.
            self.1.acquire().await?.forget();
            Ok(items)
        }
        async fn set_status(&self, _: &RepoConfig, item_id: &str, status: Status) -> Result<()> {
            let mut gh = self.gh();
            let item = gh.items.iter_mut().find(|i| i.item_id == item_id).context("no such item")?;
            item.status = Some(status);
            let issue = item.issue_number;
            gh.moves.push((issue, status));
            Ok(())
        }
        async fn issue_thread(&self, repo: &str, number: u64) -> Result<IssueThread> {
            let mut gh = self.gh();
            let comments = gh.comments.get(&number).cloned().unwrap_or_default();
            let item = gh.item(number).clone();
            Ok(IssueThread {
                number,
                title: item.title,
                body: "Make it so.".into(),
                author: item.author,
                labels: item.labels,
                url: issue_url(repo, number),
                comments,
            })
        }
        async fn comment(&self, _: &str, number: u64, body: &str) -> Result<u64> {
            Ok(self.gh().post(number, BOT, body))
        }
        async fn create_issue(&self, _: &str, title: &str, body: &str, labels: &[String]) -> Result<u64> {
            let mut gh = self.gh();
            gh.issues.push((title.into(), body.into(), labels.to_vec()));
            Ok(900 + gh.issues.len() as u64)
        }
        async fn open_or_update_pr(&self, repo: &str, head: &str, _: &str, _: &str, body: &str) -> Result<PullRequest> {
            let number = 1000 + head.trim_start_matches("forge/").parse::<u64>()?;
            self.gh().prs.push((head.into(), body.into()));
            Ok(PullRequest { number, url: format!("https://github.com/{repo}/pull/{number}"), head_sha: "head".into() })
        }
        async fn merge_pr(&self, _: &str, number: u64, sha: &str, _: &str) -> Result<()> {
            let mut gh = self.gh();
            gh.merge_tries += 1;
            if gh.fail_merges {
                bail!("Pull Request is not mergeable");
            }
            if let Some(status) = gh.refusals.pop_front() {
                let request = format!("PUT /repos/{REPO}/pulls/{number}/merge");
                return Err(GitHubError { status, request, message: "Base branch was modified".into() }.into());
            }
            gh.merges.push((number, sha.into()));
            Ok(())
        }
        async fn git_token(&self) -> Result<String> {
            Ok("test-token".into())
        }
        async fn collaborators(&self, _: &str) -> Result<Vec<String>> {
            Ok(vec![])
        }
    }

    // ---- Sessions ----

    enum Step {
        Says(Outcome),
        /// The run ended with is_error, e.g. a timeout.
        Errs(Outcome),
        Fails,
        Panics,
    }

    struct FakeRunner {
        steps: Mutex<VecDeque<Step>>,
        /// Every request, and whether its key was live during the burst.
        seen: Mutex<Vec<(BurstRequest, bool)>>,
        keys: SessionKeys,
        /// Each burst takes a permit before it returns; tests drain it to hold bursts.
        gate: Semaphore,
    }

    #[async_trait]
    impl Runner for FakeRunner {
        async fn burst(&self, req: BurstRequest) -> Result<BurstResult> {
            let live = self.keys.tag(&req.session_key).is_some();
            let session_id = Some(format!("{}-{}", req.role, req.issue));
            self.seen.lock().unwrap().push((req, live));
            self.gate.acquire().await?.forget();
            let step = self.steps.lock().unwrap().pop_front();
            match step {
                Some(Step::Says(outcome)) => Ok(BurstResult { session_id, outcome, cost_usd: Some(0.25), is_error: false }),
                Some(Step::Errs(outcome)) => Ok(BurstResult { session_id, outcome, cost_usd: None, is_error: true }),
                Some(Step::Fails) | None => bail!("claude exited 1"),
                Some(Step::Panics) => panic!("session crashed"),
            }
        }
    }

    fn pr_outcome() -> Outcome {
        Outcome::Pr { title: "Add orders".into(), body: "Adds the orders table.".into() }
    }

    fn pr() -> Step {
        Step::Says(pr_outcome())
    }

    fn question(text: &str) -> Step {
        Step::Says(Outcome::Question { text: text.into() })
    }

    fn review(pass: bool, notes: &str) -> Step {
        Step::Says(Outcome::Review {
            pass,
            notes: notes.into(),
            summary: String::new(),
            checked: String::new(),
            needs_you: vec![],
            try_steps: vec![],
        })
    }

    // ---- Git ----

    #[derive(Default)]
    struct FakeGit {
        no_commits: Mutex<bool>,
        rebases: Mutex<VecDeque<std::result::Result<(), String>>>,
        runs: Mutex<VecDeque<CmdOutput>>,
        log: Mutex<Vec<String>>,
        envs: Mutex<Vec<Vec<(String, String)>>>,
        pushes: Mutex<u32>,
    }

    impl FakeGit {
        fn did(&self, what: String) {
            self.log.lock().unwrap().push(what);
        }

        fn log(&self) -> Vec<String> {
            self.log.lock().unwrap().clone()
        }

        /// Logs a command and answers with the next scripted output, else success.
        fn run(&self, what: String, env: &[(String, String)]) -> CmdOutput {
            self.did(what);
            self.envs.lock().unwrap().push(env.to_vec());
            let ok = CmdOutput { ok: true, code: Some(0), output: "fine".into() };
            self.runs.lock().unwrap().pop_front().unwrap_or(ok)
        }
    }

    #[async_trait]
    impl GitOps for FakeGit {
        async fn worktree(&self, _: &RepoConfig, branch: &str, _: &str) -> Result<PathBuf> {
            self.did(format!("worktree {branch}"));
            Ok(PathBuf::from("/wt").join(branch.replace('/', "-")))
        }
        async fn collect(&self, _: &RepoConfig, _: &str, _: &str) -> Result<u32> {
            Ok(if *self.no_commits.lock().unwrap() { 0 } else { 2 })
        }
        async fn push(&self, _: &RepoConfig, branch: &str, _: &str) -> Result<String> {
            self.did(format!("push {branch}"));
            let mut n = self.pushes.lock().unwrap();
            *n += 1;
            Ok(format!("sha{n}"))
        }
        async fn review_checkout(&self, _: &RepoConfig, branch: &str, _: &str) -> Result<PathBuf> {
            self.did(format!("review checkout {branch}"));
            Ok(PathBuf::from("/wt").join(format!("{}-review", branch.replace('/', "-"))))
        }
        async fn remove_review_checkout(&self, _: &RepoConfig, branch: &str) -> Result<()> {
            self.did(format!("remove review checkout {branch}"));
            Ok(())
        }
        async fn land(
            &self,
            _: &RepoConfig,
            branch: &str,
            check: Option<&str>,
            env: &[(String, String)],
            _: Duration,
            _: &str,
        ) -> Result<Landing> {
            self.did(format!("land {branch}"));
            if let Err(output) = self.rebases.lock().unwrap().pop_front().unwrap_or(Ok(())) {
                return Ok(Landing::Conflict(output));
            }
            if let Some(check) = check {
                let out = self.run(format!("check {check}"), env);
                if !out.ok {
                    return Ok(Landing::CheckFailed(out.output));
                }
            }
            Ok(Landing::Ready { sha: format!("{branch} rebased") })
        }
        async fn run_on_main(
            &self,
            _: &RepoConfig,
            shell: &str,
            env: &[(String, String)],
            _: Duration,
            _: &str,
        ) -> Result<CmdOutput> {
            Ok(self.run(format!("run {shell} on main"), env))
        }
        async fn remove_worktree(&self, _: &RepoConfig, branch: &str) -> Result<()> {
            self.did(format!("remove {branch}"));
            Ok(())
        }
    }

    // ---- The world a test runs in ----

    struct World {
        engine: Arc<Engine>,
        gh: Arc<FakeGh>,
        runner: Arc<FakeRunner>,
        git: Arc<FakeGit>,
        notes: Arc<RecordingNotifier>,
        keys: SessionKeys,
        dir: tempfile::TempDir,
    }

    fn world() -> World {
        world_with(|_| {})
    }

    fn world_with(edit: impl FnOnce(&mut Config)) -> World {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = config::sample();
        cfg.data_dir = dir.path().to_path_buf();
        for (name, role) in cfg.repos[0].roles.iter_mut() {
            role.prompt_file = dir.path().join(format!("{name}.md"));
            std::fs::write(&role.prompt_file, format!("{} ROLE PROMPT", name.to_uppercase())).unwrap();
            (role.model, role.effort) = (format!("model-{name}"), if name == "ui" { "high" } else { "xhigh" }.into());
        }
        edit(&mut cfg);
        let keys = SessionKeys::new();
        let gh = Arc::new(FakeGh::default());
        let runner = Arc::new(FakeRunner {
            steps: Mutex::default(),
            seen: Mutex::default(),
            keys: keys.clone(),
            gate: Semaphore::new(1000),
        });
        let git = Arc::new(FakeGit::default());
        let notes = Arc::new(RecordingNotifier::default());
        let db = Arc::new(Db::open_in_memory().unwrap());
        let mut engine = Engine::new(Arc::new(cfg), db, gh.clone(), runner.clone(), git.clone(), notes.clone(), keys.clone());
        engine.merge_backoff = Duration::from_millis(1);
        let engine = Arc::new(engine);
        World { engine, gh, runner, git, notes, keys, dir }
    }

    impl World {
        /// A tick, then every burst it spawned run to the end.
        async fn tick(&self) {
            self.engine.tick().await.unwrap();
            self.idle().await;
        }

        async fn idle(&self) {
            tokio::time::timeout(Duration::from_secs(10), self.engine.wait_idle()).await.expect("bursts never ended");
        }

        fn script(&self, steps: Vec<Step>) {
            self.runner.steps.lock().unwrap().extend(steps);
        }

        fn requests(&self) -> Vec<BurstRequest> {
            self.runner.seen.lock().unwrap().iter().map(|(r, _)| r.clone()).collect()
        }

        fn task(&self, issue: u64) -> Task {
            self.engine.db.task(REPO, issue).unwrap().unwrap()
        }

        /// Puts a task in SQLite and its item on the Project, as an earlier tick would have.
        fn put(&self, issue: u64, status: Status, change: impl FnOnce(&mut Task)) {
            self.gh.add(issue, status, &[]);
            let mut task = Task::new(REPO, issue, &format!("Task {issue}"), "alice", status, "backend");
            change(&mut task);
            self.engine.db.upsert_task(&task).unwrap();
        }

        /// An Accepted task with an open PR whose pushed commit passed review.
        fn accepted(&self, issue: u64) {
            self.put(issue, Status::Accepted, |t| t.pr_number = Some(1000 + issue));
            self.shas(issue, "sha-reviewed", "sha-reviewed");
        }

        /// Sets what Forge last pushed for review and what last passed it.
        fn shas(&self, issue: u64, pushed: &str, reviewed: &str) {
            self.engine.db.kv_set(&format!("pushed:{REPO}#{issue}"), pushed).unwrap();
            self.engine.db.kv_set(&format!("reviewed:{REPO}#{issue}"), reviewed).unwrap();
        }

        /// (pushed, reviewed) as kv holds them.
        fn kv_shas(&self, issue: u64) -> (Option<String>, Option<String>) {
            let get = |what| self.engine.db.kv_get(&format!("{what}:{REPO}#{issue}")).unwrap();
            (get("pushed"), get("reviewed"))
        }

        fn pings(&self) -> Vec<String> {
            self.notes.pings.lock().unwrap().clone()
        }

        fn last_burst(&self) -> BurstLog {
            self.engine.db.recent_bursts(1).unwrap().remove(0)
        }
    }

    // ---- Starting ----

    #[tokio::test]
    async fn a_ready_task_starts_building_with_a_start_burst() {
        let w = world();
        w.gh.add(1, Status::Ready, &["ui"]);
        w.script(vec![question("Which table?")]);
        w.tick().await;

        assert_eq!(w.gh.gh().moves, vec![(1, Status::Building)]);
        let (req, live) = w.runner.seen.lock().unwrap()[0].clone();
        assert_eq!((req.role.as_str(), req.model.as_str(), req.effort.as_str()), ("ui", "model-ui", "high"));
        assert!(req.prompt.contains("UI ROLE PROMPT"));
        assert_eq!(req.resume, None);
        assert_eq!(req.worktree, PathBuf::from("/wt/forge-1"));
        assert!(live, "the key works while the burst runs");
        assert_eq!(w.keys.tag(&req.session_key), None, "and is revoked after");
        let task = w.task(1);
        assert_eq!(task.status, Status::Building);
        assert_eq!(task.session_id.as_deref(), Some("ui-1"));
        assert_eq!(task.bursts, 1);
        let log = w.last_burst();
        assert_eq!((log.reason.as_str(), log.outcome.as_str(), log.cost_usd), ("start", "question", Some(0.25)));
    }

    #[tokio::test]
    async fn concurrency_caps_building_tasks_and_the_tick_never_waits_on_a_burst() {
        let w = world();
        w.runner.gate.forget_permits(1000);
        w.gh.add(1, Status::Ready, &[]);
        w.gh.add(2, Status::Ready, &[]);
        w.script(vec![question("One?"), question("Two?")]);
        w.engine.tick().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        w.engine.tick().await.unwrap();
        assert_eq!(w.requests().len(), 1, "one burst, still running, and no second start");
        assert_eq!(w.gh.gh().moves, vec![(1, Status::Building)]);

        w.runner.gate.add_permits(1000);
        w.idle().await;
        w.tick().await;
        assert_eq!(w.requests().len(), 1, "a task waiting on an answer still fills the slot");
        assert_eq!(w.task(2).status, Status::Ready);
    }

    #[tokio::test]
    async fn bursts_wait_while_the_repo_has_concurrency_claims_in_flight() {
        let w = world_with(|c| c.repos[0].concurrency = 2);
        w.engine.busy().hold(REPO, 99);
        for issue in 1..=3 {
            w.put(issue, Status::Building, |_| {});
        }
        w.script(vec![question("One?"), question("Two?"), question("Three?")]);
        w.runner.gate.forget_permits(1000);
        w.engine.tick().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(w.requests().len(), 1, "a landing in flight takes room too");

        w.engine.flights().remove(&(REPO.to_string(), 99));
        w.engine.tick().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(w.requests().len(), 2, "three tasks dragged into Building run two sessions");

        w.runner.gate.add_permits(1000);
        w.idle().await;
        w.tick().await;
        assert_eq!(w.requests().len(), 3, "the last waited for a later tick");

        let w = world();
        w.engine.busy().hold(REPO, 99);
        w.gh.add(1, Status::Ready, &[]);
        w.engine.tick().await.unwrap();
        assert!(w.requests().is_empty() && w.gh.gh().moves.is_empty(), "a start needs room too");
    }

    #[tokio::test]
    async fn concurrency_zero_pauses_the_repo() {
        let w = world_with(|c| c.repos[0].concurrency = 0);
        w.gh.add(1, Status::Ready, &[]);
        w.accepted(2);
        w.tick().await;
        assert!(w.requests().is_empty());
        assert!(w.gh.gh().moves.is_empty());
        assert!(w.gh.gh().merges.is_empty());
        assert_eq!(w.task(1).status, Status::Ready, "still synced");
    }

    #[tokio::test]
    async fn a_building_task_owing_nothing_gets_a_burst() {
        let w = world_with(|c| c.repos[0].concurrency = 2);
        w.put(1, Status::Reviewed, |t| t.session_id = Some("backend-1".into()));
        w.put(2, Status::Ready, |_| {});
        w.gh.drag(1, Status::Building);
        w.gh.drag(2, Status::Building);
        w.script(vec![question("What should change?"), question("What should change?")]);
        w.tick().await;
        let reqs = w.requests();
        assert_eq!(reqs.len(), 2);
        let req = |issue| reqs.iter().find(|r| r.issue == issue).unwrap().clone();
        assert_eq!(req(1).resume.as_deref(), Some("backend-1"));
        assert!(req(1).prompt.contains("Someone answered on the issue"));
        assert_eq!(req(2).resume, None);
        assert!(req(2).prompt.contains("The task just started"));
        for issue in [1, 2] {
            assert_eq!(w.task(issue).waiting_on.as_deref(), Some("alice"));
        }
    }

    #[tokio::test]
    async fn a_project_read_from_before_forges_own_move_does_not_undo_it() {
        let w = world();
        w.gh.add(1, Status::Ready, &[]);
        w.script(vec![pr(), review(true, "Good.")]);
        w.runner.gate.forget_permits(1000);
        w.engine.tick().await.unwrap();

        w.gh.1.forget_permits(1000);
        let engine = w.engine.clone();
        let stale_tick = tokio::spawn(async move { engine.tick().await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        w.runner.gate.add_permits(1000);
        w.idle().await;
        assert_eq!(w.task(1).status, Status::Reviewed);

        w.gh.1.add_permits(1000);
        stale_tick.await.unwrap().unwrap();
        w.idle().await;
        assert_eq!(w.requests().len(), 2, "no burst for a task the tick read as Building");
        assert_eq!(w.task(1).status, Status::Reviewed);
    }

    // ---- Questions and answers ----

    #[tokio::test]
    async fn a_question_is_posted_waits_on_the_author_and_pings() {
        let w = world();
        w.gh.add(1, Status::Ready, &[]);
        w.script(vec![question("Which table?\nOrders or invoices.")]);
        w.tick().await;

        assert_eq!(w.gh.bodies(1), vec!["@alice Which table?\nOrders or invoices."]);
        let task = w.task(1);
        assert_eq!(task.waiting_on.as_deref(), Some("alice"));
        assert_eq!(task.last_seen_comment, w.gh.gh().last_comment);
        assert_eq!(task.pending, None);
        assert_eq!(w.pings(), vec![format!("{REPO}#1 needs alice: Which table? {URL}")]);
    }

    #[tokio::test]
    async fn an_answer_resumes_the_session_but_forges_own_comments_do_not() {
        let w = world();
        w.gh.add(1, Status::Ready, &[]);
        w.script(vec![question("Which table?"), question("And the index?")]);
        w.tick().await;
        w.gh.say(1, BOT, "Forge talking to itself.");
        w.tick().await;
        assert_eq!(w.requests().len(), 1);

        let answer = w.gh.say(1, "bob-gh", "Orders.");
        w.tick().await;
        let reqs = w.requests();
        assert_eq!(reqs.len(), 2);
        assert_eq!(reqs[1].resume.as_deref(), Some("backend-1"));
        assert!(reqs[1].prompt.contains("ROLE PROMPT"));
        assert_eq!(w.last_burst().reason, "answer");
        assert!(w.task(1).last_seen_comment > answer);
    }

    #[tokio::test]
    async fn a_runner_error_blocks_the_task() {
        let w = world();
        w.gh.add(1, Status::Ready, &[]);
        w.script(vec![Step::Fails]);
        w.tick().await;
        assert_eq!(w.gh.bodies(1), vec!["@alice Blocked: the session failed to run: claude exited 1"]);
        assert_eq!(w.task(1).waiting_on.as_deref(), Some("alice"));
        assert_eq!(w.last_burst().outcome, "error");
        assert_eq!(w.pings(), vec![format!("{REPO}#1 needs alice: Blocked: the session failed to run: claude exited 1 {URL}")]);
    }

    #[tokio::test]
    async fn an_errored_session_or_a_stray_review_is_blocked() {
        let cases = [
            (Step::Errs(Outcome::Blocked { reason: "timed out after 240 minutes".into() }), "timed out after 240 minutes"),
            (Step::Errs(pr_outcome()), "the session ended in an error"),
            (review(true, "Fine."), "the build session ended with a review, not a PR"),
        ];
        for (step, reason) in cases {
            let w = world();
            w.gh.add(1, Status::Ready, &[]);
            w.script(vec![step]);
            w.tick().await;
            assert_eq!(w.gh.bodies(1), vec![format!("@alice Blocked: {reason}")]);
            assert!(w.gh.gh().prs.is_empty());
            assert_eq!(w.task(1).session_id.as_deref(), Some("backend-1"), "kept to resume later");
        }
    }

    #[tokio::test]
    async fn questions_on_issues_forge_filed_go_to_the_orderer() {
        let w = world();
        w.gh.add(1, Status::Ready, &[]);
        // GitHub's GraphQL API names the bot without "[bot]".
        w.gh.gh().item(1).author = "example-forge".into();
        w.script(vec![question("Flaky or real?")]);
        w.tick().await;
        assert_eq!(w.gh.bodies(1), vec!["@bob-gh Flaky or real?"]);
        assert_eq!(w.task(1).waiting_on.as_deref(), Some("bob-gh"));
    }

    #[test]
    fn issues_asked_through_forge_name_their_asker() {
        let cfg = crate::config::sample();
        let repo = &cfg.repos[0];
        assert_eq!(asker(repo, "Fix it.\n\n_Asked by Ann through Forge._"), Some("ann-gh"));
        assert_eq!(asker(repo, "_Asked by Nobody through Forge._"), None);
        assert_eq!(asker(repo, "Make it so."), None);
    }

    #[tokio::test]
    async fn a_missing_prompt_file_blocks_without_running_a_session() {
        let w = world();
        std::fs::remove_file(w.dir.path().join("backend.md")).unwrap();
        w.gh.add(1, Status::Ready, &[]);
        w.tick().await;
        assert!(w.requests().is_empty());
        let body = &w.gh.bodies(1)[0];
        assert!(body.starts_with("@alice Blocked: can't read the backend role's prompt file"), "{body}");
        assert_eq!(w.task(1).pending, None);
    }

    #[tokio::test]
    async fn a_panicking_burst_lets_go_of_the_task_and_is_retried() {
        let w = world();
        w.gh.add(1, Status::Ready, &[]);
        w.script(vec![Step::Panics, question("Which table?")]);
        w.tick().await;
        assert_eq!(w.keys.tag(&w.requests()[0].session_key), None);
        assert_eq!(w.task(1).pending, Some(BurstReason::Start), "still owed");

        w.tick().await;
        assert_eq!(w.requests().len(), 2);
        assert_eq!(w.task(1).waiting_on.as_deref(), Some("alice"));
    }

    // ---- PRs and reviews ----

    #[tokio::test]
    async fn a_pr_without_commits_is_blocked() {
        let w = world();
        *w.git.no_commits.lock().unwrap() = true;
        w.gh.add(1, Status::Ready, &[]);
        w.script(vec![pr()]);
        w.tick().await;
        assert_eq!(w.gh.bodies(1), vec!["@alice Blocked: no commits"]);
        assert!(!w.git.log().iter().any(|l| l.starts_with("push")));
        assert_eq!(w.task(1).waiting_on.as_deref(), Some("alice"));
    }

    #[tokio::test]
    async fn a_pr_is_pushed_opened_and_reviewed() {
        let w = world();
        w.gh.add(1, Status::Ready, &[]);
        w.script(vec![pr(), review(true, "Does what #1 asks.")]);
        w.tick().await;

        assert!(w.git.log().contains(&"push forge/1".to_string()));
        assert_eq!(w.gh.gh().prs, vec![("forge/1".to_string(), "Adds the orders table.\n\nCloses #1".to_string())]);
        let reqs = w.requests();
        let r = &reqs[1];
        assert_eq!((r.role.as_str(), r.model.as_str(), r.resume.as_deref()), ("review", "model-review", None));
        assert!(r.prompt.contains("REVIEW ROLE PROMPT"));
        assert_eq!(w.last_burst().reason, "review");

        assert_eq!(r.worktree, PathBuf::from("/wt/forge-1-review"));
        assert_eq!(w.kv_shas(1), (Some("sha1".into()), Some("sha1".into())));

        let task = w.task(1);
        assert_eq!(task.status, Status::Reviewed);
        assert_eq!(task.pr_number, Some(1001));
        assert_eq!(task.pr_url.as_deref(), Some("https://github.com/example-team/shop/pull/1001"));
        assert_eq!(task.session_id.as_deref(), Some("backend-1"), "the review session isn't kept");
        assert_eq!((task.review_rounds, task.bursts, task.pending), (0, 2, None));
        assert_eq!(w.gh.gh().moves, vec![(1, Status::Building), (1, Status::Reviewed)]);
        assert_eq!(
            w.gh.bodies(1),
            vec!["@alice ready to verify: `forge try 1`\n\n<details><summary>Review notes</summary>\n\nDoes what #1 asks.\n\n</details>"]
        );
        assert_eq!(w.pings(), vec![format!("{REPO}#1 ready to verify: forge try 1 {URL}")]);
    }

    #[tokio::test]
    async fn the_review_runs_in_a_checkout_of_its_own_removed_whatever_happened() {
        for verdict in [review(true, "Good."), review(false, "Drop the helper."), Step::Fails] {
            let w = world();
            w.gh.add(1, Status::Ready, &[]);
            w.script(vec![pr(), verdict]);
            w.tick().await;
            let reqs = w.requests();
            assert_eq!(reqs[0].worktree, PathBuf::from("/wt/forge-1"));
            assert_eq!((reqs[1].worktree.to_str(), reqs[1].resume.as_deref()), (Some("/wt/forge-1-review"), None));
            assert!(reqs[1].prompt.contains("git diff origin/main...sha1"));
            let log = ["worktree forge/1", "push forge/1", "review checkout forge/1", "remove review checkout forge/1"];
            assert_eq!(w.git.log(), log);
        }
    }

    #[tokio::test]
    async fn review_notes_go_back_to_the_session_until_it_passes() {
        let w = world();
        w.gh.add(1, Status::Ready, &[]);
        w.script(vec![pr(), review(false, "Drop the helper."), pr(), review(false, "Rename it."), pr(), review(true, "Good.")]);
        w.tick().await;
        let task = w.task(1);
        assert_eq!((task.status, task.review_rounds), (Status::Building, 1));
        assert_eq!(task.pending, Some(BurstReason::ReviewNotes { notes: "Drop the helper.".into() }));

        w.tick().await;
        let reqs = w.requests();
        assert!(reqs[2].prompt.contains("Drop the helper."));
        assert_eq!(reqs[2].resume.as_deref(), Some("backend-1"));
        assert_eq!(w.task(1).review_rounds, 2);

        w.tick().await;
        let task = w.task(1);
        assert_eq!((task.status, task.review_rounds), (Status::Reviewed, 0));
        assert!(w.gh.bodies(1).iter().all(|b| !b.contains("Blocked")));
    }

    #[tokio::test]
    async fn three_failed_reviews_block_the_task() {
        let w = world();
        w.gh.add(1, Status::Ready, &[]);
        w.script(vec![pr(), review(false, "n1"), pr(), review(false, "n2"), pr(), review(false, "Still two helpers.")]);
        for _ in 0..3 {
            w.tick().await;
        }
        assert_eq!(
            w.gh.bodies(1),
            vec!["@alice Blocked: review failed 3 times\n\n<details><summary>The last review's notes</summary>\n\nStill two helpers.\n\n</details>"]
        );
        let task = w.task(1);
        assert_eq!((task.status, task.pending, task.review_rounds), (Status::Building, None, 0));
        assert_eq!(task.waiting_on.as_deref(), Some("alice"));
        assert_eq!(w.pings(), vec![format!("{REPO}#1 needs alice: Blocked: review failed 3 times {URL}")]);
    }

    // ---- Landing ----

    #[tokio::test]
    async fn an_accepted_task_lands_on_main() {
        let w = world();
        w.accepted(1);
        w.tick().await;
        assert_eq!(w.git.log(), vec!["land forge/1", "check ./scripts/check-migration-numbers.sh", "remove forge/1"]);
        assert_eq!(w.gh.gh().merges, vec![(1001, "forge/1 rebased".to_string())]);
        let landed = Some("forge/1 rebased".to_string());
        assert_eq!(w.kv_shas(1), (landed.clone(), landed));
        assert_eq!(w.gh.gh().moves, vec![(1, Status::OnMain)]);
        assert_eq!(w.task(1).status, Status::OnMain);
        assert_eq!(w.gh.bodies(1), vec!["Merged to main."]);
        assert_eq!(w.pings(), vec![format!("{REPO}#1 merged to main")]);
    }

    #[tokio::test]
    async fn a_rebase_conflict_goes_back_to_building_with_the_output() {
        let w = world();
        w.accepted(1);
        w.git.rebases.lock().unwrap().push_back(Err("CONFLICT in src/orders.rs".into()));
        w.tick().await;
        assert!(w.gh.gh().merges.is_empty());
        assert_eq!(w.gh.gh().moves, vec![(1, Status::Building)]);
        let output = "CONFLICT in src/orders.rs".to_string();
        assert_eq!(w.task(1).pending, Some(BurstReason::RebaseConflict { output }));
        assert!(w.requests().is_empty(), "the burst comes on a later tick");

        w.script(vec![question("Keep both columns?")]);
        w.tick().await;
        assert!(w.requests()[0].prompt.contains("CONFLICT in src/orders.rs"));
        assert_eq!(w.last_burst().reason, "rebase_conflict");
    }

    #[tokio::test]
    async fn a_failed_merge_check_goes_back_to_building_with_the_output() {
        let w = world();
        w.accepted(1);
        let failed = CmdOutput { ok: false, code: Some(1), output: "migration 0042 appears twice".into() };
        w.git.runs.lock().unwrap().push_back(failed);
        w.tick().await;
        assert!(w.gh.gh().merges.is_empty());
        let output = "migration 0042 appears twice".to_string();
        assert_eq!(w.task(1).pending, Some(BurstReason::MergeCheckFailed { output }));
        assert_eq!(w.task(1).status, Status::Building);

        w.script(vec![question("Renumber to 0043?")]);
        w.tick().await;
        assert!(w.requests()[0].prompt.contains("migration 0042 appears twice"));
    }

    #[tokio::test]
    async fn only_one_task_lands_per_repo_per_tick() {
        let w = world();
        w.accepted(1);
        w.accepted(2);
        w.tick().await;
        assert_eq!(w.gh.gh().merges, vec![(1001, "forge/1 rebased".to_string())]);
        w.tick().await;
        assert_eq!(w.gh.gh().merges, vec![(1001, "forge/1 rebased".to_string()), (1002, "forge/2 rebased".to_string())]);
    }

    #[tokio::test]
    async fn a_failed_merge_blocks_the_task_so_the_queue_moves_on() {
        let w = world();
        w.accepted(1);
        w.accepted(2);
        w.gh.gh().fail_merges = true;
        w.tick().await;
        let task = w.task(1);
        assert_eq!((task.status, task.waiting_on.as_deref()), (Status::Building, Some("alice")));
        assert_eq!(w.gh.bodies(1), vec!["@alice Blocked: couldn't land it: merging the PR: Pull Request is not mergeable"]);

        w.gh.gh().fail_merges = false;
        w.tick().await;
        assert_eq!(w.gh.gh().merges, vec![(1002, "forge/2 rebased".to_string())]);

        w.gh.drag(1, Status::Accepted);
        w.tick().await;
        assert_eq!(w.gh.gh().merges[1], (1001, "forge/1 rebased".to_string()), "a re-accept lands what was landed");
    }

    #[tokio::test]
    async fn a_task_accepted_while_its_burst_runs_lands_only_once_its_new_commit_passes_review() {
        let w = world();
        w.gh.add(1, Status::Ready, &[]);
        w.script(vec![pr(), review(false, "Drop the helper.")]);
        w.runner.gate.forget_permits(1000);
        w.engine.tick().await.unwrap();
        w.gh.drag(1, Status::Accepted);
        w.engine.tick().await.unwrap();
        w.runner.gate.add_permits(1000);
        w.idle().await;
        assert_eq!(w.task(1).status, Status::Accepted);
        assert_eq!(w.kv_shas(1), (Some("sha1".into()), None));

        w.tick().await;
        assert!(w.gh.gh().merges.is_empty(), "the unreviewed sha1 never lands");
        assert!(!w.git.log().iter().any(|l| l.starts_with("land")));
        assert_eq!((w.task(1).status, w.gh.gh().item(1).status), (Status::Building, Some(Status::Building)));

        w.script(vec![pr(), review(true, "Good.")]);
        w.tick().await;
        assert!(w.requests()[2].prompt.contains("Drop the helper."), "its owed burst ran");
        assert_eq!(w.task(1).status, Status::Reviewed);
        w.gh.drag(1, Status::Accepted);
        w.tick().await;
        assert_eq!(w.gh.gh().merges, vec![(1001, "forge/1 rebased".to_string())]);
    }

    #[tokio::test]
    async fn an_accepted_task_owing_nothing_whose_push_was_not_reviewed_resumes_its_session() {
        let w = world();
        w.put(1, Status::Accepted, |t| {
            t.pr_number = Some(1001);
            t.session_id = Some("backend-1".into());
        });
        w.shas(1, "sha-new", "sha-old");
        w.tick().await;
        assert!(w.gh.gh().merges.is_empty());
        assert_eq!(w.task(1).status, Status::Building);

        w.script(vec![question("What changed?")]);
        w.tick().await;
        assert_eq!(w.requests()[0].resume.as_deref(), Some("backend-1"));
        assert_eq!(w.last_burst().reason, "answer");
    }

    #[tokio::test]
    async fn a_merge_github_is_not_ready_for_is_tried_again() {
        let w = world();
        w.accepted(1);
        w.gh.gh().refusals.extend([405, 409, 405, 409]);
        w.tick().await;
        assert_eq!(w.gh.gh().merge_tries, 5);
        assert_eq!(w.gh.gh().merges, vec![(1001, "forge/1 rebased".to_string())]);
        assert_eq!(w.task(1).status, Status::OnMain);

        for (refusals, tries) in [(vec![409; 5], 5), (vec![422], 1)] {
            let w = world();
            w.accepted(1);
            w.gh.gh().refusals.extend(refusals);
            w.tick().await;
            assert_eq!(w.gh.gh().merge_tries, tries);
            assert!(w.gh.gh().merges.is_empty());
            let task = w.task(1);
            assert_eq!((task.status, task.waiting_on.as_deref()), (Status::Building, Some("alice")));
        }
    }

    #[tokio::test]
    async fn start_and_land_go_by_the_task_as_it_is_now_not_as_the_tick_read_it() {
        let w = world();
        w.put(1, Status::Ready, |_| {});
        w.put(2, Status::Ready, |_| {});
        w.accepted(3);
        w.accepted(4);
        let read: Vec<(ProjectItem, Task)> = w.gh.gh().items.iter().map(|i| (i.clone(), w.task(i.issue_number))).collect();
        w.engine.db.attach(REPO, 1, "bob-gh").unwrap();
        w.engine.db.attach(REPO, 3, "bob-gh").unwrap();
        w.engine.db.upsert_task(&Task { status: Status::Asked, ..w.task(2) }).unwrap();
        w.engine.db.upsert_task(&Task { status: Status::Building, ..w.task(4) }).unwrap();
        let repo = &w.engine.cfg.repos[0];

        w.engine.start(repo, &read).await.unwrap();
        w.engine.land_next(repo, &read).await.unwrap();
        w.engine.land_next(repo, &read[3..]).await.unwrap();
        w.idle().await;
        assert!(w.requests().is_empty());
        assert!(w.gh.gh().moves.is_empty());
        assert!(w.git.log().is_empty());
    }

    // ---- People stepping in ----

    #[tokio::test]
    async fn attached_tasks_get_no_bursts() {
        let w = world();
        w.put(1, Status::Building, |t| {
            t.pending = Some(BurstReason::AfterAttach);
            t.attached_by = Some("bob-gh".into());
        });
        w.engine.db.kv_set(&format!("attach:{REPO}#1"), &Utc::now().to_rfc3339()).unwrap();
        w.script(vec![question("Done?")]);
        w.tick().await;
        assert!(w.requests().is_empty());

        w.engine.db.detach(REPO, 1).unwrap();
        w.tick().await;
        assert_eq!(w.last_burst().reason, "after_attach");
    }

    #[tokio::test]
    async fn stale_attaches_are_ended() {
        let w = world();
        let hours_ago = |h| Some((Utc::now() - chrono::Duration::hours(h)).to_rfc3339());
        for (issue, since) in [(1, hours_ago(9)), (2, None), (3, Some("garbage".to_string())), (4, hours_ago(7))] {
            w.put(issue, Status::Reviewed, |t| t.session_id = Some(format!("backend-{issue}")));
            w.engine.db.attach(REPO, issue, "bob-gh").unwrap();
            if let Some(since) = since {
                w.engine.db.kv_set(&format!("attach:{REPO}#{issue}"), &since).unwrap();
            }
        }
        let stale_key = w.keys.issue(&format!("{REPO}#1 attach bob-gh"));
        let live_key = w.keys.issue(&format!("{REPO}#4 attach bob-gh"));
        w.tick().await;
        assert_eq!(w.keys.tag(&stale_key), None, "a stale attach's key stops working");
        assert!(w.keys.tag(&live_key).is_some());
        for issue in [1, 2, 3] {
            assert_eq!(w.task(issue).attached_by, None, "#{issue}");
            let since = w.engine.db.kv_get(&format!("attach:{REPO}#{issue}")).unwrap();
            assert!(since.unwrap_or_default().is_empty(), "#{issue}");
        }
        assert_eq!(w.task(4).attached_by.as_deref(), Some("bob-gh"));
        assert!(w.requests().is_empty());
    }

    #[tokio::test]
    async fn moved_back_tasks_stop_but_keep_their_session_and_branch() {
        let w = world();
        w.gh.add(1, Status::Ready, &[]);
        w.script(vec![question("Which table?")]);
        w.tick().await;

        w.gh.drag(1, Status::Asked);
        w.gh.say(1, "alice", "Hold on, rethinking this.");
        w.tick().await;
        assert_eq!(w.requests().len(), 1, "the answer isn't acted on");
        let task = w.task(1);
        assert_eq!((task.status, task.pending, task.waiting_on), (Status::Asked, None, None));
        assert_eq!((task.session_id.as_deref(), task.branch.as_str()), (Some("backend-1"), "forge/1"));

        w.gh.drag(1, Status::Ready);
        w.script(vec![question("Back on it: which table?")]);
        w.tick().await;
        let reqs = w.requests();
        assert_eq!(reqs[1].resume.as_deref(), Some("backend-1"));
        assert_eq!(w.last_burst().reason, "start");
    }

    // ---- E2E ----

    #[tokio::test]
    async fn e2e_runs_when_due_and_a_pass_is_only_recorded() {
        let w = world();
        w.engine.e2e_tick().await.unwrap();
        assert_eq!(w.git.log(), vec!["run npm run e2e on main"]);
        let env = vec![("STAGING_URL".to_string(), "https://staging.example.com".to_string())];
        assert_eq!(w.git.envs.lock().unwrap()[0], env);
        let run = w.engine.db.last_e2e(REPO).unwrap().unwrap();
        assert_eq!((run.passed, run.issue, run.finished_at.is_some()), (Some(true), None, true));
        assert!(w.gh.gh().issues.is_empty());
        assert!(w.pings().is_empty());

        w.engine.e2e_tick().await.unwrap();
        assert_eq!(w.git.log().len(), 1, "not due again yet");
    }

    #[tokio::test]
    async fn e2e_is_due_every_hours_after_the_last_start() {
        let w = world();
        w.engine.db.start_e2e(REPO, Utc::now() - chrono::Duration::minutes(239)).unwrap();
        w.engine.e2e_tick().await.unwrap();
        assert!(w.git.log().is_empty());

        let w = world();
        w.engine.db.start_e2e(REPO, Utc::now() - chrono::Duration::minutes(241)).unwrap();
        w.engine.e2e_tick().await.unwrap();
        assert_eq!(w.git.log().len(), 1);
    }

    #[tokio::test]
    async fn a_failed_e2e_files_an_issue_and_pings() {
        let w = world();
        let output: Vec<String> = (1..=300).map(|i| format!("line {i}")).collect();
        let failed = CmdOutput { ok: false, code: Some(1), output: output.join("\n") };
        w.git.runs.lock().unwrap().push_back(failed);
        w.engine.e2e_tick().await.unwrap();

        let (title, body, labels) = w.gh.gh().issues[0].clone();
        assert!(title.starts_with("E2E failed on staging, ") && title.ends_with(" UTC"), "{title}");
        assert!(body.contains("```\nline 101\n") && body.ends_with("line 300\n```\n"), "{body}");
        assert!(!body.contains("line 100\n"));
        assert_eq!(labels, vec!["e2e"]);
        assert_eq!(w.pings(), vec![format!("E2E failed on staging for {REPO}: https://github.com/{REPO}/issues/901")]);
        let run = w.engine.db.last_e2e(REPO).unwrap().unwrap();
        assert_eq!((run.passed, run.issue), (Some(false), Some(901)));
    }

    // ---- The loop ----

    #[tokio::test]
    async fn run_carries_on_after_failed_ticks_and_stops_on_the_signal() {
        let w = world_with(|c| c.poll_seconds = 1);
        w.gh.gh().fail_reads = true;
        let (stop, rx) = tokio::sync::watch::channel(false);
        let looping = tokio::spawn(w.engine.clone().run(rx));
        tokio::time::sleep(Duration::from_millis(1300)).await;
        assert!(w.gh.gh().reads >= 2, "ticks at 0 and 1 s despite failing");
        assert!(w.engine.db.last_e2e(REPO).unwrap().is_some(), "e2e was checked");

        stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), looping).await.expect("run stopped").unwrap();
    }

    #[test]
    fn a_passed_review_reads_in_one_layout() {
        let review = Outcome::Review {
            pass: true,
            notes: "get_json has one caller now.\nFold it in later.".into(),
            summary: "openapi.json is built when the checks run,\n  not committed.".into(),
            checked: "fmt, clippy, client tests: pass".into(),
            needs_you: vec!["Add crates/** to admin-tests.yml's triggers.".into(), " ".into()],
            try_steps: vec!["Run `npm --prefix admin test`.".into(), "Rename a route; the contract test fails.".into()],
        };
        assert_eq!(
            verify_comment("@ann ready to verify: `forge try 7`", &review),
            "@ann ready to verify: `forge try 7`\n\n\
             openapi.json is built when the checks run, not committed.\n\n\
             **Checked:** fmt, clippy, client tests: pass\n\n\
             **Needs you**\n- Add crates/** to admin-tests.yml's triggers.\n\n\
             **To try**\n1. Run `npm --prefix admin test`.\n2. Rename a route; the contract test fails.\n\n\
             <details><summary>Review notes</summary>\n\nget_json has one caller now.\nFold it in later.\n\n</details>"
        );
    }
}
