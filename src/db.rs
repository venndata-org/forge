//! Forge's own state, in one SQLite file: tasks, bursts, plan limits, sign-in
//! challenges and API sessions, e2e runs and a small key-value table.
//! GitHub stays the source of truth for issues and their Project status.

use std::path::Path;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use rusqlite::{params, Connection, OptionalExtension, Params, Row, TransactionBehavior};

use crate::types::{BurstLog, BurstReason, E2eRun, Limits, Status, Task};

pub struct Db {
    conn: Mutex<Connection>,
}

/// A pending email sign-in: the code is only ever stored hashed.
#[derive(Debug, Clone, PartialEq)]
pub struct Challenge {
    pub token_hash: String,
    pub email: String,
    pub code_hash: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub attempts: u32,
}

/// A signed-in API or browser session: the token is only ever stored hashed.
#[derive(Debug, Clone, PartialEq)]
pub struct ApiSession {
    pub token_hash: String,
    pub email: String,
    pub expires_at: DateTime<Utc>,
}

/// Each entry moves the schema up one version; `PRAGMA user_version` counts
/// the entries applied. Add new ones at the end; never edit one that shipped.
const MIGRATIONS: &[&str] = &[SCHEMA_V1];

const SCHEMA_V1: &str = "
CREATE TABLE tasks (
    repo TEXT NOT NULL,
    issue INTEGER NOT NULL,
    title TEXT NOT NULL,
    author TEXT NOT NULL,
    status TEXT NOT NULL,
    role TEXT NOT NULL,
    session_id TEXT,
    branch TEXT NOT NULL,
    pr_number INTEGER,
    pr_url TEXT,
    last_seen_comment INTEGER NOT NULL,
    waiting_on TEXT,
    attached_by TEXT,
    pending TEXT,
    review_rounds INTEGER NOT NULL,
    bursts INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (repo, issue)
);
CREATE TABLE bursts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repo TEXT NOT NULL,
    issue INTEGER NOT NULL,
    role TEXT NOT NULL,
    reason TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT NOT NULL,
    outcome TEXT NOT NULL,
    session_id TEXT,
    cost_usd REAL
);
CREATE INDEX bursts_by_start ON bursts (started_at);
CREATE TABLE limits (
    label TEXT PRIMARY KEY,
    five_hour_pct REAL,
    five_hour_resets TEXT,
    week_pct REAL,
    week_resets TEXT,
    read_at TEXT NOT NULL
);
CREATE TABLE challenges (
    token_hash TEXT PRIMARY KEY,
    email TEXT NOT NULL,
    code_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    attempts INTEGER NOT NULL
);
CREATE TABLE sessions (
    token_hash TEXT PRIMARY KEY,
    email TEXT NOT NULL,
    expires_at TEXT NOT NULL
);
CREATE TABLE e2e_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repo TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    passed INTEGER,
    issue INTEGER
);
CREATE TABLE kv (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
";

const TASK_COLUMNS: &str = "repo, issue, title, author, status, role, session_id, branch, \
    pr_number, pr_url, last_seen_comment, waiting_on, attached_by, pending, review_rounds, \
    bursts, updated_at";

const BURST_COLUMNS: &str =
    "repo, issue, role, reason, started_at, ended_at, outcome, session_id, cost_usd";

const LIMITS_COLUMNS: &str =
    "label, five_hour_pct, five_hour_resets, week_pct, week_resets, read_at";

const CHALLENGE_COLUMNS: &str = "token_hash, email, code_hash, created_at, expires_at, attempts";

impl Db {
    /// Opens (creating if needed) the file, in WAL mode, and migrates it.
    pub fn open(path: &Path) -> Result<Db> {
        let conn = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
        Db::setup(conn)
    }

    pub fn open_in_memory() -> Result<Db> {
        Db::setup(Connection::open_in_memory()?)
    }

    fn setup(mut conn: Connection) -> Result<Db> {
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.pragma_update_and_check(None, "journal_mode", "WAL", |_| Ok(()))?;
        conn.pragma_update(None, "foreign_keys", true)?;
        migrate(&mut conn)?;
        Ok(Db {
            conn: Mutex::new(conn),
        })
    }

    /// The connection. A panic elsewhere while holding it leaves SQLite
    /// consistent, so a poisoned lock is still safe to use.
    fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn rows<T>(
        &self,
        sql: &str,
        params: impl Params,
        map: fn(&Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<Vec<T>> {
        let conn = self.conn();
        let mut stmt = conn.prepare_cached(sql)?;
        let rows = stmt
            .query_map(params, map)?
            .collect::<rusqlite::Result<_>>()?;
        Ok(rows)
    }

    fn row<T>(
        &self,
        sql: &str,
        params: impl Params,
        map: fn(&Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<Option<T>> {
        Ok(self.conn().query_row(sql, params, map).optional()?)
    }

    fn execute(&self, sql: &str, params: impl Params) -> Result<usize> {
        Ok(self.conn().execute(sql, params)?)
    }

    pub fn task(&self, repo: &str, issue: u64) -> Result<Option<Task>> {
        let sql = format!("SELECT {TASK_COLUMNS} FROM tasks WHERE repo = ?1 AND issue = ?2");
        self.row(&sql, params![repo, issue], task_row)
    }

    /// Inserts the task, or updates every field of the existing one except
    /// `attached_by`, which only `attach` and `detach` change: a writer holding
    /// an older copy of the task can't undo someone stepping in.
    pub fn upsert_task(&self, task: &Task) -> Result<()> {
        let sql = format!(
            "INSERT INTO tasks ({TASK_COLUMNS}) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17) \
             ON CONFLICT(repo, issue) DO UPDATE SET title = excluded.title, author = excluded.author, \
             status = excluded.status, role = excluded.role, session_id = excluded.session_id, \
             branch = excluded.branch, pr_number = excluded.pr_number, pr_url = excluded.pr_url, \
             last_seen_comment = excluded.last_seen_comment, waiting_on = excluded.waiting_on, \
             pending = excluded.pending, review_rounds = excluded.review_rounds, \
             bursts = excluded.bursts, updated_at = excluded.updated_at"
        );
        self.execute(
            &sql,
            params![
                task.repo,
                task.issue,
                task.title,
                task.author,
                task.status,
                task.role,
                task.session_id,
                task.branch,
                task.pr_number,
                task.pr_url,
                task.last_seen_comment,
                task.waiting_on,
                task.attached_by,
                task.pending,
                task.review_rounds,
                task.bursts,
                Time(task.updated_at),
            ],
        )?;
        Ok(())
    }

    /// The repo's tasks, lowest issue number first.
    pub fn tasks(&self, repo: &str) -> Result<Vec<Task>> {
        let sql = format!("SELECT {TASK_COLUMNS} FROM tasks WHERE repo = ?1 ORDER BY issue");
        self.rows(&sql, [repo], task_row)
    }

    /// Every repo's tasks, by repo and then issue number.
    pub fn all_tasks(&self) -> Result<Vec<Task>> {
        let sql = format!("SELECT {TASK_COLUMNS} FROM tasks ORDER BY repo, issue");
        self.rows(&sql, [], task_row)
    }

    /// Marks the task attached to `who`. False when someone else already is,
    /// or the task doesn't exist. Re-attaching by the same login is true.
    pub fn attach(&self, repo: &str, issue: u64, who: &str) -> Result<bool> {
        let changed = self.execute(
            "UPDATE tasks SET attached_by = ?3 \
             WHERE repo = ?1 AND issue = ?2 AND (attached_by IS NULL OR attached_by = ?3)",
            params![repo, issue, who],
        )?;
        Ok(changed > 0)
    }

    pub fn detach(&self, repo: &str, issue: u64) -> Result<()> {
        self.execute(
            "UPDATE tasks SET attached_by = NULL WHERE repo = ?1 AND issue = ?2",
            params![repo, issue],
        )?;
        Ok(())
    }

    pub fn record_burst(&self, log: &BurstLog) -> Result<()> {
        let sql = format!(
            "INSERT INTO bursts ({BURST_COLUMNS}) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"
        );
        self.execute(
            &sql,
            params![
                log.repo,
                log.issue,
                log.role,
                log.reason,
                Time(log.started_at),
                Time(log.ended_at),
                log.outcome,
                log.session_id,
                log.cost_usd,
            ],
        )?;
        Ok(())
    }

    /// Newest first.
    pub fn recent_bursts(&self, limit: usize) -> Result<Vec<BurstLog>> {
        let sql = format!(
            "SELECT {BURST_COLUMNS} FROM bursts ORDER BY started_at DESC, id DESC LIMIT ?1"
        );
        self.rows(&sql, [limit], burst_row)
    }

    pub fn save_limits(&self, label: &str, limits: &Limits) -> Result<()> {
        let sql = format!(
            "INSERT OR REPLACE INTO limits ({LIMITS_COLUMNS}) VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
        );
        self.execute(
            &sql,
            params![
                label,
                limits.five_hour_pct,
                limits.five_hour_resets.map(Time),
                limits.week_pct,
                limits.week_resets.map(Time),
                Time(limits.read_at),
            ],
        )?;
        Ok(())
    }

    /// Every plan's last limits, by label.
    pub fn all_limits(&self) -> Result<Vec<(String, Limits)>> {
        let sql = format!("SELECT {LIMITS_COLUMNS} FROM limits ORDER BY label");
        self.rows(&sql, [], limits_row)
    }

    /// Replaces any challenge with the same token hash.
    pub fn put_challenge(&self, c: &Challenge) -> Result<()> {
        let sql = format!(
            "INSERT OR REPLACE INTO challenges ({CHALLENGE_COLUMNS}) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
        );
        self.execute(
            &sql,
            params![
                c.token_hash,
                c.email.to_lowercase(),
                c.code_hash,
                Time(c.created_at),
                Time(c.expires_at),
                c.attempts,
            ],
        )?;
        Ok(())
    }

    /// The challenge, with its email lowercased.
    pub fn challenge(&self, token_hash: &str) -> Result<Option<Challenge>> {
        let sql = format!("SELECT {CHALLENGE_COLUMNS} FROM challenges WHERE token_hash = ?1");
        self.row(&sql, [token_hash], challenge_row)
    }

    /// Adds one failed attempt and returns the new count.
    pub fn bump_attempts(&self, token_hash: &str) -> Result<u32> {
        let count = self.row(
            "UPDATE challenges SET attempts = attempts + 1 \
             WHERE token_hash = ?1 RETURNING attempts",
            [token_hash],
            |row| row.get(0),
        )?;
        count.context("no challenge with that token")
    }

    pub fn delete_challenge(&self, token_hash: &str) -> Result<()> {
        self.execute("DELETE FROM challenges WHERE token_hash = ?1", [token_hash])?;
        Ok(())
    }

    /// Challenges created for this email (case-insensitive) since `since`.
    pub fn challenges_since(&self, email: &str, since: DateTime<Utc>) -> Result<u32> {
        Ok(self.conn().query_row(
            "SELECT COUNT(*) FROM challenges WHERE email = ?1 AND created_at >= ?2",
            params![email.to_lowercase(), Time(since)],
            |row| row.get(0),
        )?)
    }

    /// Inserts or replaces the session with this token hash.
    pub fn put_session(&self, s: &ApiSession) -> Result<()> {
        self.execute(
            "INSERT OR REPLACE INTO sessions (token_hash, email, expires_at) VALUES (?1, ?2, ?3)",
            params![s.token_hash, s.email.to_lowercase(), Time(s.expires_at)],
        )?;
        Ok(())
    }

    /// The session, with its email lowercased.
    pub fn session(&self, token_hash: &str) -> Result<Option<ApiSession>> {
        self.row(
            "SELECT token_hash, email, expires_at FROM sessions WHERE token_hash = ?1",
            [token_hash],
            session_row,
        )
    }

    pub fn delete_session(&self, token_hash: &str) -> Result<()> {
        self.execute("DELETE FROM sessions WHERE token_hash = ?1", [token_hash])?;
        Ok(())
    }

    /// Deletes challenges and sessions that expired before `now`.
    pub fn purge_expired(&self, now: DateTime<Utc>) -> Result<()> {
        let conn = self.conn();
        conn.execute("DELETE FROM challenges WHERE expires_at < ?1", [Time(now)])?;
        conn.execute("DELETE FROM sessions WHERE expires_at < ?1", [Time(now)])?;
        Ok(())
    }

    /// Records a run starting and returns its id.
    pub fn start_e2e(&self, repo: &str, at: DateTime<Utc>) -> Result<i64> {
        let conn = self.conn();
        conn.execute(
            "INSERT INTO e2e_runs (repo, started_at) VALUES (?1, ?2)",
            params![repo, Time(at)],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn finish_e2e(
        &self,
        id: i64,
        at: DateTime<Utc>,
        passed: bool,
        issue: Option<u64>,
    ) -> Result<()> {
        let changed = self.execute(
            "UPDATE e2e_runs SET finished_at = ?2, passed = ?3, issue = ?4 WHERE id = ?1",
            params![id, Time(at), passed, issue],
        )?;
        ensure!(changed > 0, "no e2e run {id}");
        Ok(())
    }

    /// The repo's most recent run, finished or not.
    pub fn last_e2e(&self, repo: &str) -> Result<Option<E2eRun>> {
        self.row(
            "SELECT repo, started_at, finished_at, passed, issue FROM e2e_runs \
             WHERE repo = ?1 ORDER BY started_at DESC, id DESC LIMIT 1",
            [repo],
            e2e_row,
        )
    }

    pub fn kv_get(&self, key: &str) -> Result<Option<String>> {
        self.row("SELECT value FROM kv WHERE key = ?1", [key], |row| {
            row.get(0)
        })
    }

    pub fn kv_set(&self, key: &str, value: &str) -> Result<()> {
        self.execute(
            "INSERT INTO kv (key, value) VALUES (?1, ?2) \
             ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            [key, value],
        )?;
        Ok(())
    }
}

/// Brings the schema up to the newest version, in one transaction so two
/// processes opening the file at once can't both migrate it.
fn migrate(conn: &mut Connection) -> Result<()> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let version: usize = tx.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version > MIGRATIONS.len() {
        bail!(
            "the database is at schema version {version}, newer than this Forge ({})",
            MIGRATIONS.len()
        );
    }
    if version == MIGRATIONS.len() {
        return Ok(());
    }
    for sql in &MIGRATIONS[version..] {
        tx.execute_batch(sql)?;
    }
    tx.pragma_update(None, "user_version", MIGRATIONS.len())?;
    tx.commit()?;
    Ok(())
}

fn task_row(row: &Row<'_>) -> rusqlite::Result<Task> {
    Ok(Task {
        repo: row.get("repo")?,
        issue: row.get("issue")?,
        title: row.get("title")?,
        author: row.get("author")?,
        status: row.get("status")?,
        role: row.get("role")?,
        session_id: row.get("session_id")?,
        branch: row.get("branch")?,
        pr_number: row.get("pr_number")?,
        pr_url: row.get("pr_url")?,
        last_seen_comment: row.get("last_seen_comment")?,
        waiting_on: row.get("waiting_on")?,
        attached_by: row.get("attached_by")?,
        pending: row.get("pending")?,
        review_rounds: row.get("review_rounds")?,
        bursts: row.get("bursts")?,
        updated_at: row.get::<_, Time>("updated_at")?.0,
    })
}

fn burst_row(row: &Row<'_>) -> rusqlite::Result<BurstLog> {
    Ok(BurstLog {
        repo: row.get("repo")?,
        issue: row.get("issue")?,
        role: row.get("role")?,
        reason: row.get("reason")?,
        started_at: row.get::<_, Time>("started_at")?.0,
        ended_at: row.get::<_, Time>("ended_at")?.0,
        outcome: row.get("outcome")?,
        session_id: row.get("session_id")?,
        cost_usd: row.get("cost_usd")?,
    })
}

fn limits_row(row: &Row<'_>) -> rusqlite::Result<(String, Limits)> {
    let limits = Limits {
        five_hour_pct: row.get("five_hour_pct")?,
        five_hour_resets: row.get::<_, Option<Time>>("five_hour_resets")?.map(|t| t.0),
        week_pct: row.get("week_pct")?,
        week_resets: row.get::<_, Option<Time>>("week_resets")?.map(|t| t.0),
        read_at: row.get::<_, Time>("read_at")?.0,
    };
    Ok((row.get("label")?, limits))
}

fn challenge_row(row: &Row<'_>) -> rusqlite::Result<Challenge> {
    Ok(Challenge {
        token_hash: row.get("token_hash")?,
        email: row.get("email")?,
        code_hash: row.get("code_hash")?,
        created_at: row.get::<_, Time>("created_at")?.0,
        expires_at: row.get::<_, Time>("expires_at")?.0,
        attempts: row.get("attempts")?,
    })
}

fn session_row(row: &Row<'_>) -> rusqlite::Result<ApiSession> {
    Ok(ApiSession {
        token_hash: row.get("token_hash")?,
        email: row.get("email")?,
        expires_at: row.get::<_, Time>("expires_at")?.0,
    })
}

fn e2e_row(row: &Row<'_>) -> rusqlite::Result<E2eRun> {
    Ok(E2eRun {
        repo: row.get("repo")?,
        started_at: row.get::<_, Time>("started_at")?.0,
        finished_at: row.get::<_, Option<Time>>("finished_at")?.map(|t| t.0),
        passed: row.get("passed")?,
        issue: row.get("issue")?,
    })
}

/// A time as SQLite holds it: RFC 3339 UTC text with nanoseconds, always the
/// same width, so comparing the text compares the times.
struct Time(DateTime<Utc>);

impl ToSql for Time {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(self.0.to_rfc3339_opts(SecondsFormat::Nanos, true).into())
    }
}

impl FromSql for Time {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        DateTime::parse_from_rfc3339(value.as_str()?)
            .map(|t| Time(t.with_timezone(&Utc)))
            .map_err(|e| FromSqlError::Other(Box::new(e)))
    }
}

/// Stored as `Status::as_str`.
impl ToSql for Status {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(self.as_str().into())
    }
}

impl FromSql for Status {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let name = value.as_str()?;
        Status::parse(name)
            .ok_or_else(|| FromSqlError::Other(format!("unknown status {name:?}").into()))
    }
}

/// Stored as its JSON.
impl ToSql for BurstReason {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        serde_json::to_string(self)
            .map(ToSqlOutput::from)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))
    }
}

impl FromSql for BurstReason {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        serde_json::from_str(value.as_str()?).map_err(|e| FromSqlError::Other(Box::new(e)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use chrono::TimeZone;

    use super::*;
    use crate::types::branch_for;

    const REPO: &str = "acme/app";

    fn at(hour: u32, minute: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 20, hour, minute, 0).unwrap()
    }

    fn full_task(pending: Option<BurstReason>) -> Task {
        Task {
            repo: REPO.into(),
            issue: 412,
            title: "Add a ledger export".into(),
            author: "alice".into(),
            status: Status::Building,
            role: "build".into(),
            session_id: Some("sess-1".into()),
            branch: branch_for(412),
            pr_number: Some(77),
            pr_url: Some("https://github.com/acme/app/pull/77".into()),
            last_seen_comment: 9_000_000_001,
            waiting_on: Some("alice".into()),
            attached_by: Some("bob".into()),
            pending,
            review_rounds: 2,
            bursts: 5,
            updated_at: at(9, 30) + chrono::Duration::nanoseconds(123_456_789),
        }
    }

    fn add_task(db: &Db, repo: &str, issue: u64) {
        let task = Task::new(repo, issue, "t", "alice", Status::Ready, "build");
        db.upsert_task(&task).unwrap();
    }

    fn burst(issue: u64, started_at: DateTime<Utc>) -> BurstLog {
        BurstLog {
            repo: REPO.into(),
            issue,
            role: "build".into(),
            reason: "start".into(),
            started_at,
            ended_at: started_at + chrono::Duration::minutes(20),
            outcome: "pr".into(),
            session_id: Some(format!("sess-{issue}")),
            cost_usd: Some(1.25),
        }
    }

    fn challenge(token_hash: &str, email: &str, created_at: DateTime<Utc>) -> Challenge {
        Challenge {
            token_hash: token_hash.into(),
            email: email.into(),
            code_hash: "code-hash".into(),
            created_at,
            expires_at: created_at + chrono::Duration::minutes(10),
            attempts: 0,
        }
    }

    fn session(token_hash: &str, expires_at: DateTime<Utc>) -> ApiSession {
        ApiSession {
            token_hash: token_hash.into(),
            email: "alice@example.com".into(),
            expires_at,
        }
    }

    #[test]
    fn task_round_trips_every_field_and_pending_kind() -> Result<()> {
        let db = Db::open_in_memory()?;
        assert_eq!(db.task(REPO, 412)?, None);
        let reasons = [
            None,
            Some(BurstReason::Start),
            Some(BurstReason::Answer),
            Some(BurstReason::ReviewNotes {
                notes: "drop the helper".into(),
            }),
            Some(BurstReason::MergeCheckFailed {
                output: "1 test failed".into(),
            }),
            Some(BurstReason::RebaseConflict {
                output: "conflict in src/a.rs".into(),
            }),
            Some(BurstReason::AfterAttach),
            Some(BurstReason::Review),
        ];
        for pending in reasons {
            let task = full_task(pending);
            db.upsert_task(&task)?;
            assert_eq!(db.task(REPO, 412)?, Some(task));
        }
        let bare = Task::new(REPO, 7, "Fix login", "carol", Status::Asked, "build");
        db.upsert_task(&bare)?;
        assert_eq!(db.task(REPO, 7)?, Some(bare));
        assert_eq!(db.tasks(REPO)?.len(), 2);
        Ok(())
    }

    #[test]
    fn task_status_and_pending_are_stored_as_documented() -> Result<()> {
        let db = Db::open_in_memory()?;
        let mut task = full_task(Some(BurstReason::ReviewNotes { notes: "n".into() }));
        task.status = Status::OnMain;
        db.upsert_task(&task)?;
        let (status, pending): (String, String) =
            db.conn()
                .query_row("SELECT status, pending FROM tasks", [], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })?;
        assert_eq!(status, "on_main");
        assert_eq!(pending, r#"{"kind":"review_notes","notes":"n"}"#);
        Ok(())
    }

    #[test]
    fn numbers_beyond_i64_are_errors_not_wrapped() -> Result<()> {
        let db = Db::open_in_memory()?;
        let mut task = full_task(None);
        task.issue = i64::MAX as u64;
        db.upsert_task(&task)?;
        assert_eq!(db.task(REPO, task.issue)?, Some(task.clone()));

        task.issue = i64::MAX as u64 + 1;
        assert!(db.upsert_task(&task).is_err());
        assert!(db.task(REPO, u64::MAX).is_err());
        task.issue = 1;
        task.last_seen_comment = u64::MAX;
        assert!(db.upsert_task(&task).is_err());

        db.conn().execute("UPDATE tasks SET issue = -1", [])?;
        assert!(db.tasks(REPO).is_err());
        let id = db.start_e2e(REPO, at(1, 0))?;
        assert!(db.finish_e2e(id, at(2, 0), false, Some(u64::MAX)).is_err());
        Ok(())
    }

    #[test]
    fn tasks_come_by_issue_and_all_tasks_by_repo_then_issue() -> Result<()> {
        let db = Db::open_in_memory()?;
        for (repo, issue) in [
            (REPO, 30),
            ("acme/api", 5),
            (REPO, 2),
            (REPO, 11),
            ("acme/api", 1),
        ] {
            add_task(&db, repo, issue);
        }
        let issues = |tasks: Vec<Task>| {
            tasks
                .into_iter()
                .map(|t| (t.repo, t.issue))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            issues(db.tasks(REPO)?),
            [(REPO.into(), 2), (REPO.into(), 11), (REPO.into(), 30)]
        );
        assert_eq!(
            issues(db.all_tasks()?),
            [
                ("acme/api".into(), 1),
                ("acme/api".into(), 5),
                (REPO.into(), 2),
                (REPO.into(), 11),
                (REPO.into(), 30)
            ]
        );
        assert!(db.tasks("acme/none")?.is_empty());
        Ok(())
    }

    #[test]
    fn attach_refuses_a_second_login_and_detach_frees_it() -> Result<()> {
        let db = Db::open_in_memory()?;
        assert!(!db.attach(REPO, 1, "alice")?);
        add_task(&db, REPO, 1);
        assert!(db.attach(REPO, 1, "alice")?);
        assert!(db.attach(REPO, 1, "alice")?);
        assert!(!db.attach(REPO, 1, "bob")?);
        assert_eq!(
            db.task(REPO, 1)?.unwrap().attached_by.as_deref(),
            Some("alice")
        );

        db.detach(REPO, 1)?;
        assert_eq!(db.task(REPO, 1)?.unwrap().attached_by, None);
        assert!(db.attach(REPO, 1, "bob")?);
        db.detach(REPO, 99)?;
        Ok(())
    }

    #[test]
    fn writing_an_older_copy_keeps_who_is_attached() -> Result<()> {
        let db = Db::open_in_memory()?;
        add_task(&db, REPO, 1);
        let mut stale = db.task(REPO, 1)?.unwrap();
        assert!(db.attach(REPO, 1, "alice")?);
        stale.bursts += 1;
        db.upsert_task(&stale)?;
        let task = db.task(REPO, 1)?.unwrap();
        assert_eq!(task.attached_by.as_deref(), Some("alice"));
        assert_eq!(task.bursts, stale.bursts);
        Ok(())
    }

    #[test]
    fn attach_lets_one_login_win_a_race_across_connections() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("forge.db");
        let first = Arc::new(Db::open(&path)?);
        let second = Arc::new(Db::open(&path)?);
        add_task(&first, REPO, 1);
        let racers: Vec<_> = (0..8)
            .map(|i| {
                let db = if i % 2 == 0 {
                    first.clone()
                } else {
                    second.clone()
                };
                thread::spawn(move || db.attach(REPO, 1, &format!("user{i}")).unwrap())
            })
            .collect();
        let wins = racers
            .into_iter()
            .map(|h| h.join().unwrap())
            .filter(|won| *won)
            .count();
        assert_eq!(wins, 1);
        Ok(())
    }

    #[test]
    fn recent_bursts_are_newest_first_and_limited() -> Result<()> {
        let db = Db::open_in_memory()?;
        assert!(db.recent_bursts(5)?.is_empty());
        let mut bare = burst(3, at(10, 0));
        bare.session_id = None;
        bare.cost_usd = None;
        for log in [burst(1, at(8, 0)), bare.clone(), burst(2, at(9, 0))] {
            db.record_burst(&log)?;
        }
        assert_eq!(db.recent_bursts(2)?, [bare.clone(), burst(2, at(9, 0))]);
        assert_eq!(
            db.recent_bursts(10)?,
            [bare, burst(2, at(9, 0)), burst(1, at(8, 0))]
        );
        assert!(db.recent_bursts(0)?.is_empty());
        Ok(())
    }

    #[test]
    fn limits_upsert_by_label() -> Result<()> {
        let db = Db::open_in_memory()?;
        let full = Limits {
            five_hour_pct: Some(42.5),
            five_hour_resets: Some(at(14, 0)),
            week_pct: Some(61.0),
            week_resets: Some(at(23, 59)),
            read_at: at(9, 0),
        };
        let empty = Limits {
            five_hour_pct: None,
            five_hour_resets: None,
            week_pct: None,
            week_resets: None,
            read_at: at(9, 1),
        };
        db.save_limits("tan", &empty)?;
        db.save_limits("blue", &empty)?;
        db.save_limits("blue", &full)?;
        assert_eq!(
            db.all_limits()?,
            [("blue".into(), full), ("tan".into(), empty)]
        );
        Ok(())
    }

    #[test]
    fn challenge_lifecycle_counts_attempts() -> Result<()> {
        let db = Db::open_in_memory()?;
        assert_eq!(db.challenge("h1")?, None);
        assert!(db.bump_attempts("h1").is_err());

        db.put_challenge(&challenge("h1", "Alice@Example.COM", at(9, 0)))?;
        let stored = db.challenge("h1")?.unwrap();
        assert_eq!(stored, challenge("h1", "alice@example.com", at(9, 0)));
        assert_eq!(db.bump_attempts("h1")?, 1);
        assert_eq!(db.bump_attempts("h1")?, 2);
        assert_eq!(db.challenge("h1")?.unwrap().attempts, 2);

        db.put_challenge(&challenge("h1", "alice@example.com", at(9, 5)))?;
        assert_eq!(db.challenge("h1")?.unwrap().attempts, 0);
        db.delete_challenge("h1")?;
        assert_eq!(db.challenge("h1")?, None);
        Ok(())
    }

    #[test]
    fn challenges_since_counts_one_email_in_any_case() -> Result<()> {
        let db = Db::open_in_memory()?;
        db.put_challenge(&challenge("h1", "alice@example.com", at(8, 0)))?;
        db.put_challenge(&challenge("h2", "ALICE@example.com", at(9, 0)))?;
        db.put_challenge(&challenge("h3", "Alice@Example.com", at(9, 30)))?;
        db.put_challenge(&challenge("h4", "bob@example.com", at(9, 30)))?;
        assert_eq!(db.challenges_since("aLiCe@EXAMPLE.com", at(9, 0))?, 2);
        assert_eq!(db.challenges_since("alice@example.com", at(7, 0))?, 3);
        assert_eq!(db.challenges_since("alice@example.com", at(10, 0))?, 0);
        assert_eq!(db.challenges_since("carol@example.com", at(7, 0))?, 0);
        Ok(())
    }

    #[test]
    fn sessions_put_get_replace_and_delete() -> Result<()> {
        let db = Db::open_in_memory()?;
        assert_eq!(db.session("t1")?, None);
        let mut s = session("t1", at(12, 0));
        s.email = "Alice@Example.com".into();
        db.put_session(&s)?;
        assert_eq!(db.session("t1")?, Some(session("t1", at(12, 0))));
        db.put_session(&session("t1", at(13, 0)))?;
        assert_eq!(db.session("t1")?.unwrap().expires_at, at(13, 0));
        db.delete_session("t1")?;
        assert_eq!(db.session("t1")?, None);
        Ok(())
    }

    #[test]
    fn purge_expired_keeps_what_is_still_live() -> Result<()> {
        let db = Db::open_in_memory()?;
        let now = at(12, 0);
        db.put_challenge(&challenge("old", "a@example.com", at(11, 0)))?;
        db.put_challenge(&challenge("edge", "a@example.com", at(11, 50)))?;
        db.put_challenge(&challenge("new", "a@example.com", at(11, 55)))?;
        db.put_session(&session("old", now - chrono::Duration::seconds(1)))?;
        db.put_session(&session("edge", now))?;
        db.put_session(&session("new", at(13, 0)))?;
        db.purge_expired(now)?;
        assert_eq!(db.challenge("old")?, None);
        assert!(db.challenge("edge")?.is_some());
        assert!(db.challenge("new")?.is_some());
        assert_eq!(db.session("old")?, None);
        assert!(db.session("edge")?.is_some());
        assert!(db.session("new")?.is_some());
        Ok(())
    }

    #[test]
    fn e2e_runs_start_finish_and_last_is_latest_started() -> Result<()> {
        let db = Db::open_in_memory()?;
        assert_eq!(db.last_e2e(REPO)?, None);

        let first = db.start_e2e(REPO, at(6, 0))?;
        let unfinished = E2eRun {
            repo: REPO.into(),
            started_at: at(6, 0),
            finished_at: None,
            passed: None,
            issue: None,
        };
        assert_eq!(db.last_e2e(REPO)?, Some(unfinished));

        db.finish_e2e(first, at(6, 40), false, Some(501))?;
        let failed = E2eRun {
            repo: REPO.into(),
            started_at: at(6, 0),
            finished_at: Some(at(6, 40)),
            passed: Some(false),
            issue: Some(501),
        };
        assert_eq!(db.last_e2e(REPO)?, Some(failed));

        let second = db.start_e2e(REPO, at(12, 0))?;
        assert_ne!(first, second);
        db.start_e2e(REPO, at(3, 0))?;
        db.start_e2e("acme/api", at(23, 0))?;
        db.finish_e2e(second, at(12, 30), true, None)?;
        let last = db.last_e2e(REPO)?.unwrap();
        assert_eq!(
            (last.started_at, last.passed, last.issue),
            (at(12, 0), Some(true), None)
        );

        assert!(db.finish_e2e(9999, at(13, 0), true, None).is_err());
        Ok(())
    }

    #[test]
    fn kv_sets_and_overwrites() -> Result<()> {
        let db = Db::open_in_memory()?;
        assert_eq!(db.kv_get("cursor")?, None);
        db.kv_set("cursor", "1")?;
        db.kv_set("cursor", "2")?;
        db.kv_set("other", "x")?;
        assert_eq!(db.kv_get("cursor")?.as_deref(), Some("2"));
        assert_eq!(db.kv_get("other")?.as_deref(), Some("x"));
        Ok(())
    }

    #[test]
    fn open_sets_wal_busy_timeout_and_foreign_keys() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let db = Db::open(&dir.path().join("forge.db"))?;
        let conn = db.conn();
        let journal: String = conn.pragma_query_value(None, "journal_mode", |r| r.get(0))?;
        let busy_ms: i64 = conn.pragma_query_value(None, "busy_timeout", |r| r.get(0))?;
        let foreign_keys: bool = conn.pragma_query_value(None, "foreign_keys", |r| r.get(0))?;
        assert_eq!(
            (journal.as_str(), busy_ms, foreign_keys),
            ("wal", 5000, true)
        );
        Ok(())
    }

    #[test]
    fn reopening_keeps_data_and_does_not_migrate_again() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("forge.db");
        {
            let db = Db::open(&path)?;
            db.upsert_task(&full_task(Some(BurstReason::Start)))?;
            db.kv_set("cursor", "7")?;
        }
        // SCHEMA_V1's CREATE TABLEs would fail if this reopen ran it again.
        let db = Db::open(&path)?;
        assert_eq!(
            db.task(REPO, 412)?,
            Some(full_task(Some(BurstReason::Start)))
        );
        assert_eq!(db.kv_get("cursor")?.as_deref(), Some("7"));
        let version: usize = db
            .conn()
            .pragma_query_value(None, "user_version", |r| r.get(0))?;
        assert_eq!(version, MIGRATIONS.len());
        Ok(())
    }

    #[test]
    fn open_refuses_a_newer_schema() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("forge.db");
        Connection::open(&path)?.pragma_update(None, "user_version", 99)?;
        assert!(Db::open(&path).is_err());
        Ok(())
    }
}
