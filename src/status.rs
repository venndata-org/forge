//! What the dashboard, `forge status` and the MCP show: tasks by stage, the
//! pool, and what waits on the signed-in member.

use std::collections::HashMap;

use anyhow::Result;
use chrono::{DateTime, Utc};

use crate::config::{Config, Member, RepoConfig};
use crate::db::Db;
use crate::types::{PlanView, RepoView, Status, StatusView, Task, TaskLine, TaskView, TryCommands, WaitingItem};

/// How many tasks the On main list shows.
const ON_MAIN_SHOWN: usize = 10;
/// How far back to look for the burst that left a task waiting.
const BURSTS_SCANNED: usize = 1000;

/// Waiting on a member: Building tasks whose `waiting_on` is their login
/// ("question" or "blocked"), and Reviewed tasks ("verify") — every member
/// may verify, so all Reviewed tasks are listed, the member's own first.
pub fn build_status(cfg: &Config, db: &Db, pool: Vec<PlanView>, member: Option<&Member>, now: DateTime<Utc>) -> Result<StatusView> {
    let mut repos = Vec::new();
    let mut answers = Vec::new();
    let mut verify = Vec::new();
    for repo in &cfg.repos {
        let tasks = db.tasks(&repo.name)?;
        if let Some(m) = member.filter(|m| repo.is_member(&m.github)) {
            answers.extend(tasks.iter().filter(|t| t.status == Status::Building && is(&t.waiting_on, &m.github)).cloned());
            verify.extend(tasks.iter().filter(|t| t.status == Status::Reviewed).cloned());
        }
        repos.push(repo_view(repo, &tasks, db)?);
    }
    let mut waiting_on_you: Vec<WaitingItem> = Vec::new();
    if !answers.is_empty() {
        let asked = asked(db)?;
        for t in &answers {
            let why = if asked.get(&(t.repo.clone(), t.issue)) == Some(&true) { "question" } else { "blocked" };
            let login = t.waiting_on.as_deref().unwrap_or_default();
            waiting_on_you.push(waiting(t, why, format!("waiting on @{login}: answer on the issue")));
        }
    }
    if let Some(m) = member {
        verify.sort_by_key(|t| !t.author.eq_ignore_ascii_case(&m.github));
    }
    for t in &verify {
        waiting_on_you.push(waiting(t, "verify", format!("verify with: forge try {}", t.issue)));
    }
    Ok(StatusView { generated_at: now, repos, pool, waiting_on_you, you: member.map(|m| m.github.clone()) })
}

/// One task for `forge try`: its branch and the repo's try commands.
pub fn task_view(cfg: &Config, db: &Db, repo: &str, issue: u64) -> Result<Option<TaskView>> {
    let Some(rc) = cfg.repo(repo) else { return Ok(None) };
    let Some(t) = db.task(&rc.name, issue)? else { return Ok(None) };
    Ok(Some(TaskView {
        repo: t.repo,
        issue: t.issue,
        title: t.title,
        status: t.status,
        branch: t.branch,
        pr_url: t.pr_url,
        attached_by: t.attached_by,
        try_commands: TryCommands {
            build: rc.build.clone(),
            seed: rc.seed.clone(),
            run: rc.run.clone(),
            stop: rc.stop.clone(),
        },
    }))
}

/// An issue's web address.
pub fn issue_url(repo: &str, issue: u64) -> String {
    format!("https://github.com/{repo}/issues/{issue}")
}

fn repo_view(repo: &RepoConfig, tasks: &[Task], db: &Db) -> Result<RepoView> {
    let lines = |status: Status| -> Vec<TaskLine> { tasks.iter().filter(|t| t.status == status).map(line).collect() };
    let mut merged: Vec<&Task> = tasks.iter().filter(|t| t.status == Status::OnMain).collect();
    merged.sort_by_key(|t| std::cmp::Reverse(t.updated_at));
    Ok(RepoView {
        repo: repo.name.clone(),
        concurrency: repo.concurrency,
        orderer: repo.orderer.clone(),
        backlog: lines(Status::Backlog),
        asked: lines(Status::Asked),
        ready: lines(Status::Ready),
        building: lines(Status::Building),
        reviewed: lines(Status::Reviewed),
        accepted: lines(Status::Accepted),
        on_main: merged.into_iter().take(ON_MAIN_SHOWN).map(line).collect(),
        last_e2e: db.last_e2e(&repo.name)?,
    })
}

fn line(t: &Task) -> TaskLine {
    TaskLine {
        issue: t.issue,
        title: t.title.clone(),
        status: t.status,
        role: t.role.clone(),
        waiting_on: t.waiting_on.clone(),
        attached_by: t.attached_by.clone(),
        pr_url: t.pr_url.clone(),
        url: issue_url(&t.repo, t.issue),
    }
}

fn waiting(t: &Task, why: &str, detail: String) -> WaitingItem {
    WaitingItem {
        repo: t.repo.clone(),
        issue: t.issue,
        title: t.title.clone(),
        why: why.to_string(),
        detail,
        url: issue_url(&t.repo, t.issue),
    }
}

fn is(login: &Option<String>, github: &str) -> bool {
    login.as_deref().is_some_and(|l| l.eq_ignore_ascii_case(github))
}

/// Whether each task's newest burst ended in a question; a wait that didn't is a block.
fn asked(db: &Db) -> Result<HashMap<(String, u64), bool>> {
    let mut out = HashMap::new();
    for b in db.recent_bursts(BURSTS_SCANNED)? {
        out.entry((b.repo, b.issue)).or_insert(b.outcome == "question");
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use chrono::Duration;

    use super::*;
    use crate::types::{BurstLog, Limits};

    const REPO: &str = "example-team/shop";

    fn t0() -> DateTime<Utc> {
        "2026-10-20T08:00:00Z".parse().unwrap()
    }

    fn task(issue: u64, status: Status, author: &str) -> Task {
        let mut t = Task::new(REPO, issue, &format!("Task {issue}"), author, status, "backend");
        t.updated_at = t0();
        t
    }

    fn burst(db: &Db, issue: u64, outcome: &str, at: DateTime<Utc>) {
        db.record_burst(&BurstLog {
            repo: REPO.into(),
            issue,
            role: "backend".into(),
            reason: "start".into(),
            started_at: at,
            ended_at: at,
            outcome: outcome.into(),
            session_id: None,
            cost_usd: None,
        })
        .unwrap();
    }

    fn bob(cfg: &Config) -> Member {
        cfg.member_by_github("bob-gh").unwrap().clone()
    }

    #[test]
    fn waiting_on_you_lists_answers_then_own_verifies_then_others() {
        let cfg = crate::config::sample();
        let db = Db::open_in_memory().unwrap();
        let mut asked = task(5, Status::Building, "ann-gh");
        asked.waiting_on = Some("Bob-GH".into());
        let mut blocked = task(9, Status::Building, "bob-gh");
        blocked.waiting_on = Some("bob-gh".into());
        let mut not_mine = task(11, Status::Building, "ann-gh");
        not_mine.waiting_on = Some("ann-gh".into());
        for t in [asked, blocked, not_mine, task(2, Status::Reviewed, "ann-gh"), task(7, Status::Reviewed, "bob-gh")] {
            db.upsert_task(&t).unwrap();
        }
        db.upsert_task(&task(3, Status::Building, "bob-gh")).unwrap();
        burst(&db, 5, "blocked", t0() - Duration::hours(2));
        burst(&db, 5, "question", t0() - Duration::hours(1));
        burst(&db, 9, "pr", t0());

        let v = build_status(&cfg, &db, vec![], Some(&bob(&cfg)), t0()).unwrap();
        let got: Vec<(u64, &str, &str)> =
            v.waiting_on_you.iter().map(|w| (w.issue, w.why.as_str(), w.detail.as_str())).collect();
        assert_eq!(
            got,
            vec![
                (5, "question", "waiting on @Bob-GH: answer on the issue"),
                (9, "blocked", "waiting on @bob-gh: answer on the issue"),
                (7, "verify", "verify with: forge try 7"),
                (2, "verify", "verify with: forge try 2"),
            ]
        );
        assert_eq!(v.waiting_on_you[0].url, "https://github.com/example-team/shop/issues/5");
        assert_eq!(v.waiting_on_you[0].title, "Task 5");

        let signed_out = build_status(&cfg, &db, vec![], None, t0()).unwrap();
        assert!(signed_out.waiting_on_you.is_empty());
    }

    #[test]
    fn repos_show_each_stage_and_newest_merges() {
        let cfg = crate::config::sample();
        let db = Db::open_in_memory().unwrap();
        let mut building = task(1, Status::Building, "ann-gh");
        building.attached_by = Some("ann-gh".into());
        building.pr_url = Some("https://github.com/example-team/shop/pull/40".into());
        db.upsert_task(&building).unwrap();
        db.upsert_task(&task(2, Status::Reviewed, "ann-gh")).unwrap();
        db.upsert_task(&task(3, Status::Accepted, "ann-gh")).unwrap();
        db.upsert_task(&task(4, Status::Ready, "ann-gh")).unwrap();
        for i in 0..12 {
            let mut t = task(100 + i, Status::OnMain, "ann-gh");
            t.updated_at = t0() + Duration::minutes(i as i64);
            db.upsert_task(&t).unwrap();
        }
        let run = db.start_e2e(REPO, t0()).unwrap();
        db.finish_e2e(run, t0() + Duration::minutes(20), false, Some(140)).unwrap();
        let pool = vec![PlanView {
            label: "blue".into(),
            limits: Some(Limits { five_hour_pct: Some(40.0), five_hour_resets: None, week_pct: Some(61.0), week_resets: None, read_at: t0() }),
            at_ceiling: false,
            out_until: None,
            out_reason: None,
        }];

        let v = build_status(&cfg, &db, pool.clone(), None, t0()).unwrap();
        assert_eq!((v.generated_at, v.pool), (t0(), pool));
        let r = &v.repos[0];
        assert_eq!((r.repo.as_str(), r.concurrency), (REPO, 1));
        assert_eq!(r.building.len(), 1);
        let b = &r.building[0];
        assert_eq!(b.url, "https://github.com/example-team/shop/issues/1");
        assert_eq!((b.attached_by.as_deref(), b.role.as_str()), (Some("ann-gh"), "backend"));
        assert_eq!(b.pr_url.as_deref(), Some("https://github.com/example-team/shop/pull/40"));
        assert_eq!(r.reviewed.iter().map(|t| t.issue).collect::<Vec<_>>(), vec![2]);
        assert_eq!(r.accepted.iter().map(|t| t.issue).collect::<Vec<_>>(), vec![3]);
        assert_eq!(r.on_main.iter().map(|t| t.issue).collect::<Vec<_>>(), (102..112).rev().collect::<Vec<_>>());
        let e2e = r.last_e2e.as_ref().unwrap();
        assert_eq!((e2e.passed, e2e.issue), (Some(false), Some(140)));
    }

    #[test]
    fn task_view_carries_the_try_commands() {
        let cfg = crate::config::sample();
        let db = Db::open_in_memory().unwrap();
        let mut t = task(412, Status::Reviewed, "bob-gh");
        t.pr_url = Some("https://github.com/example-team/shop/pull/9".into());
        db.upsert_task(&t).unwrap();

        let v = task_view(&cfg, &db, "Example-Team/SHOP", 412).unwrap().unwrap();
        assert_eq!((v.repo.as_str(), v.issue, v.branch.as_str()), (REPO, 412, "forge/412"));
        assert_eq!((v.status, v.pr_url), (Status::Reviewed, t.pr_url));
        assert_eq!(
            v.try_commands,
            TryCommands {
                build: Some("cargo build".into()),
                seed: Some("./scripts/seed-synthetic.sh".into()),
                run: Some("./scripts/dev.sh".into()),
                stop: Some("./scripts/dev.sh stop".into()),
            }
        );
        assert!(task_view(&cfg, &db, REPO, 413).unwrap().is_none());
        assert!(task_view(&cfg, &db, "example-team/other", 412).unwrap().is_none());
    }
}
