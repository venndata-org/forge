//! The Claude plan pool: each request goes
//! to the plan with the most room, and no new request goes to a plan past its
//! 5-hour or weekly ceiling. Only this module ever holds a plan's login.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, TimeZone, Utc};
use reqwest::header::HeaderMap;

use crate::config::{read_secret, PoolConfig};
use crate::types::{Limits, PlanView};

/// Limits read longer ago than this count as unknown: a plan nothing has
/// read for a day is tried again rather than trusted.
const STALE_AFTER: Duration = Duration::hours(24);

/// How long a holder keeps its plan after its last request: Claude's prompt
/// cache lives at most an hour, and is lost when a session changes plans.
const HOLD_FOR: Duration = Duration::hours(1);

/// A plan's login, lent for one request. Its Debug never shows the token.
pub struct Lease {
    pub label: String,
    token: String,
}

impl Lease {
    pub fn token(&self) -> &str {
        &self.token
    }
}

impl std::fmt::Debug for Lease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lease").field("label", &self.label).finish_non_exhaustive()
    }
}

pub struct Pool {
    plans: RwLock<Vec<PlanState>>,
    five_hour_cap: f64,
    week_cap: f64,
    /// Choices made by room, so tied plans take turns.
    turns: AtomicUsize,
    /// The plan each holder (a task's key tag) last got, and when.
    held: Mutex<HashMap<String, (String, DateTime<Utc>)>>,
}

struct PlanState {
    label: String,
    token: String,
    limits: Option<Limits>,
    out_until: Option<DateTime<Utc>>,
    out_reason: Option<String>,
}

impl Pool {
    /// Reads each plan's token file (see `config::read_secret`).
    pub fn load(cfg: &PoolConfig) -> Result<Pool> {
        let mut plans = Vec::new();
        for plan in &cfg.plans {
            let token = read_secret(&plan.token_file).with_context(|| format!("plan {}", plan.label))?;
            if !token_shaped(&token) {
                bail!("plan {}'s token isn't one line of letters, digits, dashes and underscores", plan.label);
            }
            plans.push((plan.label.clone(), token));
        }
        Ok(Pool::from_tokens(plans, cfg.five_hour_cap, cfg.week_cap))
    }

    /// For tests and the daemon: plans given as (label, token).
    pub fn from_tokens(plans: Vec<(String, String)>, five_hour_cap: f64, week_cap: f64) -> Pool {
        let plans = plans
            .into_iter()
            .map(|(label, token)| PlanState { label, token, limits: None, out_until: None, out_reason: None })
            .collect();
        Pool {
            plans: RwLock::new(plans),
            five_hour_cap,
            week_cap,
            turns: AtomicUsize::new(0),
            held: Mutex::default(),
        }
    }

    /// The plan with the most room that isn't out or at a ceiling. None when every plan is out or at a ceiling.
    pub fn pick(&self, now: DateTime<Utc>) -> Option<Lease> {
        self.choose(None, now)
    }

    /// Like `pick`, but never `skip`: for retrying a request elsewhere.
    pub fn pick_other(&self, skip: &str, now: DateTime<Utc>) -> Option<Lease> {
        self.choose(Some(skip), now)
    }

    /// Like `pick` (or `pick_other` with `skip`), but keeps `holder` on the plan
    /// it last got while that plan is open:
    /// a session that changes plans loses its prompt cache.
    pub fn pick_for(&self, holder: &str, skip: Option<&str>, now: DateTime<Utc>) -> Option<Lease> {
        let mut held = self.held.lock().unwrap();
        held.retain(|_, (_, at)| now - *at < HOLD_FOR);
        let kept = held
            .get(holder)
            .map(|(label, _)| label.as_str())
            .filter(|label| Some(*label) != skip)
            .and_then(|label| self.lease_if_open(label, now));
        let lease = match kept {
            Some(lease) => lease,
            None => self.choose(skip, now)?,
        };
        held.insert(holder.to_string(), (lease.label.clone(), now));
        Some(lease)
    }

    /// Stores limits read from a response for this plan.
    pub fn record(&self, label: &str, limits: Limits) {
        if let Some(plan) = self.plans.write().unwrap().iter_mut().find(|p| p.label == label) {
            plan.limits = Some(limits);
        }
    }

    /// Keeps new requests off a plan until `until` (a 429, or a rejected login).
    pub fn mark_out(&self, label: &str, until: DateTime<Utc>, reason: &str) {
        if let Some(plan) = self.plans.write().unwrap().iter_mut().find(|p| p.label == label) {
            plan.out_until = Some(until);
            plan.out_reason = Some(reason.to_string());
        }
    }

    /// Plans whose usage is unknown or older than `every`, and not kept out:
    /// the ones a usage check should ask about.
    pub fn due_for_check(&self, now: DateTime<Utc>, every: Duration) -> Vec<Lease> {
        let plans = self.plans.read().unwrap();
        plans
            .iter()
            .filter(|p| p.out_until.is_none_or(|until| until <= now))
            .filter(|p| p.limits.as_ref().is_none_or(|l| now - l.read_at >= every))
            .map(lease)
            .collect()
    }

    /// Seeds limits saved before a restart.
    pub fn restore(&self, saved: Vec<(String, Limits)>) {
        let mut plans = self.plans.write().unwrap();
        for (label, limits) in saved {
            if let Some(plan) = plans.iter_mut().find(|p| p.label == label) {
                if plan.limits.as_ref().is_none_or(|l| l.read_at < limits.read_at) {
                    plan.limits = Some(limits);
                }
            }
        }
    }

    /// Every plan, for the dashboard. Never includes a login.
    pub fn snapshot(&self, now: DateTime<Utc>) -> Vec<PlanView> {
        self.plans
            .read()
            .unwrap()
            .iter()
            .map(|p| {
                let out_until = p.out_until.filter(|until| *until > now);
                PlanView {
                    label: p.label.clone(),
                    limits: p.limits.clone(),
                    at_ceiling: self.at_ceiling(p.limits.as_ref(), now),
                    out_until,
                    out_reason: out_until.and(p.out_reason.clone()),
                }
            })
            .collect()
    }

    fn choose(&self, skip: Option<&str>, now: DateTime<Utc>) -> Option<Lease> {
        let plans = self.plans.read().unwrap();
        let open: Vec<(&str, Option<&Limits>)> = plans
            .iter()
            .filter(|p| Some(p.label.as_str()) != skip && self.is_open(p, now))
            .map(|p| (p.label.as_str(), p.limits.as_ref()))
            .collect();
        let label = most_room(&open, now, self.turns.fetch_add(1, Ordering::Relaxed))?;
        plans.iter().find(|p| p.label == label).map(lease)
    }

    fn lease_if_open(&self, label: &str, now: DateTime<Utc>) -> Option<Lease> {
        let plans = self.plans.read().unwrap();
        plans.iter().find(|p| p.label == label && self.is_open(p, now)).map(lease)
    }

    fn is_open(&self, plan: &PlanState, now: DateTime<Utc>) -> bool {
        plan.out_until.is_none_or(|until| until <= now) && !self.at_ceiling(plan.limits.as_ref(), now)
    }

    /// The 5-hour or weekly window at or over
    /// its cap. A window that has reset, and unknown or stale limits, never are.
    fn at_ceiling(&self, limits: Option<&Limits>, now: DateTime<Utc>) -> bool {
        let Some(l) = fresh(limits, now) else { return false };
        let over = |pct: Option<f64>, resets: Option<DateTime<Utc>>, cap: f64| {
            pct.is_some_and(|p| p >= cap) && resets.is_none_or(|r| r > now)
        };
        over(l.five_hour_pct, l.five_hour_resets, self.five_hour_cap) || over(l.week_pct, l.week_resets, self.week_cap)
    }
}

fn lease(plan: &PlanState) -> Lease {
    Lease { label: plan.label.clone(), token: plan.token.clone() }
}

/// One line of letters, digits, dashes and underscores.
fn token_shaped(token: &str) -> bool {
    (20..=1000).contains(&token.len()) && token.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn fresh(limits: Option<&Limits>, now: DateTime<Utc>) -> Option<&Limits> {
    limits.filter(|l| now - l.read_at <= STALE_AFTER)
}

/// A window's percent used now: 0 once it has reset since the read.
fn used(pct: Option<f64>, resets: Option<DateTime<Utc>>, now: DateTime<Utc>) -> Option<f64> {
    pct.map(|p| if resets.is_some_and(|r| r <= now) { 0.0 } else { p })
}

/// A plan known to have room before one whose room is
/// unknown, and that before one known to be full. Plans that tie take turns
/// by label: `turn` counts the choices made, so requests spread among them.
fn most_room<'a>(plans: &[(&'a str, Option<&Limits>)], now: DateTime<Utc>, turn: usize) -> Option<&'a str> {
    let ranked: Vec<(&str, f64)> = plans
        .iter()
        .map(|(label, limits)| {
            let rank = match room_of(*limits, now) {
                None => 0.0,
                Some(room) if room > 0.0 => room,
                Some(_) => -1.0,
            };
            (*label, rank)
        })
        .collect();
    let best = ranked.iter().map(|(_, rank)| *rank).reduce(f64::max)?;
    let mut tied: Vec<&str> = ranked.iter().filter(|(_, rank)| *rank == best).map(|(label, _)| *label).collect();
    tied.sort_unstable();
    Some(tied[turn % tied.len()])
}

/// Reads `anthropic-ratelimit-unified-{5h,7d}-utilization` and `-reset`
/// headers. None when neither window is there.
pub fn limits_from_headers(headers: &HeaderMap, now: DateTime<Utc>) -> Option<Limits> {
    let (five_hour_pct, five_hour_resets) = window(headers, "5h");
    let (week_pct, week_resets) = window(headers, "7d");
    if five_hour_pct.is_none() && week_pct.is_none() {
        return None;
    }
    Some(Limits { five_hour_pct, five_hour_resets, week_pct, week_resets, read_at: now })
}

/// When a plan Anthropic refused may take requests again: the
/// `anthropic-ratelimit-unified-reset` header, in epoch seconds.
pub fn reset_from_headers(headers: &HeaderMap) -> Option<DateTime<Utc>> {
    epoch(header_number(headers, "anthropic-ratelimit-unified-reset")?)
}

/// One window's percent used and when it resets. The header gives use as a
/// fraction (0.42 is 42%), held to 1000%, and the reset in epoch seconds.
fn window(headers: &HeaderMap, key: &str) -> (Option<f64>, Option<DateTime<Utc>>) {
    let fraction = header_number(headers, &format!("anthropic-ratelimit-unified-{key}-utilization"));
    let Some(fraction) = fraction.filter(|f| *f >= 0.0) else { return (None, None) };
    let resets = header_number(headers, &format!("anthropic-ratelimit-unified-{key}-reset")).and_then(epoch);
    (Some((fraction * 100.0).min(1000.0)), resets)
}

fn header_number(headers: &HeaderMap, name: &str) -> Option<f64> {
    let text = headers.get(name)?.to_str().ok()?;
    text.trim().parse::<f64>().ok().filter(|n| n.is_finite())
}

/// Epoch seconds as a time, refusing what can't be one (0 < s < 1e11).
fn epoch(seconds: f64) -> Option<DateTime<Utc>> {
    if !(seconds > 0.0 && seconds < 1e11) {
        return None;
    }
    Utc.timestamp_millis_opt((seconds * 1000.0) as i64).single()
}

/// The percent of room a plan has left: the smaller of
/// the 5-hour and weekly room; None when its limits are unknown or stale.
pub fn room_of(limits: Option<&Limits>, now: DateTime<Utc>) -> Option<f64> {
    let l = fresh(limits, now)?;
    let five = used(l.five_hour_pct, l.five_hour_resets, now);
    let week = used(l.week_pct, l.week_resets, now);
    let fullest = five.into_iter().chain(week).reduce(f64::max)?;
    Some((100.0 - fullest).max(0.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;

    const T0: i64 = 1_760_000_000;

    fn now() -> DateTime<Utc> {
        Utc.timestamp_opt(T0, 0).unwrap()
    }

    fn mins(n: i64) -> Duration {
        Duration::minutes(n)
    }

    fn limits(five: Option<f64>, week: Option<f64>) -> Limits {
        Limits {
            five_hour_pct: five,
            five_hour_resets: Some(now() + Duration::hours(2)),
            week_pct: week,
            week_resets: Some(now() + Duration::days(3)),
            read_at: now(),
        }
    }

    #[test]
    fn plans_with_unknown_or_old_usage_are_due_for_a_check() {
        let p = pool(&["a", "b", "c", "d"]);
        p.record("b", limits(Some(10.0), Some(10.0)));
        let mut old = limits(Some(10.0), Some(10.0));
        old.read_at = now() - Duration::minutes(45);
        p.record("c", old);
        p.mark_out("d", now() + Duration::minutes(5), "rate limited");
        let due: Vec<String> = p.due_for_check(now(), Duration::minutes(30)).into_iter().map(|l| l.label).collect();
        assert_eq!(due, ["a", "c"]);
    }

    fn pool(labels: &[&str]) -> Pool {
        let plans = labels.iter().map(|l| (l.to_string(), format!("token-{l}-0123456789abcdef"))).collect();
        Pool::from_tokens(plans, 85.0, 90.0)
    }

    fn picked(pool: &Pool, at: DateTime<Utc>) -> Option<String> {
        pool.pick(at).map(|l| l.label)
    }

    #[test]
    fn room_is_the_fuller_window_left() {
        assert_eq!(room_of(None, now()), None);
        assert_eq!(room_of(Some(&limits(None, None)), now()), None);
        assert_eq!(room_of(Some(&limits(Some(30.0), Some(60.0))), now()), Some(40.0));
        assert_eq!(room_of(Some(&limits(Some(70.0), None)), now()), Some(30.0));
        assert_eq!(room_of(Some(&limits(Some(130.0), Some(10.0))), now()), Some(0.0));
    }

    #[test]
    fn a_window_that_has_reset_counts_as_empty() {
        let mut l = limits(Some(80.0), Some(20.0));
        l.five_hour_resets = Some(now() - mins(1));
        assert_eq!(room_of(Some(&l), now()), Some(80.0));
    }

    #[test]
    fn stale_limits_are_unknown() {
        let mut l = limits(Some(95.0), Some(95.0));
        l.five_hour_resets = None;
        l.week_resets = None;
        l.read_at = now() - Duration::hours(25);
        assert_eq!(room_of(Some(&l), now()), None);
        let p = pool(&["a"]);
        p.record("a", l);
        assert_eq!(picked(&p, now()).as_deref(), Some("a"));
        assert!(!p.snapshot(now())[0].at_ceiling);
    }

    #[test]
    fn picks_the_plan_with_most_room() {
        let p = pool(&["a", "b", "c"]);
        p.record("a", limits(Some(50.0), Some(10.0)));
        p.record("b", limits(Some(20.0), Some(30.0)));
        p.record("c", limits(Some(10.0), Some(60.0)));
        assert_eq!(picked(&p, now()).as_deref(), Some("b"));
        assert_eq!(picked(&p, now()).as_deref(), Some("b"));
    }

    #[test]
    fn known_room_beats_unknown_and_unknown_beats_full() {
        let full = limits(Some(100.0), Some(10.0));
        let some = limits(Some(80.0), None);
        let plans = [("full", Some(&full)), ("unknown", None), ("some", Some(&some))];
        assert_eq!(most_room(&plans, now(), 0), Some("some"));
        assert_eq!(most_room(&plans[..2], now(), 0), Some("unknown"));
        assert_eq!(most_room(&plans[..1], now(), 0), Some("full"));
        assert_eq!(most_room(&[], now(), 0), None);
    }

    #[test]
    fn ties_take_turns_by_label() {
        let p = pool(&["c", "a", "b"]);
        let order: Vec<String> = (0..4).filter_map(|_| picked(&p, now())).collect();
        assert_eq!(order, ["a", "b", "c", "a"]);
        p.record("a", limits(Some(40.0), None));
        p.record("b", limits(Some(40.0), None));
        p.record("c", limits(Some(70.0), None));
        let order: Vec<String> = (0..3).filter_map(|_| picked(&p, now())).collect();
        assert_eq!(order, ["a", "b", "a"]);
    }

    #[test]
    fn five_hour_ceiling_keeps_a_plan_out() {
        let p = pool(&["a", "b"]);
        p.record("a", limits(Some(85.0), Some(0.0)));
        p.record("b", limits(Some(84.9), Some(0.0)));
        assert_eq!(picked(&p, now()).as_deref(), Some("b"));
        p.record("b", limits(Some(86.0), Some(0.0)));
        assert_eq!(picked(&p, now()), None);
        let views = p.snapshot(now());
        assert!(views.iter().all(|v| v.at_ceiling));
    }

    #[test]
    fn week_ceiling_keeps_a_plan_out() {
        let p = pool(&["a", "b"]);
        p.record("a", limits(Some(0.0), Some(90.0)));
        p.record("b", limits(Some(80.0), Some(89.0)));
        assert_eq!(picked(&p, now()).as_deref(), Some("b"));
        assert_eq!(p.pick_other("b", now()).map(|l| l.label), None);
    }

    #[test]
    fn a_ceiling_lifts_when_its_window_resets() {
        let p = pool(&["a"]);
        p.record("a", limits(Some(90.0), Some(10.0)));
        assert_eq!(picked(&p, now()), None);
        assert_eq!(picked(&p, now() + Duration::hours(2)).as_deref(), Some("a"));
    }

    #[test]
    fn out_until_holds_then_expires() {
        let p = pool(&["a", "b"]);
        p.mark_out("a", now() + mins(5), "rate limited");
        assert_eq!(picked(&p, now()).as_deref(), Some("b"));
        assert_eq!(picked(&p, now()).as_deref(), Some("b"));
        assert_eq!(p.pick_other("b", now()).map(|l| l.label), None);
        let view = &p.snapshot(now())[0];
        assert_eq!(view.out_until, Some(now() + mins(5)));
        assert_eq!(view.out_reason.as_deref(), Some("rate limited"));
        let later = now() + mins(6);
        assert_eq!(p.pick_other("b", later).map(|l| l.label).as_deref(), Some("a"));
        let view = &p.snapshot(later)[0];
        assert_eq!((view.out_until, view.out_reason.as_deref()), (None, None));
    }

    #[test]
    fn pick_other_never_gives_the_skipped_plan() {
        let p = pool(&["a", "b"]);
        p.record("a", limits(Some(10.0), None));
        p.record("b", limits(Some(50.0), None));
        assert_eq!(p.pick_other("a", now()).map(|l| l.label).as_deref(), Some("b"));
    }

    #[test]
    fn a_holder_keeps_its_plan_while_it_is_open() {
        let p = pool(&["a", "b"]);
        p.record("a", limits(Some(10.0), None));
        p.record("b", limits(Some(50.0), None));
        let first = p.pick_for("repo#1", None, now()).unwrap();
        assert_eq!(first.label, "a");
        p.record("a", limits(Some(60.0), None));
        assert_eq!(p.pick_for("repo#1", None, now()).unwrap().label, "a");
        assert_eq!(p.pick_for("repo#2", None, now()).unwrap().label, "b");
        assert_eq!(p.pick_for("repo#1", Some("a"), now()).unwrap().label, "b");
        assert_eq!(p.pick_for("repo#1", None, now()).unwrap().label, "b");
        p.record("b", limits(Some(85.0), None));
        assert_eq!(p.pick_for("repo#1", None, now()).unwrap().label, "a");
    }

    #[test]
    fn a_hold_lapses_after_an_hour() {
        let p = pool(&["a", "b"]);
        p.record("a", limits(Some(10.0), None));
        p.record("b", limits(Some(50.0), None));
        assert_eq!(p.pick_for("repo#1", None, now()).unwrap().label, "a");
        p.record("a", limits(Some(70.0), None));
        let later = now() + Duration::minutes(61);
        assert_eq!(p.pick_for("repo#1", None, later).unwrap().label, "b");
    }

    #[test]
    fn restore_seeds_but_never_overwrites_newer() {
        let p = pool(&["a", "b"]);
        let mut newer = limits(Some(20.0), None);
        newer.read_at = now() + mins(1);
        p.record("a", newer.clone());
        p.restore(vec![
            ("a".into(), limits(Some(70.0), None)),
            ("b".into(), limits(Some(30.0), None)),
            ("gone".into(), limits(Some(1.0), None)),
        ]);
        let views = p.snapshot(now());
        assert_eq!(views[0].limits, Some(newer));
        assert_eq!(views[1].limits.as_ref().and_then(|l| l.five_hour_pct), Some(30.0));
        assert_eq!(views.len(), 2);
    }

    #[test]
    fn no_token_in_debug_or_snapshot() {
        let p = pool(&["a"]);
        let lease = p.pick(now()).unwrap();
        assert_eq!(lease.token(), "token-a-0123456789abcdef");
        assert!(!format!("{lease:?}").contains("token-a"));
        assert!(!serde_json::to_string(&p.snapshot(now())).unwrap().contains("token-a"));
    }

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(*k, HeaderValue::from_str(v).unwrap());
        }
        h
    }

    #[test]
    fn reads_both_windows_from_headers() {
        let reset5 = T0 + 3600;
        let reset7 = T0 + 86_400;
        let h = headers(&[
            ("anthropic-ratelimit-unified-5h-utilization", "0.42"),
            ("anthropic-ratelimit-unified-5h-reset", &reset5.to_string()),
            ("anthropic-ratelimit-unified-7d-utilization", " 0.9 "),
            ("anthropic-ratelimit-unified-7d-reset", &reset7.to_string()),
        ]);
        let l = limits_from_headers(&h, now()).unwrap();
        assert!((l.five_hour_pct.unwrap() - 42.0).abs() < 1e-9);
        assert_eq!(l.five_hour_resets, Utc.timestamp_opt(reset5, 0).single());
        assert!((l.week_pct.unwrap() - 90.0).abs() < 1e-9);
        assert_eq!(l.week_resets, Utc.timestamp_opt(reset7, 0).single());
        assert_eq!(l.read_at, now());
    }

    #[test]
    fn header_edge_cases() {
        assert_eq!(limits_from_headers(&HeaderMap::new(), now()), None);
        let only_week = headers(&[("anthropic-ratelimit-unified-7d-utilization", "0.1")]);
        let l = limits_from_headers(&only_week, now()).unwrap();
        assert_eq!((l.five_hour_pct, l.week_resets), (None, None));
        let bad = headers(&[
            ("anthropic-ratelimit-unified-5h-utilization", "-0.1"),
            ("anthropic-ratelimit-unified-7d-utilization", "NaN"),
        ]);
        assert_eq!(limits_from_headers(&bad, now()), None);
        let wild = headers(&[
            ("anthropic-ratelimit-unified-5h-utilization", "12"),
            ("anthropic-ratelimit-unified-5h-reset", "1e12"),
        ]);
        let l = limits_from_headers(&wild, now()).unwrap();
        assert_eq!((l.five_hour_pct, l.five_hour_resets), (Some(1000.0), None));
        let reset = headers(&[("anthropic-ratelimit-unified-reset", &(T0 + 60).to_string())]);
        assert_eq!(reset_from_headers(&reset), Some(now() + mins(1)));
        assert_eq!(reset_from_headers(&HeaderMap::new()), None);
    }

    #[test]
    fn load_reads_token_files_and_refuses_odd_ones() {
        use std::os::unix::fs::PermissionsExt;
        // Under target/, which exists only after a build that writes it there.
        let base = concat!(env!("CARGO_MANIFEST_DIR"), "/target");
        std::fs::create_dir_all(base).unwrap();
        let dir = tempfile::tempdir_in(base).unwrap();
        let write = |name: &str, text: &str| {
            let path = dir.path().join(name);
            std::fs::write(&path, text).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
            path
        };
        let plan = |label: &str, token_file| crate::config::PlanConfig { label: label.into(), token_file };
        let mut cfg = PoolConfig {
            plans: vec![plan("a", write("a", "fake-token-aaaaaaaaaaaaaaaa\n"))],
            five_hour_cap: 85.0,
            week_cap: 90.0,
            upstream: "http://127.0.0.1:1".into(),
        };
        let p = Pool::load(&cfg).unwrap();
        assert_eq!(p.pick(now()).unwrap().token(), "fake-token-aaaaaaaaaaaaaaaa");
        cfg.plans.push(plan("b", write("b", "{\"not\": \"a token\"}")));
        let err = format!("{:#}", Pool::load(&cfg).err().unwrap());
        assert!(err.contains("plan b") && !err.contains("\"not\""), "{err}");
    }
}
