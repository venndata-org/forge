//! The HTTP side: the dashboard and API on `listen` (behind TLS), and the
//! loopback-only internal API `forge attach-gate` uses on `internal_listen`.
//!
//! Public routes:
//!   GET  /                          dashboard (sign-in form when signed out)
//!   GET  /healthz                   "ok"
//!   GET  /install.sh                the one-line installer, for `curl … | sh`
//!   GET  /download/:file            the CLI builds in <data_dir>/dist (forge-darwin-arm64, forge-linux-x86_64)
//!   POST /api/signin                {email} -> {challenge}
//!   POST /api/signin/verify         {challenge, code} -> {token}, and a cookie
//!   POST /api/signout
//!   GET  /api/skill  {skill}: the Forge skill for members' Claude, with this Forge's repos (signed in)
//!   GET  /api/status                StatusView           (signed in)
//!   GET  /api/tasks/:owner/:repo/:n TaskView              (signed in)
//!   POST /api/tasks/:owner/:repo    {title, body} -> {issue, url}: a new Asked task (signed in)
//!   POST /api/tasks/:owner/:repo/:n/status {status} -> {issue, status}: Ready (the
//!        repo's orderer, from Asked or Backlog), Backlog (any member, from Asked or
//!        Ready) or Accepted (any member, from Reviewed)
//!   GET  /api/tasks/:owner/:repo/:n/screen?history=N  {live, screen (ANSI), holder, mine}: the
//!        session's tmux screen, with up to N lines of scrollback above it
//!   POST /api/tasks/:owner/:repo/:n/control {take} -> {holder, mine}: one member at a time
//!        controls a session from here; it frees itself after 10 idle minutes
//!   POST /api/tasks/:owner/:repo/:n/input   {text, submit} | {key}: types into the session (the holder only)
//!   GET  /api/tasks/:owner/:repo/:n/pair    {ask}: `forge pair` waits up to 25 s for the session's next ask
//!        (`pair::Ask`, or null), and is paired while it keeps asking; one member at a time
//!   POST /api/tasks/:owner/:repo/:n/pair/:id {answer}: the paired member's answer to that ask
//! Signed in = `Authorization: Bearer <token>` or the `forge_session` cookie.
//!
//! Internal routes (loopback):
//!   POST /internal/attach/:owner/:repo/:n {who}         -> AttachGrant, marks attached (members only)
//!   POST /internal/detach/:owner/:repo/:n {session_key} ends it, owes an AfterAttach burst

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, TimeDelta, Utc};
use std::collections::HashMap;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::auth::{Auth, AuthError};
use crate::config::{Config, Member, RepoConfig};
use crate::db::Db;
use crate::engine::Busy;
use crate::github::GitHubApi;
use crate::pool::Pool;
use crate::proxy::SessionKeys;
use crate::status;
use crate::types::{AttachGrant, BurstReason, Status, StatusView, Task, TaskView};

/// JSON bodies here are tiny; bigger ones get 413.
const BODY_LIMIT: usize = 64 * 1024;
const COOKIE: &str = "forge_session";
/// The dashboard loads nothing but itself and talks only to this server.
const CSP: &str = "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; \
                   connect-src 'self'; form-action 'none'; base-uri 'none'; frame-ancestors 'none'";

#[derive(Clone)]
pub struct ApiState {
    pub cfg: Arc<Config>,
    pub db: Arc<Db>,
    pub auth: Arc<Auth>,
    pub pool: Arc<Pool>,
    pub keys: SessionKeys,
    /// The tasks a burst is running in now.
    pub busy: Busy,
    /// For asking for tasks and moving them on the Project.
    pub gh: Arc<dyn GitHubApi>,
    /// Who controls each session from the dashboard.
    pub control: Control,
    /// Sessions' asks to paired machines, shared with the proxy.
    pub pairs: crate::pair::Pairs,
}

/// How long control of a session lasts without input.
const CONTROL_IDLE: TimeDelta = TimeDelta::minutes(10);

/// Who controls each task's session from the dashboard: one member at a time.
#[derive(Clone, Default)]
pub struct Control(Arc<std::sync::Mutex<HashMap<(String, u64), Holder>>>);

#[derive(Clone)]
struct Holder {
    github: String,
    name: String,
    until: DateTime<Utc>,
}

impl Control {
    /// The live holder, forgetting one whose time ran out.
    fn holder(&self, repo: &str, issue: u64, now: DateTime<Utc>) -> Option<Holder> {
        let mut map = self.0.lock().unwrap();
        let key = (repo.to_string(), issue);
        if map.get(&key).is_some_and(|h| h.until <= now) {
            map.remove(&key);
        }
        map.get(&key).cloned()
    }

    /// Takes or renews control for `who`; the current holder's name if someone else has it.
    fn take(&self, repo: &str, issue: u64, who: &Member, now: DateTime<Utc>) -> Result<(), String> {
        if let Some(h) = self.holder(repo, issue, now).filter(|h| !h.github.eq_ignore_ascii_case(&who.github)) {
            return Err(h.name);
        }
        let holder = Holder { github: who.github.clone(), name: who.name.clone(), until: now + CONTROL_IDLE };
        self.0.lock().unwrap().insert((repo.to_string(), issue), holder);
        Ok(())
    }

    fn release(&self, repo: &str, issue: u64, who: &Member) {
        let mut map = self.0.lock().unwrap();
        let key = (repo.to_string(), issue);
        if map.get(&key).is_some_and(|h| h.github.eq_ignore_ascii_case(&who.github)) {
            map.remove(&key);
        }
    }
}

pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/", get(dashboard))
        .route("/healthz", get(|| async { "ok" }))
        .route("/install.sh", get(install_script))
        .route("/download/:file", get(download))
        .route("/api/signin", post(signin))
        .route("/api/signin/verify", post(verify))
        .route("/api/signout", post(signout))
        .route("/api/status", get(status_json))
        .route("/api/skill", get(skill))
        .route("/api/tasks/:owner/:repo", post(new_task))
        .route("/api/tasks/:owner/:repo/:n", get(task))
        .route("/api/tasks/:owner/:repo/:n/status", post(move_task))
        .route("/api/tasks/:owner/:repo/:n/screen", get(screen))
        .route("/api/tasks/:owner/:repo/:n/control", post(control))
        .route("/api/tasks/:owner/:repo/:n/input", post(input))
        .route("/api/tasks/:owner/:repo/:n/pair", get(pair_next))
        .route("/api/tasks/:owner/:repo/:n/pair/:id", post(pair_answer))
        .layer(DefaultBodyLimit::max(BODY_LIMIT))
        .with_state(state)
}

pub fn internal_router(state: ApiState) -> Router {
    Router::new()
        .route("/internal/attach/:owner/:repo/:n", post(attach))
        .route("/internal/detach/:owner/:repo/:n", post(detach))
        .layer(DefaultBodyLimit::max(BODY_LIMIT))
        .with_state(state)
}

pub async fn serve(addr: SocketAddr, router: Router) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await.with_context(|| format!("binding {addr}"))?;
    axum::serve(listener, router).await?;
    Ok(())
}

/// An error answer: its status and `{"error": "..."}`.
struct ApiError(StatusCode, String);

type ApiResult<T> = Result<T, ApiError>;

fn fail(code: StatusCode, msg: impl Into<String>) -> ApiError {
    ApiError(code, msg.into())
}

fn signed_out() -> ApiError {
    fail(StatusCode::UNAUTHORIZED, "sign in first")
}

fn no_task() -> ApiError {
    fail(StatusCode::NOT_FOUND, "no such task")
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> ApiError {
        if let Some(e) = e.downcast_ref::<AuthError>() {
            return fail(StatusCode::TOO_MANY_REQUESTS, e.to_string());
        }
        tracing::error!("api: {e:#}");
        fail(StatusCode::INTERNAL_SERVER_ERROR, "something went wrong on the server")
    }
}

impl From<JsonRejection> for ApiError {
    fn from(r: JsonRejection) -> ApiError {
        fail(r.status(), r.body_text())
    }
}

#[derive(Deserialize)]
struct SigninBody {
    email: String,
}

#[derive(Deserialize)]
struct VerifyBody {
    challenge: String,
    code: String,
}

#[derive(Deserialize)]
struct AttachBody {
    who: String,
}

#[derive(Deserialize)]
struct DetachBody {
    session_key: String,
}

async fn dashboard() -> impl IntoResponse {
    ([(header::CONTENT_SECURITY_POLICY, CSP)], Html(DASHBOARD))
}

async fn signin(State(s): State<ApiState>, body: Result<Json<SigninBody>, JsonRejection>) -> ApiResult<Json<Value>> {
    let Json(b) = body?;
    let challenge = s.auth.begin(&b.email, Utc::now()).await?;
    Ok(Json(json!({ "challenge": challenge })))
}

async fn verify(State(s): State<ApiState>, body: Result<Json<VerifyBody>, JsonRejection>) -> ApiResult<Response> {
    let Json(b) = body?;
    let token = s
        .auth
        .finish(&b.challenge, &b.code, Utc::now())?
        .ok_or_else(|| fail(StatusCode::UNAUTHORIZED, "that code is wrong or has expired"))?;
    let max_age = u64::from(s.cfg.auth.session_days) * 86_400;
    let cookie = format!("{COOKIE}={token}; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age={max_age}");
    Ok(([(header::SET_COOKIE, cookie)], Json(json!({ "token": token }))).into_response())
}

async fn signout(State(s): State<ApiState>, headers: HeaderMap) -> ApiResult<Response> {
    if let Some(token) = token(&headers) {
        s.auth.sign_out(token)?;
    }
    let cookie = format!("{COOKIE}=; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=0");
    Ok(([(header::SET_COOKIE, cookie)], Json(json!({ "ok": true }))).into_response())
}

async fn status_json(State(s): State<ApiState>, headers: HeaderMap) -> ApiResult<Json<StatusView>> {
    let member = signed_in(&s, &headers)?;
    let now = Utc::now();
    Ok(Json(status::build_status(&s.cfg, &s.db, s.pool.snapshot(now), Some(&member), now)?))
}

async fn task(
    State(s): State<ApiState>,
    headers: HeaderMap,
    Path((owner, repo, n)): Path<(String, String, String)>,
) -> ApiResult<Json<TaskView>> {
    signed_in(&s, &headers)?;
    let n = n.parse().map_err(|_| no_task())?;
    status::task_view(&s.cfg, &s.db, &format!("{owner}/{repo}"), n)?.map(Json).ok_or_else(no_task)
}

/// The installer, pointed at this Forge.
async fn install_script(State(s): State<ApiState>) -> Response {
    let script = include_str!("../deploy/install.sh").replace("__SERVER__", s.cfg.public_url.trim_end_matches('/'));
    ([(header::CONTENT_TYPE, "text/x-shellscript; charset=utf-8")], script).into_response()
}

async fn skill(State(s): State<ApiState>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    signed_in(&s, &headers)?;
    Ok(Json(json!({ "skill": render_skill(&s.cfg) })))
}

/// The Forge skill as this Forge serves it: the generic text, then its server
/// and, for each repo, its orderer, members and `skill_notes`.
fn render_skill(cfg: &Config) -> String {
    let mut p = format!("{}\n\n## This Forge\n\nServer: {}\n", crate::cli::SKILL.trim_end(), cfg.public_url.trim_end_matches('/'));
    for rc in &cfg.repos {
        let person = |m: &crate::config::Member| format!("{} (@{})", m.name, m.github);
        let orderer = rc.members.iter().find(|m| m.github.eq_ignore_ascii_case(&rc.orderer)).map_or(format!("@{}", rc.orderer), person);
        let members: Vec<String> = rc.members.iter().map(person).collect();
        p.push_str(&format!("\n### {}\n\nOrderer: {orderer}. Members: {}.\n", rc.name, members.join(", ")));
        if let Some(notes) = rc.skill_notes.as_deref().map(str::trim).filter(|n| !n.is_empty()) {
            p.push_str(&format!("\n{notes}\n"));
        }
    }
    p
}

/// The CLI builds the installer downloads; nothing else in the folder is served.
const DOWNLOADS: [&str; 2] = ["forge-darwin-arm64", "forge-linux-x86_64"];

async fn download(State(s): State<ApiState>, Path(file): Path<String>) -> ApiResult<Response> {
    if !DOWNLOADS.contains(&file.as_str()) {
        return Err(fail(StatusCode::NOT_FOUND, "no such download"));
    }
    let bytes = tokio::fs::read(s.cfg.data_dir.join("dist").join(&file))
        .await
        .map_err(|_| fail(StatusCode::NOT_FOUND, "that build isn't on this server yet"))?;
    Ok(([(header::CONTENT_TYPE, "application/octet-stream")], bytes).into_response())
}

#[derive(Deserialize)]
struct NewTaskBody {
    title: String,
    #[serde(default)]
    body: String,
}

/// A new task: an issue on the repo, put on its Project as Asked, saying who asked.
async fn new_task(
    State(s): State<ApiState>,
    headers: HeaderMap,
    Path((owner, repo)): Path<(String, String)>,
    body: Result<Json<NewTaskBody>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let member = signed_in(&s, &headers)?;
    let Json(b) = body?;
    let rc = s.cfg.repo(&format!("{owner}/{repo}")).ok_or_else(|| fail(StatusCode::NOT_FOUND, "Forge doesn't work on that repo"))?;
    let title = b.title.trim();
    if title.is_empty() || title.len() > 200 {
        return Err(fail(StatusCode::BAD_REQUEST, "a task needs a title of at most 200 characters"));
    }
    let text = format!("{}\n\n_Asked by {} through Forge._", b.body.trim(), member.name);
    let issue = s.gh.create_issue(&rc.name, title, text.trim_start(), &[]).await?;
    let item = s.gh.add_to_project(rc, issue).await?;
    s.gh.set_status(rc, &item, Status::Asked).await?;
    tracing::info!(repo = %rc.name, issue, by = %member.github, "task asked");
    Ok(Json(json!({ "issue": issue, "url": status::issue_url(&rc.name, issue) })))
}

#[derive(Deserialize)]
struct MoveBody {
    status: String,
}

/// Moves a task on the Project: to Ready by the repo's orderer, from Asked
/// (or no status); to Accepted by any member, from Reviewed. Forge's loop picks
/// the change up on its next tick, as if it was made on the board.
async fn move_task(
    State(s): State<ApiState>,
    headers: HeaderMap,
    Path((owner, repo, n)): Path<(String, String, String)>,
    body: Result<Json<MoveBody>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let member = signed_in(&s, &headers)?;
    let Json(b) = body?;
    let rc = s.cfg.repo(&format!("{owner}/{repo}")).ok_or_else(no_task)?;
    let n: u64 = n.parse().map_err(|_| no_task())?;
    let to = Status::from_project_name(b.status.trim()).filter(|s| matches!(s, Status::Ready | Status::Accepted | Status::Backlog));
    let Some(to) = to else {
        return Err(fail(StatusCode::BAD_REQUEST, "status is Ready, Backlog or Accepted"));
    };
    let items = s.gh.project_items(rc).await?;
    let item = items.iter().find(|i| i.issue_number == n).ok_or_else(|| fail(StatusCode::NOT_FOUND, format!("#{n} isn't on the Project")))?;
    let allowed = match to {
        Status::Ready if !member.github.eq_ignore_ascii_case(&rc.orderer) => {
            return Err(fail(StatusCode::FORBIDDEN, format!("only {} orders Ready", rc.orderer)));
        }
        Status::Ready => matches!(item.status, None | Some(Status::Asked | Status::Backlog)),
        Status::Backlog => matches!(item.status, None | Some(Status::Asked | Status::Ready)),
        _ => item.status == Some(Status::Reviewed),
    };
    if !allowed {
        let now = item.status.map_or("no status", |s| s.project_name());
        return Err(fail(StatusCode::CONFLICT, format!("#{n} is {now}; it can't go to {} from there", to.project_name())));
    }
    s.gh.set_status(rc, &item.item_id, to).await?;
    tracing::info!(repo = %rc.name, issue = n, to = to.as_str(), by = %member.github, "task moved");
    Ok(Json(json!({ "issue": n, "status": to.project_name() })))
}

/// The repo Forge works on and the issue number from a path.
fn repo_issue<'a>(s: &'a ApiState, owner: &str, repo: &str, n: &str) -> ApiResult<(&'a RepoConfig, u64)> {
    let rc = s.cfg.repo(&format!("{owner}/{repo}")).ok_or_else(no_task)?;
    Ok((rc, n.parse().map_err(|_| no_task())?))
}

fn holder_json(h: Option<&Holder>, me: &Member) -> Value {
    let mine = h.is_some_and(|h| h.github.eq_ignore_ascii_case(&me.github));
    json!({ "holder": h.map(|h| json!({ "github": h.github, "name": h.name })), "mine": mine })
}

/// The session's screen, with who controls it.
#[derive(Deserialize)]
struct ScreenQuery {
    #[serde(default)]
    history: u32,
}

async fn screen(
    State(s): State<ApiState>,
    headers: HeaderMap,
    Path((owner, repo, n)): Path<(String, String, String)>,
    Query(q): Query<ScreenQuery>,
) -> ApiResult<Json<Value>> {
    let me = signed_in(&s, &headers)?;
    let (rc, n) = repo_issue(&s, &owner, &repo, &n)?;
    let screen = crate::tmux::screen(&s.cfg, &rc.name, n, q.history).await?;
    let mut v = holder_json(s.control.holder(&rc.name, n, Utc::now()).as_ref(), &me);
    v["live"] = json!(screen.is_some());
    v["screen"] = json!(screen.unwrap_or_default());
    Ok(Json(v))
}

#[derive(Deserialize)]
struct ControlBody {
    take: bool,
}

/// Takes or lets go of control of a session.
async fn control(
    State(s): State<ApiState>,
    headers: HeaderMap,
    Path((owner, repo, n)): Path<(String, String, String)>,
    body: Result<Json<ControlBody>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let me = signed_in(&s, &headers)?;
    let Json(b) = body?;
    let (rc, n) = repo_issue(&s, &owner, &repo, &n)?;
    let now = Utc::now();
    if b.take {
        s.control.take(&rc.name, n, &me, now).map_err(|name| fail(StatusCode::CONFLICT, format!("{name} has control of this session")))?;
    } else {
        s.control.release(&rc.name, n, &me);
    }
    Ok(Json(holder_json(s.control.holder(&rc.name, n, now).as_ref(), &me)))
}

#[derive(Deserialize)]
struct InputBody {
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    submit: bool,
    #[serde(default)]
    key: Option<String>,
}

/// Types into the session for the member who has control, renewing it.
async fn input(
    State(s): State<ApiState>,
    headers: HeaderMap,
    Path((owner, repo, n)): Path<(String, String, String)>,
    body: Result<Json<InputBody>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let me = signed_in(&s, &headers)?;
    let Json(b) = body?;
    let (rc, n) = repo_issue(&s, &owner, &repo, &n)?;
    let now = Utc::now();
    let mine = s.control.holder(&rc.name, n, now).is_some_and(|h| h.github.eq_ignore_ascii_case(&me.github));
    if !mine {
        return Err(fail(StatusCode::FORBIDDEN, "take control of the session first"));
    }
    let input = match (b.text.as_deref(), b.key.as_deref()) {
        (Some(text), None) if !text.is_empty() && text.len() <= 8000 => crate::tmux::Input::Text { text, submit: b.submit },
        (None, Some(key)) if crate::tmux::KEYS.contains(&key) => crate::tmux::Input::Key(key),
        _ => return Err(fail(StatusCode::BAD_REQUEST, format!("send text (at most 8000 bytes) or one key of {}", crate::tmux::KEYS.join(", ")))),
    };
    if !crate::tmux::type_into(&s.cfg, &rc.name, n, &input).await? {
        return Err(fail(StatusCode::CONFLICT, "this task has no live session"));
    }
    let _ = s.control.take(&rc.name, n, &me, now);
    Ok(Json(json!({ "ok": true })))
}

/// The paired machine's wait for the session's next ask; it stays paired while it keeps asking.
async fn pair_next(
    State(s): State<ApiState>,
    headers: HeaderMap,
    Path((owner, repo, n)): Path<(String, String, String)>,
) -> ApiResult<Json<Value>> {
    let me = signed_in(&s, &headers)?;
    let (rc, n) = repo_member(&s, &me, &owner, &repo, &n)?;
    let ask = s
        .pairs
        .next(&rc.name, n, &me.github, &me.name, crate::pair::POLL_WAIT)
        .await
        .map_err(|name| fail(StatusCode::CONFLICT, format!("{name} is paired with #{n}")))?;
    Ok(Json(json!({ "ask": ask })))
}

#[derive(Deserialize)]
struct PairAnswerBody {
    answer: String,
}

async fn pair_answer(
    State(s): State<ApiState>,
    headers: HeaderMap,
    Path((owner, repo, n, id)): Path<(String, String, String, u64)>,
    body: Result<Json<PairAnswerBody>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let me = signed_in(&s, &headers)?;
    let Json(b) = body?;
    let (rc, n) = repo_member(&s, &me, &owner, &repo, &n)?;
    if !s.pairs.answer(&rc.name, n, &me.github, id, b.answer) {
        return Err(fail(StatusCode::NOT_FOUND, "Forge's Claude isn't waiting for that answer any more"));
    }
    Ok(Json(json!({ "ok": true })))
}

/// The repo and issue, when `me` is one of the repo's members.
fn repo_member<'a>(s: &'a ApiState, me: &Member, owner: &str, repo: &str, n: &str) -> ApiResult<(&'a RepoConfig, u64)> {
    let (rc, n) = repo_issue(s, owner, repo, n)?;
    if !rc.members.iter().any(|m| m.github.eq_ignore_ascii_case(&me.github)) {
        return Err(fail(StatusCode::FORBIDDEN, format!("only {}'s members pair with its tasks", rc.name)));
    }
    Ok((rc, n))
}

/// Grants `who`, a member of the repo, the task's live session, through the
/// proxy with a fresh key, and records when in the kv `attached_at_key`.
async fn attach(
    State(s): State<ApiState>,
    Path((owner, repo, n)): Path<(String, String, String)>,
    body: Result<Json<AttachBody>, JsonRejection>,
) -> ApiResult<Json<AttachGrant>> {
    let Json(b) = body?;
    let who = b.who.trim();
    if who.is_empty() {
        return Err(fail(StatusCode::BAD_REQUEST, "who is empty"));
    }
    let (rc, t) = find(&s, &owner, &repo, &n)?;
    let who = may_attach(&s.cfg, rc, who)
        .ok_or_else(|| fail(StatusCode::FORBIDDEN, format!("{who} may not step into {}'s sessions", rc.name)))?;
    let who = who.as_str();
    let session_id = t.session_id.clone().ok_or_else(|| fail(StatusCode::CONFLICT, "this task has no session yet"))?;
    let taken = || {
        let other = t.attached_by.as_deref().unwrap_or("someone else");
        fail(StatusCode::CONFLICT, format!("{other} is attached already"))
    };
    // Stamp the time before marking, so the engine never finds an attach
    // without one, but never over someone else's attach.
    if t.attached_by.as_deref().is_some_and(|by| by != who) {
        return Err(taken());
    }
    let since = attached_at_key(&rc.name, t.issue);
    // Stepping in again keeps the first time: the 8-hour limit isn't renewable.
    let again = t.attached_by.as_deref() == Some(who) && s.db.kv_get(&since)?.is_some_and(|v| !v.is_empty());
    if !again {
        s.db.kv_set(&since, &Utc::now().to_rfc3339())?;
    }
    if !s.db.attach(&rc.name, t.issue, who)? {
        return Err(taken());
    }
    // Attach first, then look: the engine claims first, then looks for an attach.
    if s.busy.contains(&rc.name, t.issue) {
        s.db.detach(&rc.name, t.issue)?;
        s.db.kv_set(&since, "")?;
        return Err(fail(StatusCode::CONFLICT, "Forge is working in that session now; try again when its turn ends"));
    }
    let (o, r) = rc.owner_and_name();
    let role = rc.roles.get(&t.role).or_else(|| rc.roles.get(&rc.default_role));
    Ok(Json(AttachGrant {
        session_id,
        worktree: s.cfg.worktrees_dir().join(o).join(r).join(t.branch.replace('/', "-")),
        model: role.map(|r| r.model.clone()).unwrap_or_default(),
        proxy_url: format!("http://{}", s.cfg.claude.proxy_listen),
        session_key: {
            // One live attach key per task: an earlier grant's key stops working.
            s.keys.revoke_tagged(&attach_prefix(&rc.name, t.issue));
            s.keys.issue(&attach_tag(&rc.name, t.issue, who))
        },
    }))
}

/// The login's configured spelling, when it may attach: a member of the repo
/// in Forge's settings, or anyone `forge keys` last gave an attach key (a
/// collaborator with push access).
fn may_attach(cfg: &Config, repo: &RepoConfig, who: &str) -> Option<String> {
    if let Some(m) = repo.members.iter().find(|m| m.github.eq_ignore_ascii_case(who)) {
        return Some(m.github.clone());
    }
    let keys = std::fs::read_to_string(cfg.attach_keys_path()).unwrap_or_default();
    keys.lines()
        .filter_map(|line| line.split_once(" attach-gate ")?.1.split_once('"').map(|(login, _)| login))
        .find(|login| login.eq_ignore_ascii_case(who))
        .map(String::from)
}

/// Ends the task's attach, given the live key attach issued for it: the key
/// stops working, and a Building task waiting on no one owes a burst. Any
/// other key is refused and changes nothing.
async fn detach(
    State(s): State<ApiState>,
    Path((owner, repo, n)): Path<(String, String, String)>,
    body: Result<Json<DetachBody>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let Json(b) = body?;
    let (rc, t) = find(&s, &owner, &repo, &n)?;
    let ours = t.attached_by.as_deref().map(|by| attach_tag(&rc.name, t.issue, by));
    if ours.is_none() || s.keys.tag(&b.session_key) != ours {
        return Err(fail(StatusCode::BAD_REQUEST, "that key isn't this task's live attach key"));
    }
    s.keys.revoke_tagged(&attach_prefix(&rc.name, t.issue));
    s.db.detach(&rc.name, t.issue)?;
    s.db.kv_set(&attached_at_key(&rc.name, t.issue), "")?;
    // A burst already owed (review notes, a failed check) carries more than "someone looked".
    let owes = |t: &Task| t.status == Status::Building && t.waiting_on.is_none() && t.pending.is_none();
    if let Some(mut t) = s.db.task(&rc.name, t.issue)?.filter(owes) {
        t.pending = Some(BurstReason::AfterAttach);
        t.updated_at = Utc::now();
        s.db.upsert_task(&t)?;
    }
    Ok(Json(json!({ "ok": true })))
}

/// The proxy key tag of `who`'s attach to a task.
fn attach_tag(repo: &str, issue: u64, who: &str) -> String {
    format!("{}{who}", attach_prefix(repo, issue))
}

/// What every attach key tag of a task starts with.
fn attach_prefix(repo: &str, issue: u64) -> String {
    format!("{repo}#{issue} attach ")
}

/// The kv key that holds when the task's attach was granted (RFC 3339), or ""
/// once it ended.
fn attached_at_key(repo: &str, issue: u64) -> String {
    format!("attach:{repo}#{issue}")
}

/// The configured repo and one of its tasks, or 404.
fn find<'a>(s: &'a ApiState, owner: &str, repo: &str, n: &str) -> ApiResult<(&'a RepoConfig, Task)> {
    let rc = s.cfg.repo(&format!("{owner}/{repo}")).ok_or_else(no_task)?;
    let n: u64 = n.parse().map_err(|_| no_task())?;
    let t = s.db.task(&rc.name, n)?.ok_or_else(no_task)?;
    Ok((rc, t))
}

fn signed_in(s: &ApiState, headers: &HeaderMap) -> ApiResult<Member> {
    let token = token(headers).ok_or_else(signed_out)?;
    s.auth.member(token, Utc::now())?.ok_or_else(signed_out)
}

/// The API token from `Authorization: Bearer`, else from the session cookie.
fn token(headers: &HeaderMap) -> Option<&str> {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let cookie = || {
        headers
            .get_all(header::COOKIE)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(';'))
            .find_map(|c| c.trim().strip_prefix(COOKIE)?.strip_prefix('='))
    };
    bearer.or_else(cookie).map(str::trim).filter(|t| !t.is_empty())
}

/// The whole dashboard: sign-in by email code, then the status, refreshed every 30 s.
/// The dashboard: one self-contained page (see the file for its layout).
const DASHBOARD: &str = include_str!("dashboard.html");

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;
    use crate::notify::RecordingNotifier;
    use crate::types::{IssueThread, ProjectItem, PullRequest};

    const REPO: &str = "example-team/shop";

    /// GitHub for the task routes: a Project with items, and what was done to it.
    #[derive(Default)]
    struct FakeHub {
        items: std::sync::Mutex<Vec<ProjectItem>>,
        done: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl GitHubApi for FakeHub {
        async fn project_items(&self, _: &RepoConfig) -> anyhow::Result<Vec<ProjectItem>> {
            Ok(self.items.lock().unwrap().clone())
        }
        async fn set_status(&self, _: &RepoConfig, item: &str, status: Status) -> anyhow::Result<()> {
            self.done.lock().unwrap().push(format!("status {item} {}", status.as_str()));
            Ok(())
        }
        async fn issue_thread(&self, _: &str, _: u64) -> anyhow::Result<IssueThread> {
            unimplemented!()
        }
        async fn comment(&self, _: &str, _: u64, _: &str) -> anyhow::Result<u64> {
            unimplemented!()
        }
        async fn create_issue(&self, repo: &str, title: &str, body: &str, _: &[String]) -> anyhow::Result<u64> {
            self.done.lock().unwrap().push(format!("issue {repo} {title} | {body}"));
            Ok(77)
        }
        async fn open_or_update_pr(&self, _: &str, _: &str, _: &str, _: &str, _: &str) -> anyhow::Result<PullRequest> {
            unimplemented!()
        }
        async fn merge_pr(&self, _: &str, _: u64, _: &str, _: &str) -> anyhow::Result<()> {
            unimplemented!()
        }
        async fn git_token(&self) -> anyhow::Result<String> {
            unimplemented!()
        }
        async fn collaborators(&self, _: &str) -> anyhow::Result<Vec<String>> {
            unimplemented!()
        }
        async fn add_to_project(&self, _: &RepoConfig, issue: u64) -> anyhow::Result<String> {
            self.done.lock().unwrap().push(format!("add {issue}"));
            Ok(format!("item-{issue}"))
        }
    }

    fn state_with(cfg: Config) -> (ApiState, Arc<RecordingNotifier>) {
        let cfg = Arc::new(cfg);
        let db = Arc::new(Db::open_in_memory().unwrap());
        let notes = Arc::new(RecordingNotifier::default());
        let auth = Arc::new(Auth::new(db.clone(), b"test-secret".to_vec(), cfg.clone(), notes.clone()));
        let pool = Arc::new(Pool::from_tokens(vec![("blue".into(), "plan-token".into())], 85.0, 90.0));
        let gh: Arc<dyn GitHubApi> = Arc::new(FakeHub::default());
        (ApiState { cfg, db, auth, pool, keys: SessionKeys::new(), busy: Busy::default(), gh, control: Control::default(), pairs: Default::default() }, notes)
    }

    fn state() -> (ApiState, Arc<RecordingNotifier>) {
        state_with(crate::config::sample())
    }

    struct Answer {
        status: StatusCode,
        headers: HeaderMap,
        body: Vec<u8>,
    }

    impl Answer {
        fn json(&self) -> Value {
            serde_json::from_slice(&self.body).unwrap()
        }
        fn header(&self, name: &str) -> &str {
            self.headers.get(name).unwrap().to_str().unwrap()
        }
    }

    async fn send(app: &Router, method: &str, uri: &str, headers: &[(&str, &str)], body: Option<Value>) -> Answer {
        let mut req = Request::builder().method(method).uri(uri);
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let body = match body {
            Some(v) => {
                req = req.header("content-type", "application/json");
                Body::from(v.to_string())
            }
            None => Body::empty(),
        };
        let resp = app.clone().oneshot(req.body(body).unwrap()).await.unwrap();
        let (parts, body) = resp.into_parts();
        Answer { status: parts.status, headers: parts.headers, body: body.collect().await.unwrap().to_bytes().to_vec() }
    }

    fn last_code(notes: &RecordingNotifier) -> String {
        let emails = notes.emails.lock().unwrap();
        emails.last().unwrap().2.chars().filter(char::is_ascii_digit).take(6).collect()
    }

    #[tokio::test]
    async fn installer_and_downloads_are_public_and_only_the_builds() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::sample();
        cfg.data_dir = tmp.path().to_path_buf();
        let app = router(state_with(cfg.clone()).0);
        let a = send(&app, "GET", "/install.sh", &[], None).await;
        assert_eq!(a.status, StatusCode::OK);
        let script = String::from_utf8(a.body).unwrap();
        assert!(script.contains(&format!("SERVER=\"{}\"", cfg.public_url.trim_end_matches('/'))) && !script.contains("__SERVER__"));
        assert_eq!(send(&app, "GET", "/download/forge-darwin-arm64", &[], None).await.status, StatusCode::NOT_FOUND);
        std::fs::create_dir_all(tmp.path().join("dist")).unwrap();
        std::fs::write(tmp.path().join("dist/forge-darwin-arm64"), b"bin").unwrap();
        std::fs::write(tmp.path().join("dist/secret"), b"no").unwrap();
        let a = send(&app, "GET", "/download/forge-darwin-arm64", &[], None).await;
        assert_eq!((a.status, a.body.as_slice()), (StatusCode::OK, b"bin".as_slice()));
        assert_eq!(send(&app, "GET", "/download/secret", &[], None).await.status, StatusCode::NOT_FOUND);
        assert_eq!(send(&app, "GET", "/download/..%2Fstate%2Fforge.db", &[], None).await.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn the_skill_names_this_forges_repos_for_members_only() {
        let (st, notes) = state();
        let mut cfg = (*st.cfg).clone();
        cfg.repos[0].skill_notes = Some("Run `make up` to try a branch.".into());
        let app = router(ApiState { cfg: Arc::new(cfg), ..st });
        assert_eq!(send(&app, "GET", "/api/skill", &[], None).await.status, StatusCode::UNAUTHORIZED);
        let ann = format!("Bearer {}", sign_in(&app, &notes, "ann@example.com").await.0);
        let a = send(&app, "GET", "/api/skill", &[("authorization", ann.as_str())], None).await;
        let skill = serde_json::from_slice::<Value>(&a.body).unwrap()["skill"].as_str().unwrap().to_string();
        assert!(skill.starts_with("---\nname: forge\n"), "{skill}");
        assert!(skill.contains("## This Forge\n\nServer: https://forge.example.com\n"), "{skill}");
        assert!(skill.contains("### example-team/shop\n\nOrderer: Bob (@bob-gh). Members: Ann (@ann-gh), Bob (@bob-gh).\n\nRun `make up` to try a branch.\n"), "{skill}");
    }

    #[tokio::test]
    async fn a_member_pairs_a_machine_and_answers_the_sessions_ask() {
        let (st, notes) = state();
        let pairs = st.pairs.clone();
        let app = router(st);
        let ann = format!("Bearer {}", sign_in(&app, &notes, "ann@example.com").await.0);
        let ann = [("authorization", ann.as_str())];
        assert_eq!(send(&app, "GET", &format!("/api/tasks/{REPO}/412/pair"), &[], None).await.status, StatusCode::UNAUTHORIZED);
        assert_eq!(send(&app, "GET", "/api/tasks/other/repo/412/pair", &ann, None).await.status, StatusCode::NOT_FOUND);

        // Ann's machine asks for work, so the session's ask finds her paired.
        let waiting = tokio::spawn({
            let app = app.clone();
            let ann = ann[0].1.to_string();
            async move { send(&app, "GET", &format!("/api/tasks/{REPO}/412/pair"), &[("authorization", ann.as_str())], None).await }
        });
        while pairs.paired(REPO, 412, Utc::now()).is_none() {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let (answer, by) = pairs.ask(REPO, 412, "sort them".into(), "abc".into(), String::new(), Utc::now()).unwrap();
        assert_eq!(by, "Ann");
        let got = waiting.await.unwrap().json();
        assert_eq!((got["ask"]["text"].as_str(), got["ask"]["base"].as_str()), (Some("sort them"), Some("abc")));
        let id = got["ask"]["id"].as_u64().unwrap();
        let body = || Some(json!({ "answer": "sorted 12" }));
        let wrong = send(&app, "POST", &format!("/api/tasks/{REPO}/412/pair/{}", id + 1), &ann, body()).await;
        assert_eq!(wrong.status, StatusCode::NOT_FOUND);
        assert_eq!(send(&app, "POST", &format!("/api/tasks/{REPO}/412/pair/{id}"), &ann, body()).await.status, StatusCode::OK);
        assert_eq!(answer.await.unwrap(), "sorted 12");
    }

    #[tokio::test]
    async fn one_member_at_a_time_controls_a_session() {
        let (st, notes) = state();
        let control = st.control.clone();
        let app = router(st);
        let ann = format!("Bearer {}", sign_in(&app, &notes, "ann@example.com").await.0);
        let bob = format!("Bearer {}", sign_in(&app, &notes, "bob@example.com").await.0);
        let post = |uri: &'static str, who: &str, body: Value| {
            let (app, who) = (app.clone(), who.to_string());
            async move { send(&app, "POST", uri, &[("authorization", who.as_str())], Some(body)).await }
        };
        let take = json!({ "take": true });
        let a = post("/api/tasks/example-team/shop/7/control", &ann, take.clone()).await;
        assert_eq!(a.status, StatusCode::OK);
        let v: Value = serde_json::from_slice(&a.body).unwrap();
        assert_eq!((v["mine"].as_bool(), v["holder"]["name"].as_str()), (Some(true), Some("Ann")));
        let b = post("/api/tasks/example-team/shop/7/control", &bob, take.clone()).await;
        assert_eq!(b.status, StatusCode::CONFLICT);
        assert!(String::from_utf8_lossy(&b.body).contains("Ann has control"));
        assert_eq!(post("/api/tasks/example-team/shop/7/input", &bob, json!({ "text": "hi" })).await.status, StatusCode::FORBIDDEN);
        assert_eq!(post("/api/tasks/example-team/shop/7/input", &ann, json!({ "key": "rm -rf" })).await.status, StatusCode::BAD_REQUEST);
        // No tmux session here: typing reports it rather than pretending.
        assert_eq!(post("/api/tasks/example-team/shop/7/input", &ann, json!({ "text": "hi", "submit": true })).await.status, StatusCode::CONFLICT);
        // Bob can't release Ann's control; Ann can, and then Bob takes it.
        post("/api/tasks/example-team/shop/7/control", &bob, json!({ "take": false })).await;
        assert!(control.holder(REPO, 7, Utc::now()).is_some());
        post("/api/tasks/example-team/shop/7/control", &ann, json!({ "take": false })).await;
        assert_eq!(post("/api/tasks/example-team/shop/7/control", &bob, take).await.status, StatusCode::OK);
        // Control runs out after ten idle minutes.
        assert!(control.holder(REPO, 7, Utc::now() + CONTROL_IDLE + TimeDelta::seconds(1)).is_none());
        let screen = send(&app, "GET", "/api/tasks/example-team/shop/7/screen", &[("authorization", ann.as_str())], None).await;
        let v: Value = serde_json::from_slice(&screen.body).unwrap();
        assert_eq!((v["live"].as_bool(), v["mine"].as_bool()), (Some(false), Some(false)));
    }

    #[tokio::test]
    async fn members_ask_for_tasks_and_move_them_by_the_rules() {
        let hub = Arc::new(FakeHub::default());
        let item = |n: u64, status| ProjectItem {
            item_id: format!("I{n}"),
            issue_number: n,
            title: "t".into(),
            status: Some(status),
            labels: vec![],
            author: "x".into(),
        };
        hub.items.lock().unwrap().extend([item(5, Status::Asked), item(6, Status::Reviewed)]);
        let (mut st, notes) = state();
        st.gh = hub.clone();
        let app = router(st);
        let ann = format!("Bearer {}", sign_in(&app, &notes, "ann@example.com").await.0);
        let bob = format!("Bearer {}", sign_in(&app, &notes, "bob@example.com").await.0);
        let post = |uri: &'static str, who: &str, body: Value| {
            let (app, who) = (app.clone(), who.to_string());
            async move { send(&app, "POST", uri, &[("authorization", who.as_str())], Some(body)).await }
        };

        let a = post("/api/tasks/example-team/shop", &ann, json!({ "title": " Fix the date picker ", "body": "It shows US dates." })).await;
        assert_eq!(a.status, StatusCode::OK);
        let v: Value = serde_json::from_slice(&a.body).unwrap();
        assert_eq!(v["issue"], 77);
        assert_eq!(
            *hub.done.lock().unwrap(),
            [
                "issue example-team/shop Fix the date picker | It shows US dates.\n\n_Asked by Ann through Forge._",
                "add 77",
                "status item-77 asked"
            ]
        );
        assert_eq!(post("/api/tasks/example-team/shop", &ann, json!({ "title": "  " })).await.status, StatusCode::BAD_REQUEST);
        assert_eq!(post("/api/tasks/example-team/shop", "Bearer nope", json!({ "title": "x" })).await.status, StatusCode::UNAUTHORIZED);
        assert_eq!(post("/api/tasks/other/repo", &ann, json!({ "title": "x" })).await.status, StatusCode::NOT_FOUND);

        hub.done.lock().unwrap().clear();
        let ready = json!({ "status": "Ready" });
        assert_eq!(post("/api/tasks/example-team/shop/5/status", &ann, ready.clone()).await.status, StatusCode::FORBIDDEN);
        assert_eq!(post("/api/tasks/example-team/shop/5/status", &bob, ready.clone()).await.status, StatusCode::OK);
        assert_eq!(post("/api/tasks/example-team/shop/6/status", &bob, ready).await.status, StatusCode::CONFLICT);
        let accept = json!({ "status": "accepted" });
        assert_eq!(post("/api/tasks/example-team/shop/5/status", &ann, accept.clone()).await.status, StatusCode::CONFLICT);
        assert_eq!(post("/api/tasks/example-team/shop/6/status", &ann, accept.clone()).await.status, StatusCode::OK);
        assert_eq!(post("/api/tasks/example-team/shop/9/status", &ann, accept).await.status, StatusCode::NOT_FOUND);
        assert_eq!(post("/api/tasks/example-team/shop/6/status", &ann, json!({ "status": "On main" })).await.status, StatusCode::BAD_REQUEST);
        assert_eq!(post("/api/tasks/example-team/shop/5/status", &ann, json!({ "status": "Backlog" })).await.status, StatusCode::OK);
        assert_eq!(post("/api/tasks/example-team/shop/6/status", &ann, json!({ "status": "Backlog" })).await.status, StatusCode::CONFLICT);
        assert_eq!(*hub.done.lock().unwrap(), ["status I5 ready", "status I6 accepted", "status I5 backlog"]);
    }

    /// Signs in through the API and returns (token, Set-Cookie).
    async fn sign_in(app: &Router, notes: &RecordingNotifier, email: &str) -> (String, String) {
        let a = send(app, "POST", "/api/signin", &[], Some(json!({ "email": email }))).await;
        assert_eq!(a.status, StatusCode::OK);
        let challenge = a.json()["challenge"].as_str().unwrap().to_string();
        let body = json!({ "challenge": challenge, "code": last_code(notes) });
        let a = send(app, "POST", "/api/signin/verify", &[], Some(body)).await;
        assert_eq!(a.status, StatusCode::OK);
        (a.json()["token"].as_str().unwrap().to_string(), a.header("set-cookie").to_string())
    }

    fn put(state: &ApiState, issue: u64, status: Status, session: Option<&str>) {
        let mut t = Task::new(REPO, issue, "Add an export", "bob-gh", status, "ui");
        t.session_id = session.map(String::from);
        state.db.upsert_task(&t).unwrap();
    }

    /// What the kv says about the task's attach.
    fn stamp(state: &ApiState, issue: u64) -> Option<String> {
        state.db.kv_get(&format!("attach:{REPO}#{issue}")).unwrap()
    }

    async fn attach_as(app: &Router, issue: u64, who: &str) -> Answer {
        let uri = format!("/internal/attach/{REPO}/{issue}");
        send(app, "POST", &uri, &[], Some(json!({ "who": who }))).await
    }

    async fn detach_with(app: &Router, issue: u64, key: &str) -> Answer {
        let uri = format!("/internal/detach/{REPO}/{issue}");
        send(app, "POST", &uri, &[], Some(json!({ "session_key": key }))).await
    }

    #[tokio::test]
    async fn sign_in_sets_a_cookie_that_opens_status() {
        let (state, notes) = state();
        let app = router(state);
        let a = send(&app, "POST", "/api/signin", &[], Some(json!({ "email": "bob@example.com" }))).await;
        let challenge = a.json()["challenge"].as_str().unwrap().to_string();
        let wrong = json!({ "challenge": challenge, "code": "x" });
        let a = send(&app, "POST", "/api/signin/verify", &[], Some(wrong)).await;
        assert_eq!(a.status, StatusCode::UNAUTHORIZED);
        assert!(a.json()["error"].is_string());
        let right = json!({ "challenge": challenge, "code": last_code(&notes) });
        let a = send(&app, "POST", "/api/signin/verify", &[], Some(right)).await;
        assert_eq!(a.status, StatusCode::OK);
        let token = a.json()["token"].as_str().unwrap().to_string();
        assert_eq!(
            a.header("set-cookie"),
            format!("forge_session={token}; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=1209600")
        );

        let a = send(&app, "GET", "/api/status", &[], None).await;
        assert_eq!(a.status, StatusCode::UNAUTHORIZED);
        assert_eq!(a.json(), json!({ "error": "sign in first" }));
        let cookie = format!("theme=dark; forge_session={token}");
        let a = send(&app, "GET", "/api/status", &[("cookie", &cookie)], None).await;
        assert_eq!(a.status, StatusCode::OK);
        let v: StatusView = serde_json::from_slice(&a.body).unwrap();
        assert_eq!((v.repos[0].repo.as_str(), v.pool[0].label.as_str()), (REPO, "blue"));
        assert!(!String::from_utf8(a.body).unwrap().contains("plan-token"));
        let bearer = format!("Bearer {token}");
        let a = send(&app, "GET", "/api/status", &[("authorization", &bearer)], None).await;
        assert_eq!(a.status, StatusCode::OK);

        let a = send(&app, "POST", "/api/signout", &[("cookie", &cookie)], None).await;
        assert_eq!(a.status, StatusCode::OK);
        assert!(a.header("set-cookie").starts_with("forge_session=; ") && a.header("set-cookie").ends_with("Max-Age=0"));
        let a = send(&app, "GET", "/api/status", &[("authorization", &bearer)], None).await;
        assert_eq!(a.status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn non_members_get_the_same_answer_and_limits_apply() {
        let (state, notes) = state();
        let app = router(state);
        for _ in 0..5 {
            let a = send(&app, "POST", "/api/signin", &[], Some(json!({ "email": "stranger@example.com" }))).await;
            assert_eq!(a.status, StatusCode::OK);
            assert_eq!(a.json()["challenge"].as_str().unwrap().len(), 43);
        }
        assert!(notes.emails.lock().unwrap().is_empty());
        let a = send(&app, "POST", "/api/signin", &[], Some(json!({ "email": "stranger@example.com" }))).await;
        assert_eq!(a.status, StatusCode::TOO_MANY_REQUESTS);
        assert!(a.json()["error"].as_str().unwrap().contains("wait an hour"));
    }

    #[tokio::test]
    async fn bad_and_big_bodies_are_refused() {
        let (state, _) = state();
        let app = router(state);
        let big = json!({ "email": "a".repeat(70 * 1024) });
        assert_eq!(send(&app, "POST", "/api/signin", &[], Some(big)).await.status, StatusCode::PAYLOAD_TOO_LARGE);
        let a = send(&app, "POST", "/api/signin", &[], Some(json!({ "mail": "x" }))).await;
        assert_eq!(a.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(a.json()["error"].is_string());
    }

    #[tokio::test]
    async fn tasks_need_sign_in_and_must_exist() {
        let (state, notes) = state();
        put(&state, 412, Status::Reviewed, Some("s1"));
        let app = router(state);
        let (token, _) = sign_in(&app, &notes, "ann@example.com").await;
        let auth = format!("Bearer {token}");
        let uri = "/api/tasks/example-team/shop/412";
        assert_eq!(send(&app, "GET", uri, &[], None).await.status, StatusCode::UNAUTHORIZED);
        let a = send(&app, "GET", uri, &[("authorization", &auth)], None).await;
        assert_eq!(a.status, StatusCode::OK);
        let v: TaskView = serde_json::from_slice(&a.body).unwrap();
        assert_eq!((v.branch.as_str(), v.try_commands.build.as_deref()), ("forge/412", Some("cargo build")));
        for missing in ["/api/tasks/example-team/shop/413", "/api/tasks/example-team/shop/x", "/api/tasks/a/b/412"] {
            let a = send(&app, "GET", missing, &[("authorization", &auth)], None).await;
            assert_eq!((a.status, a.json()), (StatusCode::NOT_FOUND, json!({ "error": "no such task" })));
        }
    }

    #[tokio::test]
    async fn dashboard_is_one_inline_page() {
        let (state, _) = state();
        let app = router(state);
        let a = send(&app, "GET", "/", &[], None).await;
        assert_eq!(a.status, StatusCode::OK);
        assert!(a.header("content-type").starts_with("text/html"));
        assert!(a.header("content-security-policy").starts_with("default-src 'none'"));
        let html = String::from_utf8(a.body).unwrap();
        for part in ["id=\"email\"", "id=\"code\"", "/api/signin/verify", "/api/status", "prefers-color-scheme", "15000", "/screen", "/control", "/input"] {
            assert!(html.contains(part), "missing {part}");
        }
        for outside in [" src=", "<link", "@import", "url("] {
            assert!(!html.contains(outside), "loads {outside}");
        }
        // One script: a second function of the same name silently replaces the
        // first (the session view's `render` once blanked the whole board).
        let mut names: Vec<&str> = html.split("function ").skip(1).filter_map(|s| s.split('(').next()).filter(|n| !n.is_empty() && !n.contains(' ')).collect();
        names.sort_unstable();
        let twice: Vec<&&str> = names.windows(2).filter(|w| w[0] == w[1]).map(|w| &w[0]).collect();
        assert!(twice.is_empty(), "functions declared twice: {twice:?}");
        let a = send(&app, "GET", "/healthz", &[], None).await;
        assert_eq!((a.status, a.body.as_slice()), (StatusCode::OK, b"ok".as_slice()));
    }

    #[tokio::test]
    async fn attach_waits_for_a_running_burst() {
        let (state, _) = state_with(crate::config::sample());
        put(&state, 412, Status::Building, Some("sess-1"));
        state.busy.hold(REPO, 412);
        let app = internal_router(state.clone());
        assert_eq!(attach_as(&app, 412, "bob-gh").await.status, StatusCode::CONFLICT);
        assert_eq!(state.db.task(REPO, 412).unwrap().unwrap().attached_by, None);
        assert_eq!(stamp(&state, 412).as_deref(), Some(""));
    }

    #[tokio::test]
    async fn attach_grants_the_session_and_detach_owes_a_burst() {
        let mut cfg = crate::config::sample();
        cfg.repos[0].roles.get_mut("ui").unwrap().model = "ui-model".into();
        let (state, _) = state_with(cfg);
        put(&state, 412, Status::Building, Some("sess-1"));
        let app = internal_router(state.clone());

        let before = Utc::now();
        let a = attach_as(&app, 412, "BOB-gh").await;
        assert_eq!(a.status, StatusCode::OK);
        let g: AttachGrant = serde_json::from_slice(&a.body).unwrap();
        assert_eq!(g.session_id, "sess-1");
        assert_eq!(g.worktree, std::path::PathBuf::from("/var/lib/forge/worktrees/example-team/shop/forge-412"));
        assert_eq!((g.model.as_str(), g.proxy_url.as_str()), ("ui-model", "http://127.0.0.1:8471"));
        assert_eq!(state.keys.tag(&g.session_key).as_deref(), Some("example-team/shop#412 attach bob-gh"));
        assert_eq!(state.db.task(REPO, 412).unwrap().unwrap().attached_by.as_deref(), Some("bob-gh"));
        let at = chrono::DateTime::parse_from_rfc3339(&stamp(&state, 412).unwrap()).unwrap();
        assert!(at >= before && at <= Utc::now());

        let a = attach_as(&app, 412, "ann-gh").await;
        assert_eq!(a.status, StatusCode::CONFLICT);
        assert_eq!(a.json()["error"], "bob-gh is attached already");
        assert_eq!(chrono::DateTime::parse_from_rfc3339(&stamp(&state, 412).unwrap()).unwrap(), at);

        assert_eq!(detach_with(&app, 412, &g.session_key).await.status, StatusCode::OK);
        assert_eq!(state.keys.tag(&g.session_key), None);
        let t = state.db.task(REPO, 412).unwrap().unwrap();
        assert_eq!((t.attached_by, t.pending), (None, Some(BurstReason::AfterAttach)));
        assert_eq!(stamp(&state, 412).as_deref(), Some(""));
    }

    #[tokio::test]
    async fn detach_takes_only_this_tasks_live_attach_key() {
        let (state, _) = state();
        for issue in [412, 413, 414] {
            put(&state, issue, Status::Building, Some("sess"));
        }
        let app = internal_router(state.clone());
        let ours = attach_as(&app, 412, "bob-gh").await.json()["session_key"].as_str().unwrap().to_string();
        let theirs = attach_as(&app, 413, "ann-gh").await.json()["session_key"].as_str().unwrap().to_string();
        let at = stamp(&state, 412);
        let others = [
            "fk_made_up".to_string(),
            theirs.clone(),
            state.keys.issue("example-team/shop#412"),
            state.keys.issue("example-team/shop#412 attach ann-gh"),
        ];
        for key in &others {
            let a = detach_with(&app, 412, key).await;
            assert_eq!(a.status, StatusCode::BAD_REQUEST);
            assert_eq!(a.json()["error"], "that key isn't this task's live attach key");
        }
        assert_eq!(detach_with(&app, 414, "fk_made_up").await.status, StatusCode::BAD_REQUEST);

        for key in [&ours, &theirs, &others[2], &others[3]] {
            assert!(state.keys.tag(key).is_some());
        }
        let t = state.db.task(REPO, 412).unwrap().unwrap();
        assert_eq!((t.attached_by.as_deref(), t.pending), (Some("bob-gh"), None));
        assert_eq!(state.db.task(REPO, 413).unwrap().unwrap().attached_by.as_deref(), Some("ann-gh"));
        assert_eq!(state.db.task(REPO, 414).unwrap().unwrap().pending, None);
        assert_eq!(stamp(&state, 412), at);
        assert_eq!(stamp(&state, 414), None);
    }

    #[tokio::test]
    async fn attach_is_for_the_repos_members() {
        let (state, _) = state();
        put(&state, 412, Status::Building, Some("sess-1"));
        let app = internal_router(state.clone());
        let a = attach_as(&app, 412, "stranger").await;
        assert_eq!(a.status, StatusCode::FORBIDDEN);
        assert_eq!(a.json()["error"], "stranger may not step into example-team/shop's sessions");
        assert_eq!(state.db.task(REPO, 412).unwrap().unwrap().attached_by, None);
        assert_eq!(stamp(&state, 412), None);
    }

    #[tokio::test]
    async fn collaborators_with_an_attach_key_may_attach() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::sample();
        cfg.data_dir = tmp.path().to_path_buf();
        std::fs::create_dir_all(cfg.state_dir()).unwrap();
        let line = "command=\"/usr/local/bin/forge attach-gate Pusher\",restrict,pty ssh-ed25519 AAAA\n";
        std::fs::write(cfg.attach_keys_path(), line).unwrap();
        let (state, _) = state_with(cfg);
        put(&state, 412, Status::Building, Some("sess-1"));
        let app = internal_router(state.clone());
        assert_eq!(attach_as(&app, 412, "stranger").await.status, StatusCode::FORBIDDEN);
        assert_eq!(attach_as(&app, 412, "pusher").await.status, StatusCode::OK);
        assert_eq!(state.db.task(REPO, 412).unwrap().unwrap().attached_by.as_deref(), Some("Pusher"));
    }

    #[tokio::test]
    async fn stepping_in_again_keeps_the_time_and_only_the_newest_key() {
        let (state, _) = state();
        put(&state, 412, Status::Building, Some("sess-1"));
        let app = internal_router(state.clone());
        let first = attach_as(&app, 412, "bob-gh").await.json()["session_key"].as_str().unwrap().to_string();
        let since = stamp(&state, 412);
        let second = attach_as(&app, 412, "bob-gh").await.json()["session_key"].as_str().unwrap().to_string();
        assert_eq!(stamp(&state, 412), since, "the first time stands");
        assert_eq!(state.keys.tag(&first), None, "the earlier key stops working");
        assert!(state.keys.tag(&second).is_some());
        assert_eq!(detach_with(&app, 412, &second).await.status, StatusCode::OK);
        assert_eq!(state.keys.tag(&second), None);
    }

    #[tokio::test]
    async fn detach_of_a_task_waiting_on_someone_owes_nothing() {
        let (state, _) = state();
        let mut t = Task::new(REPO, 412, "Add an export", "bob-gh", Status::Building, "ui");
        t.session_id = Some("sess-1".into());
        t.waiting_on = Some("bob-gh".into());
        state.db.upsert_task(&t).unwrap();
        let app = internal_router(state.clone());
        let key = attach_as(&app, 412, "bob-gh").await.json()["session_key"].as_str().unwrap().to_string();
        assert_eq!(detach_with(&app, 412, &key).await.status, StatusCode::OK);
        let t = state.db.task(REPO, 412).unwrap().unwrap();
        assert_eq!((t.attached_by, t.pending, t.waiting_on.as_deref()), (None, None, Some("bob-gh")));
    }

    #[tokio::test]
    async fn attach_needs_a_task_with_a_session() {
        let (state, _) = state();
        put(&state, 5, Status::Building, None);
        put(&state, 6, Status::Reviewed, Some("sess-6"));
        let app = internal_router(state.clone());
        let who = || Some(json!({ "who": "ann-gh" }));
        assert_eq!(send(&app, "POST", "/internal/attach/example-team/shop/9", &[], who()).await.status, StatusCode::NOT_FOUND);
        assert_eq!(send(&app, "POST", "/internal/attach/example-team/shop/5", &[], who()).await.status, StatusCode::CONFLICT);

        let a = send(&app, "POST", "/internal/attach/example-team/shop/6", &[], who()).await;
        let key = a.json()["session_key"].as_str().unwrap().to_string();
        let a = send(&app, "POST", "/internal/detach/example-team/shop/6", &[], Some(json!({ "session_key": key }))).await;
        assert_eq!(a.status, StatusCode::OK);
        let t = state.db.task(REPO, 6).unwrap().unwrap();
        assert_eq!((t.attached_by, t.pending), (None, None));
    }
}
