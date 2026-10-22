//! The local Claude proxy. Sessions point ANTHROPIC_BASE_URL here with a
//! per-burst key; the proxy checks the key, picks a plan from the pool, swaps
//! in that plan's login, forwards to Anthropic and streams the answer back.
//! So sessions never hold a login. Listens on loopback only.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::header::{AUTHORIZATION, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, HOST};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use chrono::{DateTime, Duration, Utc};

use crate::db::Db;
use crate::pool::{limits_from_headers, reset_from_headers, Lease, Pool};

/// The keys of bursts and attached people that may use the proxy right now.
#[derive(Clone, Default)]
pub struct SessionKeys {
    inner: Arc<RwLock<HashMap<String, String>>>,
}

impl SessionKeys {
    pub fn new() -> SessionKeys {
        SessionKeys::default()
    }

    /// Makes a new random key for `tag` (e.g. "example-team/shop#412").
    pub fn issue(&self, tag: &str) -> String {
        use base64::Engine as _;
        use rand::RngCore as _;
        let mut bytes = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut bytes);
        let key = format!("fk_{}", base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes));
        self.inner.write().unwrap().insert(key.clone(), tag.to_string());
        key
    }

    pub fn revoke(&self, key: &str) {
        self.inner.write().unwrap().remove(key);
    }

    /// The tag a live key was issued for.
    pub fn tag(&self, key: &str) -> Option<String> {
        self.inner.read().unwrap().get(key).cloned()
    }

    /// Revokes every live key whose tag starts with `prefix`, e.g. a task's
    /// attach keys once the engine finds the attach stale.
    pub fn revoke_tagged(&self, prefix: &str) {
        self.inner.write().unwrap().retain(|_, tag| !tag.starts_with(prefix));
    }
}

#[derive(Clone)]
pub struct ProxyState {
    pub pool: Arc<Pool>,
    pub keys: SessionKeys,
    pub db: Arc<Db>,
    /// e.g. "https://api.anthropic.com"
    pub upstream: String,
    pub client: reqwest::Client,
}

/// Headers that belong to one connection, never passed on (RFC 9110 7.6.1).
const HOP_BY_HOP: [&str; 8] = [
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// The beta Anthropic needs to take a plan's login as a bearer token.
const OAUTH_BETA: &str = "oauth-2025-04-20";

/// The largest request body passed on; Anthropic's own limit is 32 MB.
const BODY_LIMIT: usize = 64 << 20;

/// How long a plan refused with a 429 stays out when no reset is given.
const RATE_LIMITED_FOR: Duration = Duration::minutes(5);

/// How long a plan whose login Anthropic refused stays out.
const LOGIN_REFUSED_FOR: Duration = Duration::hours(1);

/// Forwards every method and path. Requests without a live key get 401;
/// with every plan out or at a ceiling, 529 with a JSON error body.
pub fn router(state: ProxyState) -> Router {
    Router::new().fallback(forward).with_state(state)
}

pub async fn serve(addr: SocketAddr, state: ProxyState) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await.with_context(|| format!("binding {addr}"))?;
    axum::serve(listener, router(state)).await?;
    Ok(())
}

async fn forward(State(state): State<ProxyState>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let Some(tag) = keys_offered(&parts.headers).find_map(|key| state.keys.tag(key)) else {
        return error(StatusCode::UNAUTHORIZED, "authentication_error", "a live Forge session key is needed");
    };
    if !forwarded(parts.uri.path()) {
        tracing::warn!(path = parts.uri.path(), "refused a path the proxy doesn't forward");
        return error(StatusCode::NOT_FOUND, "not_found_error", "Forge forwards only the Messages and Models APIs");
    }
    let messages = parts.uri.path().starts_with("/v1/messages");
    let Ok(body) = axum::body::to_bytes(body, BODY_LIMIT).await else {
        return error(StatusCode::PAYLOAD_TOO_LARGE, "invalid_request_error", "the request body is too large");
    };
    let path = parts.uri.path_and_query().map_or("/", |p| p.as_str());
    // Check the path as it will be sent: URL parsing resolves "..", "%2e" and the like.
    let url = match reqwest::Url::parse(&format!("{}{path}", state.upstream.trim_end_matches('/'))) {
        Ok(url) if url.path() == parts.uri.path() && forwarded(url.path()) => url,
        _ => {
            tracing::warn!(path = parts.uri.path(), "refused a path that changes when sent");
            return error(StatusCode::NOT_FOUND, "not_found_error", "Forge forwards only the Messages and Models APIs");
        }
    };
    let headers = outgoing(&parts.headers);
    let Some(mut lease) = state.pool.pick_for(&tag, None, Utc::now()) else {
        return overloaded();
    };
    let mut retried = false;
    loop {
        let resp = match send(&state, &parts.method, &url, &headers, &body, &lease).await {
            Ok(resp) => resp,
            Err(e) => {
                tracing::warn!(plan = %lease.label, "upstream failed: {}", e.without_url());
                return error(StatusCode::BAD_GATEWAY, "api_error", "Forge couldn't reach Anthropic");
            }
        };
        let now = Utc::now();
        let Some((until, why)) = refusal(resp.status(), resp.headers(), messages, now) else {
            return respond(resp);
        };
        tracing::warn!(plan = %lease.label, status = resp.status().as_u16(), %until, "plan out: {why}");
        state.pool.mark_out(&lease.label, until, why);
        if retried {
            return respond(resp);
        }
        match state.pool.pick_for(&tag, Some(&lease.label), now) {
            Some(other) => (lease, retried) = (other, true),
            None => return respond(resp),
        }
    }
}

/// Sends the request on the plan's login and records the limits its answer carries.
async fn send(
    state: &ProxyState,
    method: &Method,
    url: &reqwest::Url,
    headers: &HeaderMap,
    body: &Bytes,
    lease: &Lease,
) -> reqwest::Result<reqwest::Response> {
    let resp = state
        .client
        .request(method.clone(), url.clone())
        .headers(headers.clone())
        .bearer_auth(lease.token())
        .body(body.clone())
        .send()
        .await?;
    if let Some(limits) = limits_from_headers(resp.headers(), Utc::now()) {
        if let Err(e) = state.db.save_limits(&lease.label, &limits) {
            tracing::warn!(plan = %lease.label, "saving limits: {e:#}");
        }
        state.pool.record(&lease.label, limits);
    }
    Ok(resp)
}

/// How stale a plan's usage may get before Forge checks it itself: plans in
/// use refresh it with every answer, so this only costs idle plans a request.
pub const CHECK_EVERY: Duration = Duration::minutes(30);

/// The model a usage check asks, for one token.
const CHECK_MODEL: &str = "claude-haiku-5-5";

/// Reads the usage of every plan due for a check (see [`CHECK_EVERY`]) with
/// the smallest Messages call there is, so the dashboard and the pool's
/// choices never work from a blank. A refused plan is kept out as usual.
pub async fn check_usage(state: &ProxyState) {
    let now = Utc::now();
    let url = match reqwest::Url::parse(&format!("{}/v1/messages", state.upstream.trim_end_matches('/'))) {
        Ok(url) => url,
        Err(e) => return tracing::warn!("usage check: bad upstream: {e}"),
    };
    let mut headers = HeaderMap::new();
    headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
    headers.insert("anthropic-beta", HeaderValue::from_static(OAUTH_BETA));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    let body = Bytes::from(
        serde_json::json!({
            "model": CHECK_MODEL,
            "max_tokens": 1,
            "system": "You are Claude Code, Anthropic's official CLI for Claude.",
            "messages": [{ "role": "user", "content": "ok" }],
        })
        .to_string(),
    );
    for lease in state.pool.due_for_check(now, CHECK_EVERY) {
        match send(state, &Method::POST, &url, &headers, &body, &lease).await {
            Ok(resp) => {
                if let Some((until, why)) = refusal(resp.status(), resp.headers(), true, Utc::now()) {
                    tracing::warn!(plan = %lease.label, status = resp.status().as_u16(), %until, "plan out: {why}");
                    state.pool.mark_out(&lease.label, until, why);
                }
            }
            Err(e) => tracing::warn!(plan = %lease.label, "usage check failed: {}", e.without_url()),
        }
    }
}

/// The paths a Claude session needs: the plan logins go nowhere else. No
/// escapes or dot segments, which a server might resolve to somewhere else.
fn forwarded(path: &str) -> bool {
    let plain = !path.contains('%') && !path.contains("..");
    plain && (matches!(path, "/v1/messages" | "/v1/messages/count_tokens" | "/v1/models") || path.starts_with("/v1/models/"))
}

/// Until when, and why, an answer keeps its plan out: a 429 until the unified
/// reset (else five minutes), or a login the Messages API refused, for an hour.
fn refusal(status: StatusCode, headers: &HeaderMap, messages: bool, now: DateTime<Utc>) -> Option<(DateTime<Utc>, &'static str)> {
    match status {
        StatusCode::TOO_MANY_REQUESTS => {
            let until = reset_from_headers(headers).filter(|r| *r > now).unwrap_or(now + RATE_LIMITED_FOR);
            Some((until, "rate limited"))
        }
        StatusCode::UNAUTHORIZED if messages => Some((now + LOGIN_REFUSED_FOR, "login refused")),
        _ => None,
    }
}

/// The upstream answer as it came, streamed, without hop-by-hop headers.
fn respond(resp: reqwest::Response) -> Response {
    let status = resp.status();
    let headers = without_hop_by_hop(resp.headers());
    let mut out = Response::new(Body::from_stream(resp.bytes_stream()));
    *out.status_mut() = status;
    *out.headers_mut() = headers;
    out
}

/// The keys a request offers: `Authorization: Bearer <key>`, then `x-api-key`.
fn keys_offered(headers: &HeaderMap) -> impl Iterator<Item = &str> {
    let bearer = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split_once(' '))
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
        .map(|(_, key)| key.trim());
    let api_key = headers.get("x-api-key").and_then(|v| v.to_str().ok()).map(str::trim);
    bearer.into_iter().chain(api_key).filter(|key| !key.is_empty())
}

/// The client's headers as sent upstream: no hop-by-hop, host, length or
/// session key, and the OAuth beta added to any betas it asked for.
fn outgoing(incoming: &HeaderMap) -> HeaderMap {
    let mut headers = without_hop_by_hop(incoming);
    for name in [HOST, CONTENT_LENGTH, AUTHORIZATION] {
        headers.remove(name);
    }
    headers.remove("x-api-key");
    headers.insert("anthropic-beta", with_oauth_beta(incoming));
    headers
}

fn with_oauth_beta(headers: &HeaderMap) -> HeaderValue {
    let mut betas: Vec<&str> = headers
        .get_all("anthropic-beta")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(str::trim)
        .filter(|b| !b.is_empty())
        .collect();
    if !betas.contains(&OAUTH_BETA) {
        betas.push(OAUTH_BETA);
    }
    HeaderValue::from_str(&betas.join(",")).unwrap_or(HeaderValue::from_static(OAUTH_BETA))
}

fn without_hop_by_hop(headers: &HeaderMap) -> HeaderMap {
    let listed: Vec<String> = headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|name| name.trim().to_ascii_lowercase())
        .collect();
    let mut out = headers.clone();
    for name in HOP_BY_HOP.iter().copied().chain(listed.iter().map(String::as_str)) {
        out.remove(name);
    }
    out
}

/// An error in Anthropic's own shape, so Claude Code shows it plainly.
fn error(status: StatusCode, kind: &str, message: &str) -> Response {
    let body = serde_json::json!({"type": "error", "error": {"type": kind, "message": message}});
    (status, Json(body)).into_response()
}

fn overloaded() -> Response {
    let status = StatusCode::from_u16(529).expect("529 is a status code");
    error(status, "overloaded_error", "every Claude plan is at its ceiling")
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use axum::http::header::CONTENT_TYPE;
    use chrono::TimeZone;
    use tokio::sync::mpsc;
    use tower::ServiceExt as _;

    use super::*;

    /// When the fake upstream says a busy plan may be used again.
    const RESET: i64 = 4_000_000_000;

    struct Seen {
        method: Method,
        uri: String,
        headers: HeaderMap,
        body: Bytes,
    }

    /// A fake Anthropic: records each request and answers by the plan's token.
    #[derive(Clone, Default)]
    struct Upstream {
        seen: Arc<Mutex<Vec<Seen>>>,
        stream: Arc<Mutex<Option<mpsc::Receiver<Bytes>>>>,
    }

    impl Upstream {
        fn auths(&self) -> Vec<String> {
            let seen = self.seen.lock().unwrap();
            seen.iter().map(|s| s.headers[AUTHORIZATION].to_str().unwrap().to_string()).collect()
        }
    }

    fn token(label: &str) -> String {
        format!("plan-token-{label}")
    }

    async fn upstream(State(up): State<Upstream>, req: Request) -> Response {
        let (parts, body) = req.into_parts();
        let body = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let auth = parts.headers[AUTHORIZATION].to_str().unwrap().to_string();
        let uri = parts.uri.to_string();
        up.seen.lock().unwrap().push(Seen { method: parts.method, uri, headers: parts.headers, body });
        let limits = [
            ("anthropic-ratelimit-unified-5h-utilization", "0.42".to_string()),
            ("anthropic-ratelimit-unified-5h-reset", (Utc::now().timestamp() + 3600).to_string()),
            ("anthropic-ratelimit-unified-7d-utilization", "0.1".to_string()),
        ];
        let refused = r#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}"#;
        match auth.strip_prefix("Bearer plan-token-").unwrap() {
            "busy" => {
                let headers = [("anthropic-ratelimit-unified-reset", RESET.to_string())];
                (StatusCode::TOO_MANY_REQUESTS, headers, refused).into_response()
            }
            "throttled" => (StatusCode::TOO_MANY_REQUESTS, refused).into_response(),
            "dead" => StatusCode::UNAUTHORIZED.into_response(),
            _ => match up.stream.lock().unwrap().take() {
                Some(rx) => {
                    let chunks = futures_util::stream::unfold(rx, |mut rx| async move {
                        rx.recv().await.map(|b| (Ok::<_, std::convert::Infallible>(b), rx))
                    });
                    (limits, [(CONTENT_TYPE, "text/event-stream")], Body::from_stream(chunks)).into_response()
                }
                None => (limits, [("request-id", "req_1")], r#"{"ok":true}"#).into_response(),
            },
        }
    }

    struct Setup {
        state: ProxyState,
        key: String,
        up: Upstream,
    }

    async fn setup(plans: &[&str]) -> Setup {
        let up = Upstream::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new().fallback(upstream).with_state(up.clone());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let plans = plans.iter().map(|l| (l.to_string(), token(l))).collect();
        let keys = SessionKeys::new();
        let key = keys.issue("example-team/shop#412");
        let state = ProxyState {
            pool: Arc::new(Pool::from_tokens(plans, 85.0, 90.0)),
            keys,
            db: Arc::new(Db::open_in_memory().unwrap()),
            upstream: format!("http://{addr}/"),
            client: reqwest::Client::new(),
        };
        Setup { state, key, up }
    }

    fn post(key: &str) -> Request {
        Request::post("/v1/messages?beta=true")
            .header(AUTHORIZATION, format!("Bearer {key}"))
            .body(Body::from(r#"{"model":"m"}"#))
            .unwrap()
    }

    async fn call(state: &ProxyState, req: Request) -> (StatusCode, HeaderMap, Bytes) {
        let resp = router(state.clone()).oneshot(req).await.unwrap();
        let (status, headers) = (resp.status(), resp.headers().clone());
        (status, headers, axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap())
    }

    fn json(bytes: &Bytes) -> serde_json::Value {
        serde_json::from_slice(bytes).unwrap()
    }

    #[tokio::test]
    async fn forwards_on_the_plan_login() {
        let s = setup(&["a"]).await;
        let req = Request::post("/v1/messages?beta=true")
            .header(AUTHORIZATION, format!("Bearer {}", s.key))
            .header("x-api-key", "sk-session-should-not-pass")
            .header("anthropic-beta", "claude-code-20250219, fine-grained-tool-streaming-2025-05-14")
            .header("anthropic-version", "2023-06-01")
            .header(CONNECTION, "x-drop")
            .header("x-drop", "1")
            .header("keep-alive", "timeout=5")
            .header("proxy-authorization", "Basic eHl6")
            .body(Body::from(r#"{"model":"m"}"#))
            .unwrap();
        let (status, headers, body) = call(&s.state, req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers["request-id"], "req_1");
        assert_eq!(&body[..], br#"{"ok":true}"#);
        let seen = s.up.seen.lock().unwrap();
        let got = &seen[0];
        assert_eq!((got.method.as_str(), got.uri.as_str()), ("POST", "/v1/messages?beta=true"));
        assert_eq!(&got.body[..], br#"{"model":"m"}"#);
        assert_eq!(got.headers[AUTHORIZATION], "Bearer plan-token-a");
        assert_eq!(
            got.headers["anthropic-beta"],
            "claude-code-20250219,fine-grained-tool-streaming-2025-05-14,oauth-2025-04-20"
        );
        assert_eq!(got.headers["anthropic-version"], "2023-06-01");
        for gone in ["x-api-key", "x-drop", "keep-alive", "proxy-authorization"] {
            assert!(!got.headers.contains_key(gone), "{gone} was passed on");
        }
    }

    #[tokio::test]
    async fn takes_the_key_as_x_api_key() {
        let s = setup(&["a"]).await;
        let req = Request::get("/v1/models")
            .header("x-api-key", &s.key)
            .header("anthropic-beta", OAUTH_BETA)
            .body(Body::empty())
            .unwrap();
        assert_eq!(call(&s.state, req).await.0, StatusCode::OK);
        let seen = s.up.seen.lock().unwrap();
        assert_eq!(seen[0].headers[AUTHORIZATION], "Bearer plan-token-a");
        assert_eq!(seen[0].headers["anthropic-beta"], OAUTH_BETA);
        assert!(!seen[0].headers.contains_key("x-api-key"));
    }

    #[tokio::test]
    async fn refuses_without_a_live_key() {
        let s = setup(&["a"]).await;
        let bare = Request::post("/v1/messages").body(Body::empty()).unwrap();
        let revoked = s.state.keys.issue("gone#1");
        s.state.keys.revoke(&revoked);
        for req in [bare, post("fk_unknown"), post(&revoked)] {
            let (status, _, body) = call(&s.state, req).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED);
            assert_eq!(json(&body)["error"]["type"], "authentication_error");
        }
        assert!(s.up.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn saves_the_limits_each_answer_carries() {
        let s = setup(&["a"]).await;
        call(&s.state, post(&s.key)).await;
        let saved = s.state.db.all_limits().unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].0, "a");
        assert!((saved[0].1.five_hour_pct.unwrap() - 42.0).abs() < 1e-9);
        assert!((saved[0].1.week_pct.unwrap() - 10.0).abs() < 1e-9);
        assert_eq!(s.state.pool.snapshot(Utc::now())[0].limits, Some(saved[0].1.clone()));
    }

    #[tokio::test]
    async fn streams_sse_as_it_arrives() {
        let s = setup(&["a"]).await;
        let (tx, rx) = mpsc::channel::<Bytes>(4);
        *s.up.stream.lock().unwrap() = Some(rx);
        let addr = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
        tokio::spawn(serve(addr, s.state.clone()));
        let client = reqwest::Client::new();
        let url = format!("http://{addr}/v1/messages");
        let mut resp = loop {
            match client.post(&url).bearer_auth(&s.key).body("{}").send().await {
                Ok(resp) => break resp,
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
            }
        };
        assert_eq!(resp.headers()[CONTENT_TYPE], "text/event-stream");
        let first = "event: message_start\ndata: {\"type\":\"message_start\"}\n\n";
        let rest = "event: content_block_delta\ndata: {\"delta\":{\"text\":\"hé\"}}\n\nevent: message_stop\ndata: {}\n\n";
        let mut got = Vec::new();
        tx.send(Bytes::from(first)).await.unwrap();
        let wait = std::time::Duration::from_secs(5);
        while got.len() < first.len() {
            let chunk = tokio::time::timeout(wait, resp.chunk()).await.expect("first event held back");
            got.extend_from_slice(&chunk.unwrap().unwrap());
        }
        tx.send(Bytes::from(rest)).await.unwrap();
        drop(tx);
        while let Some(chunk) = tokio::time::timeout(wait, resp.chunk()).await.unwrap().unwrap() {
            got.extend_from_slice(&chunk);
        }
        assert_eq!(String::from_utf8(got).unwrap(), format!("{first}{rest}"));
    }

    #[tokio::test]
    async fn retries_a_429_on_another_plan() {
        let s = setup(&["busy", "ok"]).await;
        let (status, _, body) = call(&s.state, post(&s.key)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(&body[..], br#"{"ok":true}"#);
        assert_eq!(s.up.auths(), ["Bearer plan-token-busy", "Bearer plan-token-ok"]);
        assert!(s.up.seen.lock().unwrap().iter().all(|r| &r.body[..] == br#"{"model":"m"}"#));
        let busy = &s.state.pool.snapshot(Utc::now())[0];
        assert_eq!(busy.out_until, Utc.timestamp_opt(RESET, 0).single());
        assert_eq!(busy.out_reason.as_deref(), Some("rate limited"));
        call(&s.state, post(&s.key)).await;
        assert_eq!(s.up.auths().len(), 3);
        assert_eq!(s.up.auths()[2], "Bearer plan-token-ok");
    }

    #[tokio::test]
    async fn a_429_with_nowhere_else_goes_back_and_holds_five_minutes() {
        let s = setup(&["throttled"]).await;
        let before = Utc::now();
        let (status, _, body) = call(&s.state, post(&s.key)).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(json(&body)["error"]["type"], "rate_limit_error");
        let until = s.state.pool.snapshot(Utc::now())[0].out_until.unwrap();
        assert!(until >= before + RATE_LIMITED_FOR && until <= Utc::now() + RATE_LIMITED_FOR);
        assert_eq!(call(&s.state, post(&s.key)).await.0.as_u16(), 529);
        assert_eq!(s.up.auths().len(), 1);
    }

    #[tokio::test]
    async fn forwards_only_the_paths_sessions_need() {
        let s = setup(&["a"]).await;
        for path in [
            "/v1/organizations/me",
            "/api/oauth/usage",
            "/v1/messages/batches",
            "/",
            "/v1/models/../../api/oauth/usage",
            "/v1/models/%2e%2e/%2E%2E/v1/organizations/me",
            "/v1/models/..%2f..%2fapi/oauth/usage",
        ] {
            let req = Request::get(path).header("x-api-key", &s.key).body(Body::empty()).unwrap();
            assert_eq!(call(&s.state, req).await.0, StatusCode::NOT_FOUND, "{path}");
        }
        assert!(s.up.seen.lock().unwrap().is_empty());
        for path in ["/v1/messages/count_tokens", "/v1/models", "/v1/models/claude-opus-5-5"] {
            let req = Request::get(path).header("x-api-key", &s.key).body(Body::empty()).unwrap();
            assert_eq!(call(&s.state, req).await.0, StatusCode::OK, "{path}");
        }
    }

    #[tokio::test]
    async fn a_refused_login_elsewhere_than_messages_keeps_the_plan() {
        let s = setup(&["dead", "ok"]).await;
        let req = Request::get("/v1/models").header("x-api-key", &s.key).body(Body::empty()).unwrap();
        assert_eq!(call(&s.state, req).await.0, StatusCode::UNAUTHORIZED);
        assert!(s.state.pool.snapshot(Utc::now()).iter().all(|p| p.out_reason.is_none()));
    }

    #[tokio::test]
    async fn a_refused_login_takes_its_plan_out() {
        let s = setup(&["dead", "ok"]).await;
        assert_eq!(call(&s.state, post(&s.key)).await.0, StatusCode::OK);
        assert_eq!(s.up.auths(), ["Bearer plan-token-dead", "Bearer plan-token-ok"]);
        let dead = &s.state.pool.snapshot(Utc::now())[0];
        assert_eq!(dead.out_reason.as_deref(), Some("login refused"));
    }

    #[tokio::test]
    async fn answers_529_when_every_plan_is_at_its_ceiling() {
        let s = setup(&["a", "b"]).await;
        let full = crate::types::Limits {
            five_hour_pct: Some(90.0),
            five_hour_resets: Some(Utc::now() + Duration::hours(1)),
            week_pct: None,
            week_resets: None,
            read_at: Utc::now(),
        };
        s.state.pool.record("a", full.clone());
        s.state.pool.record("b", full);
        let (status, _, body) = call(&s.state, post(&s.key)).await;
        assert_eq!(status.as_u16(), 529);
        let want = serde_json::json!({"type":"error","error":{"type":"overloaded_error","message":"every Claude plan is at its ceiling"}});
        assert_eq!(json(&body), want);
        assert!(s.up.seen.lock().unwrap().is_empty());
    }
}
