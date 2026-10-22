//! Telling people things: Telegram pings for the team, and email for sign-in
//! codes (Resend's HTTP API).

use std::sync::Mutex;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use serde_json::json;

use crate::config::{read_secret, Config};

#[async_trait]
pub trait Notifier: Send + Sync {
    /// A short message to the team's Telegram chat.
    async fn ping(&self, text: &str) -> Result<()>;
    async fn email(&self, to: &str, subject: &str, body: &str) -> Result<()>;
}

/// Telegram and Resend as configured; a missing one is logged and skipped.
pub struct LiveNotifier {
    http: reqwest::Client,
    telegram: Option<Telegram>,
    mail: Option<Mail>,
    telegram_base: String,
    resend_base: String,
}

struct Telegram {
    token: String,
    chat_id: String,
}

struct Mail {
    api_key: String,
    from: String,
}

impl LiveNotifier {
    pub fn from_config(cfg: &Config) -> Result<LiveNotifier> {
        LiveNotifier::with_bases(cfg, "https://api.telegram.org", "https://api.resend.com")
    }

    /// Reads the secrets now, so a bad secret file stops the daemon at start.
    fn with_bases(cfg: &Config, telegram_base: &str, resend_base: &str) -> Result<LiveNotifier> {
        let telegram = match &cfg.telegram {
            Some(t) => Some(Telegram { token: read_secret(&t.bot_token_file)?, chat_id: t.chat_id.clone() }),
            None => None,
        };
        let mail = match &cfg.mail {
            Some(m) => Some(Mail { api_key: read_secret(&m.resend_api_key_file)?, from: m.from.clone() }),
            None => None,
        };
        Ok(LiveNotifier {
            http: reqwest::Client::builder().timeout(Duration::from_secs(20)).build()?,
            telegram,
            mail,
            telegram_base: telegram_base.trim_end_matches('/').to_string(),
            resend_base: resend_base.trim_end_matches('/').to_string(),
        })
    }
}

/// Sends a request and fails on a non-2xx answer. The error never carries
/// the URL or headers, so it can't leak a token.
async fn send(service: &str, req: reqwest::RequestBuilder) -> Result<()> {
    let resp = req.send().await.map_err(|e| anyhow!("{service}: {}", e.without_url()))?;
    let status = resp.status();
    if status.is_success() {
        return Ok(());
    }
    let body: String = resp.text().await.unwrap_or_default().chars().take(300).collect();
    bail!("{service} answered {status}: {body}")
}

#[async_trait]
impl Notifier for LiveNotifier {
    async fn ping(&self, text: &str) -> Result<()> {
        let Some(t) = &self.telegram else {
            tracing::info!("no telegram configured; ping skipped");
            return Ok(());
        };
        let url = format!("{}/bot{}/sendMessage", self.telegram_base, t.token);
        let body = json!({"chat_id": t.chat_id, "text": text, "disable_web_page_preview": true});
        send("telegram", self.http.post(url).json(&body)).await
    }

    async fn email(&self, to: &str, subject: &str, body: &str) -> Result<()> {
        let Some(m) = &self.mail else {
            tracing::info!("no mail configured; email skipped");
            return Ok(());
        };
        let url = format!("{}/emails", self.resend_base);
        let payload = json!({"from": m.from, "to": [to], "subject": subject, "text": body});
        send("resend", self.http.post(url).bearer_auth(&m.api_key).json(&payload)).await
    }
}

/// Keeps every message, for tests.
#[derive(Default)]
pub struct RecordingNotifier {
    pub pings: Mutex<Vec<String>>,
    /// (to, subject, body)
    pub emails: Mutex<Vec<(String, String, String)>>,
}

#[async_trait]
impl Notifier for RecordingNotifier {
    async fn ping(&self, text: &str) -> Result<()> {
        self.pings.lock().unwrap().push(text.to_string());
        Ok(())
    }
    async fn email(&self, to: &str, subject: &str, body: &str) -> Result<()> {
        self.emails
            .lock()
            .unwrap()
            .push((to.to_string(), subject.to_string(), body.to_string()));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use axum::http::{HeaderMap, StatusCode, Uri};
    use axum::{Json, Router};
    use serde_json::Value;

    use super::*;

    /// (path, Authorization header, JSON body) of each request a fake got.
    type Seen = Arc<Mutex<Vec<(String, Option<String>, Value)>>>;

    /// A local stand-in for Telegram and Resend that answers every request with `status`.
    async fn fake(status: StatusCode) -> (String, Seen) {
        let seen: Seen = Default::default();
        let log = seen.clone();
        let app = Router::new().fallback(move |uri: Uri, headers: HeaderMap, Json(body): Json<Value>| {
            let log = log.clone();
            async move {
                let auth = headers.get("authorization").and_then(|v| v.to_str().ok()).map(String::from);
                log.lock().unwrap().push((uri.path().to_string(), auth, body));
                (status, r#"{"ok":false,"description":"chat not found"}"#)
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), seen)
    }

    fn secret(dir: &Path, name: &str, value: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join(name);
        std::fs::write(&path, value).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        path
    }

    fn config(dir: &Path) -> Config {
        let mut cfg = crate::config::sample();
        cfg.telegram.as_mut().unwrap().bot_token_file = secret(dir, "tg", "123:tg-secret\n");
        cfg.mail.as_mut().unwrap().resend_api_key_file = secret(dir, "rs", "re_secret");
        cfg
    }

    #[tokio::test]
    async fn pings_telegram() {
        let dir = tempfile::tempdir().unwrap();
        let (base, seen) = fake(StatusCode::OK).await;
        let n = LiveNotifier::with_bases(&config(dir.path()), &base, &base).unwrap();
        n.ping("example-team/shop#4 needs bob-gh").await.unwrap();
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0, "/bot123:tg-secret/sendMessage");
        assert_eq!(
            seen[0].2,
            json!({"chat_id": "-100123", "text": "example-team/shop#4 needs bob-gh", "disable_web_page_preview": true})
        );
    }

    #[tokio::test]
    async fn emails_through_resend() {
        let dir = tempfile::tempdir().unwrap();
        let (base, seen) = fake(StatusCode::OK).await;
        let n = LiveNotifier::with_bases(&config(dir.path()), &base, &base).unwrap();
        n.email("bob@example.com", "Your Forge sign-in code", "Code 123456").await.unwrap();
        let seen = seen.lock().unwrap();
        assert_eq!(seen[0].0, "/emails");
        assert_eq!(seen[0].1.as_deref(), Some("Bearer re_secret"));
        assert_eq!(
            seen[0].2,
            json!({"from": "Forge <forge@example.com>", "to": ["bob@example.com"],
                   "subject": "Your Forge sign-in code", "text": "Code 123456"})
        );
    }

    #[tokio::test]
    async fn errors_never_show_a_secret() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        let (base, _) = fake(StatusCode::INTERNAL_SERVER_ERROR).await;
        let n = LiveNotifier::with_bases(&cfg, &base, &base).unwrap();
        let ping = format!("{:#}", n.ping("hi").await.unwrap_err());
        assert!(ping.contains("500") && !ping.contains("tg-secret"), "{ping}");
        let mail = format!("{:#}", n.email("a@example.com", "s", "b").await.unwrap_err());
        assert!(mail.contains("500") && !mail.contains("re_secret"), "{mail}");

        let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap();
        let n = LiveNotifier::with_bases(&cfg, &format!("http://{closed}"), &format!("http://{closed}")).unwrap();
        let ping = format!("{:#}", n.ping("hi").await.unwrap_err());
        assert!(!ping.contains("tg-secret"), "{ping}");
        let mail = format!("{:#}", n.email("a@example.com", "s", "b").await.unwrap_err());
        assert!(!mail.contains("re_secret"), "{mail}");
    }

    #[tokio::test]
    async fn missing_config_skips() {
        let mut cfg = crate::config::sample();
        cfg.telegram = None;
        cfg.mail = None;
        let (base, seen) = fake(StatusCode::OK).await;
        let n = LiveNotifier::with_bases(&cfg, &base, &base).unwrap();
        n.ping("hi").await.unwrap();
        n.email("a@example.com", "s", "b").await.unwrap();
        assert!(seen.lock().unwrap().is_empty());
    }
}
