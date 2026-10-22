//! Email sign-in: a 6-digit code mailed to a member, good
//! for 10 minutes and 5 tries, traded for a token good for `session_days`.
//! Codes and tokens are only stored as HMACs keyed by the auth secret.

use std::sync::Arc;

use anyhow::Result;
use base64::Engine as _;
use chrono::{DateTime, Duration, Utc};
use hmac::{Hmac, Mac};
use rand::{Rng as _, RngCore as _};
use sha2::Sha256;

use crate::config::{Config, Member};
use crate::db::{ApiSession, Challenge, Db};
use crate::notify::Notifier;

const CODE_SUBJECT: &str = "Your Forge sign-in code";
const CODE_MINUTES: i64 = 10;
const MAX_ATTEMPTS: u32 = 5;
const CODES_PER_HOUR: u32 = 5;
/// Longer tokens are never ours; refusing them early keeps hashing cheap.
const MAX_TOKEN_LEN: usize = 128;
/// The longest an email address can be.
const MAX_EMAIL_LEN: usize = 254;

pub struct Auth {
    db: Arc<Db>,
    secret: Vec<u8>,
    cfg: Arc<Config>,
    notify: Arc<dyn Notifier>,
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum AuthError {
    #[error("too many sign-in codes for this email; wait an hour")]
    RateLimited,
    #[error("too many wrong codes; ask for a new one")]
    TooManyAttempts,
}

impl Auth {
    pub fn new(db: Arc<Db>, secret: Vec<u8>, cfg: Arc<Config>, notify: Arc<dyn Notifier>) -> Auth {
        Auth { db, secret, cfg, notify }
    }

    /// Starts a sign-in and returns the challenge token the client keeps.
    /// The code is emailed only when `email` is a member; the answer looks the
    /// same either way. At most 5 codes per email an hour (AuthError::RateLimited).
    pub async fn begin(&self, email: &str, now: DateTime<Utc>) -> Result<String> {
        let email = email.trim().to_lowercase();
        if email.len() > MAX_EMAIL_LEN || !email.contains('@') {
            // Can't be a member: the same answer, and nothing stored.
            return Ok(random_token());
        }
        // Keep the last hour of challenges: the rate limit counts them.
        self.db.purge_expired(now - Duration::hours(1))?;
        if self.db.challenges_since(&email, now - Duration::hours(1))? >= CODES_PER_HOUR {
            return Err(AuthError::RateLimited.into());
        }
        let challenge = random_token();
        let code = format!("{:06}", rand::thread_rng().gen_range(0..1_000_000));
        self.db.put_challenge(&Challenge {
            token_hash: self.challenge_hash(&challenge),
            email: email.clone(),
            code_hash: hex::encode(self.mac(&format!("{challenge}:{code}")).finalize().into_bytes()),
            created_at: now,
            expires_at: now + Duration::minutes(CODE_MINUTES),
            attempts: 0,
        })?;
        if let Some(member) = self.cfg.member_by_email(&email) {
            let body = format!("Your Forge sign-in code is {code}. It works for {CODE_MINUTES} minutes.");
            self.notify.email(&member.email, CODE_SUBJECT, &body).await?;
        }
        Ok(challenge)
    }

    /// Trades a challenge and its code for an API token. Ok(None) for a wrong
    /// or expired code; AuthError::TooManyAttempts after 5 wrong ones.
    pub fn finish(&self, challenge: &str, code: &str, now: DateTime<Utc>) -> Result<Option<String>> {
        if challenge.len() > MAX_TOKEN_LEN {
            return Ok(None);
        }
        let key = self.challenge_hash(challenge);
        let Some(c) = self.db.challenge(&key)? else { return Ok(None) };
        if now >= c.expires_at {
            return Ok(None);
        }
        // Count the try before checking it, so parallel guesses can't share one count.
        let tries = self.db.bump_attempts(&key)?;
        if tries == 0 {
            return Ok(None);
        }
        if tries > MAX_ATTEMPTS {
            return Err(AuthError::TooManyAttempts.into());
        }
        if !self.code_matches(&c.code_hash, challenge, code.trim()) {
            return if tries == MAX_ATTEMPTS { Err(AuthError::TooManyAttempts.into()) } else { Ok(None) };
        }
        self.db.delete_challenge(&key)?;
        if self.cfg.member_by_email(&c.email).is_none() {
            return Ok(None);
        }
        let token = random_token();
        self.db.put_session(&ApiSession {
            token_hash: self.token_hash(&token),
            email: c.email,
            expires_at: now + Duration::days(self.cfg.auth.session_days.into()),
        })?;
        Ok(Some(token))
    }

    /// The member an API token belongs to, if it's live and they still are one.
    pub fn member(&self, token: &str, now: DateTime<Utc>) -> Result<Option<Member>> {
        if token.is_empty() || token.len() > MAX_TOKEN_LEN {
            return Ok(None);
        }
        let Some(session) = self.db.session(&self.token_hash(token))? else { return Ok(None) };
        if session.expires_at <= now {
            return Ok(None);
        }
        Ok(self.cfg.member_by_email(&session.email).cloned())
    }

    pub fn sign_out(&self, token: &str) -> Result<()> {
        if token.len() <= MAX_TOKEN_LEN {
            self.db.delete_session(&self.token_hash(token))?;
        }
        Ok(())
    }

    fn mac(&self, value: &str) -> Hmac<Sha256> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.secret).expect("HMAC takes a key of any length");
        mac.update(value.as_bytes());
        mac
    }

    fn hash(&self, value: &str) -> String {
        hex::encode(self.mac(value).finalize().into_bytes())
    }

    fn challenge_hash(&self, challenge: &str) -> String {
        self.hash(&format!("challenge:{challenge}"))
    }

    fn token_hash(&self, token: &str) -> String {
        self.hash(&format!("token:{token}"))
    }

    /// Compares in constant time.
    fn code_matches(&self, code_hash: &str, challenge: &str, code: &str) -> bool {
        let Ok(stored) = hex::decode(code_hash) else { return false };
        self.mac(&format!("{challenge}:{code}")).verify_slice(&stored).is_ok()
    }
}

/// 32 random bytes, base64url.
fn random_token() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::notify::RecordingNotifier;

    fn t0() -> DateTime<Utc> {
        "2026-10-20T08:00:00Z".parse().unwrap()
    }

    fn setup() -> (Auth, Arc<RecordingNotifier>, Arc<Db>) {
        let db = Arc::new(Db::open_in_memory().unwrap());
        let notes = Arc::new(RecordingNotifier::default());
        let cfg = Arc::new(crate::config::sample());
        (Auth::new(db.clone(), b"test-secret".to_vec(), cfg, notes.clone()), notes, db)
    }

    /// The code in the last email sent.
    fn last_code(notes: &RecordingNotifier) -> String {
        let emails = notes.emails.lock().unwrap();
        let body = &emails.last().expect("an email").2;
        body.chars().filter(char::is_ascii_digit).take(6).collect()
    }

    fn wrong(code: &str) -> String {
        format!("{:06}", (code.parse::<u32>().unwrap() + 1) % 1_000_000)
    }

    fn auth_error(r: Result<Option<String>>) -> AuthError {
        r.unwrap_err().downcast::<AuthError>().unwrap()
    }

    #[tokio::test]
    async fn member_gets_a_code_that_works_once() {
        let (auth, notes, _) = setup();
        let challenge = auth.begin(" Bob@Example.com ", t0()).await.unwrap();
        let (to, subject, body) = notes.emails.lock().unwrap()[0].clone();
        assert_eq!((to.as_str(), subject.as_str()), ("bob@example.com", "Your Forge sign-in code"));
        let code = last_code(&notes);
        assert_eq!(body, format!("Your Forge sign-in code is {code}. It works for 10 minutes."));

        let token = auth.finish(&challenge, &code, t0()).unwrap().expect("a token");
        assert_eq!(auth.member(&token, t0()).unwrap().unwrap().github, "bob-gh");
        assert_eq!(auth.finish(&challenge, &code, t0()).unwrap(), None);
    }

    #[tokio::test]
    async fn wrong_code_is_none_and_five_kill_it() {
        let (auth, notes, _) = setup();
        let challenge = auth.begin("bob@example.com", t0()).await.unwrap();
        let code = last_code(&notes);
        for _ in 0..4 {
            assert_eq!(auth.finish(&challenge, &wrong(&code), t0()).unwrap(), None);
        }
        assert_eq!(auth_error(auth.finish(&challenge, &wrong(&code), t0())), AuthError::TooManyAttempts);
        assert_eq!(auth_error(auth.finish(&challenge, &code, t0())), AuthError::TooManyAttempts);
    }

    #[tokio::test]
    async fn code_expires_after_ten_minutes() {
        let (auth, notes, _) = setup();
        let challenge = auth.begin("bob@example.com", t0()).await.unwrap();
        let code = last_code(&notes);
        assert_eq!(auth.finish(&challenge, &code, t0() + Duration::minutes(10)).unwrap(), None);
        assert_eq!(auth.finish("not-a-challenge", &code, t0()).unwrap(), None);
    }

    #[tokio::test]
    async fn five_codes_an_hour_per_email() {
        let (auth, _, _) = setup();
        for i in 0..5 {
            auth.begin("bob@example.com", t0() + Duration::minutes(i * 5)).await.unwrap();
        }
        let err = auth.begin("BOB@example.com", t0() + Duration::minutes(30)).await.unwrap_err();
        assert_eq!(err.downcast::<AuthError>().unwrap(), AuthError::RateLimited);
        auth.begin("ann@example.com", t0() + Duration::minutes(30)).await.unwrap();
        auth.begin("bob@example.com", t0() + Duration::minutes(61)).await.unwrap();
    }

    #[tokio::test]
    async fn non_member_gets_no_email_and_no_way_in() {
        let (auth, notes, _) = setup();
        let challenge = auth.begin("stranger@example.com", t0()).await.unwrap();
        assert_eq!(challenge.len(), 43);
        assert!(notes.emails.lock().unwrap().is_empty());
        assert_eq!(auth.finish(&challenge, "123456", t0()).unwrap(), None);
        let odd = auth.begin(&"x".repeat(300), t0()).await.unwrap();
        assert_eq!(odd.len(), 43);
    }

    #[tokio::test]
    async fn sign_out_and_expiry_end_a_token() {
        let (auth, notes, _) = setup();
        let challenge = auth.begin("ann@example.com", t0()).await.unwrap();
        let token = auth.finish(&challenge, &last_code(&notes), t0()).unwrap().unwrap();
        assert!(auth.member(&token, t0() + Duration::days(13)).unwrap().is_some());
        assert!(auth.member(&token, t0() + Duration::days(14)).unwrap().is_none());
        assert!(auth.member("bogus", t0()).unwrap().is_none());
        auth.sign_out(&token).unwrap();
        assert!(auth.member(&token, t0()).unwrap().is_none());
    }

    #[tokio::test]
    async fn removed_member_no_longer_resolves() {
        let (auth, notes, db) = setup();
        let challenge = auth.begin("bob@example.com", t0()).await.unwrap();
        let token = auth.finish(&challenge, &last_code(&notes), t0()).unwrap().unwrap();
        let mut cfg = crate::config::sample();
        cfg.repos[0].members.retain(|m| m.github != "bob-gh");
        let auth = Auth::new(db, b"test-secret".to_vec(), Arc::new(cfg), notes);
        assert!(auth.member(&token, t0()).unwrap().is_none());
    }
}
