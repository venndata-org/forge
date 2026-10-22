//! GitHub, as Forge's own GitHub App: Project items and their Status, issue
//! threads, comments, PRs, merges, and the token git pushes with.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, LINK};
use reqwest::{Method, RequestBuilder, Response};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Mutex;

use crate::config::{read_secret, GitHubConfig, RepoConfig};
use crate::types::{Comment, IssueThread, ProjectItem, PullRequest, Status};

#[async_trait]
pub trait GitHubApi: Send + Sync {
    /// The issues on the repo's Project, in the Project's order (top first).
    /// Items that aren't issues of this repo are left out.
    async fn project_items(&self, repo: &RepoConfig) -> Result<Vec<ProjectItem>>;

    /// Sets the item's Status field to `status`'s option on the repo's Project.
    async fn set_status(&self, repo: &RepoConfig, item_id: &str, status: Status) -> Result<()>;

    /// The issue with every comment, oldest first. `repo` is "owner/repo".
    async fn issue_thread(&self, repo: &str, number: u64) -> Result<IssueThread>;

    /// Posts a comment and returns its id.
    async fn comment(&self, repo: &str, number: u64, body: &str) -> Result<u64>;

    /// Opens an issue and returns its number.
    async fn create_issue(&self, repo: &str, title: &str, body: &str, labels: &[String]) -> Result<u64>;

    /// Opens a PR from `head` into `base`, or updates the title and body of
    /// the open PR for `head`, and returns it.
    async fn open_or_update_pr(&self, repo: &str, head: &str, base: &str, title: &str, body: &str) -> Result<PullRequest>;

    /// Squash-merges the PR, only if its head is still `sha`.
    async fn merge_pr(&self, repo: &str, number: u64, sha: &str, title: &str) -> Result<()>;

    /// A short-lived installation token git can fetch and push with.
    async fn git_token(&self) -> Result<String>;

    /// The logins of the repo's collaborators who can push, for `forge keys`:
    /// "anyone on the repo" may step into a session, not read-only viewers.
    async fn collaborators(&self, repo: &str) -> Result<Vec<String>>;

    /// Puts the repo's issue on its Project and returns the item id.
    async fn add_to_project(&self, repo: &RepoConfig, issue: u64) -> Result<String> {
        let _ = (repo, issue);
        bail!("this GitHub client can't add issues to a Project")
    }
}

/// A non-2xx answer from GitHub: its status and message, never the token.
#[derive(Debug, thiserror::Error)]
#[error("GitHub answered {status} to {request}: {message}")]
pub struct GitHubError {
    pub status: u16,
    /// Method and path, e.g. "PUT /repos/o/r/pulls/3/merge".
    pub request: String,
    pub message: String,
}

/// The GitHub App client: REST and GraphQL with an installation token,
/// refreshed before it expires.
pub struct AppClient {
    app_id: u64,
    installation_id: u64,
    pem: Vec<u8>,
    api: String,
    http: reqwest::Client,
    token: Mutex<Option<InstallationToken>>,
    projects: Mutex<HashMap<(String, u64), Project>>,
}

#[derive(Clone, Deserialize)]
struct InstallationToken {
    token: String,
    expires_at: DateTime<Utc>,
}

/// An installation token is renewed this long before it expires.
const RENEW_BEFORE: TimeDelta = TimeDelta::minutes(5);

/// What Forge needs of a Project. Looked up once, and forgotten whenever a
/// call that used it fails, so a Project edited since is read afresh.
#[derive(Clone)]
struct Project {
    id: String,
    status_field: String,
    /// Each Status option's (name, id).
    options: Vec<(String, String)>,
}

/// The login GitHub shows for a deleted user.
const GHOST: &str = "ghost";

/// A Project's id and Status field; OWNER becomes "organization" or "user".
const PROJECT_QUERY: &str = r#"query($login: String!, $number: Int!) {
  owner: OWNER(login: $login) {
    projectV2(number: $number) {
      id
      field(name: "Status") { ... on ProjectV2SingleSelectField { id options { id name } } }
    }
  }
}"#;

/// An issue's node id, for adding it to a Project.
const ISSUE_ID_QUERY: &str = r#"query($owner: String!, $name: String!, $number: Int!) {
  repository(owner: $owner, name: $name) { issue(number: $number) { id } }
}"#;

const ADD_ITEM_MUTATION: &str = r#"mutation($project: ID!, $content: ID!) {
  addProjectV2ItemById(input: { projectId: $project, contentId: $content }) { item { id } }
}"#;

/// One page of a Project's items, in the Project's order.
const ITEMS_QUERY: &str = r#"query($id: ID!, $after: String) {
  node(id: $id) {
    ... on ProjectV2 {
      items(first: 100, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          fieldValueByName(name: "Status") { ... on ProjectV2ItemFieldSingleSelectValue { name } }
          content {
            ... on Issue {
              number title state
              repository { nameWithOwner }
              author { login }
              labels(first: 50) { nodes { name } }
            }
          }
        }
      }
    }
  }
}"#;

const SET_STATUS_MUTATION: &str = r#"mutation($project: ID!, $item: ID!, $field: ID!, $option: String!) {
  updateProjectV2ItemFieldValue(input: {
    projectId: $project, itemId: $item, fieldId: $field, value: { singleSelectOptionId: $option }
  }) { projectV2Item { id } }
}"#;

#[derive(Deserialize)]
struct User {
    login: String,
}

#[derive(Deserialize)]
struct Collaborator {
    login: String,
    #[serde(default)]
    permissions: Permissions,
}

#[derive(Deserialize, Default)]
struct Permissions {
    #[serde(default)]
    push: bool,
    #[serde(default)]
    maintain: bool,
    #[serde(default)]
    admin: bool,
}

impl Permissions {
    fn can_push(&self) -> bool {
        self.push || self.maintain || self.admin
    }
}

#[derive(Deserialize)]
struct Label {
    name: String,
}

#[derive(Deserialize)]
struct RestIssue {
    title: String,
    body: Option<String>,
    user: Option<User>,
    labels: Vec<Label>,
    html_url: String,
}

#[derive(Deserialize)]
struct RestComment {
    id: u64,
    body: Option<String>,
    user: Option<User>,
    created_at: DateTime<Utc>,
}

#[derive(Deserialize)]
struct RestPull {
    number: u64,
    html_url: String,
    head: RestHead,
}

#[derive(Deserialize)]
struct RestHead {
    sha: String,
}

#[derive(Deserialize)]
struct NewComment {
    id: u64,
}

#[derive(Deserialize)]
struct NewIssue {
    number: u64,
}

impl AppClient {
    /// Reads the App's private key from its file.
    pub fn new(cfg: &GitHubConfig) -> Result<AppClient> {
        let pem = read_secret(&cfg.private_key_file)?;
        AppClient::with_key(cfg.app_id, cfg.installation_id, pem.as_bytes(), &cfg.api_url)
    }

    /// A client with the App's PEM key in hand, talking to `api_url`.
    pub fn with_key(app_id: u64, installation_id: u64, pem: &[u8], api_url: &str) -> Result<AppClient> {
        EncodingKey::from_rsa_pem(pem).context("the GitHub App key isn't an RSA private key in PEM form")?;
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/vnd.github+json"));
        headers.insert("x-github-api-version", HeaderValue::from_static("2022-11-28"));
        let http = reqwest::Client::builder()
            .user_agent("forge")
            .default_headers(headers)
            .timeout(Duration::from_secs(60))
            .build()?;
        Ok(AppClient {
            app_id,
            installation_id,
            pem: pem.to_vec(),
            api: api_url.trim_end_matches('/').to_string(),
            http,
            token: Mutex::new(None),
            projects: Mutex::new(HashMap::new()),
        })
    }

    fn at(&self, method: Method, path: &str) -> RequestBuilder {
        self.http.request(method, format!("{}/{path}", self.api))
    }

    /// The installation token, exchanged for a new one near its expiry.
    async fn token(&self) -> Result<String> {
        let mut cached = self.token.lock().await;
        if let Some(t) = cached.as_ref().filter(|t| t.expires_at - RENEW_BEFORE > Utc::now()) {
            return Ok(t.token.clone());
        }
        let jwt = app_jwt(self.app_id, &self.pem, Utc::now())?;
        let path = format!("app/installations/{}/access_tokens", self.installation_id);
        let fresh: InstallationToken = json_of(send(self.at(Method::POST, &path), &jwt).await?).await?;
        ensure!(!fresh.token.is_empty(), "GitHub sent an empty installation token");
        *cached = Some(fresh.clone());
        Ok(fresh.token)
    }

    async fn call(&self, req: RequestBuilder) -> Result<Response> {
        send(req, &self.token().await?).await
    }

    /// Sends `req` and reads GitHub's JSON answer.
    async fn read<T: DeserializeOwned>(&self, req: RequestBuilder) -> Result<T> {
        json_of(self.call(req).await?).await
    }

    /// Every entry of a paged REST list, following the Link header's next page.
    async fn read_all<T: DeserializeOwned>(&self, path: &str) -> Result<Vec<T>> {
        let mut all = Vec::new();
        let api = format!("{}/", self.api);
        let mut next = Some(self.at(Method::GET, path).query(&[("per_page", "100")]));
        while let Some(req) = next.take() {
            let resp = self.call(req).await?;
            if let Some(url) = next_link(resp.headers()) {
                ensure!(url.starts_with(&api), "GitHub's next page {url} isn't under {api}");
                next = Some(self.http.get(url));
            }
            all.extend(json_of::<Vec<T>>(resp).await?);
        }
        Ok(all)
    }

    /// Runs a GraphQL query and returns its `data`. A NOT_FOUND error leaves
    /// that node null; any other error fails the call.
    async fn graphql(&self, query: &str, variables: Value) -> Result<Value> {
        let req = self.at(Method::POST, "graphql").json(&json!({ "query": query, "variables": variables }));
        let mut answer: Value = self.read(req).await?;
        let errors = answer["errors"].as_array().map(Vec::as_slice).unwrap_or_default();
        if errors.iter().any(|e| e["type"] != "NOT_FOUND") {
            let messages: Vec<&str> = errors.iter().filter_map(|e| e["message"].as_str()).collect();
            bail!("GitHub GraphQL: {}", messages.join("; "));
        }
        Ok(answer.get_mut("data").map(Value::take).unwrap_or_default())
    }

    /// The repo's Project, looked up under its owner as an org, then as a user.
    async fn project(&self, repo: &RepoConfig) -> Result<Project> {
        let mut cache = self.projects.lock().await;
        if let Some(project) = cache.get(&project_key(repo)) {
            return Ok(project.clone());
        }
        let vars = json!({ "login": repo.project_owner, "number": repo.project_number });
        for owner in ["organization", "user"] {
            let data = self.graphql(&PROJECT_QUERY.replace("OWNER", owner), vars.clone()).await?;
            let node = &data["owner"]["projectV2"];
            if node.is_null() {
                continue;
            }
            let Some(project) = project_from(node) else {
                bail!("Project #{} of {} has no single-select Status field", repo.project_number, repo.project_owner);
            };
            cache.insert(project_key(repo), project.clone());
            return Ok(project);
        }
        bail!("{} has no Project #{} the App can see", repo.project_owner, repo.project_number)
    }

    /// Passes `result` through, forgetting the repo's Project if it failed.
    async fn forget_on_error<T>(&self, repo: &RepoConfig, result: Result<T>) -> Result<T> {
        if result.is_err() {
            self.projects.lock().await.remove(&project_key(repo));
        }
        result
    }

    async fn try_project_items(&self, repo: &RepoConfig) -> Result<Vec<ProjectItem>> {
        let project = self.project(repo).await?;
        let mut items = Vec::new();
        let mut after = Value::Null;
        loop {
            let data = self.graphql(ITEMS_QUERY, json!({ "id": project.id, "after": after })).await?;
            let page = &data["node"]["items"];
            let nodes = page["nodes"].as_array().with_context(|| {
                format!("GitHub sent no items for Project #{} of {}", repo.project_number, repo.project_owner)
            })?;
            items.extend(nodes.iter().filter_map(|n| item_from(n, &repo.name)));
            after = page["pageInfo"]["endCursor"].clone();
            if page["pageInfo"]["hasNextPage"] != true || after.is_null() {
                return Ok(items);
            }
        }
    }

    async fn try_set_status(&self, repo: &RepoConfig, item_id: &str, status: Status) -> Result<()> {
        let project = self.project(repo).await?;
        let name = status.project_name();
        let option = project.options.iter().find(|(n, _)| Status::from_project_name(n) == Some(status));
        let Some((_, option)) = option else {
            let (owner, number) = (&repo.project_owner, repo.project_number);
            bail!("the Status field of Project #{number} of {owner} has no {name:?} option");
        };
        let vars = json!({ "project": project.id, "item": item_id, "field": project.status_field, "option": option });
        let data = self.graphql(SET_STATUS_MUTATION, vars).await?;
        ensure!(
            !data["updateProjectV2ItemFieldValue"]["projectV2Item"].is_null(),
            "GitHub found no Project item {item_id} to set to {name}"
        );
        Ok(())
    }
}

/// The App's own JWT (RS256): issued a minute before `now` to allow for clock
/// drift, and expiring nine minutes after it.
pub fn app_jwt(app_id: u64, pem: &[u8], now: DateTime<Utc>) -> Result<String> {
    #[derive(Serialize)]
    struct Claims {
        iat: i64,
        exp: i64,
        iss: String,
    }
    let key = EncodingKey::from_rsa_pem(pem).context("the GitHub App key isn't an RSA private key in PEM form")?;
    let claims = Claims {
        iat: (now - TimeDelta::seconds(60)).timestamp(),
        exp: (now + TimeDelta::minutes(9)).timestamp(),
        iss: app_id.to_string(),
    };
    Ok(jsonwebtoken::encode(&Header::new(Algorithm::RS256), &claims, &key)?)
}

/// Sends `req` with `bearer` as its token. A non-2xx answer becomes a
/// `GitHubError`, with the token cut from anything GitHub echoes back.
async fn send(req: RequestBuilder, bearer: &str) -> Result<Response> {
    let (http, req) = req.bearer_auth(bearer).build_split();
    let req = req?;
    let request = format!("{} {}", req.method(), req.url().path());
    let resp = http.execute(req).await.with_context(|| format!("sending {request} to GitHub"))?;
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    let text = resp.text().await.unwrap_or_default();
    let message = serde_json::from_str::<Value>(&text)
        .ok()
        .and_then(|v| v["message"].as_str().map(str::to_string))
        .unwrap_or_else(|| text.chars().take(300).collect());
    let message = message.replace(bearer, "[token]");
    Err(GitHubError { status: status.as_u16(), request, message }.into())
}

/// Reads a 2xx answer's JSON body.
async fn json_of<T: DeserializeOwned>(resp: Response) -> Result<T> {
    let path = resp.url().path().to_string();
    resp.json().await.with_context(|| format!("reading GitHub's answer from {path}"))
}

/// The `rel="next"` URL of a Link header, if there's another page.
fn next_link(headers: &HeaderMap) -> Option<String> {
    let link = headers.get(LINK)?.to_str().ok()?;
    link.split(',').find_map(|part| {
        let (url, params) = part.split_once(';')?;
        params
            .split(';')
            .any(|p| p.trim() == r#"rel="next""#)
            .then(|| url.trim().trim_start_matches('<').trim_end_matches('>').to_string())
    })
}

fn project_key(repo: &RepoConfig) -> (String, u64) {
    (repo.project_owner.to_ascii_lowercase(), repo.project_number)
}

/// Reads a Project's id and Status options; None without a Status field.
fn project_from(node: &Value) -> Option<Project> {
    let field = &node["field"];
    let options = field["options"]
        .as_array()?
        .iter()
        .filter_map(|o| Some((o["name"].as_str()?.to_string(), o["id"].as_str()?.to_string())))
        .collect();
    Some(Project {
        id: node["id"].as_str()?.to_string(),
        status_field: field["id"].as_str()?.to_string(),
        options,
    })
}

/// A Project item as Forge sees it, if it's an open issue of `repo`.
fn item_from(node: &Value, repo: &str) -> Option<ProjectItem> {
    let issue = &node["content"];
    let ours = issue["repository"]["nameWithOwner"].as_str()?.eq_ignore_ascii_case(repo);
    if !ours || issue["state"] != "OPEN" {
        return None;
    }
    Some(ProjectItem {
        item_id: node["id"].as_str()?.to_string(),
        issue_number: issue["number"].as_u64()?,
        title: issue["title"].as_str()?.to_string(),
        status: node["fieldValueByName"]["name"].as_str().and_then(Status::from_project_name),
        labels: issue["labels"]["nodes"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|l| l["name"].as_str().map(str::to_string))
            .collect(),
        author: issue["author"]["login"].as_str().unwrap_or(GHOST).to_string(),
    })
}

fn login(user: Option<User>) -> String {
    user.map_or_else(|| GHOST.to_string(), |u| u.login)
}

#[async_trait]
impl GitHubApi for AppClient {
    async fn project_items(&self, repo: &RepoConfig) -> Result<Vec<ProjectItem>> {
        let items = self.try_project_items(repo).await;
        self.forget_on_error(repo, items).await
    }

    async fn set_status(&self, repo: &RepoConfig, item_id: &str, status: Status) -> Result<()> {
        let set = self.try_set_status(repo, item_id, status).await;
        self.forget_on_error(repo, set).await
    }

    async fn issue_thread(&self, repo: &str, number: u64) -> Result<IssueThread> {
        let issue: RestIssue = self.read(self.at(Method::GET, &format!("repos/{repo}/issues/{number}"))).await?;
        let comments: Vec<RestComment> = self.read_all(&format!("repos/{repo}/issues/{number}/comments")).await?;
        Ok(IssueThread {
            number,
            title: issue.title,
            body: issue.body.unwrap_or_default(),
            author: login(issue.user),
            labels: issue.labels.into_iter().map(|l| l.name).collect(),
            url: issue.html_url,
            comments: comments
                .into_iter()
                .map(|c| Comment {
                    id: c.id,
                    author: login(c.user),
                    body: c.body.unwrap_or_default(),
                    created_at: c.created_at,
                })
                .collect(),
        })
    }

    async fn comment(&self, repo: &str, number: u64, body: &str) -> Result<u64> {
        let req = self
            .at(Method::POST, &format!("repos/{repo}/issues/{number}/comments"))
            .json(&json!({ "body": body }));
        Ok(self.read::<NewComment>(req).await?.id)
    }

    async fn add_to_project(&self, repo: &RepoConfig, issue: u64) -> Result<String> {
        let (owner, name) = repo.name.split_once('/').context("the repo name isn't owner/repo")?;
        let vars = json!({ "owner": owner, "name": name, "number": issue });
        let data = self.graphql(ISSUE_ID_QUERY, vars).await?;
        let content = data["repository"]["issue"]["id"].as_str().with_context(|| format!("{} has no issue #{issue}", repo.name))?;
        let added = async {
            let project = self.project(repo).await?;
            self.graphql(ADD_ITEM_MUTATION, json!({ "project": project.id, "content": content })).await
        }
        .await;
        let data = self.forget_on_error(repo, added).await?;
        data["addProjectV2ItemById"]["item"]["id"].as_str().map(String::from).context("GitHub added no Project item")
    }

    async fn create_issue(&self, repo: &str, title: &str, body: &str, labels: &[String]) -> Result<u64> {
        let req = self
            .at(Method::POST, &format!("repos/{repo}/issues"))
            .json(&json!({ "title": title, "body": body, "labels": labels }));
        Ok(self.read::<NewIssue>(req).await?.number)
    }

    async fn open_or_update_pr(&self, repo: &str, head: &str, base: &str, title: &str, body: &str) -> Result<PullRequest> {
        let owner = repo.split('/').next().unwrap_or(repo);
        let head_ref = format!("{owner}:{head}");
        let pulls = self.at(Method::GET, &format!("repos/{repo}/pulls"));
        let open: Vec<RestPull> = self.read(pulls.query(&[("head", head_ref.as_str()), ("state", "open")])).await?;
        let req = match open.first() {
            Some(pr) => self
                .at(Method::PATCH, &format!("repos/{repo}/pulls/{}", pr.number))
                .json(&json!({ "title": title, "body": body })),
            None => self
                .at(Method::POST, &format!("repos/{repo}/pulls"))
                .json(&json!({ "title": title, "body": body, "head": head, "base": base })),
        };
        let pr: RestPull = self.read(req).await?;
        Ok(PullRequest { number: pr.number, url: pr.html_url, head_sha: pr.head.sha })
    }

    async fn merge_pr(&self, repo: &str, number: u64, sha: &str, title: &str) -> Result<()> {
        let req = self
            .at(Method::PUT, &format!("repos/{repo}/pulls/{number}/merge"))
            .json(&json!({ "merge_method": "squash", "sha": sha, "commit_title": title }));
        let Err(e) = self.call(req).await else {
            return Ok(());
        };
        let why = match e.downcast_ref::<GitHubError>() {
            Some(g) if g.status == 405 => format!("{repo}#{number} can't merge: {}", g.message),
            Some(g) if g.status == 409 => format!("{repo}#{number}'s head moved from {sha}: {}", g.message),
            _ => return Err(e),
        };
        Err(e.context(why))
    }

    async fn git_token(&self) -> Result<String> {
        self.token().await
    }

    async fn collaborators(&self, repo: &str) -> Result<Vec<String>> {
        let users: Vec<Collaborator> = self.read_all(&format!("repos/{repo}/collaborators")).await?;
        Ok(users.into_iter().filter(|u| u.permissions.can_push()).map(|u| u.login).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{Arc, Mutex as StdMutex};

    use axum::body::Bytes;
    use axum::extract::{Query, State};
    use axum::http::{StatusCode, Uri};
    use axum::response::{IntoResponse, Response as Reply};
    use axum::{Json, Router};
    use jsonwebtoken::{DecodingKey, Validation};

    /// One request fake GitHub saw.
    struct Seen {
        method: Method,
        path: String,
        query: HashMap<String, String>,
        headers: HeaderMap,
        body: Value,
    }

    impl Seen {
        fn auth(&self) -> &str {
            self.headers.get("authorization").and_then(|v| v.to_str().ok()).unwrap_or("")
        }
    }

    #[derive(Default)]
    struct Fake {
        seen: Vec<Seen>,
        tokens_issued: u32,
    }

    type Shared = Arc<StdMutex<Fake>>;

    type Answer = (StatusCode, Option<String>, Value);

    /// A client of a fake GitHub on 127.0.0.1. Installation 2 gets hour-long
    /// tokens, 3 gets tokens near expiry, and 13 is refused.
    async fn fake_github(installation: u64) -> (AppClient, Shared) {
        let fake = Shared::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new().fallback(handle).with_state((fake.clone(), base.clone()));
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (AppClient::with_key(1, installation, test_keys().0.as_bytes(), &base).unwrap(), fake)
    }

    async fn handle(
        State((fake, base)): State<(Shared, String)>,
        method: Method,
        uri: Uri,
        Query(query): Query<HashMap<String, String>>,
        headers: HeaderMap,
        body: Bytes,
    ) -> Reply {
        let body = serde_json::from_slice(&body).unwrap_or(Value::Null);
        let seen = Seen { method, path: uri.path().to_string(), query, headers, body };
        let mut fake = fake.lock().unwrap();
        let (status, link, answer) = answer(&mut fake, &base, &seen);
        fake.seen.push(seen);
        let mut reply = (status, Json(answer)).into_response();
        if let Some(link) = link {
            reply.headers_mut().insert(LINK, link.parse().unwrap());
        }
        reply
    }

    /// What fake GitHub says to one request.
    fn answer(fake: &mut Fake, base: &str, req: &Seen) -> Answer {
        let page_two = req.query.get("page").is_some_and(|p| p == "2");
        let page = format!("<{base}{}?per_page=100&page=2>", req.path);
        let next = Some(format!(r#"{page}; rel="next", {page}; rel="last""#));
        let head = req.query.get("head").map(String::as_str);
        match (req.method.as_str(), req.path.as_str()) {
            ("POST", "/app/installations/2/access_tokens") => token(fake, TimeDelta::hours(1)),
            ("POST", "/app/installations/3/access_tokens") => token(fake, TimeDelta::minutes(4)),
            ("POST", "/app/installations/13/access_tokens") => {
                failed(StatusCode::UNAUTHORIZED, &format!("A JSON web token could not be decoded: {}", req.auth()))
            }
            ("POST", "/graphql") => ok(graphql(req)),
            ("GET", "/repos/acme/app/issues/5") => ok(json!({
                "number": 5, "title": "Fix login", "body": "It breaks.", "user": { "login": "bob" },
                "labels": [{ "name": "ui" }], "html_url": "https://github.com/acme/app/issues/5"
            })),
            ("GET", "/repos/acme/app/issues/5/comments") if page_two => ok(json!([comment(3, "forge[bot]")])),
            ("GET", "/repos/acme/app/issues/5/comments") => {
                (StatusCode::OK, next, json!([comment(1, "bob"), comment(2, "ann")]))
            }
            ("GET", "/repos/acme/app/issues/666") => {
                failed(StatusCode::FORBIDDEN, &format!("Bad credentials: {}", req.auth()))
            }
            ("POST", "/repos/acme/app/issues/5/comments") => (StatusCode::CREATED, None, json!({ "id": 9001 })),
            ("POST", "/repos/acme/app/issues") => (StatusCode::CREATED, None, json!({ "id": 123456, "number": 77 })),
            ("GET", "/repos/acme/app/pulls") if head == Some("acme:forge/1") => ok(json!([pull(3, "aaa")])),
            ("GET", "/repos/acme/app/pulls") => ok(json!([])),
            ("PATCH", "/repos/acme/app/pulls/3") => ok(pull(3, "aaa")),
            ("POST", "/repos/acme/app/pulls") => (StatusCode::CREATED, None, pull(4, "bbb")),
            ("PUT", "/repos/acme/app/pulls/3/merge") => ok(json!({ "merged": true })),
            ("PUT", "/repos/acme/app/pulls/4/merge") => {
                failed(StatusCode::CONFLICT, "Head branch was modified. Review and try the merge again.")
            }
            ("PUT", "/repos/acme/app/pulls/6/merge") => {
                failed(StatusCode::METHOD_NOT_ALLOWED, "Pull Request is not mergeable")
            }
            ("GET", "/repos/acme/app/collaborators") if page_two => {
                ok(json!([{ "login": "carol", "permissions": { "admin": true } }, { "login": "dan" }]))
            }
            ("GET", "/repos/acme/app/collaborators") => {
                let alice = json!({ "login": "alice", "permissions": { "pull": true, "push": true } });
                let viewer = json!({ "login": "viewer", "permissions": { "pull": true, "push": false } });
                let bob = json!({ "login": "bob", "permissions": { "maintain": true } });
                (StatusCode::OK, next, json!([alice, viewer, bob]))
            }
            ("GET", "/repos/acme/leaky/collaborators") => {
                (StatusCode::OK, Some(r#"<http://elsewhere.example/x?page=2>; rel="next""#.into()), json!([]))
            }
            _ => failed(StatusCode::NOT_FOUND, "Not Found"),
        }
    }

    fn ok(body: Value) -> Answer {
        (StatusCode::OK, None, body)
    }

    fn failed(status: StatusCode, message: &str) -> Answer {
        (status, None, json!({ "message": message }))
    }

    fn token(fake: &mut Fake, life: TimeDelta) -> Answer {
        fake.tokens_issued += 1;
        let token = format!("ghs_test{}", fake.tokens_issued);
        (StatusCode::CREATED, None, json!({ "token": token, "expires_at": Utc::now() + life }))
    }

    fn comment(id: u64, author: &str) -> Value {
        json!({
            "id": id, "user": { "login": author }, "body": format!("comment {id}"),
            "created_at": format!("2026-10-1{id}T08:00:00Z")
        })
    }

    fn pull(number: u64, sha: &str) -> Value {
        let url = format!("https://github.com/acme/app/pull/{number}");
        json!({ "number": number, "html_url": url, "head": { "sha": sha } })
    }

    /// Fake GitHub's GraphQL: org "acme" and user "solo" each own a Project
    /// with two pages of items; "locked" is forbidden; anyone else has none.
    fn graphql(req: &Seen) -> Value {
        let query = req.body["query"].as_str().unwrap_or("");
        let vars = &req.body["variables"];
        if query.contains("updateProjectV2ItemFieldValue") {
            return json!({ "data": { "updateProjectV2ItemFieldValue": { "projectV2Item": { "id": vars["item"] } } } });
        }
        if query.contains("node(") {
            return json!({ "data": { "node": { "items": items_page(vars["after"].as_str()) } } });
        }
        let as_org = query.contains("organization(");
        match (as_org, vars["login"].as_str().unwrap_or("")) {
            (_, "locked") => json!({
                "data": null,
                "errors": [{ "type": "FORBIDDEN", "message": "Resource not accessible by integration" }]
            }),
            (true, "acme") => json!({ "data": { "owner": { "projectV2": project("P_acme") } } }),
            (false, "solo") => json!({ "data": { "owner": { "projectV2": project("P_solo") } } }),
            _ => json!({
                "data": { "owner": null },
                "errors": [{ "type": "NOT_FOUND", "message": "Could not resolve to an owner." }]
            }),
        }
    }

    fn project(id: &str) -> Value {
        json!({ "id": id, "field": { "id": "F_status", "options": [
            { "id": "O_ready", "name": "Ready" },
            { "id": "O_building", "name": "Building" },
            { "id": "O_main", "name": "on main" },
        ] } })
    }

    /// Page one: an open issue, a draft, a closed issue and another repo's
    /// issue. Page two: an open issue with no Status or author, and one with a
    /// Status Forge doesn't know.
    fn items_page(after: Option<&str>) -> Value {
        let issue = |number: u64, state: &str, repo: &str| {
            json!({
                "number": number, "title": format!("Issue {number}"), "state": state,
                "repository": { "nameWithOwner": repo }, "author": { "login": "bob" },
                "labels": { "nodes": [{ "name": "ui" }, { "name": "bug" }] }
            })
        };
        let item = |id: &str, status: Value, content: Value| {
            json!({ "id": id, "fieldValueByName": status, "content": content })
        };
        let ready = json!({ "name": "Ready" });
        if after.is_none() {
            return json!({
                "pageInfo": { "hasNextPage": true, "endCursor": "c1" },
                "nodes": [
                    item("I20", ready.clone(), issue(20, "OPEN", "acme/app")),
                    item("IDRAFT", ready.clone(), json!({})),
                    item("I12", ready.clone(), issue(12, "CLOSED", "acme/app")),
                    item("I13", ready, issue(13, "OPEN", "acme/other")),
                ]
            });
        }
        let mut orphan = issue(11, "OPEN", "ACME/App");
        orphan["author"] = Value::Null;
        json!({
            "pageInfo": { "hasNextPage": false, "endCursor": "c2" },
            "nodes": [
                item("I11", Value::Null, orphan),
                item("I15", json!({ "name": "Todo" }), issue(15, "OPEN", "acme/app")),
            ]
        })
    }

    fn repo(owner: &str) -> RepoConfig {
        let mut repo = crate::config::sample().repos.remove(0);
        repo.name = "acme/app".into();
        repo.project_owner = owner.into();
        repo
    }

    /// The GraphQL request bodies fake GitHub saw.
    fn graphql_seen(fake: &Shared) -> Vec<Value> {
        let fake = fake.lock().unwrap();
        fake.seen.iter().filter(|s| s.path == "/graphql").map(|s| s.body.clone()).collect()
    }

    /// The claims of a JWT the test key signed, checked with its public half.
    fn verify(jwt: &str) -> Value {
        let key = DecodingKey::from_rsa_pem(test_keys().1.as_bytes()).unwrap();
        jsonwebtoken::decode::<Value>(jwt, &key, &Validation::new(Algorithm::RS256)).unwrap().claims
    }

    #[test]
    fn app_jwt_is_rs256_from_a_minute_ago_for_nine_minutes() {
        let now = Utc::now();
        let claims = verify(&app_jwt(1, test_keys().0.as_bytes(), now).unwrap());
        assert_eq!(claims["iss"], "1");
        assert_eq!(claims["iat"], now.timestamp() - 60);
        assert_eq!(claims["exp"], now.timestamp() + 540);
        assert!(app_jwt(1, b"not a key", now).is_err());
        assert!(AppClient::with_key(1, 2, b"not a key", "http://127.0.0.1:1").is_err());
    }

    #[tokio::test]
    async fn one_token_serves_every_request_with_githubs_headers() {
        let (gh, fake) = fake_github(2).await;
        assert_eq!(gh.git_token().await.unwrap(), "ghs_test1");
        gh.comment("acme/app", 5, "hi").await.unwrap();
        gh.collaborators("acme/app").await.unwrap();
        let fake = fake.lock().unwrap();
        assert_eq!(fake.tokens_issued, 1);
        let (exchange, rest) = fake.seen.split_first().unwrap();
        assert_eq!(exchange.path, "/app/installations/2/access_tokens");
        assert_eq!(verify(exchange.auth().strip_prefix("Bearer ").unwrap())["iss"], "1");
        assert_eq!(rest.len(), 3);
        assert!(rest.iter().all(|r| r.auth() == "Bearer ghs_test1"));
        for req in &fake.seen {
            assert_eq!(req.headers["accept"], "application/vnd.github+json");
            assert_eq!(req.headers["x-github-api-version"], "2022-11-28");
            assert_eq!(req.headers["user-agent"], "forge");
        }
    }

    #[tokio::test]
    async fn a_token_near_expiry_is_renewed() {
        let (gh, fake) = fake_github(3).await;
        assert_eq!(gh.git_token().await.unwrap(), "ghs_test1");
        assert_eq!(gh.git_token().await.unwrap(), "ghs_test2");
        assert_eq!(fake.lock().unwrap().tokens_issued, 2);
    }

    #[tokio::test]
    async fn project_items_are_the_repos_open_issues_in_project_order() {
        let (gh, fake) = fake_github(2).await;
        let items = gh.project_items(&repo("acme")).await.unwrap();
        let item = |id: &str, number: u64, status: Option<Status>, author: &str| ProjectItem {
            item_id: id.into(),
            issue_number: number,
            title: format!("Issue {number}"),
            status,
            labels: vec!["ui".into(), "bug".into()],
            author: author.into(),
        };
        let want = vec![
            item("I20", 20, Some(Status::Ready), "bob"),
            item("I11", 11, None, GHOST),
            item("I15", 15, None, "bob"),
        ];
        assert_eq!(items, want);
        let queries = graphql_seen(&fake);
        assert_eq!(queries.len(), 3);
        assert_eq!(queries[1]["variables"], json!({ "id": "P_acme", "after": null }));
        assert_eq!(queries[2]["variables"], json!({ "id": "P_acme", "after": "c1" }));
        gh.project_items(&repo("acme")).await.unwrap();
        assert_eq!(graphql_seen(&fake).len(), 5, "the Project is looked up once");
    }

    #[tokio::test]
    async fn a_user_owned_project_is_found_when_the_org_lookup_misses() {
        let (gh, fake) = fake_github(2).await;
        assert_eq!(gh.project_items(&repo("solo")).await.unwrap().len(), 3);
        let queries = graphql_seen(&fake);
        assert!(queries[0]["query"].as_str().unwrap().contains("organization(login: $login)"));
        assert!(queries[1]["query"].as_str().unwrap().contains("user(login: $login)"));
        assert_eq!(queries[2]["variables"]["id"], "P_solo");
        let missing = gh.project_items(&repo("nobody")).await.unwrap_err().to_string();
        assert!(missing.contains("nobody has no Project #7"), "{missing}");
        let locked = format!("{:#}", gh.project_items(&repo("locked")).await.unwrap_err());
        assert!(locked.contains("Resource not accessible by integration"), "{locked}");
    }

    #[tokio::test]
    async fn set_status_sends_the_projects_ids() {
        let (gh, fake) = fake_github(2).await;
        let acme = repo("acme");
        gh.set_status(&acme, "I20", Status::Building).await.unwrap();
        gh.set_status(&acme, "I20", Status::OnMain).await.unwrap();
        let mutations: Vec<Value> = graphql_seen(&fake)
            .into_iter()
            .filter(|q| q["query"].as_str().unwrap().starts_with("mutation"))
            .map(|q| q["variables"].clone())
            .collect();
        let want = vec![
            json!({ "project": "P_acme", "item": "I20", "field": "F_status", "option": "O_building" }),
            json!({ "project": "P_acme", "item": "I20", "field": "F_status", "option": "O_main" }),
        ];
        assert_eq!(mutations, want);
        assert_eq!(graphql_seen(&fake).len(), 3, "the Project is looked up once");
        let err = gh.set_status(&acme, "I20", Status::Reviewed).await.unwrap_err().to_string();
        assert!(err.contains("has no \"Reviewed\" option"), "{err}");
        gh.set_status(&acme, "I20", Status::Ready).await.unwrap();
        assert_eq!(graphql_seen(&fake).len(), 5, "a failed set looks the Project up again");
    }

    #[tokio::test]
    async fn issue_thread_reads_every_comment_page_oldest_first() {
        let (gh, fake) = fake_github(2).await;
        let thread = gh.issue_thread("acme/app", 5).await.unwrap();
        assert_eq!((thread.number, thread.title.as_str(), thread.author.as_str()), (5, "Fix login", "bob"));
        assert_eq!(thread.body, "It breaks.");
        assert_eq!(thread.labels, vec!["ui"]);
        assert_eq!(thread.url, "https://github.com/acme/app/issues/5");
        let comments: Vec<(u64, &str)> = thread.comments.iter().map(|c| (c.id, c.author.as_str())).collect();
        assert_eq!(comments, vec![(1, "bob"), (2, "ann"), (3, "forge[bot]")]);
        assert_eq!(thread.comments[2].body, "comment 3");
        assert_eq!(thread.comments[0].created_at.to_rfc3339(), "2026-10-11T08:00:00+00:00");
        assert_eq!(fake.lock().unwrap().seen[2].query.get("per_page").map(String::as_str), Some("100"));
    }

    #[tokio::test]
    async fn comment_and_create_issue_send_their_fields() {
        let (gh, fake) = fake_github(2).await;
        assert_eq!(gh.comment("acme/app", 5, "Merged to main.").await.unwrap(), 9001);
        let labels = vec!["e2e".to_string()];
        assert_eq!(gh.create_issue("acme/app", "E2E failed", "```\nlog\n```", &labels).await.unwrap(), 77);
        let fake = fake.lock().unwrap();
        assert_eq!(fake.seen[1].body, json!({ "body": "Merged to main." }));
        assert_eq!(fake.seen[2].body, json!({ "title": "E2E failed", "body": "```\nlog\n```", "labels": ["e2e"] }));
    }

    #[tokio::test]
    async fn open_or_update_pr_patches_the_open_pr_or_opens_one() {
        let (gh, fake) = fake_github(2).await;
        let updated = gh.open_or_update_pr("acme/app", "forge/1", "main", "Fix", "Closes #1").await.unwrap();
        let want = PullRequest { number: 3, url: "https://github.com/acme/app/pull/3".into(), head_sha: "aaa".into() };
        assert_eq!(updated, want);
        let opened = gh.open_or_update_pr("acme/app", "forge/2", "main", "Add", "Closes #2").await.unwrap();
        assert_eq!((opened.number, opened.head_sha.as_str()), (4, "bbb"));
        let fake = fake.lock().unwrap();
        let calls: Vec<(&str, &str)> = fake.seen[1..].iter().map(|s| (s.method.as_str(), s.path.as_str())).collect();
        let pulls = "/repos/acme/app/pulls";
        assert_eq!(calls, vec![("GET", pulls), ("PATCH", "/repos/acme/app/pulls/3"), ("GET", pulls), ("POST", pulls)]);
        assert_eq!(fake.seen[1].query.get("head").map(String::as_str), Some("acme:forge/1"));
        assert_eq!(fake.seen[1].query.get("state").map(String::as_str), Some("open"));
        assert_eq!(fake.seen[2].body, json!({ "title": "Fix", "body": "Closes #1" }));
        let opening = json!({ "title": "Add", "body": "Closes #2", "head": "forge/2", "base": "main" });
        assert_eq!(fake.seen[4].body, opening);
    }

    #[tokio::test]
    async fn merge_pr_squashes_at_the_head_or_says_why_not() {
        let (gh, fake) = fake_github(2).await;
        gh.merge_pr("acme/app", 3, "aaa", "Fix login (#3)").await.unwrap();
        let sent = fake.lock().unwrap().seen[1].body.clone();
        assert_eq!(sent, json!({ "merge_method": "squash", "sha": "aaa", "commit_title": "Fix login (#3)" }));
        let moved = gh.merge_pr("acme/app", 4, "bbb", "Add").await.unwrap_err();
        assert!(moved.to_string().contains("acme/app#4's head moved from bbb"), "{moved}");
        assert_eq!(moved.downcast_ref::<GitHubError>().unwrap().status, 409);
        let stuck = gh.merge_pr("acme/app", 6, "ccc", "Stuck").await.unwrap_err().to_string();
        assert!(stuck.contains("acme/app#6 can't merge: Pull Request is not mergeable"), "{stuck}");
    }

    #[tokio::test]
    async fn collaborators_follow_every_page_but_only_on_the_api() {
        let (gh, _fake) = fake_github(2).await;
        assert_eq!(gh.collaborators("acme/app").await.unwrap(), vec!["alice", "bob", "carol"]);
        let err = gh.collaborators("acme/leaky").await.unwrap_err().to_string();
        assert!(err.contains("isn't under"), "{err}");
    }

    #[tokio::test]
    async fn errors_carry_status_and_message_but_never_a_token() {
        let (gh, fake) = fake_github(2).await;
        let err = gh.issue_thread("acme/app", 666).await.unwrap_err();
        let text = format!("{err:#} {err:?}");
        assert!(text.contains("GitHub answered 403 to GET /repos/acme/app/issues/666: Bad credentials"), "{text}");
        assert!(fake.lock().unwrap().seen[1].auth().contains("ghs_test1"), "the fake saw the token");
        assert!(!text.contains("ghs_test1"), "{text}");

        let (gh, fake) = fake_github(13).await;
        let text = format!("{:#}", gh.git_token().await.unwrap_err());
        assert!(text.contains("GitHub answered 401"), "{text}");
        assert!(fake.lock().unwrap().seen[0].auth().starts_with("Bearer eyJ"), "the fake saw the JWT");
        assert!(!text.contains("eyJ"), "{text}");
    }

    /// A throwaway RSA key pair, made once per test run with `openssl`, so no key
    /// sits in the repo: (private PEM, public PEM).
    fn test_keys() -> &'static (String, String) {
        static KEYS: std::sync::OnceLock<(String, String)> = std::sync::OnceLock::new();
        KEYS.get_or_init(|| {
            use std::io::Write as _;
            let run = |args: &[&str], input: &str| {
                let mut child = std::process::Command::new("openssl")
                    .args(args)
                    .stdin(std::process::Stdio::piped())
                    .stdout(std::process::Stdio::piped())
                    .stderr(std::process::Stdio::null())
                    .spawn()
                    .expect("the tests need openssl");
                child.stdin.take().unwrap().write_all(input.as_bytes()).unwrap();
                let out = child.wait_with_output().unwrap();
                assert!(out.status.success(), "openssl {args:?} failed");
                String::from_utf8(out.stdout).unwrap()
            };
            let private = run(&["genrsa", "2048"], "");
            let public = run(&["rsa", "-pubout"], &private);
            (private, public)
        })
    }
}
