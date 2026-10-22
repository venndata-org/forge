//! `forge mcp`: a stdio MCP server so anyone's own Claude can see what Forge
//! is doing. Tools: forge_status, forge_waiting_on_me, forge_task. It uses the
//! same saved token as the CLI.

use anyhow::{anyhow, Result};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};

use crate::cli::Client;

/// The MCP version answered when the client names none.
const PROTOCOL_VERSION: &str = "2025-06-18";

const PARSE_ERROR: i64 = -32700;
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;

/// Serves newline-delimited JSON-RPC 2.0 on stdin/stdout until stdin closes.
pub async fn serve_stdio(client: Client) -> Result<()> {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();
    while let Some(line) = lines.next_line().await? {
        if let Some(reply) = handle_line(&client, &line).await {
            stdout.write_all(format!("{reply}\n").as_bytes()).await?;
            stdout.flush().await?;
        }
    }
    Ok(())
}

/// Answers one line of input; None for blank lines and notifications.
async fn handle_line(client: &Client, line: &str) -> Option<Value> {
    if line.trim().is_empty() {
        return None;
    }
    match serde_json::from_str(line) {
        Ok(msg) => handle(client, msg).await,
        Err(e) => {
            tracing::warn!("bad JSON on stdin: {e}");
            Some(error_reply(Value::Null, PARSE_ERROR, "parse error"))
        }
    }
}

/// Answers one JSON-RPC message; None for notifications.
pub async fn handle(client: &Client, msg: Value) -> Option<Value> {
    if !msg.is_object() {
        return Some(error_reply(Value::Null, INVALID_REQUEST, "expected one JSON-RPC object"));
    }
    let id = msg.get("id").cloned()?;
    let Some(method) = msg.get("method").and_then(Value::as_str) else {
        return Some(error_reply(id, INVALID_REQUEST, "no method"));
    };
    let params = msg.get("params").cloned().unwrap_or(Value::Null);
    Some(match method {
        "initialize" => ok_reply(id, initialize(&params)),
        "ping" => ok_reply(id, json!({})),
        "tools/list" => ok_reply(id, json!({ "tools": tools() })),
        "tools/call" => {
            let name = params.get("name").and_then(Value::as_str).unwrap_or_default();
            let args = params.get("arguments").cloned().unwrap_or(Value::Null);
            match call_tool(client, name, &args).await {
                Some(Ok(text)) => ok_reply(id, tool_result(text, false)),
                Some(Err(e)) => ok_reply(id, tool_result(format!("{e:#}"), true)),
                None => error_reply(id, INVALID_PARAMS, &format!("unknown tool: {name}")),
            }
        }
        _ => error_reply(id, METHOD_NOT_FOUND, &format!("method not found: {method}")),
    })
}

fn initialize(params: &Value) -> Value {
    let version = params.get("protocolVersion").and_then(Value::as_str).unwrap_or(PROTOCOL_VERSION);
    json!({
        "protocolVersion": version,
        "capabilities": { "tools": {} },
        "serverInfo": { "name": "forge", "version": env!("CARGO_PKG_VERSION") },
    })
}

fn tools() -> Value {
    let no_args = json!({ "type": "object", "properties": {} });
    json!([
        {
            "name": "forge_status",
            "description": "What Forge is doing: each repo's tasks by stage (Building, Reviewed, Accepted, On main), the last e2e run, the Claude plans' use, and what waits on you.",
            "inputSchema": no_args,
        },
        {
            "name": "forge_waiting_on_me",
            "description": "What waits on you: questions and blocks to answer on the issue, and reviewed tasks to verify with `forge try`.",
            "inputSchema": no_args,
        },
        {
            "name": "forge_task",
            "description": "One task: its status, branch, PR, who is attached, and the commands `forge try` runs.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "repo": { "type": "string", "description": "owner/repo" },
                    "issue": { "type": "integer", "description": "The issue number" },
                },
                "required": ["repo", "issue"],
            },
        },
        {
            "name": "forge_new_task",
            "description": "Ask Forge for a task: opens an issue on the repo, Asked on its Project, saying you asked. Write one small, clear change per task: what to change and why, and how to tell it's done.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "title": { "type": "string", "description": "Short and specific, at most 200 characters" },
                    "body": { "type": "string", "description": "What to change, why, and how to tell it's done" },
                    "repo": { "type": "string", "description": "owner/repo; leave out when Forge works on one repo" },
                },
                "required": ["title"],
            },
        },
        {
            "name": "forge_move",
            "description": "Move a task on the Project: to Ready (only the repo's orderer, from Asked or Backlog) so Forge starts it; to Backlog (any member, from Asked or Ready) to park it, since Forge never picks up Backlog; or to Accepted (any member, from Reviewed, after verifying with `forge try`) so Forge merges it.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "issue": { "type": "integer", "description": "The issue number" },
                    "status": { "type": "string", "enum": ["Ready", "Backlog", "Accepted"] },
                    "repo": { "type": "string", "description": "owner/repo; leave out when Forge works on one repo" },
                },
                "required": ["issue", "status"],
            },
        },
    ])
}

/// The repo a tool names, else Forge's only repo.
async fn repo_arg(client: &Client, args: &Value) -> Result<String> {
    if let Some(repo) = args.get("repo").and_then(Value::as_str).filter(|r| !r.is_empty()) {
        return Ok(repo.to_string());
    }
    match client.status().await?.repos.as_slice() {
        [only] => Ok(only.repo.clone()),
        _ => Err(anyhow!("Forge works on several repos: name one as repo (\"owner/repo\")")),
    }
}

/// Runs a tool against the API; None for a tool Forge doesn't have.
async fn call_tool(client: &Client, name: &str, args: &Value) -> Option<Result<String>> {
    Some(match name {
        "forge_status" => client.status().await.and_then(pretty),
        "forge_waiting_on_me" => client.status().await.and_then(|s| pretty(s.waiting_on_you)),
        "forge_task" => {
            let repo = args.get("repo").and_then(Value::as_str);
            let issue = args.get("issue").and_then(|v| v.as_u64().or_else(|| v.as_str()?.parse().ok()));
            match (repo, issue) {
                (Some(repo), Some(issue)) => client.task(repo, issue).await.and_then(pretty),
                _ => Err(anyhow!("forge_task needs repo (\"owner/repo\") and issue (a number)")),
            }
        }
        "forge_new_task" => {
            let title = args.get("title").and_then(Value::as_str).unwrap_or_default();
            let body = args.get("body").and_then(Value::as_str).unwrap_or_default();
            match repo_arg(client, args).await {
                Ok(repo) => client.new_task(&repo, title, body).await.and_then(pretty),
                Err(e) => Err(e),
            }
        }
        "forge_move" => {
            let issue = args.get("issue").and_then(|v| v.as_u64().or_else(|| v.as_str()?.parse().ok()));
            let status = args.get("status").and_then(Value::as_str);
            match (issue, status, repo_arg(client, args).await) {
                (Some(issue), Some(status), Ok(repo)) => client.move_task(&repo, issue, status).await.and_then(pretty),
                (_, _, Err(e)) => Err(e),
                _ => Err(anyhow!("forge_move needs issue (a number) and status (\"Ready\" or \"Accepted\")")),
            }
        }
        _ => return None,
    })
}

fn pretty<T: Serialize>(value: T) -> Result<String> {
    Ok(serde_json::to_string_pretty(&value)?)
}

fn tool_result(text: String, is_error: bool) -> Value {
    json!({ "content": [{ "type": "text", "text": text }], "isError": is_error })
}

fn ok_reply(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn error_reply(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::tests::{fake_api, sample_status, sample_task};

    fn request(id: u64, method: &str, params: Value) -> Value {
        json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params })
    }

    async fn call(client: &Client, name: &str, args: Value) -> Value {
        let reply = handle(client, request(9, "tools/call", json!({ "name": name, "arguments": args }))).await;
        reply.unwrap()["result"].clone()
    }

    #[tokio::test]
    async fn initialize_echoes_the_version() {
        let client = Client::new("http://127.0.0.1:9", None);
        let reply = handle(&client, request(1, "initialize", json!({ "protocolVersion": "2025-03-26" }))).await.unwrap();
        assert_eq!(reply["id"], 1);
        assert_eq!(reply["result"]["protocolVersion"], "2025-03-26");
        assert_eq!(reply["result"]["serverInfo"]["name"], "forge");
        assert!(reply["result"]["capabilities"]["tools"].is_object());
        let reply = handle(&client, request(2, "initialize", json!({}))).await.unwrap();
        assert_eq!(reply["result"]["protocolVersion"], PROTOCOL_VERSION);
    }

    #[tokio::test]
    async fn notifications_ping_and_unknown_methods() {
        let client = Client::new("http://127.0.0.1:9", None);
        let note = json!({ "jsonrpc": "2.0", "method": "notifications/initialized" });
        assert_eq!(handle(&client, note).await, None);
        assert_eq!(handle(&client, request(3, "ping", Value::Null)).await.unwrap()["result"], json!({}));
        let reply = handle(&client, request(4, "resources/list", Value::Null)).await.unwrap();
        assert_eq!(reply["error"]["code"], METHOD_NOT_FOUND);
        assert_eq!(reply["id"], 4);
        let reply = handle(&client, json!([1, 2])).await.unwrap();
        assert_eq!(reply["error"]["code"], INVALID_REQUEST);
        let reply = handle_line(&client, "{not json").await.unwrap();
        assert_eq!(reply["error"]["code"], PARSE_ERROR);
        assert_eq!(reply["id"], Value::Null);
        assert_eq!(handle_line(&client, "  ").await, None);
    }

    #[tokio::test]
    async fn lists_three_tools() {
        let client = Client::new("http://127.0.0.1:9", None);
        let reply = handle(&client, request(5, "tools/list", Value::Null)).await.unwrap();
        let tools = reply["result"]["tools"].as_array().unwrap();
        let names: Vec<_> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert_eq!(names, ["forge_status", "forge_waiting_on_me", "forge_task", "forge_new_task", "forge_move"]);
        let schema = &tools[2]["inputSchema"];
        assert_eq!(schema["properties"]["repo"]["type"], "string");
        assert_eq!(schema["properties"]["issue"]["type"], "integer");
        assert_eq!(schema["required"], json!(["repo", "issue"]));
    }

    #[tokio::test]
    async fn tools_call_the_api() {
        let client = Client::new(&fake_api().await, Some("good-token".into()));
        let text = |r: &Value| r["content"][0]["text"].as_str().unwrap().to_string();

        let result = call(&client, "forge_status", json!({})).await;
        assert_eq!(result["isError"], false);
        assert_eq!(result["content"][0]["type"], "text");
        assert_eq!(text(&result), serde_json::to_string_pretty(&sample_status()).unwrap());

        let result = call(&client, "forge_waiting_on_me", json!({})).await;
        assert_eq!(text(&result), serde_json::to_string_pretty(&sample_status().waiting_on_you).unwrap());

        let result = call(&client, "forge_task", json!({ "repo": "example-team/shop", "issue": 412 })).await;
        assert_eq!(text(&result), serde_json::to_string_pretty(&sample_task()).unwrap());
    }

    #[tokio::test]
    async fn tools_ask_for_and_move_tasks_on_the_only_repo() {
        let client = Client::new(&crate::cli::tests::fake_api().await, Some("good-token".into()));
        let result = call(&client, "forge_new_task", json!({ "title": "Fix the dates", "body": "Show 21 Oct, not 10/21." })).await;
        assert_eq!(result["isError"], false, "{result}");
        assert!(result["content"][0]["text"].as_str().unwrap().contains("\"issue\": 77"));
        let result = call(&client, "forge_move", json!({ "issue": 412, "status": "Accepted" })).await;
        assert_eq!(result["isError"], false, "{result}");
        let result = call(&client, "forge_move", json!({ "issue": 412 })).await;
        assert_eq!(result["isError"], true);
    }

    #[tokio::test]
    async fn api_errors_are_tool_errors() {
        let base = fake_api().await;
        let client = Client::new(&base, Some("good-token".into()));
        let result = call(&client, "forge_task", json!({ "repo": "example-team/shop", "issue": 9 })).await;
        assert_eq!(result["isError"], true);
        assert_eq!(result["content"][0]["text"], "no such task");

        let result = call(&client, "forge_task", json!({ "repo": "example-team/shop" })).await;
        assert_eq!(result["isError"], true);

        let anon = Client::new(&base, None);
        let result = call(&anon, "forge_status", json!({})).await;
        assert_eq!((result["isError"].clone(), result["content"][0]["text"].clone()), (json!(true), json!("not signed in")));

        let reply = handle(&client, request(6, "tools/call", json!({ "name": "rm_rf" }))).await.unwrap();
        assert_eq!(reply["error"]["code"], INVALID_PARAMS);
    }
}
