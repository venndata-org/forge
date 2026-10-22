# Forge: how it fits together

Forge turns GitHub issues into merged code. People put tasks in as issues on a
repo's GitHub Project and order the Ready column; Forge runs one Claude session
per task, gets each change reviewed, waits for a person to verify it, and is
the only thing that merges to main. It is one Rust binary, `forge`, running on
one box. It is not tied to any one repo: each repo's settings live in
Forge's config (`src/config.rs`).

Fewer moving parts, less code, less output. No named agents, no org chart, no
watchers of watchers. If a feature isn't in this file, it isn't in Forge.

## The task lifecycle

Statuses on the Project (`types::Status`):

| Status    | Set by  | Meaning |
|-----------|---------|---------|
| Asked     | anyone  | Filed, not yet ordered. Forge ignores it. |
| Ready     | members | Forge may take it. The orderer orders the column. |
| Building  | Forge   | A session is working on it (or waiting on an answer). |
| Reviewed  | Forge   | A PR is open and passed the review run. A member should verify with `forge try`. |
| Accepted  | members | A member verified it. Forge may land it. |
| On main   | Forge   | Merged. Staging deploys itself on merge. |
| Live      | members | Released to production by a person. Forge never touches production. |

## The loop (`engine.rs`)

Every `poll_seconds`, for each repo:

1. **Sync.** Read the Project's items (in Project order). Create a `Task` for
   any issue not yet in SQLite; update title/author/status from the Project
   (a person may move items back, e.g. Reviewed → Ready, or forward to
   Accepted/Live). `role` = `RepoConfig::role_for(labels)`.
2. **Answers.** For each Building task with `waiting_on` set, not attached and
   not in flight: read the issue thread; if any comment newer than
   `last_seen_comment` is by someone other than Forge's bot login, clear
   `waiting_on` and owe a burst with `BurstReason::Answer`.
3. **Owed bursts.** Run each Building task's `pending` burst (not attached,
   not in flight).
4. **Start.** While (Building tasks) < `concurrency`, take the first Ready item
   in Project order: set its Project status to Building, make its worktree on
   `forge/<issue>`, run a `Start` burst.
5. **Land, one at a time per repo.** Take the first Accepted task in Project
   order that isn't in flight. It lands only if the commit Forge last pushed
   for it (kv `pushed:<repo>#<n>`) is the one a review passed (kv
   `reviewed:<repo>#<n>`); otherwise it goes back to Building, so a task a
   person accepted before its latest change was reviewed can't land unreviewed.
   Then `GitOps::land` it. Forge lands exactly the
   commit it pushed and the review run read, never what the session's clone
   holds now: it rebases that commit on fresh origin/main in its private
   landing checkout, runs `merge_check` (if set) as the session user on an
   export of the result, and pushes the result. Conflict: set Building and owe
   `RebaseConflict`. Check failed: set Building and owe `MergeCheckFailed`.
   Ready: squash-merge the PR with that sha (a 405 or 409 right after a push is
   retried up to 5 times, 2 s doubling), set On main, comment "Merged to main.",
   remove the task's clone.

A Building task that owes nothing, waits on nobody and isn't running (a person
dragged it back, or a crash) owes a burst: `Answer` if it has a session, else
`Start`. Every burst and landing reads the task fresh after claiming it.

Bursts are spawned (`tokio::spawn`), never awaited by the tick; `in_flight`
keeps a task from getting two bursts at once, and no burst of any kind starts
while the repo already has `concurrency` bursts or landings in flight.

### Running a build burst

1. Issue a proxy key (`SessionKeys::issue("<repo>#<issue>")`); revoke it after.
2. Read the role's prompt file; `session::build_prompt(...)`.
3. `Runner::burst` with `resume = task.session_id`, the role's model and effort.
4. Save the session id, count the burst, record a `BurstLog`, set
   `last_seen_comment` to the newest comment id in the thread it was shown.
5. Act on the outcome:
   - **Pr**: `GitOps::collect` the session's commits; if none, treat as Blocked ("no commits"). Else push,
     `open_or_update_pr` (title from the outcome, body = outcome body +
     "\n\nCloses #<issue>"), then run a **review burst**: a fresh session (no
     resume) with the repo's `review` role and `session::review_prompt`, in a
     fresh checkout of exactly the pushed commit (`GitOps::review_checkout`),
     never the build session's clone, removed after.
     - Review pass: Project status Reviewed; comment on the issue
       "@<author> ready to verify: `forge try <issue>`" with the review notes;
       Telegram ping. Reset `review_rounds`.
     - Review fail: `review_rounds += 1`; if under 3, owe `ReviewNotes`; else
       treat as Blocked ("review failed 3 times") with the notes.
   - **Question**: comment "@<author> <text>" on the issue; `waiting_on = author`;
     ping Telegram "<repo>#<n> needs <author>: <first line> <url>".
   - **Blocked**: same as Question, with "Blocked: <reason>".
   - **Review** from a build session, or `is_error`: Blocked with the reason.
6. Never comment the same thing twice on retry; the bot's own comments never
   count as answers.

### E2E (`e2e_tick`)

For each repo with `e2e`: if the last run started more than `every_hours` ago
(or never), `GitOps::run_on_main` runs `command` as the session user on an
export of fresh main, with
`STAGING_URL` set, timeout 60 minutes. On failure, open an issue
"E2E failed on staging, <UTC time>" with the last 200 lines in a code block,
labelled `e2e`, and ping Telegram. Record the run either way.

## Sessions (`session.rs`, `tmux.rs`)

**On the box (a `session_user` is set), build sessions run in tmux**: each task's Claude runs interactively in its own tmux session,
`<owner>-<repo>-<n>`, on the session user's server at `<data_dir>/sessions/tmux.sock`,
so people can watch or join it any time. A burst types its prompt in (starting
Claude with it as the first message if Claude isn't running, under a per-task
proxy key that lives until the task lands) and waits for Claude Code's Stop
hook: `forge turn-done` writes the session id and the turn's last message to
`<data_dir>/sessions/<name>.turn.json`. A turn without a ```forge block is
nudged once. Claude stays up between bursts. Review bursts, and every burst
without a session user, run headless as below.

A headless burst is one `claude -p` run in the task's clone, with the prompt on stdin
(one argument can't exceed 128 KB on Linux):

```
claude -p <prompt> --output-format json --model <model> --effort <effort>
       --dangerously-skip-permissions [--resume <session_id>]
```

Environment: `ANTHROPIC_BASE_URL=<proxy_url>`, `ANTHROPIC_AUTH_TOKEN=<session key>`,
`FORGE_SLOT=<slot lock path>`, `FORGE_BIN=<forge binary>`, `FORGE_ISSUE=<n>`,
plus `HOME`/`PATH`/`LANG` from Forge's own environment. Nothing else of Forge's:
no GitHub token, no plan login. With `session_user`, the command runs as
`sudo -n -H -u <user> --preserve-env=<those names> -- timeout -k 60 <secs> claude ...`,
so HOME is the session user's own (where Claude keeps the session to resume)
and the session dies on time even though Forge can't kill root's sudo. Forge's
own timeout, a little longer, is the backstop (is_error = true).

Sessions are otherwise fully open: they may use the web and run anything in
their worktree.

### What every session is told (in `build_prompt`)

- You are working on issue #N of <repo> in this git worktree, on branch
  `forge/N`. Make the change the issue asks for, with as little code as does
  the job well. Lines added are a cost.
- Commit your work on this branch with clear messages. Never push, never open
  PRs, never comment on GitHub: Forge does that.
- Run builds through the build slot: `$FORGE_BIN slot -- <build command>`.
  Only one build fits on the box at a time.
- Never put real customer data anywhere; use synthetic data only.
- End your final message with exactly one block:

  ````
  ```forge
  {"outcome": "pr", "title": "...", "body": "..."}
  ```
  ````

  or `{"outcome": "question", "text": "..."}` when you need a person, or
  `{"outcome": "blocked", "reason": "..."}` when you can't go on.

Review sessions read exactly the commit Forge pushed (`git diff origin/<base>...<sha>`)
and end with `{"outcome": "review", "pass": true|false, "notes": "..."}`.
They read the diff (`git diff origin/<base>...HEAD`), check it does what the
issue asks with no needless code, and change nothing.

`parse_outcome` takes the **last** ```forge block; anything unreadable is
`Blocked { reason: "no forge block in the session's answer" }` (or similar).

## The Claude pool and proxy (`pool.rs`, `proxy.rs`)

Each request goes to the plan with the most room; a plan at its 5-hour or
weekly cap (default 85% and 90%) gets no new requests.

The proxy forwards only what a session needs (`/v1/messages`,
`/v1/messages/count_tokens`, `/v1/models`; anything else, or any path with an
escape or a dot segment, is 404) to `upstream` +
path + query. It needs a live
session key as `Authorization: Bearer <key>` or `x-api-key: <key>`, else 401.
It picks a plan (`Pool::pick_for`, which keeps a task on the plan it last used
for up to an hour, so its prompt cache survives), replaces auth with `Authorization: Bearer
<plan token>`, drops `x-api-key`, and adds `oauth-2025-04-20` to
`anthropic-beta` (keeping any betas the client sent). It streams the response
body back unchanged (SSE included) with the upstream status and headers (minus
hop-by-hop). From each response's headers it records limits
(`limits_from_headers`) into the pool and SQLite. On a 429 it marks the plan
out until the reset header (else 5 minutes), and on a 401 from the Messages API
for an hour; either
way it retries once on another plan before any byte is sent back. No plan with room: 529 with
`{"type":"error","error":{"type":"overloaded_error","message":"every Claude plan is at its ceiling"}}`.
It never logs a token, a key or a request body.

## Paths on the box, and who may write them

Forge's daemon runs as `forge`, the only user that can read the plan logins.
Sessions, attach and every command from a repo (merge check, e2e) run as
`forge-run`. The one hard rule: nothing `forge-run` can write is ever run, or
read as git config, by `forge`.

Under `data_dir`:
- `state/`: only `forge`. `state/forge.db` is the SQLite file;
  `state/landing/<owner>/<repo>/<branch with "/" as "-">` is the private
  checkout where Forge rebases a task before merging it.
- `mirrors/<owner>/<repo>.git`: one bare mirror per repo, written only by
  `forge`, readable by sessions. It fetches GitHub into `refs/remotes/origin/*`
  and holds what Forge took from each session in `refs/forge/incoming/<branch>`
  and what it last pushed (the commit reviewed, then landed) in
  `refs/forge/pushed/<branch>`. Pushes lease against that ref (or against no
  branch at all, the first time), so Forge never overwrites a push it didn't make.
- `worktrees/<owner>/<repo>/<branch with "/" as "-">`: each task's own clone,
  e.g. `worktrees/example-team/shop/forge-412`, made by Forge from the mirror and
  then the session's alone: Forge never runs git in it again, it only fetches
  from it. The attach grant points at the same path.
- `worktrees/.runs/`: throwaway exports (`git archive`) where the merge check
  and e2e run as `forge-run`.
- `build.lock`: the build slot (group-writable, so sessions can take it).

Forge's own git never runs hooks or an fsmonitor, and its GitHub token travels
only in git's environment, never in an argument, URL or config file.

## People-facing parts

- **Auth** (`auth.rs`): 6-digit code, HMAC-SHA256(secret,
  challenge_token + ":" + code) stored; 10-minute expiry; 5 wrong tries kills
  the challenge; 5 codes per email per hour. API tokens are 32 random bytes,
  base64url, stored as HMAC(secret, "token:" + token). Non-members get a
  challenge token but no email.
- **API and dashboard** (`api.rs`, `status.rs`): routes listed in `api.rs`.
  The dashboard is one small inline HTML page (no external scripts or fonts)
  that signs in by email code and shows `StatusView`: waiting on you first,
  then each repo's Building / Reviewed / Accepted / On main, the last e2e run,
  and each plan's 5-hour and week use. Refreshes every 30 s.
- **Setup and install**: `curl -fsSL <forge>/install.sh | sh` (the script is
  `deploy/install.sh`, served by the API with the server filled in) downloads
  `<forge>/download/forge-<os>-<arch>` (from `<data_dir>/dist`; only
  forge-darwin-arm64 and forge-linux-x86_64 are served) into `~/.local/bin`,
  then runs `forge setup`: sign in (asking for the email), `claude mcp add
  --scope user forge -- <forge> mcp` unless it's there, the skill
  `skills/forge/SKILL.md` written to `~/.claude/skills/forge/`, and an SSH check
  (the gate answers an unknown command with exit 2; a refused key is ssh's 255).
- **CLI** (`cli.rs`): `forge setup`, `login`, `status`, `new`, `ready`, `accept`,
  `try`, `stop`, `attach`, `watch`. `new` asks for a task and `ready`/`accept`
  move one, through the API (`POST /api/tasks/:o/:r` and `/:n/status`: Ready by
  the repo's orderer from Asked or no status, Accepted by any member from
  Reviewed; the App makes the change and the loop picks it up). `stop`
  ends the running `forge try` for that task (its process group, recorded in
  `<try folder>.pid`; SIGINT, then SIGTERM after 15 s) and runs the repo's
  `stop` command in the try folder. `try` uses the
  person's own GitHub access and, by default, Forge's own bare clone at
  `~/.cache/forge/repos/<owner>/<repo>.git` (made on first use over SSH):
  `git fetch origin forge/N`, a worktree at `~/.cache/forge/try/<repo>-N`, then
  the repo's `build`, `seed`, `run` there. `--dir <clone>` uses your own clone
  instead (worktree at `<clone>/../<repo>-try-N`).
- **MCP** (`mcp.rs`): MCP over stdio, protocol version "2025-06-18" (echo the
  client's if it sends one), tools `forge_status`, `forge_waiting_on_me`,
  `forge_task {repo, issue}`, `forge_new_task {title, body?, repo?}` and
  `forge_move {issue, status: Ready|Accepted, repo?}` (repo defaults to Forge's
  only repo), each returning text content (JSON pretty-printed).
- **Watch and attach** (`attach.rs`): `forge watch N` and `forge attach N` are
  `ssh -t <host> watch|attach <repo> N`; the gate runs `tmux attach -r` (watch,
  read-only) or `tmux attach` (join: what you type goes to the same Claude,
  mid-turn included) on the task's tmux session. `forge keys` gives a key to
  every member and every collaborator who can push ("anyone on the repo";
  read-only viewers don't count; members are added by name, since GitHub
  doesn't list org owners as the App's collaborators); being in that list is
  the permission. The internal API's older attach grant (`claude --resume`
  between bursts, 8-hour limit, `engine::Busy`) is no longer used by the gate.
- **Pings** (`notify.rs`): Telegram `sendMessage` to `chat_id`; email via
  `POST https://api.resend.com/emails`. A missing config logs and skips.

## Rules for everyone writing Forge

- Read `src/types.rs` and `src/config.rs` first. They are frozen: don't change
  them. If you truly need a change there, make it in your copy and say exactly
  what and why in your report.
- Don't change another module's public signatures. Add what you need inside
  your own module.
- Match the house style: doc comments that say what a thing is for, in plain
  words; no comment noise; small functions; `anyhow::Result` at the edges.
- Every module has unit tests in a `#[cfg(test)] mod tests`. No test touches
  the network beyond 127.0.0.1, GitHub, Anthropic, Telegram or Resend; use a
  local axum server, temp dirs, file:// git remotes and fake binaries (a shell
  script in a temp dir) instead.
- Never print or log a secret. Never read real credentials on this machine.
- Nothing about one team goes in this repo: no organisation, repo, person,
  plan, domain or box of anyone who runs Forge. Those live in a Forge's own
  config, and in `skill_notes` and `notes` there. Tests and examples use
  example-team/shop, Ann and Bob, and the plans blue, red and tan. The
  `No team specifics` workflow checks every push against a term list kept as
  the `FORBIDDEN_TERMS` Actions secret, and prints only file names.
- `cargo build`, `cargo test` and `cargo clippy --all-targets -- -D warnings`
  pass before you report.
