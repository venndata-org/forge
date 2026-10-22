# Forge

Forge turns GitHub issues into merged code. You ask for a change; the repo's orderer orders the queue; a Claude session builds it on Forge's box; a second session reviews it; you try it on your machine and accept it; Forge merges it. You can watch any session live, or join it.

Dashboard: your Forge's address, e.g. https://forge.example.com

## Set up (once, about a minute)

```sh
curl -fsSL https://forge.example.com/install.sh | sh
```

This installs the `forge` command in `~/.local/bin`, then runs `forge setup`, which:

1. signs you in with a 6-digit code sent to your email;
2. adds Forge's tools to Claude Code, so your own Claude can ask for tasks, check status and accept work;
3. installs a Forge skill, so your Claude knows how the flow works;
4. checks that SSH reaches Forge, for `forge watch` and `forge attach`. These use an SSH key on your GitHub account.

Run `forge setup` again any time; it only does what's missing.

## How a task moves

| Status | Who moves it | How |
|---|---|---|
| **Backlog** | Anyone | `forge backlog <n>` parks a task. Forge never picks it up until it moves to Ready |
| **Asked** | Anyone | `forge new "Title" --body "what and why"`, ask your Claude, or open an issue on the repo |
| **Ready** | The orderer | `forge ready <n>`, or the repo's Project board |
| **Building** | Forge | A Claude session works on it. Questions come as comments on the issue; answer there |
| **Reviewed** | Forge | PR open and a separate review passed. Now it's waiting on you |
| **Accepted** | Any member | `forge try <n>` to run it locally, then `forge accept <n>` |
| **On main** | Forge | Rebased, migration check run, merged. Only Forge merges to main |
| **Live** | Forge | main deploys to staging by itself |

Production releases are separate: people run them, never Forge.

## Commands

| Command | What it does |
|---|---|
| `forge status` | What Forge is doing, and what waits on you |
| `forge new "<title>" --body "<text>"` | Ask for a task (`--body -` reads stdin) |
| `forge ready <n>` | Move a task to Ready (the orderer) |
| `forge backlog <n>` | Park a task for later |
| `forge try <n>` | Run the task's branch on your machine with the repo's `run` command, from any folder |
| `forge stop <n>` | Stop what `forge try` started |
| `forge accept <n>` | Accept a Reviewed task you verified; Forge merges it |
| `forge watch <n>` | Watch the live session, read-only |
| `forge attach <n>` | Join the live session and type to it. Ctrl-b then d leaves without stopping it |
| `forge setup` | Sign in, add Forge to Claude Code, check SSH |

Your Claude has the same through MCP: `forge_status`, `forge_waiting_on_me`, `forge_task`, `forge_new_task` and `forge_move`.

## Writing a good task

- One change per task. Small and clear beats big and vague.
- The title says what changes. The body says why, and how you'll know it's done.
- Lines added are a cost: ask for the smallest change that does the job.
- Synthetic data only. Never paste real customer data into an issue or a comment.

## For people working on Forge itself

`BUILD.md` is the design and the contract each part keeps. `deploy/RUNBOOK.md` puts Forge on a box. Build with `cargo build --release`; run the tests with `cargo test`.
