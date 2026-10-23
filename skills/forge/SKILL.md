---
name: forge
description: Work with Forge, the service that turns GitHub issues into merged code with Claude sessions. Use when the user wants to ask for a change to a repo Forge works, check what Forge is doing or what waits on them, order or accept a task, try a task's branch locally, or watch or join a running session.
---

# Forge

Forge works a repo's issues through to main (`forge status` names its server and repos). GitHub is the only task store: an issue per task, with a Status column on the repo's Project.

## The path of a task

0. **Backlog**: parked for later. Forge never picks it up until it moves to Ready.
1. **Asked**: someone asks for it.
2. **Ready**: the repo's orderer orders the queue. Only they move tasks to Ready.
3. **Building**: Forge runs a Claude session for it on its box. A question from the session arrives as a comment on the issue.
4. **Reviewed**: Forge opened a PR and a separate review passed. A person verifies it now.
5. **Accepted**: a member verified it. Forge rebases, runs the migration check, and merges.
6. **On main**, then **Live**: main deploys to staging by itself. Production releases are done by people, never Forge.

Nobody pushes to main; only Forge merges.

## What you can do for the user

With the Forge MCP tools:
- `forge_status`: what Forge is doing, by stage.
- `forge_waiting_on_me`: questions to answer and tasks to verify.
- `forge_task`: one task's status, branch and PR.
- `forge_new_task`: ask for a task. Write one small, clear change per task. The title says what changes; the body says why, and how to tell it's done. Lines added are a cost, so ask for the smallest change that does the job.
- `forge_move`: `Ready` (only the orderer, from Asked or Backlog), `Backlog` (any member, to park a task) or `Accepted` (any member, from Reviewed, after verifying).

With the `forge` CLI, which the user runs in their terminal:
- `forge try <n>`: runs the task's branch on this machine with the repo's `run` command, from any folder. `forge stop <n>` stops it.
- `forge pair <n> <folder>...`: lets the task's session ask a Claude on this machine to work with the files in those folders (the user's real reports, say), until Ctrl-C. That Claude runs sandboxed to those folders and the task's code; the files stay here, and only its answers go back.
- `forge watch <n>`: the live session, read-only. `forge attach <n>`: join it and type. Ctrl-b then d leaves either one without stopping it.
- `forge new "<title>" --body "<text>"`, `forge ready <n>`, `forge backlog <n>`, `forge accept <n>`, `forge status`.

To answer a session's question or send feedback on a task, comment on its GitHub issue as the user (for example with `gh issue comment <n> -R <owner>/<repo>`). The session reads new comments on its next turn.

## Rules

- Accept only what was verified: run `forge try <n>` (or have the user do it) before `forge_move` to Accepted.
- Synthetic data only. Never put real customer data in an issue, a comment or a local stack.
- Forge never gets production credentials. Production stays with people.
