# Putting Forge on its box

In order. Nothing here copies a secret between machines: every secret file is
written on the box, by the person who owns it.

## 1. The box (an admin)

1. An Ubuntu 24.04 server with enough cores and memory for the repo's builds,
   your SSH key, and a firewall allowing 22, 80 and 443.
2. Point Forge's domain at it (DNS only, no proxy).
3. Copy `deploy/` to the box and run `sudo FORGE_DOMAIN=<domain> bash deploy/setup-box.sh`.

## 2. GitHub (an org owner)

1. Create a GitHub App in the org, no webhook, with repository permissions
   Contents, Issues and Pull requests read and write, Metadata read, and
   organization permission Projects read and write. Install it on the repos
   Forge works only. Note the App id and installation id.
2. Generate its private key and save it on the box as
   `/etc/forge/github-app.pem` (owner forge, mode 600). Delete the download.
3. Create a Project for the repo with a single-select Status field:
   Backlog, Asked, Ready, Building, Reviewed, Accepted, On main, Live. Add the
   repo's issues to it. Note its number.
4. Protect `main`: require a pull request with **0 required approvals** (Forge
   never approves; one approval would make every merge fail), and restrict who
   can push to the App. Decide whether admins may bypass in an emergency.
5. Actions: every push of a `forge/<n>` branch can run the repo's workflows on
   a session's code. Make sure no workflow that runs on non-main branches gets
   a secret, or turn those workflows off. Set Actions' default `GITHUB_TOKEN`
   to read-only.

## 3. People-facing services

1. Telegram: make a bot with @BotFather, add it to the team chat, and save its
   token as `/etc/forge/telegram-token` (forge, 600). Put the chat id in the
   config.
2. Resend: an API key for Forge's sender address, saved as
   `/etc/forge/resend-key` (forge, 600).
3. `head -c 32 /dev/urandom | base64 > /etc/forge/auth-secret` (forge, 600).

## 4. Install

1. Claude Code as a root-owned copy at `/usr/local/bin/claude`: run the native
   installer, then `install -o root -g root -m 755 <the installed binary>
   /usr/local/bin/claude`. Never a link into a folder another user can write:
   Forge runs it too. `claude.bin` names it, since sudo resets PATH.
2. The repo's toolchain for sessions. The merge check and e2e run in a fresh
   export each time, so they install their own dependencies. Don't add
   forge-run to the docker group: that would let a session read the plan
   logins. Use rootless Docker for forge-run if builds need containers.
   With rootless Docker, set `DOCKER_HOST` to
   `unix:///run/user/<forge-run's uid>/docker.sock` under `env` in forge-run's
   `~/.claude/settings.json`, so sessions find it. Any wrapper you put in
   `/usr/local/bin` (rustup's proxies, say) must be bash, not sh: dash drops
   environment variables whose names have hyphens, such as cargo's
   `CARGO_BIN_EXE_<name>`, and tests that use them stop compiling.
3. Build Forge (`cargo build --release` in this repo) and install
   `target/release/forge` as `/usr/local/bin/forge` (root, 755).
4. Copy `prompts/*.md` to `/etc/forge/prompts/` and `deploy/example.forge.toml`
   to `/etc/forge/forge.toml` (root, mode 644: it holds no secrets, and
   `forge attach-gate` reads it as forge-run); fill every TODO.

## 5. Claude plans, then start (each plan's owner, on the box)

1. For each plan in `[pool]`: as root, `claude setup-token`, sign in as that
   plan in your own browser, then paste the token it prints into
   `install -o forge -g forge -m 600 /dev/stdin /etc/forge/plans/<label>`
   (end with Ctrl-D). Clear the terminal after.
2. `sudo -u forge forge check-config` until it says ok.
3. `systemctl enable --now forge-tmux forge forge-keys.timer forge-claude-update.timer`, then
   `systemctl start forge-keys` once so people can attach. Collaborators on the
   repo who can push, and the members, get a key; read-only viewers don't.

## 6. First run

1. Everyone: `forge login --server https://<domain> --email <you>`.
   That's all `status`, `try` and `attach` need. Attach signs in over SSH as
   forge-run with a key on your GitHub account; if that key isn't one ssh
   tries by default, add `Host <domain>` with its `IdentityFile` to
   `~/.ssh/config`.
2. The orderer moves one small issue to Ready. Watch the dashboard. Its first
   burst proves the proxy works with the plans: if Claude rejects the login
   through the proxy, stop; don't hand sessions a login instead.
3. When it reaches Reviewed: `forge try <n>`, then move it to Accepted.
4. Then a few more, still one at a time. Raise `concurrency` by hand.
