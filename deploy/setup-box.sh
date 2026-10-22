#!/usr/bin/env bash
# Prepares a fresh Ubuntu 24.04 box for Forge. Run as root, once; safe to re-run.
# It makes the users and folders, the sudo rule and sshd block for attach, the
# Caddy site, and installs the services. It never creates or copies a secret:
# the secret files under /etc/forge are written by people, on this box (RUNBOOK.md).
set -euo pipefail

DOMAIN="${FORGE_DOMAIN:?set FORGE_DOMAIN to the domain of this box, e.g. forge.example.com}"

apt-get update -q
apt-get install -yq git curl ca-certificates sudo caddy tmux libssl-dev pkg-config

# forge runs the daemon and alone reads /etc/forge. forge-run runs Claude sessions,
# checks, e2e and attach; through the forge-work group it reads Forge's mirrors and
# writes the clones Forge makes for it.
getent group forge-work >/dev/null || groupadd --system forge-work
id forge >/dev/null 2>&1 || useradd --system --home-dir /var/lib/forge --shell /usr/sbin/nologin --gid forge-work forge
id forge-run >/dev/null 2>&1 || useradd --create-home --shell /bin/bash --gid forge-work forge-run
getent group forge >/dev/null || groupadd --system forge
usermod -a -G forge forge

install -d -o forge -g forge-work -m 2750 /var/lib/forge
# Only forge writes these folders. forge-run writes only inside each task's clone and
# each check's export, which Forge opens to the group once made.
install -d -o forge -g forge-work -m 2750 /var/lib/forge/worktrees /var/lib/forge/mirrors
# Forge's database and landing checkouts: no session can open them.
install -d -o forge -g forge -m 700 /var/lib/forge/state
install -o forge -g forge-work -m 660 /dev/null /var/lib/forge/build.lock
# Sessions work in clones forge owns and fetch from its mirrors, so forge-run's git
# must trust repos forge owns. Only forge-run's: root and forge keep git's ownership
# check, so they never read config a session wrote.
usermod -c "Forge sessions,umask=0007" forge-run
sudo -u forge-run -H git config --global --replace-all safe.directory '*'
# 751: forge-run may open the (secret-free) config by name but can't list the folder.
install -d -o root -g forge -m 751 /etc/forge
install -d -o forge -g forge -m 700 /etc/forge/plans
install -d -o root -g root -m 755 /etc/forge/ssh /etc/forge/prompts

# forge runs sessions, checks, e2e and its fetches from clones as forge-run. forge
# outranks forge-run, so letting it run anything as forge-run grants nothing new.
# Each command runs under coreutils' timeout (/usr/bin/timeout): forge can't signal
# another user's processes, so the command ends itself at its limit.
# !log_allowed: sudo would otherwise log each session's environment, proxy key included.
# !pam_session: no PAM session (and its log lines) for each of Forge's many sudo calls.
cat >/etc/sudoers.d/forge <<'EOF'
Defaults:forge umask=0007, umask_override, !log_allowed, !pam_session
forge ALL=(forge-run) NOPASSWD:SETENV: ALL
EOF
chmod 440 /etc/sudoers.d/forge
visudo -cf /etc/sudoers.d/forge

# Attach: forge-run logs in only with the key of a collaborator who can push
# (forge-keys writes the list),
# forced to attach-gate. The list is root's: no session can add a key to it.
cat >/etc/ssh/sshd_config.d/forge.conf <<'EOF'
Match User forge-run
    AuthorizedKeysFile /etc/forge/ssh/forge-run.keys
    PasswordAuthentication no
    AllowTcpForwarding no
    AllowAgentForwarding no
    X11Forwarding no
    PermitTTY yes
EOF
touch /etc/forge/ssh/forge-run.keys && chmod 644 /etc/forge/ssh/forge-run.keys
sshd -t && systemctl reload ssh

cat >/etc/caddy/Caddyfile <<EOF
${DOMAIN} {
    reverse_proxy 127.0.0.1:8470
}
EOF
systemctl reload caddy || systemctl restart caddy

# Claude Code for sessions: a root-owned copy in a fixed path the config names.
# Never a link into a folder another user can write: forge runs it too.
if [ ! -x /usr/local/bin/claude ]; then
    echo "Install Claude Code as a root-owned /usr/local/bin/claude (RUNBOOK.md, step 5), then re-run." >&2
fi

install -m 644 "$(dirname "$0")/forge.service" /etc/systemd/system/forge.service
install -m 644 "$(dirname "$0")/forge-tmux.service" /etc/systemd/system/forge-tmux.service
# needrestart (apt, unattended upgrades) must never restart the tmux server:
# that would end every task's Claude session.
mkdir -p /etc/needrestart/conf.d
echo '$nrconf{override_rc}{qr(^forge-tmux\.service$)} = 0;' > /etc/needrestart/conf.d/forge.conf
install -m 644 "$(dirname "$0")/forge-keys.service" /etc/systemd/system/forge-keys.service
install -m 644 "$(dirname "$0")/forge-keys.timer" /etc/systemd/system/forge-keys.timer
# Hourly, root updates that copy; sessions don't update themselves.
install -m 755 "$(dirname "$0")/forge-claude-update.sh" /usr/local/sbin/forge-claude-update
install -m 644 "$(dirname "$0")/forge-claude-update.service" /etc/systemd/system/forge-claude-update.service
install -m 644 "$(dirname "$0")/forge-claude-update.timer" /etc/systemd/system/forge-claude-update.timer
systemctl daemon-reload
echo "Box ready. Next: secrets, config and 'forge check-config' (RUNBOOK.md)."

# Swap, so a big compile beside three sessions slows the box rather than
# getting killed (a big Rust workspace can take about 8 GB to compile).
if ! swapon --show | grep -q /swapfile; then
  fallocate -l 16G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
  grep -q '^/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

# Sessions run interactive Claude in tmux (tmux.rs): skip its first-run screens.
sudo -u forge-run -H python3 -I - <<'PY'
import json, os
p = os.path.expanduser("~/.claude.json")
d = json.load(open(p)) if os.path.exists(p) else {}
d.update(hasCompletedOnboarding=True, theme="dark")
# Trust the worktrees folder. Newer Claude Code no longer extends that to the
# folders under it, so Forge also trusts each task's folder (`forge trust`).
d.setdefault("projects", {}).setdefault("/var/lib/forge/worktrees", {})["hasTrustDialogAccepted"] = True
s = os.path.expanduser("~/.claude/settings.json")
os.makedirs(os.path.dirname(s), exist_ok=True)
settings = json.load(open(s)) if os.path.exists(s) else {}
settings["skipDangerousModePermissionPrompt"] = True
for path, data in ((p, d), (s, settings)):
    fd = os.open(path + ".new", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(path + ".new", path)
PY
