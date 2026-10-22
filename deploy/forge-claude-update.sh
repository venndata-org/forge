#!/bin/sh
# Keeps the root-owned Claude Code that Forge's sessions run current: updates
# root's own install, then swaps /usr/local/bin/claude for it when it changed.
# Sessions stay root-proof (forge-run can't write the binary) and move to the
# new version at their next burst (tmux.rs restarts an outdated one).
set -eu
export HOME=/root
/root/.local/bin/claude update >/dev/null 2>&1 || true
new=$(readlink -f /root/.local/bin/claude)
if ! cmp -s "$new" /usr/local/bin/claude; then
    install -o root -g root -m 755 "$new" /usr/local/bin/claude.new
    mv -f /usr/local/bin/claude.new /usr/local/bin/claude
    echo "Claude Code is now $(/usr/local/bin/claude --version)"
fi
