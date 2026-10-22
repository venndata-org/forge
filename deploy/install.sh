#!/bin/sh
# Forge's installer, served at <forge>/install.sh: puts the forge CLI in
# ~/.local/bin (or $FORGE_INSTALL_DIR), then runs `forge setup`.
set -eu
SERVER="__SERVER__"
os=$(uname -s | tr '[:upper:]' '[:lower:]')
arch=$(uname -m)
case "$os-$arch" in
  darwin-arm64) file=forge-darwin-arm64 ;;
  linux-x86_64 | linux-amd64) file=forge-linux-x86_64 ;;
  *) echo "Forge has no build for $os on $arch yet." >&2; exit 1 ;;
esac
dir="${FORGE_INSTALL_DIR:-$HOME/.local/bin}"
mkdir -p "$dir"
tmp="$dir/.forge.download"
curl -fsSL "$SERVER/download/$file" -o "$tmp"
chmod 755 "$tmp"
mv "$tmp" "$dir/forge"
echo "Installed $dir/forge"
case ":$PATH:" in
  *":$dir:"*) ;;
  *) echo "Add it to your PATH, e.g. in ~/.zshrc:  export PATH=\"$dir:\$PATH\"" ;;
esac
exec "$dir/forge" setup --server "$SERVER" </dev/tty
