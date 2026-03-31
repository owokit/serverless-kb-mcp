#!/bin/sh
set -eu

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

if git rev-parse --git-dir >/dev/null 2>&1; then
  git submodule sync --recursive
  git submodule update --init --recursive
fi

if [ -d "$repo_root/serverless-kb-mcp/.git" ] || [ -f "$repo_root/serverless-kb-mcp/.git" ]; then
  git -C "$repo_root/serverless-kb-mcp" fetch origin main --prune --depth=1
  git -C "$repo_root/serverless-kb-mcp" checkout --detach origin/main
  git -C "$repo_root/serverless-kb-mcp" reset --hard origin/main
  git -C "$repo_root/serverless-kb-mcp" submodule sync --recursive
  git -C "$repo_root/serverless-kb-mcp" submodule update --init --recursive --remote --checkout --force
  git -C "$repo_root/serverless-kb-mcp" submodule foreach --recursive '
    git fetch origin main --prune --depth=1
    git checkout --detach origin/main
    git reset --hard origin/main
  '
fi
