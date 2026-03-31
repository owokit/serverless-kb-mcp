#!/bin/sh
set -eu

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

git config core.hooksPath .githooks
printf '%s\n' "core.hooksPath=.githooks"
printf '%s\n' "Installed hooks:"
printf '%s\n' "  post-merge -> scripts/sync-submodules.sh"
printf '%s\n' "  post-checkout -> scripts/sync-submodules.sh"
