# Git Hooks

This repository ships a local hook setup for keeping the `serverless-kb-mcp`
submodule refreshed after checkout/merge operations.

## What it does

- `post-merge`
- `post-checkout`

Both hooks call `scripts/sync-submodules.sh`, which:

- synchronizes submodule metadata
- initializes submodules recursively
- refreshes `serverless-kb-mcp` to `origin/main`
- refreshes nested submodules recursively

## Important note

Git does not provide a real `pre-pull` hook.

For `git pull`, the closest reliable automation is `post-merge`.
If you pull with rebase, `post-merge` will not run, so use the sync script
manually or avoid `--rebase` for this workflow.

## One-time setup on Windows

Run:

```powershell
.\scripts\install-git-hooks.ps1
```

This sets:

```text
core.hooksPath=.githooks
```

## Manual sync

You can also run:

```bash
bash scripts/sync-submodules.sh
```
