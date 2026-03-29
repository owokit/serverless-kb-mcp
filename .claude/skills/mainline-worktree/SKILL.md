---
description: 适用于新建 worktree、任务分支或本地任务目录时使用：默认从 `origin/main` 派生，但在创建 worktree 之前必须先把本地 `main` 更新到最新 `origin/main`，并处理分支/路径冲突。
name: mainline-worktree
---


# Mainline Worktree

## 目标

默认以 `origin/main` 作为基线直接派生任务分支和 worktree。但在任何 worktree 创建动作之前，必须先把本地 `main` 切到最新并与 `origin/main` 对齐，确保当前基线是最新的。除非用户明确要求维护本地 `main`，否则不要把 `switch main` + `pull` 之外的步骤作为默认流程。

## 执行顺序

1. 先同步本地 `main`
   - 先切换到本地 `main`
   - 执行 `git pull --ff-only origin main`，把本地 `main` 更新到最新
   - 如果 `main` 无法快进，先停下并说明原因，不要绕过这个步骤
2. 再确认基线
   - 默认基线是 `origin/main`
   - 用户明确指定其他分支时，改用 `origin/<branch>`
   - 确认本地 `main` 已经和 `origin/main` 对齐后，再创建 worktree
3. 再创建 worktree
   - 优先使用 `git worktree add <path> -b <branch> origin/main`
   - 如果使用其他基线，把最后一个参数替换成对应的 `origin/<branch>`
   - 不要为了创建 worktree 再去切换本地 `main`
4. 创建后回显
   - 回显基线、分支名、worktree 路径
   - 简要说明命名为什么能表达任务内容

## 命名公式

- 任务类型前缀必须有语义，不使用 `codex`、`worktrees` 这类空壳
- 前缀从这组里选：`bugfix`、`feature`、`docs`、`infra`、`refactor`、`hotfix`、`chore`、`spike`
- 分支名：`<prefix>/<slug>`
- worktree 路径：`<prefix>/<domain>/<slug>`
- `slug` 统一写成：`<domain>[-<issue>]-<action>-<object>[-<constraint>]`
- 有 issue 时优先带上编号
- 同一任务的分支和路径必须共享同一个 `slug`

## 示例

- `bugfix/search-ocr-dedup-fix`
- `docs/skill-mainline-worktree-standardize`
- `infra/issue123-worktree-mainline-bootstrap`

## 冲突处理

- 分支已存在：停止，不要自动复用
- worktree 路径已存在且已经是 worktree：停止，直接回显现有 worktree
- worktree 路径已存在但不是 worktree：停止，提示路径冲突
- 当前工作区有未提交修改：停止，不要 `reset`、不要 `stash`、不要删除文件
- 如果目标分支已经承载较大的既有改动，不要为了补一个小修复而 `switch -C`、`reset` 或重建分支；先保留现有提交历史，再用追加提交或 `cherry-pick` 恢复遗漏内容
- 对已有 PR 分支做任何可能触发强制推送的动作前，必须先确认远端 `main` 和 PR head 的真实状态，避免把已有大改动误覆盖成单一补丁

## 禁止项

- 不要默认从当前分支创建
- 不要在本地 `main` 落后的情况下直接创建 worktree
- 不要把 `switch main` / `pull` 作为可选步骤
- 不要自动重命名用户已经指定的分支或路径
- 不要为了“帮忙整理现场”做破坏性操作
