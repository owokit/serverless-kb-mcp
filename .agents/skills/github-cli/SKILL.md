---
description: 面向 `gh` CLI 的 GitHub 通用操作技能，覆盖 issue、PR、分支、仓库元数据、workflow/run 读取与评论写入。使用于智能体需要创建、读取、更新、评论或关闭 GitHub issue/PR，管理分支，检查 GitHub Actions，或选择 `gh`、`git`、`gh api` 命令时。
name: github-cli
---


# GitHub CLI

## 何时使用

当任务涉及 GitHub issue、PR、分支、仓库信息、workflow/run 状态、评论/审核，或者需要给出具体 `gh` 命令时，使用此技能。

## 操作原则

- 本地分支、提交、拉取、推送、worktree 交给 `git`。
- 仓库元数据、issue、PR、workflow、run 交给 `gh`。
- 找不到第一方命令时，再用 `gh api`。
- 写入前先确认仓库、目标编号、当前分支和基线分支。
- 读写正文时优先使用 `--body-file` 处理长文本或模板内容。
- issue 线程评论和 PR review 评论分开处理，不要混用。
- 需要仓库模板、标签或检查清单时，先读对应模板再写内容。

## 参考文件

- [command-catalog.md](references/command-catalog.md)：按本机 `gh help` 整理的顶层命令族总目录。
- [auth-project-release-search-status.md](references/auth-project-release-search-status.md)：`auth`、`project`、`release`、`search`、`status` 的常用命令示例。
- [repo.md](references/repo.md)：仓库发现、fork、clone、默认仓库与元数据读取。
- [issues.md](references/issues.md)：issue 列表、查看、创建、编辑、评论、关闭与重开。
- [pull-requests.md](references/pull-requests.md)：PR 列表、查看、创建、编辑、评论、review 与合并。
- [branches-workflows.md](references/branches-workflows.md)：分支、push/pull、worktree、workflow/run 与日志读取。

## 输出要求

- 先给最短且安全的命令，再根据需要补充说明。
- 需要写入时，先确认目标仓库和对象编号。
- 仓库已有模板或规则时，按模板和规则执行，不要自造格式。
- 遇到不确定的子命令或 flag，先跑 `gh help` 或 `gh <command> --help`，不要凭记忆拼写。
