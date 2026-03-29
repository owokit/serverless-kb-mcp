# gh 命令目录

说明：本文件按本机 `gh help` 的顶层命令族整理。要看某一族的完整子命令，运行 `gh help <command>` 或 `gh <command> --help`。

## 核心命令

- `auth`: 认证 `gh` 和 `git`
- `browse`: 在浏览器打开仓库、issue、PR 等
- `codespace`: 管理 codespaces
- `gist`: 管理 gists
- `issue`: 管理 issues
- `org`: 管理 organizations
- `pr`: 管理 pull requests
- `project`: 管理 GitHub Projects
- `release`: 管理 releases
- `repo`: 管理 repositories

## GitHub Actions 命令

- `cache`: 管理 GitHub Actions caches
- `run`: 查看 workflow runs
- `workflow`: 查看和运行 workflows

## 别名

- `co`: `pr checkout` 的别名

## 其他命令

- `agent-task`: 处理 agent tasks（preview）
- `alias`: 创建命令快捷方式
- `api`: 发送认证后的 GitHub API 请求
- `attestation`: 处理 artifact attestations
- `completion`: 生成 shell completion scripts
- `config`: 管理 `gh` 配置
- `copilot`: 运行 GitHub Copilot CLI（preview）
- `extension`: 管理 `gh` extensions
- `gpg-key`: 管理 GPG keys
- `label`: 管理 labels
- `licenses`: 查看第三方许可证信息
- `preview`: 执行 `gh` features previews
- `ruleset`: 查看 repo rulesets
- `search`: 搜索 repos、issues、PR、代码和 commits
- `secret`: 管理 GitHub secrets
- `ssh-key`: 管理 SSH keys
- `status`: 查看相关 issues、PR、通知和仓库动态
- `variable`: 管理 GitHub Actions variables

## 使用建议

- 先用 `gh help` 确认命令族，再用 `gh <command> --help` 确认子命令。
- 需要权限相关命令时，先跑 `gh auth status`。
- `project` 命令需要 `project` scope；如果缺 scope，用 `gh auth refresh -s project` 补齐。
