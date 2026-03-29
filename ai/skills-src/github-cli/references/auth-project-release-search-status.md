# auth / project / release / search / status

说明：以下命令族适合日常 GitHub 账号、项目面板、release 发布、跨仓库搜索和工作状态查看。

## auth

### 子命令

- `login`: 登录 GitHub 账号
- `logout`: 登出 GitHub 账号
- `refresh`: 刷新认证凭据
- `setup-git`: 用 `gh` 配置 `git`
- `status`: 查看当前 host 的活跃账号和认证状态
- `switch`: 切换活跃账号
- `token`: 打印当前 host / account 使用的 token

### 常用示例

- 检查登录状态：`gh auth status`
- 登录并配置 git：`gh auth login`
- 刷新 `project` scope：`gh auth refresh -s project`
- 为当前账号配置 git credential helper：`gh auth setup-git`
- 切换账号：`gh auth switch`
- 查看 token：`gh auth token -h github.com`

## project

### 子命令

- `close`: 关闭 project
- `copy`: 复制 project
- `create`: 创建 project
- `delete`: 删除 project
- `edit`: 编辑 project
- `field-create`: 新建字段
- `field-delete`: 删除字段
- `field-list`: 列出字段
- `item-add`: 添加 issue 或 PR 到 project
- `item-archive`: 归档项目项
- `item-create`: 创建 draft issue 项
- `item-delete`: 按 ID 删除项目项
- `item-edit`: 编辑项目项
- `item-list`: 列出项目项
- `link`: 将 project 关联到仓库或 team
- `list`: 列出 owner 下的 projects
- `mark-template`: 标记为模板
- `unlink`: 解除仓库或 team 关联
- `view`: 查看 project

### 常用示例

- 创建项目：`gh project create --owner monalisa --title "Roadmap"`
- 查看项目：`gh project view 1 --owner cli --web`
- 列出字段：`gh project field-list 1 --owner cli`
- 列出项目项：`gh project item-list 1 --owner cli`
- 添加 issue 到项目：`gh project item-add 1 --owner cli --url https://github.com/OWNER/REPO/issues/123`
- 添加 PR 到项目：`gh project item-add 1 --owner cli --url https://github.com/OWNER/REPO/pull/456`

## release

### 子命令

- `create`: 创建 release
- `list`: 列出 releases
- `delete`: 删除 release
- `delete-asset`: 删除 release asset
- `download`: 下载 release assets
- `edit`: 编辑 release
- `upload`: 上传 assets 到 release
- `verify`: 验证 release 的 attestation
- `verify-asset`: 验证 asset 来源
- `view`: 查看 release

### 常用示例

- 列出 release：`gh release list`
- 查看 release：`gh release view v1.2.3`
- 创建 release：`gh release create v1.2.3 --title "v1.2.3" --notes "..." `
- 上传 asset：`gh release upload v1.2.3 dist/app.zip`
- 下载 asset：`gh release download v1.2.3`
- 编辑 release：`gh release edit v1.2.3 --notes "..." `

## search

### 子命令

- `code`: 搜索代码
- `commits`: 搜索提交
- `issues`: 搜索 issues
- `prs`: 搜索 pull requests
- `repos`: 搜索 repositories

### 常用示例

- 搜索 issue：`gh search issues "crash on startup"`
- 搜索 PR：`gh search prs "fix auth"`
- 搜索仓库：`gh search repos "topic:serverless"`
- 搜索代码：`gh search code "CreatePullRequest" --language go`
- 搜索提交：`gh search commits "fix typo"`
- PowerShell 里带排除条件时：`gh --% search issues -- "label:bug -label:duplicate"`

## status

### 常用示例

- 查看订阅仓库动态：`gh status`
- 排除仓库：`gh status -e cli/cli -e cli/go-gh`
- 限定组织：`gh status -o cli`

### 提示

- `gh status` 主要用于查看待处理事项、提及、review request 和仓库动态。
- 需要跨仓库分析时，优先配合 `gh search` 使用。
