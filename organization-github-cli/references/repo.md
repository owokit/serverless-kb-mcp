# 仓库命令

## 完整子命令

- `create`: 创建新仓库
- `list`: 列出仓库
- `archive`: 归档仓库
- `autolink`: 管理自动链接引用
- `clone`: 克隆仓库到本地
- `delete`: 删除仓库
- `deploy-key`: 管理仓库 deploy key
- `edit`: 编辑仓库设置
- `fork`: 创建 fork
- `gitignore`: 查看可用的 `.gitignore` 模板
- `license`: 查看可用的许可证模板
- `rename`: 重命名仓库
- `set-default`: 为当前目录配置默认仓库
- `sync`: 同步 fork
- `unarchive`: 取消归档仓库
- `view`: 查看仓库

## 常用场景

- 查看仓库元数据：`gh repo view owner/repo --json nameWithOwner,defaultBranchRef,url,description,visibility,sshUrl,updatedAt`
- 克隆仓库：`gh repo clone owner/repo`
- Fork 仓库：`gh repo fork owner/repo --clone=false`
- 设置默认仓库：`gh repo set-default owner/repo`
- 同步 fork：`gh repo sync`
- 列出用户或组织下的仓库：`gh repo list owner --limit 20`
- 当 `owner/repo` 不明确时，先用 `gh repo view` 再做 issue、PR 或 workflow 操作。
