# Issue 命令

## 完整子命令

- `create`: 创建 issue
- `list`: 列出 issue
- `status`: 查看相关 issue、PR、通知
- `close`: 关闭 issue
- `comment`: 添加 issue 评论
- `delete`: 删除 issue
- `develop`: 管理 issue 关联分支
- `edit`: 编辑 issue
- `lock`: 锁定 issue 会话
- `pin`: 置顶 issue
- `reopen`: 重开 issue
- `transfer`: 转移 issue 到其他仓库
- `unlock`: 解锁 issue 会话
- `unpin`: 取消置顶 issue
- `view`: 查看 issue

## 常用用法

- 列表：`gh issue list --state open`
- 按条件筛选：`gh issue list --search "..." --label "type: task" --assignee @me`
- 查看详情：`gh issue view 123`
- 查看评论：`gh issue view 123 --comments`
- 结构化读取：`gh issue view 123 --json title,body,state,labels,comments,author,url`
- 新建：`gh issue create --title "..." --body "..." --label "type: task" --label "status: needs-triage" --assignee @me`
- 长正文或模板正文：`gh issue create --title "..." --body-file body.md`
- 编辑：`gh issue edit 123 --title "..." --body "..." --add-label "needs-triage" --remove-label "blocked"`
- 指派与去指派：`gh issue edit 123 --add-assignee user --remove-assignee user`
- 评论：`gh issue comment 123 --body "..."`
- 关闭：`gh issue close 123`
- 重开：`gh issue reopen 123`
- 管理关联分支：`gh issue develop 123`

## 提示

- issue 评论是普通线程评论，不是 PR review。
- 使用仓库模板时，先读模板再写 `--body` 或 `--body-file`。
