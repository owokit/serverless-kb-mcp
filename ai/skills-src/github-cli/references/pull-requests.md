# PR 命令

## 完整子命令

- `create`: 创建 PR
- `list`: 列出 PR
- `status`: 查看相关 PR、review 请求和通知
- `checkout`: 检出 PR 到本地
- `checks`: 查看单个 PR 的 CI 状态
- `close`: 关闭 PR
- `comment`: 添加 PR 评论
- `diff`: 查看 PR 差异
- `edit`: 编辑 PR
- `lock`: 锁定 PR 会话
- `merge`: 合并 PR
- `ready`: 标记 PR 为可 review
- `reopen`: 重开 PR
- `revert`: 回滚 PR
- `review`: 添加 PR review
- `unlock`: 解锁 PR 会话
- `update-branch`: 更新 PR 分支
- `view`: 查看 PR

## 常用用法

- 列表：`gh pr list --state open`
- 按条件筛选：`gh pr list --search "..." --author @me --base main`
- 查看详情：`gh pr view 456`
- 查看评论和 review：`gh pr view 456 --comments`
- 结构化读取：`gh pr view 456 --json title,body,state,headRefName,baseRefName,comments,reviews,reviewDecision,statusCheckRollup,files,url`
- 差异：`gh pr diff 456`
- 切换到本地分支：`gh pr checkout 456`
- 创建：`gh pr create --title "..." --body "..." --base main --head feature/branch`
- 从提交自动生成正文：`gh pr create --fill`
- 从首条提交生成正文：`gh pr create --fill-first`
- 草稿 PR：`gh pr create --draft ...`
- 编辑：`gh pr edit 456 --title "..." --body "..." --base main --add-label "needs review"`
- 添加或移除 review 人：`gh pr edit 456 --add-reviewer user --remove-reviewer user`
- 线程评论：`gh pr comment 456 --body "..."`
- 提交 review：`gh pr review 456 --comment --body "..."` / `--approve` / `--request-changes`
- 查看 checks：`gh pr checks 456`
- 标记就绪：`gh pr ready 456`
- 更新分支：`gh pr update-branch 456`
- 回滚 PR：`gh pr revert 456`
- 合并：`gh pr merge 456 --squash --delete-branch`
- 关闭与重开：`gh pr close 456` / `gh pr reopen 456`

## 提示

- `gh pr comment` 是普通对话评论，`gh pr review` 是代码 review。
- `gh co` 等价于 `gh pr checkout`。
- 打开 PR 前，先确认 `--base` 和 `--head` 没有写错。
- PR 正文如果要关联 issue，先放普通文本 `Closes #123` 或 `Fixes #123`。
