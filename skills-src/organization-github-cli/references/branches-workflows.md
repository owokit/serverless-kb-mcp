# 分支与 Workflow

## 分支和推送

- 拉最新元数据：`git fetch origin`
- 切到主分支：`git switch main`
- 快进更新主分支：`git pull --ff-only origin main`
- 从远端主分支新建任务分支：`git switch -c feature/my-task origin/main`
- 切换到已有分支：`git switch feature/my-task`
- 查看分支跟踪：`git branch -vv`
- 推送并建立上游：`git push -u origin feature/my-task`
- 仅快进拉取当前分支：`git pull --ff-only`
- 比较分支：`git diff main...feature/my-task`
- 创建 worktree：`git worktree add ../worktrees/feature/my-task -b feature/my-task origin/main`

## Workflow 子命令

- `disable`: 禁用 workflow
- `enable`: 启用 workflow
- `list`: 列出 workflows
- `run`: 触发 workflow_dispatch
- `view`: 查看 workflow 摘要

## Run 子命令

- `cancel`: 取消 workflow run
- `delete`: 删除 workflow run
- `download`: 下载 run 产物
- `list`: 列出最近的 workflow runs
- `rerun`: 重新运行 run
- `view`: 查看 run 摘要
- `watch`: 观察 run 直到完成

## 常用用法

- 列出 workflow：`gh workflow list`
- 查看 workflow：`gh workflow view ci.yml`
- 读取 workflow YAML：`gh workflow view ci.yml --yaml`
- 触发手动 workflow：`gh workflow run ci.yml --ref feature/my-task`
- 列出 run：`gh run list --workflow ci.yml`
- 过滤 run：`gh run list --branch feature/my-task --status failure --event pull_request`
- 查看 run：`gh run view 123456`
- 查看日志：`gh run view 123456 --log`
- 观看执行：`gh run watch 123456`
- 重新运行失败项：`gh run rerun 123456 --failed`
- 取消 run：`gh run cancel 123456`
- 下载 run 产物（artifact）：`gh run download 123456`

## 提示

- `workflow` 读定义，`run` 读执行结果。
- 本地分支和提交状态用 `git`，GitHub Actions 状态用 `gh`。
- 下载 run 产物（artifact）：`gh run download 123456`
