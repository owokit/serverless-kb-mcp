# 开源仓库 CI 策略

本文记录仓库当前公开 CI、门禁链路和辅助 workflow 的职责边界。仓库不再保留相关审查自动化。

## 设计原则

- 默认 PR 门禁只使用 GitHub 官方 hosted runner。
- 所有 Node.js 和 Python 运行时版本都通过 `.github/config/ci-runtime.json` 统一管理。
- 运行时安装必须通过 `.github/actions/setup-runtime/action.yml`，workflow 不直接写死 `actions/setup-node` 或 `actions/setup-python` 版本。
- 需要云侧行为时，优先采用固定输入、fixture、local emulator 和本地仿真。

## 默认 PR 门禁链

当前默认门禁链如下：

`Workflow Sanity -> Guardrails -> Logic CI -> Contract CI -> Local Integration CI`

其中：

- `Workflow Sanity` 负责 workflow 语法、tabs、actionlint 和清单一致性。
- `Guardrails` 负责 secret shape 和中文乱码扫描。
- `Logic CI` 负责代码逻辑层校验。
- 当前默认门禁里还没有独立的 `mypy` / `pyright` lane；相关 typing gaps 继续按 issue 241 跟踪，避免把“尚未接入的类型检查”误写成已落地门禁。
- `Contract CI` 负责 provider、序列化和存储契约。
- `Local Integration CI` 负责本地编排集成验证。
- `Package Release` 只在上游成功后发布产物。

## Workflow 清单

### `workflow-sanity.yml`

- 展示名：`Workflow Sanity`
- 触发：`pull_request`、`push`、`workflow_dispatch`
- 职责：校验 workflow 命名、tabs、actionlint 和 inventory 一致性

### `guardrails.yml`

- 展示名：`Guardrails`
- 触发：`workflow_run`、`workflow_dispatch`
- 职责：扫描疑似私用区字符、简体中文乱码和 secret 形状

### `ci-failure-comment-relay.yml`

- 展示名：`CI Failure Comment Relay`
- 触发：`workflow_run`
- 职责：把公开 CI 失败回评统一回写到关联 PR
- 实现脚本：`tools/ci/comment_pr_failure.py`

### `pr-path-conflict-guard.yml`

- 展示名：`PR Path Conflict Guard`
- 触发：`pull_request_target`、`workflow_dispatch`
- 职责：扫描并行 PR 的路径漂移和删除 / 重命名冲突

### `issue-hierarchy-guard.yml`

- 展示名：`Issue Hierarchy Guard`
- 触发：`issues`、`workflow_dispatch`
- 职责：校验主 issue 与子 issue 的关闭层级，并在最后一个子 issue 关闭后自动关闭父 issue

### `issue-similarity-triage.yml`

- 展示名：`Issue Similarity Triage`
- 触发：`issues`、`workflow_dispatch`
- 职责：当新 issue 与现有 issue 高度相似时，自动添加 `status: similar-issue` 标签并记录最相近的历史 issue

### `issue-similarity-closure.yml`

- 展示名：`Issue Similarity Closure`
- 触发：`schedule`、`workflow_dispatch`
- 职责：定时关闭带有 `status: similar-issue` 标签且已超过 3 天未处理的 issue

### `logic-ci.yml`

- 展示名：`Logic CI`
- 触发：`workflow_run`、`workflow_dispatch`
- 职责：运行逻辑层测试、类型检查、lint 和构建检查

### `contract-ci.yml`

- 展示名：`Contract CI`
- 触发：`workflow_run`、`workflow_dispatch`
- 职责：校验 provider 契约、序列化格式和存储契约

### `local-integration-ci.yml`

- 展示名：`Local Integration CI`
- 触发：`pull_request`、`workflow_run`、`workflow_dispatch`
- 职责：在本地仿真环境里串联整条运行链路

### `codeql.yml`

- 展示名：`CodeQL JavaScript / TypeScript / Python`
- 触发：`pull_request`、`push`、`schedule`、`workflow_dispatch`
- 职责：静态安全分析

### `stale-issues.yml`

- 展示名：`Stale Issues`
- 触发：`schedule`、`workflow_dispatch`
- 职责：清理长期无活动的 issue / PR

### `merged-branch-cleanup.yml`

- 展示名：`Branch Lifecycle Cleanup`
- 触发：`pull_request_target`、`schedule`、`workflow_dispatch`
- 职责：收敛 PR 分支生命周期标签（`branch:protected` / `branch:deletable`），并在保留期结束后删除符合条件的分支
- 标签：`branch:protected` 表示打开中的 PR，`branch:deletable` 表示已关闭且可删除的 PR
- 关闭语义：PR 一旦状态变为关闭，无论是 merged 还是 unmerged，先移除 `branch:protected`，再补上 `branch:deletable`；merged 分支在仓库保护检查通过后可立即被删除，未 merged 分支则继续等待保留期结束；closed 事件和定时扫描都会回扫历史 closed PR，补齐以前残留的 `branch:protected`
### `dependabot-auto-merge.yml`

- 展示名：`Dependabot Auto Merge`
- 触发：`pull_request`
- 职责：自动处理 Dependabot 更新

### `external-validation.yml`

- 展示名：`External Validation`
- 触发：`workflow_dispatch`
- 职责：手动执行外部验证或网络相关检查

### `docs-ci.yml`

- 展示名：`Docs CI`
- 触发：`pull_request`、`workflow_dispatch`
- 职责：校验文档和 workflow 名称一致性

### `security-ci.yml`

- 展示名：`Security CI`
- 触发：`workflow_run`、`workflow_dispatch`
- 职责：额外的安全审计门禁

### `package-release.yml`

- 展示名：`Package Release`
- 触发：`workflow_run`、`workflow_dispatch`
- 职责：在 `Local Integration CI` 成功后发布包

### `prod-deploy.yml`

- 展示名：`Prod Deploy`
- 触发：`workflow_dispatch`
- 职责：手动生产部署

### `destroy.yml`

- 展示名：`Destroy`
- 触发：`workflow_dispatch`
- 职责：手动销毁环境

## 参考材料

- `examples/workflows/workflow_reference_only/*` 仅作为 reference-only 素材，不进入默认 PR 门禁。
- `tools/ci/validate_workflows.py` 负责检查 workflow 命名、触发器和文档一致性。
- `docs/open-source-ci-strategy.md` 与 `AGENTS.md` 必须保持同步。

## 备注

- 默认门禁链之外的 workflow 不得依赖真实云资源作为通过前提。
- 新增或调整 workflow 时，先更新校验脚本，再更新文档和测试。
