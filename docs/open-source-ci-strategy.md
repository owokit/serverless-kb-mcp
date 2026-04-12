# 开源仓库 CI 策略

本文记录仓库当前公开 CI、默认门禁链和辅助 workflow 的职责边界。

## 设计原则

- 默认 PR 门禁只使用 GitHub 官方 hosted runner。
- Node.js 和 Python 运行时必须通过 `.github/config/ci-runtime.json` 统一管理。
- 所有 workflow 运行时安装都必须走 `.github/actions/setup-runtime/action.yml`，不要直接写死 `actions/setup-node` / `actions/setup-python` 的版本。
- 需要外部行为时，优先使用 fixture、local emulator 和本地仿真。

## 默认 PR 门禁链

当前默认门禁链如下：

`Workflow Sanity -> Guardrails -> Logic CI -> Contract CI -> Local Integration CI`

职责分工：

- `PR Validate`：快速检查 workflow lint、Python boundary tests、Lambda packaging smoke 和 CDK synth contract。
- `Workflow Sanity`：检查 workflow 命名、tabs、actionlint 和 inventory 一致性。
- `Guardrails`：扫描 secret shape、简体中文乱码和私用区字符。
- `Logic CI`：运行逻辑层测试、类型检查、lint 和构建检查。
- `Contract CI`：验证 provider 契约、序列化格式和存储契约。
- `Local Integration CI`：在本地仿真环境里串起整条链路。
- `Package Release`：只在上游 CI 成功后发布产物。

## 关键 workflow

### `workflow-sanity.yml`

- 展示名：`Workflow Sanity`
- 触发：`pull_request`、`push`、`workflow_dispatch`
- 职责：校验 workflow 命名、tabs、actionlint 和清单一致性。

### `guardrails.yml`

- 展示名：`Guardrails`
- 触发：`workflow_run`、`workflow_dispatch`
- 职责：扫描疑似私用区字符、简体中文乱码和 secret 形状。

### `ci-failure-comment-relay.yml`

- 展示名：`CI Failure Comment Relay`
- 触发：`workflow_run`
- 职责：把公开 CI 失败回写到对应 PR。
- 实现脚本：`ocr-service/tools/ci/comment_pr_failure.py`

### `pr-path-conflict-guard.yml`

- 展示名：`PR Path Conflict Guard`
- 触发：`pull_request_target`、`workflow_dispatch`
- 职责：扫描并行 PR 的删除 / 重命名路径漂移。

### `issue-hierarchy-guard.yml`

- 展示名：`Issue Hierarchy Guard`
- 触发：`issues`、`workflow_dispatch`
- 职责：校验主 issue 与子 issue 的关闭层级。

### `issue-similarity-triage.yml`

- 展示名：`Issue Similarity Triage`
- 触发：`issues`、`workflow_dispatch`
- 职责：相似 issue 自动归类。

### `issue-similarity-closure.yml`

- 展示名：`Issue Similarity Closure`
- 触发：`schedule`、`workflow_dispatch`
- 职责：定时关闭长期未处理的相似 issue。

### `logic-ci.yml`

- 展示名：`Logic CI`
- 触发：`workflow_run`、`workflow_dispatch`
- 职责：运行逻辑层测试、类型检查、lint 和构建检查。

### `contract-ci.yml`

- 展示名：`Contract CI`
- 触发：`workflow_run`、`workflow_dispatch`
- 职责：验证 provider 契约、序列化格式和存储契约。

### `local-integration-ci.yml`

- 展示名：`Local Integration CI`
- 触发：`pull_request`、`workflow_run`、`workflow_dispatch`
- 职责：在本地仿真环境里验证整条运行链路。

### `codeql.yml`

- 展示名：`CodeQL JavaScript / TypeScript / Python`
- 触发：`pull_request`、`push`、`schedule`、`workflow_dispatch`
- 职责：静态安全分析。

### `stale-issues.yml`

- 展示名：`Stale Issues`
- 触发：`schedule`、`workflow_dispatch`
- 职责：清理长期无活动的 issue / PR。

### `merged-branch-cleanup.yml`

- 展示名：`Branch Lifecycle Cleanup`
- 触发：`pull_request_target`、`schedule`、`workflow_dispatch`
- 职责：收敛 PR 分支生命周期标签，并在满足条件时清理分支。
- 标签：`branch:protected` 表示仍在打开的 PR，`branch:deletable` 表示已经关闭且满足删除条件的 PR。

### `dependabot-auto-merge.yml`

- 展示名：`Dependabot Auto Merge`
- 触发：`pull_request`
- 职责：自动合并 Dependabot 更新。

### `external-validation.yml`

- 展示名：`External Validation`
- 触发：`workflow_dispatch`
- 职责：手动运行外部网络或 AWS 标记测试。

### `docs-ci.yml`

- 展示名：`Docs CI`
- 触发：`pull_request`、`workflow_dispatch`
- 职责：检查文档和 workflow 名称一致性。

### `security-ci.yml`

- 展示名：`Security CI`
- 触发：`workflow_run`、`workflow_dispatch`
- 职责：额外的安全审计门禁。

### `package-release.yml`

- 展示名：`Package Release`
- 触发：`workflow_run`、`workflow_dispatch`
- 职责：只在 `Local Integration CI` 成功后发布产物。

### `pr-validate.yml`

- 展示名：`PR Validate`
- 触发：`pull_request`、`workflow_dispatch`
- 职责：PR 的快速边界验证。

### `aws-smoke.yml`

- 展示名：`AWS Smoke`
- 触发：`schedule`、`release`、`workflow_dispatch`
- 职责：在真实 AWS 上验证 OIDC 部署、`/mcp` discovery 和 ingest 启动边界。
- 访问控制：对 `remote_mcp` 的 `/mcp` 探测必须显式携带 `X-API-Key`，并使用 `REMOTE_MCP_API_KEY_VALUE` secret。

### `prod-deploy.yml`

- 展示名：`Prod Deploy`
- 触发：`workflow_dispatch`
- 职责：手动生产部署入口，统一通过 `scripts/prod-deploy.sh` 解析路径、资产和 CDK 部署。
- 访问控制：部署输入必须提供 `REMOTE_MCP_API_KEY_VALUE`，以便 API Gateway API Key 能被创建并绑定到 Usage Plan。

### `destroy.yml`

- 展示名：`Destroy`
- 触发：`workflow_dispatch`
- 职责：手动销毁环境。

### `ai-skills-sync.yml`

- 展示名：`AI Skills Sync`
- 触发：`pull_request`
- 职责：同步技能源与生成物一致性。

## 参考素材

- `examples/workflows/workflow_reference_only/*` 只作为 reference-only 素材，不进入默认 PR 门禁。
- `ocr-service/tools/ci/validate_workflows.py` 负责检查 workflow 命名、触发器和文档一致性。
- `AGENTS.md` 必须与本文保持同步。

## 备注

- 默认门禁之外的 workflow 不得依赖真实云资源作为前置条件。
- 修改 workflow 时，先更新校验脚本，再更新文档和测试。
