# serverless-kb-mcp

面向 AWS 的企业级无服务器文档摄取与语义检索系统。

[English](../README.md) | 简体中文

这个仓库不是一个单一应用壳，而是一个多入口工作区。

- Python 工作区根目录是 `services/pyproject.toml` 和 `services/uv.lock`。
- 核心后端服务位于 `services/ocr-pipeline/`。
- 基础设施与部署代码位于 `infra/cdk/`。
- 仅供参考的 workflow 样例位于 `examples/workflows/workflow_reference_only/`。
- 文档中心位于 `docs/`。

## 一览

| 项目 | 说明 |
| --- | --- |
| 对外主入口 | `Ingest Lambda` |
| 仓库身份 | `serverless-kb-mcp` |
| Python 工作区根 | `services/` |
| 服务源代码根 | `services/ocr-pipeline/` |
| 文档中心 | `docs/README.md` |
| 校验方式 | GitHub Actions PR workflow 与 checks |

## 能力说明

- 以 `version_id` 作为主身份的版本感知型文档摄取。
- 通过确定性轮询和结果拉取完成异步 OCR 编排。
- 为提取结果、切片资产和可回放材料生成结构化 manifest。
- 按 profile 隔离的 embedding 与向量存储，支持多模型或多维度场景。
- 在返回检索结果前重新校验对象状态的查询侧保护。

## 仓库结构

仓库围绕运行时链路组织，而不是按单体包组织。

1. `S3` 产生文档事件。
2. `S3 Event Notification` 将事件投递到 `SQS ingest queue`。
3. `Ingest Lambda` 校验对象版本，执行幂等检查，并启动 `Step Functions Standard`。
4. `Step Functions Standard` 负责 OCR 提交、轮询、结果拉取、manifest 生成和 embedding 任务下发。
5. 提取 worker 将结构化产物持久化到 manifest bucket。
6. `Embed Lambda` 消费 embedding 任务，并将向量写入配置好的后端。

这种组织方式在保持对外入口简洁的同时，也保留了清晰的运行时边界。

## 服务边界

服务包现在物理上位于 `services/ocr-pipeline/src/serverless_mcp/`。

- `services/pyproject.toml` 将 `serverless-mcp-service` 指向 `services/ocr-pipeline/` 源码树。
- `services/ocr-pipeline/README.md` 说明包级边界和当前模块布局。
- `services/ocr-pipeline/src/serverless_mcp/__init__.py` 现在只是普通包初始化文件，不再承担兼容外壳职责。

## 文档

| 文档 | 用途 |
| --- | --- |
| [文档中心](../docs/README.md) | 部署、运行时和边界文档总索引 |
| [服务实现说明](../services/ocr-pipeline/README.md) | 包级实现说明 |
| [开源边界说明](../docs/open-source-boundaries.md) | 仓库边界与外部使用约束 |
| [开源交付策略](../docs/open-source-ci-strategy.md) | 开源 CI 与交付模型 |
| [简体中文入口](../README.md) | 简体中文入口与本地导航 |

## Python 工具链

- 使用 `uv sync --locked --project services` 准备 Python 环境。
- 默认本地验证使用 `uv run --project services pytest -q`、`uv run --project services ruff check .` 和 `uv run --project services python ...`。
- 将 `services/pyproject.toml` 和 `services/uv.lock` 视为 Python 依赖的唯一事实来源。
- 日常开发保持在 uv 路径上，不要再引入临时的 `pip` 或 `venv` 启动方式。

## 本地化版本

| 语言 | 入口 |
| --- | --- |
| English | `../README.md` |
| 简体中文 | `i18n/README.zh-CN.md` |

## 验证

- PR 通过 GitHub Actions workflow 和 checks 验证。
- 基础设施和运行时改动仍应遵循仓库既有边界约束。
- 文档更新后要同步保持与 docs 中心和语言入口的链接一致。
