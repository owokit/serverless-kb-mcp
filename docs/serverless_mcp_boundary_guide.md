# serverless_mcp 边界说明

本文记录 `ocr-service/ocr-pipeline/src/serverless_mcp` 当前的主要边界，便于后续继续拆分时对齐职责。

## 当前主边界

- `serverless_mcp/domain/`：领域模型、值对象、schema 和错误类型。
- `serverless_mcp/core/`：序列化、解析和低层公共能力；`serverless_mcp/core/parsers.py` 是解析入口的正式位置。
- `serverless_mcp/runtime/`：环境加载、AWS 客户端和运行时装配。
- `serverless_mcp/mcp_gateway/`：查询侧 MCP gateway，负责协议装配、tool 注册和 query-side 服务调度。
- `serverless_mcp/entrypoints/`：Lambda / API 入口层，`remote_mcp.py` 只保留最薄 wrapper。
- `serverless_mcp/extract/`：提取链路的业务实现与编排。
- `serverless_mcp/embed/`：嵌入链路的业务实现与投影状态管理。
- `serverless_mcp/query/`：查询服务。
- `serverless_mcp/status/`：任务状态查询服务。
- `serverless_mcp/storage/`：持久化实现。

## storage 子边界

`storage/` 只保留真实实现，不再保留根级转发壳。

- `serverless_mcp/storage/state/object_state_repository.py`：对象主状态、版本推进、幂等与旧版本治理。
- `serverless_mcp/storage/state/execution_state_repository.py`：提取 / 嵌入执行态。
- `serverless_mcp/storage/manifest/repository.py`：manifest 与资产写入、索引维护。
- `serverless_mcp/storage/projection/repository.py`：按 embedding profile 隔离的投影状态。
- `serverless_mcp/storage/paths.py`：manifest 和 asset 路径生成。
- `serverless_mcp/storage/batch.py`：DynamoDB batch write 重试辅助。

## runtime 装配规则

- `serverless_mcp/runtime/config.py` 负责环境变量加载和 `load_settings()`。
- `serverless_mcp/runtime/aws_clients.py` 负责 boto3 客户端构建和缓存。
- `serverless_mcp/runtime/bootstrap.py` 只提供 `build_runtime_context()` 这类 composition root 汇总，不放业务逻辑。
- `serverless_mcp/runtime/ingest.py`、`serverless_mcp/runtime/embed_runtime.py`、`serverless_mcp/runtime/query_runtime.py`、`serverless_mcp/status/runtime.py` 分别装配各自服务。
- `serverless_mcp/runtime/pipeline.py`、`serverless_mcp/runtime/step_functions_workflow.py`、`serverless_mcp/runtime/worker.py` 这类旧别名壳已经删除，不应再引入。

## 入口层规则

- `serverless_mcp/entrypoints/*` 只放对外 handler。
- `tools/packaging/serverless_mcp/lambda_wrappers.py` 维护 Lambda wrapper 注册表和 wrapper 生成逻辑，不属于服务包公共 API。
- 旧的根级兼容壳已经删除，新增代码必须直接引用 `serverless_mcp.*` 的正式包路径。
- `serverless_mcp/embed/application.py`、`serverless_mcp/query/application.py`、`serverless_mcp/status/application.py` 是正式应用层；`serverless_mcp/extract/service.py`、`serverless_mcp/embed/worker.py`、`serverless_mcp/query/service.py`、`serverless_mcp/status/service.py`、`serverless_mcp/embed/parser.py`、`serverless_mcp/events/parser.py` 这类旧别名壳已经删除，不应再引入。
- `mcp_gateway/` 是新的 query-side 入口边界；它只注册业务化 tools，不直接承载 OCR、Step Functions 或 embedding worker。
- `mcp/server/*` 这组旧命名空间已经退场，不应再从 `mcp.server.*` 导入。

## 代码约束

- 新代码优先直接依赖 `domain`、`storage/*` 子包和 `runtime/*` builder。
- 如果某个文件只是在转发导出，应该删除它，或者把逻辑折回真正的实现模块。
- 不要再引入新的根级兼容层。
