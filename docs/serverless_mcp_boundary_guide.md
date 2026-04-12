# serverless_mcp 边界说明

本文记录 `ocr-service/ocr-pipeline/src/serverless_mcp` 当前的主要边界，便于后续继续拆分职责。

## 主要边界

- `serverless_mcp/domain/`：领域模型、值对象、schema 和错误类型。
- `serverless_mcp/core/`：序列化、解析和通用基础能力。
- `serverless_mcp/runtime/`：环境加载、AWS 客户端和运行时装配。
- `serverless_mcp/mcp_gateway/`：查询侧 MCP gateway，负责协议装配、tool 注册和 query-side 调度。
- `serverless_mcp/entrypoints/`：Lambda / API 入口层，只保留最薄 wrapper。
- `serverless_mcp/extract/`：提取链路的业务实现与编排。
- `serverless_mcp/embed/`：嵌入链路的业务实现与状态管理。
- `serverless_mcp/query/`：查询服务。
- `serverless_mcp/status/`：任务状态查询服务。
- `serverless_mcp/storage/`：持久化实现。

## 入口约束

- `remote_mcp` 只是查询侧 wrapper，实际协议处理仍在 `serverless_mcp.mcp_gateway.handler` 和 vendored handler 中。
- 远程 MCP 前门现在由 API Gateway API Key 保护，不应再被描述为匿名开放。
- `X-API-Key` 只属于 API Gateway 边界，不属于 `serverless_mcp` 运行时的业务契约。
- `serverless_mcp/mcp_gateway/` 只负责 query-side tools，不直接承载 OCR、Step Functions 或 embedding worker。

## 代码约束

- 新代码优先直接依赖 `domain`、`storage/*` 和 `runtime/*` 的正式包路径。
- 如果某个文件只是转发导出，应该优先删掉或折叠回真正的实现模块。
- 不要再引入新的根级兼容层。

## 运行提示

- `SERVERLESS_MCP_PIPELINE_CONFIG_PATH` 指向 `infra/pipeline-config.json`。
- API Key 访问门槛由部署层和 workflow 处理，不需要在 `serverless_mcp` 里自己手写 header 校验。
- 如果未来切换到 Cognito / Lambda Authorizer / IAM，先改边界文档，再改运行时文档和 workflow。
