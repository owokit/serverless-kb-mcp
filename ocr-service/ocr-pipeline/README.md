# serverless_mcp

`ocr-service/ocr-pipeline/` 是服务源代码树，`ocr-service/ocr-pipeline/src/serverless_mcp/` 是 `serverless-mcp-service` 的正式包根。

当前默认对外查询前门由 API Gateway REST API 保护，访问 `/mcp` 时必须携带 `X-API-Key`。这个 key 只是个人阶段的轻量访问门槛和流量治理手段，不等价于企业级认证授权。

## 目录边界

- `serverless_mcp/domain/`：领域模型、值对象、schema 和错误类型。
- `serverless_mcp/core/`：序列化、解析和通用基础能力，不承担 AWS 装配逻辑。
- `serverless_mcp/runtime/`：配置加载、AWS 客户端和 composition root。
- `serverless_mcp/mcp_gateway/`：查询侧 MCP gateway，负责协议装配、tool 注册和 query-side 调度。
- `serverless_mcp/entrypoints/`：Lambda / API 入口层，只保留最薄 handler。
- `serverless_mcp/extract/`、`serverless_mcp/embed/`、`serverless_mcp/query/`、`serverless_mcp/status/`、`serverless_mcp/ocr/`：业务实现与应用服务。
- `tests/`：服务包级测试。

## 配置约定

- `SERVERLESS_MCP_PIPELINE_CONFIG_PATH` 指向 `infra/pipeline-config.json`。
- 部署层通过 `REMOTE_MCP_API_KEY_VALUE` 提供 API key。
- 不要把 API key 写进代码、日志或 PR 描述。

## 本地测试

- 在仓库根目录执行 `uv sync --locked --project ocr-service`。
- 再执行 `uv run --project ocr-service pytest -q`。
- 如果需要 lint，可以运行 `uv run --project ocr-service ruff check ocr-pipeline/src ocr-pipeline/tests ocr-service/tools/ci`。
