# serverless_mcp

`services/ocr-pipeline/` 是服务源码树，`services/ocr-pipeline/src/serverless_mcp/` 是 `serverless-mcp-service` 的正式包根。

当前布局已经完成物理收口，不再依赖 `serverless_mcp/__init__.py` 的兼容注入。

## 目录边界

- `serverless_mcp/domain/`：领域模型、值对象、schema 和错误类型。
- `serverless_mcp/core/`：序列化、解析和通用基础能力，不承担 AWS 装配逻辑。
- `serverless_mcp/runtime/`：配置加载、AWS 客户端、composition root 和运行时构造。
- `serverless_mcp/server/`：远程 MCP 处理器使用的官方 FastMCP 兼容导出和传输安全配置。
- `serverless_mcp/entrypoints/`：Lambda 与 API 入口层，只保留 handler 与最薄适配。
- `serverless_mcp/extract/`、`serverless_mcp/embed/`、`serverless_mcp/query/`、`serverless_mcp/status/`、`serverless_mcp/ocr/`：业务实现与应用服务。
- `serverless_mcp/storage/`：持久化实现，包括状态、manifest、projection 和路径工具。
- `tests/`：服务包级测试。

## 当前公共入口

`tools/packaging/serverless_mcp/lambda_wrappers.py` 是包装器注册表的单一事实来源，对外可见与可打包的入口如下：

- `serverless_mcp.entrypoints.ingest.lambda_handler`
- `serverless_mcp.entrypoints.extract_prepare.lambda_handler`
- `serverless_mcp.entrypoints.extract_sync.lambda_handler`
- `serverless_mcp.entrypoints.extract_submit.lambda_handler`
- `serverless_mcp.entrypoints.extract_poll.lambda_handler`
- `serverless_mcp.entrypoints.extract_persist.lambda_handler`
- `serverless_mcp.entrypoints.extract_mark_failed.lambda_handler`
- `serverless_mcp.entrypoints.embed.lambda_handler`
- `serverless_mcp.entrypoints.remote_mcp.lambda_handler`
- `serverless_mcp.entrypoints.backfill.lambda_handler`
- `serverless_mcp.entrypoints.job_status.lambda_handler`

## 配置约定

- `SERVERLESS_MCP_PIPELINE_CONFIG_PATH` 指向仓库根目录的 `pipeline-config.json`。
- `services/pyproject.toml` 通过 editable install 把 `serverless-mcp-service` 指向 `services/ocr-pipeline/`，正式包根位于 `services/ocr-pipeline/src/serverless_mcp/`。
- 运行时不要再向上回扫仓库根目录去猜依赖或入口，配置必须显式提供，也不要再依赖父目录注入。

## 打包约定

- Lambda wrapper 由 `tools/packaging/serverless_mcp/package_lambda.py` 生成。
- Lambda wrapper 注册表由 `tools/packaging/serverless_mcp/lambda_wrappers.py` 维护，并作为打包流水线的唯一事实来源。
- 打包产物名称由仓库名、函数名和目标产物类型共同决定。
- 运行时导入路径必须始终指向 `serverless_mcp`，不要再引用旧的 `s3vectors_mcp` 目录名，也不要再引用旧的 `mcp.server` 命名空间。

## 本地测试

- 在仓库根目录先执行 `uv sync --locked --project services`。
- 再使用 `uv run --project services pytest -q` 运行服务包测试。
- 如需 lint，可使用 `uv run --project services ruff check ocr-pipeline/src ocr-pipeline/tests tools/ci`。

## 维护提示

- 新增或移动模块时，要同步更新测试中的 import 路径和脚本路径。
- 如果继续发生包名变更，先更新这里、对应脚本和测试，再改 workflow 引用。
