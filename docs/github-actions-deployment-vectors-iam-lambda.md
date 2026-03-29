# 03 GitHub Actions 部署说明

本页说明当前仓库的部署资源边界、IAM 权限面、打包产物和环境变量治理口径。

## 1. 部署前提

### 1.1 向量存储

- 先创建专用的 vector bucket。
- 每个 embedding profile 对应一个独立的 index。
- 不同 `provider / model / dimension` 不得混写到同一个 index。

### 1.2 资源就绪状态

部署前应确认：

- vector bucket 已创建。
- 对应 index 已创建。
- 目标 profile 的 `provider / model / dimension` 与 index 一致。
- 默认 OpenAI profile 已启用，其他 profile 只有在配置中显式开启后才参与部署。

## 2. IAM 与权限边界

### 2.1 Ingest Lambda

最小权限应包含：

- 读取 source bucket 元数据。
- 读取 `object_state`。
- 读取和写入入站队列。

### 2.2 Extract Workflow Lambdas

最小权限应包含：

- 写入 manifest bucket。
- 调用 OCR 相关 API。
- 写入 `manifest_index`。
- 读取 `object_state`。
- 多 profile 场景下更新 `embedding_projection_state`。

### 2.3 Embed Lambda

最小权限应包含：

- 读取 manifest bucket。
- 调用 embedding provider。
- 写入 `S3 Vectors`。
- 更新 `embedding_projection_state`。

### 2.4 Remote MCP / API Gateway / CloudFront

最小权限应包含：

- `API Gateway REST` 所需的运行权限。
- 对应的 `remote_mcp` Lambda 访问权限。
- 如启用分发，再使用 `CloudFront` 做 CDN。

相关环境变量：

- `CLOUDFRONT_DISTRIBUTION_DOMAIN`
- `CLOUDFRONT_KEY_PAIR_ID`
- `CLOUDFRONT_PRIVATE_KEY_PEM`
- `CLOUDFRONT_URL_TTL_SECONDS`

### 2.5 最小权限原则

- 只授予当前阶段必需的读写能力。
- 不把部署账户权限扩展给运行时代码。
- 不把管理面权限混到查询面或抽取面。

## 3. 产物打包与发布

### 3.1 Lambda ZIP 与 Layer ZIP

当前仓库仍拆分为：

- 10 个 Lambda ZIP
- 3 个 Layer ZIP

Layer 分组为：

- `core`
- `extract`
- `embedding`

### 3.2 GitHub Actions 流程

当前发布顺序为：

1. 构建 Lambda ZIP 和 Layer ZIP。
2. 上传 ZIP 到 S3。
3. 调用部署脚本更新 Lambda、Layer、API Gateway 和相关基础设施。
4. 部署成功后再创建或更新 Release。

## 4. Lambda 入口与环境变量

### 4.1 统一约定

- 对外入口以 `Ingest Lambda` 为主。
- 内部运行时继续拆分为 ingest / extract / embed / remote MCP / backfill / job status 等入口。
- 入口层只保留 handler 与最薄适配，不在入口里编排业务流程。

### 4.2 Ingest Lambda

入口：

`serverless_mcp.entrypoints.ingest.lambda_handler`

用途：

- 解析 `bucket / key / version_id / sequencer`。
- 执行幂等与版本拦截预检查。
- 启动 `Step Functions Standard`。

### 4.3 Extract Workflow Lambdas

入口：

`serverless_mcp.entrypoints.extract.lambda_handler`

用途：

- `extract_prepare`：准备输入和上下文。
- `extract_poll`：轮询 PaddleOCR 任务状态。
- `extract_persist`：拉取结果、生成 manifest、写入 bucket。

### 4.4 Embed Lambda

入口：

`serverless_mcp.entrypoints.embed.lambda_handler`

用途：

- 消费 embedding 任务。
- 调用 embedding provider。
- 写入 `S3 Vectors`。
- 更新 `embedding_projection_state`。

### 4.5 Remote MCP Lambda

入口：

`serverless_mcp.entrypoints.remote_mcp.lambda_handler`

用途：

- 作为查询侧 MCP gateway 的最薄 wrapper。
- 通过 AWS Labs `mcp-lambda-handler` 暴露标准 MCP 协议。
- 只注册业务化 tools，不直接暴露 embedding/vector/workflow 内部能力。
- 使用 `OBJECT_STATE_TABLE`、`MANIFEST_INDEX_TABLE`、`MANIFEST_BUCKET`、`MANIFEST_PREFIX`、`ALLOW_UNAUTHENTICATED_QUERY`、`QUERY_TENANT_CLAIM`、`QUERY_MAX_TOP_K`、`QUERY_MAX_NEIGHBOR_EXPAND` 等查询配置。
- 通过 `EMBEDDING_PROFILES_JSON`、`ALLOW_UNAUTHENTICATED_QUERY`、`QUERY_TENANT_CLAIM`、`QUERY_MAX_TOP_K`、`QUERY_MAX_NEIGHBOR_EXPAND` 控制查询行为。
- 默认 session 策略为 stateless-first；如需显式会话，再通过 `MCP_SESSION_TABLE` 或 `REMOTE_MCP_SESSION_TABLE` 切换到 DynamoDB session backend。

### 4.6 Backfill Lambda

入口：

`serverless_mcp.entrypoints.backfill.lambda_handler`

用途：

- 按 profile 回填历史 embedding。
- 用于新增 profile 上线或维度切换后的重嵌。

## 5. 变更联动

如果发生以下变化，必须同步更新 `docs/`、`infra/`、相关 workflow 和测试：

- 资源拓扑变化
- IAM 权限边界变化
- 环境变量变化
- 部署顺序变化
- embedding provider、profile、vector bucket 或 index 变化

如果修改了 Python 代码，还需要确认仓库验证链是否仍然通过，并保持文档与代码边界一致。
