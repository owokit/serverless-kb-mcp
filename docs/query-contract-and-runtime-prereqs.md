# 查询契约与运行前提

本文补充远程查询响应的契约，以及运行和部署时必须满足的前提条件。

## 查询契约

- `query`、`results`、`overall_status`、`progress_percent`、`current_stage`、`stages` 是对外稳定字段。
- `degraded_profiles` 用于提示某个 profile 在查询过程中退化，只记录 `profile_id`、`stage`、`error` 和可选的 `manifest_s3_uri`。
- `metadata` 中的内部字段会在返回前清理，不应当作为客户端契约。
- 某个 profile 的 manifest 或对象状态读失败时，查询应尽量返回其余可用结果，并通过 `degraded_profiles` 反映退化原因。

## 运行前提

- `manifest` 和 `cleanup` 路径依赖 S3 versioning，相关 bucket 必须开启 Versioning。
- `object_state`、`manifest_index`、`embedding_projection_state` 和队列权限必须在部署时一起校验。
- PaddleOCR 的出口 URL 必须使用 HTTPS，提交端和结果下载端都要满足 host 校验。
- 远程 MCP 查询的业务上下文仍然需要 tenant；这是查询业务语义，不是 API Gateway 访问控制。
- 外部 MCP 前门现在由 API Gateway API Key 保护，调用 `/mcp` 时必须显式带 `X-API-Key`。

## 迁移说明

- 如果旧部署依赖 `remote_mcp_default_tenant_id="lookup"` 的匿名回退，现在会在缺少 `tenant_id` 时返回错误。
- 如果确实要保留匿名查询，必须显式启用 `allow_unauthenticated_query=true`，并把 `remote_mcp_default_tenant_id` 改成明确的公开 tenant ID。
- 如果不打算开放匿名查询，就保持 `allow_unauthenticated_query=false`，并要求调用方始终传入 tenant 上下文或认证声明。
- API Key 保护只影响外部前门，不改变内部 query runtime 的 tenant 语义。

## 排障提示

- 看到 `tenant_id is required`，说明请求缺少 tenant 上下文。
- 看到 403，先检查 API Key、Usage Plan 和 stage 绑定，再看 Lambda 日志。
- 看到 `degraded_profiles`，优先检查 manifest、object state 和 projection state 是否一致。

## 运维建议

- 外部 MCP 前门建议把 API key 放在 GitHub secret、参数文件或本地环境变量里，而不是写进文档正文。
- 如果未来切换到 Cognito / Lambda Authorizer / IAM，优先保留当前 query contract，不要把业务字段和访问控制字段混成一层。
