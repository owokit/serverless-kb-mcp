# 查询契约与运行前提

本文档补充远程查询响应契约，以及运行时和部署前必须满足的前提条件。

## 查询契约

- `query`、`results`、`overall_status`、`progress_percent`、`current_stage`、`stages` 是对外稳定字段。
- `degraded_profiles` 用于提示某个 profile 在查询过程中退化，只记录 `profile_id`、`stage`、`error` 和可选的 `manifest_s3_uri`。
- `metadata` 中的内部字段会在返回前被清理，不应依赖它们作为客户端契约。
- 当某个 profile 的 manifest 或对象状态读取失败时，查询应尽量返回其余可用结果，同时通过 `degraded_profiles` 反映退化原因。

## 运行前提

- `manifest` 与 `cleanup` 路径依赖 S3 版本号，相关 bucket 必须开启 Versioning。
- `object_state`、`manifest_index`、`embedding_projection_state` 和队列权限必须在部署时一起校验。
- PaddleOCR 出口 URL 必须使用 HTTPS，并且提交端和结果下载端都要满足 host 校验。
- 远程 MCP 查询必须显式提供 tenant 上下文，或由已认证请求中的 tenant 声明解析得到。
- 如果启用匿名查询，必须显式配置允许策略，不能依赖隐式默认值回退到共享 lookup 租户。

## 迁移说明

- 既有部署如果之前依赖 `remote_mcp_default_tenant_id="lookup"` 的匿名回退，现在会在缺少 `tenant_id` 时返回 `tenant_id is required`。
- 如果你要保留匿名查询，请把 `allow_unauthenticated_query` 显式设为 `true`，并把 `remote_mcp_default_tenant_id` 改成真实 tenant ID，而不是继续使用 `lookup`。
- 如果你不打算开放匿名查询，就保持 `allow_unauthenticated_query=false`，并要求调用方始终传入 `tenant_id` 或认证声明。

## 排障提示

- 如果查询结果出现 `degraded_profiles`，先检查 manifest 是否缺失，再检查 object state 和 projection state 是否一致。
- 如果看到 `tenant_id is required`，说明请求没有带 tenant 上下文，也没有可用的认证声明或匿名配置。
- 如果 PaddleOCR 状态轮询连续失败，优先区分网络抖动和配置错误，再决定是否重试。

## 2026-04 运行时补充

- 生产默认已经收紧为 `allow_unauthenticated_query=false`，匿名查询不再作为默认入口。
- 如果业务确实需要匿名访问，必须显式开启 `allow_unauthenticated_query=true`，并把 `remote_mcp_default_tenant_id` 配成明确的公开 tenant，不要继续依赖 `lookup` 作为默认回退。
- Embed 侧旧版本向量清理由独立的 Step Functions cleanup workflow 负责，运行时需要显式注入 `VECTOR_CLEANUP_STATE_MACHINE_ARN`。
- 查询侧优先走 chunk projection 读取，只有投影缺失或回源失败时才读取完整 manifest。
## 清理可观测性

- 旧版本向量清理由独立的 Step Functions cleanup workflow 执行，embed 侧只负责生成 cleanup plan 并发起编排。
- cleanup dispatch 会输出结构化 metric `embed.cleanup.dispatch`，`status` 取值为 `started`、`succeeded` 或 `failed`。
- 失败时会额外带上 `error_type`，并写回 `object_state.last_error`，用于排障和回放。
- cleanup execution name 由 cleanup plan 的确定性 payload 派生，相同计划重复触发时应得到相同 execution name，便于幂等去重。
- cleanup dispatch 成功后，不等待删除完成；真正的删除重试、补偿和幂等判断由 cleanup workflow 自己负责。
