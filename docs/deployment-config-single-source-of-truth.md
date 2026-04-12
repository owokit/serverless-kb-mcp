# 部署配置单一来源

`infra/pipeline-config.json` 是仓库里部署命名和默认值的唯一配置来源。CDK、部署脚本和验证流程都应该从这里读取同一份配置，而不是把默认值分散到 workflow 或 shell 脚本里。

## 配置分工

- `name_prefix`：统一派生资源名前缀。
- `name_suffix`：按账号 / 区域追加后缀，`auto` 表示自动拼接。
- `resource_names`：显式列出所有 AWS 资源名。
- `defaults`：保存运行时默认值、API Gateway stage 名称、API Key 保护开关、Usage Plan 配额和节流参数。
- `embedding_profiles`：保存每个 embedding profile 的 provider / model / dimension / vector bucket / vector index / enable flags。
- `lambda_settings`：保存每个 Lambda 的内存和超时设置。

## 当前与 API Key 相关的单一事实来源

`defaults` 里新增了以下字段：

- `remote_mcp_api_key_protection_enabled`：是否对外部 MCP 前门启用 API Key 保护。
- `remote_mcp_api_throttle_rate_limit`：API Gateway stage 和 Usage Plan 的 rate limit。
- `remote_mcp_api_throttle_burst_limit`：API Gateway stage 和 Usage Plan 的 burst limit。
- `remote_mcp_api_quota_limit`：Usage Plan quota 的周期内请求数上限。
- `remote_mcp_api_quota_period`：Quota 周期，当前仅允许 `DAY`、`WEEK`、`MONTH`。

`resource_names` 里新增了：

- `remote_mcp_usage_plan`
- `remote_mcp_api_key`

API Key 的值不写进 `pipeline-config.json`，而是通过部署输入传入：

- 本地 / 手工部署：`REMOTE_MCP_API_KEY_VALUE`
- GitHub Actions production deploy：`secrets.REMOTE_MCP_API_KEY_VALUE`
- GitHub Actions smoke：`secrets.REMOTE_MCP_API_KEY_VALUE`

## 使用方式

- `infra/cdk/bin/app.ts` 读取 `infra/pipeline-config.json`，再把 `DeploymentInputs` 传给各个 stack。
- `scripts/prod-deploy.sh` 读取同一份配置，并在 API Key 保护开启时要求 `REMOTE_MCP_API_KEY_VALUE` 存在。
- `aws-smoke.yml` 使用同一份 API key 去验证 `/mcp`。

## 约束

- 不要在 workflow 里再散落新的 throttle / quota magic value。
- 如果 future upgrade 需要更强的认证方式，优先在 `defaults` 里增加新的访问控制配置，而不是把逻辑写死在 Lambda。
- 如果以后关闭 API Key 保护，仍然保留同一份 `pipeline-config.json` 作为部署输入中心。
