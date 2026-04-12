# AWS 控制台手工部署：查询治理与验证

本文说明远程 MCP 前门当前的访问模型、如何验证 `/mcp`、以及如何轮换 API Key。

## 1. 当前结论

```text
API Gateway REST -> remote_mcp Lambda -> mcp_gateway.handler -> AWS Labs mcp-lambda-handler -> tools
```

- `remote_mcp` 仍然是对外查询入口，但现在不再默认匿名开放。
- `/mcp` 路径由 API Gateway REST API 暴露，且必须携带 `X-API-Key`。
- API Key 不是企业级认证授权方案，它只是当前个人阶段的轻量访问门槛和流量治理手段。
- 未来如果要升级到 Cognito、Lambda Authorizer 或 IAM/SigV4，代码结构已经保留了 API Gateway 方法级鉴权的扩展位。

## 2. 访问方式

请求 `/mcp` 时需要带上 API Gateway 要求的 header：

```bash
curl -H "X-API-Key: <your-api-key>" \
  https://<api-id>.execute-api.<region>.amazonaws.com/mcp
```

- header 名称固定是 `X-API-Key`，大小写按 HTTP 规范处理即可。
- 访问失败时，先确认 API Key、Usage Plan 和 stage 绑定是否都已部署。
- 如果没有带 key，API Gateway 应该返回 403，而不是 Lambda 自己做 header 校验。

## 3. Usage Plan 与 quota

当前对外前门使用 API Gateway 原生 Usage Plan：

- API Key 绑定到 Usage Plan。
- Usage Plan 再绑定到 `mcp` stage。
- throttle 和 quota 都由 `infra/pipeline-config.json` 集中控制。
- quota 是按周期计数的治理手段，不是强安全边界。

建议运维时关注：

- rate limit：是否过低，导致个人常用调用被限流。
- burst limit：是否足以覆盖短时峰值。
- quota：是否足以支持当前使用频率。

## 4. 手工验证步骤

1. 部署完成后，先确认 API Gateway stage 已经是 `mcp`。
2. 确认 `REMOTE_MCP_API_KEY_VALUE` 已经通过部署输入提供。
3. 使用带 `X-API-Key` 的请求访问 `/mcp`。
4. 不带 key 再访问一次，预期返回 403。
5. 如果 `/mcp` 仍然能匿名访问，优先检查 API method 的 `apiKeyRequired`，再检查 Usage Plan / ApiKey / Stage 绑定。

## 5. 轮换 API Key

1. 生成新的 `REMOTE_MCP_API_KEY_VALUE`。
2. 更新 GitHub secret 或本地部署环境变量。
3. 重新部署 API stack。
4. 更新 smoke / 验证步骤中引用的同一 secret。
5. 删除旧 key 或让旧 key 失效。

轮换时不要把 key 直接写进日志、PR 描述或 issue 正文。

## 6. 限制

- API Key 只适合当前个人阶段的访问门槛和配额治理。
- 它不提供用户级别身份绑定，也不替代签名请求。
- 如果未来要做细粒度授权，应该切到 Cognito Authorizer、Lambda Authorizer 或 IAM/SigV4。

## 7. 同步要求

- 如果远程 MCP 前门的访问方式变化，必须同步更新 `docs/README.md`、`docs/deployment-config-single-source-of-truth.md` 和 workflow。
- 如果 `X-API-Key` 的获取方式变化，必须同步更新 smoke、生产部署和验证脚本。
