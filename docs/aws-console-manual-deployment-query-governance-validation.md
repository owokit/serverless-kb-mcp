# 05 AWS 控制台部署：远程 MCP 公共入口校验

本文只保留远程 MCP 公共查询入口的当前结论和校验步骤。

## 1. 当前结论

```text
API Gateway REST -> remote_mcp Lambda -> mcp_gateway.handler -> AWS Labs mcp-lambda-handler -> tools
```

- `remote_mcp` 仍是唯一对外查询入口，但它现在只是 `mcp_gateway` 的薄 wrapper。
- 公网入口通过 `API Gateway REST` 的 `mcp` stage 访问，公开 URL 形态是 `.../mcp`。
- `Lambda Function URL` 不作为标准入口。
- 远程 MCP 入口现在按开放访问处理，不保留额外的 DNS rebinding 保护。
- 协议层由 AWS Labs `mcp-lambda-handler` 统一处理，不再手写 JSON-RPC 路由。
- 查询侧 tools 只暴露业务能力，不直接暴露 embedding、vector index 或 worker 内部接口。

## 2. 运行时配置

远程 MCP Lambda 仍然需要这些查询相关环境变量：

- `OBJECT_STATE_TABLE`
- `MANIFEST_INDEX_TABLE`
- `MANIFEST_BUCKET`
- `MANIFEST_PREFIX`
- `ALLOW_UNAUTHENTICATED_QUERY`
- `QUERY_TENANT_CLAIM`
- `QUERY_MAX_TOP_K`
- `QUERY_MAX_NEIGHBOR_EXPAND`

如果需要返回 CloudFront 签名 URL，还需要：

- `CLOUDFRONT_DISTRIBUTION_DOMAIN`
- `CLOUDFRONT_KEY_PAIR_ID`
- `CLOUDFRONT_PRIVATE_KEY_PEM`
- `CLOUDFRONT_URL_TTL_SECONDS`

## 3. 入口校验

部署后直接验证下面的地址：

```text
https://qfelbun8hl.execute-api.us-east-1.amazonaws.com/mcp
```

如果返回的是 MCP 协议响应或工具列表，说明入口可用。
如果 GET `/mcp` 返回 discovery 文档，则说明薄 wrapper 也可访问。

如果仍然出现 `403` 或 `421`，优先检查：

1. API Gateway 的 stage 和 proxy 路由是否部署正确
2. Lambda 是否仍挂着旧的 Function URL 配置
3. 是否还有旧的 host 校验或边缘代理缓存
4. `mcp_gateway` 是否仍然误引用了摄取链路内部实现

## 4. 同步规则

- 如果远程 MCP 入口协议变化，必须同步更新 `docs/` 和 `infra/`
- 如果只是入口开放策略变化，不要再把旧的 Function URL 口径写回文档
- 如果新增任何新的访问保护，必须先更新代码，再更新说明
