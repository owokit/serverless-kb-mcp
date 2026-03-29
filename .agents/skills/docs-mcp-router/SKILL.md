---
description: 将 AWS、Azure、Google、GitHub、Cloudflare、OpenAI 和 Anthropic 的文档问题路由到对应的官方 MCP 或第一方文档来源。用于 Codex 需要最新 API 文档、SDK 参考、示例、故障排查、发布说明或产品指引时。
name: docs-mcp-router
---


# 文档 MCP 路由

## 目的

当用户询问某个受支持厂商的官方文档、API 行为、SDK 用法、示例、故障排查或发布说明时，使用这个 skill。优先使用第一方来源，不要凭记忆回答。

## 路由规则

- AWS：先用 `mcp__aws_knowledge_mcp__aws__search_documentation`，需要完整上下文时再用 `mcp__aws_knowledge_mcp__aws__read_documentation`。
- Azure：先用 `mcp__microsoft_docs_mcp__microsoft_docs_search`，再用 `mcp__microsoft_docs_mcp__microsoft_docs_fetch`；需要代码示例时用 `mcp__microsoft_docs_mcp__microsoft_code_sample_search`。
- Google：先用 `mcp__google_developer_knowledge__search_documents`，再用 `mcp__google_developer_knowledge__get_documents`。
- GitHub：针对产品文档，使用 `mcp__github__search_code` 在官方 `github/docs` 仓库中检索，再用 `mcp__github__get_file_contents` 读取匹配页面。`issue/PR` 工具只用于仓库工作流问题。
- Cloudflare：使用 `mcp__cloudflare_docs_mcp__search_cloudflare_documentation`；进行 Pages 到 Workers 迁移时使用 `mcp__cloudflare_docs_mcp__migrate_pages_to_workers_guide`。
- OpenAI：先用 `mcp__openaiDeveloperDocs__search_openai_docs`，再用 `mcp__openaiDeveloperDocs__fetch_openai_doc`；需要时再用 `mcp__openaiDeveloperDocs__list_openai_docs` 和 `mcp__openaiDeveloperDocs__get_openapi_spec`。
- Anthropic：如果当前环境里存在专门的 Anthropic 文档 MCP，就优先使用；否则不要猜测，只能在允许浏览器回退时使用官方 Anthropic 文档站点。

## 输出规则

- 如果请求有歧义，先确认厂商再搜索。
- 做跨厂商比较时，分别检索每个厂商的官方来源。
- 最终回答优先保留准确的页面标题、URL 或 API 名称。
- 说明来源是否为官方，并说明是否由 MCP 提供。
- 如果没有可用的第一方来源，直接说明。
