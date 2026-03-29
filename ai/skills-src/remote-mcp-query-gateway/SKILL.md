---
name: remote-mcp-query-gateway
description: 适用于本仓库远程 MCP 查询侧网关的设计、重构、测试与文档同步，覆盖 API Gateway /mcp、Lambda 入口、AWS Labs mcp-lambda-handler、业务化 tools 和 query-side services。
---

# 远程 MCP 查询网关

当任务涉及本仓库的远程 MCP 查询入口、协议层收敛、tool 边界、Lambda 入口、MCP 生命周期测试或相关文档同步时，优先使用这个 skill。

## 适用范围

- `ocr-service/ocr-pipeline/src/serverless_mcp/entrypoints/remote_mcp.py`
- `ocr-service/ocr-pipeline/src/serverless_mcp/mcp_gateway/`
- `ocr-service/ocr-pipeline/src/awslabs/mcp_lambda_handler/`
- `ocr-service/ocr-pipeline/tests/unit/serverless_mcp/`
- `docs/` 中与远程 MCP 查询入口、部署、验证相关的文档

## 当前基线架构

```text
API Gateway /mcp
-> MCP Query Lambda
-> serverless_mcp.entrypoints.remote_mcp.lambda_handler
-> serverless_mcp.mcp_gateway.handler
-> AWS Labs mcp-lambda-handler
-> mcp_gateway.tools.*
-> mcp_gateway.services.*
-> retrieval / document / status services
-> vector store / metadata store / object storage
```

## 模块边界

- `handler.py` 只做 Lambda 接入、HTTP 探针分发和协议适配。
- `server.py` 只负责 handler 初始化、tool 注册和 discovery 文档。
- `auth.py` 只解析 API Gateway authorizer / identity 上下文。
- `schemas.py` 只放请求和响应的结构化 schema。
- `sessions.py` 只放会话后端适配，默认 stateless-first。
- `tools/` 只做参数校验、service 调度和结果整形。
- `services/` 只组合查询侧业务能力，不直接暴露 embedding、vector 或 worker 内部算子。

## 必须遵守的约束

- 不要使用 `run-model-context-protocol-servers-with-aws-lambda` 的 stdio wrapper 方案。
- 不要自己实现 JSON-RPC 路由。
- 不要把 OCR、Step Functions、embedding worker 迁入 MCP 入口 Lambda。
- 不要把 embedding/vector/index 细节暴露成 MCP tools。
- 默认采用 stateless-first；只有确有需要时，再启用 DynamoDB session backend。
- 如库 API 与示例有差异，先检查当前包源码或可用符号，再编码。
- 远程 MCP 入口的变化，优先影响查询侧，不要回卷 ingestion pipeline。

## 推荐工具集合

首批工具应保持业务化，只暴露稳定查询能力：

- `search_documents`
- `get_document_excerpt`
- `list_document_versions`
- `get_ingestion_status`

如果必须新增工具，优先满足业务语义，再考虑是否需要扩展到运维能力。不要新增直接操纵 embedding 或 vector store 的 tool。

## 实施顺序

1. 先识别当前 MCP 入口、协议层和查询能力。
2. 再确认 `mcp_gateway` 的模块边界和文件落位。
3. 再接入 AWS Labs `mcp-lambda-handler`。
4. 再收敛 tools 到业务能力，删除重复的旧实现。
5. 再调整 API Gateway / Lambda 入口和测试。
6. 最后同步文档、技能和交付说明。

## 测试要求

- 必须补齐 MCP 生命周期测试：`initialize`、`tools/list`、`tools/call`、错误参数路径。
- 必须补齐 discovery 探针测试。
- 修改 Python 代码后，必须跑仓库要求的全量测试。
- 若改动影响协议或边界，要同时检查 `ruff`、pytest 和 PR 模板描述是否一致。

## 交付检查

- PR 正文前部必须写明 `Closes #123` / `Fixes #123` / 跨仓库引用。
- 如果只覆盖部分 leaf issue，不能让父 issue 误关闭。
- 修改 skill 后，必须运行 `python ai/scripts/sync-ai.py`，同步到 `.agents/skills/` 和 `.claude/skills/`。
- 如果新增或调整了本 skill 的边界，也要同步更新 `AGENTS.md`。
- 提交前要确认 PR 的真实状态，避免在已关闭或已合并分支上继续叙事。

## 备注

这个 skill 只描述查询侧 MCP 网关的当前方案，不负责 ingestion pipeline、OCR 编排、Step Functions 状态机或 embedding 生成链路。
