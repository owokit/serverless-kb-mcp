"""
EN: Thin Lambda wrapper that exposes the query-side MCP gateway handler.
CN: 暴露查询侧 MCP 网关 handler 的最薄 Lambda wrapper。
"""
from __future__ import annotations

from serverless_mcp.mcp_gateway.handler import lambda_handler as gateway_lambda_handler

lambda_handler = gateway_lambda_handler
