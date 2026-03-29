"""
EN: Lambda entrypoint for the query-side MCP gateway.
CN: 查询侧 MCP 网关的 Lambda 入口。
"""
from __future__ import annotations

import json
from typing import Any

from serverless_mcp.mcp_gateway.auth import push_request_context
from serverless_mcp.mcp_gateway.server import build_discovery_document, get_mcp_handler


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    EN: Dispatch API Gateway requests to the vendored MCP Lambda handler.
    CN: 将 API Gateway 请求分发给 vendored 的 MCP Lambda handler。
    """
    if _is_discovery_get(event):
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json; charset=utf-8",
                "Cache-Control": "no-store",
            },
            "body": json.dumps(build_discovery_document(), ensure_ascii=False),
        }

    with push_request_context(event):
        return get_mcp_handler().handle_request(event, context)


def _is_discovery_get(event: dict[str, Any]) -> bool:
    """
    EN: Detect plain GET probes that should return the discovery document.
    CN: 检测应返回 discovery 文档的普通 GET 探针。
    """
    if not isinstance(event, dict):
        return False
    method = str(event.get("httpMethod") or "").upper()
    if not method:
        request_context = event.get("requestContext")
        if isinstance(request_context, dict):
            http_context = request_context.get("http")
            if isinstance(http_context, dict):
                method = str(http_context.get("method") or "").upper()
    if method != "GET":
        return False
    path = str(event.get("path") or event.get("rawPath") or "")
    return path in {"", "/", "/mcp", "/mcp/"}
