"""
EN: Remote MCP Lambda handler that exposes the document search tool over streamable HTTP.
CN: 通过 streamable HTTP 暴露文档搜索工具的远程 MCP Lambda 处理器。
"""
from __future__ import annotations

import json
import os
from copy import copy
from contextvars import ContextVar
from functools import lru_cache
from typing import Any

from aws_lambda_powertools import Logger
from mcp import ErrorData, McpError
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import LATEST_PROTOCOL_VERSION

try:
    from mangum import Mangum
except ImportError:  # pragma: no cover - deployment layer provides Mangum.
    class Mangum:  # type: ignore[no-redef]
        """
        EN: Lightweight fallback used only when Mangum is unavailable in local test environments.
        CN: 仅在本地测试环境缺少 Mangum 时使用的轻量级回退实现。
        """

        def __init__(self, app) -> None:
            self._app = app

        def __call__(self, event: dict, context: Any) -> dict:
            raise RuntimeError("Mangum is required to run the remote MCP Lambda handler")

from serverless_mcp.core.serialization import serialize_query_response
from serverless_mcp.query.request import TenantIdConflictError, build_remote_query_request
from serverless_mcp.runtime.config import load_settings
from serverless_mcp.runtime.delivery import build_cloudfront_delivery_service
from serverless_mcp.runtime.query_runtime import build_query_service

_logger = Logger(service=os.environ.get("POWERTOOLS_SERVICE_NAME", "serverless-mcp-service"))
_REQUEST_TENANT_ID: ContextVar[str | None] = ContextVar("remote_mcp_request_tenant_id", default=None)
_REQUEST_SECURITY_SCOPE: ContextVar[tuple[str, ...]] = ContextVar("remote_mcp_request_security_scope", default=())


@lru_cache(maxsize=1)
def _build_service():
    """
    EN: Build the retrieval service used by the remote MCP server.
    CN: 构建远程 MCP 服务使用的检索服务。
    """
    return build_query_service()


@lru_cache(maxsize=1)
def _build_delivery_service():
    """
    EN: Build the optional CloudFront delivery helper for source document URLs.
    CN: 构建可选的 CloudFront 源文档交付辅助工具。
    """
    return build_cloudfront_delivery_service()


def search_documents(
    query: str,
    tenant_id: str | None = None,
    top_k: int = 10,
    neighbor_expand: int = 1,
    doc_type: str | None = None,
    key: str | None = None,
) -> dict[str, Any]:
    """
    EN: Execute semantic search and return structured retrieval results for MCP clients.
    CN: 执行语义搜索，并向 MCP 客户端返回结构化检索结果。
    """
    settings = load_settings()
    request_tenant_id = _REQUEST_TENANT_ID.get()
    request_security_scope = _REQUEST_SECURITY_SCOPE.get()
    try:
        request = build_remote_query_request(
            query=query,
            tenant_id=tenant_id,
            request_tenant_id=request_tenant_id,
            request_security_scope=request_security_scope,
            top_k=top_k,
            neighbor_expand=neighbor_expand,
            doc_type=doc_type,
            key=key,
            settings=settings,
        )
    except (PermissionError, TenantIdConflictError) as exc:
        raise McpError(ErrorData(code=403, message=str(exc))) from exc

    response = _build_service().search(
        query=request.query,
        tenant_id=request.tenant_id,
        top_k=request.top_k,
        neighbor_expand=request.neighbor_expand,
        security_scope=request.security_scope,
        doc_type=request.doc_type,
        key=request.key,
    )
    delivery_service = _build_delivery_service()
    return serialize_query_response(
        response,
        delivery_resolver=(
            lambda source: _serialize_delivery(delivery_service.deliver_source_document(source))
            if delivery_service
            else None
        ),
    )


def _serialize_delivery(delivery: object) -> dict[str, Any]:
    """
    EN: Normalize the delivery object into a JSON-serializable dictionary.
    CN: 将交付对象规范化为可 JSON 序列化的字典。
    """
    return {
        "url": getattr(delivery, "url"),
        "expires_at": getattr(delivery, "expires_at"),
    }


@lru_cache(maxsize=1)
def _build_mcp_server() -> FastMCP:
    """
    EN: Build and cache the official FastMCP server for warm Lambda reuse.
    CN: 构建并缓存官方 FastMCP 服务器，供热启动 Lambda 复用。
    """
    mcp = FastMCP(
        "mcp-doc-pipeline",
        instructions=(
            "Semantic document retrieval over the internal S3 to S3 Vectors pipeline. "
            "Use the search_documents tool and pass tenant_id for anonymous access."
        ),
        json_response=True,
        stateless_http=True,
        streamable_http_path="/mcp",
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
    )
    mcp.tool(
        name="search_documents",
        description="Search documents through the deployed retrieval pipeline.",
    )(search_documents)
    return mcp


@lru_cache(maxsize=1)
def _build_asgi_handler():
    """
    EN: Build and cache the ASGI adapter so warm Lambda invocations reuse the Mangum wrapper.
    CN: 构建并缓存 ASGI 适配器，使热启动 Lambda 调用复用同一个 Mangum 包装器。
    """
    return Mangum(_McpGatewayASGIApp(_build_mcp_server().streamable_http_app()))


class _McpGatewayASGIApp:
    """
    EN: Return a discovery document for plain browser probes and forward real MCP traffic.
    CN: 为普通浏览器探针返回 discovery 文档，并转发真实的 MCP 流量。
    """

    def __init__(self, app) -> None:
        self._app = app

    async def __call__(self, scope, receive, send) -> None:
        if _should_return_mcp_discovery(scope):
            await _send_json_response(send, _build_mcp_discovery_document())
            return
        normalized_scope = _normalize_mcp_scope(scope)
        token = _REQUEST_TENANT_ID.set(_extract_request_tenant_id(normalized_scope))
        scope_token = _REQUEST_SECURITY_SCOPE.set(_extract_request_security_scope(normalized_scope))
        try:
            await self._app(normalized_scope, receive, send)
        finally:
            _REQUEST_SECURITY_SCOPE.reset(scope_token)
            _REQUEST_TENANT_ID.reset(token)


def _normalize_mcp_scope(scope: dict[str, Any]) -> dict[str, Any]:
    """
    EN: Normalize Lambda URL root requests so clients can use the base URL directly.
    CN: 规范化 Lambda URL 根请求，方便客户端直接使用基础 URL。
    """
    path = str(scope.get("path") or "")
    if path not in {"", "/", "/mcp/"}:
        return scope
    normalized_scope = copy(scope)
    normalized_scope["path"] = "/mcp"
    normalized_scope["raw_path"] = b"/mcp"
    return normalized_scope


def _should_return_mcp_discovery(scope: dict[str, Any]) -> bool:
    """
    EN: Detect plain GET probes that are not requesting the MCP streamable transport.
    CN: 检测未请求 MCP streamable 传输的普通 GET 探针。
    """
    if str(scope.get("type") or "") != "http":
        return False
    if str(scope.get("method") or "").upper() != "GET":
        return False
    path = str(scope.get("path") or "")
    if path not in {"", "/", "/mcp", "/mcp/"}:
        return False
    accept = _header_value(scope, b"accept").lower()
    return "text/event-stream" not in accept


def _header_value(scope: dict[str, Any], header_name: bytes) -> str:
    """
    EN: Read a header value from an ASGI scope using case-insensitive byte matching.
    CN: 通过大小写不敏感的字节匹配从 ASGI scope 读取 header 值。
    """
    target = header_name.lower()
    for key, value in scope.get("headers") or []:
        if key.lower() == target:
            return value.decode("latin-1")
    return ""


def _extract_request_tenant_id(scope: dict[str, Any]) -> str | None:
    """
    EN: Extract an authenticated tenant claim from the AWS event attached to the ASGI scope.
    CN: 从附着在 ASGI scope 上的 AWS event 中提取已认证的 tenant 声明。
    """
    event = scope.get("aws.event") if isinstance(scope, dict) else None
    if not isinstance(event, dict):
        return None
    request_context = event.get("requestContext")
    if not isinstance(request_context, dict):
        return None
    authorizer = request_context.get("authorizer")
    if not isinstance(authorizer, dict):
        return None

    settings = load_settings()
    claim_sources: list[dict[str, Any]] = []
    jwt = authorizer.get("jwt")
    if isinstance(jwt, dict):
        claims = jwt.get("claims")
        if isinstance(claims, dict):
            claim_sources.append(claims)
    claims = authorizer.get("claims")
    if isinstance(claims, dict):
        claim_sources.append(claims)

    for source in claim_sources:
        claim_value = source.get(settings.query_tenant_claim)
        if isinstance(claim_value, str) and claim_value.strip():
            return claim_value.strip()
    return None


def _extract_request_security_scope(scope: dict[str, Any]) -> tuple[str, ...]:
    """
    EN: Extract a normalized security scope tuple from the AWS event attached to the ASGI scope.
    CN: 从附着在 ASGI scope 上的 AWS event 中提取规范化的 security scope 元组。
    """
    event = scope.get("aws.event") if isinstance(scope, dict) else None
    if not isinstance(event, dict):
        return ()
    request_context = event.get("requestContext")
    if not isinstance(request_context, dict):
        return ()
    authorizer = request_context.get("authorizer")
    if not isinstance(authorizer, dict):
        return ()

    claim_sources: list[dict[str, Any]] = []
    jwt = authorizer.get("jwt")
    if isinstance(jwt, dict):
        claims = jwt.get("claims")
        if isinstance(claims, dict):
            claim_sources.append(claims)
    claims = authorizer.get("claims")
    if isinstance(claims, dict):
        claim_sources.append(claims)

    scope_values: list[str] = []
    for source in claim_sources:
        for key in ("security_scope", "scope", "scopes", "cognito:groups"):
            value = source.get(key)
            scope_values.extend(_coerce_scope_values(value))
    return tuple(dict.fromkeys(scope_values))


def _coerce_scope_values(value: object) -> list[str]:
    """
    EN: Coerce a scope claim into a list of unique string scopes.
    CN: 将 scope 声明转换为唯一字符串 scope 列表。
    """
    if isinstance(value, str):
        tokens = [item.strip() for item in value.replace(",", " ").replace(";", " ").split()]
        return [token for token in tokens if token]
    if isinstance(value, (list, tuple, set)):
        scopes: list[str] = []
        for item in value:
            if not isinstance(item, str):
                continue
            token = item.strip()
            if token:
                scopes.append(token)
        return scopes
    return []


def _build_mcp_discovery_document() -> dict[str, Any]:
    """
    EN: Build a compact discovery document for humans and generic HTTP probes.
    CN: 为人类和通用 HTTP 探针构建简洁的 discovery 文档。
    """
    return {
        "protocolVersion": LATEST_PROTOCOL_VERSION,
        "serverInfo": {
            "name": "mcp-doc-pipeline",
            "version": "unknown",
        },
        "capabilities": {
            "tools": {
                "listChanged": False,
            },
        },
        "transport": "streamable-http",
        "endpoint": "/mcp",
        "tools": [
            {
                "name": "search_documents",
                "description": "Search documents through the deployed retrieval pipeline.",
            }
        ],
        "instructions": "Semantic document retrieval over the internal S3 to S3 Vectors pipeline.",
    }


async def _send_json_response(send, payload: dict[str, Any]) -> None:
    """
    EN: Send a compact JSON response without relying on the MCP transport layer.
    CN: 不依赖 MCP 传输层发送精简 JSON 响应。
    """
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = [
        (b"content-type", b"application/json; charset=utf-8"),
        (b"cache-control", b"no-store"),
        (b"x-content-type-options", b"nosniff"),
    ]
    await send({"type": "http.response.start", "status": 200, "headers": headers})
    await send({"type": "http.response.body", "body": body})


def lambda_handler(event: dict, context: Any) -> dict:
    """
    EN: Adapt Lambda Function URL HTTP events onto the official FastMCP streamable HTTP app.
    CN: 将 Lambda Function URL 的 HTTP 事件适配到官方 FastMCP streamable HTTP 应用。
    """
    return _build_asgi_handler()(event, context)
