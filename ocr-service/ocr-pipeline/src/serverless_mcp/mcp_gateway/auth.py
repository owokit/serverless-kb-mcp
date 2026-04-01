"""
EN: Request identity helpers for the query-side MCP gateway.
CN: 查询侧 MCP 网关的请求身份辅助工具。
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Iterator

from serverless_mcp.runtime.config import load_settings

REQUEST_TENANT_ID: ContextVar[str | None] = ContextVar("mcp_gateway_request_tenant_id", default=None)


def get_request_tenant_id() -> str | None:
    """
    EN: Read the authenticated tenant from the current request context.
    CN: 从当前请求上下文读取已认证 tenant。
    """
    return REQUEST_TENANT_ID.get()


@contextmanager
def push_request_context(event: dict[str, Any]) -> Iterator[None]:
    """
    EN: Populate request identity context variables for the duration of one Lambda invocation.
    CN: 在单次 Lambda 调用期间填充请求身份上下文变量。
    """
    token = REQUEST_TENANT_ID.set(extract_request_tenant_id(event))
    try:
        yield
    finally:
        REQUEST_TENANT_ID.reset(token)


def extract_request_tenant_id(event: dict[str, Any]) -> str | None:
    """
    EN: Extract the authenticated tenant claim from an API Gateway event.
    CN: 从 API Gateway 事件中提取已认证的 tenant 声明。
    """
    authorizer_claims = _extract_authorizer_claims(event)
    if not authorizer_claims:
        return None
    settings = load_settings()
    claim_value = authorizer_claims.get(settings.query_tenant_claim)
    if isinstance(claim_value, str) and claim_value.strip():
        return claim_value.strip()
    return None


def resolve_effective_tenant_id(
    explicit_tenant_id: str | None,
    *,
    allow_unauthenticated_query: bool,
    default_tenant_id: str | None,
) -> str:
    """
    EN: Resolve the tenant identity for a query-side tool call.
    CN: 为查询侧 tool 调用解析 tenant 身份。
    """
    authenticated_tenant_id = get_request_tenant_id()
    explicit_value = explicit_tenant_id.strip() if isinstance(explicit_tenant_id, str) and explicit_tenant_id.strip() else None

    if authenticated_tenant_id:
        if explicit_value and explicit_value != authenticated_tenant_id:
            raise PermissionError("tenant_id must match the authenticated request tenant")
        return authenticated_tenant_id

    if explicit_value:
        if allow_unauthenticated_query:
            return explicit_value
        raise PermissionError("tenant_id requires an authenticated request or unauthenticated query mode")

    if allow_unauthenticated_query:
        fallback_tenant_id = (default_tenant_id or "").strip()
        if fallback_tenant_id:
            return fallback_tenant_id
    raise ValueError("tenant_id is required")


def _extract_authorizer_claims(event: dict[str, Any]) -> dict[str, Any]:
    """
    EN: Extract authorizer claims from API Gateway REST or HTTP API payloads.
    CN: 从 API Gateway REST 或 HTTP API 载荷中提取 authorizer claims。
    """
    if not isinstance(event, dict):
        return {}
    request_context = event.get("requestContext")
    if not isinstance(request_context, dict):
        return {}
    authorizer = request_context.get("authorizer")
    if not isinstance(authorizer, dict):
        return {}

    claims_sources: list[dict[str, Any]] = []
    jwt = authorizer.get("jwt")
    if isinstance(jwt, dict):
        claims = jwt.get("claims")
        if isinstance(claims, dict):
            claims_sources.append(claims)
    claims = authorizer.get("claims")
    if isinstance(claims, dict):
        claims_sources.append(claims)

    merged: dict[str, Any] = {}
    for source in claims_sources:
        merged.update(source)
    return merged

