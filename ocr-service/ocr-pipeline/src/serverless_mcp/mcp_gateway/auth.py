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
REQUEST_SECURITY_SCOPE: ContextVar[tuple[str, ...]] = ContextVar(
    "mcp_gateway_request_security_scope",
    default=(),
)


def get_request_tenant_id() -> str | None:
    """
    EN: Read the authenticated tenant from the current request context.
    CN: 从当前请求上下文读取已认证 tenant。
    """
    return REQUEST_TENANT_ID.get()


def get_request_security_scope() -> tuple[str, ...]:
    """
    EN: Read the normalized security scope from the current request context.
    CN: 从当前请求上下文读取规范化后的 security scope。
    """
    return REQUEST_SECURITY_SCOPE.get()


@contextmanager
def push_request_context(event: dict[str, Any]) -> Iterator[None]:
    """
    EN: Populate request identity context variables for the duration of one Lambda invocation.
    CN: 在单次 Lambda 调用期间填充请求身份上下文变量。
    """
    token = REQUEST_TENANT_ID.set(extract_request_tenant_id(event))
    scope_token = REQUEST_SECURITY_SCOPE.set(extract_request_security_scope(event))
    try:
        yield
    finally:
        REQUEST_SECURITY_SCOPE.reset(scope_token)
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


def extract_request_security_scope(event: dict[str, Any]) -> tuple[str, ...]:
    """
    EN: Extract and normalize security scope claims from an API Gateway event.
    CN: 从 API Gateway 事件中提取并规范化 security scope 声明。
    """
    authorizer_claims = _extract_authorizer_claims(event)
    if not authorizer_claims:
        return ()

    scope_values: list[str] = []
    for key in ("security_scope", "scope", "scopes", "cognito:groups"):
        scope_values.extend(_coerce_scope_values(authorizer_claims.get(key)))
    return tuple(dict.fromkeys(scope_values))


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


def _coerce_scope_values(value: object) -> list[str]:
    """
    EN: Coerce a scope claim into a deduplicated list of strings.
    CN: 将 scope 声明转换为去重后的字符串列表。
    """
    if isinstance(value, str):
        tokens = [item.strip() for item in value.replace(",", " ").replace(";", " ").split()]
        return [token for token in tokens if token]
    if isinstance(value, (list, tuple, set)):
        values: list[str] = []
        for item in value:
            if isinstance(item, str) and item.strip():
                values.append(item.strip())
        return values
    return []

