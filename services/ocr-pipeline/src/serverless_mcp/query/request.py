"""
EN: Shared request contract helpers for the remote MCP query surface.
CN: 远程 MCP 查询表面的共享请求契约辅助工具。
"""
from __future__ import annotations

from dataclasses import dataclass

from serverless_mcp.core.serialization import coerce_bounded_int, coerce_required_str
from serverless_mcp.runtime.config import Settings, load_settings


@dataclass(frozen=True, slots=True)
class RemoteQueryRequest:
    """
    EN: Normalized query request resolved from MCP parameters and application settings.
    CN: 从 MCP 参数和应用设置解析得到的规范化查询请求。
    """

    query: str
    tenant_id: str
    top_k: int
    neighbor_expand: int
    security_scope: tuple[str, ...] = ()
    doc_type: str | None = None
    key: str | None = None


def build_remote_query_request(
    *,
    query: str,
    tenant_id: str | None,
    request_tenant_id: str | None = None,
    top_k: int,
    neighbor_expand: int,
    doc_type: str | None,
    key: str | None,
    settings: Settings | None = None,
    request_security_scope: tuple[str, ...] | None = None,
) -> RemoteQueryRequest:
    """
    EN: Validate the public query contract before the remote MCP handler calls the core service.
    CN: 在远程 MCP handler 调用核心服务前校验对外查询契约。
    """
    active_settings = settings or load_settings()
    return RemoteQueryRequest(
        query=coerce_required_str(query, field_name="query"),
        tenant_id=_resolve_tenant_id(tenant_id, request_tenant_id=request_tenant_id, settings=active_settings),
        top_k=coerce_bounded_int(top_k, field_name="top_k", minimum=1, maximum=active_settings.query_max_top_k),
        neighbor_expand=coerce_bounded_int(
            neighbor_expand,
            field_name="neighbor_expand",
            minimum=0,
            maximum=active_settings.query_max_neighbor_expand,
        ),
        security_scope=_normalize_security_scope(request_security_scope),
        doc_type=doc_type.strip() if isinstance(doc_type, str) and doc_type.strip() else None,
        key=key.strip() if isinstance(key, str) and key.strip() else None,
    )


def _resolve_tenant_id(tenant_id: str | None, *, request_tenant_id: str | None, settings: Settings) -> str:
    """
    EN: Resolve tenant_id from the explicit request, authenticated claims, or an opt-in anonymous default.
    CN: 从显式请求、已认证声明或显式启用的匿名默认值中解析 tenant_id。

    EN: Anonymous fallback is only honored when allow_unauthenticated_query is true and the configured default is not the shared lookup tenant.
    CN: 只有在 allow_unauthenticated_query 为 true 且配置的默认值不是共享 lookup tenant 时，才接受匿名回退。
    """
    if isinstance(tenant_id, str) and tenant_id.strip():
        return tenant_id.strip()
    if isinstance(request_tenant_id, str) and request_tenant_id.strip():
        return request_tenant_id.strip()
    if settings.allow_unauthenticated_query:
        default_tenant_id = (settings.remote_mcp_default_tenant_id or "").strip()
        if default_tenant_id and default_tenant_id != "lookup":
            return default_tenant_id
    raise ValueError("tenant_id is required")


def _normalize_security_scope(security_scope: tuple[str, ...] | None) -> tuple[str, ...]:
    """
    EN: Normalize security scope values into a stable tuple for query-time authorization.
    CN: 将 security scope 规范化为稳定元组，用于查询时授权。
    """
    if not security_scope:
        return ()
    normalized: list[str] = []
    for item in security_scope:
        if not isinstance(item, str):
            continue
        value = item.strip()
        if not value or value in normalized:
            continue
        normalized.append(value)
    return tuple(normalized)
