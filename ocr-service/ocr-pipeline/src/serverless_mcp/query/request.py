"""
EN: Shared request contract helpers for the remote MCP query surface.
CN: 远程 MCP 查询表面的共享请求契约辅助工具。
"""
from __future__ import annotations

from dataclasses import dataclass

from serverless_mcp.core.serialization import coerce_bounded_int, coerce_required_str
from serverless_mcp.runtime.config import Settings, load_settings


class TenantIdConflictError(PermissionError):
    """
    EN: Raised when an authenticated tenant disagrees with a caller-supplied tenant_id.
    CN: 当已认证租户与调用方显式传入的 tenant_id 不一致时抛出。
    """


@dataclass(frozen=True, slots=True)
class RemoteQueryRequest:
    """
    EN: Normalized query request resolved from MCP parameters and application settings.
    CN: 由 MCP 参数和应用设置解析得到的规范化查询请求。
    """

    query: str
    tenant_id: str
    top_k: int
    neighbor_expand: int
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
        doc_type=doc_type.strip() if isinstance(doc_type, str) and doc_type.strip() else None,
        key=key.strip() if isinstance(key, str) and key.strip() else None,
    )


def _resolve_tenant_id(tenant_id: str | None, *, request_tenant_id: str | None, settings: Settings) -> str:
    """
    EN: Bind the query tenant to the authenticated claim when present and only permit matching caller overrides.
    CN: 当认证声明存在时，将查询租户绑定到认证租户，并且只允许相同的调用方覆盖值。

    EN: Anonymous fallback is only honored when unauthenticated queries are explicitly enabled and the configured default is not the shared lookup tenant.
    CN: 只有在显式启用匿名查询且配置的默认值不是共享 lookup tenant 时，才接受匿名回退。
    """
    explicit_tenant_id = tenant_id.strip() if isinstance(tenant_id, str) and tenant_id.strip() else None
    authenticated_tenant_id = (
        request_tenant_id.strip() if isinstance(request_tenant_id, str) and request_tenant_id.strip() else None
    )

    if authenticated_tenant_id:
        if explicit_tenant_id and explicit_tenant_id != authenticated_tenant_id:
            raise TenantIdConflictError("tenant_id must match the authenticated request tenant")
        return authenticated_tenant_id

    if explicit_tenant_id:
        if settings.allow_unauthenticated_query:
            return explicit_tenant_id
        raise PermissionError("tenant_id requires an authenticated request or unauthenticated query mode")

    if settings.allow_unauthenticated_query:
        default_tenant_id = (settings.remote_mcp_default_tenant_id or "").strip()
        if default_tenant_id and default_tenant_id != "lookup":
            return default_tenant_id
    raise ValueError("tenant_id is required")
