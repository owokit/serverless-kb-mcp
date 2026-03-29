"""
EN: Retrieval service helpers for query-side MCP tools.
CN: 查询侧 MCP tools 的检索服务辅助函数。
"""
from __future__ import annotations

from serverless_mcp.core.serialization import coerce_bounded_int, coerce_required_str, serialize_query_response
from serverless_mcp.mcp_gateway.auth import get_request_security_scope, resolve_effective_tenant_id
from serverless_mcp.mcp_gateway.schemas import dump_json_payload


def search_documents(
    *,
    context,
    query: str,
    tenant_id: str | None = None,
    top_k: int = 10,
    neighbor_expand: int = 1,
    doc_type: str | None = None,
    key: str | None = None,
) -> str:
    """
    EN: Execute semantic retrieval and serialize the ranked response for MCP clients.
    CN: 执行语义检索，并将排序结果序列化给 MCP 客户端。
    """
    active_settings = context.settings
    query_text = coerce_required_str(query, field_name="query")
    resolved_tenant_id = resolve_effective_tenant_id(
        tenant_id,
        allow_unauthenticated_query=active_settings.allow_unauthenticated_query,
        default_tenant_id=active_settings.remote_mcp_default_tenant_id,
    )
    bounded_top_k = coerce_bounded_int(
        top_k,
        field_name="top_k",
        minimum=1,
        maximum=active_settings.query_max_top_k,
    )
    bounded_neighbor_expand = coerce_bounded_int(
        neighbor_expand,
        field_name="neighbor_expand",
        minimum=0,
        maximum=active_settings.query_max_neighbor_expand,
    )
    response = context.query_service.search(
        query=query_text,
        tenant_id=resolved_tenant_id,
        top_k=bounded_top_k,
        neighbor_expand=bounded_neighbor_expand,
        security_scope=get_request_security_scope(),
        doc_type=doc_type,
        key=key,
    )
    payload = serialize_query_response(
        response,
        delivery_resolver=(
            lambda source: _serialize_delivery(context.delivery_service.deliver_source_document(source))
            if context.delivery_service is not None
            else None
        ),
    )
    return dump_json_payload(payload)


def _serialize_delivery(delivery: object) -> dict[str, object]:
    """
    EN: Normalize a delivery object into JSON-safe data.
    CN: 将交付对象规范化为 JSON 安全数据。
    """
    return {
        "url": getattr(delivery, "url"),
        "expires_at": getattr(delivery, "expires_at"),
    }

