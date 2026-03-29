"""
EN: Search tool for the query-side MCP gateway.
CN: 查询侧 MCP 网关的检索 tool。
"""
from __future__ import annotations

from serverless_mcp.mcp_gateway.services.context import get_gateway_context
from serverless_mcp.mcp_gateway.services.retrieval_service import search_documents as _search_documents


def search_documents(
    query: str,
    tenant_id: str | None = None,
    top_k: int = 10,
    neighbor_expand: int = 1,
    doc_type: str | None = None,
    key: str | None = None,
) -> str:
    """
    EN: Search documents through the deployed retrieval pipeline.
    CN: 通过已部署的检索流水线搜索文档。

    Args:
        query: Search query text.
        tenant_id: Optional tenant override.
        top_k: Maximum number of results to return.
        neighbor_expand: Number of neighboring chunks to include around each hit.
        doc_type: Optional document type filter.
        key: Optional exact object key filter.
    """
    return _search_documents(
        context=get_gateway_context(),
        query=query,
        tenant_id=tenant_id,
        top_k=top_k,
        neighbor_expand=neighbor_expand,
        doc_type=doc_type,
        key=key,
    )

