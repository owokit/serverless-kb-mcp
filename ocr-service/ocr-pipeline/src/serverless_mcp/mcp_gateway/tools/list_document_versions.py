"""
EN: Version listing tool for the query-side MCP gateway.
CN: 查询侧 MCP 网关的版本列表 tool。
"""
from __future__ import annotations

from serverless_mcp.mcp_gateway.services.context import get_gateway_context
from serverless_mcp.mcp_gateway.services.document_service import list_document_versions as _list_document_versions


def list_document_versions(
    bucket: str,
    key: str,
    tenant_id: str | None = None,
    limit: int = 10,
) -> str:
    """
    EN: List known source object versions and current status snapshots.
    CN: 列出已知的源对象版本和当前状态快照。

    Args:
        bucket: Source bucket name.
        key: Source object key.
        tenant_id: Optional tenant override.
        limit: Maximum number of versions to return.
    """
    return _list_document_versions(
        context=get_gateway_context(),
        bucket=bucket,
        key=key,
        tenant_id=tenant_id,
        limit=limit,
    )

