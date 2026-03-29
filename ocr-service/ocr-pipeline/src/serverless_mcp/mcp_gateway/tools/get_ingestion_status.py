"""
EN: Status tool for the query-side MCP gateway.
CN: 查询侧 MCP 网关的状态 tool。
"""
from __future__ import annotations

from serverless_mcp.mcp_gateway.services.context import get_gateway_context
from serverless_mcp.mcp_gateway.services.status_service import get_ingestion_status as _get_ingestion_status


def get_ingestion_status(
    bucket: str,
    key: str,
    version_id: str | None = None,
    tenant_id: str | None = None,
) -> str:
    """
    EN: Return the ingestion status snapshot for one S3 object version.
    CN: 返回单个 S3 对象版本的摄取状态快照。

    Args:
        bucket: Source bucket name.
        key: Source object key.
        version_id: Optional object version identifier.
        tenant_id: Optional tenant override.
    """
    return _get_ingestion_status(
        context=get_gateway_context(),
        bucket=bucket,
        key=key,
        version_id=version_id,
        tenant_id=tenant_id,
    )

