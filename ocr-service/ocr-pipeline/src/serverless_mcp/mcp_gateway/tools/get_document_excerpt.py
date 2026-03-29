"""
EN: Document excerpt tool for the query-side MCP gateway.
CN: 查询侧 MCP 网关的文档摘录 tool。
"""
from __future__ import annotations

from serverless_mcp.mcp_gateway.services.context import get_gateway_context
from serverless_mcp.mcp_gateway.services.document_service import get_document_excerpt as _get_document_excerpt


def get_document_excerpt(
    bucket: str,
    key: str,
    version_id: str | None = None,
    tenant_id: str | None = None,
    chunk_id: str | None = None,
    max_chunks: int = 3,
    max_chars: int = 2400,
) -> str:
    """
    EN: Return a manifest-backed excerpt for one document version.
    CN: 返回单个文档版本的 manifest 摘录。

    Args:
        bucket: Source bucket name.
        key: Source object key.
        version_id: Optional object version identifier.
        tenant_id: Optional tenant override.
        chunk_id: Optional chunk identifier to target one excerpt.
        max_chunks: Maximum number of chunks to include when chunk_id is omitted.
        max_chars: Maximum total excerpt length when chunk_id is omitted.
    """
    return _get_document_excerpt(
        context=get_gateway_context(),
        bucket=bucket,
        key=key,
        version_id=version_id,
        tenant_id=tenant_id,
        chunk_id=chunk_id,
        max_chunks=max_chunks,
        max_chars=max_chars,
    )

