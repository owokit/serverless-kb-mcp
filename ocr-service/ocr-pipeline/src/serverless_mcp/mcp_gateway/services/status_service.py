"""
EN: Status query helpers for query-side MCP tools.
CN: 查询侧 MCP tools 的状态查询辅助函数。
"""
from __future__ import annotations

from serverless_mcp.core.serialization import coerce_required_str
from serverless_mcp.mcp_gateway.auth import resolve_effective_tenant_id
from serverless_mcp.mcp_gateway.schemas import dump_json_payload
from serverless_mcp.status.request import build_job_status_request


def get_ingestion_status(
    *,
    context,
    bucket: str,
    key: str,
    version_id: str | None = None,
    tenant_id: str | None = None,
) -> str:
    """
    EN: Return the current ingestion status snapshot for one S3 object version.
    CN: 返回单个 S3 对象版本的当前摄取状态快照。
    """
    active_settings = context.settings
    resolved_tenant_id = resolve_effective_tenant_id(
        tenant_id,
        allow_unauthenticated_query=active_settings.allow_unauthenticated_query,
        default_tenant_id=active_settings.remote_mcp_default_tenant_id,
    )
    request = build_job_status_request(
        {
            "bucket": coerce_required_str(bucket, field_name="bucket"),
            "key": coerce_required_str(key, field_name="key"),
            "version_id": version_id,
            "tenant_id": resolved_tenant_id,
        }
    )
    result = context.status_service.build_status(request)
    return dump_json_payload(result)

