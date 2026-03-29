"""
EN: Document-centric helpers for query-side MCP tools.
CN: 查询侧 MCP tools 的文档中心化辅助函数。
"""
from __future__ import annotations

from dataclasses import asdict

from serverless_mcp.core.serialization import coerce_bounded_int, coerce_required_str
from serverless_mcp.mcp_gateway.auth import resolve_effective_tenant_id
from serverless_mcp.mcp_gateway.schemas import dump_json_payload
from serverless_mcp.status.request import build_job_status_request


def get_document_excerpt(
    *,
    context,
    bucket: str,
    key: str,
    version_id: str | None = None,
    tenant_id: str | None = None,
    chunk_id: str | None = None,
    max_chunks: int = 3,
    max_chars: int = 2400,
) -> str:
    """
    EN: Load the manifest-backed excerpt for one document version.
    CN: 为单个文档版本加载基于 manifest 的摘录。
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
    status_payload = context.status_service.build_status(request)
    manifest_summary = status_payload.get("manifest")
    manifest_s3_uri = None
    if isinstance(manifest_summary, dict):
        manifest_s3_uri = manifest_summary.get("manifest_s3_uri")
    if not manifest_s3_uri:
        return dump_json_payload(
            {
                "bucket": bucket,
                "key": key,
                "version_id": version_id,
                "tenant_id": resolved_tenant_id,
                "status": status_payload,
                "excerpt": None,
                "message": "manifest is not available for this document version",
            }
        )

    manifest = context.manifest_repo.load_manifest(manifest_s3_uri)
    selected_chunks = _select_chunks(
        manifest.chunks,
        chunk_id=chunk_id,
        max_chunks=coerce_bounded_int(max_chunks, field_name="max_chunks", minimum=1, maximum=20),
        max_chars=coerce_bounded_int(max_chars, field_name="max_chars", minimum=120, maximum=12000),
    )
    excerpt = "\n\n".join(chunk.text for chunk in selected_chunks)
    payload = {
        "bucket": bucket,
        "key": key,
        "version_id": version_id or status_payload.get("version_id"),
        "tenant_id": resolved_tenant_id,
        "document_id": status_payload.get("lookup", {}).get("object_pk") if isinstance(status_payload.get("lookup"), dict) else None,
        "manifest_s3_uri": manifest_s3_uri,
        "doc_type": manifest.doc_type,
        "status": status_payload,
        "excerpt": excerpt,
        "chunks": [asdict(chunk) for chunk in selected_chunks],
    }
    return dump_json_payload(payload)


def list_document_versions(
    *,
    context,
    bucket: str,
    key: str,
    tenant_id: str | None = None,
    limit: int = 10,
) -> str:
    """
    EN: List known S3 versions for one document and attach current status snapshots.
    CN: 列出单个文档已知的 S3 版本，并附上当前状态快照。
    """
    active_settings = context.settings
    resolved_tenant_id = resolve_effective_tenant_id(
        tenant_id,
        allow_unauthenticated_query=active_settings.allow_unauthenticated_query,
        default_tenant_id=active_settings.remote_mcp_default_tenant_id,
    )
    bounded_limit = coerce_bounded_int(limit, field_name="limit", minimum=1, maximum=25)
    response = context.s3_client.list_object_versions(Bucket=coerce_required_str(bucket, field_name="bucket"), Prefix=coerce_required_str(key, field_name="key"))
    versions: list[dict[str, object]] = []
    for entry in (response.get("Versions") or []):
        if entry.get("Key") != key:
            continue
        version_id = entry.get("VersionId")
        if not isinstance(version_id, str) or not version_id.strip():
            continue
        status_payload = context.status_service.build_status(
            build_job_status_request(
                {
                    "bucket": bucket,
                    "key": key,
                    "version_id": version_id,
                    "tenant_id": resolved_tenant_id,
                }
            )
        )
        versions.append(
            {
                "version_id": version_id,
                "is_latest": bool(entry.get("IsLatest")),
                "is_delete_marker": False,
                "last_modified": _serialize_datetime(entry.get("LastModified")),
                "size": entry.get("Size"),
                "etag": entry.get("ETag"),
                "status": status_payload,
            }
        )
        if len(versions) >= bounded_limit:
            break

    for entry in (response.get("DeleteMarkers") or []):
        if len(versions) >= bounded_limit:
            break
        if entry.get("Key") != key:
            continue
        version_id = entry.get("VersionId")
        if not isinstance(version_id, str) or not version_id.strip():
            continue
        versions.append(
            {
                "version_id": version_id,
                "is_latest": bool(entry.get("IsLatest")),
                "is_delete_marker": True,
                "last_modified": _serialize_datetime(entry.get("LastModified")),
                "status": {
                    "bucket": bucket,
                    "key": key,
                    "version_id": version_id,
                    "tenant_id": resolved_tenant_id,
                    "overall_status": "DELETED",
                },
            }
        )

    payload = {
        "bucket": bucket,
        "key": key,
        "tenant_id": resolved_tenant_id,
        "limit": bounded_limit,
        "versions": versions,
    }
    return dump_json_payload(payload)


def _select_chunks(chunks: list[object], *, chunk_id: str | None, max_chunks: int, max_chars: int) -> list[object]:
    """
    EN: Select a compact excerpt window from manifest chunks.
    CN: 从 manifest chunks 中选择紧凑的摘录窗口。
    """
    if not chunks:
        return []
    if chunk_id:
        for index, chunk in enumerate(chunks):
            if getattr(chunk, "chunk_id", None) == chunk_id:
                return [chunk]
        return []

    selected: list[object] = []
    total_chars = 0
    for chunk in chunks[:max_chunks]:
        text = getattr(chunk, "text", "") or ""
        if not text.strip():
            continue
        if selected and total_chars + len(text) > max_chars:
            break
        selected.append(chunk)
        total_chars += len(text)
    return selected


def _serialize_datetime(value: object) -> str | None:
    """
    EN: Normalize boto3 datetime values into ISO strings.
    CN: 将 boto3 datetime 值规范化为 ISO 字符串。
    """
    iso_method = getattr(value, "isoformat", None)
    if callable(iso_method):
        return iso_method()
    if isinstance(value, str):
        return value
    return None
