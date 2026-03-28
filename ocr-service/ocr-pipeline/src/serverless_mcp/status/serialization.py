"""
EN: Serialization helpers for job status responses and state records.
CN: 用于 job status 响应与状态记录的序列化辅助函数。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from serverless_mcp.storage.state.object_state_repository import ObjectStateLookupRecord


def build_missing_response(*, bucket: str, key: str, version_id: str | None, tenant_id: str | None) -> dict[str, Any]:
    """
    EN: Build a NOT_FOUND status response with all stages at zero progress.
    CN: 构建 NOT_FOUND 状态响应，所有阶段进度均为零。
    """
    return {
        "job_id": version_id,
        "tenant_id": tenant_id,
        "bucket": bucket,
        "key": key,
        "version_id": version_id,
        "object_pk": None,
        "is_latest_version": False,
        "overall_status": "NOT_FOUND",
        "current_stage": "source",
        "progress_percent": 0,
        "updated_at": None,
        "source": None,
        "lookup": None,
        "object_state": None,
        "manifest": None,
        "profiles": [],
        "stages": [
            {"name": "source", "status": "NOT_FOUND", "progress_percent": 0},
            {"name": "ingest", "status": "PENDING", "progress_percent": 0},
            {"name": "extract", "status": "PENDING", "progress_percent": 0},
            {"name": "manifest", "status": "PENDING", "progress_percent": 0},
            {"name": "embedding", "status": "PENDING", "progress_percent": 0},
        ],
    }


def serialize_lookup(lookup: ObjectStateLookupRecord) -> dict[str, Any]:
    """
    EN: Serialize a lookup record into a JSON-safe dictionary.
    CN: 将 lookup 记录序列化为 JSON 安全字典。
    """
    return {
        "pk": lookup.pk,
        "object_pk": lookup.object_pk,
        "tenant_id": lookup.tenant_id,
        "bucket": lookup.bucket,
        "key": lookup.key,
        "latest_version_id": lookup.latest_version_id,
        "latest_sequencer": lookup.latest_sequencer,
        "latest_manifest_s3_uri": lookup.latest_manifest_s3_uri,
        "is_deleted": lookup.is_deleted,
        "updated_at": lookup.updated_at,
    }


def serialize_object_state(current_state: object) -> dict[str, Any]:
    """
    EN: Serialize an object_state record into a JSON-safe dictionary.
    CN: 将 object_state 记录序列化为 JSON 安全字典。
    """
    return {
        "pk": getattr(current_state, "pk", None),
        "latest_version_id": getattr(current_state, "latest_version_id", None),
        "latest_sequencer": getattr(current_state, "latest_sequencer", None),
        "extract_status": getattr(current_state, "extract_status", None),
        "embed_status": getattr(current_state, "embed_status", None),
        "previous_version_id": getattr(current_state, "previous_version_id", None),
        "previous_manifest_s3_uri": getattr(current_state, "previous_manifest_s3_uri", None),
        "latest_manifest_s3_uri": getattr(current_state, "latest_manifest_s3_uri", None),
        "is_deleted": getattr(current_state, "is_deleted", None),
        "last_error": getattr(current_state, "last_error", None),
        "updated_at": getattr(current_state, "updated_at", None),
    }


def serialize_projection_record(record: Any) -> dict[str, Any]:
    """
    EN: Serialize an embedding projection state record into a JSON-safe dictionary.
    CN: 将 embedding projection 状态记录序列化为 JSON 安全字典。
    """
    return {
        "pk": getattr(record, "pk", None),
        "sk": getattr(record, "sk", None),
        "object_pk": getattr(record, "object_pk", None),
        "version_id": getattr(record, "version_id", None),
        "profile_id": getattr(record, "profile_id", None),
        "provider": getattr(record, "provider", None),
        "model": getattr(record, "model", None),
        "dimension": getattr(record, "dimension", None),
        "write_status": getattr(record, "write_status", None),
        "query_status": getattr(record, "query_status", None),
        "manifest_s3_uri": getattr(record, "manifest_s3_uri", None),
        "vector_bucket_name": getattr(record, "vector_bucket_name", None),
        "vector_index_name": getattr(record, "vector_index_name", None),
        "vector_count": getattr(record, "vector_count", None),
        "last_error": getattr(record, "last_error", None),
        "updated_at": getattr(record, "updated_at", None),
    }


def serialize_source_info(source_info: dict[str, Any]) -> dict[str, Any]:
    """
    EN: Serialize S3 HEAD response into a compact source info dictionary.
    CN: 将 S3 HEAD 响应序列化为精简来源信息字典。
    """
    last_modified = source_info.get("LastModified")
    if isinstance(last_modified, datetime):
        last_modified_value: str | None = last_modified.isoformat()
    elif isinstance(last_modified, str):
        last_modified_value = last_modified
    else:
        last_modified_value = None
    return {
        "version_id": source_info.get("VersionId"),
        "content_length": source_info.get("ContentLength"),
        "content_type": source_info.get("ContentType"),
        "etag": source_info.get("ETag"),
        "last_modified": last_modified_value,
        "storage_class": source_info.get("StorageClass"),
    }
