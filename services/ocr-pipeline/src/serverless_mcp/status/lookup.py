"""
EN: Lookup helpers for resolving job status state from S3 and DynamoDB.
CN: 用于从 S3 和 DynamoDB 解析 job status 状态的查找辅助函数。
"""
from __future__ import annotations

from typing import Any
from urllib.parse import quote, unquote

from botocore.exceptions import ClientError

from serverless_mcp.domain.models import S3ObjectRef
from serverless_mcp.storage.manifest.repository import ManifestRepository
from serverless_mcp.storage.projection.repository import EmbeddingProjectionStateRepository
from serverless_mcp.storage.state.execution_state_repository import ExecutionStateRepository
from serverless_mcp.storage.state.object_state_repository import ObjectStateLookupRecord, ObjectStateRepository

_MANIFEST_LOAD_FAILURE_TYPES = (ClientError, KeyError, OSError, RuntimeError, TypeError, ValueError)


def build_object_pk(tenant_id: str, bucket: str, key: str) -> str:
    """
    EN: Compose the DynamoDB object primary key from tenant_id, bucket, and key.
    CN: 根据 tenant_id、bucket 和 key 拼接 DynamoDB 对象主键。
    """
    return f"{quote(tenant_id, safe='')}#{quote(bucket, safe='')}#{quote(key, safe='')}"


def tenant_id_from_object_pk(object_pk: str) -> str:
    """
    EN: Decode the tenant segment from an escaped composite object primary key.
    CN: 从转义后的复合对象主键中解码 tenant 片段。
    """
    if not object_pk:
        return ""
    tenant_segment = object_pk.split("#", 1)[0]
    return unquote(tenant_segment)


def build_source_ref(*, tenant_id: str, bucket: str, key: str, version_id: str, sequencer: str | None) -> S3ObjectRef:
    """
    EN: Build an S3ObjectRef from tenant, bucket, key, version_id, and sequencer.
    CN: 根据 tenant、bucket、key、version_id 和 sequencer 构建 S3ObjectRef。
    """
    return S3ObjectRef(
        tenant_id=tenant_id,
        bucket=bucket,
        key=key,
        version_id=version_id,
        sequencer=sequencer,
    )


def load_execution_state(
    *,
    object_pk: str,
    execution_state_repo: ExecutionStateRepository | None,
    object_state_repo: ObjectStateRepository,
) -> object | None:
    """
    EN: Load execution-state first, then fall back to object_state when the dedicated table is unavailable.
    CN: 优先读取 execution-state，若专用表不可用则回退到 object_state。
    """
    if execution_state_repo is not None:
        return execution_state_repo.get_state(object_pk=object_pk)
    return object_state_repo.get_state(object_pk=object_pk)


def resolve_lookup(
    *,
    bucket: str,
    key: str,
    tenant_id: str | None,
    object_state_repo: ObjectStateRepository,
    execution_state_repo: ExecutionStateRepository | None,
) -> ObjectStateLookupRecord | None:
    """
    EN: Resolve a lookup record by bucket/key, falling back to object_state when tenant_id is available.
    CN: 按 bucket/key 解析 lookup 记录，在存在 tenant_id 时回退到 object_state。
    """
    lookup = object_state_repo.get_lookup_record(bucket=bucket, key=key)
    if lookup is not None or not tenant_id:
        return lookup

    object_pk = build_object_pk(tenant_id, bucket, key)
    current_state = load_execution_state(
        object_pk=object_pk,
        execution_state_repo=execution_state_repo,
        object_state_repo=object_state_repo,
    )
    if current_state is None:
        return None
    return ObjectStateLookupRecord(
        pk=f"lookup-v2#{quote(bucket, safe='')}#{quote(key, safe='')}",
        object_pk=object_pk,
        tenant_id=tenant_id,
        bucket=bucket,
        key=key,
        latest_version_id=getattr(current_state, "latest_version_id", None),
        latest_sequencer=getattr(current_state, "latest_sequencer", None),
        latest_manifest_s3_uri=getattr(current_state, "latest_manifest_s3_uri", None),
        is_deleted=getattr(current_state, "is_deleted", False),
        updated_at=getattr(current_state, "updated_at", None),
    )


def head_source_object(
    *,
    s3_client: object,
    bucket: str,
    key: str,
    version_id: str | None,
    lookup: ObjectStateLookupRecord | None,
) -> dict[str, Any] | None:
    """
    EN: HEAD the source S3 object to fetch metadata; return None if the object is not found.
    CN: HEAD 源 S3 对象以获取元数据；若对象不存在则返回 None。
    """
    kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key}
    if version_id:
        kwargs["VersionId"] = version_id
    elif lookup is not None:
        kwargs["VersionId"] = lookup.latest_version_id
    try:
        return s3_client.head_object(**kwargs)
    except ClientError as exc:
        if _is_not_found_error(exc):
            return None
        raise


def resolve_latest_version_id(
    *,
    lookup: ObjectStateLookupRecord | None,
    current_state: object | None,
    source_info: dict[str, Any] | None,
) -> str | None:
    """
    EN: Determine the latest version_id from lookup, current_state, or S3 HEAD response.
    CN: 从 lookup、current_state 或 S3 HEAD 响应中确定最新 version_id。
    """
    if lookup is not None:
        if current_state is not None:
            return getattr(current_state, "latest_version_id", None) or lookup.latest_version_id
        return lookup.latest_version_id
    if source_info is not None:
        version_id = source_info.get("version_id") or source_info.get("VersionId")
        if isinstance(version_id, str) and version_id.strip():
            return version_id.strip()
    return None


def resolve_manifest_uri(
    *,
    source_ref: S3ObjectRef,
    requested_version_id: str | None,
    lookup: ObjectStateLookupRecord | None,
    current_state: object | None,
    manifest_repo: ManifestRepository | None,
) -> str | None:
    """
    EN: Resolve the manifest S3 URI from state, lookup, or by building a fallback URI.
    CN: 从状态、lookup 或回退规则中解析 manifest S3 URI。
    """
    candidate = None
    if current_state is not None:
        candidate = getattr(current_state, "latest_manifest_s3_uri", None)
    if not candidate and lookup is not None and (
        not requested_version_id or requested_version_id == lookup.latest_version_id
    ):
        candidate = lookup.latest_manifest_s3_uri
    if candidate:
        return candidate
    if manifest_repo is None or not requested_version_id:
        return None
    return manifest_repo.build_manifest_s3_uri(source=source_ref, version_id=requested_version_id)


def load_manifest_summary(
    *,
    manifest_s3_uri: str | None,
    manifest_repo: ManifestRepository | None,
) -> dict[str, Any] | None:
    """
    EN: Load manifest from S3 and build a compact summary with chunk and asset counts.
    CN: 从 S3 加载 manifest，并生成包含 chunk 与 asset 数量的精简摘要。
    """
    if not manifest_s3_uri or manifest_repo is None:
        return None
    try:
        manifest = manifest_repo.load_manifest(manifest_s3_uri)
    except _MANIFEST_LOAD_FAILURE_TYPES as exc:
        return {
            "manifest_s3_uri": manifest_s3_uri,
            "chunk_count": 0,
            "asset_count": 0,
            "embedding_item_count": 0,
            "load_failed": True,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        }
    return {
        "manifest_s3_uri": manifest_s3_uri,
        "chunk_count": len(manifest.chunks),
        "asset_count": len(manifest.assets),
        "embedding_item_count": len(manifest.chunks) + len(manifest.assets),
    }


def load_projection_records(
    *,
    object_pk: str,
    version_id: str | None,
    projection_state_repo: EmbeddingProjectionStateRepository | None,
) -> list[object]:
    """
    EN: Load all embedding projection state records for a given object version.
    CN: 加载给定对象版本的全部 embedding projection 状态记录。
    """
    if projection_state_repo is None or not object_pk or not version_id:
        return []
    return list(projection_state_repo.list_version_records(object_pk=object_pk, version_id=version_id))


def tenant_matches_request(
    requested_tenant_id: str,
    lookup: ObjectStateLookupRecord | None,
    current_state: object | None,
) -> bool:
    """
    EN: Verify that the lookup or state record matches the requested tenant_id.
    CN: 验证 lookup 或状态记录是否匹配请求的 tenant_id。
    """
    if lookup is not None and lookup.tenant_id != requested_tenant_id:
        return False
    if current_state is not None:
        current_tenant_id = tenant_id_from_object_pk(str(getattr(current_state, "pk", "")))
        if current_tenant_id and current_tenant_id != requested_tenant_id:
            return False
    return lookup is not None or current_state is not None


def _is_not_found_error(exc: ClientError) -> bool:
    """
    EN: Check whether a ClientError indicates a not-found condition.
    CN: 检查 ClientError 是否表示未找到对象。
    """
    code = str(exc.response.get("Error", {}).get("Code", ""))
    return code in {"404", "NotFound", "NoSuchKey", "NoSuchBucket", "ResourceNotFoundException"}
