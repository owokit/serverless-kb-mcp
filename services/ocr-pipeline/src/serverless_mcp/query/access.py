"""
EN: Query access-control and validation helpers.
CN: 查询访问控制与校验辅助函数。
"""
from __future__ import annotations

from serverless_mcp.domain.models import EmbeddingProjectionStateRecord, ObjectStateRecord, QueryDegradedProfile


def sanitize_result_metadata(metadata: dict[str, object]) -> dict[str, object]:
    """
    EN: Remove internal fields from result metadata before exposing to clients.
    CN: 在对外返回前移除结果元数据中的内部字段。
    """
    redacted = dict(metadata)
    for field in (
        "tenant_id",
        "bucket",
        "key",
        "manifest_s3_uri",
        "profile_id",
        "provider",
        "model",
        "dimension",
        "security_scope",
        "chunk_id",
    ):
        redacted.pop(field, None)
    return redacted


def metadata_is_truthy(metadata: dict[str, object], field: str) -> bool:
    """
    EN: Interpret a metadata field as a boolean, supporting bool, numeric, and string types.
    CN: 将元数据字段解释为布尔值，支持 bool、数字和字符串类型。
    """
    value = metadata.get(field)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return False


def metadata_security_scope(metadata: dict[str, object]) -> tuple[str, ...]:
    """
    EN: Normalize vector metadata security_scope into a comparable tuple.
    CN: 将向量元数据中的 security_scope 规范化为可比较元组。
    """
    value = metadata.get("security_scope")
    if isinstance(value, str):
        tokens = [item.strip() for item in value.replace(",", " ").replace(";", " ").split()]
        return tuple(dict.fromkeys(token for token in tokens if token))
    if isinstance(value, (list, tuple, set)):
        scopes: list[str] = []
        for item in value:
            if not isinstance(item, str):
                continue
            token = item.strip()
            if token and token not in scopes:
                scopes.append(token)
        return tuple(scopes)
    return ()


def security_scope_allows_access(candidate_scopes: tuple[str, ...], request_scopes: tuple[str, ...]) -> bool:
    """
    EN: Allow public vectors or vectors whose scope intersects the request scope.
    CN: 允许公开向量，或其 scope 与请求 scope 有交集的向量。
    """
    if not candidate_scopes:
        return True
    if not request_scopes:
        return False
    return bool(set(candidate_scopes) & set(request_scopes))


def is_queryable_object_state(
    object_state: ObjectStateRecord | None,
    source,
    *,
    require_global_embed_status: bool,
) -> bool:
    """
    EN: Validate that an object_state record allows query-time retrieval for the given source version.
    CN: 校验 object_state 记录是否允许对给定 source 版本进行查询时读取。
    """
    if object_state is None:
        return False
    if object_state.is_deleted:
        return False
    if object_state.latest_version_id != source.version_id:
        return False
    if object_state.extract_status != "EXTRACTED":
        return False
    if require_global_embed_status and object_state.embed_status != "INDEXED":
        return False
    return True


def is_queryable_projection_state(
    projection_state: EmbeddingProjectionStateRecord | None,
    *,
    require_projection_state: bool,
) -> bool:
    """
    EN: Validate that a per-profile projection state is indexed and queryable.
    CN: 校验按 profile 区分的 projection 状态是否已索引且可查询。
    """
    if projection_state is None:
        return not require_projection_state
    return projection_state.query_status == "INDEXED"


def record_degraded_profile(
    degraded_profiles: list[QueryDegradedProfile],
    degraded_keys: set[tuple[str, str, str | None]],
    *,
    profile_id: str,
    stage: str,
    error: str,
    manifest_s3_uri: str | None = None,
) -> None:
    """
    EN: Record a degraded profile entry with deduplication by (profile_id, stage, manifest_s3_uri).
    CN: 按 (profile_id, stage, manifest_s3_uri) 去重后记录一个 degraded profile 条目。
    """
    key = (profile_id, stage, manifest_s3_uri)
    if key in degraded_keys:
        return
    degraded_keys.add(key)
    degraded_profiles.append(
        QueryDegradedProfile(
            profile_id=profile_id,
            stage=stage,
            error=error,
            manifest_s3_uri=manifest_s3_uri,
        )
    )
