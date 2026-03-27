"""
EN: Job status aggregation service combining S3, DynamoDB, and manifest state into a read-only view.
CN: 将 S3、DynamoDB 和 manifest 状态聚合为只读视图的 job status 服务。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from serverless_mcp.runtime.config import Settings
from serverless_mcp.storage.manifest.repository import ManifestRepository
from serverless_mcp.storage.projection.repository import EmbeddingProjectionStateRepository
from serverless_mcp.storage.state.execution_state_repository import ExecutionStateRepository
from serverless_mcp.storage.state.object_state_repository import ObjectStateRepository
from serverless_mcp.status.lookup import (
    build_object_pk,
    build_source_ref,
    head_source_object,
    load_execution_state,
    load_manifest_summary,
    load_projection_records,
    resolve_latest_version_id,
    resolve_lookup,
    resolve_manifest_uri,
    tenant_id_from_object_pk,
    tenant_matches_request,
)
from serverless_mcp.status.mapping import (
    build_profile_rows,
    build_stage_rows,
    latest_timestamp,
    resolve_current_stage,
    resolve_overall_status,
    resolve_progress_percent,
)
from serverless_mcp.status.serialization import (
    build_missing_response,
    serialize_lookup,
    serialize_object_state,
    serialize_projection_record,
    serialize_source_info,
)


@dataclass(frozen=True, slots=True)
class JobStatusRequest:
    """
    EN: Minimal identity used to resolve one document job status.
    CN: 用于解析单个文档任务状态的最小身份信息。
    """

    bucket: str
    key: str
    version_id: str | None = None
    tenant_id: str | None = None


class JobStatusService:
    """
    EN: Aggregate S3, DynamoDB, and manifest state into one read-only status view.
    CN: 将 S3、DynamoDB 和 manifest 状态聚合为一个只读状态视图。
    """

    def __init__(
        self,
        *,
        settings: Settings,
        s3_client: object,
        object_state_repo: ObjectStateRepository,
        execution_state_repo: ExecutionStateRepository | None = None,
        projection_state_repo: EmbeddingProjectionStateRepository | None = None,
        manifest_repo: ManifestRepository | None = None,
    ) -> None:
        self._settings = settings
        self._s3 = s3_client
        self._object_state_repo = object_state_repo
        self._execution_state_repo = execution_state_repo
        self._projection_state_repo = projection_state_repo
        self._manifest_repo = manifest_repo

    @classmethod
    def from_settings(cls, settings: Settings, *, s3_client: object, dynamodb_client: object) -> "JobStatusService":
        """
        EN: Build the status service from environment-derived settings.
        CN: 从环境推导的 settings 构建状态服务。
        """
        object_state_repo = ObjectStateRepository(
            table_name=settings.object_state_table,
            dynamodb_client=dynamodb_client,
        )
        execution_state_repo = None
        if settings.execution_state_table:
            execution_state_repo = ExecutionStateRepository(
                table_name=settings.execution_state_table,
                dynamodb_client=dynamodb_client,
            )
        projection_state_repo = None
        if settings.embedding_projection_state_table:
            projection_state_repo = EmbeddingProjectionStateRepository(
                table_name=settings.embedding_projection_state_table,
                dynamodb_client=dynamodb_client,
            )
        manifest_repo = None
        if settings.manifest_bucket and settings.manifest_index_table:
            manifest_repo = ManifestRepository(
                manifest_bucket=settings.manifest_bucket,
                manifest_prefix=settings.manifest_prefix,
                s3_client=s3_client,
                dynamodb_client=dynamodb_client,
                manifest_index_table=settings.manifest_index_table,
            )
        return cls(
            settings=settings,
            s3_client=s3_client,
            object_state_repo=object_state_repo,
            execution_state_repo=execution_state_repo,
            projection_state_repo=projection_state_repo,
            manifest_repo=manifest_repo,
        )

    def build_status(self, request: JobStatusRequest) -> dict[str, Any]:
        """
        EN: Resolve one version-aware job status snapshot.
        CN: 解析一个按版本感知的 job status 快照。
        """
        lookup = resolve_lookup(
            bucket=request.bucket,
            key=request.key,
            tenant_id=request.tenant_id,
            object_state_repo=self._object_state_repo,
            execution_state_repo=self._execution_state_repo,
        )
        current_state = load_execution_state(
            object_pk=lookup.object_pk,
            execution_state_repo=self._execution_state_repo,
            object_state_repo=self._object_state_repo,
        ) if lookup else None

        if request.tenant_id and not tenant_matches_request(request.tenant_id, lookup, current_state):
            return build_missing_response(
                bucket=request.bucket,
                key=request.key,
                version_id=request.version_id,
                tenant_id=request.tenant_id,
            )

        source_info_raw = head_source_object(
            s3_client=self._s3,
            bucket=request.bucket,
            key=request.key,
            version_id=request.version_id,
            lookup=lookup,
        )
        source_info = serialize_source_info(source_info_raw) if source_info_raw is not None else None
        if lookup is None and source_info is None:
            return build_missing_response(
                bucket=request.bucket,
                key=request.key,
                version_id=request.version_id,
                tenant_id=request.tenant_id,
            )

        latest_version_id = resolve_latest_version_id(
            lookup=lookup,
            current_state=current_state,
            source_info=source_info,
        )
        resolved_version_id = request.version_id or latest_version_id
        requested_version_is_latest = bool(
            not resolved_version_id or not latest_version_id or resolved_version_id == latest_version_id
        )
        version_state = current_state if requested_version_is_latest else None
        tenant_id = request.tenant_id or (lookup.tenant_id if lookup else None) or (
            tenant_id_from_object_pk(getattr(current_state, "pk", "")) if current_state else ""
        )
        object_pk = build_object_pk(tenant_id, request.bucket, request.key) if tenant_id else (lookup.object_pk if lookup else "")

        source_ref = build_source_ref(
            tenant_id=tenant_id or (lookup.tenant_id if lookup else "unknown"),
            bucket=request.bucket,
            key=request.key,
            version_id=resolved_version_id or (lookup.latest_version_id if lookup else ""),
            sequencer=lookup.latest_sequencer if lookup else None,
        )

        manifest_uri = resolve_manifest_uri(
            source_ref=source_ref,
            requested_version_id=resolved_version_id,
            lookup=lookup,
            current_state=version_state,
            manifest_repo=self._manifest_repo,
        )
        manifest_summary = load_manifest_summary(
            manifest_s3_uri=manifest_uri,
            manifest_repo=self._manifest_repo,
        )
        projection_records = load_projection_records(
            object_pk=object_pk,
            version_id=resolved_version_id,
            projection_state_repo=self._projection_state_repo,
        )
        profile_rows = build_profile_rows(
            [serialize_projection_record(record) for record in projection_records],
            manifest_summary,
        )
        stage_rows = build_stage_rows(
            lookup_present=lookup is not None,
            current_state=version_state,
            manifest_summary=manifest_summary,
            profile_rows=profile_rows,
            source_info=source_info,
        )
        overall_status = resolve_overall_status(
            lookup_present=lookup is not None,
            current_state=version_state,
            manifest_summary=manifest_summary,
            profile_rows=profile_rows,
            source_info=source_info,
        )
        progress_percent = resolve_progress_percent(stage_rows)
        latest_updated_at = latest_timestamp(
            lookup.updated_at if lookup else None,
            current_state.updated_at if current_state else None,
            *(row.get("updated_at") for row in profile_rows),
        )

        return {
            "job_id": resolved_version_id or request.version_id,
            "tenant_id": tenant_id or (lookup.tenant_id if lookup else None),
            "bucket": request.bucket,
            "key": request.key,
            "version_id": resolved_version_id or request.version_id,
            "object_pk": object_pk or None,
            "is_latest_version": bool(not current_state or resolved_version_id == current_state.latest_version_id),
            "overall_status": overall_status,
            "current_stage": resolve_current_stage(stage_rows),
            "progress_percent": progress_percent,
            "updated_at": latest_updated_at,
            "source": source_info,
            "lookup": serialize_lookup(lookup) if lookup else None,
            "object_state": serialize_object_state(current_state) if current_state else None,
            "manifest": manifest_summary,
            "profiles": profile_rows,
            "stages": stage_rows,
        }
