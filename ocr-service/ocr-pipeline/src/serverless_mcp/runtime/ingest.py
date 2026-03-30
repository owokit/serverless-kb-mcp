"""
EN: Ingest workflow starter that enforces idempotency and launches Step Functions executions.
CN: 同上。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from typing import Protocol

from botocore.exceptions import ClientError

from serverless_mcp.embed.vector_repository import S3VectorRepository
from serverless_mcp.core.parsers import parse_event
from serverless_mcp.domain.models import EmbeddingProfile, ObjectStateRecord, S3ObjectRef
from serverless_mcp.runtime.aws_resolution import resolve_step_functions_state_machine_arn
from serverless_mcp.runtime.bootstrap import (
    build_manifest_repo,
    build_object_state_repo,
    build_projection_state_repo,
    build_runtime_context,
)
from serverless_mcp.runtime.config import Settings
from serverless_mcp.runtime.embedding_profiles import get_write_profiles
from serverless_mcp.storage.manifest.repository import ManifestRepository
from serverless_mcp.storage.state.execution_state_repository import ExecutionStateRepository
from serverless_mcp.storage.projection.repository import EmbeddingProjectionStateRepository
from serverless_mcp.storage.state.object_state_repository import DuplicateOrStaleEventError, ObjectStateRepository


_STEP_FUNCTIONS_FAILURE_TYPES = (ClientError, KeyError, OSError, RuntimeError, TypeError, ValueError)


class _DeleteLifecycleManager(Protocol):
    """
    EN: Structural protocol for delete-marker side effects such as vector invalidation.
    CN: 同上。
    """

    def handle_delete(self, *, source: S3ObjectRef) -> None: ...


class DeleteMarkerGovernance:
    """
    EN: Invalidate vectors for the latest visible document version after a delete marker becomes authoritative.
    CN: 同上。
    """

    def __init__(
        self,
        *,
        object_state_repo: ObjectStateRepository,
        manifest_repo: ManifestRepository,
        vector_repo: S3VectorRepository,
        profiles: tuple[EmbeddingProfile, ...],
        projection_state_repo: EmbeddingProjectionStateRepository | None = None,
        execution_state_repo: ExecutionStateRepository | None = None,
    ) -> None:
        self._object_state_repo = object_state_repo
        self._execution_state_repo = execution_state_repo
        self._manifest_repo = manifest_repo
        self._vector_repo = vector_repo
        self._profiles = tuple(profile for profile in profiles if profile.enable_write)
        self._projection_state_repo = projection_state_repo

    def handle_delete(self, *, source: S3ObjectRef) -> None:
        """
        EN: Mark current vectors stale for every write-enabled profile after object deletion.
        CN: 同上。
        """
        lookup = self._object_state_repo.get_lookup_for_source(source)
        if lookup is None:
            return
        state = self._execution_state_repo.get_state(object_pk=lookup.object_pk) if self._execution_state_repo else self._object_state_repo.get_state(object_pk=lookup.object_pk)
        if state is None or not state.latest_manifest_s3_uri:
            return
        manifest = self._manifest_repo.load_manifest(state.latest_manifest_s3_uri)
        for profile in self._profiles:
            stale_keys = _build_vector_keys(profile_id=profile.profile_id, manifest=manifest)
            self._vector_repo.mark_vectors_stale(profile=profile, keys=stale_keys)
            if self._projection_state_repo is not None:
                self._projection_state_repo.mark_deleted(
                    source=manifest.source,
                    profile=profile,
                    manifest_s3_uri=state.latest_manifest_s3_uri,
                )


class IngestWorkflowStarter:
    """
    EN: Start Step Functions Standard executions for S3 object versions after idempotency checks.
    CN: 同上。
    """

    def __init__(
        self,
        *,
        object_state_repo: ObjectStateRepository,
        stepfunctions_client: object,
        state_machine_arn: str,
        delete_lifecycle_manager: _DeleteLifecycleManager | None = None,
    ) -> None:
        self._object_state_repo = object_state_repo
        self._stepfunctions = stepfunctions_client
        self._state_machine_arn = state_machine_arn
        self._delete_lifecycle_manager = delete_lifecycle_manager

    def handle_batch(self, event: dict) -> dict:
        """
        EN: Process S3 event batch, enforce idempotency, and route create/delete events.
        CN: 同上。
        """
        batch = parse_event(event)
        started: list[dict] = []
        skipped: list[dict] = []
        deleted: list[dict] = []
        failed: list[dict] = []

        for job in batch.jobs:
            try:
                # EN: Route delete events to vector invalidation before skipping extract logic.
                # CN: 灏嗗垹闄や簨浠惰矾鐢卞埌鍚戦噺澶辨晥澶勭悊锛岀劧鍚庤烦杩囨彁鍙栭€昏緫銆?
                if job.operation == "DELETE":
                    deleted_record = self._object_state_repo.mark_deleted(
                        bucket=job.source.bucket,
                        key=job.source.key,
                        version_id=job.source.version_id,
                        sequencer=job.source.sequencer,
                    )
                    if self._delete_lifecycle_manager is not None:
                        self._delete_lifecycle_manager.handle_delete(source=job.source)
                    deleted.append(
                        {
                            "document_uri": job.source.document_uri,
                            "object_pk": deleted_record.pk,
                        }
                    )
                    continue

                processing_state = self._build_processing_state(job.source)
                execution = self._stepfunctions.start_execution(
                    stateMachineArn=self._state_machine_arn,
                    name=_build_execution_name(job.source),
                    input=json.dumps(
                        {
                            "job": asdict(job),
                            "processing_state": asdict(processing_state),
                        },
                        ensure_ascii=False,
                    ),
                )
                started.append(
                    {
                        "document_uri": job.source.document_uri,
                        "execution_arn": execution["executionArn"],
                    }
                )
            except DuplicateOrStaleEventError:
                skipped.append(
                    {
                        "document_uri": job.source.document_uri,
                        "reason": "duplicate_or_stale_event",
                        "error_type": "DuplicateOrStaleEventError",
                        "stage": "idempotency_check",
                    }
                )
            except ClientError as exc:
                if exc.response.get("Error", {}).get("Code") == "ExecutionAlreadyExists":
                    skipped.append(
                        {
                            "document_uri": job.source.document_uri,
                            "reason": "execution_already_exists",
                            "error_type": "ClientError",
                            "stage": "stepfunctions_start",
                        }
                    )
                    continue
                failed.append(_build_failure_record(job.source.document_uri, "stepfunctions_start", exc))
                raise
            except _STEP_FUNCTIONS_FAILURE_TYPES as exc:
                failed.append(_build_failure_record(job.source.document_uri, "stepfunctions_start", exc))
                raise

        return {
            "statusCode": 200,
            "raw_record_count": batch.raw_record_count,
            "started_count": len(started),
            "deleted_count": len(deleted),
            "skipped_count": len(skipped),
            "failed_count": len(failed),
            "started": started,
            "deleted": deleted,
            "skipped": skipped,
            "failed": failed,
        }

    def _build_processing_state(self, source: S3ObjectRef):
        """
        EN: Build the queued processing state from the current snapshot without persisting before execution start.
        CN: 同上。
        """
        current_state = self._object_state_repo.get_state(object_pk=source.object_pk)
        normalized_sequencer = _normalize_sequencer_value(source.sequencer)
        if current_state is not None:
            # EN: Reject if the version_id is already the latest, preventing duplicate workflow launches.
            # CN: 同上。
            if current_state.latest_version_id == source.version_id:
                raise DuplicateOrStaleEventError(source.document_uri)
            # EN: Reject stale events whose sequencer is not strictly newer than the stored value.
            # CN: 鎷掔粷 sequencer 涓嶄弗鏍兼柊浜庡凡瀛樺偍鍊肩殑杩囨湡浜嬩欢銆?
            if (
                normalized_sequencer
                and current_state.latest_sequencer
                and current_state.latest_sequencer >= normalized_sequencer
            ):
                raise DuplicateOrStaleEventError(source.document_uri)
        # EN: Capture previous version metadata for downstream old-version cleanup references.
        # CN: 同上。
        previous_version_id = current_state.latest_version_id if current_state is not None else None
        previous_manifest_s3_uri = current_state.latest_manifest_s3_uri if current_state is not None else None
        return ObjectStateRecord(
            pk=source.object_pk,
            latest_version_id=source.version_id,
            latest_sequencer=normalized_sequencer,
            extract_status="QUEUED",
            embed_status="PENDING",
            previous_version_id=previous_version_id,
            previous_manifest_s3_uri=previous_manifest_s3_uri,
            latest_manifest_s3_uri=None,
            is_deleted=False,
            last_error="",
        )


def _build_execution_name(source: S3ObjectRef) -> str:
    """
    EN: Generate unique Step Functions execution name from S3 object identity.
    CN: 同上。
    """
    tenant_digest = hashlib.sha1(source.tenant_id.encode("utf-8")).hexdigest()[:8]
    bucket_digest = hashlib.sha1(source.bucket.encode("utf-8")).hexdigest()[:8]
    key_digest = hashlib.sha1(source.key.encode("utf-8")).hexdigest()[:16]
    version_digest = hashlib.sha1(source.version_id.encode("utf-8")).hexdigest()[:12]
    sequencer = source.sequencer or "noseq"
    sequencer_digest = hashlib.sha1(sequencer.encode("utf-8")).hexdigest()[:12]
    return (
        f"ingest-{tenant_digest}-b{bucket_digest}-"
        f"k{key_digest}-v{version_digest}-s{sequencer_digest}"
    )


def _normalize_sequencer_value(sequencer: str | None) -> str | None:
    """
    EN: Normalize sequencer text so in-memory stale checks match repository ordering semantics.
    CN: 同上。
    """
    value = (sequencer or "").strip()
    if not value:
        return None
    return value.upper().zfill(32)


def _build_vector_keys(*, profile_id: str, manifest) -> list[str]:
    """
    EN: Build vector keys for all chunks and image assets in the given manifest.
    CN: 同上。
    """
    keys = [f"{profile_id}#{manifest.source.version_pk}#{chunk.chunk_id}" for chunk in manifest.chunks]
    for asset in manifest.assets:
        if asset.chunk_type in {"page_image_chunk", "slide_image_chunk", "image_chunk"}:
            keys.append(f"{profile_id}#{manifest.source.version_pk}#{asset.asset_id}")
    return keys


def _build_failure_record(document_uri: str, stage: str, exc: Exception) -> dict[str, object]:
    """
    EN: Build a structured failure payload for ingest batch diagnostics.
    CN: 为 ingest 批处理诊断构建结构化失败负载。
    """
    return {
        "document_uri": document_uri,
        "stage": stage,
        "error_type": type(exc).__name__,
        "reason": "unexpected_error",
        "error": str(exc),
    }


def build_ingest_workflow_starter(
    lambda_context: object | None = None,
    settings: Settings | None = None,
) -> IngestWorkflowStarter:
    """
    EN: Build the ingest workflow starter with required dependencies.
    CN: 使用所需依赖构建 ingest workflow starter。
    """
    runtime_context = build_runtime_context(settings=settings)
    active_settings = runtime_context.settings
    if not active_settings.step_functions_state_machine_arn:
        raise ValueError("STEP_FUNCTIONS_STATE_MACHINE_ARN is required for ingest worker")
    if not active_settings.execution_state_table:
        raise ValueError("EXECUTION_STATE_TABLE is required for ingest worker")
    clients = runtime_context.clients
    state_machine_arn = resolve_step_functions_state_machine_arn(
        state_machine_ref=active_settings.step_functions_state_machine_arn,
    )
    object_state_repo = build_object_state_repo(settings=active_settings, clients=clients)
    execution_state_repo = ExecutionStateRepository(
        table_name=active_settings.execution_state_table,
        dynamodb_client=clients.dynamodb,
    )
    delete_lifecycle_manager = None
    write_profiles = get_write_profiles(active_settings)
    if active_settings.manifest_bucket and active_settings.manifest_index_table and write_profiles:
        projection_state_repo = build_projection_state_repo(settings=active_settings, clients=clients)
        manifest_repo = build_manifest_repo(settings=active_settings, clients=clients)
        if manifest_repo is None:
            raise ValueError("MANIFEST_BUCKET and MANIFEST_INDEX_TABLE are required for ingest worker cleanup")
        delete_lifecycle_manager = DeleteMarkerGovernance(
            object_state_repo=object_state_repo,
            execution_state_repo=execution_state_repo,
            manifest_repo=manifest_repo,
            vector_repo=S3VectorRepository(s3vectors_client=clients.s3vectors),
            profiles=write_profiles,
            projection_state_repo=projection_state_repo,
        )

    return IngestWorkflowStarter(
        object_state_repo=object_state_repo,
        stepfunctions_client=clients.stepfunctions,
        state_machine_arn=state_machine_arn,
        delete_lifecycle_manager=delete_lifecycle_manager,
    )
