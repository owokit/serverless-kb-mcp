"""
EN: Manifest persistence and embed job dispatching for extraction results.
CN: 提取结果的 manifest 持久化和 embed 作业分发。

This module provides components for persisting extraction manifests to S3
and dispatching embedding jobs to SQS queues.
本模块提供将提取 manifest 持久化到 S3 以及将 embedding 作业分发到 SQS 队列的组件。
"""
from __future__ import annotations

from botocore.exceptions import ClientError

from serverless_mcp.embed.dispatcher import EmbeddingJobDispatcher, build_jobs_for_profiles
from serverless_mcp.extract.application import ExtractionService
from serverless_mcp.domain.embedding_schema import validate_embedding_requests
from serverless_mcp.domain.models import (
    ChunkManifest,
    EmbeddingProfile,
    ProcessingOutcome,
    ObjectStateRecord,
    S3ObjectRef,
)
from serverless_mcp.runtime.observability import emit_metric, emit_trace
from serverless_mcp.storage.state.execution_state_repository import ExecutionStateRepository
from serverless_mcp.storage.manifest.repository import ManifestRepository
from serverless_mcp.storage.state.object_state_repository import DuplicateOrStaleEventError, ObjectStateRepository


_PERSIST_PREPARATION_FAILURE_TYPES = (ClientError, KeyError, OSError, RuntimeError, TypeError, ValueError)


class ManifestPersister:
    """
    EN: Persist extraction manifest to S3 manifest bucket and update object state.
    CN: 将提取 manifest 持久化到 S3 manifest bucket 并更新 object state。

    This component owns the boundary between extract and embed phases,
    handling only manifest persistence and state updates without embed job dispatching.
    此组件拥有 extract 和 embed 阶段之间的边界，仅处理 manifest 持久化和状态更新，
    不处理 embed 作业分发。
    """

    def __init__(
        self,
        *,
        extraction_service: ExtractionService,
        object_state_repo: ObjectStateRepository,
        manifest_repo: ManifestRepository,
        execution_state_repo: ExecutionStateRepository | None = None,
    ) -> None:
        self._extraction_service = extraction_service
        self._object_state_repo = object_state_repo
        self._execution_state_repo = execution_state_repo
        self._manifest_repo = manifest_repo

    def persist(
        self,
        *,
        source: S3ObjectRef,
        manifest: ChunkManifest,
        previous_version_id: str | None = None,
        previous_manifest_s3_uri: str | None = None,
    ) -> tuple[ChunkManifest, str]:
        """
        EN: Persist manifest to S3 and update object state, returning manifest and URI.
        CN: 将 manifest 持久化到 S3 并更新 object state，返回 manifest 和 URI。

        Args:
            source:
                EN: S3 object reference with bucket/key/version_id identity.
                CN: 通过 bucket、key 和 version_id 标识的 S3 对象引用。
            manifest:
                EN: Chunk manifest containing text chunks and asset references.
                CN: 包含文本 chunk 和资源引用的 chunk manifest。
            previous_version_id:
                EN: Previous version_id for version progression tracking.
                CN: 用于版本推进追踪的 previous_version_id。
            previous_manifest_s3_uri:
                EN: Previous manifest S3 URI for version chain.
                CN: 版本链中上一个 manifest 的 S3 URI。

        Returns:
            EN: Tuple of (persisted_manifest, manifest_s3_uri).
            CN: (persisted_manifest, manifest_s3_uri) 元组。
        """
        current_state = self._object_state_repo.get_state(object_pk=source.object_pk)
        if current_state is not None and (
            current_state.latest_version_id != source.version_id or current_state.extract_status != "EXTRACTING"
        ):
            raise StaleExtractionStateError(
                f"Extraction state is stale or complete for {source.document_uri}: "
                f"version_id={current_state.latest_version_id}, "
                f"extract_status={current_state.extract_status}"
            )

        persisted = self._manifest_repo.persist_manifest(
            manifest,
            previous_version_id=previous_version_id,
        )

        try:
            object_state = self._object_state_repo.mark_extract_done(source, persisted.manifest_s3_uri)
            if self._execution_state_repo is not None:
                self._execution_state_repo.mark_extract_done(source, persisted.manifest_s3_uri)
        except DuplicateOrStaleEventError:
            latest_state = self._object_state_repo.get_state(object_pk=source.object_pk) or current_state
            raise StaleExtractionStateError(
                f"State became stale during commit for {source.document_uri}: "
                f"version_id={latest_state.latest_version_id if latest_state else 'unknown'}"
            ) from None

        return persisted.manifest, persisted.manifest_s3_uri


class EmbedJobDispatcher:
    """
    EN: Dispatch embedding jobs to SQS based on persisted manifest and embedding profiles.
    CN: 基于已持久化的 manifest 和 embedding profile 将 embedding 作业分发到 SQS。

    This component handles the fan-out of embedding jobs per enabled profile
    and dispatches them to the SQS embed queue.
    此组件处理按启用 profile 扇出的 embedding 作业，并将其分发到 SQS embed 队列。
    """

    def __init__(
        self,
        *,
        embed_dispatcher: EmbeddingJobDispatcher,
        manifest_repo: ManifestRepository,
        embedding_profiles: tuple[EmbeddingProfile, ...],
    ) -> None:
        self._embed_dispatcher = embed_dispatcher
        self._manifest_repo = manifest_repo
        self._embedding_profiles = embedding_profiles

    def dispatch(
        self,
        *,
        source: S3ObjectRef,
        manifest: ChunkManifest,
        manifest_s3_uri: str,
        trace_id: str,
        previous_version_id: str | None = None,
        previous_manifest_s3_uri: str | None = None,
    ) -> int:
        """
        EN: Build and dispatch embedding jobs for all enabled profiles.
        CN: 为所有已启用的 profile 构建并分发 embedding 作业。

        Args:
            source:
                EN: S3 object reference with bucket/key/version_id identity.
                CN: 通过 bucket、key 和 version_id 标识的 S3 对象引用。
            manifest:
                EN: Chunk manifest containing text chunks and asset references.
                CN: 包含文本 chunk 和资源引用的 chunk manifest。
            manifest_s3_uri:
                EN: Version-aware S3 URI of the persisted manifest.
                CN: 已持久化 manifest 的带版本 S3 URI。
            trace_id:
                EN: Trace identifier for request correlation.
                CN: 用于请求关联的 trace 标识符。
            previous_version_id:
                EN: Previous version_id for version progression tracking.
                CN: 用于版本推进追踪的 previous_version_id。
            previous_manifest_s3_uri:
                EN: Previous manifest S3 URI for version chain.
                CN: 版本链中上一个 manifest 的 S3 URI。

        Returns:
            EN: Number of embedding jobs dispatched.
            CN: 分发的 embedding 作业数量。
        """
        embedding_requests = build_embedding_requests(
            manifest=manifest,
            manifest_s3_uri=manifest_s3_uri,
            extraction_service=None,  # Requests already built at this point
        )
        validate_embedding_requests(embedding_requests)

        embedding_jobs = build_jobs_for_profiles(
            source=source,
            trace_id=trace_id,
            manifest_s3_uri=manifest_s3_uri,
            requests=embedding_requests,
            profiles=self._embedding_profiles,
            previous_version_id=previous_version_id,
            previous_manifest_s3_uri=previous_manifest_s3_uri,
        )

        if not embedding_jobs:
            return 0

        self._embed_dispatcher.dispatch_many(embedding_jobs)
        return len(embedding_jobs)


# =============================================================================
# EN: Original ExtractionResultPersister preserved for backward compatibility.
# CN: 为保持向后兼容而保留的原始 ExtractionResultPersister。
# =============================================================================


class ExtractionResultPersister:
    """
    EN: Persist extraction manifest to S3 manifest bucket and dispatch embedding jobs to SQS.
    CN: 将提取 manifest 持久化到 S3 manifest bucket，并向 SQS 分发 embedding 作业。

    This component owns the boundary between extract and embed phases.
    此组件拥有 extract 和 embed 阶段之间的边界。
    """

    def __init__(
        self,
        *,
        extraction_service: ExtractionService,
        object_state_repo: ObjectStateRepository,
        manifest_repo: ManifestRepository,
        embed_dispatcher: EmbeddingJobDispatcher,
        embedding_profiles: tuple[EmbeddingProfile, ...],
        execution_state_repo: ExecutionStateRepository | None = None,
    ) -> None:
        self._extraction_service = extraction_service
        self._object_state_repo = object_state_repo
        self._execution_state_repo = execution_state_repo
        self._manifest_repo = manifest_repo
        self._embed_dispatcher = embed_dispatcher
        self._embedding_profiles = embedding_profiles

    def persist(
        self,
        *,
        source: S3ObjectRef,
        manifest: ChunkManifest,
        trace_id: str,
        previous_version_id: str | None = None,
        previous_manifest_s3_uri: str | None = None,
    ) -> ProcessingOutcome:
        """
        EN: Extract stage only persists manifest and dispatches whole-document embed job, not writing vectors directly.
        CN: 提取阶段仅持久化 manifest 并分发整篇文档的 embed 作业，不直接写入向量。

        Args:
            source:
                EN: S3 object reference with bucket/key/version_id identity.
                CN: 通过 bucket、key 和 version_id 标识的 S3 对象引用。
            manifest:
                EN: Chunk manifest containing text chunks and asset references.
                CN: 包含文本 chunk 和资源引用的 chunk manifest。
            trace_id:
                EN: Trace identifier for request correlation.
                CN: 用于请求关联的 trace 标识符。
            previous_version_id:
                EN: Previous version_id for version progression tracking.
                CN: 用于版本推进追踪的 previous_version_id。
            previous_manifest_s3_uri:
                EN: Previous manifest S3 URI for version chain.
                CN: 版本链中上一个 manifest 的 S3 URI。

        Returns:
            EN: Processing outcome with manifest URI and chunk counts.
            CN: 包含 manifest URI 和 chunk 计数的处理结果。
        """
        current_state = self._object_state_repo.get_state(object_pk=source.object_pk)
        if current_state is not None and (
            current_state.latest_version_id != source.version_id or current_state.extract_status != "EXTRACTING"
        ):
            return self._build_skipped_outcome(
                source=source,
                object_state=current_state,
                reason="stale_or_completed_state",
                stage="preflight",
            )

        persisted = self._manifest_repo.persist_manifest(
            manifest,
            previous_version_id=previous_version_id,
        )
        try:
            embedding_requests = self._extraction_service.build_embedding_requests(
                persisted.manifest,
                manifest_s3_uri=persisted.manifest_s3_uri,
            )
            validate_embedding_requests(embedding_requests)
            # EN: Fan out one embed job per enabled profile so each vector index is populated independently.
            # CN: 为每个已启用的 profile 分发一个 embed 作业，让各自的向量索引独立填充。
            embedding_jobs = build_jobs_for_profiles(
                source=source,
                trace_id=trace_id,
                manifest_s3_uri=persisted.manifest_s3_uri,
                requests=embedding_requests,
                profiles=self._embedding_profiles,
                previous_version_id=previous_version_id,
                previous_manifest_s3_uri=previous_manifest_s3_uri,
            )
        except _PERSIST_PREPARATION_FAILURE_TYPES:
            self._manifest_repo.rollback_manifest(
                persisted.manifest,
                manifest_s3_uri=persisted.manifest_s3_uri,
                previous_version_id=previous_version_id,
            )
            raise
        try:
            self._embed_dispatcher.dispatch_many(embedding_jobs)
        except _DISPATCH_FAILURE_TYPES:
            self._manifest_repo.rollback_manifest(
                persisted.manifest,
                manifest_s3_uri=persisted.manifest_s3_uri,
                previous_version_id=previous_version_id,
            )
            raise
        try:
            object_state = self._object_state_repo.mark_extract_done(source, persisted.manifest_s3_uri)
            if self._execution_state_repo is not None:
                self._execution_state_repo.mark_extract_done(source, persisted.manifest_s3_uri)
        except DuplicateOrStaleEventError:
            latest_state = self._object_state_repo.get_state(object_pk=source.object_pk) or current_state
            return self._build_skipped_outcome(
                source=source,
                object_state=latest_state,
                reason="stale_during_commit",
                stage="commit",
                manifest_s3_uri=persisted.manifest_s3_uri,
            )

        return ProcessingOutcome(
            source=source,
            manifest_s3_uri=persisted.manifest_s3_uri,
            chunk_count=len(persisted.manifest.chunks),
            asset_count=len(persisted.manifest.assets),
            embedding_request_count=len(embedding_requests),
            object_state=object_state,
        )

    def _build_skipped_outcome(
        self,
        *,
        source: S3ObjectRef,
        object_state: ObjectStateRecord | None,
        reason: str,
        stage: str,
        manifest_s3_uri: str | None = None,
    ) -> ProcessingOutcome:
        """
        EN: Build a benign skip outcome when extract state is already stale or complete.
        CN: 当提取状态已经过时或完成时，构建良性的跳过结果。
        """
        if object_state is None:
            object_state = ObjectStateRecord(
                pk=source.object_pk,
                latest_version_id=source.version_id,
                latest_sequencer=source.sequencer,
                extract_status="SKIPPED",
                embed_status="PENDING",
                latest_manifest_s3_uri=manifest_s3_uri,
            )
        emitted_manifest_s3_uri = manifest_s3_uri or object_state.latest_manifest_s3_uri or ""
        emit_trace(
            "persist_ocr_result.skipped",
            document_uri=source.document_uri,
            reason=reason,
            skip_stage=stage,
            object_pk=object_state.pk,
            latest_version_id=object_state.latest_version_id,
            extract_status=object_state.extract_status,
            embed_status=object_state.embed_status,
            manifest_s3_uri=emitted_manifest_s3_uri,
        )
        emit_metric("extract.persist.skip", reason=reason, stage=stage)
        return ProcessingOutcome(
            source=source,
            manifest_s3_uri=emitted_manifest_s3_uri,
            chunk_count=0,
            asset_count=0,
            embedding_request_count=0,
            object_state=object_state,
        )


_DISPATCH_FAILURE_TYPES = (ClientError, KeyError, OSError, RuntimeError, TypeError, ValueError)


class StaleExtractionStateError(ValueError):
    """
    EN: Raised when extraction state is stale or already complete during persist.
    CN: 当在持久化过程中提取状态已过时或已完成时抛出。
    """


def build_embedding_requests(
    *,
    manifest: ChunkManifest,
    manifest_s3_uri: str,
    extraction_service: ExtractionService | None,
) -> list:
    """
    EN: Build embedding requests from manifest, optionally using extraction service.
    CN: 从 manifest 构建 embedding 请求，可选择使用 extraction service。

    Args:
        manifest:
            EN: Chunk manifest containing text chunks and asset references.
            CN: 包含文本 chunk 和资源引用的 chunk manifest。
        manifest_s3_uri:
            EN: S3 URI of the persisted manifest for metadata reference.
            CN: 已持久化 manifest 的 S3 URI，用于元数据引用。
        extraction_service:
            EN: Optional extraction service for building requests (unused, kept for interface).
            CN: 可选的 extraction service 用于构建请求（未使用，为保持接口）。

    Returns:
        EN: List of embedding requests.
        CN: embedding 请求列表。
    """
    # EN: This function is provided for compatibility; actual request building
    # uses ExtractionService.build_embedding_requests() in the original class.
    # 此函数为兼容提供；实际请求构建在原始类中使用
    # ExtractionService.build_embedding_requests()。
    if extraction_service is not None:
        return extraction_service.build_embedding_requests(manifest, manifest_s3_uri=manifest_s3_uri)
    return []
