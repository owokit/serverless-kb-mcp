"""
EN: Persist extraction manifests, fan out embedding jobs, and commit extract state.
CN: 持久化提取 manifest、扇出 embedding 作业并提交提取状态。
"""
from __future__ import annotations

from botocore.exceptions import ClientError

from serverless_mcp.domain.models import ChunkManifest, EmbeddingProfile, ObjectStateRecord, ProcessingOutcome, S3ObjectRef
from serverless_mcp.embed.dispatcher import EmbeddingJobDispatcher
from serverless_mcp.extract.application import ExtractionService
from serverless_mcp.extract.state_commit import ExtractionStateCommitter, StaleExtractionStateError
from serverless_mcp.runtime.observability import emit_metric, emit_trace
from serverless_mcp.storage.manifest.repository import ManifestRepository


_PERSIST_PREPARATION_FAILURE_TYPES = (ClientError, KeyError, OSError, RuntimeError, TypeError, ValueError)


class ExtractionResultPersister:
    """
    EN: Persist a manifest, dispatch embedding work, and commit extract state.
    CN: 持久化 manifest、分发 embedding 作业并提交提取状态。

    This service owns the manifest / embed orchestration boundary.
    此服务拥有 manifest / embed 的编排边界。
    """

    def __init__(
        self,
        *,
        extraction_service: ExtractionService,
        state_committer: ExtractionStateCommitter,
        manifest_repo: ManifestRepository,
        embed_dispatcher: EmbeddingJobDispatcher,
        embedding_profiles: tuple[EmbeddingProfile, ...],
    ) -> None:
        self._extraction_service = extraction_service
        self._state_committer = state_committer
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
        EN: Persist a manifest, dispatch profile-scoped embed jobs, and finalize state.
        CN: 持久化 manifest、分发按 profile 划分的 embed 作业并完成状态提交。
        """
        current_state = self._state_committer.get_state(object_pk=source.object_pk)
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
            # EN: Validate requests before fan-out so schema drift fails before any SQS writes.
            # CN: 在扇出前校验请求，确保 schema 漂移在任何 SQS 写入之前失败。
            embedding_jobs_count = self._embed_dispatcher.dispatch_for_profiles(
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
            object_state = self._state_committer.commit(
                source=source,
                manifest_s3_uri=persisted.manifest_s3_uri,
                current_state=current_state,
            )
        except StaleExtractionStateError:
            latest_state = self._state_committer.get_state(object_pk=source.object_pk) or current_state
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
            embedding_request_count=embedding_jobs_count,
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
