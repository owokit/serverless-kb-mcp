"""
EN: Extraction result persister that writes manifest to S3 and dispatches embed jobs.
CN: 灏嗘彁鍙栫粨鏋滄寔涔呭寲鍒?S3 manifest锛屽苟鍒嗗彂 embed 浣滀笟鐨勬寔涔呭寲鍣ㄣ€?
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


_DISPATCH_FAILURE_TYPES = (ClientError, KeyError, OSError, RuntimeError, TypeError, ValueError)
_PERSIST_PREPARATION_FAILURE_TYPES = (ClientError, KeyError, OSError, RuntimeError, TypeError, ValueError)


class ExtractionResultPersister:
    """
    EN: Persist extraction manifest to S3 manifest bucket and dispatch embedding jobs to SQS.
    CN: 灏嗘彁鍙?manifest 鎸佷箙鍖栧埌 S3 manifest bucket锛屽苟鎶?embedding 浣滀笟鍒嗗彂鍒?SQS銆?

    This component owns the boundary between extract and embed phases.
    璇ョ粍浠惰礋璐ｆ彁鍙栭樁娈典笌宓屽叆闃舵涔嬮棿鐨勮竟鐣屻€?
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
        CN: 鎻愬彇闃舵鍙寔涔呭寲 manifest 骞跺垎鍙戞暣绡囨枃妗ｇ殑 embed 浣滀笟锛屼笉浼氱洿鎺ュ啓鍏ュ悜閲忋€?

        Args:
            source:
                EN: S3 object reference with bucket/key/version_id identity.
                CN: 閫氳繃 bucket銆乲ey 鍜?version_id 鏍囪瘑鐨?S3 瀵硅薄寮曠敤銆?
            manifest:
                EN: Chunk manifest containing text chunks and asset references.
                CN: 鍖呭惈鏂囨湰 chunk 鍜岃祫婧愬紩鐢ㄧ殑 chunk manifest銆?
            trace_id:
                EN: Trace identifier for request correlation.
                CN: 鐢ㄤ簬璇锋眰鍏宠仈鐨?trace 鏍囪瘑銆?
            previous_version_id:
                EN: Previous version_id for version progression tracking.
                CN: 鐢ㄤ簬鐗堟湰鎺ㄨ繘璺熻釜鐨?previous_version_id銆?
            previous_manifest_s3_uri:
                EN: Previous manifest S3 URI for version chain.
                CN: 鐗堟湰閾句腑涓婁竴浠?manifest 鐨?S3 URI銆?

        Returns:
            EN: Processing outcome with manifest URI and chunk counts.
            CN: 鍖呭惈 manifest URI 鍜?chunk 璁℃暟鐨勫鐞嗙粨鏋溿€?
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
            # CN: 涓烘瘡涓凡鍚敤鐨?profile 鍒嗗彂涓€浠?embed 浣滀笟锛岃鍚勮嚜鐨勫悜閲忕储寮曠嫭绔嬪～鍏呫€?
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
        CN: 瑜版挻褰侀崣鏍Ц閹礁鍑℃潻鍥ㄦ埂閹存牕鍑＄€瑰本鍨氶弮鑸电€楦款暙閸欓娈戦獮鍐叉嫲鐠哄嫯绻冪紒鎾寸亯閵?
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
