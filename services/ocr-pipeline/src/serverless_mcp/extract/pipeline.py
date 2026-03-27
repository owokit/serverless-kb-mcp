"""
EN: Extraction result persister that writes manifest to S3 and dispatches embed jobs.
CN: 将提取结果持久化到 S3 manifest，并分发 embed 作业的持久化器。
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
    S3ObjectRef,
)
from serverless_mcp.storage.state.execution_state_repository import ExecutionStateRepository
from serverless_mcp.storage.manifest.repository import ManifestRepository
from serverless_mcp.storage.state.object_state_repository import ObjectStateRepository


_DISPATCH_FAILURE_TYPES = (ClientError, KeyError, OSError, RuntimeError, TypeError, ValueError)


class ExtractionResultPersister:
    """
    EN: Persist extraction manifest to S3 manifest bucket and dispatch embedding jobs to SQS.
    CN: 将提取 manifest 持久化到 S3 manifest bucket，并把 embedding 作业分发到 SQS。

    This component owns the boundary between extract and embed phases.
    该组件负责提取阶段与嵌入阶段之间的边界。
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
        CN: 提取阶段只持久化 manifest 并分发整篇文档的 embed 作业，不会直接写入向量。

        Args:
            source:
                EN: S3 object reference with bucket/key/version_id identity.
                CN: 通过 bucket、key 和 version_id 标识的 S3 对象引用。
            manifest:
                EN: Chunk manifest containing text chunks and asset references.
                CN: 包含文本 chunk 和资源引用的 chunk manifest。
            trace_id:
                EN: Trace identifier for request correlation.
                CN: 用于请求关联的 trace 标识。
            previous_version_id:
                EN: Previous version_id for version progression tracking.
                CN: 用于版本推进跟踪的 previous_version_id。
            previous_manifest_s3_uri:
                EN: Previous manifest S3 URI for version chain.
                CN: 版本链中上一份 manifest 的 S3 URI。

        Returns:
            EN: Processing outcome with manifest URI and chunk counts.
            CN: 包含 manifest URI 和 chunk 计数的处理结果。
        """
        persisted = self._manifest_repo.persist_manifest(
            manifest,
            previous_version_id=previous_version_id,
        )
        embedding_requests = self._extraction_service.build_embedding_requests(
            persisted.manifest,
            manifest_s3_uri=persisted.manifest_s3_uri,
        )
        validate_embedding_requests(embedding_requests)
        # EN: Fan out one embed job per enabled profile so each vector index is populated independently.
        # CN: 为每个已启用的 profile 分发一份 embed 作业，让各自的向量索引独立填充。
        embedding_jobs = build_jobs_for_profiles(
            source=source,
            trace_id=trace_id,
            manifest_s3_uri=persisted.manifest_s3_uri,
            requests=embedding_requests,
            profiles=self._embedding_profiles,
            previous_version_id=previous_version_id,
            previous_manifest_s3_uri=previous_manifest_s3_uri,
        )
        try:
            self._embed_dispatcher.dispatch_many(embedding_jobs)
        except _DISPATCH_FAILURE_TYPES:
            self._manifest_repo.rollback_manifest(
                persisted.manifest,
                manifest_s3_uri=persisted.manifest_s3_uri,
                previous_version_id=previous_version_id,
            )
            raise
        object_state = self._object_state_repo.mark_extract_done(source, persisted.manifest_s3_uri)
        if self._execution_state_repo is not None:
            self._execution_state_repo.mark_extract_done(source, persisted.manifest_s3_uri)

        return ProcessingOutcome(
            source=source,
            manifest_s3_uri=persisted.manifest_s3_uri,
            chunk_count=len(persisted.manifest.chunks),
            asset_count=len(persisted.manifest.assets),
            embedding_request_count=len(embedding_requests),
            object_state=object_state,
        )
