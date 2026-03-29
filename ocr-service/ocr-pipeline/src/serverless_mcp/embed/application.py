"""
EN: Embed worker that coordinates profile-scoped embedding calls and S3 Vectors persistence.
CN: 协调按 profile 划分的嵌入调用和 S3 Vectors 持久化的嵌入工作器。
"""
from __future__ import annotations

from time import monotonic
from typing import Protocol

from botocore.exceptions import ClientError

from serverless_mcp.embed.asset_source import EmbedAssetSource
from serverless_mcp.embed.vector_repository import S3VectorRepository
from serverless_mcp.runtime.observability import emit_trace
from serverless_mcp.domain.embedding_schema import validate_embedding_job_message
from serverless_mcp.domain.models import (
    ChunkManifest,
    EmbeddingJobMessage,
    EmbeddingOutcome,
    EmbeddingProfile,
    ObjectStateRecord,
    VectorRecord,
)
from serverless_mcp.storage.state.execution_state_repository import ExecutionStateRepository
from serverless_mcp.storage.projection.repository import EmbeddingProjectionStateRepository
from serverless_mcp.storage.manifest.repository import ManifestRepository
from serverless_mcp.storage.state.object_state_repository import ObjectStateRepository


_EMBED_FAILURE_TYPES = (ClientError, KeyError, OSError, RuntimeError, TypeError, ValueError)


class UnknownEmbeddingProfileError(ValueError):
    """
    EN: Raised when an embed job references a profile that is no longer active in the current runtime.
    CN: 当嵌入作业引用了当前运行时不再活跃的 profile 时抛出。
    """


class _EmbeddingClient(Protocol):
    """
    EN: Structural protocol for provider-specific embedding clients.
    CN: 各 provider 专用嵌入客户端的结构协议。
    """

    def embed_text(self, request) -> list[float]: ...

    def embed_bytes(self, *, payload: bytes, mime_type: str, request) -> list[float]: ...


class VersionCleanupService:
    """
    EN: Handle cleanup of previous version vectors, projection state, and manifests.
    CN: 处理旧版本向量、投影状态和 manifest 的清理。

    This service is responsible for:
    1. Deleting stale vectors when a new version is indexed
    2. Cleaning up projection state records for the previous version
    3. Deleting previous version manifests after all profiles have indexed
    此服务负责：
    1. 在新版本被索引时删除过时向量
    2. 清理上一版本的投影状态记录
    3. 在所有 profile 索引完成后删除上一版本的 manifest
    """

    def __init__(
        self,
        *,
        vector_repo: S3VectorRepository,
        manifest_repo: ManifestRepository | None,
        projection_state_repo: EmbeddingProjectionStateRepository | None,
        embedding_profiles: dict[str, EmbeddingProfile],
    ) -> None:
        self._vector_repo = vector_repo
        self._manifest_repo = manifest_repo
        self._projection_state_repo = projection_state_repo
        self._embedding_profiles = embedding_profiles

    def cleanup_previous_version_vectors(
        self,
        *,
        job: EmbeddingJobMessage,
        profile: EmbeddingProfile,
    ) -> None:
        """
        EN: Delete vectors and projection state records for the previous version under the given profile.
        CN: 删除指定 profile 下旧版本的向量和 projection state 记录。

        Args:
            job:
                EN: Embedding job message containing version information.
                CN: 包含版本信息的 embedding 作业消息。
            profile:
                EN: The embedding profile whose vectors should be cleaned up.
                CN: 应该清理其向量的 embedding profile。
        """
        if not job.previous_version_id:
            return
        if not self._manifest_repo:
            raise ValueError("manifest_repo is required for previous version vector governance")

        # EN: Locate the previous manifest, falling back to repository lookup if the URI was not carried in the job.
        # CN: 定位上一版 manifest；如果作业中没有携带 URI，则回退到仓库查询。
        previous_manifest_s3_uri = job.previous_manifest_s3_uri or self._manifest_repo.find_manifest_s3_uri(
            source=job.source,
            version_id=job.previous_version_id,
        )
        if not previous_manifest_s3_uri:
            return

        try:
            previous_manifest = self._manifest_repo.load_manifest(previous_manifest_s3_uri)
        except ClientError as exc:
            # EN: If the previous manifest was already deleted, treat it as no-op.
            # CN: 如果旧 manifest 已经被删掉，就当作 no-op。
            if _is_missing_object_error(exc):
                return
            raise

        stale_keys = _build_vector_keys(profile_id=job.profile_id, manifest=previous_manifest)
        # EN: Delete stale vectors and clean up per-version projection records.
        # CN: 删除过期向量并清理按版本划分的 projection 记录。
        self._vector_repo.delete_vectors(profile=profile, keys=stale_keys)
        if self._projection_state_repo is not None:
            self._projection_state_repo.delete_version_records(source=job.source, version_id=job.previous_version_id)

    def cleanup_previous_manifest_if_complete(
        self,
        *,
        job: EmbeddingJobMessage,
    ) -> None:
        """
        EN: Delete the previous version's manifest artifacts only when all active profiles have indexed the new version.
        CN: 只有当所有启用写入的 profile 都对该版本达到 INDEXED 时才删除旧 manifest 产物。

        Args:
            job:
                EN: Embedding job message containing version information.
                CN: 包含版本信息的 embedding 作业消息。
        """
        if not job.previous_version_id or not self._manifest_repo:
            return
        if not self._is_version_complete(job.source.object_pk, job.source.version_id):
            return
        self._manifest_repo.delete_previous_version_artifacts(
            source=job.source,
            previous_version_id=job.previous_version_id,
            previous_manifest_s3_uri=job.previous_manifest_s3_uri,
        )

    def _is_version_complete(self, object_pk: str, version_id: str) -> bool:
        """
        EN: Check whether all write-enabled profiles have reached INDEXED status for this version.
        CN: 检查所有启用写入的 profile 是否已达到该版本的 INDEXED 状态。
        """
        enabled_profiles = [profile for profile in self._embedding_profiles.values() if profile.enable_write]
        if not enabled_profiles:
            return False
        if self._projection_state_repo is None:
            return len(enabled_profiles) == 1

        for profile in enabled_profiles:
            state = self._projection_state_repo.get_state(
                object_pk=object_pk,
                version_id=version_id,
                profile_id=profile.profile_id,
            )
            if state is None or state.query_status != "INDEXED":
                return False
        return True


class EmbedWorker:
    """
    EN: Process one profile-scoped embedding job, then clean previous-version artifacts once the new vectors are durably written.
    CN: 处理单个按 profile 划分的嵌入作业，待新向量持久化后再清理旧版本产物。
    """

    def __init__(
        self,
        *,
        embedding_clients: dict[str, _EmbeddingClient],
        embedding_profiles: dict[str, EmbeddingProfile],
        asset_source: EmbedAssetSource,
        vector_repo: S3VectorRepository,
        object_state_repo: ObjectStateRepository,
        execution_state_repo: ExecutionStateRepository | None = None,
        manifest_repo: ManifestRepository | None = None,
        projection_state_repo: EmbeddingProjectionStateRepository | None = None,
        version_cleanup_service: VersionCleanupService | None = None,
    ) -> None:
        if projection_state_repo is None and len(embedding_profiles) > 1:
            raise ValueError(
                "EMBEDDING_PROJECTION_STATE_TABLE is required when multiple write profiles are active"
            )
        self._embedding_clients = embedding_clients
        self._embedding_profiles = embedding_profiles
        self._asset_source = asset_source
        self._vector_repo = vector_repo
        self._object_state_repo = object_state_repo
        self._execution_state_repo = execution_state_repo
        self._manifest_repo = manifest_repo
        self._projection_state_repo = projection_state_repo
        self._version_cleanup_service = version_cleanup_service or self._build_default_version_cleanup_service()

    def _build_default_version_cleanup_service(self) -> VersionCleanupService:
        """
        EN: Build the default VersionCleanupService with current dependencies.
        CN: 使用当前依赖构建默认的 VersionCleanupService。
        """
        return VersionCleanupService(
            vector_repo=self._vector_repo,
            manifest_repo=self._manifest_repo,
            projection_state_repo=self._projection_state_repo,
            embedding_profiles=self._embedding_profiles,
        )

    def process(self, job: EmbeddingJobMessage) -> EmbeddingOutcome:
        """
        EN: Write vectors first, then reconcile previous-version cleanup, and finally mark the profile ready.
        CN: 先写入向量，再协调旧版本清理，最后标记 profile 就绪。

        Args:
            job:
                EN: Embedding job message containing source identity, profile, and embedding requests.
                CN: 包含源身份、profile 和嵌入请求的嵌入作业消息。

        Returns:
            EN: Embedding outcome with vector count and object state snapshot.
            CN: 包含向量数量和 object_state 快照的嵌入结果。

        Raises:
            EN: UnknownEmbeddingProfileError if the profile is not registered in this runtime.
            CN: 如果 profile 未在当前运行时注册，则抛出 UnknownEmbeddingProfileError。
        """
        validate_embedding_job_message(job)
        profile = self._require_profile(job.profile_id)
        client = self._require_client(job.profile_id)
        try:
            # EN: Mark embed status as running; single-profile uses object_state, multi-profile uses projection_state.
            # CN: 标记嵌入为运行中；单 profile 写 object_state，多个 profile 写 projection_state。
            self._mark_embed_running(job)
            # EN: Embed each request into a vector, then persist all vectors to S3 Vectors.
            # CN: 先将每个请求嵌入为向量，再把所有向量持久化到 S3 Vectors。
            vectors = [
                self._embed_request(job, profile, client, request, request_index=index)
                for index, request in enumerate(job.requests)
            ]
            self._vector_repo.put_vectors(job=job, profile=profile, vectors=vectors)
            # EN: Clean up previous-version vectors and projection state if a prior version exists.
            # CN: 如果存在上一版本，则清理上一版本的向量和 projection state。
            if job.previous_version_id:
                self._handle_version_cleanup(job=job, profile=profile)
            # EN: Complete object state and mark projection done, then attempt previous-version manifest cleanup.
            # CN: 完成 object_state 更新并标记 projection 完成，然后尝试清理旧版本 manifest。
            object_state = self._complete_object_state(job)
            outcome = EmbeddingOutcome(
                source=job.source,
                profile_id=job.profile_id,
                manifest_s3_uri=job.manifest_s3_uri,
                vector_count=len(vectors),
                object_state=object_state,
            )
            self._mark_embed_completed(job=job, outcome=outcome)
            if job.previous_version_id:
                self._handle_manifest_cleanup(job=job)
        except _EMBED_FAILURE_TYPES as exc:
            self._mark_embed_failed(job=job, profile=profile, exc=exc)
            raise
        return outcome

    def _mark_embed_running(self, job: EmbeddingJobMessage) -> None:
        """
        EN: Mark embed status as running in the appropriate state repository.
        CN: 在对应的状态仓储中将嵌入标记为运行中。
        """
        if self._execution_state_repo is not None:
            self._execution_state_repo.mark_embed_running(job.source)
        if self._projection_state_repo is None:
            self._object_state_repo.mark_embed_running(job.source)
        if self._projection_state_repo is not None:
            profile = self._require_profile(job.profile_id)
            self._projection_state_repo.mark_running(
                source=job.source,
                profile=profile,
                manifest_s3_uri=job.manifest_s3_uri,
            )

    def _handle_version_cleanup(self, *, job: EmbeddingJobMessage, profile: EmbeddingProfile) -> None:
        """
        EN: Handle cleanup of previous version vectors with error isolation.
        CN: 处理旧版本向量清理，带错误隔离。
        """
        try:
            self._version_cleanup_service.cleanup_previous_version_vectors(job=job, profile=profile)
        except _EMBED_FAILURE_TYPES as exc:
            emit_trace(
                "embed.cleanup_previous_version.failed",
                profile_id=job.profile_id,
                document_uri=job.source.document_uri,
                previous_version_id=job.previous_version_id,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            self._object_state_repo.mark_embed_cleanup_failed(job.source, str(exc))

    def _handle_manifest_cleanup(self, *, job: EmbeddingJobMessage) -> None:
        """
        EN: Handle manifest cleanup with error isolation, marking cleanup failure without propagating.
        CN: 处理 manifest 清理，带错误隔离，标记清理失败但不传播错误。
        """
        try:
            self._version_cleanup_service.cleanup_previous_manifest_if_complete(job=job)
        except _EMBED_FAILURE_TYPES as exc:
            emit_trace(
                "embed.cleanup_previous_manifest.failed",
                profile_id=job.profile_id,
                document_uri=job.source.document_uri,
                previous_version_id=job.previous_version_id,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            self._object_state_repo.mark_embed_cleanup_failed(job.source, str(exc))

    def _mark_embed_completed(self, *, job: EmbeddingJobMessage, outcome: EmbeddingOutcome) -> None:
        """
        EN: Mark embed as completed in the appropriate state repository.
        CN: 在对应的状态仓储中将嵌入标记为完成。
        """
        if self._projection_state_repo is not None:
            profile = self._require_profile(job.profile_id)
            self._projection_state_repo.mark_done(outcome=outcome, profile=profile)
        elif self._execution_state_repo is not None:
            self._execution_state_repo.mark_embed_done(job.source)

    def _mark_embed_failed(
        self,
        *,
        job: EmbeddingJobMessage,
        profile: EmbeddingProfile,
        exc: Exception,
    ) -> None:
        """
        EN: On failure, mark embed as failed in the appropriate state repository.
        CN: 失败时，在对应的状态仓储中将嵌入标记为失败。
        """
        if self._execution_state_repo is not None:
            self._execution_state_repo.mark_embed_failed(job.source, str(exc))
        if self._projection_state_repo is None:
            self._object_state_repo.mark_embed_failed(job.source, str(exc))
        if self._projection_state_repo is not None:
            self._projection_state_repo.mark_failed(
                source=job.source,
                profile=profile,
                manifest_s3_uri=job.manifest_s3_uri,
                error_message=str(exc),
            )

    def _embed_request(
        self,
        job: EmbeddingJobMessage,
        profile: EmbeddingProfile,
        client: _EmbeddingClient,
        request,
        *,
        request_index: int,
    ) -> VectorRecord:
        """
        EN: Embed a single request (text or asset) and produce a VectorRecord with full metadata.
        CN: 将单个请求（文本或资产）嵌入，并生成包含完整元数据的 VectorRecord。

        Args:
            job:
                EN: Parent embedding job message.
                CN: 父级嵌入作业消息。
            profile:
                EN: Target embedding profile.
                CN: 目标 embedding profile。
            client:
                EN: Provider-specific embedding client.
                CN: provider 专用嵌入客户端。
            request:
                EN: Individual embedding request from the job.
                CN: 来自作业的单个嵌入请求。
            request_index:
                EN: Zero-based index of this request within the job.
                CN: 该请求在作业中的零基索引。

        Returns:
            EN: VectorRecord with embedding data, key, and enriched metadata.
            CN: 包含嵌入数据、键和扩展元数据的 VectorRecord。
        """
        request_context = {
            "profile_id": profile.profile_id,
            "document_uri": job.source.document_uri,
            "chunk_id": request.chunk_id,
            "chunk_type": request.chunk_type,
            "content_kind": request.content_kind,
            "request_index": request_index,
            "mime_type": request.mime_type,
            "task_type": request.task_type,
        }
        request_start = monotonic()
        payload_size_bytes: int | None = None
        try:
            # EN: Route by content_kind - text goes directly, binary assets are loaded from S3 first.
            # CN: 按 content_kind 路由，文本直接嵌入，二进制资产先从 S3 加载。
            if request.content_kind == "text":
                if not request.text:
                    raise ValueError("Text embedding request requires text")
                payload_size_bytes = len(request.text.encode("utf-8"))
                emit_trace("embed.request.start", **request_context, payload_size_bytes=payload_size_bytes)
                data = client.embed_text(request)
            else:
                if not request.asset_s3_uri or not request.mime_type:
                    raise ValueError(f"Asset embedding request {request.chunk_id} is missing asset_s3_uri or mime_type")
                payload = self._asset_source.load_s3_uri(request.asset_s3_uri)
                payload_size_bytes = len(payload)
                emit_trace("embed.request.start", **request_context, payload_size_bytes=payload_size_bytes)
                data = client.embed_bytes(payload=payload, mime_type=request.mime_type, request=request)
        except _EMBED_FAILURE_TYPES as exc:
            emit_trace(
                "embed.request.failed",
                **request_context,
                payload_size_bytes=payload_size_bytes,
                elapsed_ms=round((monotonic() - request_start) * 1000, 2),
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            raise

        # EN: Enrich metadata with manifest reference and profile identity for query-time lookup.
        # CN: 先用 manifest 引用和 profile 身份补充元数据，供查询时查找。
        metadata = dict(request.metadata)
        metadata["manifest_s3_uri"] = job.manifest_s3_uri
        metadata["chunk_id"] = request.chunk_id
        metadata["chunk_type"] = request.chunk_type
        metadata["is_latest"] = True
        metadata["profile_id"] = profile.profile_id
        metadata["provider"] = profile.provider
        metadata["model"] = profile.model
        metadata["dimension"] = profile.dimension
        return VectorRecord(
            key=f"{job.profile_id}#{job.source.version_pk}#{request.chunk_id}",
            data=data,
            metadata=metadata,
        )

    def _complete_object_state(self, job: EmbeddingJobMessage) -> ObjectStateRecord:
        """
        EN: Finalize object_state - single-profile marks done directly, multi-profile reads current state.
        CN: 完成 object_state：单 profile 直接标记 done，多 profile 则读取当前状态。
        """
        if self._projection_state_repo is None:
            return self._object_state_repo.mark_embed_done(job.source)

        state = None
        if self._execution_state_repo is not None:
            state = self._execution_state_repo.get_state(object_pk=job.source.object_pk)
        if state is None:
            state = self._object_state_repo.get_state(object_pk=job.source.object_pk)
        if state is None:
            raise ValueError(f"object_state is missing for {job.source.document_uri}")
        return state

    def _require_profile(self, profile_id: str) -> EmbeddingProfile:
        """
        EN: Look up the embedding profile by ID or raise UnknownEmbeddingProfileError.
        CN: 按 ID 查找 embedding profile，找不到就抛出 UnknownEmbeddingProfileError。
        """
        profile = self._embedding_profiles.get(profile_id)
        if profile is None:
            raise UnknownEmbeddingProfileError(f"Unknown embedding profile: {profile_id}")
        return profile

    def _require_client(self, profile_id: str) -> _EmbeddingClient:
        """
        EN: Look up the embedding client for the given profile ID.
        CN: 按 profile ID 查找 embedding client。
        """
        client = self._embedding_clients.get(profile_id)
        if client is None:
            raise ValueError(f"Embedding client is not configured for profile: {profile_id}")
        return client


def _build_vector_keys(*, profile_id: str, manifest: ChunkManifest) -> list[str]:
    """
    EN: Derive all vector keys from a manifest's chunks and image-type assets for a given profile.
    CN: 生成给定 profile 的全部向量键。
    """
    keys = [f"{profile_id}#{manifest.source.version_pk}#{chunk.chunk_id}" for chunk in manifest.chunks]
    for asset in manifest.assets:
        if asset.chunk_type in {"page_image_chunk", "slide_image_chunk", "image_chunk"}:
            keys.append(f"{profile_id}#{manifest.source.version_pk}#{asset.asset_id}")
    return keys


def _is_missing_object_error(exc: ClientError) -> bool:
    """
    EN: Determine whether a ClientError indicates the S3 object does not exist.
    CN: 判断 ClientError 是否表示 S3 对象不存在。
    """
    error = exc.response.get("Error", {})
    code = str(error.get("Code", ""))
    return code in {"NoSuchKey", "404", "NotFound"}
