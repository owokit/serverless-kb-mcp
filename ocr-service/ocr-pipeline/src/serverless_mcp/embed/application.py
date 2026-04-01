"""
EN: Embed worker that coordinates profile-scoped embedding calls and S3 Vectors persistence.
CN: 鍗忚皟鎸?profile 鍒掑垎鐨勫祵鍏ヨ皟鐢ㄥ拰 S3 Vectors 鎸佷箙鍖栫殑宓屽叆宸ヤ綔鍣ㄣ€?
"""
from __future__ import annotations

from time import monotonic
from typing import Protocol

from botocore.exceptions import ClientError

from serverless_mcp.embed.asset_source import EmbedAssetSource
from serverless_mcp.runtime.observability import emit_trace
from serverless_mcp.domain.embedding_schema import validate_embedding_job_message
from serverless_mcp.domain.models import EmbeddingJobMessage, EmbeddingOutcome, EmbeddingProfile, ObjectStateRecord, VectorRecord
from serverless_mcp.storage.state.execution_state_repository import ExecutionStateRepository
from serverless_mcp.storage.projection.repository import EmbeddingProjectionStateRepository
from serverless_mcp.storage.manifest.repository import ManifestRepository
from serverless_mcp.storage.state.object_state_repository import ObjectStateRepository


_EMBED_FAILURE_TYPES = (ClientError, KeyError, OSError, RuntimeError, TypeError, ValueError)


class UnknownEmbeddingProfileError(ValueError):
    """
    EN: Raised when an embed job references a profile that is no longer active in the current runtime.
    CN: 褰撳祵鍏ヤ綔涓氬紩鐢ㄤ簡褰撳墠杩愯鏃朵笉鍐嶆椿璺冪殑 profile 鏃舵姏鍑恒€?
    """


class _EmbeddingClient(Protocol):
    """
    EN: Structural protocol for provider-specific embedding clients.
    CN: 鍚?provider 涓撶敤宓屽叆瀹㈡埛绔殑缁撴瀯鍗忚銆?
    """

    def embed_text(self, request) -> list[float]: ...

    def embed_bytes(self, *, payload: bytes, mime_type: str, request) -> list[float]: ...


class VersionCleanupService:
    """
    EN: Handle cleanup of previous version projection state and manifests.
    CN: ????????? projection state ??manifest ?????

    This service is responsible for:
    1. Cleaning up projection state records for the previous version
    2. Deleting previous version manifests after all profiles have indexed
    ?????????
    1. ????????????????????
    2. ?????profile ???????????????????manifest
    """
    def __init__(
        self,
        *,
        manifest_repo: ManifestRepository | None,
        projection_state_repo: EmbeddingProjectionStateRepository | None,
        embedding_profiles: dict[str, EmbeddingProfile],
    ) -> None:
        self._manifest_repo = manifest_repo
        self._projection_state_repo = projection_state_repo
        self._embedding_profiles = embedding_profiles

    def cleanup_previous_version_state(
        self,
        *,
        job: EmbeddingJobMessage,
    ) -> None:
        """
        EN: Record the need for previous-version cleanup without executing it in Lambda.
        CN: ?????Lambda ???????????????????????????????
        """
        if not job.previous_version_id:
            return
        return

    def cleanup_previous_manifest_if_complete(
        self,
        *,
        job: EmbeddingJobMessage,
    ) -> None:
        """
        EN: Delete the previous version's manifest artifacts only when all active profiles have indexed the new version.
        CN: 鍙湁褰撴墍鏈夊惎鐢ㄥ啓鍏ョ殑 profile 閮藉璇ョ増鏈揪鍒?INDEXED 鏃舵墠鍒犻櫎鏃?manifest 浜х墿銆?

        Args:
            job:
                EN: Embedding job message containing version information.
                CN: 鍖呭惈鐗堟湰淇℃伅鐨?embedding 浣滀笟娑堟伅銆?
        """
        if not job.previous_version_id or not self._manifest_repo:
            return
        if not self._is_version_complete(job.source.object_pk, job.source.version_id):
            return
        previous_manifest_s3_uri = job.previous_manifest_s3_uri or self._manifest_repo.find_manifest_s3_uri(
            source=job.source,
            version_id=job.previous_version_id,
        )
        self._manifest_repo.delete_previous_version_artifacts(
            source=job.source,
            previous_version_id=job.previous_version_id,
            previous_manifest_s3_uri=previous_manifest_s3_uri,
        )

    def _is_version_complete(self, object_pk: str, version_id: str) -> bool:
        """
        EN: Check whether all write-enabled profiles have reached INDEXED status for this version.
        CN: 妫€鏌ユ墍鏈夊惎鐢ㄥ啓鍏ョ殑 profile 鏄惁宸茶揪鍒拌鐗堟湰鐨?INDEXED 鐘舵€併€?
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
    EN: Process one profile-scoped embedding job, then reconcile previous-version cleanup once the new vectors are durably written.
    CN: ????????profile ????????????????????????????????????????
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
        CN: 浣跨敤褰撳墠渚濊禆鏋勫缓榛樿鐨?VersionCleanupService銆?
        """
        return VersionCleanupService(
            manifest_repo=self._manifest_repo,
            projection_state_repo=self._projection_state_repo,
            embedding_profiles=self._embedding_profiles,
        )

    def process(self, job: EmbeddingJobMessage) -> EmbeddingOutcome:
        """
        EN: Write vectors first, then reconcile previous-version cleanup, and finally mark the profile ready.
        CN: 鍏堝啓鍏ュ悜閲忥紝鍐嶅崗璋冩棫鐗堟湰娓呯悊锛屾渶鍚庢爣璁?profile 灏辩华銆?

        Args:
            job:
                EN: Embedding job message containing source identity, profile, and embedding requests.
                CN: 鍖呭惈婧愯韩浠姐€乸rofile 鍜屽祵鍏ヨ姹傜殑宓屽叆浣滀笟娑堟伅銆?

        Returns:
            EN: Embedding outcome with vector count and object state snapshot.
            CN: 鍖呭惈鍚戦噺鏁伴噺鍜?object_state 蹇収鐨勫祵鍏ョ粨鏋溿€?

        Raises:
            EN: UnknownEmbeddingProfileError if the profile is not registered in this runtime.
            CN: 濡傛灉 profile 鏈湪褰撳墠杩愯鏃舵敞鍐岋紝鍒欐姏鍑?UnknownEmbeddingProfileError銆?
        """
        validate_embedding_job_message(job)
        profile = self._require_profile(job.profile_id)
        client = self._require_client(job.profile_id)
        try:
            # EN: Mark embed status as running; single-profile uses object_state, multi-profile uses projection_state.
            # CN: 鏍囪宓屽叆涓鸿繍琛屼腑锛涘崟 profile 鍐?object_state锛屽涓?profile 鍐?projection_state銆?
            self._mark_embed_running(job)
            # EN: Embed each request into a vector, then persist all vectors to S3 Vectors.
            # CN: 鍏堝皢姣忎釜璇锋眰宓屽叆涓哄悜閲忥紝鍐嶆妸鎵€鏈夊悜閲忔寔涔呭寲鍒?S3 Vectors銆?
            vectors = [
                self._embed_request(job, profile, client, request, request_index=index)
                for index, request in enumerate(job.requests)
            ]
            self._vector_repo.put_vectors(job=job, profile=profile, vectors=vectors)
            # EN: Clean up previous-version vectors and projection state if a prior version exists.
            # CN: 濡傛灉瀛樺湪涓婁竴鐗堟湰锛屽垯娓呯悊涓婁竴鐗堟湰鐨勫悜閲忓拰 projection state銆?
            if job.previous_version_id:
                self._handle_version_cleanup(job=job)
            # EN: Complete object state and mark projection done, then attempt previous-version manifest cleanup.
            # CN: 瀹屾垚 object_state 鏇存柊骞舵爣璁?projection 瀹屾垚锛岀劧鍚庡皾璇曟竻鐞嗘棫鐗堟湰 manifest銆?
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
        CN: 鍦ㄥ搴旂殑鐘舵€佷粨鍌ㄤ腑灏嗗祵鍏ユ爣璁颁负杩愯涓€?
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

    def _handle_version_cleanup(self, *, job: EmbeddingJobMessage) -> None:
        """
        EN: Handle cleanup of previous version vectors with error isolation.
        CN: 澶勭悊鏃х増鏈悜閲忔竻鐞嗭紝甯﹂敊璇殧绂汇€?
        """
        try:
            self._version_cleanup_service.cleanup_previous_version_state(job=job)
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
        CN: 澶勭悊 manifest 娓呯悊锛屽甫閿欒闅旂锛屾爣璁版竻鐞嗗け璐ヤ絾涓嶄紶鎾敊璇€?
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
        CN: 鍦ㄥ搴旂殑鐘舵€佷粨鍌ㄤ腑灏嗗祵鍏ユ爣璁颁负瀹屾垚銆?
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
        CN: 澶辫触鏃讹紝鍦ㄥ搴旂殑鐘舵€佷粨鍌ㄤ腑灏嗗祵鍏ユ爣璁颁负澶辫触銆?
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
        CN: 灏嗗崟涓姹傦紙鏂囨湰鎴栬祫浜э級宓屽叆锛屽苟鐢熸垚鍖呭惈瀹屾暣鍏冩暟鎹殑 VectorRecord銆?

        Args:
            job:
                EN: Parent embedding job message.
                CN: 鐖剁骇宓屽叆浣滀笟娑堟伅銆?
            profile:
                EN: Target embedding profile.
                CN: 鐩爣 embedding profile銆?
            client:
                EN: Provider-specific embedding client.
                CN: provider 涓撶敤宓屽叆瀹㈡埛绔€?
            request:
                EN: Individual embedding request from the job.
                CN: 鏉ヨ嚜浣滀笟鐨勫崟涓祵鍏ヨ姹傘€?
            request_index:
                EN: Zero-based index of this request within the job.
                CN: 璇ヨ姹傚湪浣滀笟涓殑闆跺熀绱㈠紩銆?

        Returns:
            EN: VectorRecord with embedding data, key, and enriched metadata.
            CN: 鍖呭惈宓屽叆鏁版嵁銆侀敭鍜屾墿灞曞厓鏁版嵁鐨?VectorRecord銆?
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
            # CN: 鎸?content_kind 璺敱锛屾枃鏈洿鎺ュ祵鍏ワ紝浜岃繘鍒惰祫浜у厛浠?S3 鍔犺浇銆?
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
        # CN: 鍏堢敤 manifest 寮曠敤鍜?profile 韬唤琛ュ厖鍏冩暟鎹紝渚涙煡璇㈡椂鏌ユ壘銆?
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
        CN: 瀹屾垚 object_state锛氬崟 profile 鐩存帴鏍囪 done锛屽 profile 鍒欒鍙栧綋鍓嶇姸鎬併€?
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
        CN: 鎸?ID 鏌ユ壘 embedding profile锛屾壘涓嶅埌灏辨姏鍑?UnknownEmbeddingProfileError銆?
        """
        profile = self._embedding_profiles.get(profile_id)
        if profile is None:
            raise UnknownEmbeddingProfileError(f"Unknown embedding profile: {profile_id}")
        return profile

    def _require_client(self, profile_id: str) -> _EmbeddingClient:
        """
        EN: Look up the embedding client for the given profile ID.
        CN: 鎸?profile ID 鏌ユ壘 embedding client銆?
        """
        client = self._embedding_clients.get(profile_id)
        if client is None:
            raise ValueError(f"Embedding client is not configured for profile: {profile_id}")
        return client

