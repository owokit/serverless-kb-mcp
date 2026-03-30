"""
EN: Query service for profile-aware semantic search with S3 Vectors and manifest-based neighbor expansion.
CN: 同上。
"""
from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, wait
from threading import Lock
from typing import Protocol

from botocore.exceptions import ClientError

from serverless_mcp.embed.vector_repository import S3VectorRepository, VectorQueryMatch
from serverless_mcp.domain.models import (
    EmbeddingProfile,
    EmbeddingRequest,
    QueryDegradedProfile,
    EmbeddingProjectionStateRecord,
    ObjectStateRecord,
    QueryResponse,
    QueryResultItem,
    S3ObjectRef,
)
from serverless_mcp.storage.state.execution_state_repository import ExecutionStateRepository
from serverless_mcp.storage.manifest.repository import ManifestRepository
from serverless_mcp.storage.projection.repository import EmbeddingProjectionStateRepository
from serverless_mcp.storage.state.object_state_repository import ObjectStateRepository
from serverless_mcp.query.access import (
    is_queryable_object_state,
    is_queryable_projection_state,
    metadata_is_truthy,
    metadata_security_scope,
    record_degraded_profile,
    sanitize_result_metadata,
    security_scope_allows_access,
)
from serverless_mcp.query.fusion import RankedCandidate, build_metadata_filter, resolve_context, source_from_metadata
from serverless_mcp.query.retry import retry_read


_PROFILE_QUERY_FAILURE_TYPES = (ClientError, KeyError, OSError, RuntimeError, TypeError, ValueError)
_MANIFEST_LOAD_FAILURE_TYPES = (ClientError, KeyError, OSError, RuntimeError, TypeError, ValueError)


class _EmbeddingClient(Protocol):
    """
    EN: Structural protocol for query-time embedding clients.
    CN: 同上。
    """

    def embed_text(self, request: EmbeddingRequest) -> list[float]: ...


class QueryService:
    """
    EN: Execute profile-aware semantic search and fuse profile-specific rankings with fallback latest-version validation.
    CN: 同上。
    """

    def __init__(
        self,
        *,
        embedding_clients: dict[str, _EmbeddingClient],
        query_profiles: tuple[EmbeddingProfile, ...],
        vector_repo: S3VectorRepository,
        manifest_repo: ManifestRepository,
        object_state_repo: ObjectStateRepository,
        execution_state_repo: ExecutionStateRepository | None = None,
        projection_state_repo: EmbeddingProjectionStateRepository | None = None,
        profile_timeout_seconds: float | None = None,
        query_embedding_cache_size: int = 256,
    ) -> None:
        self._embedding_clients = embedding_clients
        self._query_profiles = query_profiles
        self._vector_repo = vector_repo
        self._manifest_repo = manifest_repo
        self._object_state_repo = object_state_repo
        self._execution_state_repo = execution_state_repo
        self._projection_state_repo = projection_state_repo
        self._profile_timeout_seconds = profile_timeout_seconds
        self._query_embedding_cache_size = max(1, query_embedding_cache_size)
        self._query_embedding_cache: OrderedDict[tuple[str, str], list[float]] = OrderedDict()
        self._query_embedding_cache_lock = Lock()

    def search(
        self,
        *,
        query: str,
        tenant_id: str,
        top_k: int = 10,
        neighbor_expand: int = 1,
        security_scope: tuple[str, ...] = (),
        doc_type: str | None = None,
        key: str | None = None,
    ) -> QueryResponse:
        """
        EN: Search across enabled profiles and fuse the ranked matches with reciprocal rank fusion.
        CN: 同上。
        """
        enabled_profiles = [
            profile
            for profile in self._query_profiles
            if profile.enable_query and profile.profile_id in self._embedding_clients
        ]
        if not enabled_profiles:
            return QueryResponse(query=query, results=[])

        metadata_filter = build_metadata_filter(
            tenant_id=tenant_id,
            doc_type=doc_type,
            key=key,
        )
        ranked_candidates: dict[str, RankedCandidate] = {}
        profile_results: list[tuple[int, list[VectorQueryMatch]]] = []
        degraded_profiles: list[QueryDegradedProfile] = []
        degraded_keys: set[tuple[str, str, str | None]] = set()

        executor = ThreadPoolExecutor(max_workers=min(len(enabled_profiles), 4))
        try:
            futures = [
                (
                    profile_index,
                    profile,
                    executor.submit(
                        self._search_profile,
                        profile_index=profile_index,
                        profile=profile,
                        query=query,
                        top_k=top_k,
                        metadata_filter=metadata_filter,
                    ),
                )
                for profile_index, profile in enumerate(enabled_profiles)
            ]
            done, _not_done = wait([item[2] for item in futures], timeout=self._profile_timeout_seconds)
            for profile_index, profile, future in futures:
                if future in done:
                    try:
                        profile_index, matches = future.result()
                    except _PROFILE_QUERY_FAILURE_TYPES as exc:
                        record_degraded_profile(
                            degraded_profiles,
                            degraded_keys,
                            profile_id=profile.profile_id,
                            stage="profile_query",
                            error=str(exc),
                        )
                        continue
                    profile_results.append((profile_index, matches))
                    continue
                future.cancel()
                record_degraded_profile(
                    degraded_profiles,
                    degraded_keys,
                    profile_id=profile.profile_id,
                    stage="profile_timeout",
                    error=(
                        f"profile query timed out after {self._profile_timeout_seconds} seconds"
                        if self._profile_timeout_seconds is not None
                        else "profile query did not complete"
                    ),
                )
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        for _, matches in sorted(profile_results, key=lambda item: item[0]):
            self._accumulate_rrf(
                profile_matches=matches,
                ranked_candidates=ranked_candidates,
            )

        manifest_cache: dict[str, object] = {}
        state_cache: dict[str, ObjectStateRecord | None] = {}
        projection_cache: dict[tuple[str, str, str], EmbeddingProjectionStateRecord | None] = {}
        manifest_failures: set[str] = set()
        results: list[QueryResultItem] = []

        sorted_candidates = sorted(ranked_candidates.values(), key=lambda item: item.rrf_score, reverse=True)[:top_k]
        unique_object_pks = list({candidate.source.object_pk for candidate in sorted_candidates})
        if unique_object_pks:
            batch_states = self._load_execution_states_batch(object_pks=unique_object_pks)
            state_cache.update(batch_states)
        if self._projection_state_repo is not None and sorted_candidates:
            projection_cache.update(
                self._load_projection_states_batch(
                    keys=[
                        (candidate.source.object_pk, candidate.source.version_id, candidate.match.profile_id)
                        for candidate in sorted_candidates
                    ]
                )
            )

        for candidate in sorted_candidates:
            metadata = sanitize_result_metadata(candidate.match.metadata)
            source = candidate.source
            candidate_security_scope = metadata_security_scope(candidate.match.metadata)
            if not security_scope_allows_access(candidate_security_scope, security_scope):
                continue
            vector_is_latest = metadata_is_truthy(candidate.match.metadata, "is_latest")
            object_state = state_cache.get(source.object_pk)
            if object_state is None:
                if not vector_is_latest:
                    continue
            elif not is_queryable_object_state(
                object_state,
                source,
                require_global_embed_status=self._projection_state_repo is None,
            ):
                continue

            projection_key = (source.object_pk, source.version_id, candidate.match.profile_id)
            if projection_key not in projection_cache:
                projection_cache[projection_key] = self._load_projection_state(source, candidate.match.profile_id)
            projection_state = projection_cache[projection_key]
            if self._projection_state_repo is not None:
                if projection_state is None:
                    if not vector_is_latest:
                        continue
                elif not is_queryable_projection_state(
                    projection_state,
                    require_projection_state=True,
                ):
                    continue

            manifest_s3_uri = candidate.match.manifest_s3_uri
            manifest = manifest_cache.get(manifest_s3_uri)
            if manifest is None:
                if manifest_s3_uri in manifest_failures:
                    continue
                try:
                    manifest = retry_read(
                        lambda: self._manifest_repo.load_manifest(manifest_s3_uri),
                        label="manifest",
                        resource_id=manifest_s3_uri,
                    )
                except _MANIFEST_LOAD_FAILURE_TYPES as exc:
                    manifest_failures.add(manifest_s3_uri)
                    record_degraded_profile(
                        degraded_profiles,
                        degraded_keys,
                        profile_id=candidate.match.profile_id,
                        stage="manifest_load",
                        error=str(exc),
                        manifest_s3_uri=manifest_s3_uri,
                    )
                    continue
                manifest_cache[manifest_s3_uri] = manifest

            context = resolve_context(manifest, candidate.match.chunk_id, neighbor_expand)
            if context is None:
                continue

            results.append(
                QueryResultItem(
                    key=candidate.match.key,
                    distance=candidate.match.distance,
                    source=source,
                    manifest_s3_uri=None,
                    metadata={
                        **metadata,
                        "__fusion_score__": candidate.rrf_score,
                        "__profile_hits__": candidate.profile_hits,
                    },
                    match=context["match"],
                    neighbors=context["neighbors"],
                )
            )

        return QueryResponse(query=query, results=results, degraded_profiles=tuple(degraded_profiles))

    def _search_profile(
        self,
        *,
        profile_index: int,
        profile: EmbeddingProfile,
        query: str,
        top_k: int,
        metadata_filter: dict,
    ) -> tuple[int, list[VectorQueryMatch]]:
        """
        EN: Query one embedding profile and return the index with ranked matches.
        CN: 同上。
        """
        client = self._embedding_clients.get(profile.profile_id)
        if client is None:
            return profile_index, []

        query_vector = self._embed_query(profile=profile, client=client, query=query)
        matches = self._vector_repo.query_vectors(
            profile=profile,
            query_vector=query_vector,
            top_k=top_k,
            metadata_filter=metadata_filter,
        )
        return profile_index, matches

    def _embed_query(self, *, profile: EmbeddingProfile, client: _EmbeddingClient, query: str) -> list[float]:
        """
        EN: Embed the query text using the profile's client, with LRU caching.
        CN: 同上。
        """
        cache_key = (profile.profile_id, query)
        with self._query_embedding_cache_lock:
            cached_vector = self._query_embedding_cache.get(cache_key)
            if cached_vector is not None:
                self._query_embedding_cache.move_to_end(cache_key)
                return list(cached_vector)

            query_vector = client.embed_text(
                EmbeddingRequest(
                    chunk_id="query",
                    chunk_type="section_text_chunk",
                    content_kind="text",
                    text=query,
                    output_dimensionality=profile.dimension,
                    task_type="RETRIEVAL_QUERY",
                )
            )
            cached_vector = list(query_vector)
            existing_vector = self._query_embedding_cache.get(cache_key)
            if existing_vector is not None:
                self._query_embedding_cache.move_to_end(cache_key)
                return list(existing_vector)
            self._query_embedding_cache[cache_key] = cached_vector
            self._query_embedding_cache.move_to_end(cache_key)
            while len(self._query_embedding_cache) > self._query_embedding_cache_size:
                self._query_embedding_cache.popitem(last=False)
            return list(cached_vector)

    def _accumulate_rrf(
        self,
        *,
        profile_matches: list[VectorQueryMatch],
        ranked_candidates: dict[str, RankedCandidate],
    ) -> None:
        """
        EN: Accumulate reciprocal rank fusion scores for one profile result set.
        CN: 同上。
        """
        for rank, match in enumerate(profile_matches, start=1):
            metadata = dict(match.metadata)
            source = source_from_metadata(metadata)
            dedupe_key = f"{source.version_pk}#{match.chunk_id}"
            rrf_score = 1.0 / (60 + rank)
            existing = ranked_candidates.get(dedupe_key)
            if existing is None:
                ranked_candidates[dedupe_key] = RankedCandidate(
                    match=match,
                    source=source,
                    rrf_score=rrf_score,
                )
                continue
            existing.rrf_score += rrf_score
            existing.profile_hits += 1
            if match.distance is not None and (
                existing.match.distance is None or match.distance < existing.match.distance
            ):
                existing.match = match

    def _load_projection_state(
        self,
        source: S3ObjectRef,
        profile_id: str,
    ) -> EmbeddingProjectionStateRecord | None:
        """
        EN: Load the per-profile projection state when that governance table is configured.
        CN: 同上。
        """
        if self._projection_state_repo is None:
            return None
        return self._projection_state_repo.get_state(
            object_pk=source.object_pk,
            version_id=source.version_id,
            profile_id=profile_id,
        )

    def _load_projection_states_batch(
        self,
        *,
        keys: list[tuple[str, str, str]],
    ) -> dict[tuple[str, str, str], EmbeddingProjectionStateRecord | None]:
        """
        EN: Batch load projection states when the repository supports it, otherwise fall back per key.
        CN: 当仓库支持时批量读取 projection state，否则按 key 回退。
        """
        if self._projection_state_repo is None or not keys:
            return {}
        batch_loader = getattr(self._projection_state_repo, "get_states_batch", None)
        if callable(batch_loader):
            try:
                return batch_loader(keys=keys)
            except TypeError:
                pass
        return {
            key: self._projection_state_repo.get_state(
                object_pk=key[0],
                version_id=key[1],
                profile_id=key[2],
            )
            for key in keys
        }

    def _load_execution_state(self, *, object_pk: str) -> ObjectStateRecord | None:
        """
        EN: Load execution-state first, then fall back to object_state when necessary.
        CN: 优先读取 execution-state，必要时再回退到 object_state。
        """
        if self._execution_state_repo is not None:
            return self._execution_state_repo.get_state(object_pk=object_pk)
        return self._object_state_repo.get_state(object_pk=object_pk)

    def _load_execution_states_batch(self, *, object_pks: list[str]) -> dict[str, ObjectStateRecord | None]:
        """
        EN: Batch load execution states for multiple object PKs, falling back to object_state when necessary.
        CN: 批量读取多个 object PK 的 execution-state，必要时回退到 object_state。
        """
        if not object_pks:
            return {}
        if self._execution_state_repo is not None:
            return self._execution_state_repo.get_states_batch(object_pks=object_pks)
        return self._object_state_repo.get_states_batch(object_pks=object_pks)
