"""
EN: Tests for QueryService covering multi-profile fusion, corrupt manifest handling, projection state gating, and neighbor expansion.
CN: 同上。
"""

import pytest

from serverless_mcp.embed.vector_repository import VectorQueryMatch
from serverless_mcp.query import application as query_application_module
from serverless_mcp.query.application import QueryService
from serverless_mcp.domain.models import (
    ChunkManifest,
    ChunkManifestRecord,
    EmbeddingProfile,
    ExtractedAsset,
    ExtractedChunk,
    ObjectStateRecord,
    S3ObjectRef,
)
from botocore.exceptions import EndpointConnectionError


class _FakeGeminiClient:
    # EN: Stub Gemini embedding client returning fixed vectors.
    # CN: 同上。
    def embed_text(self, request):
        assert request.task_type == "RETRIEVAL_QUERY"
        return [0.1, 0.2]


class _FakeOpenAIClient:
    # EN: Stub OpenAI embedding client returning fixed vectors.
    # CN: 同上。
    def embed_text(self, request):
        assert request.task_type == "RETRIEVAL_QUERY"
        return [0.3, 0.4]


class _FailingQueryClient:
    # EN: Embedding client that raises RuntimeError on embed_text.
    # CN: 同上。
    def embed_text(self, request):
        raise RuntimeError("provider temporarily unavailable")


class _FakeVectorRepo:
    # EN: In-memory stand-in for S3VectorRepository.
    # CN: 同上。
    def query_vectors(self, *, profile, query_vector, top_k, metadata_filter):
        assert top_k == 5
        assert metadata_filter == {"tenant_id": {"$eq": "tenant-a"}}
        if profile.profile_id == "gemini-default":
            assert query_vector == [0.1, 0.2]
            return [
                VectorQueryMatch(
                    key="gemini-default#tenant-a#bucket-a#docs/guide.md#v2#chunk#000002",
                    chunk_id="chunk#000002",
                    manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
                    profile_id="gemini-default",
                    distance=0.01,
                    metadata={
                        "tenant_id": "tenant-a",
                        "bucket": "bucket-a",
                        "key": "docs/guide.md",
                        "version_id": "v2",
                        "is_latest": True,
                        "language": "zh",
                        "manifest_s3_uri": "s3://manifest-bucket/manifests/example.json",
                        "chunk_id": "chunk#000002",
                    },
                ),
                VectorQueryMatch(
                    key="gemini-default#tenant-a#bucket-a#docs/guide.md#v1#chunk#000001",
                    chunk_id="chunk#000001",
                    manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
                    profile_id="gemini-default",
                    distance=0.02,
                    metadata={
                        "tenant_id": "tenant-a",
                        "bucket": "bucket-a",
                        "key": "docs/guide.md",
                        "version_id": "v1",
                        "is_latest": False,
                        "language": "zh",
                        "manifest_s3_uri": "s3://manifest-bucket/manifests/example.json",
                        "chunk_id": "chunk#000001",
                    },
                ),
            ]
        assert query_vector == [0.3, 0.4]
        return [
            VectorQueryMatch(
                key="openai-text-small#tenant-a#bucket-a#docs/guide.md#v2#chunk#000002",
                chunk_id="chunk#000002",
                manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
                profile_id="openai-text-small",
                distance=0.01,
                metadata={
                    "tenant_id": "tenant-a",
                    "bucket": "bucket-a",
                    "key": "docs/guide.md",
                    "version_id": "v2",
                    "language": "zh",
                    "manifest_s3_uri": "s3://manifest-bucket/manifests/example.json",
                    "chunk_id": "chunk#000002",
                },
            ),
        ]


class _DuplicateProjectionVectorRepo:
    # EN: Vector repo returning duplicate projection keys across different chunks.
    # CN: 鍚屼笂銆?
    def query_vectors(self, *, profile, query_vector, top_k, metadata_filter):
        return [
            VectorQueryMatch(
                key="gemini-default#tenant-a#bucket-a#docs/guide.md#v2#chunk#000001",
                chunk_id="chunk#000001",
                manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
                profile_id="gemini-default",
                distance=0.01,
                metadata={
                    "tenant_id": "tenant-a",
                    "bucket": "bucket-a",
                    "key": "docs/guide.md",
                    "version_id": "v2",
                    "is_latest": True,
                    "manifest_s3_uri": "s3://manifest-bucket/manifests/example.json",
                    "chunk_id": "chunk#000001",
                },
            ),
            VectorQueryMatch(
                key="gemini-default#tenant-a#bucket-a#docs/guide.md#v2#chunk#000002",
                chunk_id="chunk#000002",
                manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
                profile_id="gemini-default",
                distance=0.02,
                metadata={
                    "tenant_id": "tenant-a",
                    "bucket": "bucket-a",
                    "key": "docs/guide.md",
                    "version_id": "v2",
                    "is_latest": True,
                    "manifest_s3_uri": "s3://manifest-bucket/manifests/example.json",
                    "chunk_id": "chunk#000002",
                },
            ),
        ]


class _RestrictedVectorRepo:
    # EN: Vector repo returning one restricted match and one public match.
    # CN: 返回一个受限匹配和一个公开匹配的 vector repo。
    def query_vectors(self, *, profile, query_vector, top_k, metadata_filter):
        return [
            VectorQueryMatch(
                key="gemini-default#tenant-a#bucket-a#docs/secret.md#v2#chunk#000002",
                chunk_id="chunk#000002",
                manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
                profile_id="gemini-default",
                distance=0.01,
                metadata={
                    "tenant_id": "tenant-a",
                    "bucket": "bucket-a",
                    "key": "docs/secret.md",
                    "version_id": "v2",
                    "is_latest": True,
                    "security_scope": ["team-a"],
                    "manifest_s3_uri": "s3://manifest-bucket/manifests/example.json",
                    "chunk_id": "chunk#000002",
                },
            ),
            VectorQueryMatch(
                key="gemini-default#tenant-a#bucket-a#docs/public.md#v2#chunk#000003",
                chunk_id="chunk#000003",
                manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
                profile_id="gemini-default",
                distance=0.02,
                metadata={
                    "tenant_id": "tenant-a",
                    "bucket": "bucket-a",
                    "key": "docs/public.md",
                    "version_id": "v2",
                    "is_latest": True,
                    "manifest_s3_uri": "s3://manifest-bucket/manifests/example.json",
                    "chunk_id": "chunk#000003",
                },
            ),
        ]


class _FakeManifestRepo:
    # EN: In-memory stand-in for ManifestRepository.
    # CN: 同上。
    def load_manifest(self, manifest_s3_uri):
        source = S3ObjectRef(
            tenant_id="tenant-a",
            bucket="bucket-a",
            key="docs/guide.md",
            version_id="v2",
        )
        return ChunkManifest(
            source=source,
            doc_type="md",
            chunks=[
                ExtractedChunk(
                    chunk_id="chunk#000001",
                    chunk_type="section_text_chunk",
                    text="first",
                    doc_type="md",
                    token_estimate=1,
                    metadata={"source_format": "markdown"},
                ),
                ExtractedChunk(
                    chunk_id="chunk#000002",
                    chunk_type="section_text_chunk",
                    text="second",
                    doc_type="md",
                    token_estimate=1,
                    metadata={"source_format": "markdown"},
                ),
                ExtractedChunk(
                    chunk_id="chunk#000003",
                    chunk_type="section_text_chunk",
                    text="third",
                    doc_type="md",
                    token_estimate=1,
                    metadata={"source_format": "markdown"},
                ),
            ],
            metadata={"source_format": "markdown", "section_count": 3},
        )


class _ProjectionManifestRepo(_FakeManifestRepo):
    # EN: Manifest repo that serves projection records without needing the full manifest.
    # CN: 鐢ㄤ簬 projection records 鐨勬柊 manifest repo 鏇胯韓銆?
    def __init__(self) -> None:
        self.projection_calls: list[tuple[str, str]] = []

    def list_version_records(self, *, source, version_id):
        self.projection_calls.append((source.object_pk, version_id))
        return [
            ChunkManifestRecord(
                pk=f"{source.object_pk}#{version_id}",
                sk="chunk#000001",
                tenant_id=source.tenant_id,
                bucket=source.bucket,
                key=source.key,
                version_id=version_id,
                chunk_id="chunk#000001",
                chunk_type="section_text_chunk",
                doc_type="md",
                is_latest=True,
                security_scope=(),
                language="zh",
                page_no=1,
                text_preview="projection first",
                manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
            ),
            ChunkManifestRecord(
                pk=f"{source.object_pk}#{version_id}",
                sk="chunk#000002",
                tenant_id=source.tenant_id,
                bucket=source.bucket,
                key=source.key,
                version_id=version_id,
                chunk_id="chunk#000002",
                chunk_type="section_text_chunk",
                doc_type="md",
                is_latest=True,
                security_scope=(),
                language="zh",
                page_no=2,
                text_preview="projection second",
                manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
            ),
        ]

    def load_manifest(self, manifest_s3_uri):
        raise AssertionError("full manifest load should not be needed when projection records are available")


class _RetryingManifestRepo:
    # EN: Manifest repo that fails once before returning a valid manifest.
    # CN: 先失败一次然后返回有效 manifest 的仓储替身。
    def __init__(self) -> None:
        self.calls = 0

    def load_manifest(self, manifest_s3_uri):
        self.calls += 1
        if self.calls == 1:
            raise EndpointConnectionError(endpoint_url=manifest_s3_uri)
        return _FakeManifestRepo().load_manifest(manifest_s3_uri)


class _FakeObjectStateRepo:
    # EN: In-memory stand-in for ObjectStateRepository.
    # CN: 同上。
    def __init__(self, *, embed_status="INDEXED", is_deleted=False):
        self._embed_status = embed_status
        self._is_deleted = is_deleted

    def get_state(self, *, object_pk):
        return ObjectStateRecord(
            pk=object_pk,
            latest_version_id="v2",
            latest_sequencer="002",
            extract_status="EXTRACTED",
            embed_status=self._embed_status,
            is_deleted=self._is_deleted,
        )

    def get_states_batch(self, *, object_pks):
        return {pk: self.get_state(object_pk=pk) for pk in object_pks}


class _MissingObjectStateRepo:
    # EN: Object state repo returning None for all lookups.
    # CN: 同上。
    def get_state(self, *, object_pk):
        return None

    def get_states_batch(self, *, object_pks):
        return {pk: None for pk in object_pks}


class _FakeProjectionStateRepo:
    # EN: Stand-in for EmbeddingProjectionStateRepository.
    # CN: 同上。
    def get_state(self, *, object_pk, version_id, profile_id):
        class _Record:
            # EN: Stand-in for projection state record.
            # CN: 同上。
            query_status = "INDEXED"

        return _Record()


class _BatchProjectionStateRepo:
    # EN: Projection state repo that records batch lookups.
    # CN: 记录 batch 查询的 projection state repo 替身。
    def __init__(self) -> None:
        self.batch_keys: list[tuple[str, str, str]] = []

    def get_states_batch(self, *, keys):
        self.batch_keys.extend(keys)
        return {
            key: _FakeProjectionStateRepo().get_state(
                object_pk=key[0],
                version_id=key[1],
                profile_id=key[2],
            )
            for key in keys
        }


def test_query_service_uses_batch_projection_state_reads() -> None:
    """
    EN: Query service should batch load projection states when the repository supports it.
    CN: QueryService 在仓库支持时应批量加载 projection state。
    """
    batch_repo = _BatchProjectionStateRepo()
    service = QueryService(
        embedding_clients={
            "gemini-default": _FakeGeminiClient(),
            "openai-text-small": _FakeOpenAIClient(),
        },
        query_profiles=_build_profiles(),
        vector_repo=_FakeVectorRepo(),
        manifest_repo=_FakeManifestRepo(),
        object_state_repo=_FakeObjectStateRepo(),
        projection_state_repo=batch_repo,
    )

    result = service.search(
        query="hello",
        tenant_id="tenant-a",
        top_k=5,
        neighbor_expand=1,
    )

    assert len(result.results) == 1
    assert set(batch_repo.batch_keys) == {
        ("tenant-a#bucket-a#docs%2Fguide.md", "v2", "gemini-default"),
        ("tenant-a#bucket-a#docs%2Fguide.md", "v1", "gemini-default"),
    }


def test_query_service_forwards_projection_state_batch_keys() -> None:
    """
    EN: Query service should forward raw projection batch keys and let the repository dedupe.
    CN: QueryService 搴旇鐩存帴杞彂 projection batch key锛岀敱浠撳簱杩涜鍘婚噸銆?
    """
    batch_repo = _BatchProjectionStateRepo()
    service = QueryService(
        embedding_clients={"gemini-default": _FakeGeminiClient()},
        query_profiles=(
            EmbeddingProfile(
                profile_id="gemini-default",
                provider="gemini",
                model="gemini-embedding-2-preview",
                dimension=3072,
                vector_bucket_name="vector-bucket",
                vector_index_name="index-gemini",
                supported_content_kinds=("text", "image"),
            ),
        ),
        vector_repo=_DuplicateProjectionVectorRepo(),
        manifest_repo=_FakeManifestRepo(),
        object_state_repo=_FakeObjectStateRepo(),
        projection_state_repo=batch_repo,
    )

    result = service.search(
        query="hello",
        tenant_id="tenant-a",
        top_k=5,
        neighbor_expand=1,
    )

    assert len(result.results) == 2
    assert batch_repo.batch_keys == [
        ("tenant-a#bucket-a#docs%2Fguide.md", "v2", "gemini-default"),
        ("tenant-a#bucket-a#docs%2Fguide.md", "v2", "gemini-default"),
    ]


def _build_profiles():
    return (
        EmbeddingProfile(
            profile_id="gemini-default",
            provider="gemini",
            model="gemini-embedding-2-preview",
            dimension=3072,
            vector_bucket_name="vector-bucket",
            vector_index_name="index-gemini",
            supported_content_kinds=("text", "image"),
        ),
        EmbeddingProfile(
            profile_id="openai-text-small",
            provider="openai",
            model="text-embedding-3-small",
            dimension=1536,
            vector_bucket_name="vector-bucket",
            vector_index_name="index-openai",
            supported_content_kinds=("text",),
        ),
    )


def test_query_service_filters_stale_versions_and_expands_neighbors() -> None:
    """
    EN: Query service filters stale versions and expands neighbors.
    CN: 楠岃瘉 QueryService 杩囨护杩囨湡鐗堟湰骞舵墿灞曢偦灞呫€?
    """
    service = QueryService(
        embedding_clients={
            "gemini-default": _FakeGeminiClient(),
            "openai-text-small": _FakeOpenAIClient(),
        },
        query_profiles=_build_profiles(),
        vector_repo=_FakeVectorRepo(),
        manifest_repo=_FakeManifestRepo(),
        object_state_repo=_FakeObjectStateRepo(),
        projection_state_repo=_FakeProjectionStateRepo(),
    )

    result = service.search(
        query="hello",
        tenant_id="tenant-a",
        top_k=5,
        neighbor_expand=1,
    )

    assert len(result.results) == 1
    hit = result.results[0]
    assert hit.source.version_id == "v2"
    assert hit.match.text == "second"
    assert [neighbor.text for neighbor in hit.neighbors] == ["first", "third"]
    assert hit.metadata["__fusion_score__"] > 0
    assert hit.metadata["__profile_hits__"] == 2
    assert hit.metadata["version_id"] == "v2"
    assert hit.metadata["is_latest"] is True
    assert "bucket" not in hit.metadata
    assert "key" not in hit.metadata
    assert "security_scope" not in hit.metadata


def test_query_service_prefers_projection_records_over_full_manifest_load() -> None:
    """
    EN: Query service should use manifest projection records before loading the full manifest.
    CN: QueryService 搭閿熸枻鎷峰垯浼氬厛鐢?projection records锛屽啀璇诲彇瀹屾暣 manifest銆?
    """
    manifest_repo = _ProjectionManifestRepo()
    service = QueryService(
        embedding_clients={"gemini-default": _FakeGeminiClient()},
        query_profiles=(_build_profiles()[0],),
        vector_repo=_FakeVectorRepo(),
        manifest_repo=manifest_repo,
        object_state_repo=_FakeObjectStateRepo(),
        projection_state_repo=_FakeProjectionStateRepo(),
    )

    result = service.search(
        query="hello",
        tenant_id="tenant-a",
        top_k=5,
        neighbor_expand=1,
    )

    assert len(result.results) == 1
    assert result.results[0].match.text == "projection second"
    assert [neighbor.text for neighbor in result.results[0].neighbors] == ["projection first"]
    assert manifest_repo.projection_calls == [("tenant-a#bucket-a#docs%2Fguide.md", "v2")]


def test_query_service_does_not_filter_restricted_vectors_by_security_scope() -> None:
    """
    EN: Query service should no longer gate vector access on security scope metadata.
    CN: QueryService 不再应该用 security scope 元数据控制向量访问。
    """
    service = QueryService(
        embedding_clients={"gemini-default": _FakeGeminiClient()},
        query_profiles=(_build_profiles()[0],),
        vector_repo=_RestrictedVectorRepo(),
        manifest_repo=_FakeManifestRepo(),
        object_state_repo=_FakeObjectStateRepo(),
        projection_state_repo=_FakeProjectionStateRepo(),
    )

    result = service.search(
        query="hello",
        tenant_id="tenant-a",
        top_k=5,
        neighbor_expand=0,
    )

    assert [hit.key for hit in result.results] == [
        "gemini-default#tenant-a#bucket-a#docs/secret.md#v2#chunk#000002",
        "gemini-default#tenant-a#bucket-a#docs/public.md#v2#chunk#000003",
    ]


def test_query_service_keeps_successful_profiles_when_one_profile_fails() -> None:
    """
    EN: Query service keeps successful profiles when one profile fails.
    CN: 楠岃瘉涓€涓?profile 澶辫触鏃?QueryService 淇濈暀鎴愬姛鐨?profile銆?
    """
    service = QueryService(
        embedding_clients={
            "gemini-default": _FakeGeminiClient(),
            "openai-text-small": _FailingQueryClient(),
        },
        query_profiles=_build_profiles(),
        vector_repo=_FakeVectorRepo(),
        manifest_repo=_FakeManifestRepo(),
        object_state_repo=_FakeObjectStateRepo(),
        projection_state_repo=_FakeProjectionStateRepo(),
    )

    result = service.search(
        query="hello",
        tenant_id="tenant-a",
        top_k=5,
        neighbor_expand=1,
    )

    assert len(result.results) == 1
    assert result.results[0].source.version_id == "v2"
    assert result.degraded_profiles[0].profile_id == "openai-text-small"
    assert result.degraded_profiles[0].stage == "profile_query"
    assert "provider temporarily unavailable" in result.degraded_profiles[0].error


class _CorruptManifestRepo:
    # EN: Manifest repo that raises on corrupt entries.
    # CN: 同上。
    def __init__(self) -> None:
        self.calls: list[str] = []

    def load_manifest(self, manifest_s3_uri):
        self.calls.append(manifest_s3_uri)
        if manifest_s3_uri.endswith("bad.json"):
            raise RuntimeError("manifest is corrupt")
        source = S3ObjectRef(
            tenant_id="tenant-a",
            bucket="bucket-a",
            key="docs/guide.md",
            version_id="v2",
        )
        return ChunkManifest(
            source=source,
            doc_type="md",
            chunks=[
                ExtractedChunk(
                    chunk_id="chunk#000002",
                    chunk_type="section_text_chunk",
                    text="second",
                    doc_type="md",
                    token_estimate=1,
                    metadata={"source_format": "markdown"},
                )
            ],
            metadata={"source_format": "markdown"},
        )


class _MixedManifestVectorRepo:
    # EN: Vector repo returning mixed good and bad manifest matches.
    # CN: 杩斿洖娣峰悎濂藉潖 manifest 鍛戒腑鐨?vector repo銆?
    def query_vectors(self, *, profile, query_vector, top_k, metadata_filter):
        return [
            VectorQueryMatch(
                key="gemini-default#tenant-a#bucket-a#docs/guide.md#v2#chunk#000001",
                chunk_id="chunk#000001",
                manifest_s3_uri="s3://manifest-bucket/manifests/bad.json",
                profile_id="gemini-default",
                distance=0.01,
                metadata={
                    "tenant_id": "tenant-a",
                    "bucket": "bucket-a",
                    "key": "docs/guide.md",
                    "version_id": "v2",
                    "is_latest": True,
                    "language": "zh",
                    "manifest_s3_uri": "s3://manifest-bucket/manifests/bad.json",
                    "chunk_id": "chunk#000001",
                },
            ),
            VectorQueryMatch(
                key="gemini-default#tenant-a#bucket-a#docs/guide.md#v2#chunk#000002",
                chunk_id="chunk#000002",
                manifest_s3_uri="s3://manifest-bucket/manifests/good.json",
                profile_id="gemini-default",
                distance=0.02,
                metadata={
                    "tenant_id": "tenant-a",
                    "bucket": "bucket-a",
                    "key": "docs/guide.md",
                    "version_id": "v2",
                    "is_latest": True,
                    "language": "zh",
                    "manifest_s3_uri": "s3://manifest-bucket/manifests/good.json",
                    "chunk_id": "chunk#000002",
                },
            ),
        ]


def test_query_service_skips_corrupt_manifests_without_failing_the_whole_query() -> None:
    """
    EN: Query service skips corrupt manifests without failing the whole query.
    CN: 同上。
    """
    manifest_repo = _CorruptManifestRepo()
    service = QueryService(
        embedding_clients={"gemini-default": _FakeGeminiClient()},
        query_profiles=(_build_profiles()[0],),
        vector_repo=_MixedManifestVectorRepo(),
        manifest_repo=manifest_repo,
        object_state_repo=_FakeObjectStateRepo(),
        projection_state_repo=_FakeProjectionStateRepo(),
    )

    result = service.search(
        query="hello",
        tenant_id="tenant-a",
        top_k=5,
        neighbor_expand=0,
    )

    assert len(result.results) == 1
    assert result.results[0].match.text == "second"
    assert result.degraded_profiles[0].stage == "manifest_load"
    assert result.degraded_profiles[0].manifest_s3_uri == "s3://manifest-bucket/manifests/bad.json"


def test_query_service_retries_transient_manifest_reads() -> None:
    """
    EN: Query service retries transient manifest reads before surfacing the result.
    CN: QueryService 会先重试瞬时 manifest 读取，再返回结果。
    """
    manifest_repo = _RetryingManifestRepo()
    service = QueryService(
        embedding_clients={"gemini-default": _FakeGeminiClient()},
        query_profiles=(_build_profiles()[0],),
        vector_repo=_FakeVectorRepo(),
        manifest_repo=manifest_repo,
        object_state_repo=_FakeObjectStateRepo(),
        projection_state_repo=_FakeProjectionStateRepo(),
    )

    result = service.search(
        query="hello",
        tenant_id="tenant-a",
        top_k=5,
        neighbor_expand=1,
    )

    assert len(result.results) == 1
    assert manifest_repo.calls == 2


def test_retry_read_rejects_impossible_zero_attempt_configuration() -> None:
    """
    EN: retry_read rejects impossible zero-attempt configuration.
    CN: retry_read 会拒绝不可能的零次重试配置。
    """
    with pytest.raises(RuntimeError, match="impossible state"):
        query_application_module.retry_read(lambda: None, label="manifest", max_attempts=0)


class _FakeAssetVectorRepo:
    # EN: Vector repo returning asset-based matches.
    # CN: 杩斿洖鍩轰簬璧勪骇鍖归厤鐨?vector repo銆?
    def query_vectors(self, *, profile, query_vector, top_k, metadata_filter):
        return [
            VectorQueryMatch(
                key="gemini-default#tenant-a#bucket-a#docs/guide.md#v2#asset#000001",
                chunk_id="asset#000001",
                manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
                profile_id="gemini-default",
                distance=0.01,
                metadata={
                    "tenant_id": "tenant-a",
                    "bucket": "bucket-a",
                    "key": "docs/guide.md",
                    "version_id": "v2",
                    "language": "zh",
                },
            )
        ]


class _FakeAssetManifestRepo:
    # EN: Manifest repo returning an asset-only manifest.
    # CN: 杩斿洖浠呭惈璧勪骇鐨?manifest 鐨?manifest repo銆?
    def load_manifest(self, manifest_s3_uri):
        source = S3ObjectRef(
            tenant_id="tenant-a",
            bucket="bucket-a",
            key="docs/guide.md",
            version_id="v2",
        )
        return ChunkManifest(
            source=source,
            doc_type="md",
            chunks=[],
            assets=[
                ExtractedAsset(
                    asset_id="asset#000001",
                    chunk_type="page_image_chunk",
                    mime_type="image/png",
                    asset_s3_uri="s3://manifest-bucket/assets/asset#000001.png",
                    page_no=1,
                )
            ],
        )


def test_query_service_does_not_expose_asset_s3_uri() -> None:
    """
    EN: Query service does not expose asset s3 uri.
    CN: 楠岃瘉 QueryService 涓嶆毚闇茶祫浜?S3 URI銆?
    """
    service = QueryService(
        embedding_clients={"gemini-default": _FakeGeminiClient()},
        query_profiles=(_build_profiles()[0],),
        vector_repo=_FakeAssetVectorRepo(),
        manifest_repo=_FakeAssetManifestRepo(),
        object_state_repo=_FakeObjectStateRepo(),
        projection_state_repo=_FakeProjectionStateRepo(),
    )

    result = service.search(
        query="show image",
        tenant_id="tenant-a",
        top_k=5,
        neighbor_expand=0,
    )

    assert len(result.results) == 1
    assert result.results[0].match.asset_s3_uri is None


def test_query_service_caps_neighbor_expansion_for_large_requests() -> None:
    """
    EN: Query service caps neighbor expansion so large requests still return bounded context.
    CN: QueryService 会限制 neighbor expansion，避免大请求返回过大的上下文。
    """
    service = QueryService(
        embedding_clients={"gemini-default": _FakeGeminiClient()},
        query_profiles=(_build_profiles()[0],),
        vector_repo=_FakeVectorRepo(),
        manifest_repo=_FakeManifestRepo(),
        object_state_repo=_FakeObjectStateRepo(),
        projection_state_repo=_FakeProjectionStateRepo(),
    )

    result = service.search(
        query="hello",
        tenant_id="tenant-a",
        top_k=5,
        neighbor_expand=999,
    )

    assert len(result.results) == 1
    assert [neighbor.text for neighbor in result.results[0].neighbors] == ["first", "third"]


class _PendingProjectionStateRepo:
    # EN: Projection state repo returning PENDING status.
    # CN: 杩斿洖 PENDING 鐘舵€佺殑鎶曞奖鐘舵€?repo銆?
    def get_state(self, *, object_pk, version_id, profile_id):
        class _Record:
            # EN: Stand-in for projection state record.
            # CN: 同上。
            query_status = "PENDING"

        return _Record()


class _DeletedProjectionStateRepo:
    # EN: Projection state repo returning DELETED status.
    # CN: 杩斿洖 DELETED 鐘舵€佺殑鎶曞奖鐘舵€?repo銆?
    def get_state(self, *, object_pk, version_id, profile_id):
        class _Record:
            # EN: Stand-in for projection state record.
            # CN: 同上。
            query_status = "DELETED"

        return _Record()


class _MissingProjectionStateRepo:
    # EN: Projection state repo returning None for all queries.
    # CN: 鎵€鏈夋煡璇㈤兘杩斿洖 None 鐨勬姇褰辩姸鎬?repo銆?
    def get_state(self, *, object_pk, version_id, profile_id):
        return None


class _MissingGovernanceVectorRepo:
    # EN: Vector repo returning results with configurable is_latest flag.
    # CN: 同上。
    def __init__(self, *, is_latest: bool):
        self._is_latest = is_latest

    def query_vectors(self, *, profile, query_vector, top_k, metadata_filter):
        assert profile.profile_id == "gemini-default"
        return [
            VectorQueryMatch(
                key="gemini-default#tenant-a#bucket-a#docs/guide.md#v2#chunk#000002",
                chunk_id="chunk#000002",
                manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
                profile_id="gemini-default",
                distance=0.01,
                metadata={
                    "tenant_id": "tenant-a",
                    "bucket": "bucket-a",
                    "key": "docs/guide.md",
                    "version_id": "v2",
                    "is_latest": self._is_latest,
                    "language": "zh",
                    "manifest_s3_uri": "s3://manifest-bucket/manifests/example.json",
                    "chunk_id": "chunk#000002",
                },
            )
        ]


def test_query_service_skips_deleted_or_not_ready_results() -> None:
    """
    EN: Query service skips deleted or not ready results.
    CN: 同上。
    """
    service = QueryService(
        embedding_clients={"gemini-default": _FakeGeminiClient()},
        query_profiles=(_build_profiles()[0],),
        vector_repo=_FakeVectorRepo(),
        manifest_repo=_FakeManifestRepo(),
        object_state_repo=_FakeObjectStateRepo(is_deleted=True),
        projection_state_repo=_PendingProjectionStateRepo(),
    )

    result = service.search(
        query="hello",
        tenant_id="tenant-a",
        top_k=5,
        neighbor_expand=1,
    )

    assert result.results == []


def test_query_service_skips_projection_records_marked_deleted() -> None:
    """
    EN: Query service skips projection records marked deleted.
    CN: 同上。
    """
    service = QueryService(
        embedding_clients={"gemini-default": _FakeGeminiClient()},
        query_profiles=(_build_profiles()[0],),
        vector_repo=_FakeVectorRepo(),
        manifest_repo=_FakeManifestRepo(),
        object_state_repo=_FakeObjectStateRepo(embed_status="INDEXED"),
        projection_state_repo=_DeletedProjectionStateRepo(),
    )

    result = service.search(
        query="hello",
        tenant_id="tenant-a",
        top_k=5,
        neighbor_expand=1,
    )

    assert result.results == []


def test_query_service_allows_projection_ready_results_when_global_embed_status_is_pending() -> None:
    """
    EN: Query service allows projection ready results when global embed status is pending.
    CN: 同上。
    """
    service = QueryService(
        embedding_clients={"gemini-default": _FakeGeminiClient()},
        query_profiles=(_build_profiles()[0],),
        vector_repo=_FakeVectorRepo(),
        manifest_repo=_FakeManifestRepo(),
        object_state_repo=_FakeObjectStateRepo(embed_status="PENDING"),
        projection_state_repo=_FakeProjectionStateRepo(),
    )

    result = service.search(
        query="hello",
        tenant_id="tenant-a",
        top_k=5,
        neighbor_expand=1,
    )

    assert len(result.results) == 1
    assert result.results[0].source.version_id == "v2"


def test_query_service_allows_latest_vectors_without_projection_record_when_projection_state_table_is_enabled() -> None:
    """
    EN: Query service allows lavectors without projection record when projection state table is enabled.
    CN: 同上。
    """
    service = QueryService(
        embedding_clients={"gemini-default": _FakeGeminiClient()},
        query_profiles=(_build_profiles()[0],),
        vector_repo=_FakeVectorRepo(),
        manifest_repo=_FakeManifestRepo(),
        object_state_repo=_FakeObjectStateRepo(embed_status="PENDING"),
        projection_state_repo=_MissingProjectionStateRepo(),
    )

    result = service.search(
        query="hello",
        tenant_id="tenant-a",
        top_k=5,
        neighbor_expand=1,
    )

    assert len(result.results) == 1
    assert result.results[0].source.version_id == "v2"


def test_query_service_allows_latest_vectors_when_governance_tables_are_missing() -> None:
    """
    EN: Query service allows lavectors when governance tables are missing.
    CN: 同上。
    """
    service = QueryService(
        embedding_clients={"gemini-default": _FakeGeminiClient()},
        query_profiles=(_build_profiles()[0],),
        vector_repo=_MissingGovernanceVectorRepo(is_latest=True),
        manifest_repo=_FakeManifestRepo(),
        object_state_repo=_MissingObjectStateRepo(),
        projection_state_repo=_MissingProjectionStateRepo(),
    )

    result = service.search(
        query="hello",
        tenant_id="tenant-a",
        top_k=5,
        neighbor_expand=1,
    )

    assert len(result.results) == 1
    assert result.results[0].source.version_id == "v2"


def test_query_service_rejects_non_latest_vectors_when_governance_tables_are_missing() -> None:
    """
    EN: Query service rejects non lavectors when governance tables are missing.
    CN: 楠岃瘉娌荤悊琛ㄧ己澶辨椂鎷掔粷闈炴渶鏂板悜閲忋€?
    """
    service = QueryService(
        embedding_clients={"gemini-default": _FakeGeminiClient()},
        query_profiles=(_build_profiles()[0],),
        vector_repo=_MissingGovernanceVectorRepo(is_latest=False),
        manifest_repo=_FakeManifestRepo(),
        object_state_repo=_MissingObjectStateRepo(),
        projection_state_repo=_MissingProjectionStateRepo(),
    )

    result = service.search(
        query="hello",
        tenant_id="tenant-a",
        top_k=5,
        neighbor_expand=1,
    )

    assert result.results == []


def test_query_service_requires_global_embed_status_without_projection_state_table() -> None:
    """
    EN: Query service requires global embed status without projection state table.
    CN: 楠岃瘉鏃犳姇褰辩姸鎬佽〃鏃堕渶瑕佸叏灞€ embed 鐘舵€併€?
    """
    service = QueryService(
        embedding_clients={"gemini-default": _FakeGeminiClient()},
        query_profiles=(_build_profiles()[0],),
        vector_repo=_FakeVectorRepo(),
        manifest_repo=_FakeManifestRepo(),
        object_state_repo=_FakeObjectStateRepo(embed_status="PENDING"),
    )

    result = service.search(
        query="hello",
        tenant_id="tenant-a",
        top_k=5,
        neighbor_expand=1,
    )

    assert result.results == []


def test_query_service_shutdowns_executor_without_waiting_on_timeout(monkeypatch) -> None:
    """
    EN: Query service should stop waiting on timed-out profile work before shutting down the executor.
    CN: QueryService 在 profile 超时时应停止等待，再无阻塞地关闭 executor。
    """
    class _FakeExecutor:
        def __init__(self, *args, **kwargs):
            self.shutdown_calls = []
            self.futures = []

        def submit(self, fn, **kwargs):
            class _Future:
                def __init__(self, *, profile_index: int, timed_out: bool) -> None:
                    self.profile_index = profile_index
                    self.timed_out = timed_out
                    self.cancelled = False

                def result(self):
                    if self.timed_out:
                        raise AssertionError("timed-out future should not be awaited")
                    return (
                        self.profile_index,
                        [
                            VectorQueryMatch(
                                key="gemini-default#tenant-a#bucket-a#docs/guide.md#v2#chunk#000002",
                                chunk_id="chunk#000002",
                                manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
                                profile_id="gemini-default",
                                distance=0.01,
                                metadata={
                                    "tenant_id": "tenant-a",
                                    "bucket": "bucket-a",
                                    "key": "docs/guide.md",
                                    "version_id": "v2",
                                    "is_latest": True,
                                    "manifest_s3_uri": "s3://manifest-bucket/manifests/example.json",
                                    "chunk_id": "chunk#000002",
                                },
                            )
                        ],
                    )

                def cancel(self):
                    self.cancelled = True
                    return True

            future = _Future(profile_index=kwargs["profile_index"], timed_out=kwargs["profile_index"] == 1)
            self.futures.append(future)
            return future

        def shutdown(self, *, wait, cancel_futures):
            self.shutdown_calls.append((wait, cancel_futures))

    fake_executor = _FakeExecutor()

    def _fake_thread_pool_executor(*args, **kwargs):
        return fake_executor

    def _fake_wait(futures, timeout):
        return {fake_executor.futures[0]}, {fake_executor.futures[1]}

    monkeypatch.setattr(query_application_module, "ThreadPoolExecutor", _fake_thread_pool_executor)
    monkeypatch.setattr(query_application_module, "wait", _fake_wait)

    service = QueryService(
        embedding_clients={
            "gemini-default": _FakeGeminiClient(),
            "openai-text-small": _FakeOpenAIClient(),
        },
        query_profiles=_build_profiles(),
        vector_repo=_FakeVectorRepo(),
        manifest_repo=_FakeManifestRepo(),
        object_state_repo=_FakeObjectStateRepo(),
        projection_state_repo=_FakeProjectionStateRepo(),
        profile_timeout_seconds=0.01,
    )

    result = service.search(query="hello", tenant_id="tenant-a", top_k=5, neighbor_expand=1)

    assert len(result.results) == 1
    assert fake_executor.shutdown_calls == [(False, True)]
    assert fake_executor.futures[1].cancelled is True
