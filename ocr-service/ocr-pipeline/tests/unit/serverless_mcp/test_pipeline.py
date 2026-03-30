"""
EN: Tests for ExtractionResultPersister covering manifest persistence, embed job dispatch, and fan-out by profile.
CN: 鍚屼笂銆?
"""

from serverless_mcp.embed.dispatcher import build_jobs_for_profiles
from serverless_mcp.extract.result_persister import ExtractionResultPersister
from serverless_mcp.extract.state_commit import ExtractionStateCommitter
from serverless_mcp.domain.models import (
    ChunkManifest,
    EmbeddingProfile,
    EmbeddingRequest,
    ExtractedChunk,
    ObjectStateRecord,
    PersistedManifest,
    S3ObjectRef,
)
from serverless_mcp.storage.state.object_state_repository import DuplicateOrStaleEventError


class _FakeExtractionService:
    # EN: Stand-in for ExtractionService returning a fixed manifest.
    # CN: 鏉╂柨娲栭崶鍝勭暰 manifest 閻?ExtractionService 閺囪儻闊╅妴?
    def build_embedding_requests(self, manifest, *, manifest_s3_uri):
        return [
            EmbeddingRequest(
                chunk_id="chunk#000001",
                chunk_type="page_text_chunk",
                content_kind="text",
                text="hello",
                metadata={
                    "tenant_id": manifest.source.tenant_id,
                    "bucket": manifest.source.bucket,
                    "key": manifest.source.key,
                    "version_id": manifest.source.version_id,
                    "document_uri": manifest.source.document_uri,
                    "security_scope": list(manifest.source.security_scope),
                    "language": manifest.source.language,
                    "doc_type": manifest.doc_type,
                    "source_format": manifest.metadata.get("source_format", manifest.doc_type),
                    "manifest_s3_uri": manifest_s3_uri,
                    "is_latest": True,
                },
            )
        ]


class _FakeManifestRepo:
    # EN: In-memory stand-in for ManifestRepository.
    # CN: 鍚屼笂銆?
    def __init__(self) -> None:
        self.cleanup_calls = []
        self.persist_calls = []
        self.rollback_calls = []

    def delete_previous_version_artifacts(self, *, source, previous_version_id=None, previous_manifest_s3_uri=None):
        self.cleanup_calls.append((source.document_uri, previous_version_id, previous_manifest_s3_uri))

    def persist_manifest(self, manifest, *, previous_version_id=None):
        self.persist_calls.append((manifest.source.document_uri, previous_version_id))
        self.persisted = manifest
        self.previous_version_id = previous_version_id
        return PersistedManifest(
            manifest=manifest,
            manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
        )

    def rollback_manifest(self, manifest, *, manifest_s3_uri, previous_version_id=None):
        self.rollback_calls.append((manifest.source.document_uri, manifest_s3_uri, previous_version_id))


class _FakeObjectStateRepo:
    def __init__(
        self,
        *,
        current_state: ObjectStateRecord | None = None,
        mark_extract_done_error: Exception | None = None,
    ) -> None:
        self.calls = []
        self.current_state = current_state
        self.mark_extract_done_error = mark_extract_done_error

    def get_state(self, *, object_pk: str):
        return self.current_state

    def mark_extract_done(self, source, manifest_s3_uri):
        self.calls.append(("mark_extract_done", source.document_uri, manifest_s3_uri))
        if self.mark_extract_done_error is not None:
            raise self.mark_extract_done_error
        return ObjectStateRecord(
            pk=source.object_pk,
            latest_version_id=source.version_id,
            latest_sequencer=source.sequencer,
            extract_status="EXTRACTED",
            embed_status="PENDING",
            latest_manifest_s3_uri=manifest_s3_uri,
        )


class _FakeDispatcher:
    # EN: Captures dispatched embedding jobs for assertion.
    # CN: 鍚屼笂銆?
    def __init__(self, *, fail: bool = False):
        self.jobs = []
        self.fail = fail

    def dispatch_many(self, jobs):
        if self.fail:
            raise RuntimeError("dispatch failed")
        self.jobs.extend(jobs)

    def dispatch_for_profiles(
        self,
        *,
        source,
        trace_id: str,
        manifest_s3_uri: str,
        requests,
        profiles,
        previous_version_id,
        previous_manifest_s3_uri,
    ) -> int:
        jobs = build_jobs_for_profiles(
            source=source,
            trace_id=trace_id,
            manifest_s3_uri=manifest_s3_uri,
            requests=requests,
            profiles=profiles,
            previous_version_id=previous_version_id,
            previous_manifest_s3_uri=previous_manifest_s3_uri,
        )
        self.dispatch_many(jobs)
        return len(jobs)


def _make_state_committer(object_state_repo, execution_state_repo=None) -> ExtractionStateCommitter:
    return ExtractionStateCommitter(
        object_state_repo=object_state_repo,
        execution_state_repo=execution_state_repo,
    )


def test_persister_persists_manifest_and_dispatches_embed_job() -> None:
    """
    EN: Persister persists manifest and dispatches embed job.
    CN: 妤犲矁鐦?persister 閹镐椒绠欓崠?manifest 楠炶泛鍨庨崣?embed job閵?
    """
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/guide.pdf", version_id="v1")
    dispatcher = _FakeDispatcher()
    persister = ExtractionResultPersister(
        extraction_service=_FakeExtractionService(),
        state_committer=_make_state_committer(_FakeObjectStateRepo()),
        manifest_repo=_FakeManifestRepo(),
        embed_dispatcher=dispatcher,
        embedding_profiles=(
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
    )

    outcome = persister.persist(
        source=source,
        manifest=ChunkManifest(
            source=source,
            doc_type="pdf",
            chunks=[
                ExtractedChunk(
                    chunk_id="chunk#000001",
                    chunk_type="page_text_chunk",
                    text="hello",
                    doc_type="pdf",
                    token_estimate=2,
                    page_no=1,
                    page_span=(1, 1),
                )
            ],
        ),
        trace_id="trace-1",
    )

    assert outcome.object_state.extract_status == "EXTRACTED"
    assert outcome.object_state.embed_status == "PENDING"
    assert outcome.embedding_request_count == 1
    assert len(dispatcher.jobs) == 1
    assert dispatcher.jobs[0].manifest_s3_uri == "s3://manifest-bucket/manifests/example.json"
    assert dispatcher.jobs[0].profile_id == "gemini-default"
    assert dispatcher.jobs[0].previous_version_id is None
    assert dispatcher.jobs[0].previous_manifest_s3_uri is None



def test_persister_skips_stale_state_before_manifest_persistence() -> None:
    """
    EN: Persister should short-circuit when the current state is already complete for another version.
    CN: 当前状态已完成且不是同一版本时，persister 应直接跳过，不再持久化 manifest。
    """
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/guide.pdf", version_id="v1")
    current_state = ObjectStateRecord(
        pk=source.object_pk,
        latest_version_id="v2",
        latest_sequencer="0002",
        extract_status="EXTRACTED",
        embed_status="PENDING",
        latest_manifest_s3_uri="s3://manifest-bucket/manifests/v2.json",
    )
    manifest_repo = _FakeManifestRepo()
    dispatcher = _FakeDispatcher()
    persister = ExtractionResultPersister(
        extraction_service=_FakeExtractionService(),
        state_committer=_make_state_committer(_FakeObjectStateRepo(current_state=current_state)),
        manifest_repo=manifest_repo,
        embed_dispatcher=dispatcher,
        embedding_profiles=(
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
    )

    outcome = persister.persist(
        source=source,
        manifest=ChunkManifest(
            source=source,
            doc_type="pdf",
            chunks=[
                ExtractedChunk(
                    chunk_id="chunk#000001",
                    chunk_type="page_text_chunk",
                    text="hello",
                    doc_type="pdf",
                    token_estimate=2,
                    page_no=1,
                    page_span=(1, 1),
                )
            ],
        ),
        trace_id="trace-1",
    )

    assert outcome.chunk_count == 0
    assert outcome.asset_count == 0
    assert outcome.embedding_request_count == 0
    assert outcome.object_state is current_state
    assert manifest_repo.persist_calls == []
    assert dispatcher.jobs == []


def test_persister_skips_when_mark_extract_done_detects_duplicate_or_stale_event() -> None:
    """
    EN: Persister should surface a skip outcome when the final state write loses a race.
    CN: 当最终状态写入丢失竞争时，persister 应返回跳过结果而不是抛错。
    """
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/guide.pdf", version_id="v1")
    current_state = ObjectStateRecord(
        pk=source.object_pk,
        latest_version_id=source.version_id,
        latest_sequencer=source.sequencer,
        extract_status="EXTRACTING",
        embed_status="PENDING",
    )
    object_state_repo = _FakeObjectStateRepo(
        current_state=current_state,
        mark_extract_done_error=DuplicateOrStaleEventError(source.document_uri),
    )
    dispatcher = _FakeDispatcher()
    persister = ExtractionResultPersister(
        extraction_service=_FakeExtractionService(),
        state_committer=_make_state_committer(object_state_repo),
        manifest_repo=_FakeManifestRepo(),
        embed_dispatcher=dispatcher,
        embedding_profiles=(
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
    )

    outcome = persister.persist(
        source=source,
        manifest=ChunkManifest(
            source=source,
            doc_type="pdf",
            chunks=[
                ExtractedChunk(
                    chunk_id="chunk#000001",
                    chunk_type="page_text_chunk",
                    text="hello",
                    doc_type="pdf",
                    token_estimate=2,
                )
            ],
        ),
        trace_id="trace-1",
    )

    assert outcome.chunk_count == 0
    assert outcome.asset_count == 0
    assert outcome.embedding_request_count == 0
    assert outcome.object_state.latest_version_id == source.version_id
    assert object_state_repo.calls == [("mark_extract_done", source.document_uri, "s3://manifest-bucket/manifests/example.json")]
    assert len(dispatcher.jobs) == 1


def test_persister_rolls_back_manifest_when_embedding_request_build_fails() -> None:
    """
    EN: Persist should roll back a manifest if request construction fails after the manifest is written.
    CN: 当 manifest 已写入但后续请求构建失败时，persist 应回滚 manifest。
    """
    class _FailingExtractionService:
        def build_embedding_requests(self, manifest, *, manifest_s3_uri):
            raise ValueError("bad embedding request")

    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/guide.pdf", version_id="v1")
    manifest_repo = _FakeManifestRepo()
    persister = ExtractionResultPersister(
        extraction_service=_FailingExtractionService(),
        state_committer=_make_state_committer(_FakeObjectStateRepo(
            current_state=ObjectStateRecord(
                pk=source.object_pk,
                latest_version_id=source.version_id,
                latest_sequencer=source.sequencer,
                extract_status="EXTRACTING",
                embed_status="PENDING",
            )
        )),
        manifest_repo=manifest_repo,
        embed_dispatcher=_FakeDispatcher(),
        embedding_profiles=(
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
    )

    try:
        persister.persist(
            source=source,
            manifest=ChunkManifest(
                source=source,
                doc_type="pdf",
                chunks=[
                    ExtractedChunk(
                        chunk_id="chunk#000001",
                        chunk_type="page_text_chunk",
                        text="hello",
                        doc_type="pdf",
                        token_estimate=2,
                    )
                ],
            ),
            trace_id="trace-1",
        )
    except ValueError as exc:
        assert str(exc) == "bad embedding request"
    else:
        raise AssertionError("embedding request validation failure should surface to the caller")

    assert manifest_repo.persist_calls == [(source.document_uri, None)]
    assert manifest_repo.rollback_calls == [(source.document_uri, "s3://manifest-bucket/manifests/example.json", None)]


def test_persister_passes_previous_version_id_to_manifest_repo() -> None:
    """
    EN: Persister passes previous version id to manifest repo.
    CN: 妤犲矁鐦?persister 鐏?previous_version_id 娴肩娀鈧帞绮?manifest repo閵?
    """
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/guide.pdf", version_id="v1")
    manifest_repo = _FakeManifestRepo()
    persister = ExtractionResultPersister(
        extraction_service=_FakeExtractionService(),
        state_committer=_make_state_committer(_FakeObjectStateRepo()),
        manifest_repo=manifest_repo,
        embed_dispatcher=_FakeDispatcher(),
        embedding_profiles=(
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
    )

    persister.persist(
        source=source,
        manifest=ChunkManifest(
            source=source,
            doc_type="pdf",
            chunks=[
                ExtractedChunk(
                    chunk_id="chunk#000001",
                    chunk_type="page_text_chunk",
                    text="hello",
                    doc_type="pdf",
                    token_estimate=2,
                    page_no=1,
                    page_span=(1, 1),
                )
            ],
        ),
        trace_id="trace-1",
        previous_version_id="v0",
        previous_manifest_s3_uri="s3://manifest-bucket/manifests/v0.json",
    )

    assert manifest_repo.cleanup_calls == []
    assert manifest_repo.persist_calls == [(source.document_uri, "v0")]
    assert manifest_repo.previous_version_id == "v0"
    assert persister._embed_dispatcher.jobs[0].previous_manifest_s3_uri == "s3://manifest-bucket/manifests/v0.json"


def test_persister_fans_out_jobs_by_profile() -> None:
    """
    EN: Persister fans out jobs by profile.
    CN: 妤犲矁鐦?persister 閹?profile 閹靛洤鍤?embed job閵?
    """
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/guide.pdf", version_id="v1")
    dispatcher = _FakeDispatcher()
    persister = ExtractionResultPersister(
        extraction_service=_FakeExtractionService(),
        state_committer=_make_state_committer(_FakeObjectStateRepo()),
        manifest_repo=_FakeManifestRepo(),
        embed_dispatcher=dispatcher,
        embedding_profiles=(
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
        ),
    )

    persister.persist(
        source=source,
        manifest=ChunkManifest(
            source=source,
            doc_type="pdf",
            chunks=[
                ExtractedChunk(
                    chunk_id="chunk#000001",
                    chunk_type="page_text_chunk",
                    text="hello",
                    doc_type="pdf",
                    token_estimate=2,
                    page_no=1,
                    page_span=(1, 1),
                )
            ],
        ),
        trace_id="trace-1",
    )

    assert [job.profile_id for job in dispatcher.jobs] == ["gemini-default", "openai-text-small"]


def test_persister_does_not_mark_extract_done_when_dispatch_fails() -> None:
    """
    EN: Persister does not mark extract done when dispatch fails.
    CN: 鍚屼笂銆?
    """
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/guide.pdf", version_id="v1")
    object_state_repo = _FakeObjectStateRepo()
    dispatcher = _FakeDispatcher(fail=True)
    persister = ExtractionResultPersister(
        extraction_service=_FakeExtractionService(),
        state_committer=_make_state_committer(object_state_repo),
        manifest_repo=_FakeManifestRepo(),
        embed_dispatcher=dispatcher,
        embedding_profiles=(
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
    )

    try:
        persister.persist(
            source=source,
            manifest=ChunkManifest(
                source=source,
                doc_type="pdf",
                chunks=[
                    ExtractedChunk(
                        chunk_id="chunk#000001",
                        chunk_type="page_text_chunk",
                        text="hello",
                        doc_type="pdf",
                        token_estimate=2,
                    )
                ],
            ),
            trace_id="trace-1",
        )
    except RuntimeError as exc:
        assert str(exc) == "dispatch failed"
    else:
        raise AssertionError("dispatch failure should surface to the caller")

    assert object_state_repo.calls == []
    assert persister._manifest_repo.rollback_calls == [(source.document_uri, "s3://manifest-bucket/manifests/example.json", None)]



