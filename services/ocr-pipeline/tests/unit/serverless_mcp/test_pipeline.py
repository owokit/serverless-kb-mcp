"""
EN: Tests for ExtractionResultPersister covering manifest persistence, embed job dispatch, and fan-out by profile.
CN: 同上。
"""

from serverless_mcp.extract.pipeline import ExtractionResultPersister
from serverless_mcp.domain.models import (
    ChunkManifest,
    EmbeddingProfile,
    EmbeddingRequest,
    ExtractedChunk,
    ObjectStateRecord,
    PersistedManifest,
    S3ObjectRef,
)


class _FakeExtractionService:
    # EN: Stand-in for ExtractionService returning a fixed manifest.
    # CN: 杩斿洖鍥哄畾 manifest 鐨?ExtractionService 鏇胯韩銆?
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
    # CN: 同上。
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
    # EN: In-memory stand-in for ObjectStateRepository.
    # CN: 同上。
    def __init__(self) -> None:
        self.calls = []

    def mark_extract_done(self, source, manifest_s3_uri):
        self.calls.append(("mark_extract_done", source.document_uri, manifest_s3_uri))
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
    # CN: 同上。
    def __init__(self, *, fail: bool = False):
        self.jobs = []
        self.fail = fail

    def dispatch_many(self, jobs):
        if self.fail:
            raise RuntimeError("dispatch failed")
        self.jobs.extend(jobs)


def test_persister_persists_manifest_and_dispatches_embed_job() -> None:
    """
    EN: Persister persists manifest and dispatches embed job.
    CN: 楠岃瘉 persister 鎸佷箙鍖?manifest 骞跺垎鍙?embed job銆?
    """
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/guide.pdf", version_id="v1")
    dispatcher = _FakeDispatcher()
    persister = ExtractionResultPersister(
        extraction_service=_FakeExtractionService(),
        object_state_repo=_FakeObjectStateRepo(),
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


def test_persister_passes_previous_version_id_to_manifest_repo() -> None:
    """
    EN: Persister passes previous version id to manifest repo.
    CN: 楠岃瘉 persister 灏?previous_version_id 浼犻€掔粰 manifest repo銆?
    """
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/guide.pdf", version_id="v1")
    manifest_repo = _FakeManifestRepo()
    persister = ExtractionResultPersister(
        extraction_service=_FakeExtractionService(),
        object_state_repo=_FakeObjectStateRepo(),
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
    CN: 楠岃瘉 persister 鎸?profile 鎵囧嚭 embed job銆?
    """
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/guide.pdf", version_id="v1")
    dispatcher = _FakeDispatcher()
    persister = ExtractionResultPersister(
        extraction_service=_FakeExtractionService(),
        object_state_repo=_FakeObjectStateRepo(),
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
    CN: 同上。
    """
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/guide.pdf", version_id="v1")
    object_state_repo = _FakeObjectStateRepo()
    dispatcher = _FakeDispatcher(fail=True)
    persister = ExtractionResultPersister(
        extraction_service=_FakeExtractionService(),
        object_state_repo=object_state_repo,
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
