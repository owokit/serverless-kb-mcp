"""
EN: Tests for ExtractWorker covering job processing, embed delegation, and processing state reuse.
CN: 同上。
"""

from serverless_mcp.domain.models import (
    ChunkManifest,
    ExtractJobMessage,
    ExtractedChunk,
    ObjectStateRecord,
    S3ObjectRef,
)
from serverless_mcp.extract.worker import ExtractWorker


class _FakeExtractionService:
    # EN: Stand-in for ExtractionService returning a fixed manifest.
    # CN: 杩斿洖鍥哄畾 manifest 鐨?ExtractionService 鏇胯韩銆?
    def extract_from_s3(self, source):
        return ChunkManifest(
            source=source,
            doc_type="md",
            chunks=[
                ExtractedChunk(
                    chunk_id="chunk#000001",
                    chunk_type="section_text_chunk",
                    text="hello",
                    doc_type="md",
                    token_estimate=2,
                    metadata={"source_format": "markdown"},
                )
            ],
            assets=[],
            metadata={"source_format": "markdown", "section_count": 1},
        )


class _FailingExtractionService(_FakeExtractionService):
    # EN: Stand-in that raises to trigger the worker failure path.
    # CN: 用于触发 worker 失败路径的镜像服务。
    def extract_from_s3(self, source):
        raise RuntimeError("original extraction failed")


class _FakeObjectStateRepo:
    # EN: In-memory stand-in for ObjectStateRepository.
    # CN: 同上。
    def __init__(self):
        self.started = []
        self.failed = []

    def start_processing(self, source):
        self.started.append(source.document_uri)
        return ObjectStateRecord(
            pk=source.object_pk,
            latest_version_id=source.version_id,
            latest_sequencer=source.sequencer,
            extract_status="EXTRACTING",
            embed_status="PENDING",
            previous_version_id="v122",
            previous_manifest_s3_uri="s3://manifest-bucket/manifests/v122.json",
        )

    def mark_failed(self, source, error_message):
        self.failed.append((source.document_uri, error_message))
        return ObjectStateRecord(
            pk=source.object_pk,
            latest_version_id=source.version_id,
            latest_sequencer=source.sequencer,
            extract_status="FAILED",
            embed_status="FAILED",
            last_error=error_message,
        )


class _BrokenFailureObjectStateRepo(_FakeObjectStateRepo):
    # EN: Stand-in that raises while persisting failure bookkeeping.
    # CN: 在写入失败状态时抛错的镜像仓库。
    def mark_failed(self, source, error_message):
        super().mark_failed(source, error_message)
        raise RuntimeError("mark_failed failed")

class _FakeResultPersister:
    # EN: Records ExtractionResultPersister.persist calls.
    # CN: 璁板綍 ExtractionResultPersister.persist 璋冪敤銆?
    def __init__(self):
        self.calls = []

    def persist(self, *, source, manifest, trace_id, previous_version_id=None, previous_manifest_s3_uri=None):
        self.calls.append((source.document_uri, trace_id, manifest.doc_type, previous_version_id, previous_manifest_s3_uri))
        return type(
            "Outcome",
            (),
            {
                "chunk_count": 1,
                "asset_count": 0,
                "embedding_request_count": 1,
                "manifest_s3_uri": "s3://manifest-bucket/manifests/example.json",
            },
        )()


def test_worker_processes_job_and_delegates_embed_dispatch() -> None:
    """
    EN: Worker processes job and delegates embed dispatch.
    CN: 同上。
    """
    persister = _FakeResultPersister()
    worker = ExtractWorker(
        extraction_service=_FakeExtractionService(),
        object_state_repo=_FakeObjectStateRepo(),
        result_persister=persister,
    )
    source = S3ObjectRef(
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="docs/guide.md",
        version_id="v123",
        sequencer="001",
        security_scope=("team-a",),
    )

    outcome = worker.process(ExtractJobMessage(source=source, trace_id="trace-1"))

    assert outcome.chunk_count == 1
    assert outcome.embedding_request_count == 1
    assert outcome.manifest_s3_uri == "s3://manifest-bucket/manifests/example.json"
    assert persister.calls == [(
        source.document_uri,
        "trace-1",
        "md",
        "v122",
        "s3://manifest-bucket/manifests/v122.json",
    )]


def test_worker_reuses_existing_processing_state() -> None:
    """
    EN: Worker reuses existing processing state.
    CN: 楠岃瘉 worker 澶嶇敤宸叉湁 processing state銆?
    """
    state_repo = _FakeObjectStateRepo()
    persister = _FakeResultPersister()
    worker = ExtractWorker(
        extraction_service=_FakeExtractionService(),
        object_state_repo=state_repo,
        result_persister=persister,
    )
    source = S3ObjectRef(
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="docs/guide.md",
        version_id="v123",
        sequencer="001",
        security_scope=("team-a",),
    )

    worker.process(
        ExtractJobMessage(source=source, trace_id="trace-1"),
        processing_state=ObjectStateRecord(
            pk=source.object_pk,
            latest_version_id=source.version_id,
            latest_sequencer=source.sequencer,
            extract_status="EXTRACTING",
            embed_status="PENDING",
            previous_version_id="v100",
            previous_manifest_s3_uri="s3://manifest-bucket/manifests/v100.json",
        ),
    )

    assert state_repo.started == []
    assert persister.calls == [(
        source.document_uri,
        "trace-1",
        "md",
        "v100",
        "s3://manifest-bucket/manifests/v100.json",
    )]


def test_worker_preserves_original_exception_when_failure_marking_also_fails() -> None:
    """
    EN: Worker keeps the original exception visible even if mark_failed also raises.
    CN: 即使 mark_failed 也抛错，worker 仍保留原始异常可见。
    """
    worker = ExtractWorker(
        extraction_service=_FailingExtractionService(),
        object_state_repo=_BrokenFailureObjectStateRepo(),
        result_persister=_FakeResultPersister(),
    )
    source = S3ObjectRef(
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="docs/guide.md",
        version_id="v123",
        sequencer="001",
        security_scope=("team-a",),
    )

    try:
        worker.process(ExtractJobMessage(source=source, trace_id="trace-1"))
    except Exception as exc:
        assert "original extraction failed" in str(exc)
        assert "mark_failed failed" in "".join(getattr(exc, "__notes__", []))
    else:
        raise AssertionError("Expected worker.process to raise the original exception")
