"""
EN: Tests for StepFunctionsExtractWorkflow covering OCR polling, state reuse, failure marking, and poll budget capping.
CN: 同上。
"""

from dataclasses import dataclass

from serverless_mcp.extract.workflow import StepFunctionsExtractWorkflow
from serverless_mcp.extract.contracts import build_extract_failure_details
from serverless_mcp.domain.models import (
    ChunkManifest,
    ExtractJobMessage,
    ExtractedChunk,
    ObjectStateRecord,
    ProcessingOutcome,
    S3ObjectRef,
)


@dataclass
class _Submission:
    # EN: Dataclass representing an OCR job submission.
    # CN: 琛ㄧず OCR job 鎻愪氦鐨?dataclass銆?
    job_id: str


@dataclass
class _Status:
    # EN: Dataclass representing an OCR job status response.
    # CN: 琛ㄧず OCR job 鐘舵€佸搷搴旂殑 dataclass銆?
    job_id: str
    state: str
    json_url: str | None = None
    error_message: str | None = None


class _FakeExtractWorker:
    # EN: Stand-in for ExtractWorker that records processing states.
    # CN: 璁板綍 processing state 鐨?ExtractWorker 鏇胯韩銆?
    def __init__(self):
        self.processing_states = []

    def process(self, job, *, processing_state=None):
        self.processing_states.append(processing_state)
        return ProcessingOutcome(
            source=job.source,
            manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
            chunk_count=1,
            asset_count=0,
            embedding_request_count=1,
            object_state=ObjectStateRecord(
                pk=job.source.object_pk,
                latest_version_id=job.source.version_id,
                latest_sequencer=job.source.sequencer,
                extract_status="EXTRACTED",
                embed_status="PENDING",
                latest_manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
            ),
        )


class _FakePersister:
    # EN: Records ExtractionResultPersister.persist calls with previous version context.
    # CN: 璁板綍甯︽棫鐗堟湰涓婁笅鏂囩殑 ExtractionResultPersister.persist 璋冪敤銆?
    def __init__(self):
        self.previous_versions = []

    def persist(self, *, source, manifest, trace_id, previous_version_id=None, previous_manifest_s3_uri=None):
        self.previous_versions.append((previous_version_id, previous_manifest_s3_uri))
        return ProcessingOutcome(
            source=source,
            manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
            chunk_count=len(manifest.chunks),
            asset_count=len(manifest.assets),
            embedding_request_count=1,
            object_state=ObjectStateRecord(
                pk=source.object_pk,
                latest_version_id=source.version_id,
                latest_sequencer=source.sequencer,
                extract_status="EXTRACTED",
                embed_status="PENDING",
                latest_manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
            ),
        )


class _FakeObjectStateRepo:
    # EN: In-memory stand-in for ObjectStateRepository.
    # CN: 同上。
    def __init__(self):
        self.activate_ingest_state_calls = []
        self.failed = []

    def start_processing(self, source):
        return ObjectStateRecord(
            pk=source.object_pk,
            latest_version_id=source.version_id,
            latest_sequencer=source.sequencer,
            extract_status="EXTRACTING",
            embed_status="PENDING",
            previous_version_id="v0",
            previous_manifest_s3_uri="s3://manifest-bucket/manifests/v0.json",
        )

    def activate_ingest_state(self, source, processing_state):
        self.activate_ingest_state_calls.append((source.document_uri, processing_state.previous_version_id))
        return ObjectStateRecord(
            pk=source.object_pk,
            latest_version_id=source.version_id,
            latest_sequencer=source.sequencer,
            extract_status="EXTRACTING",
            embed_status="PENDING",
            previous_version_id=processing_state.previous_version_id,
            previous_manifest_s3_uri=processing_state.previous_manifest_s3_uri,
        )

    def mark_extract_failed(self, source, message):
        self.failed.append((source.document_uri, message))
        return ObjectStateRecord(
            pk=source.object_pk,
            latest_version_id=source.version_id,
            latest_sequencer=source.sequencer,
            extract_status="FAILED",
            embed_status="FAILED",
            last_error=message,
        )


class _FakeSourceRepo:
    # EN: Stand-in for S3DocumentSource returning fixed payload.
    # CN: 杩斿洖鍥哄畾杞借嵎鐨?S3DocumentSource 鏇胯韩銆?
    def fetch(self, source):
        class Payload:
            body = b"%PDF-1.7"

        return Payload()


class _FakeOCRClient:
    # EN: Stub PaddleOCR client that tracks polls and returns canned responses.
    # CN: 同上。
    def __init__(self):
        self.polls = 0

    def submit_job(self, *, payload, key):
        return _Submission(job_id="job-1")

    def get_job_status(self, job_id):
        self.polls += 1
        state = "running" if self.polls == 1 else "done"
        return _Status(
            job_id=job_id,
            state=state,
            json_url="https://example.com/result.jsonl",
            error_message=None,
        )

    def download_json_lines(self, json_url):
        return [{"result": {"layoutParsingResults": [{"markdown": {"text": "hello"}, "outputImages": {}}]}}]

    def download_binary(self, url):
        return b"binary", "image/png"


class _FakeManifestBuilder:
    # EN: Stand-in for PaddleOCRManifestBuilder returning a fixed manifest.
    # CN: 杩斿洖鍥哄畾 manifest 鐨?PaddleOCRManifestBuilder 鏇胯韩銆?
    def build_manifest(self, *, source, json_lines, binary_loader):
        return ChunkManifest(
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
                    metadata={"source_format": "pdf"},
                )
            ],
            metadata={"source_format": "pdf", "page_count": 1, "visual_page_numbers": [], "page_image_asset_count": 0},
        )


def test_step_functions_workflow_polls_until_ocr_completes() -> None:
    """
    EN: Step functions workflow polls until ocr completes.
    CN: 同上。
    """
    persister = _FakePersister()
    workflow = StepFunctionsExtractWorkflow(
        extract_worker=_FakeExtractWorker(),
        result_persister=persister,
        object_state_repo=_FakeObjectStateRepo(),
        source_repo=_FakeSourceRepo(),
        ocr_client=_FakeOCRClient(),
        manifest_builder=_FakeManifestBuilder(),
        poll_interval_seconds=10,
        max_poll_attempts=3,
    )
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/scan.pdf", version_id="v1")

    prepared = workflow.prepare_job(
        job=ExtractJobMessage(source=source, trace_id="trace-1"),
    )
    assert prepared["poll_attempt"] == 0
    assert prepared["max_poll_attempts"] == 3
    submission = workflow.submit_ocr_job(job=ExtractJobMessage(source=source, trace_id="trace-1"))
    status = workflow.poll_ocr_job(job_id=submission["job_id"], poll_attempt=prepared["poll_attempt"])
    assert status["state"] == "running"
    assert status["poll_attempt"] == 1
    status = workflow.poll_ocr_job(job_id=submission["job_id"], poll_attempt=status["poll_attempt"])
    assert status["state"] == "done"
    assert status["poll_attempt"] == 2
    result = workflow.persist_ocr_result(
        job=ExtractJobMessage(source=source, trace_id="trace-1"),
        processing_state=ObjectStateRecord(**prepared["processing_state"]),
        json_url=status["json_url"] or "",
    )

    assert result["chunk_count"] == 1
    assert persister.previous_versions == [("v0", "s3://manifest-bucket/manifests/v0.json")]


def test_step_functions_workflow_reuses_ingest_processing_state() -> None:
    """
    EN: Step functions workflow reuses ingest processing state.
    CN: 楠岃瘉 Step Functions workflow 澶嶇敤 ingest processing state銆?
    """
    extract_worker = _FakeExtractWorker()
    state_repo = _FakeObjectStateRepo()
    workflow = StepFunctionsExtractWorkflow(
        extract_worker=extract_worker,
        result_persister=_FakePersister(),
        object_state_repo=state_repo,
        source_repo=_FakeSourceRepo(),
        ocr_client=_FakeOCRClient(),
        manifest_builder=_FakeManifestBuilder(),
    )
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/guide.md", version_id="v1")
    ingest_state = ObjectStateRecord(
        pk=source.object_pk,
        latest_version_id=source.version_id,
        latest_sequencer=source.sequencer,
        extract_status="QUEUED",
        embed_status="PENDING",
        previous_version_id="v0",
        previous_manifest_s3_uri="s3://manifest-bucket/manifests/v0.json",
    )

    prepared = workflow.prepare_job(
        job=ExtractJobMessage(source=source, trace_id="trace-1"),
        processing_state=ingest_state,
    )
    workflow.sync_extract(
        job=ExtractJobMessage(source=source, trace_id="trace-1"),
        processing_state=ObjectStateRecord(**prepared["processing_state"]),
    )

    assert state_repo.activate_ingest_state_calls == [(source.document_uri, "v0")]
    assert extract_worker.processing_states[0].previous_version_id == "v0"
    assert extract_worker.processing_states[0].previous_manifest_s3_uri == "s3://manifest-bucket/manifests/v0.json"


def test_step_functions_workflow_marks_failed_state() -> None:
    """
    EN: Step functions workflow marks failed state.
    CN: 同上。
    """
    state_repo = _FakeObjectStateRepo()
    workflow = StepFunctionsExtractWorkflow(
        extract_worker=_FakeExtractWorker(),
        result_persister=_FakePersister(),
        object_state_repo=state_repo,
        source_repo=_FakeSourceRepo(),
        ocr_client=_FakeOCRClient(),
        manifest_builder=_FakeManifestBuilder(),
    )
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/scan.pdf", version_id="v1")

    result = workflow.mark_failed(
        job=ExtractJobMessage(source=source, trace_id="trace-1"),
        failure=build_extract_failure_details("PaddleOCRJobFailed", "timeout"),
    )

    assert result["document_uri"] == source.document_uri
    assert result["error_message"] == "PaddleOCRJobFailed: timeout"
    assert result["failure_domain"] == "ocr"
    assert result["object_state"]["extract_status"] == "FAILED"
    assert state_repo.failed == [(source.document_uri, "PaddleOCRJobFailed: timeout")]


def test_build_extract_failure_details_classifies_manifest_errors_as_persist() -> None:
    """
    EN: Manifest-related Step Functions errors should map to the persist domain.
    CN: 与 manifest 相关的 Step Functions 错误应归类为 persist 域。
    """
    failure = build_extract_failure_details("ManifestBuildFailed", None)

    assert failure.domain == "persist"


def test_step_functions_workflow_caps_ocr_poll_budget_to_ten_minutes() -> None:
    """
    EN: Step functions workflow caps ocr poll budget to ten minutes.
    CN: 同上。
    """
    workflow = StepFunctionsExtractWorkflow(
        extract_worker=_FakeExtractWorker(),
        result_persister=_FakePersister(),
        object_state_repo=_FakeObjectStateRepo(),
        source_repo=_FakeSourceRepo(),
        ocr_client=_FakeOCRClient(),
        manifest_builder=_FakeManifestBuilder(),
        poll_interval_seconds=10,
        max_poll_attempts=180,
    )
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/scan.pdf", version_id="v1")

    prepared = workflow.prepare_job(job=ExtractJobMessage(source=source, trace_id="trace-1"))

    assert prepared["max_poll_attempts"] == 60
