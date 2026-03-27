"""
EN: Tests for the job status Lambda handler and JobStatusService aggregation logic.
CN: 娴嬭瘯 job status Lambda handler 鍜?JobStatusService 鑱氬悎閫昏緫銆?
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace

from botocore.exceptions import ClientError

from serverless_mcp.entrypoints.job_status import lambda_handler
from serverless_mcp.domain.models import (
    ChunkManifest,
    EmbeddingProjectionStateRecord,
    ExtractedChunk,
    ObjectStateRecord,
    S3ObjectRef,
)
from serverless_mcp.storage.state.object_state_repository import ObjectStateLookupRecord
from serverless_mcp.status.application import JobStatusRequest, JobStatusService


class _FakeObjectStateRepo:
    # EN: In-memory stand-in for ObjectStateRepository.
    # CN: 同上。
    def __init__(self, *, lookup, state) -> None:
        self.lookup = lookup
        self.state = state

    def get_lookup_record(self, *, bucket: str, key: str):
        return self.lookup

    def get_state(self, *, object_pk: str):
        return self.state


class _FakeProjectionRepo:
    def __init__(self, records) -> None:
        self.records = records

    def list_version_records(self, *, object_pk: str, version_id: str):
        return self.records


class _FakeManifestRepo:
    # EN: In-memory stand-in for ManifestRepository.
    # CN: 同上。
    def __init__(self, manifest: ChunkManifest) -> None:
        self.manifest = manifest

    def load_manifest(self, manifest_s3_uri: str) -> ChunkManifest:
        return self.manifest

    def build_manifest_s3_uri(self, *, source: S3ObjectRef, version_id: str) -> str:
        return f"s3://manifest/{source.object_pk}/{version_id}/manifest.json"


class _FakeS3Client:
    # EN: In-memory stand-in for S3 client with versioned storage.
    # CN: 同上。
    def __init__(self, *, head_object_result: dict[str, object] | None = None, error_code: str | None = None) -> None:
        self.head_object_result = head_object_result
        self.error_code = error_code
        self.head_calls: list[dict[str, object]] = []

    def head_object(self, **kwargs):
        self.head_calls.append(kwargs)
        if self.error_code:
            raise ClientError({"Error": {"Code": self.error_code, "Message": "missing"}}, "HeadObject")
        return self.head_object_result or {}


def test_lambda_handler_serializes_query_payload(monkeypatch) -> None:
    """
    EN: Lambda handler serializes query payload.
    CN: 楠岃瘉 Lambda handler 搴忓垪鍖栨煡璇㈠弬鏁般€?
    """
    captured: list[JobStatusRequest] = []

    class _FakeService:
        def build_status(self, request: JobStatusRequest) -> dict[str, object]:
            captured.append(request)
            return {"overall_status": "DONE"}

    monkeypatch.setattr("serverless_mcp.entrypoints.job_status.build_job_status_service", lambda: _FakeService())

    response = lambda_handler(
        {
            "queryStringParameters": {
                "bucket": "source-bucket",
                "key": "docs/report.pdf",
                "version_id": "v1",
            }
        },
        SimpleNamespace(aws_request_id="req-1"),
    )

    assert response["statusCode"] == 200
    assert response["headers"]["Cache-Control"] == "no-store"
    assert captured[0] == JobStatusRequest(bucket="source-bucket", key="docs/report.pdf", version_id="v1", tenant_id=None)


def test_lambda_handler_rejects_invalid_json_body() -> None:
    """
    EN: Verify that malformed JSON bodies are returned as HTTP 400.
    CN: 验证格式损坏的 JSON body 会返回 HTTP 400。
    """
    response = lambda_handler({"body": "{\"bucket\": \"source-bucket\""}, None)

    assert response["statusCode"] == 400
    assert "Request body must be a JSON object" in json.loads(response["body"])["message"]


def test_lambda_handler_rejects_conflicting_payload_sources() -> None:
    """
    EN: Verify that conflicting field values across query and body sources are rejected.
    CN: 验证 query 与 body 中冲突的字段值会被拒绝。
    """
    response = lambda_handler(
        {
            "queryStringParameters": {
                "bucket": "source-bucket",
                "key": "docs/report.pdf",
            },
            "body": json.dumps(
                {
                    "bucket": "override-bucket",
                    "key": "docs/report.pdf",
                }
            ),
        },
        None,
    )

    assert response["statusCode"] == 400
    assert "Conflicting bucket values" in json.loads(response["body"])["message"]


def test_job_status_service_aggregates_complete_version() -> None:
    """
    EN: Job status service aggregates complete version.
    CN: 同上。
    """
    lookup = ObjectStateLookupRecord(
        pk="lookup#source-bucket#docs/report.pdf",
        object_pk="tenant-a#source-bucket#docs/report.pdf",
        tenant_id="tenant-a",
        bucket="source-bucket",
        key="docs/report.pdf",
        latest_version_id="v1",
        latest_sequencer="0001",
        latest_manifest_s3_uri="s3://manifest-bucket/manifests/tenant-a/source-bucket/docs/report.pdf/v1/manifest.json",
        is_deleted=False,
        updated_at="2026-03-21T10:00:00+00:00",
    )
    current_state = ObjectStateRecord(
        pk="tenant-a#source-bucket#docs/report.pdf",
        latest_version_id="v1",
        latest_sequencer="0001",
        extract_status="EXTRACTED",
        embed_status="INDEXED",
        latest_manifest_s3_uri=lookup.latest_manifest_s3_uri,
        is_deleted=False,
        last_error="",
        updated_at="2026-03-21T10:05:00+00:00",
    )
    projection_record = EmbeddingProjectionStateRecord(
        pk="tenant-a#source-bucket#docs/report.pdf#v1",
        sk="gemini-default",
        object_pk="tenant-a#source-bucket#docs/report.pdf",
        version_id="v1",
        profile_id="gemini-default",
        provider="gemini",
        model="gemini-embedding-2-preview",
        dimension=3072,
        write_status="INDEXED",
        query_status="INDEXED",
        manifest_s3_uri=lookup.latest_manifest_s3_uri,
        vector_bucket_name="vector-bucket",
        vector_index_name="vector-index",
        vector_count=1,
        last_error="",
        updated_at="2026-03-21T10:06:00+00:00",
    )
    manifest = ChunkManifest(
        source=S3ObjectRef(
            tenant_id="tenant-a",
            bucket="source-bucket",
            key="docs/report.pdf",
            version_id="v1",
        ),
        doc_type="pdf",
        chunks=[
            ExtractedChunk(
                chunk_id="chunk-1",
                chunk_type="document_markdown_chunk",
                text="hello",
                doc_type="pdf",
                token_estimate=8,
                metadata={"source_format": "pdf"},
            )
        ],
        metadata={"source_format": "pdf", "page_count": 1, "visual_page_numbers": [], "page_image_asset_count": 0},
    )
    service = JobStatusService(
        settings=SimpleNamespace(),
        s3_client=_FakeS3Client(
            head_object_result={
                "VersionId": "v1",
                "ContentLength": 123,
                "ContentType": "application/pdf",
                "ETag": '"abc"',
                "LastModified": datetime(2026, 3, 21, 10, 6, tzinfo=UTC),
            }
        ),
        object_state_repo=_FakeObjectStateRepo(lookup=lookup, state=current_state),
        projection_state_repo=_FakeProjectionRepo([projection_record]),
        manifest_repo=_FakeManifestRepo(manifest),
    )

    result = service.build_status(JobStatusRequest(bucket="source-bucket", key="docs/report.pdf", version_id="v1"))

    assert result["overall_status"] == "DONE"
    assert result["progress_percent"] == 100
    assert result["manifest"]["embedding_item_count"] == 1
    assert result["profiles"][0]["status"] == "INDEXED"
    assert result["source"]["version_id"] == "v1"
    assert result["stages"][-1]["status"] == "DONE"


def test_job_status_service_returns_not_found_when_s3_and_lookup_are_missing() -> None:
    """
    EN: Job status service returns not found when s3 and lookup are missing.
    CN: 楠岃瘉 S3 鍜?lookup 鍧囦笉瀛樺湪鏃惰繑鍥?NOT_FOUND銆?
    """
    service = JobStatusService(
        settings=SimpleNamespace(),
        s3_client=_FakeS3Client(error_code="NoSuchKey"),
        object_state_repo=_FakeObjectStateRepo(lookup=None, state=None),
    )

    result = service.build_status(JobStatusRequest(bucket="source-bucket", key="docs/missing.pdf"))

    assert result["overall_status"] == "NOT_FOUND"
    assert result["progress_percent"] == 0
    assert result["stages"][0]["status"] == "NOT_FOUND"


def test_job_status_service_uses_source_metadata_when_lookup_is_missing() -> None:
    """
    EN: Job status service uses source metadata when lookup is missing.
    CN: 同上。
    """
    service = JobStatusService(
        settings=SimpleNamespace(),
        s3_client=_FakeS3Client(
            head_object_result={
                "VersionId": "v-source",
                "ContentLength": 321,
                "ContentType": "application/pdf",
                "ETag": '"etag"',
                "LastModified": datetime(2026, 3, 21, 10, 7, tzinfo=UTC),
            }
        ),
        object_state_repo=_FakeObjectStateRepo(lookup=None, state=None),
    )

    result = service.build_status(JobStatusRequest(bucket="source-bucket", key="docs/uploaded.pdf"))

    assert result["overall_status"] == "UPLOADED"
    assert result["version_id"] == "v-source"
    assert result["source"]["version_id"] == "v-source"
    assert result["source"]["content_length"] == 321


def test_job_status_service_surfaces_manifest_load_failures() -> None:
    """
    EN: Job status service surfaces manifest load failures as a degraded status snapshot.
    CN: 验证 job status service 会将 manifest 加载失败暴露为降级快照。
    """
    lookup = ObjectStateLookupRecord(
        pk="lookup#source-bucket#docs/report.pdf",
        object_pk="tenant-a#source-bucket#docs/report.pdf",
        tenant_id="tenant-a",
        bucket="source-bucket",
        key="docs/report.pdf",
        latest_version_id="v1",
        latest_sequencer="0001",
        latest_manifest_s3_uri="s3://manifest-bucket/manifests/tenant-a/source-bucket/docs/report.pdf/v1/manifest.json",
        is_deleted=False,
        updated_at="2026-03-21T10:00:00+00:00",
    )
    current_state = ObjectStateRecord(
        pk="tenant-a#source-bucket#docs/report.pdf",
        latest_version_id="v1",
        latest_sequencer="0001",
        extract_status="EXTRACTED",
        embed_status="PENDING",
        latest_manifest_s3_uri=lookup.latest_manifest_s3_uri,
        is_deleted=False,
        last_error="",
        updated_at="2026-03-21T10:05:00+00:00",
    )

    class _CorruptManifestRepo:
        def load_manifest(self, manifest_s3_uri: str):
            raise ValueError("manifest schema mismatch")

    service = JobStatusService(
        settings=SimpleNamespace(),
        s3_client=_FakeS3Client(),
        object_state_repo=_FakeObjectStateRepo(lookup=lookup, state=current_state),
        manifest_repo=_CorruptManifestRepo(),
    )

    result = service.build_status(JobStatusRequest(bucket="source-bucket", key="docs/report.pdf", version_id="v1"))

    assert result["overall_status"] == "MANIFEST_FAILED"
    assert result["manifest"]["load_failed"] is True
    assert result["manifest"]["error_type"] == "ValueError"
    assert result["stages"][3]["status"] == "FAILED"


def test_job_status_service_rejects_tenant_mismatch_before_head_object() -> None:
    """
    EN: Job status service rejects tenant mismatch before head object.
    CN: 同上。
    """
    lookup = ObjectStateLookupRecord(
        pk="lookup#source-bucket#docs/report.pdf",
        object_pk="tenant-a#source-bucket#docs/report.pdf",
        tenant_id="tenant-a",
        bucket="source-bucket",
        key="docs/report.pdf",
        latest_version_id="v1",
        latest_sequencer="0001",
        latest_manifest_s3_uri="s3://manifest-bucket/manifests/tenant-a/source-bucket/docs/report.pdf/v1/manifest.json",
        is_deleted=False,
        updated_at="2026-03-21T10:00:00+00:00",
    )
    current_state = ObjectStateRecord(
        pk="tenant-a#source-bucket#docs/report.pdf",
        latest_version_id="v1",
        latest_sequencer="0001",
        extract_status="EXTRACTED",
        embed_status="INDEXED",
        latest_manifest_s3_uri=lookup.latest_manifest_s3_uri,
        is_deleted=False,
        last_error="",
        updated_at="2026-03-21T10:05:00+00:00",
    )
    s3_client = _FakeS3Client(
        head_object_result={
            "VersionId": "v1",
            "ContentLength": 123,
            "ContentType": "application/pdf",
            "ETag": '"abc"',
            "LastModified": datetime(2026, 3, 21, 10, 6, tzinfo=UTC),
        }
    )
    service = JobStatusService(
        settings=SimpleNamespace(),
        s3_client=s3_client,
        object_state_repo=_FakeObjectStateRepo(lookup=lookup, state=current_state),
        projection_state_repo=_FakeProjectionRepo([]),
        manifest_repo=_FakeManifestRepo(
            ChunkManifest(
                source=S3ObjectRef(
                    tenant_id="tenant-a",
                    bucket="source-bucket",
                    key="docs/report.pdf",
                    version_id="v1",
                ),
                doc_type="pdf",
                chunks=[],
                metadata={"source_format": "pdf"},
            )
        ),
    )

    result = service.build_status(JobStatusRequest(bucket="source-bucket", key="docs/report.pdf", tenant_id="tenant-b"))

    assert result["overall_status"] == "NOT_FOUND"
    assert result["lookup"] is None
    assert s3_client.head_calls == []
