"""
EN: Tests for embedding request and job message schema validation.
CN: 娴嬭瘯 embedding request 鍜?job message 鐨?schema 鏍￠獙銆?
"""

from serverless_mcp.domain.embedding_schema import validate_embedding_job_message, validate_embedding_request
from serverless_mcp.domain.models import EmbeddingJobMessage, EmbeddingRequest, S3ObjectRef
from serverless_mcp.domain.schema_errors import SchemaValidationError


def _request(*, manifest_s3_uri: str) -> EmbeddingRequest:
    source = S3ObjectRef(
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="docs/report.pdf",
        version_id="v1",
        security_scope=("team-a",),
    )
    return EmbeddingRequest(
        chunk_id="chunk#000001",
        chunk_type="page_text_chunk",
        content_kind="text",
        text="hello world",
        metadata={
            "tenant_id": source.tenant_id,
            "bucket": source.bucket,
            "key": source.key,
            "version_id": source.version_id,
            "document_uri": source.document_uri,
            "security_scope": list(source.security_scope),
            "language": source.language,
            "doc_type": "pdf",
            "source_format": "pdf",
            "manifest_s3_uri": manifest_s3_uri,
            "is_latest": True,
        },
    )


def test_validate_embedding_request_accepts_document_request() -> None:
    """
    EN: Verify that a well-formed embedding request passes schema validation.
    CN: 楠岃瘉鏍煎紡姝ｇ‘鐨?embedding request 閫氳繃 schema 鏍￠獙銆?
    """
    request = _request(manifest_s3_uri="s3://manifest-bucket/report/manifest.json?versionId=v1")

    validated = validate_embedding_request(request)

    assert validated.metadata["source_format"] == "pdf"


def test_validate_embedding_request_allows_missing_security_scope() -> None:
    """
    EN: Verify that security_scope is optional in embedding request metadata.
    CN: 楠岃瘉 embedding request metadata 涓?security_scope 涓哄彲閫夊瓧娈点€?
    """
    request = _request(manifest_s3_uri="s3://manifest-bucket/report/manifest.json?versionId=v1")
    request.metadata.pop("security_scope")

    validated = validate_embedding_request(request)

    assert validated.metadata["source_format"] == "pdf"


def test_validate_embedding_request_rejects_missing_manifest_uri() -> None:
    """
    EN: Verify that missing manifest_s3_uri raises SchemaValidationError.
    CN: 楠岃瘉缂哄皯 manifest_s3_uri 鏃舵姏鍑?SchemaValidationError銆?
    """
    request = _request(manifest_s3_uri="s3://manifest-bucket/report/manifest.json?versionId=v1")
    request.metadata.pop("manifest_s3_uri")

    try:
        validate_embedding_request(request)
    except SchemaValidationError as exc:
        assert "manifest_s3_uri" in str(exc)
    else:
        raise AssertionError("expected embedding request validation to fail")


def test_validate_embedding_job_message_requires_requests() -> None:
    """
    EN: Verify that validate_embedding_job_message accepts a job with at least one request.
    CN: 楠岃瘉 validate_embedding_job_message 鎺ュ彈鑷冲皯鍖呭惈涓€涓?request 鐨?job銆?
    """
    request = _request(manifest_s3_uri="s3://manifest-bucket/report/manifest.json?versionId=v1")
    job = EmbeddingJobMessage(
        source=S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/report.pdf", version_id="v1"),
        profile_id="gemini-default",
        trace_id="trace-1",
        manifest_s3_uri="s3://manifest-bucket/report/manifest.json?versionId=v1",
        requests=[request],
    )

    validated = validate_embedding_job_message(job)

    assert validated.profile_id == "gemini-default"

