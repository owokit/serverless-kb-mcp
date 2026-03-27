"""
EN: Tests for ExtractionService embedding request building and metadata handling.
CN: 娴嬭瘯 ExtractionService 鐨?embedding request 鏋勫缓鍜?metadata 澶勭悊銆?
"""

from serverless_mcp.extract.application import ExtractionService
from serverless_mcp.domain.models import ChunkManifest, ExtractedChunk, S3ObjectRef


def test_build_embedding_requests_omits_empty_sequence_metadata() -> None:
    """
    EN: Verify that empty sequence metadata fields (security_scope, section_path) are omitted from request metadata.
    CN: 同上。
    """
    service = ExtractionService()
    source = S3ObjectRef(
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="docs/guide.md",
        version_id="v1",
    )
    manifest = ChunkManifest(
        source=source,
        doc_type="md",
        chunks=[
            ExtractedChunk(
                chunk_id="chunk#000001",
                chunk_type="section_text_chunk",
                text="hello",
                doc_type="md",
                token_estimate=2,
                section_path=(),
                metadata={"source_format": "markdown"},
            )
        ],
    )

    requests = service.build_embedding_requests(manifest, manifest_s3_uri="s3://manifest-bucket/manifests/v1.json")

    assert len(requests) == 1
    metadata = requests[0].metadata
    assert metadata["tenant_id"] == "tenant-a"
    assert metadata["bucket"] == "bucket-a"
    assert metadata["key"] == "docs/guide.md"
    assert metadata["version_id"] == "v1"
    assert metadata["manifest_s3_uri"] == "s3://manifest-bucket/manifests/v1.json"
    assert "security_scope" not in metadata
    assert "section_path" not in metadata


def test_build_embedding_requests_preserves_non_empty_sequence_metadata() -> None:
    """
    EN: Verify that non-empty section_path is preserved and converted to a list in request metadata.
    CN: 同上。
    """
    service = ExtractionService()
    source = S3ObjectRef(
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="docs/guide.md",
        version_id="v1",
    )
    manifest = ChunkManifest(
        source=source,
        doc_type="md",
        chunks=[
            ExtractedChunk(
                chunk_id="chunk#000001",
                chunk_type="section_text_chunk",
                text="hello",
                doc_type="md",
                token_estimate=2,
                section_path=("section-a", "section-b"),
                metadata={"source_format": "markdown"},
            )
        ],
    )

    requests = service.build_embedding_requests(manifest, manifest_s3_uri="s3://manifest-bucket/manifests/v1.json")

    metadata = requests[0].metadata
    assert metadata["section_path"] == ["section-a", "section-b"]

