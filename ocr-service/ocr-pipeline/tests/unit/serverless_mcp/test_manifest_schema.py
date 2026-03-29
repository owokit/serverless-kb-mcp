"""
EN: Tests for chunk manifest schema validation.
CN: 娴嬭瘯 chunk manifest schema 鏍￠獙銆?
"""

from serverless_mcp.domain.manifest_schema import validate_chunk_manifest
from serverless_mcp.domain.models import ChunkManifest, ExtractedAsset, ExtractedChunk, S3ObjectRef
from serverless_mcp.domain.schema_errors import SchemaValidationError


def test_validate_chunk_manifest_accepts_registered_pdf_manifest() -> None:
    """
    EN: Validate chunk manifest accepts registered pdf manifest.
    CN: 楠岃瘉宸叉敞鍐岀殑 PDF manifest 閫氳繃 chunk manifest 鏍￠獙銆?
    """
    manifest = ChunkManifest(
        source=S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/report.pdf", version_id="v1"),
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
        assets=[
            ExtractedAsset(
                asset_id="asset#000001",
                chunk_type="page_image_chunk",
                mime_type="image/png",
                payload=b"image-bytes",
                page_no=1,
                metadata={"source_format": "pdf"},
            )
        ],
        metadata={"source_format": "pdf", "page_count": 1, "visual_page_numbers": [], "page_image_asset_count": 1},
    )

    spec = validate_chunk_manifest(manifest)

    assert spec.doc_type == "pdf"
    assert spec.source_format == "pdf"


def test_validate_chunk_manifest_rejects_missing_source_format() -> None:
    """
    EN: Validate chunk manifest rejects missing source format.
    CN: 楠岃瘉缂哄皯 source_format 鏃?chunk manifest 鏍￠獙澶辫触銆?
    """
    manifest = ChunkManifest(
        source=S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/report.pdf", version_id="v1"),
        doc_type="pdf",
        chunks=[],
        metadata={"page_count": 1},
    )

    try:
        validate_chunk_manifest(manifest)
    except SchemaValidationError as exc:
        assert "source_format" in str(exc)
    else:
        raise AssertionError("expected manifest schema validation to fail")


