"""
EN: Tests for query response serialization including version identity preservation.
CN: 同上。
"""

from __future__ import annotations

from serverless_mcp.core.serialization import serialize_query_response
from serverless_mcp.domain.models import (
    QueryDegradedProfile,
    QueryResponse,
    QueryResultContext,
    QueryResultItem,
    S3ObjectRef,
)


def test_serialize_query_response_preserves_version_identity() -> None:
    """
    EN: Serialize query response preserves version identity.
    CN: 同上。
    """
    source_v1 = S3ObjectRef(
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="docs/guide.pdf",
        version_id="v1",
    )
    source_v2 = S3ObjectRef(
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="docs/guide.pdf",
        version_id="v2",
    )
    payload_v1 = serialize_query_response(
        QueryResponse(
            query="hello",
            results=[
                QueryResultItem(
                    key="hit-1",
                    distance=0.01,
                    source=source_v1,
                    manifest_s3_uri=None,
                    metadata={"doc_type": "pdf"},
                    match=QueryResultContext(
                        chunk_id="chunk#1",
                        chunk_type="page_text_chunk",
                        text="hello",
                    ),
                )
            ],
            degraded_profiles=(
                QueryDegradedProfile(
                    profile_id="gemini-default",
                    stage="profile_query",
                    error="timeout",
                ),
            ),
        )
    )
    payload_v2 = serialize_query_response(
        QueryResponse(
            query="hello",
            results=[
                QueryResultItem(
                    key="hit-1",
                    distance=0.01,
                    source=source_v2,
                    manifest_s3_uri=None,
                    metadata={"doc_type": "pdf"},
                    match=QueryResultContext(
                        chunk_id="chunk#1",
                        chunk_type="page_text_chunk",
                        text="hello",
                    ),
                )
            ],
        )
    )

    assert payload_v1["results"][0]["document_id"] != payload_v2["results"][0]["document_id"]
    assert payload_v1["results"][0]["version_id"] == "v1"
    assert payload_v1["results"][0]["object_pk"] == "tenant-a#bucket-a#docs%2Fguide.pdf"
    assert payload_v1["results"][0]["document_uri"].endswith("?versionId=v1")
    assert payload_v1["degraded_profiles"][0]["profile_id"] == "gemini-default"
