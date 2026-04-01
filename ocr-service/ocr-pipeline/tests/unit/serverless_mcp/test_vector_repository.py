"""
EN: Tests for S3VectorRepository covering put/query serialization and metadata normalization.
CN: S3VectorRepository ? put/query ???? metadata ??????
"""

from serverless_mcp.embed.vector_repository import S3VectorRepository
from serverless_mcp.domain.models import EmbeddingProfile, VectorRecord


class _FakeS3VectorsClient:
    """
    EN: In-memory stand-in for the S3 Vectors client.
    CN: S3 Vectors ?????????
    """

    def __init__(self):
        self.put_calls = []
        self.query_calls = []

    def put_vectors(self, **kwargs):
        self.put_calls.append(kwargs)

    def query_vectors(self, **kwargs):
        self.query_calls.append(kwargs)
        return {"vectors": []}


def test_put_vectors_serializes_float32_payload() -> None:
    """
    EN: Put vectors serializes float32 payload.
    CN: put_vectors ???????? float32 payload?
    """
    client = _FakeS3VectorsClient()
    repo = S3VectorRepository(s3vectors_client=client)
    profile = EmbeddingProfile(
        profile_id="openai-text-small",
        provider="openai",
        model="text-embedding-3-small",
        dimension=1536,
        vector_bucket_name="vector-bucket",
        vector_index_name="index-openai",
        supported_content_kinds=("text",),
    )

    repo.put_vectors(
        job=None,
        profile=profile,
        vectors=[
            VectorRecord(
                key="openai-text-small#tenant-a#bucket-a#docs/guide.pdf#v1#chunk#000001",
                data=[1.5, 2.25],
                metadata={
                    "tenant_id": "tenant-a",
                    "is_latest": True,
                    "security_scope": [],
                    "section_path": (),
                },
            )
        ],
    )

    assert client.put_calls[0]["vectors"] == [
        {
            "key": "openai-text-small#tenant-a#bucket-a#docs/guide.pdf#v1#chunk#000001",
            "data": {"float32": [1.5, 2.25]},
            "metadata": {"tenant_id": "tenant-a", "is_latest": True},
        }
    ]


def test_query_vectors_serializes_float32_payload() -> None:
    """
    EN: Query vectors serializes float32 payload.
    CN: query_vectors ?????????? float32 payload?
    """
    client = _FakeS3VectorsClient()
    repo = S3VectorRepository(s3vectors_client=client)
    profile = EmbeddingProfile(
        profile_id="openai-text-small",
        provider="openai",
        model="text-embedding-3-small",
        dimension=1536,
        vector_bucket_name="vector-bucket",
        vector_index_name="index-openai",
        supported_content_kinds=("text",),
    )

    repo.query_vectors(profile=profile, query_vector=[0.5, 0.25], top_k=3)

    assert client.query_calls[0]["queryVector"] == {"float32": [0.5, 0.25]}


def test_query_vectors_rejects_vectors_missing_manifest_metadata() -> None:
    """
    EN: Query vectors rejects vectors missing manifest metadata.
    CN: query_vectors ????? manifest metadata ????
    """

    class _BadClient(_FakeS3VectorsClient):
        def query_vectors(self, **kwargs):
            return {
                "vectors": [
                    {
                        "key": "bad-vector",
                        "data": {"float32": [0.5, 0.25]},
                        "metadata": {"chunk_id": "chunk#000001"},
                    }
                ]
            }

    repo = S3VectorRepository(s3vectors_client=_BadClient())
    profile = EmbeddingProfile(
        profile_id="openai-text-small",
        provider="openai",
        model="text-embedding-3-small",
        dimension=1536,
        vector_bucket_name="vector-bucket",
        vector_index_name="index-openai",
        supported_content_kinds=("text",),
    )

    try:
        repo.query_vectors(profile=profile, query_vector=[0.5, 0.25], top_k=3)
    except ValueError as exc:
        assert "missing manifest_s3_uri or chunk_id metadata" in str(exc)
    else:
        raise AssertionError("missing metadata should raise an error")


def test_normalize_metadata_drops_empty_sequences() -> None:
    """
    EN: Normalize metadata drops empty sequences.
    CN: _normalize_metadata ???????
    """
    from serverless_mcp.embed.vector_repository import _normalize_metadata

    assert _normalize_metadata(
        {
            "tenant_id": "tenant-a",
            "security_scope": [],
            "section_path": (),
            "page_span": [1, 2],
        }
    ) == {
        "tenant_id": "tenant-a",
        "page_span": [1, 2],
    }
