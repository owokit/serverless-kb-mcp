"""
EN: Tests for S3VectorRepository covering staleness marking, deletion, put/query serialization, and metadata normalization.
CN: 同上。
"""

from serverless_mcp.embed.vector_repository import S3VectorRepository
from serverless_mcp.domain.models import EmbeddingProfile, VectorRecord


class _FakeS3VectorsClient:
    # EN: In-memory stand-in for S3 Vectors client.
    # CN: 同上。
    def __init__(self):
        self.put_calls = []
        self.get_calls = []
        self.delete_calls = []
        self.query_calls = []

    def put_vectors(self, **kwargs):
        self.put_calls.append(kwargs)

    def get_vectors(self, **kwargs):
        self.get_calls.append(kwargs)
        return {
            "vectors": [
                {
                    "key": "tenant-a#bucket-a#docs/guide.pdf#v1#chunk#000001",
                    "data": {"float32": [0.1, 0.2]},
                    "metadata": {
                        "tenant_id": "tenant-a",
                        "version_id": "v1",
                        "chunk_id": "chunk#000001",
                        "manifest_s3_uri": "s3://manifest-bucket/manifests/v1.json",
                        "is_latest": True,
                    },
                }
            ]
        }

    def delete_vectors(self, **kwargs):
        self.delete_calls.append(kwargs)

    def query_vectors(self, **kwargs):
        self.query_calls.append(kwargs)
        return {"vectors": []}


def test_mark_vectors_stale_rewrites_metadata() -> None:
    """
    EN: Mark vectors stale rewrites metadata.
    CN: 楠岃瘉 mark_vectors_stale 閲嶅啓 is_latest metadata銆?
    """
    client = _FakeS3VectorsClient()
    repo = S3VectorRepository(s3vectors_client=client)
    profile = EmbeddingProfile(
        profile_id="gemini-default",
        provider="gemini",
        model="gemini-embedding-2-preview",
        dimension=3072,
        vector_bucket_name="vector-bucket",
        vector_index_name="index-a",
        supported_content_kinds=("text", "image"),
    )

    repo.mark_vectors_stale(profile=profile, keys=["tenant-a#bucket-a#docs/guide.pdf#v1#chunk#000001"])

    assert client.get_calls[0]["keys"] == ["tenant-a#bucket-a#docs/guide.pdf#v1#chunk#000001"]
    assert len(client.put_calls) == 1
    rewritten = client.put_calls[0]["vectors"][0]
    assert rewritten["metadata"]["is_latest"] is False
    assert rewritten["key"] == "tenant-a#bucket-a#docs/guide.pdf#v1#chunk#000001"


def test_delete_vectors_removes_previous_version_keys() -> None:
    """
    EN: Delete vectors removes previous version keys.
    CN: 楠岃瘉 delete_vectors 鍒犻櫎鏃х増鏈?key銆?
    """
    client = _FakeS3VectorsClient()
    repo = S3VectorRepository(s3vectors_client=client)
    profile = EmbeddingProfile(
        profile_id="gemini-default",
        provider="gemini",
        model="gemini-embedding-2-preview",
        dimension=3072,
        vector_bucket_name="vector-bucket",
        vector_index_name="index-a",
        supported_content_kinds=("text", "image"),
    )

    repo.delete_vectors(profile=profile, keys=["tenant-a#bucket-a#docs/guide.pdf#v1#chunk#000001"])

    assert client.delete_calls[0]["keys"] == ["tenant-a#bucket-a#docs/guide.pdf#v1#chunk#000001"]


def test_put_vectors_serializes_float32_payload() -> None:
    """
    EN: Put vectors serializes float32 payload.
    CN: 楠岃瘉 put_vectors 搴忓垪鍖栦负 float32 杞借嵎銆?
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
    CN: 楠岃瘉 query_vectors 搴忓垪鍖栦负 float32 杞借嵎銆?
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
    CN: 楠岃瘉 query_vectors 鎷掔粷缂哄皯 manifest metadata 鐨勫悜閲忋€?
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
    CN: 楠岃瘉 _normalize_metadata 鍘婚櫎绌哄簭鍒楀€笺€?
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

