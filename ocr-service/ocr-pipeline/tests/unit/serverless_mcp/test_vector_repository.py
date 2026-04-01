"""
EN: Tests for S3VectorRepository covering put/query serialization and metadata normalization.
CN: S3VectorRepository ? put/query ???? metadata ??????
"""

from botocore.exceptions import ClientError

import serverless_mcp.embed.vector_repository as vector_repository_module
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


class _ThrottlingS3VectorsClient(_FakeS3VectorsClient):
    """
    EN: Fake client that throttles the first PutVectors call.
    CN: 会在首次 PutVectors 调用时返回限流错误的假客户端。
    """

    def __init__(self, failures: int = 1):
        super().__init__()
        self.failures_remaining = failures

    def put_vectors(self, **kwargs):
        self.put_calls.append(kwargs)
        if self.failures_remaining > 0:
            self.failures_remaining -= 1
            raise ClientError(
                {
                    "Error": {
                        "Code": "TooManyRequestsException",
                        "Message": "throttled",
                    }
                },
                "PutVectors",
            )


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


def test_put_vectors_batches_by_count_and_retries_throttle(monkeypatch) -> None:
    """
    EN: Put vectors splits batches by count and retries throttled requests.
    CN: put_vectors 会按数量拆批，并在限流时重试。
    """
    monkeypatch.setattr(vector_repository_module, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(vector_repository_module.random, "uniform", lambda *_args, **_kwargs: 0.0)

    client = _ThrottlingS3VectorsClient(failures=1)
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
    vectors = [
        VectorRecord(
            key=f"openai-text-small#tenant-a#bucket-a#docs/guide.pdf#v1#chunk#{index:06d}",
            data=[float(index), float(index) + 0.5],
            metadata={"tenant_id": "tenant-a", "chunk_id": f"chunk#{index:06d}"},
        )
        for index in range(501)
    ]

    repo.put_vectors(job=None, profile=profile, vectors=vectors)

    assert len(client.put_calls) == 3
    assert len(client.put_calls[0]["vectors"]) == 500
    assert len(client.put_calls[1]["vectors"]) == 500
    assert len(client.put_calls[2]["vectors"]) == 1


def test_put_vectors_batches_by_payload_bytes(monkeypatch) -> None:
    """
    EN: Put vectors splits batches when the estimated request body exceeds the budget.
    CN: 当估算请求体超过预算时，put_vectors 会拆分批次。
    """
    monkeypatch.setattr(vector_repository_module, "_MAX_PUT_VECTORS_REQUEST_BYTES", 200)

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
    vectors = [
        VectorRecord(
            key=f"openai-text-small#tenant-a#bucket-a#docs/guide.pdf#v1#chunk#{index:06d}",
            data=[1.0, 2.0],
            metadata={
                "tenant_id": "tenant-a",
                "chunk_id": f"chunk#{index:06d}",
                "long_text": "x" * 120,
            },
        )
        for index in range(2)
    ]

    repo.put_vectors(job=None, profile=profile, vectors=vectors)

    assert len(client.put_calls) == 2
    assert len(client.put_calls[0]["vectors"]) == 1
    assert len(client.put_calls[1]["vectors"]) == 1


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
