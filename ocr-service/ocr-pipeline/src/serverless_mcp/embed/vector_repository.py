"""
EN: S3 Vectors repository for profile-scoped vector persistence, querying, and version governance.
CN: 用于按 profile 作用域持久化、查询和治理版本的 S3 Vectors 仓储。
"""
from __future__ import annotations

import json
import random
from array import array
from dataclasses import dataclass
from typing import Any
from time import sleep

from botocore.exceptions import ClientError

from serverless_mcp.domain.models import EmbeddingJobMessage, EmbeddingProfile, VectorRecord


@dataclass(slots=True)
class VectorQueryMatch:
    """
    EN: Vector query match result with key, metadata, distance, and source profile.
    CN: 向量查询匹配结果，包含 key、metadata、distance 和来源 profile。
    """
    key: str
    chunk_id: str
    manifest_s3_uri: str
    metadata: dict[str, Any]
    profile_id: str
    distance: float | None = None


class S3VectorRepository:
    """
    EN: Manage vector lifecycle in S3 Vectors for one or more embedding profiles, including put, stale-mark, delete, and query.
    CN: 管理一个或多个 embedding profile 的 S3 Vectors 向量生命周期，包括写入、标记过期、删除和查询。
    """

    def __init__(self, *, s3vectors_client: object) -> None:
        """
        Args:
            s3vectors_client:
                EN: Boto3 S3 Vectors client used for put, get, delete, and query operations.
                CN: 用于 put、get、delete 和 query 操作的 Boto3 S3 Vectors 客户端。
        """
        self._client = s3vectors_client

    def put_vectors(self, *, job: EmbeddingJobMessage, profile: EmbeddingProfile, vectors: list[VectorRecord]) -> None:
        """
        EN: Persist vectors into the bucket and index bound to the job profile.
        CN: 将向量持久化到 job profile 绑定的 bucket 和 index。

        Args:
            job:
                EN: Embedding job message providing document identity and manifest reference.
                CN: 提供文档身份和 manifest 引用的 embedding 作业消息。
            profile:
                EN: Target embedding profile that owns the vector bucket and index.
                CN: 拥有向量 bucket 和 index 的目标 embedding profile。
            vectors:
                EN: List of vector records to persist.
                CN: 要持久化的向量记录列表。
        """
        for batch in _batch_vector_records(vectors):
            self._put_records(profile=profile, vectors=batch)

    def query_vectors(
        self,
        *,
        profile: EmbeddingProfile,
        query_vector: list[float],
        top_k: int,
        metadata_filter: dict[str, Any] | None = None,
    ) -> list[VectorQueryMatch]:
        """
        EN: Query one profile-specific S3 Vectors index with similarity search and metadata filtering.
        CN: 使用相似度搜索和 metadata 过滤查询单个 profile 专用 S3 Vectors index。

        Args:
            profile:
                EN: Embedding profile whose vector index to query.
                CN: 待查询的 embedding profile。
            query_vector:
                EN: Query embedding vector as a list of floats.
                CN: 以浮点数列表形式表示的查询 embedding 向量。
            top_k:
                EN: Maximum number of nearest neighbors to return.
                CN: 要返回的最大近邻数量。
            metadata_filter:
                EN: Optional metadata filter expression.
                CN: 可选的 metadata 过滤表达式。

        Returns:
            EN: List of vector query matches with distance and metadata.
            CN: 包含距离和 metadata 的向量查询匹配列表。
        """
        payload: dict[str, Any] = {
            "vectorBucketName": profile.vector_bucket_name,
            "indexName": profile.vector_index_name,
            "queryVector": {"float32": _float32_values(query_vector)},
            "topK": top_k,
            "returnMetadata": True,
            "returnDistance": True,
        }
        if metadata_filter:
            payload["filter"] = metadata_filter
        response = self._client.query_vectors(**payload)

        # EN: Extract manifest_s3_uri and chunk_id from each match for downstream result lookup.
        # CN: 从每个匹配中提取 manifest_s3_uri 和 chunk_id，供下游结果查找使用。
        matches: list[VectorQueryMatch] = []
        for vector in response.get("vectors") or []:
            metadata = vector.get("metadata") or {}
            manifest_s3_uri = metadata.get("manifest_s3_uri")
            chunk_id = metadata.get("chunk_id")
            if not manifest_s3_uri or not chunk_id:
                raise ValueError(f"Vector {vector.get('key')} is missing manifest_s3_uri or chunk_id metadata")
            matches.append(
                VectorQueryMatch(
                    key=vector["key"],
                    chunk_id=str(chunk_id),
                    manifest_s3_uri=str(manifest_s3_uri),
                    metadata=metadata,
                    profile_id=profile.profile_id,
                    distance=float(vector["distance"]) if "distance" in vector else None,
                )
            )
        return matches

    def _put_records(self, *, profile: EmbeddingProfile, vectors: list[VectorRecord]) -> None:
        """
        EN: Serialize vector records and write them to S3 Vectors via the underlying client.
        CN: 通过底层客户端序列化向量记录并写入 S3 Vectors。
        """
        payload = _build_put_vectors_payload(vectors)
        self._put_vectors_with_retry(profile=profile, payload=payload)

    def _put_vectors_with_retry(self, *, profile: EmbeddingProfile, payload: list[dict[str, Any]]) -> None:
        """
        EN: Retry throttled PutVectors requests with capped exponential backoff and jitter.
        CN: 针对限流的 PutVectors 请求执行有上限的指数退避和抖动重试。
        """
        attempt = 0
        while True:
            attempt += 1
            try:
                self._client.put_vectors(
                    vectorBucketName=profile.vector_bucket_name,
                    indexName=profile.vector_index_name,
                    vectors=payload,
                )
                return
            except ClientError as exc:
                if not _is_retryable_put_vectors_error(exc) or attempt >= _MAX_PUT_VECTORS_ATTEMPTS:
                    raise
                _sleep_before_retry(attempt)


def _extract_vector_data(vector: dict[str, Any]) -> list[float]:
    """
    EN: Extract float32 vector data from a raw S3 Vectors get_vectors response entry.
    CN: 从原始 S3 Vectors get_vectors 响应条目中提取 float32 向量数据。
    """
    data = vector.get("data")
    if isinstance(data, dict):
        float32 = data.get("float32")
        if isinstance(float32, list):
            return [float(value) for value in float32]
    if isinstance(data, list):
        return [float(value) for value in data]
    raise ValueError(f"Vector {vector.get('key')} does not contain float32 data")


def _normalize_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    """
    EN: Normalize metadata values to S3 Vectors-compatible scalars, serializing complex types as JSON.
    CN: 将 metadata 值规范化为 S3 Vectors 兼容的标量，并把复杂类型序列化为 JSON。
    """
    normalized: dict[str, Any] = {}
    for key, value in metadata.items():
        if value is None:
            continue
        if isinstance(value, (list, tuple)) and not value:
            continue
        if isinstance(value, (str, bool, int, float)):
            normalized[key] = value
            continue
        if isinstance(value, (list, tuple)):
            if all(isinstance(item, (str, bool, int, float)) for item in value):
                normalized[key] = list(value)
            else:
                normalized[key] = json.dumps(list(value), ensure_ascii=False)
            continue
        normalized[key] = json.dumps(value, ensure_ascii=False)
    return normalized


def _chunked(items: list[str], size: int) -> list[list[str]]:
    """
    EN: Split a list into fixed-size batches for batch API calls.
    CN: 将列表拆分为固定大小的批次，用于批量 API 调用。
    """
    return [items[index : index + size] for index in range(0, len(items), size)]


def _build_put_vectors_payload(vectors: list[VectorRecord]) -> list[dict[str, Any]]:
    """
    EN: Build the serialized payload for a PutVectors request.
    CN: 构建 PutVectors 请求所需的序列化负载。
    """
    return [
        {
            "key": vector.key,
            "data": {"float32": _float32_values(vector.data)},
            "metadata": _normalize_metadata(vector.metadata),
        }
        for vector in vectors
    ]


def _batch_vector_records(vectors: list[VectorRecord]) -> list[list[VectorRecord]]:
    """
    EN: Split vector records into request-safe batches by count and estimated payload size.
    CN: 按数量和估算请求体大小将向量记录拆分为符合请求限制的批次。
    """
    batches: list[list[VectorRecord]] = []
    current_batch: list[VectorRecord] = []
    for vector in vectors:
        candidate_batch = current_batch + [vector]
        if current_batch and (
            len(candidate_batch) > _MAX_PUT_VECTORS_PER_REQUEST
            or _estimate_put_vectors_payload_bytes(candidate_batch) > _MAX_PUT_VECTORS_REQUEST_BYTES
        ):
            batches.append(current_batch)
            current_batch = [vector]
            continue
        current_batch = candidate_batch
        if len(current_batch) >= _MAX_PUT_VECTORS_PER_REQUEST:
            batches.append(current_batch)
            current_batch = []
    if current_batch:
        batches.append(current_batch)
    return batches


def _estimate_put_vectors_payload_bytes(vectors: list[VectorRecord]) -> int:
    """
    EN: Estimate the serialized request size for a PutVectors batch.
    CN: 估算 PutVectors 批次的序列化请求体大小。
    """
    payload = _build_put_vectors_payload(vectors)
    return len(
        json.dumps({"vectors": payload}, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    )


def _is_retryable_put_vectors_error(exc: ClientError) -> bool:
    """
    EN: Check whether the client error is a throttling-style failure that should be retried.
    CN: 判断客户端错误是否属于应当重试的限流类失败。
    """
    error = exc.response.get("Error") or {}
    return error.get("Code") in {
        "TooManyRequestsException",
        "ThrottlingException",
        "ProvisionedThroughputExceededException",
    }


def _sleep_before_retry(attempt: int) -> None:
    """
    EN: Sleep with capped exponential backoff and jitter before the next retry.
    CN: 在下一次重试前执行带上限的指数退避和抖动睡眠。
    """
    base_delay = min(_MAX_PUT_VECTORS_BACKOFF_SECONDS, _INITIAL_PUT_VECTORS_BACKOFF_SECONDS * (2 ** (attempt - 1)))
    jitter = random.uniform(0.0, base_delay / 2)
    sleep(base_delay + jitter)


_MAX_PUT_VECTORS_PER_REQUEST = 500
_MAX_PUT_VECTORS_REQUEST_BYTES = 20 * 1024 * 1024
_MAX_PUT_VECTORS_ATTEMPTS = 5
_INITIAL_PUT_VECTORS_BACKOFF_SECONDS = 0.25
_MAX_PUT_VECTORS_BACKOFF_SECONDS = 3.0


def _float32_values(values: list[float]) -> list[float]:
    """
    EN: Convert a float list to IEEE 754 single-precision float32 values.
    CN: 将浮点数列表转换为 IEEE 754 单精度 float32 值。
    """
    return list(array("f", values))

