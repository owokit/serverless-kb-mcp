"""
EN: S3 Vectors repository for profile-scoped vector persistence, querying, and version governance.
CN: 用于按 profile 作用域持久化、查询和治理版本的 S3 Vectors 仓储。
"""
from __future__ import annotations

import json
from array import array
from dataclasses import dataclass
from typing import Any

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
        self._put_records(profile=profile, vectors=vectors)

    def mark_vectors_stale(self, *, profile: EmbeddingProfile, keys: list[str]) -> None:
        """
        EN: Mark previous version vectors in the same profile as not latest by rewriting their metadata.
        CN: 通过重写 metadata 将同一 profile 中上一版本向量标记为非最新。

        Args:
            profile:
                EN: Embedding profile whose vector index contains the target vectors.
                CN: 目标向量所在的 embedding profile。
            keys:
                EN: Vector keys to mark as stale.
                CN: 待标记为过期的向量键列表。
        """
        if not keys:
            return
        # EN: Process in batches to stay within S3 Vectors API limits.
        # CN: 分批处理以满足 S3 Vectors API 限制。
        for batch in _chunked(keys, 500):
            # EN: Fetch existing vectors, set is_latest=False, then rewrite them.
            # CN: 先获取现有向量，将 is_latest 设为 False，然后重新写入。
            response = self._client.get_vectors(
                vectorBucketName=profile.vector_bucket_name,
                indexName=profile.vector_index_name,
                keys=batch,
                returnData=True,
                returnMetadata=True,
            )
            stale_records: list[VectorRecord] = []
            for vector in response.get("vectors") or []:
                metadata = dict(vector.get("metadata") or {})
                metadata["is_latest"] = False
                stale_records.append(
                    VectorRecord(
                        key=vector["key"],
                        data=_extract_vector_data(vector),
                        metadata=metadata,
                    )
                )
            if stale_records:
                self._put_records(profile=profile, vectors=stale_records)

    def delete_vectors(self, *, profile: EmbeddingProfile, keys: list[str]) -> None:
        """
        EN: Delete previous version vectors before writing the replacement version for the same profile.
        CN: 在写入同一 profile 的替换版本之前，删除上一版本向量。

        Args:
            profile:
                EN: Embedding profile whose vector index contains the target vectors.
                CN: 目标向量所在的 embedding profile。
            keys:
                EN: Vector keys to delete.
                CN: 要删除的向量键。
        """
        if not keys:
            return
        # EN: Batch-delete in chunks of 500 to stay within S3 Vectors API limits.
        # CN: 按 500 条一批删除，满足 S3 Vectors API 限制。
        for batch in _chunked(keys, 500):
            self._client.delete_vectors(
                vectorBucketName=profile.vector_bucket_name,
                indexName=profile.vector_index_name,
                keys=batch,
            )

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
        # EN: Serialize vectors to float32 arrays for S3 Vectors storage.
        # CN: 将向量序列化为 float32 数组以存入 S3 Vectors。
        payload = [
            {
                "key": vector.key,
                "data": {"float32": _float32_values(vector.data)},
                "metadata": _normalize_metadata(vector.metadata),
            }
            for vector in vectors
        ]
        self._client.put_vectors(
            vectorBucketName=profile.vector_bucket_name,
            indexName=profile.vector_index_name,
            vectors=payload,
        )


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


def _float32_values(values: list[float]) -> list[float]:
    """
    EN: Convert a float list to IEEE 754 single-precision float32 values.
    CN: 将浮点数列表转换为 IEEE 754 单精度 float32 值。
    """
    return list(array("f", values))

