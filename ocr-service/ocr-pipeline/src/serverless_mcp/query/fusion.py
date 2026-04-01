"""
EN: Query fusion helpers for ranking, metadata filtering, and context resolution.
CN: 用于排序、元数据过滤和上下文解析的查询融合辅助函数。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from serverless_mcp.domain.models import ChunkManifestRecord, QueryResultContext, S3ObjectRef
from serverless_mcp.embed.vector_repository import VectorQueryMatch


MAX_CONTEXT_NEIGHBOR_EXPAND = 64


@dataclass(slots=True)
class RankedCandidate:
    """
    EN: Candidate accumulated from one or more profile-specific ranked lists.
    CN: 由一个或多个 profile 的排序结果累积得到的候选项。
    """

    match: VectorQueryMatch
    source: S3ObjectRef
    rrf_score: float
    profile_hits: int = 1


def build_metadata_filter(
    *,
    tenant_id: str,
    doc_type: str | None,
    key: str | None,
) -> dict:
    """
    EN: Build a S3 Vectors metadata filter from tenant_id and optional doc_type/key constraints.
    CN: 根据 tenant_id 和可选的 doc_type / key 约束构建 S3 Vectors 元数据过滤器。
    """
    clauses: list[dict] = [{"tenant_id": {"$eq": tenant_id}}]
    if doc_type:
        clauses.append({"doc_type": {"$eq": doc_type}})
    if key:
        clauses.append({"key": {"$eq": key}})
    if len(clauses) == 1:
        return clauses[0]
    return {"$and": clauses}


def source_from_metadata(metadata: dict[str, object]) -> S3ObjectRef:
    """
    EN: Reconstruct an S3ObjectRef from vector metadata fields.
    CN: 从向量元数据字段重建 S3ObjectRef。
    """
    return S3ObjectRef(
        tenant_id=str(metadata["tenant_id"]),
        bucket=str(metadata["bucket"]),
        key=str(metadata["key"]),
        version_id=str(metadata["version_id"]),
        language=str(metadata.get("language") or "zh"),
    )


def accumulate_rrf(
    *,
    profile_matches: list[VectorQueryMatch],
    ranked_candidates: dict[str, RankedCandidate],
) -> None:
    """
    EN: Accumulate reciprocal rank fusion scores for one profile result set.
    CN: 累积单个 profile 结果集的 reciprocal rank fusion 分数。
    """
    for rank, match in enumerate(profile_matches, start=1):
        metadata = dict(match.metadata)
        source = source_from_metadata(metadata)
        dedupe_key = f"{source.version_pk}#{match.chunk_id}"
        rrf_score = 1.0 / (60 + rank)
        existing = ranked_candidates.get(dedupe_key)
        if existing is None:
            ranked_candidates[dedupe_key] = RankedCandidate(
                match=match,
                source=source,
                rrf_score=rrf_score,
            )
            continue
        existing.rrf_score += rrf_score
        existing.profile_hits += 1
        if match.distance is not None and (
            existing.match.distance is None or match.distance < existing.match.distance
        ):
            existing.match = match


def resolve_context(manifest: Any, chunk_id: str, neighbor_expand: int) -> dict[str, QueryResultContext | list[QueryResultContext]] | None:
    """
    EN: Find the matched chunk or asset in the manifest and collect surrounding neighbors.
    CN: 在 manifest 中查找匹配的 chunk 或资产，并收集周围邻居。
    """
    for index, chunk in enumerate(manifest.chunks):
        if chunk.chunk_id != chunk_id:
            continue
        bounded_expand = min(neighbor_expand, MAX_CONTEXT_NEIGHBOR_EXPAND)
        start = max(0, index - bounded_expand)
        end = min(len(manifest.chunks), index + bounded_expand + 1)
        neighbors = [
            chunk_to_context(manifest.chunks[position])
            for position in range(start, end)
            if position != index
        ]
        return {
            "match": chunk_to_context(chunk),
            "neighbors": neighbors,
        }

    for asset in manifest.assets:
        if asset.asset_id != chunk_id:
            continue
        return {
            "match": QueryResultContext(
                chunk_id=asset.asset_id,
                chunk_type=asset.chunk_type,
                asset_s3_uri=None,
                page_no=asset.page_no,
                slide_no=asset.slide_no,
            ),
            "neighbors": [],
        }
    return None


def resolve_context_from_records(
    records: list[ChunkManifestRecord],
    chunk_id: str,
    neighbor_expand: int,
) -> dict[str, QueryResultContext | list[QueryResultContext]] | None:
    """
    EN: Resolve query context from manifest index projection records without loading the full manifest.
    CN: 不加载完整 manifest，直接通过 manifest 索引投影记录解析查询上下文。
    """
    for index, record in enumerate(records):
        if record.chunk_id != chunk_id:
            continue
        bounded_expand = min(neighbor_expand, MAX_CONTEXT_NEIGHBOR_EXPAND)
        start = max(0, index - bounded_expand)
        end = min(len(records), index + bounded_expand + 1)
        neighbors = [
            _record_to_context(records[position])
            for position in range(start, end)
            if position != index
        ]
        return {
            "match": _record_to_context(record),
            "neighbors": neighbors,
        }
    return None


def chunk_to_context(chunk: Any) -> QueryResultContext:
    """
    EN: Convert a manifest chunk record into a QueryResultContext for client consumption.
    CN: 将 manifest chunk 记录转换为供客户端消费的 QueryResultContext。
    """
    return QueryResultContext(
        chunk_id=chunk.chunk_id,
        chunk_type=chunk.chunk_type,
        text=chunk.text,
        page_no=chunk.page_no,
        page_span=chunk.page_span,
        slide_no=chunk.slide_no,
        section_path=chunk.section_path,
    )


def _record_to_context(record: ChunkManifestRecord) -> QueryResultContext:
    """
    EN: Convert a projection record into a QueryResultContext using the stored preview text.
    CN: 使用索引层保存的预览文本将投影记录转换为 QueryResultContext。
    """
    return QueryResultContext(
        chunk_id=record.chunk_id,
        chunk_type=record.chunk_type,
        text=record.text_preview,
        page_no=record.page_no,
        page_span=record.page_span,
        slide_no=record.slide_no,
        section_path=record.section_path,
    )
