"""
EN: Core serialization helpers shared by Lambda handlers and MCP adapters.
CN: Lambda 处理器和 MCP 适配层共享的核心序列化辅助工具。
"""
from __future__ import annotations

import json
from dataclasses import asdict
from hashlib import sha1
from typing import Any, Callable

from serverless_mcp.domain.models import QueryResponse, S3ObjectRef


def coerce_required_str(value: Any, *, field_name: str) -> str:
    """
    EN: Coerce a required value to a stripped string.
    CN: 将必需字段转换为去除首尾空白的字符串。
    """
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value.strip()


def coerce_bounded_int(value: Any, *, field_name: str, minimum: int, maximum: int) -> int:
    """
    EN: Coerce a value to an integer and enforce an inclusive range.
    CN: 将值转换为整数并校验其在闭区间范围内。
    """
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer") from exc
    if parsed < minimum or parsed > maximum:
        raise ValueError(f"{field_name} must be between {minimum} and {maximum}")
    return parsed


def error_response(status_code: int, message: str) -> dict:
    """
    EN: Build a compact JSON error response for Lambda handlers.
    CN: 为 Lambda 处理器构建紧凑的 JSON 错误响应。
    """
    return {
        "statusCode": status_code,
        "body": json.dumps({"message": message}, ensure_ascii=False),
        "headers": {"Content-Type": "application/json; charset=utf-8"},
    }


def serialize_query_response(
    response: QueryResponse,
    *,
    delivery_resolver: Callable[[S3ObjectRef], dict | None] | None = None,
) -> dict[str, object]:
    """
    EN: Serialize a ranked query response for remote MCP clients.
    CN: 将排序后的查询响应序列化为远程 MCP 客户端可消费的结构。
    """
    results: list[dict[str, object]] = []
    for rank, item in enumerate(response.results, start=1):
        metadata = dict(item.metadata)
        fusion_score = metadata.pop("__fusion_score__", None)
        profile_hit_count = metadata.pop("__profile_hits__", None)
        result = {
            "document_id": build_document_id(item.source),
            "object_pk": item.source.object_pk,
            "version_id": item.source.version_id,
            "document_uri": item.source.document_uri,
            "rank": rank,
            "fusion_score": float(fusion_score) if fusion_score is not None else None,
            "profile_hit_count": int(profile_hit_count) if profile_hit_count is not None else None,
            "metadata": metadata,
            "match": asdict(item.match),
            "neighbors": [asdict(neighbor) for neighbor in item.neighbors],
        }
        if delivery_resolver is not None:
            result["delivery"] = delivery_resolver(item.source)
        results.append(result)
    return {
        "query": response.query,
        "results": results,
        "degraded_profiles": [asdict(item) for item in response.degraded_profiles],
    }


def build_document_id(source: S3ObjectRef) -> str:
    """
    EN: Derive a stable document identifier from tenant, bucket, key, and version_id.
    CN: 基于 tenant、bucket、key 和 version_id 生成稳定的 document identifier。
    """
    digest = sha1(
        f"{source.tenant_id}\0{source.bucket}\0{source.key}\0{source.version_id}".encode("utf-8"),
        usedforsecurity=False,
    ).hexdigest()
    return f"doc_{digest[:20]}"

