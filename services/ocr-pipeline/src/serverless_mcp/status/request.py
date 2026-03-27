"""
EN: Shared request parsing helpers for the job status service entrypoint.
CN: job status 服务入口共享的请求解析辅助工具。
"""
from __future__ import annotations

import json
from typing import Any

from serverless_mcp.core.serialization import coerce_required_str
from serverless_mcp.status.application import JobStatusRequest


def build_job_status_request(event: dict[str, Any]) -> JobStatusRequest:
    """
    EN: Parse a job status request from query string, body, or direct invocation payload.
    CN: 从查询字符串、body 或直接调用载荷中解析 job status 请求。
    """
    payload = extract_job_status_payload(event)
    return JobStatusRequest(
        bucket=coerce_required_str(payload.get("bucket") or payload.get("source_bucket"), field_name="bucket"),
        key=coerce_required_str(payload.get("key") or payload.get("source_key"), field_name="key"),
        version_id=_optional_str(payload.get("version_id")),
        tenant_id=_optional_str(payload.get("tenant_id")),
    )


def extract_job_status_payload(event: dict[str, Any]) -> dict[str, object]:
    """
    EN: Merge job status payload sources while rejecting conflicting field values.
    CN: 合并 job status 负载来源，同时拒绝冲突的字段值。
    """
    if not isinstance(event, dict):
        raise ValueError("Event payload must be an object")
    payload: dict[str, object] = {}
    seen_sources: dict[str, str] = {}
    query_params = event.get("queryStringParameters")
    if isinstance(query_params, dict):
        _merge_payload_source(payload, query_params, source_name="queryStringParameters", seen_sources=seen_sources)
    body = event.get("body")
    if isinstance(body, str) and body.strip():
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError as exc:
            raise ValueError("Request body must be a JSON object") from exc
        if not isinstance(parsed, dict):
            raise ValueError("Request body must be a JSON object")
        _merge_payload_source(payload, parsed, source_name="body", seen_sources=seen_sources)
    elif isinstance(body, dict):
        _merge_payload_source(payload, body, source_name="body", seen_sources=seen_sources)
    # EN: Merge direct event keys last only when they do not conflict with earlier sources.
    # CN: 仅在与前序来源不冲突时，才最后合并直接位于事件顶层的键。
    direct_keys = {
        key: value
        for key, value in event.items()
        if key not in {"queryStringParameters", "body", "headers", "requestContext", "isBase64Encoded"}
    }
    _merge_payload_source(payload, direct_keys, source_name="direct_event", seen_sources=seen_sources)
    return payload


def _merge_payload_source(
    payload: dict[str, object],
    source: dict[str, object],
    *,
    source_name: str,
    seen_sources: dict[str, str],
) -> None:
    """
    EN: Merge one payload source while rejecting conflicting values across origins.
    CN: 合并单个负载来源，同时拒绝不同来源之间的冲突值。
    """
    for key, value in source.items():
        if value is None:
            continue
        previous_source = seen_sources.get(key)
        if previous_source is not None and payload.get(key) != value:
            raise ValueError(f"Conflicting {key} values supplied by {previous_source} and {source_name}")
        payload[key] = value
        seen_sources[key] = source_name


def _optional_str(value: object) -> str | None:
    """
    EN: Convert a value to a stripped string or return None when absent.
    CN: 将值转换为去除首尾空白的字符串，缺失时返回 None。
    """
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None
