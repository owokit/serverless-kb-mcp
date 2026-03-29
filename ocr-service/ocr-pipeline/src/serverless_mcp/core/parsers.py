"""
EN: Core parsers for S3 notifications and embed queue payloads.
CN: S3 通知和 embedding 队列负载的核心解析器。
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote_plus

from serverless_mcp.domain.models import EmbeddingJobMessage, EmbeddingRequest, ExtractJobMessage, S3ObjectRef, utc_now_iso


@dataclass(slots=True)
class ParsedEventBatch:
    """
    EN: Parsed event batch containing ingest jobs and raw record count.
    CN: 包含 ingest 作业与原始记录数的解析后事件批次。
    """

    jobs: list[ExtractJobMessage]
    raw_record_count: int


SUPPORTED_S3_CREATE_PREFIX = "ObjectCreated:"
SUPPORTED_S3_REMOVE_PREFIX = "ObjectRemoved:"
MAX_SQS_NESTING_DEPTH = 5


def parse_event(event: dict[str, Any], *, _depth: int = 0) -> ParsedEventBatch:
    """
    EN: Parse S3 event notification or SQS batch into ingest job messages.
    CN: 将 S3 事件通知或 SQS 批次解析为 ingest 作业消息。
    """
    if _is_s3_test_event(event):
        return ParsedEventBatch(jobs=[], raw_record_count=1)

    records = event.get("Records")
    if isinstance(records, list):
        jobs: list[ExtractJobMessage] = []
        for record in records:
            jobs.extend(_parse_record(record, depth=_depth))
        return ParsedEventBatch(jobs=jobs, raw_record_count=len(records))

    raise ValueError("Unsupported event payload")


def parse_embedding_event(event: dict) -> list[EmbeddingJobMessage]:
    """
    EN: Parse SQS event batch into embedding job messages.
    CN: 将 SQS 批次解析为 embedding 作业消息。
    """
    records = event.get("Records") or []
    jobs: list[EmbeddingJobMessage] = []
    for record in records:
        body = record.get("body")
        if not isinstance(body, str):
            continue
        payload = json.loads(body)
        source_payload = dict(payload["source"])
        if "security_scope" in source_payload and isinstance(source_payload["security_scope"], list):
            source_payload["security_scope"] = tuple(source_payload["security_scope"])
        source = S3ObjectRef(**source_payload)
        requests = [EmbeddingRequest(**item) for item in payload.get("requests", [])]
        jobs.append(
            EmbeddingJobMessage(
                source=source,
                profile_id=payload["profile_id"],
                trace_id=payload["trace_id"],
                manifest_s3_uri=payload["manifest_s3_uri"],
                requests=requests,
                previous_version_id=payload.get("previous_version_id"),
                previous_manifest_s3_uri=payload.get("previous_manifest_s3_uri"),
                requested_at=payload.get("requested_at") or utc_now_iso(),
            )
        )
    return jobs


def _parse_record(record: dict[str, Any], *, depth: int) -> list[ExtractJobMessage]:
    event_source = record.get("eventSource") or record.get("EventSource")
    if event_source == "aws:s3":
        event_name = record.get("eventName", "")
        if event_name.startswith(SUPPORTED_S3_CREATE_PREFIX):
            return [_build_job(_parse_s3_record(record), trace_id=record.get("responseElements", {}).get("x-amz-request-id"))]
        if event_name.startswith(SUPPORTED_S3_REMOVE_PREFIX):
            return [
                _build_job(
                    _parse_s3_record(record),
                    trace_id=record.get("responseElements", {}).get("x-amz-request-id"),
                    operation="DELETE",
                )
            ]
        return []
    if event_source == "aws:sqs":
        if depth >= MAX_SQS_NESTING_DEPTH:
            raise ValueError(f"Nested SQS event depth exceeds {MAX_SQS_NESTING_DEPTH}")
        body = record.get("body")
        if not isinstance(body, str):
            return []
        payload = json.loads(body)
        return parse_event(payload, _depth=depth + 1).jobs
    return []


def _parse_s3_record(record: dict[str, Any]) -> S3ObjectRef:
    _validate_s3_event_version(record.get("eventVersion"))
    s3_record = record["s3"]
    bucket = s3_record["bucket"]["name"]
    object_info = s3_record["object"]
    version_id = object_info.get("versionId")
    if not version_id:
        raise ValueError("S3 event is missing versionId")
    return S3ObjectRef(
        tenant_id=record.get("tenant_id", "lookup"),
        bucket=bucket,
        key=unquote_plus(object_info["key"]),
        version_id=version_id,
        sequencer=object_info.get("sequencer"),
        etag=object_info.get("eTag"),
        language=record.get("language", "zh"),
    )


def _build_job(source: S3ObjectRef, *, trace_id: str | None, operation: str = "UPSERT") -> ExtractJobMessage:
    return ExtractJobMessage(
        source=source,
        trace_id=trace_id or source.document_uri,
        operation=operation,
    )


def _is_s3_test_event(event: dict[str, Any]) -> bool:
    return event.get("Event") == "s3:TestEvent" and event.get("Service") == "Amazon S3"


def _validate_s3_event_version(event_version: str | None) -> None:
    if not event_version:
        raise ValueError("S3 event is missing eventVersion")
    major, _, minor = event_version.partition(".")
    if major != "2":
        raise ValueError(f"Unsupported S3 event major version: {event_version}")
    if minor and int(minor) < 1:
        raise ValueError(f"Unsupported S3 event minor version: {event_version}")

