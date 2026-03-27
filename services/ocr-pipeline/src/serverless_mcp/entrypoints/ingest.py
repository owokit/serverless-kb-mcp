"""
EN: Lambda handler for the ingest workflow entry point.
CN: 为 ingest 工作流入口提供的 Lambda 处理器。
"""
from __future__ import annotations

import os
from time import monotonic

from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

from serverless_mcp.runtime.ingest import build_ingest_workflow_starter
from serverless_mcp.core.serialization import error_response
from serverless_mcp.core.batch import classify_batch_failure, is_sqs_batch
from serverless_mcp.runtime.observability import emit_trace

_logger = Logger(service=os.environ.get("POWERTOOLS_SERVICE_NAME", "serverless-mcp-service"))
_INGEST_BATCH_FAILURE_TYPES = (ClientError, KeyError, OSError, RuntimeError, TypeError, ValueError)


def lambda_handler(event: dict, _context) -> dict:
    """
    EN: Lambda entry point for ingest workflow, accepts S3 events and starts Step Functions executions.
    CN: ingest 工作流的 Lambda 入口，接收 S3 事件并启动 Step Functions 执行。

    Args:
        event:
            EN: S3 event notification batch containing bucket/key/version_id/sequencer.
            CN: 包含 bucket、key、version_id 和 sequencer 的 S3 事件通知批次。
        _context:
            EN: Lambda context (unused).
            CN: Lambda 上下文对象，当前未使用。

    Returns:
        EN: Batch processing result with started, skipped, and failed counts.
        CN: 包含已启动、已跳过和失败数量的批处理结果。
    """
    handler_start = monotonic()
    request_id = getattr(_context, "aws_request_id", None)
    remaining_ms = getattr(_context, "get_remaining_time_in_millis", lambda: None)()
    _logger.info("Starting ingest worker batch")
    emit_trace(
        "handler.start",
        handler="ingest",
        request_id=request_id,
        remaining_ms=remaining_ms,
        event_keys=sorted(event.keys()) if isinstance(event, dict) else None,
    )
    # EN: Early return on empty or non-dict events before allocating any AWS resources.
    # CN: 在分配任何 AWS 资源之前，先对空事件或非字典事件提前返回。
    if not isinstance(event, dict) or not event:
        emit_trace(
            "handler.validation_failed",
            handler="ingest",
            request_id=request_id,
            error_message="Records are required for ingest worker",
        )
        return error_response(400, "Records are required for ingest worker")

    starter = build_ingest_workflow_starter(_context)
    records = event.get("Records") or []
    if not is_sqs_batch(records):
        result = starter.handle_batch(event)
        emit_trace(
            "handler.success",
            handler="ingest",
            request_id=request_id,
            elapsed_ms=round((monotonic() - handler_start) * 1000, 2),
            raw_record_count=len(records),
            started_count=result.get("started_count"),
            skipped_count=result.get("skipped_count"),
            failed_count=result.get("failed_count"),
        )
        return result

    # EN: Process each SQS record independently, reporting per-item failures for partial batch retries.
    # CN: 逐条处理每个 SQS 记录，并为部分批次重试报告单项失败。
    started_count = 0
    skipped_count = 0
    failed_count = 0
    batch_item_failures: list[dict[str, str]] = []
    failed_records: list[dict[str, object]] = []

    for index, record in enumerate(records):
        try:
            result = starter.handle_batch({"Records": [record]})
            started_count += int(result.get("started_count", 0))
            skipped_count += int(result.get("skipped_count", 0))
            failed_count += int(result.get("failed_count", 0))
            failed_records.extend(result.get("failed", []) or [])
        except _INGEST_BATCH_FAILURE_TYPES as exc:
            failed_count += 1
            failure = classify_batch_failure(record, index, exc, stage="ingest_batch_record")
            batch_item_failures.append({"itemIdentifier": failure["itemIdentifier"]})
            failed_records.append(failure)
            _logger.exception("Failed to process ingest SQS record")

    result = {
        "statusCode": 200,
        "raw_record_count": len(records),
        "started_count": started_count,
        "skipped_count": skipped_count,
        "failed_count": failed_count,
        "batchItemFailures": batch_item_failures,
        "failed_records": failed_records,
    }
    emit_trace(
        "handler.success",
        handler="ingest",
        request_id=request_id,
        elapsed_ms=round((monotonic() - handler_start) * 1000, 2),
        raw_record_count=len(records),
        started_count=started_count,
        skipped_count=skipped_count,
        failed_count=failed_count,
    )
    return result
