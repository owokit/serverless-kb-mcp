"""
EN: Lambda handler for the embed workflow consuming SQS embed queue.
CN: 消费 SQS embed 队列的 embed 工作流 Lambda 处理器。
"""
from __future__ import annotations

import os
from dataclasses import asdict
from functools import lru_cache
from time import monotonic

from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

from serverless_mcp.runtime.embed_runtime import build_embed_worker
from serverless_mcp.core.serialization import error_response
from serverless_mcp.core.parsers import parse_embedding_event
from serverless_mcp.embed.application import UnknownEmbeddingProfileError
from serverless_mcp.core.batch import classify_batch_failure, is_sqs_batch
from serverless_mcp.runtime.observability import emit_trace


_logger = Logger(service=os.environ.get("POWERTOOLS_SERVICE_NAME", "serverless-mcp-service"))
_EMBED_BATCH_FAILURE_TYPES = (ClientError, KeyError, OSError, RuntimeError, TypeError, ValueError)


@lru_cache(maxsize=1)
def _get_worker():
    """Return a cached embed worker built from the runtime composition root."""
    return build_embed_worker()


def lambda_handler(event: dict, _context) -> dict:
    """
    EN: Lambda entry point for embed workflow, consumes SQS messages and writes profile-scoped vectors.
    CN: embed 工作流的 Lambda 入口，消费 SQS 消息并写入按 profile 划分的向量。
    """
    handler_start = monotonic()
    request_id = getattr(_context, "aws_request_id", None)
    remaining_ms = getattr(_context, "get_remaining_time_in_millis", lambda: None)()
    _logger.info("Starting embed worker batch")
    emit_trace(
        "handler.start",
        handler="embed",
        request_id=request_id,
        remaining_ms=remaining_ms,
        event_keys=sorted(event.keys()) if isinstance(event, dict) else None,
    )
    if not event:
        emit_trace(
            "handler.validation_failed",
            handler="embed",
            request_id=request_id,
            error_message="Records are required for embed worker",
        )
        return error_response(400, "Records are required for embed worker")
    records = event.get("Records") or []
    if not is_sqs_batch(records):
        result = _process_embed_event(event)
        emit_trace(
            "handler.success",
            handler="embed",
            request_id=request_id,
            elapsed_ms=round((monotonic() - handler_start) * 1000, 2),
            raw_record_count=len(records),
            processed_count=result.get("processed_count"),
            failed_count=result.get("failed_count"),
        )
        return result

    processed: list[dict] = []
    failed: list[dict] = []
    batch_item_failures: list[dict[str, str]] = []
    failed_records: list[dict[str, object]] = []

    for index, record in enumerate(records):
        try:
            result = _process_embed_event({"Records": [record]})
            processed.extend(result.get("processed", []))
            failed.extend(result.get("failed", []))
        except _EMBED_BATCH_FAILURE_TYPES as exc:
            # EN: Record per-item failure and collect itemIdentifier for SQS partial batch retry.
            # CN: 记录单项失败，并收集 itemIdentifier 以便 SQS 部分批次重试。
            failure = classify_batch_failure(
                record,
                index,
                exc,
                stage="embed_batch_record",
                reason="inactive_profile" if type(exc).__name__ == "UnknownEmbeddingProfileError" else "unexpected_error",
            )
            failed.append(failure)
            failed_records.append(failure)
            batch_item_failures.append({"itemIdentifier": failure["itemIdentifier"]})
            _logger.exception("Failed to process embed SQS record")

    result = {
        "statusCode": 200,
        "processed_count": len(processed),
        "failed_count": len(failed),
        "processed": processed,
        "failed": failed,
        "batchItemFailures": batch_item_failures,
        "failed_records": failed_records,
    }
    emit_trace(
        "handler.success",
        handler="embed",
        request_id=request_id,
        elapsed_ms=round((monotonic() - handler_start) * 1000, 2),
        raw_record_count=len(records),
        processed_count=len(processed),
        failed_count=len(failed),
    )
    return result


def _process_embed_event(event: dict) -> dict:
    """
    EN: Parse and process a single embed event, dispatching jobs to the embed worker.
    CN: 解析并处理单个 embed 事件，将作业分发给 embed worker。
    """
    jobs = parse_embedding_event(event)
    processed: list[dict] = []
    failed: list[dict] = []
    worker = _get_worker()

    for job in jobs:
        try:
            processed.append(asdict(worker.process(job)))
        except UnknownEmbeddingProfileError as exc:
            # EN: Skip jobs targeting inactive profiles without failing the batch.
            # CN: 跳过指向已停用 profile 的作业，但不让整个批次失败。
            _logger.warning(
                "Skipping embed job for inactive embedding profile",
                extra={
                    "document_uri": job.source.document_uri,
                    "profile_id": job.profile_id,
                    "manifest_s3_uri": job.manifest_s3_uri,
                },
            )
            failed.append(
                {
                    "document_uri": job.source.document_uri,
                    "profile_id": job.profile_id,
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                    "stage": "profile_lookup",
                    "disposition": "skipped_inactive_profile",
                }
            )
        except _EMBED_BATCH_FAILURE_TYPES as exc:
            failed.append(
                {
                    "document_uri": job.source.document_uri,
                    "profile_id": job.profile_id,
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                    "stage": "embed_job",
                    "disposition": "job_error",
                }
            )
            if os.environ.get("FAIL_ON_JOB_ERROR", "true").lower() == "true":
                # EN: Propagate error to trigger SQS redrive when strict failure mode is enabled.
                # CN: 在启用严格失败模式时向上抛出错误，以触发 SQS 重投。
                raise

    return {
        "statusCode": 200,
        "processed_count": len(processed),
        "failed_count": len(failed),
        "processed": processed,
        "failed": failed,
    }
