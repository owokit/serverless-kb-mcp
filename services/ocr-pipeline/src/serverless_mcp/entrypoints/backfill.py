"""
EN: Lambda handler for historical embedding backfill by profile.
CN: 按 profile 执行历史 embedding 回填的 Lambda 处理器。
"""
from __future__ import annotations

import os
from dataclasses import asdict
from time import monotonic

from aws_lambda_powertools import Logger

from serverless_mcp.runtime.embed_runtime import build_backfill_service
from serverless_mcp.core.serialization import error_response
from serverless_mcp.runtime.observability import emit_trace

_logger = Logger(service=os.environ.get("POWERTOOLS_SERVICE_NAME", "serverless-mcp-service"))


def lambda_handler(event: dict, _context) -> dict:
    """
    EN: Lambda entry point for embedding backfill, dispatches re-embed jobs for historical content.
    CN: embedding 回填的 Lambda 入口，为历史内容重新分发重嵌作业。
    """
    handler_start = monotonic()
    request_id = getattr(_context, "aws_request_id", None)
    emit_trace(
        "handler.start",
        handler="backfill",
        request_id=request_id,
        event_keys=sorted(event.keys()) if isinstance(event, dict) else None,
    )
    if not isinstance(event, dict) or not event:
        emit_trace(
            "handler.validation_failed",
            handler="backfill",
            request_id=request_id,
            error_message="profile_id is required for backfill worker",
        )
        return error_response(400, "profile_id is required for backfill worker")

    from serverless_mcp.embed.backfill_request import build_backfill_request

    request = build_backfill_request(event)
    backfill_kwargs = {
        "profile_id": request.profile_id,
        "trace_id": request.trace_id,
        "force": request.force,
    }
    if request.resume_after_object_pk is not None:
        backfill_kwargs["resume_after_object_pk"] = request.resume_after_object_pk
    if request.max_records is not None:
        backfill_kwargs["max_records"] = request.max_records
    outcome = build_backfill_service().backfill_profile(**backfill_kwargs)
    _logger.info("Embedding backfill completed", extra={"profile_id": request.profile_id, "outcome": asdict(outcome)})
    result = {
        "statusCode": 200,
        "profile_id": outcome.profile_id,
        "scanned_count": outcome.scanned_count,
        "eligible_count": outcome.eligible_count,
        "dispatched_job_count": outcome.dispatched_job_count,
        "skipped_deleted_count": outcome.skipped_deleted_count,
        "skipped_not_ready_count": outcome.skipped_not_ready_count,
        "skipped_stale_count": outcome.skipped_stale_count,
        "skipped_projection_count": outcome.skipped_projection_count,
        "resume_after_object_pk": outcome.resume_after_object_pk,
        "is_truncated": outcome.is_truncated,
        "samples": [asdict(sample) for sample in outcome.samples],
    }
    emit_trace(
        "handler.success",
        handler="backfill",
        request_id=request_id,
        elapsed_ms=round((monotonic() - handler_start) * 1000, 2),
        profile_id=request.profile_id,
        force=request.force,
        resume_after_object_pk=request.resume_after_object_pk,
        max_records=request.max_records,
        scanned_count=outcome.scanned_count,
        eligible_count=outcome.eligible_count,
        dispatched_job_count=outcome.dispatched_job_count,
    )
    return result

