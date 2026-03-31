"""
EN: Lambda handler for the Step Functions extract router.
CN: Step Functions extract 路由 Lambda 处理器。
"""
from __future__ import annotations

from time import monotonic
from urllib.parse import urlparse

from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError
from pydantic import ValidationError

from serverless_mcp.core.serialization import error_response
from serverless_mcp.extract.contracts import build_extract_failure_details
from serverless_mcp.extract.handlers.support import _get_components, validate_job, validate_processing_state
from serverless_mcp.runtime.observability import emit_metric, emit_trace


_logger = Logger(service="serverless-mcp-service")
_ROUTER_FAILURE_TYPES = (ClientError, KeyError, OSError, RuntimeError, TypeError, ValueError)


def lambda_handler(event: dict, _context) -> dict:
    """
    EN: Route Step Functions actions with structural pattern matching and validated payloads.
    CN: 通过结构化模式匹配和校验后的负载路由 Step Functions 动作。
    """
    if not event:
        emit_trace("handler.empty_event")
        emit_metric("extract.handler.failure", action="extract", failure_kind="empty_event")
        return error_response(400, "action is required for extract workflow")
    action = event.get("action")
    if not isinstance(action, str) or not action:
        emit_metric("extract.handler.failure", action="extract", failure_kind="missing_action")
        raise ValueError("action is required for extract workflow")
    request_id = getattr(_context, "aws_request_id", None)
    remaining_ms = getattr(_context, "get_remaining_time_in_millis", lambda: None)()
    _logger.info("Starting extract workflow handler")
    emit_trace(
        "handler.start",
        action=action,
        request_id=request_id,
        remaining_ms=remaining_ms,
        event_keys=sorted(event.keys()),
    )
    emit_metric("extract.handler.invocation", action=action)
    handler_start = monotonic()
    try:
        workflow = _get_components().workflow_for(action)
        match event:
            case {"action": "prepare_job", "job": job_payload}:
                job = validate_job(job_payload, required_for="prepare_job")
                emit_trace(
                    "handler.dispatch",
                    action=action,
                    document_uri=job.source.document_uri,
                    trace_id=job.trace_id,
                )
                result = workflow.prepare_job(job=job)
            case {
                "action": "sync_extract",
                "job": job_payload,
                "processing_state": processing_state_payload,
            }:
                job = validate_job(job_payload, required_for="sync_extract")
                processing_state = validate_processing_state(
                    processing_state_payload,
                    required_for="sync_extract",
                )
                emit_trace(
                    "handler.dispatch",
                    action=action,
                    document_uri=job.source.document_uri,
                    trace_id=job.trace_id,
                    processing_state_pk=processing_state.pk,
                )
                result = workflow.sync_extract(job=job, processing_state=processing_state)
            case {"action": "submit_ocr_job", "job": job_payload}:
                job = validate_job(job_payload, required_for="submit_ocr_job")
                emit_trace(
                    "handler.dispatch",
                    action=action,
                    document_uri=job.source.document_uri,
                    trace_id=job.trace_id,
                )
                result = workflow.submit_ocr_job(job=job)
            case {"action": "poll_ocr_job", "job_id": str(job_id)} if job_id:
                poll_attempt = int(event.get("poll_attempt") or 0)
                max_poll_attempts = int(event.get("max_poll_attempts") or 0) or None
                emit_trace(
                    "handler.dispatch",
                    action=action,
                    job_id=job_id,
                    poll_attempt=poll_attempt,
                    max_poll_attempts=max_poll_attempts,
                )
                result = workflow.poll_ocr_job(
                    job_id=job_id,
                    poll_attempt=poll_attempt,
                    max_poll_attempts=max_poll_attempts,
                )
            case {
                "action": "persist_ocr_result",
                "job": job_payload,
                "processing_state": processing_state_payload,
            }:
                json_url = event.get("json_url")
                normalized_json_url = json_url.strip() if isinstance(json_url, str) and json_url.strip() else None
                markdown_url = event.get("markdown_url")
                normalized_markdown_url = markdown_url.strip() if isinstance(markdown_url, str) and markdown_url.strip() else None
                if normalized_json_url is None and normalized_markdown_url is None:
                    raise ValueError("json_url or markdown_url is required for persist_ocr_result")
                job = validate_job(job_payload, required_for="persist_ocr_result")
                processing_state = validate_processing_state(
                    processing_state_payload,
                    required_for="persist_ocr_result",
                )
                trace_payload = {
                    "action": action,
                    "document_uri": job.source.document_uri,
                    "trace_id": job.trace_id,
                    "processing_state_pk": processing_state.pk,
                    "json_url_present": normalized_json_url is not None,
                    "markdown_url_present": normalized_markdown_url is not None,
                }
                if normalized_json_url is not None:
                    trace_payload.update(
                        json_url_host=urlparse(normalized_json_url).hostname,
                        json_url_path=urlparse(normalized_json_url).path,
                    )
                if normalized_markdown_url is not None:
                    trace_payload.update(
                        markdown_url_host=urlparse(normalized_markdown_url).hostname,
                        markdown_url_path=urlparse(normalized_markdown_url).path,
                    )
                emit_trace(
                    "handler.dispatch",
                    **trace_payload,
                )
                result = workflow.persist_ocr_result(
                    job=job,
                    processing_state=processing_state,
                    json_url=normalized_json_url,
                    markdown_url=normalized_markdown_url,
                )
            case {"action": "mark_failed", "job": job_payload, "failure": failure_payload}:
                job = validate_job(job_payload, required_for="mark_failed")
                if not isinstance(failure_payload, dict):
                    raise ValueError("failure is required for mark_failed")
                failure = build_extract_failure_details(
                    str(failure_payload.get("error") or "").strip(),
                    str(failure_payload.get("cause") or "").strip() or None,
                )
                emit_trace(
                    "handler.dispatch",
                    action=action,
                    document_uri=job.source.document_uri,
                    trace_id=job.trace_id,
                    error_message=failure.message,
                    failure_domain=failure.domain,
                )
                emit_metric("extract.failure.domain_recorded", action=action, failure_kind=failure.domain)
                result = workflow.mark_failed(job=job, failure=failure)
            case {"action": str(unsupported_action)}:
                raise ValueError(f"Unsupported extract workflow action: {unsupported_action}")
        emit_trace(
            "handler.success",
            action=action,
            request_id=request_id,
            elapsed_ms=round((monotonic() - handler_start) * 1000, 2),
            result_keys=sorted(result.keys()) if isinstance(result, dict) else None,
        )
        emit_metric(
            "extract.handler.duration_ms",
            round((monotonic() - handler_start) * 1000, 2),
            unit="Milliseconds",
            action=action,
            outcome="success",
        )
        return result
    except ValidationError as exc:
        emit_trace(
            "handler.validation_failed",
            action=action,
            request_id=request_id,
            error_message=str(exc),
        )
        emit_metric("extract.handler.failure", action=action, failure_kind="validation")
        raise ValueError(f"Invalid extract workflow payload: {exc}") from exc
    except _ROUTER_FAILURE_TYPES as exc:
        emit_trace(
            "handler.failed",
            action=action,
            request_id=request_id,
            elapsed_ms=round((monotonic() - handler_start) * 1000, 2),
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
        emit_metric("extract.handler.failure", action=action, failure_kind=type(exc).__name__)
        raise
