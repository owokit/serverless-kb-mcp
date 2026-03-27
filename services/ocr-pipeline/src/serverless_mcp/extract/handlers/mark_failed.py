"""
EN: Lambda handler for the extract failure-marking step.
CN: extract 失败标记步骤的 Lambda 处理器。
"""
from __future__ import annotations

from serverless_mcp.extract.contracts import build_extract_failure_details
from serverless_mcp.extract.handlers.common import build_action_handler
from serverless_mcp.extract.handlers.support import _get_components, validate_job
from serverless_mcp.runtime.observability import emit_metric, emit_trace


def _run_mark_failed(event: dict, _context: object | None) -> dict:
    """
    EN: Validate the failure payload and mark the object state as failed.
    CN: 校验失败负载并将对象状态标记为失败。
    """
    workflow = _get_components().workflow_for("mark_failed")
    job = validate_job(event.get("job"), required_for="mark_failed")
    failure = build_extract_failure_details(
        str(event.get("error") or "").strip(),
        str(event.get("cause") or "").strip() or None,
    )
    emit_trace(
        "handler.dispatch",
        action="mark_failed",
        document_uri=job.source.document_uri,
        trace_id=job.trace_id,
        error=failure.error,
        cause=failure.cause,
        failure_domain=failure.domain,
    )
    emit_metric("extract.failure.domain_recorded", action="mark_failed", failure_kind=failure.domain)
    return workflow.mark_failed(job=job, failure=failure)


lambda_handler = build_action_handler("mark_failed", _run_mark_failed)

