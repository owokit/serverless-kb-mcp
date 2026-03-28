"""
EN: Lambda handler for the extract sync step.
CN: extract sync 步骤的 Lambda 处理器。
"""
from __future__ import annotations

from serverless_mcp.extract.handlers.common import build_action_handler
from serverless_mcp.extract.handlers.support import _get_components, validate_job, validate_processing_state
from serverless_mcp.runtime.observability import emit_trace


def _run_sync_extract(event: dict, _context: object | None) -> dict:
    """
    EN: Validate the extracted object state and run the synchronous extract path.
    CN: 校验提取状态并执行同步 extract 路径。
    """
    workflow = _get_components().workflow_for("sync_extract")
    job = validate_job(event.get("job"), required_for="sync_extract")
    processing_state = validate_processing_state(event.get("processing_state"), required_for="sync_extract")
    emit_trace(
        "handler.dispatch",
        action="sync_extract",
        document_uri=job.source.document_uri,
        trace_id=job.trace_id,
        processing_state_pk=processing_state.pk,
    )
    return workflow.sync_extract(job=job, processing_state=processing_state)


lambda_handler = build_action_handler("sync_extract", _run_sync_extract)
