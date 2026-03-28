"""
EN: Lambda handler for the extract prepare step.
CN: extract prepare 步骤的 Lambda 处理器。
"""
from __future__ import annotations

from serverless_mcp.extract.handlers.common import build_action_handler
from serverless_mcp.extract.handlers.support import _get_components, validate_job
from serverless_mcp.runtime.observability import emit_trace


def _run_prepare_job(event: dict, _context: object | None) -> dict:
    """
    EN: Validate the job payload and prepare the extract workflow state.
    CN: 校验作业负载并准备 extract 工作流状态。
    """
    workflow = _get_components().workflow_for("prepare_job")
    job = validate_job(event.get("job"), required_for="prepare_job")
    emit_trace("handler.dispatch", action="prepare_job", document_uri=job.source.document_uri, trace_id=job.trace_id)
    return workflow.prepare_job(job=job)


lambda_handler = build_action_handler("prepare_job", _run_prepare_job)
