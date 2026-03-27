"""
EN: Lambda handler for the extract submit step.
CN: extract submit 步骤的 Lambda 处理器。
"""
from __future__ import annotations

from serverless_mcp.extract.handlers.common import build_action_handler
from serverless_mcp.extract.handlers.support import _get_components, validate_job
from serverless_mcp.runtime.observability import emit_trace


def _run_submit_ocr_job(event: dict, _context: object | None) -> dict:
    """
    EN: Validate the job payload and submit the PaddleOCR request.
    CN: 校验作业负载并提交 PaddleOCR 请求。
    """
    workflow = _get_components().workflow_for("submit_ocr_job")
    job = validate_job(event.get("job"), required_for="submit_ocr_job")
    emit_trace("handler.dispatch", action="submit_ocr_job", document_uri=job.source.document_uri, trace_id=job.trace_id)
    return workflow.submit_ocr_job(job=job)


lambda_handler = build_action_handler("submit_ocr_job", _run_submit_ocr_job)

