"""
EN: Lambda handler for the extract poll step.
CN: extract poll 步骤的 Lambda 处理器。
"""
from __future__ import annotations

from serverless_mcp.extract.handlers.common import build_action_handler
from serverless_mcp.extract.handlers.support import _get_components
from serverless_mcp.runtime.observability import emit_trace


def _run_poll_ocr_job(event: dict, _context: object | None) -> dict:
    """
    EN: Validate the poll payload and query PaddleOCR once.
    CN: 校验轮询负载并查询 PaddleOCR 一次。
    """
    workflow = _get_components().workflow_for("poll_ocr_job")
    job_id = str(event.get("job_id") or "").strip()
    if not job_id:
        raise ValueError("job_id is required for poll_ocr_job")
    poll_attempt = int(event.get("poll_attempt") or 0)
    max_poll_attempts = int(event.get("max_poll_attempts") or 0) or None
    emit_trace(
        "handler.dispatch",
        action="poll_ocr_job",
        job_id=job_id,
        poll_attempt=poll_attempt,
        max_poll_attempts=max_poll_attempts,
    )
    return workflow.poll_ocr_job(job_id=job_id, poll_attempt=poll_attempt, max_poll_attempts=max_poll_attempts)


lambda_handler = build_action_handler("poll_ocr_job", _run_poll_ocr_job)

