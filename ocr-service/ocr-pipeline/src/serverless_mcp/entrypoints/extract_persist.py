"""
EN: Lambda handler for the extract persist step.
CN: extract persist 步骤的 Lambda 处理器。
"""
from __future__ import annotations

from urllib.parse import urlparse

from serverless_mcp.extract.handlers.common import build_action_handler
from serverless_mcp.extract.handlers.support import _get_components, validate_job, validate_processing_state
from serverless_mcp.runtime.observability import emit_trace


def _run_persist_ocr_result(event: dict, _context: object | None) -> dict:
    """
    EN: Validate the persisted OCR payload and complete manifest persistence.
    CN: 校验 OCR 持久化负载并完成 manifest 持久化。
    """
    workflow = _get_components().workflow_for("persist_ocr_result")
    json_url = event.get("json_url")
    normalized_json_url = json_url.strip() if isinstance(json_url, str) and json_url.strip() else None
    markdown_url = event.get("markdown_url")
    normalized_markdown_url = markdown_url.strip() if isinstance(markdown_url, str) and markdown_url.strip() else None
    if normalized_json_url is None and normalized_markdown_url is None:
        raise ValueError("json_url or markdown_url is required for persist_ocr_result")
    job = validate_job(event.get("job"), required_for="persist_ocr_result")
    processing_state = validate_processing_state(event.get("processing_state"), required_for="persist_ocr_result")
    trace_payload = {
        "action": "persist_ocr_result",
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
    return workflow.persist_ocr_result(
        job=job,
        processing_state=processing_state,
        json_url=normalized_json_url,
        markdown_url=normalized_markdown_url,
    )


lambda_handler = build_action_handler("persist_ocr_result", _run_persist_ocr_result)
