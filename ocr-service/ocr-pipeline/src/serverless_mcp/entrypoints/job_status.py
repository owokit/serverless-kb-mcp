"""
EN: Lambda handler for read-only job status aggregation.
CN: 用于只读作业状态聚合的 Lambda 处理器。
"""
from __future__ import annotations

import json
import os
from time import monotonic

from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

from serverless_mcp.status.runtime import build_job_status_service
from serverless_mcp.core.serialization import error_response
from serverless_mcp.status.request import build_job_status_request


_logger = Logger(service=os.environ.get("POWERTOOLS_SERVICE_NAME", "serverless-mcp-service"))
_JOB_STATUS_FAILURE_TYPES = (ClientError, KeyError, OSError, RuntimeError, TypeError, ValueError)


def lambda_handler(event: dict, _context) -> dict:
    """
    EN: Resolve one job status snapshot from HTTP query parameters or a direct invocation payload.
    CN: 从 HTTP 查询参数或直接调用载荷中解析一份作业状态快照。

    Args:
        event:
            EN: API Gateway event, direct invocation payload, or raw status query dict.
            CN: API Gateway 事件、直接调用载荷，或原始状态查询字典。
        _context:
            EN: Lambda context object used for request_id extraction.
            CN: 用于提取 request_id 的 Lambda 上下文对象。

    Returns:
        EN: HTTP response dict with JSON-encoded job status body.
        CN: 包含 JSON 编码的任务状态 body 的 HTTP 响应字典。

    Raises:
        EN: ValueError when required bucket or key fields are missing.
        EN: Exception when the underlying status service fails unexpectedly.
        CN: 当缺少必需的 bucket 或 key 字段时抛出 ValueError。
        CN: 当底层状态服务意外失败时抛出 Exception。
    """
    handler_start = monotonic()
    request_id = getattr(_context, "aws_request_id", None)
    _logger.info("Starting job status lookup")
    try:
        request = build_job_status_request(event)
        result = build_job_status_service().build_status(request)
        response = {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json; charset=utf-8",
                "Cache-Control": "no-store",
            },
            "body": json.dumps(result, ensure_ascii=False),
        }
        _logger.info(
            "job status lookup completed",
            request_id=request_id,
            elapsed_ms=round((monotonic() - handler_start) * 1000, 2),
            status=result.get("overall_status"),
        )
        return response
    except ValueError as exc:
        return error_response(400, str(exc))
    except _JOB_STATUS_FAILURE_TYPES:
        _logger.exception("job status lookup failed")
        raise
