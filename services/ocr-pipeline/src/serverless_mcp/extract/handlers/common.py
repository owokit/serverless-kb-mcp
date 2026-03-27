"""
EN: Shared helpers for action-scoped extract handlers.
CN: 面向动作拆分后的 extract handler 共享辅助函数。
"""
from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from time import monotonic

from pydantic import ValidationError

from serverless_mcp.core.serialization import error_response
from serverless_mcp.runtime.observability import emit_metric, emit_trace


def build_action_handler(
    action: str,
    executor: Callable[[dict, object | None], dict],
) -> Callable[[dict, object | None], dict]:
    """
    EN: Wrap an action-specific executor with consistent tracing and error handling.
    CN: 用一致的追踪和错误处理包装动作级执行器。
    """

    @wraps(executor)
    def _handler(event: dict, context: object | None) -> dict:
        if not isinstance(event, dict):
            raise ValueError("Event payload must be an object")
        if not event:
            emit_trace("handler.empty_event", action=action)
            emit_metric("extract.handler.failure", action=action, failure_kind="empty_event")
            return error_response(400, f"{action} is required for extract workflow")

        request_id = getattr(context, "aws_request_id", None)
        remaining_ms = getattr(context, "get_remaining_time_in_millis", lambda: None)()
        _log_handler_start(action, event, request_id=request_id, remaining_ms=remaining_ms)
        emit_metric("extract.handler.invocation", action=action)
        handler_start = monotonic()
        try:
            result = executor(event, context)
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
        except Exception as exc:  # noqa: BLE001
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

    return _handler


def _log_handler_start(action: str, event: dict, *, request_id: str | None, remaining_ms: int | None) -> None:
    """
    EN: Emit a standardized start trace for action-scoped handlers.
    CN: 为动作级 handler 输出统一的启动 trace。
    """
    emit_trace(
        "handler.start",
        action=action,
        request_id=request_id,
        remaining_ms=remaining_ms,
        event_keys=sorted(event.keys()),
    )
