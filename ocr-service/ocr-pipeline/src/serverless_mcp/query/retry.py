"""
EN: Query retry helpers for bounded read retries.
CN: 用于有界重试读取的查询重试辅助函数。
"""
from __future__ import annotations

from collections.abc import Callable
from time import sleep


def retry_read(
    factory: Callable[[], object],
    *,
    label: str,
    resource_id: str | None = None,
    max_attempts: int = 3,
) -> object:
    """
    EN: Retry transient read paths with a small bounded backoff.
    CN: 以小的有界退避重试瞬时读取路径。
    """
    last_exc: Exception | None = None
    for attempt in range(max_attempts):
        try:
            return factory()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt + 1 >= max_attempts or not is_retryable_read_error(exc):
                raise
            sleep(min(0.05 * (2**attempt), 0.5))
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"retry_read reached an impossible state for {label!r} (max_attempts={max_attempts})")


def is_retryable_read_error(exc: Exception) -> bool:
    """
    EN: Treat client/network throttles as retryable while leaving validation errors alone.
    CN: 将客户端/网络/限流错误视为可重试，而保留校验错误直接失败。
    """
    if isinstance(exc, ValueError):
        return False
    name = type(exc).__name__
    return name in {"ClientError", "EndpointConnectionError", "ReadTimeoutError", "ConnectionError", "TimeoutError"}
