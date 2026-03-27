"""
EN: Shared DynamoDB batch-write retry helper with exponential backoff.
CN: 带指数退避的 DynamoDB batch write 重试共享辅助函数。
"""
from __future__ import annotations

import time
import random
from collections.abc import Callable


def flush_batch_write(
    dynamodb_client: object,
    request_items: dict[str, list[dict]],
    *,
    max_attempts: int = 8,
    base_delay_seconds: float = 0.05,
    sleeper: Callable[[float], None] = time.sleep,
    jitter: Callable[[float, float], float] = random.uniform,
) -> None:
    """
    EN: Drain a batch_write_item request with bounded exponential backoff.
    CN: 使用有界指数退避清空 batch_write_item 请求。
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    pending = request_items
    for attempt in range(max_attempts):
        response = dynamodb_client.batch_write_item(RequestItems=pending)
        pending = response.get("UnprocessedItems") or {}
        if not pending:
            return
        if attempt == max_attempts - 1:
            break
        max_sleep = min(base_delay_seconds * (2**attempt), 2.0)
        sleeper(jitter(0.0, max_sleep))

    raise RuntimeError(
        "DynamoDB batch write did not drain after "
        f"{max_attempts} attempts; unprocessed tables={', '.join(sorted(pending)) or '<unknown>'}"
    )
