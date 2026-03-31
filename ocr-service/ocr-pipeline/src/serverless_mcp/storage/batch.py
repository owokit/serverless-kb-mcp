"""
EN: Shared DynamoDB batch-write retry helper with exponential backoff.
CN: 带指数退避的 DynamoDB batch write 重试共享辅助函数。
"""
from __future__ import annotations

import time
import random
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")
R = TypeVar("R")


def dedupe_preserve_order(items: list[T]) -> list[T]:
    """
    EN: Return items with duplicates removed while preserving first-seen order.
    CN: 杩斿洖鍘婚噸鍚庣殑鍒楄〃锛屽苟淇濇寔棣栨鍑虹幇鐨勯『搴忋€?
    """
    if not items:
        return []
    return list(dict.fromkeys(items))


def batch_get_records(
    dynamodb_client: object,
    *,
    table_name: str,
    items: list[T],
    build_request_key: Callable[[T], dict[str, dict[str, str]]],
    parse_request_key: Callable[[dict[str, dict[str, str]]], T],
    parse_record_key: Callable[[R], T],
    parse_record: Callable[[dict], R],
    max_attempts: int = 8,
    base_delay_seconds: float = 0.05,
    sleeper: Callable[[float], None] = time.sleep,
    jitter: Callable[[float, float], float] = random.uniform,
) -> dict[T, R | None]:
    """
    EN: Load batch-get records by logical key with duplicate suppression and retry handling.
    CN: 按逻辑主键批量读取记录，自动去重并处理未完成项重试。
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    if not items:
        return {}

    records: dict[T, R | None] = {item: None for item in items}
    pending = dedupe_preserve_order(items)

    for attempt in range(max_attempts):
        pending = dedupe_preserve_order(pending)
        response = dynamodb_client.batch_get_item(
            RequestItems={
                table_name: {
                    "Keys": [build_request_key(item) for item in pending],
                    "ConsistentRead": True,
                }
            }
        )

        for item in response.get("Responses", {}).get(table_name, []):
            record = parse_record(item)
            records[parse_record_key(record)] = record

        unprocessed_items = response.get("UnprocessedKeys", {}).get(table_name, {}).get("Keys") or []
        if not unprocessed_items:
            return records

        pending = [parse_request_key(item) for item in unprocessed_items]
        if attempt < max_attempts - 1:
            max_sleep = min(base_delay_seconds * (2**attempt), 2.0)
            sleeper(jitter(0.0, max_sleep))

    raise RuntimeError(
        "DynamoDB batch_get_item did not drain after "
        f"{max_attempts} attempts; table={table_name}"
    )


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
