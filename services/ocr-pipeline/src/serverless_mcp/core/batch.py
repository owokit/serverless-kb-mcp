"""
EN: Shared SQS batch adapter helpers for service handlers.
CN: 服务 handler 共享的 SQS 批处理适配辅助工具。
"""
from __future__ import annotations

from typing import Any


def is_sqs_batch(records: list[dict[str, Any]]) -> bool:
    """
    EN: Detect whether every record in the batch originates from SQS.
    CN: 检测批次中的每条记录是否都来自 SQS。
    """
    return bool(records) and all((record.get("eventSource") or record.get("EventSource")) == "aws:sqs" for record in records)


def resolve_sqs_item_identifier(record: dict[str, Any], index: int) -> str:
    """
    EN: Resolve the SQS message identifier for partial batch failure reporting.
    CN: 解析用于部分批次失败报告的 SQS 消息标识。
    """
    return str(record.get("messageId") or record.get("messageID") or index)


def classify_batch_failure(
    record: dict[str, Any],
    index: int,
    exc: Exception,
    *,
    stage: str,
    reason: str = "unexpected_error",
) -> dict[str, object]:
    """
    EN: Convert a record-level batch failure into a structured diagnostic entry.
    CN: 将记录级批处理失败转换成结构化诊断条目。
    """
    return {
        "itemIdentifier": resolve_sqs_item_identifier(record, index),
        "error_type": type(exc).__name__,
        "reason": reason if reason != "unexpected_error" or type(exc).__name__ != "ValueError" else "validation_error",
        "error_message": str(exc),
        "stage": stage,
    }
