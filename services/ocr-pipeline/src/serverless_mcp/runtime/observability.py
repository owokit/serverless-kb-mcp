"""
EN: Lightweight structured logging for CloudWatch trace correlation.
CN: 用于 CloudWatch 追踪关联的轻量级结构化日志模块。
"""
from __future__ import annotations

import json
import os


_SENSITIVE_KEY_PARTS = (
    "api_key",
    "authorization",
    "credential",
    "password",
    "private_key",
    "secret",
    "token",
)


def emit_trace(stage: str, **fields: object) -> None:
    """
    EN: Emit a compact JSON trace line for CloudWatch correlation.
    CN: 输出一行紧凑的 JSON 追踪日志，便于在 CloudWatch 中关联排障。

    Args:
        stage:
            EN: Pipeline stage identifier such as "handler.start", "extract", or "persist".
            CN: 管道阶段标识符，如 "handler.start"、"extract" 或 "persist"。
        **fields:
        EN: Arbitrary key-value pairs included in the trace payload, serialized via default=str.
        CN: 包含在追踪负载中的任意键值对，通过 default=str 序列化。
    """
    print(json.dumps(_sanitize_trace_record(stage, fields), ensure_ascii=False), flush=True)


def emit_metric(
    metric_name: str,
    value: float = 1,
    *,
    unit: str = "Count",
    namespace: str | None = None,
    **dimensions: object,
) -> None:
    """
    EN: Emit a compact structured metric line for log-based dashboards and alerts.
    CN: 输出紧凑的结构化 metric 日志，供基于日志的看板和告警使用。
    """
    record = {
        "stage": "metric",
        "namespace": namespace or os.environ.get("METRICS_NAMESPACE", "McpKnowledgeS3Vectors"),
        "metric_name": metric_name,
        "metric_value": value,
        "metric_unit": unit,
        "dimensions": _sanitize_metric_dimensions(dimensions),
    }
    print(json.dumps(record, ensure_ascii=False), flush=True)


def _sanitize_trace_record(stage: str, fields: dict[str, object]) -> dict[str, object]:
    """
    EN: Normalize trace fields into a JSON-safe record and redact obvious secret-bearing keys.
    CN: 将 trace 字段规范化为 JSON 安全记录，并对明显携带密钥的字段做脱敏。
    """
    record: dict[str, object] = {"stage": stage}
    for key, value in fields.items():
        record[key] = _sanitize_trace_value(key, value)
    return record


def _sanitize_metric_dimensions(dimensions: dict[str, object]) -> dict[str, object]:
    """
    EN: Normalize metric dimensions and redact obvious secret-bearing fields.
    CN: 规范化 metric 维度并脱敏明显携带密钥的字段。
    """
    return {str(key): _sanitize_trace_value(str(key), value) for key, value in dimensions.items()}


def _sanitize_trace_value(field_name: str, value: object, *, depth: int = 0) -> object:
    """
    EN: Convert nested trace values into JSON-safe primitives with shallow redaction.
    CN: 将嵌套 trace 值转换成 JSON 安全的基础类型，并做浅层脱敏。
    """
    if _is_sensitive_field(field_name):
        return "[redacted]"
    if value is None or isinstance(value, (bool, int, float, str)):
        if isinstance(value, str) and len(value) > 2000:
            return value[:2000] + "..."
        return value
    if depth >= 3:
        return str(value)
    if isinstance(value, dict):
        return {str(key): _sanitize_trace_value(str(key), item, depth=depth + 1) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize_trace_value(field_name, item, depth=depth + 1) for item in value]
    return str(value)


def _is_sensitive_field(field_name: str) -> bool:
    """
    EN: Detect trace fields that should be redacted before logging.
    CN: 检测在日志前需要脱敏的 trace 字段。
    """
    lowered = field_name.lower()
    return any(part in lowered for part in _SENSITIVE_KEY_PARTS)
