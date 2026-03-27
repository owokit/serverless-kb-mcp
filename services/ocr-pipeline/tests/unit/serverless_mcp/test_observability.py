"""
EN: Tests for structured trace emission and redaction.
CN: 结构化 trace 输出与脱敏测试。
"""

from __future__ import annotations

import json

from serverless_mcp.runtime.observability import emit_metric, emit_trace


def test_emit_trace_redacts_secret_like_fields(capsys) -> None:
    """
    EN: Verify that secret-like trace fields are redacted before logging.
    CN: 验证类似密钥的 trace 字段在写日志前会被脱敏。
    """
    emit_trace(
        "handler.start",
        api_key="secret-value",
        nested={"token": "also-secret", "count": 1},
        message="visible",
    )

    payload = json.loads(capsys.readouterr().out.strip())

    assert payload["stage"] == "handler.start"
    assert payload["api_key"] == "[redacted]"
    assert payload["nested"]["token"] == "[redacted]"
    assert payload["nested"]["count"] == 1
    assert payload["message"] == "visible"


def test_emit_metric_redacts_secret_like_fields(capsys, monkeypatch) -> None:
    """
    EN: Verify that metric dimensions are sanitized and namespaced before logging.
    CN: 验证 metric 维度会脱敏并在日志中带上命名空间。
    """
    monkeypatch.setenv("METRICS_NAMESPACE", "TestNamespace")
    emit_metric(
        "extract.handler.failure",
        2,
        unit="Count",
        action="mark_failed",
        api_key="secret-value",
        nested={"token": "also-secret", "count": 1},
    )

    payload = json.loads(capsys.readouterr().out.strip())

    assert payload["stage"] == "metric"
    assert payload["namespace"] == "TestNamespace"
    assert payload["metric_name"] == "extract.handler.failure"
    assert payload["metric_value"] == 2
    assert payload["metric_unit"] == "Count"
    assert payload["dimensions"]["api_key"] == "[redacted]"
    assert payload["dimensions"]["nested"]["token"] == "[redacted]"
    assert payload["dimensions"]["nested"]["count"] == 1
    assert payload["dimensions"]["action"] == "mark_failed"
