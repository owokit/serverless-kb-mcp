"""
EN: Ingest Lambda entrypoint tests that protect S3/SQS dispatch and partial batch semantics.
CN: 保护 S3/SQS 分流与 partial batch 语义的 ingest Lambda 入口测试。
"""
from __future__ import annotations

import json
from typing import Any

import pytest

from fixtures.events import S3_CREATE_EVENT, SQS_BATCH_EVENT, deep_copy_event
from fixtures.lambda_context import make_lambda_context
from serverless_mcp.entrypoints import ingest as ingest_entrypoint


class _FakeStarter:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def handle_batch(self, event: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(event)
        record = event["Records"][0]
        message_id = record.get("messageId")
        if message_id == "msg-2":
            raise ValueError("bad record payload")
        if message_id == "msg-1" or record.get("eventName") == "ObjectCreated:Put":
            return {
                "statusCode": 200,
                "started_count": 1,
                "skipped_count": 0,
                "failed_count": 0,
                "failed": [],
            }
        return {
            "statusCode": 200,
            "started_count": 0,
            "skipped_count": 0,
            "failed_count": 0,
            "failed": [],
        }


@pytest.fixture(autouse=True)
def _stub_tracing(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict[str, Any]]]:
    traces: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(ingest_entrypoint, "emit_trace", lambda name, **payload: traces.append((name, payload)))
    return traces


def test_lambda_handler_rejects_empty_or_non_dict_event(monkeypatch: pytest.MonkeyPatch) -> None:
    called = []
    monkeypatch.setattr(ingest_entrypoint, "build_ingest_workflow_starter", lambda _context=None: called.append("starter") or _FakeStarter())

    empty = ingest_entrypoint.lambda_handler({}, make_lambda_context())
    not_dict = ingest_entrypoint.lambda_handler(["unexpected"], make_lambda_context())

    assert empty["statusCode"] == 400
    assert json.loads(empty["body"])["message"] == "Records are required for ingest worker"
    assert not_dict["statusCode"] == 400
    assert json.loads(not_dict["body"])["message"] == "Records are required for ingest worker"
    assert called == []


def test_lambda_handler_passes_s3_batch_through_to_starter(monkeypatch: pytest.MonkeyPatch, _stub_tracing) -> None:
    starter = _FakeStarter()
    monkeypatch.setattr(ingest_entrypoint, "build_ingest_workflow_starter", lambda _context=None: starter)

    result = ingest_entrypoint.lambda_handler(deep_copy_event(S3_CREATE_EVENT), make_lambda_context(aws_request_id="req-s3"))

    assert result["started_count"] == 1
    assert result["failed_count"] == 0
    assert result["batchItemFailures"] == []
    assert starter.calls == [S3_CREATE_EVENT]
    assert any(name == "handler.start" for name, _ in _stub_tracing)
    assert any(name == "handler.success" for name, _ in _stub_tracing)


def test_lambda_handler_processes_sqs_records_independently_and_reports_batch_failures(monkeypatch: pytest.MonkeyPatch, _stub_tracing) -> None:
    starter = _FakeStarter()
    monkeypatch.setattr(ingest_entrypoint, "build_ingest_workflow_starter", lambda _context=None: starter)

    result = ingest_entrypoint.lambda_handler(deep_copy_event(SQS_BATCH_EVENT), make_lambda_context(aws_request_id="req-sqs"))

    assert result["started_count"] == 1
    assert result["failed_count"] == 1
    assert result["batchItemFailures"] == [{"itemIdentifier": "msg-2"}]
    assert result["failed_records"][0]["error_type"] == "ValueError"
    assert result["failed_records"][0]["reason"] == "validation_error"
    assert len(starter.calls) == 2
    assert starter.calls[0]["Records"][0]["messageId"] == "msg-1"
    assert starter.calls[1]["Records"][0]["messageId"] == "msg-2"


def test_lambda_handler_uses_request_context_fields_without_affecting_dispatch(monkeypatch: pytest.MonkeyPatch, _stub_tracing) -> None:
    starter = _FakeStarter()
    captured: list[Any] = []

    def fake_build_starter(context=None):
        captured.append(context)
        return starter

    monkeypatch.setattr(ingest_entrypoint, "build_ingest_workflow_starter", fake_build_starter)

    context = make_lambda_context(aws_request_id="req-context", remaining_ms=1234)
    result = ingest_entrypoint.lambda_handler(deep_copy_event(S3_CREATE_EVENT), context)

    assert result["statusCode"] == 200
    assert captured == [context]
    assert any(name == "handler.start" and payload["request_id"] == "req-context" for name, payload in _stub_tracing)
    assert any(name == "handler.start" and payload["remaining_ms"] == 1234 for name, payload in _stub_tracing)
    assert any(name == "handler.success" and payload["raw_record_count"] == 1 for name, payload in _stub_tracing)
