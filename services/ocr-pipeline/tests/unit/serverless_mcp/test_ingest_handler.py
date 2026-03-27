"""
EN: Tests for the ingest Lambda handler covering SQS batch failure reporting and event validation.
CN: 同上。
"""

from serverless_mcp.entrypoints import ingest as ingest_handler
import json


class _FakeStarter:
    # EN: Captures IngestWorkflowStarter handle_batch calls.
    # CN: 鎹曡幏 IngestWorkflowStarter 鐨?handle_batch 璋冪敤銆?
    def __init__(self) -> None:
        self.calls = []

    def handle_batch(self, event):
        self.calls.append(event)
        record = event["Records"][0]
        if record.get("messageId") == "msg-2":
            raise RuntimeError("boom")
        return {
            "started_count": 1,
            "skipped_count": 0,
            "failed_count": 0,
        }


def test_ingest_handler_returns_partial_batch_failures_for_sqs(monkeypatch) -> None:
    """
    EN: Ingest handler returns partial batch failures for sqs.
    CN: 楠岃瘉 ingest handler 瀵?SQS 璁板綍澶辫触杩斿洖 batchItemFailures銆?
    """
    starter = _FakeStarter()
    monkeypatch.setattr(ingest_handler, "build_ingest_workflow_starter", lambda _context=None: starter)

    result = ingest_handler.lambda_handler(
        {
            "Records": [
                {"eventSource": "aws:sqs", "messageId": "msg-1", "body": "{}"},
                {"eventSource": "aws:sqs", "messageId": "msg-2", "body": "{}"},
            ]
        },
        None,
    )

    assert result["started_count"] == 1
    assert result["failed_count"] == 1
    assert result["batchItemFailures"] == [{"itemIdentifier": "msg-2"}]
    assert len(starter.calls) == 2


def test_ingest_handler_passes_lambda_context_to_starter(monkeypatch) -> None:
    """
    EN: Ingest handler passes lambda context to starter.
    CN: 楠岃瘉 ingest handler 灏?Lambda context 浼犻€掔粰 starter銆?
    """
    captured_context = []
    starter = _FakeStarter()

    def fake_build_starter(context=None):
        captured_context.append(context)
        return starter

    monkeypatch.setattr(ingest_handler, "build_ingest_workflow_starter", fake_build_starter)

    context = type("Context", (), {"invoked_function_arn": "arn:aws:lambda:us-east-1:123456789012:function:test"})()
    ingest_handler.lambda_handler({"Records": [{"eventSource": "aws:sqs", "messageId": "msg-1", "body": "{}"}]}, context)

    assert captured_context == [context]


def test_ingest_handler_rejects_empty_event() -> None:
    """
    EN: Ingest handler rejects empty event.
    CN: 楠岃瘉 ingest handler 瀵圭┖浜嬩欢杩斿洖 400銆?
    """
    result = ingest_handler.lambda_handler({}, None)

    assert result["statusCode"] == 400
    assert json.loads(result["body"])["message"] == "Records are required for ingest worker"


def test_ingest_handler_rejects_non_dict_event() -> None:
    """
    EN: Ingest handler rejects non dict event.
    CN: 楠岃瘉 ingest handler 瀵归潪瀛楀吀浜嬩欢杩斿洖 400銆?
    """
    result = ingest_handler.lambda_handler(["unexpected"], None)

    assert result["statusCode"] == 400
    assert json.loads(result["body"])["message"] == "Records are required for ingest worker"


