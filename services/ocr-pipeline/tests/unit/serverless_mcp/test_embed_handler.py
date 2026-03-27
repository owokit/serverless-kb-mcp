"""
EN: Tests for the embed Lambda handler including SQS partial batch failure and profile validation.
CN: 测试 embed Lambda handler，包含 SQS 部分批量失败和 profile 校验。
"""

import json

from serverless_mcp.entrypoints import embed as embed_handler


def test_embed_handler_returns_partial_batch_failures_for_sqs(monkeypatch) -> None:
    """
    EN: Verify that the embed handler returns batchItemFailures for SQS records that raise during processing.
    CN: 验证当 SQS 记录在处理时抛错，embed handler 会返回 batchItemFailures。
    """
    calls = []

    def _fake_process_embed_event(event):
        calls.append(event)
        record = event["Records"][0]
        if record.get("messageId") == "msg-2":
            raise RuntimeError("boom")
        return {
            "processed": [{"document_uri": "s3://bucket-a/docs/guide.pdf?versionId=v1"}],
            "failed": [],
        }

    monkeypatch.setattr(embed_handler, "_process_embed_event", _fake_process_embed_event)

    result = embed_handler.lambda_handler(
        {
            "Records": [
                {"eventSource": "aws:sqs", "messageId": "msg-1", "body": "{}"},
                {"eventSource": "aws:sqs", "messageId": "msg-2", "body": "{}"},
            ]
        },
        None,
    )

    assert result["processed_count"] == 1
    assert result["failed_count"] == 1
    assert result["batchItemFailures"] == [{"itemIdentifier": "msg-2"}]
    assert result["failed_records"][0]["error_type"] == "RuntimeError"
    assert result["failed_records"][0]["reason"] == "unexpected_error"
    assert len(calls) == 2

def test_embed_handler_rejects_empty_event() -> None:
    """
    EN: Verify the handler returns 400 when the event dict is empty.
    CN: 验证当事件字典为空时 handler 返回 400。
    """
    result = embed_handler.lambda_handler({}, None)

    assert result["statusCode"] == 400
    assert json.loads(result["body"])["message"] == "Records are required for embed worker"


def test_process_embed_event_surfaces_structured_inactive_profile_failure(monkeypatch) -> None:
    """
    EN: Verify that inactive profile failures are reported with a structured disposition.
    CN: 验证 inactive profile 失败会带有结构化 disposition。
    """

    class _FailingWorker:
        def process(self, job):
            raise embed_handler.UnknownEmbeddingProfileError("profile disabled")

    monkeypatch.setattr(embed_handler, "_get_worker", lambda: _FailingWorker())

    result = embed_handler._process_embed_event(
        {
            "Records": [
                {
                    "body": json.dumps(
                        {
                            "source": {
                                "tenant_id": "tenant-a",
                                "bucket": "bucket-a",
                                "key": "docs/guide.pdf",
                                "version_id": "v1",
                            },
                            "profile_id": "gemini-default",
                            "trace_id": "trace-1",
                            "manifest_s3_uri": "s3://manifest-bucket/manifests/example.json",
                            "requests": [],
                        }
                    )
                }
            ]
        }
    )

    assert result["failed_count"] == 1
    assert result["failed"][0]["error_type"] == "UnknownEmbeddingProfileError"
    assert result["failed"][0]["disposition"] == "skipped_inactive_profile"
