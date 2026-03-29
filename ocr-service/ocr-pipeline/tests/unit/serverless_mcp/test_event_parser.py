"""
EN: Tests for S3 event parsing including direct events, SQS-wrapped events, and delete markers.
CN: 同上。
"""

import json

from serverless_mcp.core.parsers import parse_event


def test_parse_event_supports_direct_s3_event() -> None:
    """
    EN: Verify that a direct S3 ObjectCreated:Put event is parsed into a job with the correct key.
    CN: 同上。
    """
    batch = parse_event(
        {
            "Records": [
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "eventName": "ObjectCreated:Put",
                    "tenant_id": "tenant-a",
                    "language": "zh",
                    "s3": {
                        "bucket": {"name": "bucket-a"},
                        "object": {
                            "key": "docs%2Fguide.md",
                            "versionId": "v123",
                            "sequencer": "001",
                            "eTag": "etag-1",
                        },
                    },
                }
            ]
        }
    )

    assert batch.raw_record_count == 1
    assert len(batch.jobs) == 1
    assert batch.jobs[0].source.key == "docs/guide.md"


def test_parse_event_supports_sqs_wrapped_s3_event() -> None:
    """
    EN: Verify that an S3 event wrapped inside an SQS body is unwrapped and parsed correctly.
    CN: 同上。
    """
    batch = parse_event(
        {
            "Records": [
                {
                    "eventSource": "aws:sqs",
                    "body": json.dumps(
                        {
                            "Records": [
                                {
                                    "eventVersion": "2.1",
                                    "eventSource": "aws:s3",
                                    "eventName": "ObjectCreated:Put",
                                    "s3": {
                                        "bucket": {"name": "bucket-a"},
                                        "object": {
                                            "key": "docs%2Fguide.md",
                                            "versionId": "v123",
                                        },
                                    },
                                }
                            ]
                        }
                    ),
                }
            ]
        }
    )

    assert batch.raw_record_count == 1
    assert len(batch.jobs) == 1
    assert batch.jobs[0].source.document_uri == "s3://bucket-a/docs/guide.md?versionId=v123"


def test_parse_event_supports_delete_marker_event() -> None:
    """
    EN: Verify that an ObjectRemoved:DeleteMarkerCreated event produces a DELETE operation job.
    CN: 楠岃瘉 ObjectRemoved:DeleteMarkerCreated 浜嬩欢鐢熸垚 DELETE 鎿嶄綔 job銆?
    """
    batch = parse_event(
        {
            "Records": [
                {
                    "eventVersion": "2.3",
                    "eventSource": "aws:s3",
                    "eventName": "ObjectRemoved:DeleteMarkerCreated",
                    "s3": {
                        "bucket": {"name": "bucket-a"},
                        "object": {
                            "key": "docs%2Fguide.md",
                            "versionId": "delete-v1",
                            "sequencer": "00A",
                        },
                    },
                }
            ]
        }
    )

    assert len(batch.jobs) == 1
    assert batch.jobs[0].operation == "DELETE"
    assert batch.jobs[0].source.tenant_id == "lookup"


def test_parse_event_ignores_s3_test_event() -> None:
    """
    EN: Verify that the S3 TestEvent notification is ignored and produces no jobs.
    CN: 同上。
    """
    batch = parse_event(
        {
            "Service": "Amazon S3",
            "Event": "s3:TestEvent",
            "Bucket": "bucket-a",
        }
    )

    assert batch.raw_record_count == 1
    assert batch.jobs == []


def test_parse_event_rejects_excessive_sqs_nesting() -> None:
    """
    EN: Verify that excessively nested SQS wrappers are rejected with a clear error.
    CN: 验证过深的 SQS 嵌套会以明确错误拒绝。
    """
    payload: dict[str, object] = {
        "Records": [
            {
                "eventSource": "aws:s3",
                "eventName": "ObjectCreated:Put",
                "eventVersion": "2.1",
                "s3": {
                    "bucket": {"name": "bucket-a"},
                    "object": {
                        "key": "docs%2Fguide.md",
                        "versionId": "v123",
                    },
                },
            }
        ]
    }
    for _ in range(6):
        payload = {
            "Records": [
                {
                    "eventSource": "aws:sqs",
                    "body": json.dumps(payload),
                }
            ]
        }

    try:
        parse_event(payload)
    except ValueError as exc:
        assert "Nested SQS event depth exceeds 5" in str(exc)
    else:
        raise AssertionError("Expected parse_event to reject excessive SQS nesting")
