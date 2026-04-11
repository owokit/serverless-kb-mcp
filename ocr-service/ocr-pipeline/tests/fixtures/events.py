"""
EN: Canonical event payload fixtures for Lambda and API Gateway boundary tests.
CN: Lambda 与 API Gateway 边界测试的标准事件负载 fixtures。
"""
from __future__ import annotations

from copy import deepcopy


S3_CREATE_EVENT = {
    "Records": [
        {
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "eventName": "ObjectCreated:Put",
            "responseElements": {"x-amz-request-id": "req-s3-create"},
            "s3": {
                "bucket": {"name": "source-bucket"},
                "object": {
                    "key": "docs%2Fguide.pdf",
                    "versionId": "v1",
                    "sequencer": "001",
                },
            },
        }
    ]
}

S3_DELETE_EVENT = {
    "Records": [
        {
            "eventVersion": "2.3",
            "eventSource": "aws:s3",
            "eventName": "ObjectRemoved:DeleteMarkerCreated",
            "responseElements": {"x-amz-request-id": "req-s3-delete"},
            "s3": {
                "bucket": {"name": "source-bucket"},
                "object": {
                    "key": "docs%2Fguide.pdf",
                    "versionId": "delete-v1",
                    "sequencer": "002",
                },
            },
        }
    ]
}

SQS_BATCH_EVENT = {
    "Records": [
        {"eventSource": "aws:sqs", "messageId": "msg-1", "body": "{}"},
        {"eventSource": "aws:sqs", "messageId": "msg-2", "body": "{}"},
    ]
}

APIGW_REST_GET_ROOT = {"httpMethod": "GET", "path": "/"}
APIGW_REST_GET_MCP = {"httpMethod": "GET", "path": "/mcp"}
APIGW_HTTP_GET_MCP = {"requestContext": {"http": {"method": "GET"}}, "rawPath": "/mcp/"}
APIGW_POST = {"httpMethod": "POST", "path": "/mcp"}


def deep_copy_event(payload: dict) -> dict:
    return deepcopy(payload)
