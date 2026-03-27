"""EN: Hosted-runner integration coverage for local S3, DynamoDB, and SQS emulators.
CN: 面向 hosted runner 的本地 S3、DynamoDB 和 SQS 仿真覆盖。
"""

from __future__ import annotations

import os

import pytest
from botocore.exceptions import ClientError

from serverless_mcp.runtime.aws_clients import build_aws_client


pytestmark = pytest.mark.integration


def test_hosted_runner_aws_roundtrip_covers_s3_dynamodb_and_sqs() -> None:
    required_env_names = ("TEST_S3_BUCKET", "TEST_OBJECT_STATE_TABLE", "TEST_MANIFEST_INDEX_TABLE")
    missing_env_names = [name for name in required_env_names if not os.environ.get(name)]
    if missing_env_names:
        pytest.skip("Missing local integration env vars: " + ", ".join(missing_env_names))

    region = os.environ.get("TEST_AWS_REGION", "us-east-1")
    s3_bucket = os.environ["TEST_S3_BUCKET"]
    object_state_table = os.environ["TEST_OBJECT_STATE_TABLE"]
    manifest_index_table = os.environ["TEST_MANIFEST_INDEX_TABLE"]
    queue_name = os.environ.get("TEST_SQS_QUEUE_NAME", "local-roundtrip-queue")

    s3_client = build_aws_client("s3", region_name=region)
    dynamodb_client = build_aws_client("dynamodb", region_name=region)
    sqs_client = build_aws_client("sqs", region_name=region)

    _ensure_bucket(s3_client, s3_bucket)
    _ensure_tables(dynamodb_client, object_state_table, manifest_index_table)

    bucket_key = "checks/local-roundtrip.txt"
    s3_client.put_object(Bucket=s3_bucket, Key=bucket_key, Body=b"local roundtrip", ContentType="text/plain")
    roundtrip_object = s3_client.get_object(Bucket=s3_bucket, Key=bucket_key)
    assert roundtrip_object["Body"].read() == b"local roundtrip"

    item = {
        "pk": {"S": "local#item"},
        "latest_version_id": {"S": "v1"},
    }
    dynamodb_client.put_item(TableName=object_state_table, Item=item)
    stored = dynamodb_client.get_item(TableName=object_state_table, Key={"pk": {"S": "local#item"}})
    assert stored["Item"]["latest_version_id"]["S"] == "v1"

    queue_url = sqs_client.create_queue(QueueName=queue_name)["QueueUrl"]
    sqs_client.send_message(QueueUrl=queue_url, MessageBody="local message")
    message_response = sqs_client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
    messages = message_response.get("Messages") or []
    assert len(messages) == 1
    assert messages[0]["Body"] == "local message"
    sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=messages[0]["ReceiptHandle"])


def _ensure_bucket(s3_client: object, bucket_name: str) -> None:
    """EN: Create the test bucket and enable versioning if not already present.
    CN: 创建测试 bucket，如不存在则启用版本控制。"""
    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code") or "")
        if code not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
            raise
    try:
        s3_client.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={"Status": "Enabled"},
        )
    except ClientError:
        pass


def _ensure_tables(dynamodb_client: object, object_state_table: str, manifest_index_table: str) -> None:
    """EN: Create DynamoDB tables for object state and manifest index if missing.
    CN: 如缺少 object_state 和 manifest_index 表则创建对应的 DynamoDB 表。"""
    existing = set(dynamodb_client.list_tables().get("TableNames") or [])
    if object_state_table not in existing:
        dynamodb_client.create_table(
            TableName=object_state_table,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "record_type", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "lookup-record-type-index",
                    "KeySchema": [{"AttributeName": "record_type", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )
    if manifest_index_table not in existing:
        dynamodb_client.create_table(
            TableName=manifest_index_table,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
