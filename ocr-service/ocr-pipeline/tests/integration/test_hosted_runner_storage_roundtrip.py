"""
EN: Hosted-runner integration tests for the storage layer roundtrip (S3, DynamoDB, manifest).
CN: 面向 hosted runner 的存储层往返集成测试（S3、DynamoDB、manifest）。
"""
from __future__ import annotations

import os

import pytest
from botocore.exceptions import ClientError

from serverless_mcp.runtime.aws_clients import build_aws_client
from serverless_mcp.extract.s3_source import S3DocumentSource
from serverless_mcp.domain.models import ChunkManifest, ExtractedChunk, S3ObjectRef
from serverless_mcp.storage.manifest.repository import ManifestRepository
from serverless_mcp.storage.state.object_state_repository import ObjectStateRepository


pytestmark = pytest.mark.integration


def test_hosted_runner_storage_roundtrip() -> None:
    required_env_names = ("TEST_S3_BUCKET", "TEST_OBJECT_STATE_TABLE", "TEST_MANIFEST_INDEX_TABLE")
    missing_env_names = [name for name in required_env_names if not os.environ.get(name)]
    if missing_env_names:
        pytest.skip("Missing local integration env vars: " + ", ".join(missing_env_names))

    region = os.environ.get("TEST_AWS_REGION", "us-east-1")
    s3_bucket = os.environ["TEST_S3_BUCKET"]
    object_state_table = os.environ["TEST_OBJECT_STATE_TABLE"]
    manifest_index_table = os.environ["TEST_MANIFEST_INDEX_TABLE"]

    s3_client = build_aws_client("s3", region_name=region)
    dynamodb_client = build_aws_client("dynamodb", region_name=region)

    _ensure_bucket(s3_client, s3_bucket)
    _ensure_tables(dynamodb_client, object_state_table, manifest_index_table)

    source_put = s3_client.put_object(
        Bucket=s3_bucket,
        Key="docs/sample.pdf",
        Body=b"%PDF-1.7 sample",
        ContentType="application/pdf",
    )
    version_id = str(source_put.get("VersionId") or "local-version")

    source = S3ObjectRef(
        tenant_id="tenant-a",
        bucket=s3_bucket,
        key="docs/sample.pdf",
        version_id=version_id,
        language="zh",
    )

    document_source = S3DocumentSource(s3_client=s3_client)
    payload = document_source.fetch(source)
    assert payload.body == b"%PDF-1.7 sample"

    object_state_repo = ObjectStateRepository(
        table_name=object_state_table,
        dynamodb_client=dynamodb_client,
    )
    manifest_repo = ManifestRepository(
        manifest_bucket=s3_bucket,
        manifest_prefix="manifests",
        s3_client=s3_client,
        dynamodb_client=dynamodb_client,
        manifest_index_table=manifest_index_table,
    )

    queued = object_state_repo.queue_for_ingest(source)
    assert queued.latest_version_id == source.version_id

    manifest = ChunkManifest(
        source=source,
        doc_type="pdf",
        chunks=[
            ExtractedChunk(
                chunk_id="chunk#000001",
                chunk_type="page_text_chunk",
                text="sample text",
                doc_type="pdf",
                token_estimate=2,
                page_no=1,
                page_span=(1, 1),
                metadata={"source_format": "pdf"},
            )
        ],
        assets=[],
        metadata={
            "source_format": "pdf",
            "page_count": 1,
            "page_image_asset_count": 0,
            "visual_page_numbers": [],
        },
    )

    persisted = manifest_repo.persist_manifest(manifest, previous_version_id=queued.previous_version_id)
    loaded = manifest_repo.load_manifest(persisted.manifest_s3_uri)
    assert loaded.source.document_uri == source.document_uri
    assert loaded.chunks[0].text == "sample text"

    done = object_state_repo.mark_extract_done(source, persisted.manifest_s3_uri)
    assert done.latest_manifest_s3_uri == persisted.manifest_s3_uri
    assert object_state_repo.get_state(object_pk=source.object_pk).latest_manifest_s3_uri == persisted.manifest_s3_uri


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
