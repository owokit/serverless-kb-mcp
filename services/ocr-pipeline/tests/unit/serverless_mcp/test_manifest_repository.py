"""
EN: Tests for ManifestRepository including S3 persistence, DynamoDB indexing, rollback, and artifact deletion.
CN: 测试 ManifestRepository，包括 S3 持久化、DynamoDB 索引、回滚和产物删除。
"""

from __future__ import annotations

import io

from serverless_mcp.domain.models import ChunkManifest, ExtractedAsset, ExtractedChunk, S3ObjectRef
from serverless_mcp.storage.paths import optimize_source_file_name
from serverless_mcp.storage.manifest.repository import ManifestRepository


class _FakeS3Client:
    # EN: In-memory stand-in for S3 client with versioned storage.
    # CN: 带版本化存储的 S3 客户端内存替身。
    def __init__(self) -> None:
        self.objects: list[dict[str, object]] = []
        self.deleted: list[dict[str, object]] = []
        self.batch_deleted: list[dict[str, object]] = []
        self.storage: dict[tuple[str, str, str], bytes] = {}
        self.latest_versions: dict[tuple[str, str], str] = {}
        self._version_counter = 0

    def put_object(self, **kwargs):
        self._version_counter += 1
        version_id = f"v{self._version_counter:06d}"
        bucket = kwargs["Bucket"]
        key = kwargs["Key"]
        self.objects.append({**kwargs, "VersionId": version_id})
        self.storage[(bucket, key, version_id)] = kwargs["Body"]
        self.latest_versions[(bucket, key)] = version_id
        return {"VersionId": version_id}

    def get_object(self, **kwargs):
        bucket = kwargs["Bucket"]
        key = kwargs["Key"]
        version_id = kwargs.get("VersionId") or self.latest_versions[(bucket, key)]
        payload = self.storage[(bucket, key, version_id)]
        return {"Body": io.BytesIO(payload)}

    def delete_object(self, **kwargs):
        self.deleted.append(kwargs)
        bucket = kwargs["Bucket"]
        key = kwargs["Key"]
        version_id = kwargs.get("VersionId") or self.latest_versions.get((bucket, key))
        if version_id is None:
            return
        self.storage.pop((bucket, key, version_id), None)
        if self.latest_versions.get((bucket, key)) == version_id:
            remaining = [item_version for (item_bucket, item_key, item_version) in self.storage if item_bucket == bucket and item_key == key]
            if remaining:
                self.latest_versions[(bucket, key)] = remaining[-1]
            else:
                self.latest_versions.pop((bucket, key), None)

    def delete_objects(self, **kwargs):
        self.batch_deleted.append(kwargs)
        bucket = kwargs["Bucket"]
        for item in kwargs["Delete"]["Objects"]:
            key = item["Key"]
            version_id = item.get("VersionId") or self.latest_versions.get((bucket, key))
            if version_id is None:
                continue
            self.storage.pop((bucket, key, version_id), None)


class _FakeDynamoClient:
    # EN: In-memory stand-in for DynamoDB client.
    # CN: DynamoDB 客户端的内存替身。
    def __init__(self, *, fail_on_second_batch: bool = False):
        self.items: dict[tuple[str, str], dict[str, dict[str, str | bool]]] = {}
        self.fail_on_second_batch = fail_on_second_batch
        self.batch_calls = 0

    def query(self, **kwargs):
        pk = kwargs["ExpressionAttributeValues"][":pk"]["S"]
        matched = [item for (item_pk, _), item in self.items.items() if item_pk == pk]
        return {"Items": matched}

    def batch_write_item(self, *, RequestItems):
        self.batch_calls += 1
        if self.fail_on_second_batch and self.batch_calls == 2:
            raise RuntimeError("ddb write failed")
        for request in RequestItems["manifest-index"]:
            if "PutRequest" in request:
                item = request["PutRequest"]["Item"]
                self.items[(item["pk"]["S"], item["sk"]["S"])] = item
            else:
                key = request["DeleteRequest"]["Key"]
                self.items.pop((key["pk"]["S"], key["sk"]["S"]), None)
        return {"UnprocessedItems": {}}


def _build_repo(*, s3_client: _FakeS3Client, dynamodb_client: _FakeDynamoClient) -> ManifestRepository:
    return ManifestRepository(
        manifest_bucket="manifest-bucket",
        manifest_prefix="",
        s3_client=s3_client,
        dynamodb_client=dynamodb_client,
        manifest_index_table="manifest-index",
    )


def _source() -> S3ObjectRef:
    return S3ObjectRef(
        tenant_id="tenant-a",
        bucket="source-bucket",
        key="资料/中文 名称（最终版）.pdf",
        version_id="source-v1",
    )


def test_manifest_repository_uses_readable_key_layout_without_source_version_folder() -> None:
    """
    EN: Manifest repository uses readable key layout without source version folder.
    CN: 验证 ManifestRepository 使用不含源版本文件夹的可读 key 布局。
    """
    repo = _build_repo(s3_client=_FakeS3Client(), dynamodb_client=_FakeDynamoClient())
    source = _source()

    uri = repo.build_manifest_s3_uri(source=source, version_id=source.version_id)

    expected_root = optimize_source_file_name(source)
    assert uri == f"s3://manifest-bucket/{expected_root}/manifest.json"
    assert source.version_id not in uri


def test_manifest_repository_persists_versioned_assets_before_manifest() -> None:
    """
    EN: Manifest repository persists versioned assets before manifest.
    CN: 验证 ManifestRepository 会在 manifest 之前持久化版本化资产。
    """
    s3_client = _FakeS3Client()
    dynamodb_client = _FakeDynamoClient()
    repo = _build_repo(s3_client=s3_client, dynamodb_client=dynamodb_client)
    source = _source()

    persisted = repo.persist_manifest(
        ChunkManifest(
            source=source,
            doc_type="pdf",
            chunks=[
            ExtractedChunk(
                chunk_id="chunk#000001",
                chunk_type="page_text_chunk",
                text="hello",
                doc_type="pdf",
                token_estimate=1,
                page_no=1,
                page_span=(1, 1),
                metadata={"source_format": "pdf"},
            )
        ],
            assets=[
                ExtractedAsset(
                    asset_id="asset#000001",
                    chunk_type="document_markdown_chunk",
                    mime_type="text/plain",
                    payload=b"hello",
                    metadata={"source_format": "pdf"},
                )
            ],
            metadata={
                "source_format": "pdf",
                "page_count": 1,
                "page_image_asset_count": 0,
                "visual_page_numbers": [],
            },
        ),
    )

    assert persisted.manifest.source.version_id == source.version_id
    assert s3_client.objects
    assert dynamodb_client.items


def test_manifest_repository_batches_s3_rollbacks_for_matching_buckets() -> None:
    """
    EN: Manifest repository batches S3 delete calls when rollback objects share the same bucket.
    CN: 验证 ManifestRepository 在回滚对象属于同一 bucket 时会批量删除 S3 对象。
    """
    s3_client = _FakeS3Client()
    repo = _build_repo(s3_client=s3_client, dynamodb_client=_FakeDynamoClient())

    repo._delete_s3_objects(
        [
            ("manifest-bucket", "manifests/a.json", "v1"),
            ("manifest-bucket", "manifests/b.json", "v2"),
            ("manifest-bucket", "manifests/c.json", None),
        ]
    )

    assert len(s3_client.batch_deleted) == 1
    assert s3_client.batch_deleted[0]["Bucket"] == "manifest-bucket"
    assert len(s3_client.batch_deleted[0]["Delete"]["Objects"]) == 3
