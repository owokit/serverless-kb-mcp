"""
EN: Tests for ObjectStateRepository covering ingest queuing, state activation, delete replay, and lookup resolution.
CN: 同上。
"""

from serverless_mcp.domain.models import ObjectStateRecord, S3ObjectRef
from serverless_mcp.storage.state.object_state_repository import (
    ObjectStateLookupRecord,
    ObjectStateRepository,
    _normalize_sequencer,
)


class _CapturingDynamoDbClient:
    # EN: Records DynamoDB calls for assertion without making real requests.
    # CN: 同上。
    def __init__(self) -> None:
        self.update_calls = []
        self.put_calls = []
        self.transact_calls = []

    def update_item(self, **kwargs):
        self.update_calls.append(kwargs)
        return {"Attributes": {}}

    def put_item(self, **kwargs):
        self.put_calls.append(kwargs)
        return {}

    def get_item(self, **_kwargs):
        return {}

    def transact_write_items(self, **kwargs):
        self.transact_calls.append(kwargs)
        return {}


class _NoWriteDynamoDbClient:
    # EN: DynamoDB client that asserts no write operations are attempted.
    # CN: 同上。
    def update_item(self, **_kwargs):
        raise AssertionError("update_item should not be called for delete-marker replay")

    def put_item(self, **_kwargs):
        raise AssertionError("put_item should not be called for delete-marker replay")

    def scan(self, **_kwargs):
        raise AssertionError("scan should not be called for delete-marker replay")

    def query(self, **_kwargs):
        raise AssertionError("query should not be called for delete-marker replay")

    def transact_write_items(self, **_kwargs):
        raise AssertionError("transact_write_items should not be called for delete-marker replay")


class _LookupQueryDynamoDbClient:
    # EN: DynamoDB client that returns paginated lookup query results.
    # CN: 同上。
    def __init__(self, pages: list[dict[str, object]]) -> None:
        self.query_calls = []
        self._pages = list(pages)

    def query(self, **kwargs):
        self.query_calls.append(kwargs)
        if self._pages:
            return self._pages.pop(0)
        return {"Items": []}


class _ReplayDeleteRepository(ObjectStateRepository):
    # EN: ObjectStateRepository subclass that simulates already-deleted state.
    # CN: 妯℃嫙宸插垹闄ょ姸鎬佺殑 ObjectStateRepository 瀛愮被銆?
    def get_lookup_record(self, *, bucket: str, key: str):
        return ObjectStateLookupRecord(
            pk=f"lookup-v2#{bucket}#{key}",
            object_pk=f"tenant-a#{bucket}#{key}",
            tenant_id="tenant-a",
            bucket=bucket,
            key=key,
            latest_version_id="delete-v1",
            latest_sequencer=_normalize_sequencer("2"),
            latest_manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
            is_deleted=True,
            updated_at="2025-01-01T00:00:00+00:00",
        )

    def get_lookup_for_source(self, source: S3ObjectRef):
        return self.get_lookup_record(bucket=source.bucket, key=source.key)

    def get_state(self, *, object_pk: str):
        return ObjectStateRecord(
            pk=object_pk,
            latest_version_id="delete-v1",
            latest_sequencer=_normalize_sequencer("2"),
            extract_status="EXTRACTED",
            embed_status="INDEXED",
            latest_manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
            is_deleted=True,
            updated_at="2025-01-01T00:00:00+00:00",
        )


class _MissingStateDeleteRepository(ObjectStateRepository):
    # EN: ObjectStateRepository subclass that simulates a missing state record for delete replay.
    # CN: 模拟 delete replay 时 state 记录缺失的 ObjectStateRepository 子类。
    def get_lookup_record(self, *, bucket: str, key: str):
        return ObjectStateLookupRecord(
            pk=f"lookup-v2#{bucket}#{key}",
            object_pk=f"tenant-a#{bucket}#{key}",
            tenant_id="tenant-a",
            bucket=bucket,
            key=key,
            latest_version_id="delete-v1",
            latest_sequencer=_normalize_sequencer("2"),
            latest_manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
            is_deleted=False,
            updated_at="2025-01-01T00:00:00+00:00",
        )

    def get_state(self, *, object_pk: str):
        return None


def test_normalize_sequencer_keeps_lexicographic_order_for_variable_length_values() -> None:
    """
    EN: Normalize sequencer keeps lexicographic order for variable length values.
    CN: 楠岃瘉 _normalize_sequencer 瀵瑰彉闀垮€间繚鎸佸瓧鍏稿簭銆?
    """
    assert _normalize_sequencer("f") < _normalize_sequencer("10")
    assert _normalize_sequencer("a") == _normalize_sequencer("A")
    assert _normalize_sequencer("  ") is None


def test_queue_for_ingest_persists_normalized_sequencer() -> None:
    """
    EN: Queue for ingest persists normalized sequencer.
    CN: 同上。
    """
    dynamodb = _CapturingDynamoDbClient()
    repo = ObjectStateRepository(table_name="object-state", dynamodb_client=dynamodb)
    source = S3ObjectRef(
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="docs/guide.pdf",
        version_id="v1",
        sequencer="a",
    )

    record = repo.queue_for_ingest(source)

    normalized = _normalize_sequencer("a")
    assert record.latest_sequencer == normalized
    assert dynamodb.transact_calls[0]["TransactItems"][0]["Update"]["ExpressionAttributeValues"][":sequencer"]["S"] == normalized
    assert dynamodb.transact_calls[0]["TransactItems"][1]["Put"]["Item"]["latest_sequencer"]["S"] == normalized
    assert dynamodb.transact_calls[0]["TransactItems"][1]["Put"]["Item"]["pk"]["S"] == "lookup-v2#bucket-a#docs%2Fguide.pdf"


class _ExistingStateRepository(ObjectStateRepository):
    # EN: ObjectStateRepository subclass with pre-seeded state.
    # CN: 棰勭疆鐘舵€佺殑 ObjectStateRepository 瀛愮被銆?
    def __init__(self, *, state: ObjectStateRecord, dynamodb_client) -> None:
        super().__init__(table_name="object-state", dynamodb_client=dynamodb_client)
        self._state = state

    def get_state(self, *, object_pk: str):
        return self._state


def test_activate_ingest_state_claims_provided_payload_and_clears_manifest_pointer() -> None:
    """
    EN: Activate ingest state claims provided payload and clears manifest pointer.
    CN: 同上。
    """
    dynamodb = _CapturingDynamoDbClient()
    repo = _ExistingStateRepository(
        state=ObjectStateRecord(
            pk="tenant-a#bucket-a#docs/guide.pdf",
            latest_version_id="v0",
            latest_sequencer=_normalize_sequencer("0"),
            extract_status="EXTRACTED",
            embed_status="INDEXED",
            latest_manifest_s3_uri="s3://manifest-bucket/manifests/v0.json",
        ),
        dynamodb_client=dynamodb,
    )
    source = S3ObjectRef(
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="docs/guide.pdf",
        version_id="v1",
        sequencer="1",
    )
    preview = ObjectStateRecord(
        pk=source.object_pk,
        latest_version_id=source.version_id,
        latest_sequencer=_normalize_sequencer("1"),
        extract_status="QUEUED",
        embed_status="PENDING",
        previous_version_id="v0",
        previous_manifest_s3_uri="s3://manifest-bucket/manifests/v0.json",
    )

    record = repo.activate_ingest_state(source, preview)

    assert record.extract_status == "EXTRACTING"
    assert record.previous_version_id == "v0"
    assert record.latest_manifest_s3_uri is None
    assert "REMOVE latest_manifest_s3_uri" in dynamodb.transact_calls[0]["TransactItems"][0]["Update"]["UpdateExpression"]
    assert dynamodb.transact_calls[0]["TransactItems"][1]["Put"]["Item"]["latest_version_id"]["S"] == "v1"
    assert "latest_manifest_s3_uri" not in dynamodb.transact_calls[0]["TransactItems"][1]["Put"]["Item"]


def test_activate_ingest_state_reuses_existing_extracting_state_without_new_write() -> None:
    """
    EN: Activate ingest state reuses existing extracting state without new write.
    CN: 楠岃瘉 activate_ingest_state 澶嶇敤宸叉湁 EXTRACTING 鐘舵€佽€屼笉鍐欏叆銆?
    """
    existing = ObjectStateRecord(
        pk="tenant-a#bucket-a#docs/guide.pdf",
        latest_version_id="v1",
        latest_sequencer=_normalize_sequencer("1"),
        extract_status="EXTRACTING",
        embed_status="PENDING",
        previous_version_id="v0",
        previous_manifest_s3_uri="s3://manifest-bucket/manifests/v0.json",
    )
    repo = _ExistingStateRepository(state=existing, dynamodb_client=_NoWriteDynamoDbClient())
    source = S3ObjectRef(
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="docs/guide.pdf",
        version_id="v1",
        sequencer="1",
    )

    record = repo.activate_ingest_state(
        source,
        ObjectStateRecord(
            pk=source.object_pk,
            latest_version_id=source.version_id,
            latest_sequencer=_normalize_sequencer("1"),
            extract_status="QUEUED",
            embed_status="PENDING",
            previous_version_id="v0",
            previous_manifest_s3_uri="s3://manifest-bucket/manifests/v0.json",
        ),
    )

    assert record is existing


def test_mark_deleted_replays_same_delete_marker_without_new_write() -> None:
    """
    EN: Mark deleted replays same delete marker without new write.
    CN: 楠岃瘉 mark_deleted 瀵圭浉鍚屽垹闄ゆ爣璁板箓绛夎繑鍥炶€屼笉鍐欏叆銆?
    """
    repo = _ReplayDeleteRepository(table_name="object-state", dynamodb_client=_NoWriteDynamoDbClient())

    record = repo.mark_deleted(
        bucket="bucket-a",
        key="docs/guide.pdf",
        version_id="delete-v1",
        sequencer="2",
    )

    assert record.is_deleted is True
    assert record.latest_version_id == "delete-v1"
    assert record.latest_manifest_s3_uri == "s3://manifest-bucket/manifests/example.json"


def test_mark_deleted_creates_tombstone_when_state_is_missing() -> None:
    """
    EN: Delete markers should still persist as tombstones when state is missing.
    CN: 即使 state 缺失，delete marker 也应持久化为 tombstone。
    """
    dynamodb = _CapturingDynamoDbClient()
    repo = _MissingStateDeleteRepository(table_name="object-state", dynamodb_client=dynamodb)

    record = repo.mark_deleted(
        bucket="bucket-a",
        key="docs/guide.pdf",
        version_id="delete-v1",
        sequencer="2",
    )

    assert record.is_deleted is True
    assert record.pk == "tenant-a#bucket-a#docs/guide.pdf"
    assert record.latest_sequencer == _normalize_sequencer("2")
    assert dynamodb.transact_calls[0]["TransactItems"][0]["Put"]["Item"]["is_deleted"]["BOOL"] is True
    assert dynamodb.transact_calls[0]["TransactItems"][1]["Put"]["Item"]["is_deleted"]["BOOL"] is True


class _DuplicateLookupQueryDynamoDbClient:
    # EN: DynamoDB client returning duplicate lookup records for dedup testing.
    # CN: 同上。
    def __init__(self) -> None:
        self.query_calls = []

    def query(self, **kwargs):
        self.query_calls.append(kwargs)
        return {
            "Items": [
                {
                    "pk": {"S": "lookup#bucket-a#docs/guide.pdf"},
                    "record_type": {"S": "LOOKUP"},
                    "object_pk": {"S": "tenant-a#bucket-a#docs/guide.pdf"},
                    "tenant_id": {"S": "tenant-a"},
                    "bucket": {"S": "bucket-a"},
                    "key": {"S": "docs/guide.pdf"},
                    "latest_version_id": {"S": "v1"},
                    "latest_sequencer": {"S": "00000000000000000000000000000001"},
                    "latest_manifest_s3_uri": {"S": "s3://manifest-bucket/manifests/v1.json"},
                    "is_deleted": {"BOOL": False},
                    "updated_at": {"S": "2026-03-18T00:00:00+00:00"},
                },
                {
                    "pk": {"S": "lookup-v2#bucket-a#docs%2Fguide.pdf"},
                    "record_type": {"S": "LOOKUP"},
                    "object_pk": {"S": "tenant-a#bucket-a#docs/guide.pdf"},
                    "tenant_id": {"S": "tenant-a"},
                    "bucket": {"S": "bucket-a"},
                    "key": {"S": "docs/guide.pdf"},
                    "latest_version_id": {"S": "v2"},
                    "latest_sequencer": {"S": "00000000000000000000000000000002"},
                    "latest_manifest_s3_uri": {"S": "s3://manifest-bucket/manifests/v2.json"},
                    "is_deleted": {"BOOL": False},
                    "updated_at": {"S": "2026-03-19T00:00:00+00:00"},
                },
            ],
            "LastEvaluatedKey": None,
        }


def test_get_lookup_for_source_uses_explicit_lookup_path() -> None:
    """
    EN: Get lookup for source uses explicit lookup path.
    CN: 同上。
    """
    class _LookupClient:
        # EN: DynamoDB client for explicit lookup key resolution.
        # CN: 同上。
        def __init__(self) -> None:
            self.calls = []

        def get_item(self, **kwargs):
            self.calls.append(kwargs)
            return {
                "Item": {
                    "pk": {"S": "lookup-v2#bucket-a#docs/guide.pdf"},
                    "record_type": {"S": "LOOKUP"},
                    "object_pk": {"S": "tenant-a#bucket-a#docs/guide.pdf"},
                    "tenant_id": {"S": "tenant-a"},
                    "bucket": {"S": "bucket-a"},
                    "key": {"S": "docs/guide.pdf"},
                    "latest_version_id": {"S": "v1"},
                    "latest_sequencer": {"S": "00000000000000000000000000000001"},
                    "latest_manifest_s3_uri": {"S": "s3://manifest-bucket/manifests/v1.json"},
                    "is_deleted": {"BOOL": False},
                    "updated_at": {"S": "2026-03-18T00:00:00+00:00"},
                }
            }

    ddb = _LookupClient()
    repo = ObjectStateRepository(table_name="object-state", dynamodb_client=ddb)
    source = S3ObjectRef(
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="docs/guide.pdf",
        version_id="v1",
    )

    lookup = repo.get_lookup_for_source(source)

    assert lookup is not None
    assert lookup.object_pk == "tenant-a#bucket-a#docs/guide.pdf"
    assert ddb.calls[0]["Key"]["pk"]["S"] == "lookup-v2#bucket-a#docs%2Fguide.pdf"


def test_get_lookup_record_falls_back_to_legacy_lookup_key() -> None:
    """
    EN: Get lookup record falls back to legacy lookup key.
    CN: 楠岃瘉 get_lookup_record 鍥為€€鍒版棫鐗?lookup key銆?
    """
    class _LegacyLookupClient:
        # EN: DynamoDB client supporting legacy lookup key fallback.
        # CN: 同上。
        def __init__(self) -> None:
            self.calls = []

        def get_item(self, **kwargs):
            self.calls.append(kwargs)
            if kwargs["Key"]["pk"]["S"] != "lookup#bucket-a#docs%2Fguide.pdf":
                return {}
            return {
                "Item": {
                    "pk": {"S": "lookup#bucket-a#docs%2Fguide.pdf"},
                    "record_type": {"S": "LOOKUP"},
                    "object_pk": {"S": "tenant-a#bucket-a#docs/guide.pdf"},
                    "tenant_id": {"S": "tenant-a"},
                    "bucket": {"S": "bucket-a"},
                    "key": {"S": "docs/guide.pdf"},
                    "latest_version_id": {"S": "v1"},
                    "latest_sequencer": {"S": "00000000000000000000000000000001"},
                    "latest_manifest_s3_uri": {"S": "s3://manifest-bucket/manifests/v1.json"},
                    "is_deleted": {"BOOL": False},
                    "updated_at": {"S": "2026-03-18T00:00:00+00:00"},
                }
            }

    ddb = _LegacyLookupClient()
    repo = ObjectStateRepository(table_name="object-state", dynamodb_client=ddb)
    source = S3ObjectRef(
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="docs/guide.pdf",
        version_id="v1",
    )

    lookup = repo.get_lookup_record(bucket=source.bucket, key=source.key)

    assert lookup is not None
    assert lookup.pk == "lookup#bucket-a#docs%2Fguide.pdf"
    assert [call["Key"]["pk"]["S"] for call in ddb.calls] == [
        "lookup-v2#bucket-a#docs%2Fguide.pdf",
        "lookup#bucket-a#docs%2Fguide.pdf",
    ]


def test_iter_lookup_records_queries_lookup_index_and_paginates() -> None:
    """
    EN: Iter lookup records queries the lookup index and paginates results.
    CN: 同上。
    """
    dynamodb = _LookupQueryDynamoDbClient(
        pages=[
            {
                "Items": [
                    {
                        "pk": {"S": "lookup#bucket-a#docs/guide.pdf"},
                        "record_type": {"S": "LOOKUP"},
                        "object_pk": {"S": "tenant-a#bucket-a#docs/guide.pdf"},
                        "tenant_id": {"S": "tenant-a"},
                        "bucket": {"S": "bucket-a"},
                        "key": {"S": "docs/guide.pdf"},
                        "latest_version_id": {"S": "v1"},
                        "latest_sequencer": {"S": "00000000000000000000000000000001"},
                        "latest_manifest_s3_uri": {"S": "s3://manifest-bucket/manifests/v1.json"},
                        "is_deleted": {"BOOL": False},
                        "updated_at": {"S": "2026-03-18T00:00:00+00:00"},
                    }
                ],
                "LastEvaluatedKey": {"pk": {"S": "lookup#bucket-a#docs/guide.pdf"}},
            },
            {
                "Items": [
                    {
                        "pk": {"S": "lookup-v2#bucket-a#docs%2Fguide.pdf"},
                        "record_type": {"S": "LOOKUP"},
                        "object_pk": {"S": "tenant-a#bucket-a#docs/guide.pdf"},
                        "tenant_id": {"S": "tenant-a"},
                        "bucket": {"S": "bucket-a"},
                        "key": {"S": "docs/guide.pdf"},
                        "latest_version_id": {"S": "v2"},
                        "latest_sequencer": {"S": "00000000000000000000000000000002"},
                        "latest_manifest_s3_uri": {"S": "s3://manifest-bucket/manifests/v2.json"},
                        "is_deleted": {"BOOL": False},
                        "updated_at": {"S": "2026-03-19T00:00:00+00:00"},
                    }
                ]
            },
        ]
    )
    repo = ObjectStateRepository(table_name="object-state", dynamodb_client=dynamodb)

    records = list(repo.iter_lookup_records())

    assert len(records) == 1
    assert records[0].object_pk == "tenant-a#bucket-a#docs/guide.pdf"
    assert records[0].pk == "lookup-v2#bucket-a#docs%2Fguide.pdf"
    assert dynamodb.query_calls[0]["IndexName"] == "lookup-record-type-index"
    assert dynamodb.query_calls[0]["KeyConditionExpression"] == "record_type = :record_type"
    assert dynamodb.query_calls[0]["ExpressionAttributeValues"][":record_type"]["S"] == "LOOKUP"
    assert dynamodb.query_calls[0]["ConsistentRead"] is False
    assert dynamodb.query_calls[1]["ExclusiveStartKey"] == {"pk": {"S": "lookup#bucket-a#docs/guide.pdf"}}


def test_iter_lookup_records_prefers_new_lookup_key_for_duplicate_objects() -> None:
    """
    EN: Iter lookup records prefers new lookup key for duplicate objects.
    CN: 同上。
    """
    dynamodb = _DuplicateLookupQueryDynamoDbClient()
    repo = ObjectStateRepository(table_name="object-state", dynamodb_client=dynamodb)

    records = list(repo.iter_lookup_records())

    assert len(records) == 1
    assert records[0].pk == "lookup-v2#bucket-a#docs%2Fguide.pdf"
    assert records[0].latest_version_id == "v2"


def test_object_identity_keys_escape_delimiter_collisions() -> None:
    """
    EN: Object identity keys escape delimiter collisions for tenant, bucket, key, and version values.
    CN: 对象身份主键会对 tenant、bucket、key 和 version 值中的分隔符碰撞进行转义。
    """
    source = S3ObjectRef(
        tenant_id="tenant#a",
        bucket="bucket/with#hash",
        key="docs/guide#final%.pdf",
        version_id="v1#latest",
    )

    assert source.object_pk == "tenant%23a#bucket%2Fwith%23hash#docs%2Fguide%23final%25.pdf"
    assert source.version_pk == "tenant%23a#bucket%2Fwith%23hash#docs%2Fguide%23final%25.pdf#v1%23latest"
