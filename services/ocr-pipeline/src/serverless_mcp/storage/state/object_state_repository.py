"""
EN: Object state repository for version progression and idempotency enforcement using DynamoDB.
CN: 使用 DynamoDB 实现版本推进与幂等约束的 object_state 仓库。

This repository manages the object_state table, enforcing that S3 events are processed
in sequencer order and that previous_version_id is persisted before latest_version_id is flipped.
同上。
涔嬪墠鎸佷箙鍖?previous_version_id銆?
"""
from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Iterator
from urllib.parse import quote

from botocore.exceptions import ClientError

from serverless_mcp.domain.models import ObjectStateRecord, S3ObjectRef, utc_now_iso

_STATE_RECORD_TYPE = "STATE"
_LOOKUP_RECORD_TYPE = "LOOKUP"
_LOOKUP_RECORD_INDEX_NAME = "lookup-record-type-index"


@dataclass(frozen=True, slots=True)
class ObjectStateLookupRecord:
    """
    EN: Lookup record that resolves one S3 bucket/key pair to the authoritative object_state key.
    CN: 将一个 S3 bucket/key 对解析到权威 object_state 键的查找记录。

    The lookup table enables efficient bucket/key to object_pk resolution without scanning
    the object_state table, and supports both v2 and legacy key formats for backward compatibility.
    同上。
    骞跺悓鏃舵敮鎸?v2 鍜屾棫鐗?key 鏍煎紡浠ヤ繚鎸佸悜鍚庡吋瀹广€?
    """

    pk: str
    object_pk: str
    tenant_id: str
    bucket: str
    key: str
    latest_version_id: str
    latest_sequencer: str | None
    latest_manifest_s3_uri: str | None = None
    is_deleted: bool = False
    updated_at: str = ""


class DuplicateOrStaleEventError(RuntimeError):
    """
    EN: Raised when S3 event is duplicate or has stale sequencer compared to current state.
    CN: 当 S3 事件重复，或 sequencer 相比当前状态已经过时时抛出。

    This error indicates that the incoming event should be skipped because a newer or
    identical event has already been processed.
    同上。
    """


class ObjectStateRepository:
    """
    EN: Manage object_state records for version progression, idempotency, and status tracking.
    CN: 管理用于版本推进、幂等和状态跟踪的 object_state 记录。

    All state transitions use DynamoDB transactions to atomically update both object_state
    and lookup records, preventing split-brain between the two tables.
    鎵€鏈夌姸鎬佽浆鎹㈠潎浣跨敤 DynamoDB 浜嬪姟鍘熷瓙鎬у湴鍚屾椂鏇存柊 object_state 鍜?lookup 璁板綍锛?
    同上。
    """

    def __init__(self, *, table_name: str, dynamodb_client: object) -> None:
        # EN: DynamoDB table name shared by object_state and lookup records.
        # CN: object_state 和 lookup 记录共享的 DynamoDB 表名。
        self._table_name = table_name
        # EN: Boto3 DynamoDB client for conditional writes and consistent reads.
        # CN: 用于条件写入和一致性读取的 Boto3 DynamoDB 客户端。
        self._ddb = dynamodb_client
        self._table_sort_key_name = self._resolve_table_sort_key_name()

    def queue_for_ingest(self, source: S3ObjectRef) -> ObjectStateRecord:
        """
        EN: Persist previous_version_id before flipping latest_version_id, reject stale events.
        CN: 在切换 latest_version_id 之前先持久化 previous_version_id，并拒绝过时事件。

        Args:
            source:
                EN: S3 object reference containing bucket, key, version_id, and sequencer.
                CN: 包含 bucket、key、version_id 和 sequencer 的 S3 对象引用。

        Returns:
            EN: The newly created or updated ObjectStateRecord with extract_status=QUEUED.
            CN: 新建或更新后的 ObjectStateRecord，extract_status 为 QUEUED。

        Raises:
            EN: DuplicateOrStaleEventError if the sequencer is stale or the event is a duplicate.
            CN: 当 sequencer 过时或事件重复时抛出 DuplicateOrStaleEventError。
        """
        normalized_sequencer = _normalize_sequencer(source.sequencer)
        current_state = self.get_state(object_pk=source.object_pk)
        record = ObjectStateRecord(
            pk=source.object_pk,
            latest_version_id=source.version_id,
            latest_sequencer=normalized_sequencer,
            extract_status="QUEUED",
            embed_status="PENDING",
            updated_at=utc_now_iso(),
        )
        expression_names = {"#updated_at": "updated_at"}
        expression_values = {
            ":version_id": {"S": source.version_id},
            ":extract_status": {"S": "QUEUED"},
            ":embed_status": {"S": "PENDING"},
            ":updated_at": {"S": record.updated_at},
            ":empty_error": {"S": ""},
            ":false": {"BOOL": False},
        }
        update_parts = [
            "latest_version_id = :version_id",
            "extract_status = :extract_status",
            "embed_status = :embed_status",
            "#updated_at = :updated_at",
            "last_error = :empty_error",
            "is_deleted = :false",
        ]

        # EN: Condition ensures only new records or newer sequencers can proceed.
        # CN: 这个条件确保只有新记录或更新的 sequencer 才能继续。
        condition = "attribute_not_exists(pk)"
        if normalized_sequencer:
            expression_names["#latest_sequencer"] = "latest_sequencer"
            expression_values[":sequencer"] = {"S": normalized_sequencer}
            update_parts.append("#latest_sequencer = :sequencer")
            condition = "attribute_not_exists(pk) OR attribute_not_exists(#latest_sequencer) OR #latest_sequencer < :sequencer"

        try:
            self._transact_state_and_lookup(
                update_item={
                    "TableName": self._table_name,
                    "Key": self._build_state_key(record.pk),
                    "UpdateExpression": "SET " + ", ".join(update_parts),
                    "ConditionExpression": condition,
                    "ExpressionAttributeNames": expression_names,
                    "ExpressionAttributeValues": expression_values,
                },
                lookup_record=ObjectStateLookupRecord(
                    pk=_build_lookup_pk(bucket=source.bucket, key=source.key),
                    object_pk=source.object_pk,
                    tenant_id=source.tenant_id,
                    bucket=source.bucket,
                    key=source.key,
                    latest_version_id=source.version_id,
                    latest_sequencer=normalized_sequencer,
                    latest_manifest_s3_uri=current_state.latest_manifest_s3_uri if current_state else None,
                    is_deleted=False,
                    updated_at=record.updated_at,
                ),
            )
        except ClientError as exc:
            if _is_duplicate_or_stale_dynamodb_error(exc):
                raise DuplicateOrStaleEventError(source.document_uri) from exc
            raise

        # EN: Capture previous_version_id for downstream cleanup after the transaction succeeds.
        # CN: 在事务成功后记录 previous_version_id，供下游清理使用。
        previous_version_id = current_state.latest_version_id if current_state is not None else None
        if previous_version_id and previous_version_id != source.version_id:
            record.previous_version_id = previous_version_id
            record.previous_manifest_s3_uri = current_state.latest_manifest_s3_uri
        record.last_error = ""
        return record

    def start_processing(self, source: S3ObjectRef) -> ObjectStateRecord:
        """
        EN: Initialize or resume processing state for an S3 object version.
        CN: 初始化或恢复某个 S3 对象版本的处理状态。

        Args:
            source:
                EN: S3 object reference to initialize or resume processing for.
                CN: 需要初始化或恢复处理的 S3 对象引用。

        Returns:
            EN: The ObjectStateRecord with extract_status promoted to EXTRACTING.
            CN: extract_status 已提升为 EXTRACTING 的 ObjectStateRecord。

        Raises:
            EN: DuplicateOrStaleEventError if the version cannot be claimed.
            CN: 当该版本无法被认领时抛出 DuplicateOrStaleEventError。
        """
        current_state = self.get_state(object_pk=source.object_pk)
        if (
            current_state is not None
            and current_state.latest_version_id == source.version_id
            and current_state.extract_status in {"QUEUED", "EXTRACTING"}
            and not current_state.is_deleted
        ):
            preview = ObjectStateRecord(
                pk=source.object_pk,
                latest_version_id=source.version_id,
                latest_sequencer=_normalize_sequencer(source.sequencer),
                extract_status="QUEUED",
                embed_status="PENDING",
                previous_version_id=current_state.previous_version_id,
                previous_manifest_s3_uri=current_state.previous_manifest_s3_uri,
                updated_at=current_state.updated_at,
            )
            return self.activate_ingest_state(source, preview)

        preview = _build_ingest_processing_state(source=source, current_state=current_state)
        return self.activate_ingest_state(source, preview)

    def activate_ingest_state(self, source: S3ObjectRef, processing_state: ObjectStateRecord) -> ObjectStateRecord:
        """
        EN: Claim or resume the queued ingest state for one version and promote it to EXTRACTING.
        CN: 认领或恢复某个版本的排队 ingest 状态，并将其提升为 EXTRACTING。

        Args:
            source:
                EN: S3 object reference to activate.
                CN: 要激活的 S3 对象引用。
            processing_state:
                EN: Preview state containing previous_version_id and previous_manifest_s3_uri.
                CN: 包含 previous_version_id 和 previous_manifest_s3_uri 的预览状态。

        Returns:
            EN: The activated ObjectStateRecord with extract_status=EXTRACTING.
            CN: 已激活且 extract_status 为 EXTRACTING 的 ObjectStateRecord。

        Raises:
            EN: DuplicateOrStaleEventError if the version cannot be claimed or is already complete.
            CN: 当版本无法认领或已完成时抛出 DuplicateOrStaleEventError。
        """
        current_state = self.get_state(object_pk=source.object_pk)
        if (
            current_state is not None
            and current_state.latest_version_id == source.version_id
            and not current_state.is_deleted
        ):
            if current_state.extract_status == "EXTRACTING":
                return current_state
            if current_state.extract_status == "QUEUED":
                running = self.mark_extract_running(source)
                running.previous_version_id = current_state.previous_version_id
                running.previous_manifest_s3_uri = current_state.previous_manifest_s3_uri
                return running
            raise DuplicateOrStaleEventError(source.document_uri)

        normalized_sequencer = _normalize_sequencer(source.sequencer)
        updated_at = utc_now_iso()
        expression_names = {
            "#updated_at": "updated_at",
            "#latest_sequencer": "latest_sequencer",
        }
        expression_values = {
            ":version_id": {"S": source.version_id},
            ":extract_status": {"S": "EXTRACTING"},
            ":embed_status": {"S": "PENDING"},
            ":updated_at": {"S": updated_at},
            ":last_error": {"S": ""},
            ":false": {"BOOL": False},
        }
        parts = [
            "latest_version_id = :version_id",
            "extract_status = :extract_status",
            "embed_status = :embed_status",
            "#updated_at = :updated_at",
            "last_error = :last_error",
            "is_deleted = :false",
        ]
        remove_parts = ["latest_manifest_s3_uri"]
        condition = "attribute_not_exists(pk)"
        if normalized_sequencer:
            expression_values[":sequencer"] = {"S": normalized_sequencer}
            parts.append("#latest_sequencer = :sequencer")
            condition = "attribute_not_exists(pk) OR attribute_not_exists(#latest_sequencer) OR #latest_sequencer < :sequencer"
        if processing_state.previous_version_id:
            expression_values[":previous_version_id"] = {"S": processing_state.previous_version_id}
            parts.append("previous_version_id = :previous_version_id")
        else:
            remove_parts.append("previous_version_id")
        if processing_state.previous_manifest_s3_uri:
            expression_values[":previous_manifest"] = {"S": processing_state.previous_manifest_s3_uri}
            parts.append("previous_manifest_s3_uri = :previous_manifest")
        else:
            remove_parts.append("previous_manifest_s3_uri")

        update_expression = "SET " + ", ".join(parts)
        if remove_parts:
            update_expression += " REMOVE " + ", ".join(remove_parts)

        try:
            self._transact_state_and_lookup(
                update_item={
                    "TableName": self._table_name,
                    "Key": self._build_state_key(source.object_pk),
                    "UpdateExpression": update_expression,
                    "ConditionExpression": condition,
                    "ExpressionAttributeNames": expression_names,
                    "ExpressionAttributeValues": expression_values,
                },
                lookup_record=ObjectStateLookupRecord(
                    pk=_build_lookup_pk(bucket=source.bucket, key=source.key),
                    object_pk=source.object_pk,
                    tenant_id=source.tenant_id,
                    bucket=source.bucket,
                    key=source.key,
                    latest_version_id=source.version_id,
                    latest_sequencer=normalized_sequencer,
                    latest_manifest_s3_uri=None,
                    is_deleted=False,
                    updated_at=updated_at,
                ),
            )
        except ClientError as exc:
            if _is_duplicate_or_stale_dynamodb_error(exc):
                raise DuplicateOrStaleEventError(source.document_uri) from exc
            raise

        return ObjectStateRecord(
            pk=source.object_pk,
            latest_version_id=source.version_id,
            latest_sequencer=normalized_sequencer,
            extract_status="EXTRACTING",
            embed_status="PENDING",
            previous_version_id=processing_state.previous_version_id,
            previous_manifest_s3_uri=processing_state.previous_manifest_s3_uri,
            latest_manifest_s3_uri=None,
            is_deleted=False,
            last_error="",
            updated_at=updated_at,
        )

    def get_state(self, *, object_pk: str) -> ObjectStateRecord | None:
        """
        EN: Load the latest object state record by primary key with consistent reads.
        CN: 通过主键读取最新的 object_state 记录，并使用一致性读取。

        Args:
            object_pk:
                EN: Primary key of the object_state record (tenant_id#bucket#key).
                CN: object_state 记录的主键（tenant_id#bucket#key）。

        Returns:
            EN: The ObjectStateRecord if it exists, otherwise None.
            CN: 存在时返回 ObjectStateRecord，否则返回 None。
        """
        response = self._ddb.get_item(
            TableName=self._table_name,
            Key=self._build_state_key(object_pk),
            ConsistentRead=True,
        )
        item = response.get("Item")
        if not item:
            return None
        return _deserialize_object_state(item)

    def get_states_batch(self, *, object_pks: list[str]) -> dict[str, ObjectStateRecord | None]:
        """
        EN: Load object state records for multiple objects in a single batch query.
        CN: 在单次批量查询中加载多个对象的 object_state 记录。

        Returns a dict mapping object_pk to ObjectStateRecord (or None if not found).
        """
        if not object_pks:
            return {}
        results: dict[str, ObjectStateRecord | None] = {pk: None for pk in object_pks}
        keys = [self._build_state_key(pk) for pk in object_pks]
        response = self._ddb.batch_get_item(
            RequestItems={
                self._table_name: {
                    "Keys": keys,
                    "ConsistentRead": True,
                }
            }
        )
        for item in response.get("Responses", {}).get(self._table_name, []):
            record = _deserialize_object_state(item)
            results[record.pk] = record
        unprocessed = response.get("UnprocessedKeys", {}).get(self._table_name, {}).get("Keys", [])
        while unprocessed:
            response = self._ddb.batch_get_item(
                RequestItems={
                    self._table_name: {
                        "Keys": unprocessed,
                        "ConsistentRead": True,
                    }
                }
            )
            for item in response.get("Responses", {}).get(self._table_name, []):
                record = _deserialize_object_state(item)
                results[record.pk] = record
            unprocessed = response.get("UnprocessedKeys", {}).get(self._table_name, {}).get("Keys", [])
        return results

    def get_lookup_record(self, *, bucket: str, key: str) -> ObjectStateLookupRecord | None:
        """
        EN: Load the explicit lookup item for one bucket/key pair.
        CN: 读取某个 bucket/key 对应的显式查找记录。

        Args:
            bucket:
                EN: S3 bucket name.
                CN: S3 bucket 名称。
            key:
                EN: S3 object key.
                CN: S3 对象 key。

        Returns:
            EN: The ObjectStateLookupRecord if found, otherwise None.
            CN: 找到时返回 ObjectStateLookupRecord，否则返回 None。
        """
        return self._load_lookup_record(bucket=bucket, key=key)

    def get_lookup_for_source(self, source: S3ObjectRef) -> ObjectStateLookupRecord | None:
        """
        EN: Load the lookup item for one versioned S3 source object.
        CN: 读取某个带版本的 S3 源对象对应的查找记录。

        Args:
            source:
                EN: S3 object reference containing bucket and key.
                CN: 包含 bucket 和 key 的 S3 对象引用。

        Returns:
            EN: The ObjectStateLookupRecord if found, otherwise None.
            CN: 找到时返回 ObjectStateLookupRecord，否则返回 None。
        """
        return self._load_lookup_record(bucket=source.bucket, key=source.key)

    def get_lookup(self, *, bucket: str, key: str) -> ObjectStateLookupRecord | None:
        """
        EN: Backward-compatible alias for lookup access by bucket/key.
        CN: 按 bucket/key 访问查找记录的向后兼容别名。

        Args:
            bucket:
                EN: S3 bucket name.
                CN: S3 bucket 名称。
            key:
                EN: S3 object key.
                CN: S3 对象 key。

        Returns:
            EN: The ObjectStateLookupRecord if found, otherwise None.
            CN: 找到时返回 ObjectStateLookupRecord，否则返回 None。
        """
        return self.get_lookup_record(bucket=bucket, key=key)

    def iter_lookup_records(self) -> Iterator[ObjectStateLookupRecord]:
        """
        EN: Iterate over lookup records that describe the latest known S3 object identity for backfill and replay.
        CN: 遍历描述最新已知 S3 对象身份的查找记录，供回填和重放使用。

        Returns:
            EN: Iterator yielding ObjectStateLookupRecord for each unique object_pk.
            CN: 为每个唯一 object_pk 产出 ObjectStateLookupRecord 的迭代器。
        """
        exclusive_start_key: dict[str, dict[str, str]] | None = None
        lookup_records: dict[str, ObjectStateLookupRecord] = {}
        while True:
            kwargs = {
                "TableName": self._table_name,
                "IndexName": _LOOKUP_RECORD_INDEX_NAME,
                "KeyConditionExpression": "record_type = :record_type",
                "ExpressionAttributeValues": {":record_type": {"S": _LOOKUP_RECORD_TYPE}},
                "ExpressionAttributeNames": {"#pk": "pk", "#key": "key"},
                "ConsistentRead": False,
                "ProjectionExpression": (
                    "#pk, object_pk, tenant_id, bucket, #key, latest_version_id, latest_sequencer, "
                    "latest_manifest_s3_uri, is_deleted, updated_at"
                ),
            }
            if exclusive_start_key is not None:
                kwargs["ExclusiveStartKey"] = exclusive_start_key
            response = self._ddb.query(**kwargs)
            for item in response.get("Items") or []:
                record = _deserialize_lookup_record(item)
                current = lookup_records.get(record.object_pk)
                if current is None or _prefer_lookup_record(record, current):
                    lookup_records[record.object_pk] = record
            exclusive_start_key = response.get("LastEvaluatedKey")
            if not exclusive_start_key:
                break

        for record in sorted(
            lookup_records.values(),
            key=lambda item: (item.object_pk, item.latest_version_id, item.updated_at or ""),
        ):
            yield record

    def mark_deleted(self, *, bucket: str, key: str, version_id: str, sequencer: str | None) -> ObjectStateRecord:
        """
        EN: Mark the current object version deleted using bucket/key lookup and delete-marker version identity.
        CN: 使用 bucket/key 查找和 delete-marker 版本身份，将当前对象版本标记为已删除。

        Args:
            bucket:
                EN: S3 bucket name.
                CN: S3 bucket 名称。
            key:
                EN: S3 object key.
                CN: S3 对象 key。
            version_id:
                EN: Delete-marker version_id.
                CN: delete-marker 的 version_id。
            sequencer:
                EN: Optional S3 event sequencer for ordering.
                CN: 可选的 S3 事件 sequencer，用于排序。

        Returns:
            EN: The updated ObjectStateRecord with is_deleted=True.
            CN: 更新后的 ObjectStateRecord，is_deleted 为 True。
        """
        lookup = self.get_lookup_record(bucket=bucket, key=key)
        if lookup is None:
            raise DuplicateOrStaleEventError(f"s3://{bucket}/{key}?versionId={version_id}")
        object_pk = lookup.object_pk
        tenant_id = lookup.tenant_id
        current_state = self.get_state(object_pk=object_pk)
        if current_state is not None and current_state.is_deleted and current_state.latest_version_id == version_id:
            return current_state

        normalized_sequencer = _normalize_sequencer(sequencer)
        updated_at = utc_now_iso()
        tombstone_state = ObjectStateRecord(
            pk=object_pk,
            latest_version_id=version_id,
            latest_sequencer=normalized_sequencer,
            extract_status="FAILED",
            embed_status="FAILED",
            previous_version_id=current_state.previous_version_id if current_state else None,
            previous_manifest_s3_uri=current_state.previous_manifest_s3_uri if current_state else None,
            latest_manifest_s3_uri=current_state.latest_manifest_s3_uri if current_state else None,
            is_deleted=True,
            last_error="",
            updated_at=updated_at,
        )
        tombstone_lookup = ObjectStateLookupRecord(
            pk=lookup.pk if lookup is not None else _build_legacy_lookup_pk(bucket=bucket, key=key),
            object_pk=object_pk,
            tenant_id=tenant_id,
            bucket=bucket,
            key=key,
            latest_version_id=version_id,
            latest_sequencer=normalized_sequencer,
            latest_manifest_s3_uri=current_state.latest_manifest_s3_uri if current_state else None,
            is_deleted=True,
            updated_at=updated_at,
        )
        if current_state is None:
            self._ddb.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": self._table_name,
                            "Item": _serialize_object_state(tombstone_state),
                        }
                    },
                    {
                        "Put": {
                            "TableName": self._table_name,
                            "Item": _serialize_lookup_record(tombstone_lookup),
                        }
                    },
                ]
            )
            return tombstone_state

        expression_names = {
            "#updated_at": "updated_at",
            "#latest_sequencer": "latest_sequencer",
        }
        expression_values = {
            ":version_id": {"S": version_id},
            ":updated_at": {"S": updated_at},
            ":true": {"BOOL": True},
            ":last_error": {"S": ""},
        }
        parts = [
            "latest_version_id = :version_id",
            "#updated_at = :updated_at",
            "is_deleted = :true",
            "last_error = :last_error",
        ]
        condition = "attribute_exists(pk)"
        if normalized_sequencer:
            expression_values[":sequencer"] = {"S": normalized_sequencer}
            parts.append("#latest_sequencer = :sequencer")
            condition = "attribute_not_exists(#latest_sequencer) OR #latest_sequencer < :sequencer"

        try:
            self._transact_state_and_lookup(
                update_item={
                    "TableName": self._table_name,
                    "Key": self._build_state_key(lookup.object_pk),
                    "UpdateExpression": "SET " + ", ".join(parts),
                    "ConditionExpression": condition,
                    "ExpressionAttributeNames": expression_names,
                    "ExpressionAttributeValues": expression_values,
                },
                lookup_record=ObjectStateLookupRecord(
                    pk=lookup.pk,
                    object_pk=lookup.object_pk,
                    tenant_id=lookup.tenant_id,
                    bucket=bucket,
                    key=key,
                    latest_version_id=version_id,
                    latest_sequencer=normalized_sequencer,
                    latest_manifest_s3_uri=current_state.latest_manifest_s3_uri,
                    is_deleted=True,
                    updated_at=updated_at,
                ),
            )
        except ClientError as exc:
            if _is_duplicate_or_stale_dynamodb_error(exc):
                raise DuplicateOrStaleEventError(f"s3://{bucket}/{key}?versionId={version_id}") from exc
            raise

        return tombstone_state

    def mark_extract_running(self, source: S3ObjectRef) -> ObjectStateRecord:
        """
        EN: Transition extract status to EXTRACTING with pending embed status.
        CN: 将 extract 状态切换为 EXTRACTING，同时保留待处理的 embed 状态。

        Args:
            source:
                EN: S3 object reference for the version being extracted.
                CN: 正在提取的版本对应的 S3 对象引用。

        Returns:
            EN: The updated ObjectStateRecord with extract_status=EXTRACTING.
            CN: extract_status 为 EXTRACTING 的更新后 ObjectStateRecord。
        """
        return self._update_status(
            source,
            extract_status="EXTRACTING",
            embed_status="PENDING",
            latest_manifest_s3_uri=None,
            last_error="",
        )

    def mark_extract_done(self, source: S3ObjectRef, manifest_s3_uri: str) -> ObjectStateRecord:
        """
        EN: Mark extract as EXTRACTED and record the manifest S3 URI for embed dispatch.
        CN: 将 extract 标记为 EXTRACTED，并记录用于分发 embed 的 manifest S3 URI。

        Args:
            source:
                EN: S3 object reference for the version whose extraction completed.
                CN: 提取已完成的版本对应的 S3 对象引用。
            manifest_s3_uri:
                EN: Version-aware S3 URI of the persisted manifest.
                CN: 持久化 manifest 的带版本 S3 URI。

        Returns:
            EN: The updated ObjectStateRecord with extract_status=EXTRACTED.
            CN: extract_status 为 EXTRACTED 的更新后 ObjectStateRecord。
        """
        return self._update_status(
            source,
            extract_status="EXTRACTED",
            embed_status="PENDING",
            latest_manifest_s3_uri=manifest_s3_uri,
            last_error="",
        )

    def mark_extract_failed(self, source: S3ObjectRef, error_message: str) -> ObjectStateRecord:
        """
        EN: Mark both extract and embed as FAILED and record the truncated error message.
        CN: 将 extract 和 embed 都标记为 FAILED，并记录截断后的错误消息。

        Args:
            source:
                EN: S3 object reference for the version whose extraction failed.
                CN: 提取失败的版本对应的 S3 对象引用。
            error_message:
                EN: Error description, truncated to 1000 characters.
                CN: 错误描述，最多截断为 1000 个字符。

        Returns:
            EN: The updated ObjectStateRecord with extract_status=FAILED and embed_status=FAILED.
            CN: extract_status 和 embed_status 都为 FAILED 的更新后 ObjectStateRecord。
        """
        return self._update_status(
            source,
            extract_status="FAILED",
            embed_status="FAILED",
            latest_manifest_s3_uri=None,
            last_error=error_message[:1000],
        )

    def mark_failed(self, source: S3ObjectRef, error_message: str) -> ObjectStateRecord:
        """
        EN: Alias for mark_extract_failed for backward compatibility.
        CN: 供向后兼容使用的 mark_extract_failed 别名。

        Args:
            source:
                EN: S3 object reference for the failed version.
                CN: 失败版本对应的 S3 对象引用。
            error_message:
                EN: Error description.
                CN: 错误描述。

        Returns:
            EN: The updated ObjectStateRecord with FAILED status.
            CN: 状态为 FAILED 的更新后 ObjectStateRecord。
        """
        return self.mark_extract_failed(source, error_message)

    def mark_embed_running(self, source: S3ObjectRef) -> ObjectStateRecord:
        """
        EN: Mark embed status as EMBEDDING while keeping extract at EXTRACTED.
        CN: 将 embed 状态标记为 EMBEDDING，同时保持 extract 为 EXTRACTED。

        Args:
            source:
                EN: S3 object reference for the version being embedded.
                CN: 正在执行 embedding 的版本对应的 S3 对象引用。

        Returns:
            EN: The updated ObjectStateRecord with embed_status=EMBEDDING.
            CN: embed_status 为 EMBEDDING 的更新后 ObjectStateRecord。
        """
        return self._update_status(
            source,
            extract_status="EXTRACTED",
            embed_status="EMBEDDING",
            latest_manifest_s3_uri=None,
            last_error="",
        )

    def mark_embed_done(self, source: S3ObjectRef) -> ObjectStateRecord:
        """
        EN: Mark embed as INDEXED to signal vector persistence is complete.
        CN: 将 embed 标记为 INDEXED，表示向量持久化已经完成。

        Args:
            source:
                EN: S3 object reference for the version whose embedding completed.
                CN: embedding 已完成的版本对应的 S3 对象引用。

        Returns:
            EN: The updated ObjectStateRecord with embed_status=INDEXED.
            CN: embed_status 为 INDEXED 的更新后 ObjectStateRecord。
        """
        return self._update_status(
            source,
            extract_status="EXTRACTED",
            embed_status="INDEXED",
            latest_manifest_s3_uri=None,
            last_error="",
        )

    def mark_embed_failed(self, source: S3ObjectRef, error_message: str) -> ObjectStateRecord:
        """
        EN: Mark embed as FAILED while keeping extract at EXTRACTED for retry visibility.
        CN: 将 embed 标记为 FAILED，同时保持 extract 为 EXTRACTED，便于重试可见。

        Args:
            source:
                EN: S3 object reference for the version whose embedding failed.
                CN: embedding 失败的版本对应的 S3 对象引用。
            error_message:
                EN: Error description, truncated to 1000 characters.
                CN: 错误描述，最多截断为 1000 个字符。

        Returns:
            EN: The updated ObjectStateRecord with embed_status=FAILED.
            CN: embed_status 为 FAILED 的更新后 ObjectStateRecord。
        """
        return self._update_status(
            source,
            extract_status="EXTRACTED",
            embed_status="FAILED",
            latest_manifest_s3_uri=None,
            last_error=error_message[:1000],
        )

    def mark_embed_cleanup_failed(self, source: S3ObjectRef, error_message: str) -> ObjectStateRecord:
        """
        EN: Record a best-effort cleanup failure without changing the successful embed status.
        CN: 记录 best-effort 清理失败，但不改变已经成功的 embed 状态。
        """
        return self._update_status(
            source,
            extract_status="EXTRACTED",
            embed_status="INDEXED",
            latest_manifest_s3_uri=None,
            last_error=error_message[:1000],
        )

    def _update_status(
        self,
        source: S3ObjectRef,
        *,
        extract_status: str,
        embed_status: str,
        latest_manifest_s3_uri: str | None,
        last_error: str,
    ) -> ObjectStateRecord:
        """
        EN: Atomically update object_state and lookup record with conditional version check.
        CN: 通过条件版本检查，原子更新 object_state 和 lookup 记录。

        Args:
            source:
                EN: S3 object reference for the version being updated.
                CN: 正在更新的版本对应的 S3 对象引用。
            extract_status:
                EN: New extract status to set.
                CN: 要设置的新 extract 状态。
            embed_status:
                EN: New embed status to set.
                CN: 要设置的新 embed 状态。
            latest_manifest_s3_uri:
                EN: Optional manifest S3 URI to persist; None preserves existing value.
                CN: 可选的 manifest S3 URI；None 表示保留现有值。
            last_error:
                EN: Error message to record, or empty string on success.
                CN: 需要记录的错误消息，成功时传空字符串。

        Returns:
            EN: The updated ObjectStateRecord after the transaction.
            CN: 事务完成后的更新后 ObjectStateRecord。

        Raises:
            EN: DuplicateOrStaleEventError if the version_id condition check fails.
            CN: 当 version_id 条件检查失败时抛出 DuplicateOrStaleEventError。
        """
        normalized_sequencer = _normalize_sequencer(source.sequencer)
        updated_at = utc_now_iso()
        expression_names = {
            "#updated_at": "updated_at",
            "#latest_version_id": "latest_version_id",
        }
        expression_values = {
            ":version_id": {"S": source.version_id},
            ":extract_status": {"S": extract_status},
            ":embed_status": {"S": embed_status},
            ":updated_at": {"S": updated_at},
            ":last_error": {"S": last_error},
        }
        parts = [
            "extract_status = :extract_status",
            "embed_status = :embed_status",
            "#updated_at = :updated_at",
            "last_error = :last_error",
        ]
        if latest_manifest_s3_uri is not None:
            expression_values[":manifest"] = {"S": latest_manifest_s3_uri}
            parts.append("latest_manifest_s3_uri = :manifest")

        current_state = self.get_state(object_pk=source.object_pk)
        persisted_manifest_s3_uri = latest_manifest_s3_uri
        if persisted_manifest_s3_uri is None and current_state is not None:
            persisted_manifest_s3_uri = current_state.latest_manifest_s3_uri

        try:
            self._transact_state_and_lookup(
                update_item={
                    "TableName": self._table_name,
                    "Key": self._build_state_key(source.object_pk),
                    "UpdateExpression": "SET " + ", ".join(parts),
                    "ConditionExpression": "#latest_version_id = :version_id",
                    "ExpressionAttributeNames": expression_names,
                    "ExpressionAttributeValues": expression_values,
                },
                lookup_record=ObjectStateLookupRecord(
                    pk=_build_lookup_pk(bucket=source.bucket, key=source.key),
                    object_pk=source.object_pk,
                    tenant_id=source.tenant_id,
                    bucket=source.bucket,
                    key=source.key,
                    latest_version_id=source.version_id,
                    latest_sequencer=normalized_sequencer,
                    latest_manifest_s3_uri=persisted_manifest_s3_uri,
                    is_deleted=False,
                    updated_at=updated_at,
                ),
            )
        except ClientError as exc:
            if _is_duplicate_or_stale_dynamodb_error(exc):
                raise DuplicateOrStaleEventError(source.document_uri) from exc
            raise

        return ObjectStateRecord(
            pk=source.object_pk,
            latest_version_id=source.version_id,
            latest_sequencer=normalized_sequencer,
            extract_status=extract_status,  # type: ignore[arg-type]
            embed_status=embed_status,  # type: ignore[arg-type]
            previous_version_id=current_state.previous_version_id if current_state else None,
            previous_manifest_s3_uri=current_state.previous_manifest_s3_uri if current_state else None,
            latest_manifest_s3_uri=persisted_manifest_s3_uri,
            is_deleted=False,
            last_error=last_error,
            updated_at=updated_at,
        )

    def _put_lookup_record(self, record: ObjectStateLookupRecord) -> None:
        """
        EN: Persist a lookup record to DynamoDB for bucket/key resolution.
        CN: 将 lookup 记录持久化到 DynamoDB，供 bucket/key 解析使用。
        """
        self._ddb.put_item(TableName=self._table_name, Item=_serialize_lookup_record(record))

    def _transact_state_and_lookup(self, *, update_item: dict, lookup_record: ObjectStateLookupRecord) -> None:
        """
        EN: Execute a DynamoDB transaction updating both object_state and lookup atomically.
        CN: 执行 DynamoDB 事务，原子更新 object_state 和 lookup。
        """
        self._ddb.transact_write_items(
            TransactItems=[
                {"Update": update_item},
                {
                    "Put": {
                        "TableName": self._table_name,
                        "Item": _serialize_lookup_record(lookup_record),
                    }
                },
            ]
        )

    def _load_lookup_record(self, *, bucket: str, key: str) -> ObjectStateLookupRecord | None:
        """
        EN: Load lookup record, checking both v2 and legacy key formats for backward compatibility.
        CN: 加载 lookup 记录，同时检查 v2 和旧版键格式以保持向后兼容。
        """
        for pk in (
            _build_lookup_pk(bucket=bucket, key=key),
            _build_legacy_lookup_pk(bucket=bucket, key=key),
        ):
            response = self._ddb.get_item(
                TableName=self._table_name,
                Key=self._build_lookup_key(pk),
                ConsistentRead=True,
            )
            item = response.get("Item")
            if item:
                return _deserialize_lookup_record(item)
        return None

    def _resolve_table_sort_key_name(self) -> str | None:
        """
        EN: Inspect the DynamoDB table schema once and cache the sort key name when available.
        CN: 在初始化时缓存表的 sort key，以便所有读写都使用同一种 key 形状。
        """
        describe_table = getattr(self._ddb, "describe_table", None)
        if not callable(describe_table):
            return None
        try:
            response = describe_table(TableName=self._table_name)
        except Exception:
            return None
        key_schema = response.get("Table", {}).get("KeySchema", [])
        for entry in key_schema:
            if entry.get("KeyType") != "RANGE":
                continue
            attribute_name = entry.get("AttributeName")
            if isinstance(attribute_name, str) and attribute_name:
                return attribute_name
        return None

    def _build_state_key(self, object_pk: str) -> dict[str, dict[str, str]]:
        """
        EN: Build a DynamoDB key for an object_state record.
        CN: 为 object_state 记录构建 DynamoDB key。
        """
        return self._build_table_key(object_pk, record_type=_STATE_RECORD_TYPE)

    def _build_lookup_key(self, pk: str) -> dict[str, dict[str, str]]:
        """
        EN: Build a DynamoDB key for a lookup record.
        CN: 为 lookup 记录构建 DynamoDB key。
        """
        return self._build_table_key(pk, record_type=_LOOKUP_RECORD_TYPE)

    def _build_table_key(self, pk: str, *, record_type: str) -> dict[str, dict[str, str]]:
        """
        EN: Build a DynamoDB key that works for hash-only and composite-state tables.
        CN: 构建同时适用于 hash-only 和复合主键的 DynamoDB key。
        """
        key: dict[str, dict[str, str]] = {"pk": {"S": pk}}
        if self._table_sort_key_name:
            key[self._table_sort_key_name] = {"S": record_type}
        return key


def _build_lookup_pk(*, bucket: str, key: str) -> str:
    """
    EN: Build the v2 lookup primary key for a bucket/key pair.
    CN: 为 bucket/key 对构建 v2 版 lookup 主键。
    """
    return f"lookup-v2#{quote(bucket, safe='')}#{quote(key, safe='')}"


def _build_legacy_lookup_pk(*, bucket: str, key: str) -> str:
    """
    EN: Build the legacy lookup primary key for backward-compatible reads.
    CN: 为向后兼容读取构建旧版 lookup 主键。
    """
    return f"lookup#{quote(bucket, safe='')}#{quote(key, safe='')}"


def _serialize_lookup_record(record: ObjectStateLookupRecord) -> dict[str, dict[str, str | bool]]:
    """
    EN: Serialize an ObjectStateLookupRecord into a DynamoDB-compatible item dictionary.
    CN: 将 ObjectStateLookupRecord 序列化为兼容 DynamoDB 的 item 字典。
    """
    item: dict[str, dict[str, str | bool]] = {
        "pk": {"S": record.pk},
        "record_type": {"S": _LOOKUP_RECORD_TYPE},
        "sk": {"S": _LOOKUP_RECORD_TYPE},
        "object_pk": {"S": record.object_pk},
        "tenant_id": {"S": record.tenant_id},
        "bucket": {"S": record.bucket},
        "key": {"S": record.key},
        "latest_version_id": {"S": record.latest_version_id},
        "is_deleted": {"BOOL": record.is_deleted},
        "updated_at": {"S": record.updated_at},
    }
    if record.latest_sequencer:
        item["latest_sequencer"] = {"S": record.latest_sequencer}
    if record.latest_manifest_s3_uri:
        item["latest_manifest_s3_uri"] = {"S": record.latest_manifest_s3_uri}
    return item


def _serialize_object_state(record: ObjectStateRecord) -> dict[str, dict[str, str | bool]]:
    """
    EN: Serialize an ObjectStateRecord into a DynamoDB-compatible item dictionary.
    CN: 将 ObjectStateRecord 序列化为 DynamoDB 兼容的 item 字典。
    """
    item: dict[str, dict[str, str | bool]] = {
        "pk": {"S": record.pk},
        "record_type": {"S": _STATE_RECORD_TYPE},
        "sk": {"S": _STATE_RECORD_TYPE},
        "latest_version_id": {"S": record.latest_version_id},
        "extract_status": {"S": record.extract_status},
        "embed_status": {"S": record.embed_status},
        "is_deleted": {"BOOL": record.is_deleted},
        "last_error": {"S": record.last_error or ""},
        "updated_at": {"S": record.updated_at},
    }
    if record.latest_sequencer:
        item["latest_sequencer"] = {"S": record.latest_sequencer}
    if record.previous_version_id:
        item["previous_version_id"] = {"S": record.previous_version_id}
    if record.previous_manifest_s3_uri:
        item["previous_manifest_s3_uri"] = {"S": record.previous_manifest_s3_uri}
    if record.latest_manifest_s3_uri:
        item["latest_manifest_s3_uri"] = {"S": record.latest_manifest_s3_uri}
    return item


def _build_ingest_processing_state(
    *,
    source: S3ObjectRef,
    current_state: ObjectStateRecord | None,
) -> ObjectStateRecord:
    """
    EN: Build a queued processing state with version progression from the current snapshot.
    CN: 根据当前快照构建排队处理状态，并推进版本信息。
    """
    normalized_sequencer = _normalize_sequencer(source.sequencer)
    if current_state is not None:
        if current_state.latest_version_id == source.version_id:
            raise DuplicateOrStaleEventError(source.document_uri)
        if (
            normalized_sequencer
            and current_state.latest_sequencer
            and current_state.latest_sequencer >= normalized_sequencer
        ):
            raise DuplicateOrStaleEventError(source.document_uri)
    previous_version_id = None
    previous_manifest_s3_uri = None
    if current_state is not None:
        previous_version_id = current_state.latest_version_id
        previous_manifest_s3_uri = current_state.latest_manifest_s3_uri
    return ObjectStateRecord(
        pk=source.object_pk,
        latest_version_id=source.version_id,
        latest_sequencer=normalized_sequencer,
        extract_status="QUEUED",
        embed_status="PENDING",
        previous_version_id=previous_version_id,
        previous_manifest_s3_uri=previous_manifest_s3_uri,
        latest_manifest_s3_uri=None,
        is_deleted=False,
        last_error="",
        updated_at=utc_now_iso(),
    )


def _normalize_sequencer(sequencer: str | None) -> str | None:
    """
    EN: Normalize sequencer text for consistent ordering across DynamoDB condition checks.
    CN: 规范化 sequencer 文本，以便在 DynamoDB 条件检查中保持一致排序。
    """
    value = (sequencer or "").strip()
    if not value:
        return None
    return value.upper().zfill(32)


def _deserialize_object_state(item: dict[str, dict[str, str | bool]]) -> ObjectStateRecord:
    """
    EN: Deserialize a DynamoDB item into an ObjectStateRecord domain model.
    CN: 将 DynamoDB item 反序列化为 ObjectStateRecord 领域模型。
    """
    return ObjectStateRecord(
        pk=item["pk"]["S"],  # type: ignore[index]
        latest_version_id=item["latest_version_id"]["S"],  # type: ignore[index]
        latest_sequencer=item.get("latest_sequencer", {}).get("S"),  # type: ignore[union-attr]
        extract_status=item["extract_status"]["S"],  # type: ignore[index,arg-type]
        embed_status=item["embed_status"]["S"],  # type: ignore[index,arg-type]
        previous_version_id=item.get("previous_version_id", {}).get("S"),  # type: ignore[union-attr]
        previous_manifest_s3_uri=item.get("previous_manifest_s3_uri", {}).get("S"),  # type: ignore[union-attr]
        latest_manifest_s3_uri=item.get("latest_manifest_s3_uri", {}).get("S"),  # type: ignore[union-attr]
        is_deleted=bool(item.get("is_deleted", {}).get("BOOL", False)),  # type: ignore[union-attr]
        last_error=item.get("last_error", {}).get("S"),  # type: ignore[union-attr]
        updated_at=item.get("updated_at", {}).get("S", ""),  # type: ignore[union-attr]
    )


def _deserialize_lookup_record(item: dict[str, dict[str, str | bool]]) -> ObjectStateLookupRecord:
    """
    EN: Deserialize a DynamoDB item into an ObjectStateLookupRecord.
    CN: 将 DynamoDB item 反序列化为 ObjectStateLookupRecord。
    """
    return ObjectStateLookupRecord(
        pk=item["pk"]["S"],  # type: ignore[index]
        object_pk=item["object_pk"]["S"],  # type: ignore[index]
        tenant_id=item["tenant_id"]["S"],  # type: ignore[index]
        bucket=item["bucket"]["S"],  # type: ignore[index]
        key=item["key"]["S"],  # type: ignore[index]
        latest_version_id=item["latest_version_id"]["S"],  # type: ignore[index]
        latest_sequencer=item.get("latest_sequencer", {}).get("S"),  # type: ignore[union-attr]
        latest_manifest_s3_uri=item.get("latest_manifest_s3_uri", {}).get("S"),  # type: ignore[union-attr]
        is_deleted=bool(item.get("is_deleted", {}).get("BOOL", False)),  # type: ignore[union-attr]
        updated_at=item.get("updated_at", {}).get("S", ""),  # type: ignore[union-attr]
    )


def _prefer_lookup_record(candidate: ObjectStateLookupRecord, current: ObjectStateLookupRecord) -> bool:
    """
    EN: Prefer the non-legacy lookup key when both versions exist for the same object.
    CN: 当同一个对象同时存在两种版本时，优先选择非旧版 lookup key。
    """
    candidate_is_legacy = _is_legacy_lookup_pk(candidate.pk)
    current_is_legacy = _is_legacy_lookup_pk(current.pk)
    if candidate_is_legacy != current_is_legacy:
        return not candidate_is_legacy
    return candidate.updated_at >= current.updated_at


def _is_duplicate_or_stale_dynamodb_error(exc: ClientError) -> bool:
    """
    EN: Recognize duplicate/stale conditional write failures, including transactional wrappers.
    CN: 识别重复/过期导致的条件写失败，也兼容事务包装后的异常。
    """
    error = exc.response.get("Error", {})
    code = error.get("Code")
    if code == "ConditionalCheckFailedException":
        return True
    if code != "TransactionCanceledException":
        return False

    cancellation_reasons = exc.response.get("CancellationReasons")
    if isinstance(cancellation_reasons, list):
        for reason in cancellation_reasons:
            if isinstance(reason, dict) and reason.get("Code") == "ConditionalCheckFailed":
                return True

    message = error.get("Message")
    return isinstance(message, str) and "ConditionalCheckFailed" in message


def _is_legacy_lookup_pk(pk: str) -> bool:
    # EN: Check whether the primary key uses the legacy lookup# prefix.
    # CN: 检查主键是否使用旧版 lookup# 前缀。
    return pk.startswith("lookup#")
