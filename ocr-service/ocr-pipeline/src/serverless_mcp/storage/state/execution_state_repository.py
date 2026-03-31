"""
EN: Execution-state repository for extract and embed workflow status.
CN: 用于 extract 和 embed 工作流状态的 execution-state 仓储。
"""
from __future__ import annotations

from dataclasses import dataclass

from serverless_mcp.domain.models import ObjectStateRecord, S3ObjectRef, utc_now_iso
from serverless_mcp.storage.batch import dedupe_preserve_order
from .object_state_repository import DuplicateOrStaleEventError, _normalize_sequencer


@dataclass(frozen=True, slots=True)
class ExecutionStateLookupRecord:
    """
    EN: Lookup record that keeps object identity and latest execution-state pointer.
    CN: 保存对象身份和最新 execution-state 指针的查找记录。
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


class ExecutionStateRepository:
    """
    EN: Persist per-object workflow execution state in a dedicated DynamoDB table.
    CN: 将单个对象的工作流执行态持久化到独立的 DynamoDB 表。
    """

    def __init__(self, *, table_name: str, dynamodb_client: object) -> None:
        # EN: DynamoDB table dedicated to execution-state transitions.
        # CN: 专用于 execution-state 变迁的 DynamoDB 表。
        self._table_name = table_name
        # EN: Boto3 DynamoDB client used for reads and writes.
        # CN: 用于读写的 Boto3 DynamoDB client。
        self._ddb = dynamodb_client

    def get_state(self, *, object_pk: str) -> ObjectStateRecord | None:
        """
        EN: Load the current execution state snapshot for one object.
        CN: 读取单个对象当前的 execution-state 快照。
        """
        response = self._ddb.get_item(
            TableName=self._table_name,
            Key={"pk": {"S": object_pk}},
            ConsistentRead=True,
        )
        item = response.get("Item")
        if not item:
            return None
        return _deserialize_execution_state(item)

    def get_states_batch(self, *, object_pks: list[str]) -> dict[str, ObjectStateRecord | None]:
        """
        EN: Load multiple execution state records by primary keys using batch get.
        CN: 通过主键批量读取多个 execution-state 记录。

        Args:
            object_pks:
                EN: List of primary keys of the execution-state records.
                CN: execution-state 记录的主键列表。

        Returns:
            EN: Dictionary mapping object_pk to ObjectStateRecord (or None if not found).
            CN: 从 object_pk 到 ObjectStateRecord 的字典（未找到时为 None）。
        """
        if not object_pks:
            return {}

        result: dict[str, ObjectStateRecord | None] = {}
        remaining = dedupe_preserve_order(object_pks)

        while remaining:
            batch = remaining[:100]
            remaining = remaining[100:]
            batch = dedupe_preserve_order(batch)

            keys = [{"pk": {"S": pk}} for pk in batch]
            response = self._ddb.batch_get_item(
                RequestItems={
                    self._table_name: {
                        "Keys": keys,
                        "ConsistentRead": True,
                    }
                }
            )

            items = response.get("Responses", {}).get(self._table_name, [])
            for item in items:
                record = _deserialize_execution_state(item)
                result[record.pk] = record

            unprocessed = response.get("UnprocessedKeys", {})
            if unprocessed.get(self._table_name, {}).get("Keys"):
                remaining.extend([pk for pk in batch if pk not in result])

        for pk in object_pks:
            if pk not in result:
                result[pk] = None

        return result

    def list_object_records(self) -> list[ObjectStateRecord]:
        """
        EN: Return all execution-state records currently stored in the table.
        CN: 返回当前表中保存的全部 execution-state 记录。
        """
        paginator = self._ddb.get_paginator("scan")
        records: list[ObjectStateRecord] = []
        for page in paginator.paginate(TableName=self._table_name, ConsistentRead=True):
            for item in page.get("Items", []):
                records.append(_deserialize_execution_state(item))
        return records

    def start_processing(self, source: S3ObjectRef) -> ObjectStateRecord:
        """
        EN: Claim the queued execution state for one version and promote it to EXTRACTING.
        CN: 认领某个版本的 queued execution-state，并将其提升到 EXTRACTING。
        """
        current_state = self.get_state(object_pk=source.object_pk)
        normalized_sequencer = _normalize_sequencer(source.sequencer)
        if current_state is not None:
            if current_state.latest_version_id == source.version_id:
                if current_state.extract_status == "EXTRACTING":
                    return current_state
                if current_state.extract_status == "QUEUED":
                    return self.mark_extract_running(source)
                raise DuplicateOrStaleEventError(source.document_uri)
            if (
                normalized_sequencer
                and current_state.latest_sequencer
                and current_state.latest_sequencer >= normalized_sequencer
            ):
                raise DuplicateOrStaleEventError(source.document_uri)

        preview = _build_ingest_processing_state(source=source, current_state=current_state)
        return self.activate_ingest_state(source, preview)

    def activate_ingest_state(self, source: S3ObjectRef, processing_state: ObjectStateRecord) -> ObjectStateRecord:
        """
        EN: Persist a queued execution-state snapshot and promote it to EXTRACTING.
        CN: 持久化 queued execution-state 快照并提升为 EXTRACTING。
        """
        current_state = self.get_state(object_pk=source.object_pk)
        if current_state is not None:
            if current_state.latest_version_id == source.version_id:
                if current_state.extract_status == "EXTRACTING":
                    return current_state
                if current_state.extract_status == "QUEUED":
                    return self.mark_extract_running(source)
                raise DuplicateOrStaleEventError(source.document_uri)
            normalized_sequencer = _normalize_sequencer(source.sequencer)
            if (
                normalized_sequencer
                and current_state.latest_sequencer
                and current_state.latest_sequencer >= normalized_sequencer
            ):
                raise DuplicateOrStaleEventError(source.document_uri)

        record = ObjectStateRecord(
            pk=source.object_pk,
            latest_version_id=processing_state.latest_version_id,
            latest_sequencer=processing_state.latest_sequencer,
            extract_status="EXTRACTING",
            embed_status="PENDING",
            previous_version_id=processing_state.previous_version_id,
            previous_manifest_s3_uri=processing_state.previous_manifest_s3_uri,
            latest_manifest_s3_uri=processing_state.latest_manifest_s3_uri,
            is_deleted=False,
            last_error="",
            updated_at=utc_now_iso(),
        )
        self._put_record(record)
        return record

    def mark_extract_running(self, source: S3ObjectRef) -> ObjectStateRecord:
        """
        EN: Mark the execution state as EXTRACTING.
        CN: 将 execution-state 标记为 EXTRACTING。
        """
        current_state = self.get_state(object_pk=source.object_pk)
        if current_state is not None and current_state.latest_version_id == source.version_id:
            if current_state.extract_status == "EXTRACTING":
                return current_state
            if current_state.extract_status not in {"QUEUED", "FAILED"}:
                raise DuplicateOrStaleEventError(source.document_uri)
        record = self._clone_state(
            source=source,
            current_state=current_state,
            extract_status="EXTRACTING",
            embed_status="PENDING",
            last_error="",
        )
        self._put_record(record)
        return record

    def mark_extract_done(
        self,
        source: S3ObjectRef,
        manifest_s3_uri: str,
        *,
        embed_status: str = "PENDING",
    ) -> ObjectStateRecord:
        """
        EN: Mark the extract stage complete and persist the manifest URI.
        CN: 将 extract 阶段标记为完成并持久化 manifest URI。
        """
        record = self._clone_state(
            source=source,
            current_state=self.get_state(object_pk=source.object_pk),
            extract_status="EXTRACTED",
            embed_status=embed_status,
            latest_manifest_s3_uri=manifest_s3_uri,
            last_error="",
        )
        self._put_record(record)
        return record

    def mark_extract_failed(self, source: S3ObjectRef, error_message: str) -> ObjectStateRecord:
        """
        EN: Mark the extract stage failed.
        CN: 将 extract 阶段标记为失败。
        """
        record = self._clone_state(
            source=source,
            current_state=self.get_state(object_pk=source.object_pk),
            extract_status="FAILED",
            embed_status="FAILED",
            last_error=error_message[:1000],
        )
        self._put_record(record)
        return record

    def mark_failed(self, source: S3ObjectRef, error_message: str) -> ObjectStateRecord:
        """
        EN: Alias for mark_extract_failed kept for workflow call sites.
        CN: 为工作流调用点保留的 mark_extract_failed 别名。
        """
        return self.mark_extract_failed(source, error_message)

    def mark_embed_running(self, source: S3ObjectRef) -> ObjectStateRecord:
        """
        EN: Mark the embed stage as running.
        CN: 将 embed 阶段标记为运行中。
        """
        record = self._clone_state(
            source=source,
            current_state=self.get_state(object_pk=source.object_pk),
            extract_status="EXTRACTED",
            embed_status="EMBEDDING",
            last_error="",
        )
        self._put_record(record)
        return record

    def mark_embed_done(self, source: S3ObjectRef) -> ObjectStateRecord:
        """
        EN: Mark the embed stage as complete.
        CN: 将 embed 阶段标记为完成。
        """
        record = self._clone_state(
            source=source,
            current_state=self.get_state(object_pk=source.object_pk),
            extract_status="EXTRACTED",
            embed_status="INDEXED",
            last_error="",
        )
        self._put_record(record)
        return record

    def mark_embed_failed(self, source: S3ObjectRef, error_message: str) -> ObjectStateRecord:
        """
        EN: Mark the embed stage as failed.
        CN: 将 embed 阶段标记为失败。
        """
        record = self._clone_state(
            source=source,
            current_state=self.get_state(object_pk=source.object_pk),
            extract_status="EXTRACTED",
            embed_status="FAILED",
            last_error=error_message[:1000],
        )
        self._put_record(record)
        return record

    def mark_embed_cleanup_failed(self, source: S3ObjectRef, error_message: str) -> ObjectStateRecord:
        """
        EN: Record a cleanup failure after the vectors have already been indexed.
        CN: 在向量已经索引完成后记录清理失败。
        """
        record = self._clone_state(
            source=source,
            current_state=self.get_state(object_pk=source.object_pk),
            extract_status="EXTRACTED",
            embed_status="INDEXED",
            last_error=error_message[:1000],
        )
        self._put_record(record)
        return record

    def _put_record(self, record: ObjectStateRecord) -> None:
        """
        EN: Persist one execution-state record to DynamoDB.
        CN: 将一条 execution-state 记录写入 DynamoDB。
        """
        self._ddb.put_item(TableName=self._table_name, Item=_serialize_execution_state(record))

    def _clone_state(
        self,
        *,
        source: S3ObjectRef,
        current_state: ObjectStateRecord | None,
        extract_status: str,
        embed_status: str,
        latest_manifest_s3_uri: str | None = None,
        last_error: str = "",
    ) -> ObjectStateRecord:
        """
        EN: Build a new execution-state record from the previous snapshot, preserving version lineage.
        CN: 基于上一份快照构建新的 execution-state 记录，并保留版本链路信息。
        """
        normalized_sequencer = _normalize_sequencer(source.sequencer)
        return ObjectStateRecord(
            pk=source.object_pk,
            latest_version_id=source.version_id,
            latest_sequencer=normalized_sequencer,
            extract_status=extract_status,  # type: ignore[arg-type]
            embed_status=embed_status,  # type: ignore[arg-type]
            previous_version_id=current_state.previous_version_id if current_state else None,
            previous_manifest_s3_uri=current_state.previous_manifest_s3_uri if current_state else None,
            latest_manifest_s3_uri=latest_manifest_s3_uri
            if latest_manifest_s3_uri is not None
            else (current_state.latest_manifest_s3_uri if current_state else None),
            is_deleted=False,
            last_error=last_error,
            updated_at=utc_now_iso(),
        )


def _build_ingest_processing_state(
    *,
    source: S3ObjectRef,
    current_state: ObjectStateRecord | None,
) -> ObjectStateRecord:
    """
    EN: Build the queued execution-state snapshot for one source object.
    CN: 为单个源对象构建 queued execution-state 快照。
    """
    previous_version_id = current_state.latest_version_id if current_state is not None else None
    previous_manifest_s3_uri = current_state.latest_manifest_s3_uri if current_state is not None else None
    return ObjectStateRecord(
        pk=source.object_pk,
        latest_version_id=source.version_id,
        latest_sequencer=_normalize_sequencer(source.sequencer),
        extract_status="QUEUED",
        embed_status="PENDING",
        previous_version_id=previous_version_id,
        previous_manifest_s3_uri=previous_manifest_s3_uri,
        latest_manifest_s3_uri=None,
        is_deleted=False,
        last_error="",
    )


def _serialize_execution_state(record: ObjectStateRecord) -> dict[str, dict[str, str | bool]]:
    """
    EN: Serialize an execution-state record into a DynamoDB item dictionary.
    CN: 将 execution-state 记录序列化为 DynamoDB item 字典。
    """
    item: dict[str, dict[str, str | bool]] = {
        "pk": {"S": record.pk},
        "latest_version_id": {"S": record.latest_version_id},
        "extract_status": {"S": record.extract_status},
        "embed_status": {"S": record.embed_status},
        "updated_at": {"S": record.updated_at},
        "is_deleted": {"BOOL": record.is_deleted},
    }
    if record.latest_sequencer is not None:
        item["latest_sequencer"] = {"S": record.latest_sequencer}
    if record.previous_version_id is not None:
        item["previous_version_id"] = {"S": record.previous_version_id}
    if record.previous_manifest_s3_uri is not None:
        item["previous_manifest_s3_uri"] = {"S": record.previous_manifest_s3_uri}
    if record.latest_manifest_s3_uri is not None:
        item["latest_manifest_s3_uri"] = {"S": record.latest_manifest_s3_uri}
    if record.last_error is not None:
        item["last_error"] = {"S": record.last_error}
    return item


def _deserialize_execution_state(item: dict[str, dict[str, str | bool]]) -> ObjectStateRecord:
    """
    EN: Deserialize a DynamoDB item into an execution-state domain model.
    CN: 将 DynamoDB item 反序列化为 execution-state 领域模型。
    """
    return ObjectStateRecord(
        pk=item["pk"]["S"],  # type: ignore[index,arg-type]
        latest_version_id=item["latest_version_id"]["S"],  # type: ignore[index,arg-type]
        latest_sequencer=item.get("latest_sequencer", {}).get("S"),  # type: ignore[index,arg-type]
        extract_status=item["extract_status"]["S"],  # type: ignore[index,arg-type]
        embed_status=item["embed_status"]["S"],  # type: ignore[index,arg-type]
        previous_version_id=item.get("previous_version_id", {}).get("S"),  # type: ignore[index,arg-type]
        previous_manifest_s3_uri=item.get("previous_manifest_s3_uri", {}).get("S"),  # type: ignore[index,arg-type]
        latest_manifest_s3_uri=item.get("latest_manifest_s3_uri", {}).get("S"),  # type: ignore[index,arg-type]
        is_deleted=bool(item.get("is_deleted", {}).get("BOOL", False)),  # type: ignore[index,arg-type]
        last_error=item.get("last_error", {}).get("S"),  # type: ignore[index,arg-type]
        updated_at=item["updated_at"]["S"],  # type: ignore[index,arg-type]
    )
