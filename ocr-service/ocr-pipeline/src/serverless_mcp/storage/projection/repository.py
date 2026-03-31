"""
EN: Repository for per-profile embedding projection state tracked in DynamoDB.
CN: 同上。

Each document version may have multiple embedding_profiles; this repository isolates write
and query readiness per profile so that model upgrades, dimension changes, and index
migrations do not interfere with each other.
同上。
浣挎ā鍨嬪崌绾с€佺淮搴﹀彉鏇村拰绱㈠紩杩佺Щ浜掍笉骞叉壈銆?
"""
from __future__ import annotations

from urllib.parse import quote

from serverless_mcp.domain.models import EmbeddingOutcome, EmbeddingProfile, EmbeddingProjectionStateRecord, S3ObjectRef, utc_now_iso
from serverless_mcp.storage.batch import batch_get_records, flush_batch_write


class EmbeddingProjectionStateRepository:
    """
    EN: Track write and query readiness for each document version and embedding profile pair.
    CN: 同上。

    This repository manages the embedding_projection_state table, enforcing that vectors
    for different providers, models, or dimensions never share the same S3 Vectors index.
    同上。
    鐨勫悜閲忔案杩滀笉浼氬叡鐢ㄥ悓涓€涓?S3 Vectors index銆?
    """

    def __init__(self, *, table_name: str, dynamodb_client: object) -> None:
        # EN: Table name for the embedding_projection_state DynamoDB table.
        # CN: embedding_projection_state DynamoDB 琛ㄧ殑琛ㄥ悕銆?
        self._table_name = table_name
        # EN: Boto3 DynamoDB client for conditional writes and consistent reads.
        # CN: 同上。
        self._ddb = dynamodb_client

    def mark_running(self, *, source: S3ObjectRef, profile: EmbeddingProfile, manifest_s3_uri: str) -> EmbeddingProjectionStateRecord:
        """
        EN: Mark one profile projection as running before vector persistence starts.
        CN: 同上。

        Args:
            source:
                EN: S3 object reference containing bucket, key, version_id, and tenant identity.
                CN: 同上。
            profile:
                EN: Embedding profile specifying provider, model, dimension, and vector index.
                CN: 鎸囧畾 provider銆乵odel銆乨imension 鍜?vector index 鐨?embedding profile銆?
            manifest_s3_uri:
                EN: S3 URI of the manifest that will be embedded.
                CN: 鍗冲皢琚?embed 鐨?manifest 鐨?S3 URI銆?

        Returns:
            EN: The newly created projection state record with write_status=EMBEDDING.
            CN: 鏂板垱寤虹殑 projection state 璁板綍锛寃rite_status=EMBEDDING銆?
        """
        record = self._build_record(
            source=source,
            profile=profile,
            write_status="EMBEDDING",
            query_status="PENDING",
            manifest_s3_uri=manifest_s3_uri,
            vector_count=None,
            last_error="",
        )
        self._put_record(record)
        return record

    def get_state(
        self,
        *,
        object_pk: str,
        version_id: str,
        profile_id: str,
    ) -> EmbeddingProjectionStateRecord | None:
        """
        EN: Load one profile projection state for one document version.
        CN: 璇诲彇鍗曚釜鏂囨。鐗堟湰鍦ㄥ崟涓?profile 涓嬬殑 projection 鐘舵€併€?

        Args:
            object_pk:
                EN: Primary key of the source object (tenant_id#bucket#key).
                CN: 同上。
            version_id:
                EN: S3 version_id to look up.
                CN: 瑕佹煡璇㈢殑 S3 version_id銆?
            profile_id:
                EN: Identifier of the embedding profile to look up.
                CN: 瑕佹煡璇㈢殑 embedding profile 鏍囪瘑绗︺€?

        Returns:
            EN: The projection state record if it exists, otherwise None.
            CN: 瀛樺湪鏃惰繑鍥?projection state 璁板綍锛屽惁鍒欒繑鍥?None銆?
        """
        response = self._ddb.get_item(
            TableName=self._table_name,
            Key={
                "pk": {"S": f"{object_pk}#{quote(version_id, safe='')}"},
                "sk": {"S": profile_id},
            },
            ConsistentRead=True,
        )
        item = response.get("Item")
        if not item:
            return None
        return _deserialize_projection_state(item)

    def get_states_batch(
        self,
        *,
        keys: list[tuple[str, str, str]],
    ) -> dict[tuple[str, str, str], EmbeddingProjectionStateRecord | None]:
        """
        EN: Load multiple projection states in batches keyed by object/version/profile triplets.
        CN: 按 object/version/profile 三元组批量加载多个 projection state。
        """
        if not keys:
            return {}

        return batch_get_records(
            self._ddb,
            table_name=self._table_name,
            items=keys,
            build_request_key=lambda key: {
                "pk": {"S": _build_projection_pk(object_pk=key[0], version_id=key[1])},
                "sk": {"S": key[2]},
            },
            parse_request_key=lambda item: _parse_projection_key(item["pk"]["S"], item["sk"]["S"]),
            parse_record_key=lambda record: (record.object_pk, record.version_id, record.profile_id),
            parse_record=_deserialize_projection_state,
        )

    def list_version_records(self, *, object_pk: str, version_id: str) -> list[EmbeddingProjectionStateRecord]:
        """
        EN: Load all profile projection states for one document version.
        CN: 璇诲彇鍗曚釜鏂囨。鐗堟湰涓嬫墍鏈?profile projection 鐘舵€併€?

        Args:
            object_pk:
                EN: Primary key of the source object.
                CN: 同上。
            version_id:
                EN: S3 version_id whose projections to list.
                CN: 瑕佸垪鍑?projection 鐨?S3 version_id銆?

        Returns:
            EN: List of projection state records across all embedding profiles.
            CN: 璺ㄦ墍鏈?embedding profile 鐨?projection state 璁板綍鍒楄〃銆?
        """
        return self._load_version_records(object_pk=object_pk, version_id=version_id)

    def mark_done(self, *, outcome: EmbeddingOutcome, profile: EmbeddingProfile) -> EmbeddingProjectionStateRecord:
        """
        EN: Mark one profile projection as queryable after vectors have been written successfully.
        CN: 同上。

        Args:
            outcome:
                EN: Embedding outcome containing source, manifest_s3_uri, and vector_count.
                CN: 鍖呭惈 source銆乵anifest_s3_uri 鍜?vector_count 鐨?embedding 缁撴灉銆?
            profile:
                EN: Embedding profile that completed vector persistence.
                CN: 瀹屾垚鍚戦噺鎸佷箙鍖栫殑 embedding profile銆?

        Returns:
            EN: The updated projection state record with write_status=INDEXED and query_status=INDEXED.
            CN: 鏇存柊鍚庣殑 projection state 璁板綍锛寃rite_status=INDEXED 涓?query_status=INDEXED銆?
        """
        record = self._build_record(
            source=outcome.source,
            profile=profile,
            write_status="INDEXED",
            query_status="INDEXED",
            manifest_s3_uri=outcome.manifest_s3_uri,
            vector_count=outcome.vector_count,
            last_error="",
        )
        self._put_record(record)
        return record

    def mark_failed(
        self,
        *,
        source: S3ObjectRef,
        profile: EmbeddingProfile,
        manifest_s3_uri: str,
        error_message: str,
    ) -> EmbeddingProjectionStateRecord:
        """
        EN: Mark one profile projection as failed while keeping document version identity intact.
        CN: 同上。

        Args:
            source:
                EN: S3 object reference for the failed embedding attempt.
                CN: 澶辫触鐨?embedding 灏濊瘯瀵瑰簲鐨?S3 瀵硅薄寮曠敤銆?
            profile:
                EN: Embedding profile that encountered the failure.
                CN: 閬囧埌澶辫触鐨?embedding profile銆?
            manifest_s3_uri:
                EN: S3 URI of the manifest that was being embedded.
                CN: 姝ｅ湪琚?embed 鐨?manifest 鐨?S3 URI銆?
            error_message:
                EN: Error description, truncated to 1000 characters.
                CN: 同上。

        Returns:
            EN: The updated projection state record with write_status=FAILED.
            CN: 鏇存柊鍚庣殑 projection state 璁板綍锛寃rite_status=FAILED銆?
        """
        record = self._build_record(
            source=source,
            profile=profile,
            write_status="FAILED",
            query_status="FAILED",
            manifest_s3_uri=manifest_s3_uri,
            vector_count=None,
            last_error=error_message[:1000],
        )
        self._put_record(record)
        return record

    def mark_deleted(
        self,
        *,
        source: S3ObjectRef,
        profile: EmbeddingProfile,
        manifest_s3_uri: str,
        error_message: str = "source object deleted",
    ) -> EmbeddingProjectionStateRecord:
        """
        EN: Mark one profile projection deleted so query readiness no longer depends only on object_state fallback.
        CN: 同上。

        Args:
            source:
                EN: S3 object reference for the deleted source object.
                CN: 宸插垹闄ゆ簮瀵硅薄鐨?S3 瀵硅薄寮曠敤銆?
            profile:
                EN: Embedding profile whose projection is being marked deleted.
                CN: 瑕佹爣璁板垹闄ょ殑 embedding profile銆?
            manifest_s3_uri:
                EN: S3 URI of the last known manifest for this profile.
                CN: 璇?profile 鏈€鍚庡凡鐭?manifest 鐨?S3 URI銆?
            error_message:
                EN: Optional deletion reason, defaults to "source object deleted".
                CN: 同上。

        Returns:
            EN: The updated projection state record with write_status=DELETED.
            CN: 鏇存柊鍚庣殑 projection state 璁板綍锛寃rite_status=DELETED銆?
        """
        record = self._build_record(
            source=source,
            profile=profile,
            write_status="DELETED",
            query_status="DELETED",
            manifest_s3_uri=manifest_s3_uri,
            vector_count=None,
            last_error=error_message[:1000],
        )
        self._put_record(record)
        return record

    def delete_version_records(self, *, source: S3ObjectRef, version_id: str) -> None:
        """
        EN: Delete all projection state records for one document version when a newer version is replacing it.
        CN: 同上。

        Args:
            source:
                EN: S3 object reference used to derive the object_pk.
                CN: 同上。
            version_id:
                EN: Old version_id whose projection records should be removed.
                CN: 瑕佸垹闄ゅ叾 projection 璁板綍鐨勬棫 version_id銆?
        """
        # EN: Load all profile records for this version before deleting.
        # CN: 同上。
        records = self._load_version_records(object_pk=source.object_pk, version_id=version_id)
        if not records:
            return
        # EN: Delete in batches of 25 to respect DynamoDB batch_write_item limit.
        # CN: 浠?25 鏉′负涓€鎵瑰垹闄わ紝浠ラ伒瀹?DynamoDB batch_write_item 闄愬埗銆?
        for batch in _chunked(records, 25):
            request_items = {
                self._table_name: [
                    {
                        "DeleteRequest": {
                            "Key": {
                                "pk": {"S": record.pk},
                                "sk": {"S": record.sk},
                            }
                        }
                    }
                    for record in batch
                ]
            }
            self._flush_batch_write(request_items)

    def _build_record(
        self,
        *,
        source: S3ObjectRef,
        profile: EmbeddingProfile,
        write_status: str,
        query_status: str,
        manifest_s3_uri: str,
        vector_count: int | None,
        last_error: str,
    ) -> EmbeddingProjectionStateRecord:
        """
        EN: Build an EmbeddingProjectionStateRecord with composite key and profile metadata.
        CN: 同上。

        Args:
            source:
                EN: S3 object reference for deriving the partition key.
                CN: 同上。
            profile:
                EN: Embedding profile supplying provider, model, dimension, and vector index.
                CN: 鎻愪緵 provider銆乵odel銆乨imension 鍜?vector index 鐨?embedding profile銆?
            write_status:
                EN: Write lifecycle status (EMBEDDING, INDEXED, FAILED, DELETED).
                CN: 鍐欏叆鐢熷懡鍛ㄦ湡鐘舵€侊紙EMBEDDING銆両NDEXED銆丗AILED銆丏ELETED锛夈€?
            query_status:
                EN: Query readiness status mirroring write_status lifecycle.
                CN: 同上。
            manifest_s3_uri:
                EN: S3 URI of the manifest being or already embedded.
                CN: 姝ｅ湪鎴栧凡琚?embed 鐨?manifest 鐨?S3 URI銆?
            vector_count:
                EN: Number of vectors written, None when not yet indexed.
                CN: 同上。
            last_error:
                EN: Truncated error message or empty string on success.
                CN: 同上。

        Returns:
            EN: Fully populated EmbeddingProjectionStateRecord.
            CN: 同上。
        """
        pk = source.version_pk
        sk = profile.profile_id
        return EmbeddingProjectionStateRecord(
            pk=pk,
            sk=sk,
            object_pk=source.object_pk,
            version_id=source.version_id,
            profile_id=profile.profile_id,
            provider=profile.provider,
            model=profile.model,
            dimension=profile.dimension,
            write_status=write_status,  # type: ignore[arg-type]
            query_status=query_status,  # type: ignore[arg-type]
            manifest_s3_uri=manifest_s3_uri,
            vector_bucket_name=profile.vector_bucket_name,
            vector_index_name=profile.vector_index_name,
            vector_count=vector_count,
            last_error=last_error,
            updated_at=utc_now_iso(),
        )

    def _put_record(self, record: EmbeddingProjectionStateRecord) -> None:
        """
        EN: Persist one projection state record to DynamoDB, including optional fields when present.
        CN: 灏嗗崟涓?projection state 璁板綍鎸佷箙鍖栧埌 DynamoDB锛屽瓨鍦ㄥ彲閫夊瓧娈垫椂涓€骞跺啓鍏ャ€?

        Args:
            record:
                EN: Projection state record to upsert by composite (pk, sk) key.
                CN: 同上。
        """
        item = {
            "pk": {"S": record.pk},
            "sk": {"S": record.sk},
            "object_pk": {"S": record.object_pk},
            "version_id": {"S": record.version_id},
            "profile_id": {"S": record.profile_id},
            "provider": {"S": record.provider},
            "model": {"S": record.model},
            "dimension": {"N": str(record.dimension)},
            "write_status": {"S": record.write_status},
            "query_status": {"S": record.query_status},
            "updated_at": {"S": record.updated_at},
        }
        if record.manifest_s3_uri:
            item["manifest_s3_uri"] = {"S": record.manifest_s3_uri}
        if record.vector_bucket_name:
            item["vector_bucket_name"] = {"S": record.vector_bucket_name}
        if record.vector_index_name:
            item["vector_index_name"] = {"S": record.vector_index_name}
        if record.vector_count is not None:
            item["vector_count"] = {"N": str(record.vector_count)}
        if record.last_error is not None:
            item["last_error"] = {"S": record.last_error}

        self._ddb.put_item(TableName=self._table_name, Item=item)

    def _load_version_records(self, *, object_pk: str, version_id: str) -> list[EmbeddingProjectionStateRecord]:
        # EN: Query all projection state records for one object version using strong consistency.
        # CN: 同上。
        response = self._ddb.query(
            TableName=self._table_name,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": f"{object_pk}#{quote(version_id, safe='')}"}},
            ConsistentRead=True,
        )
        return [_deserialize_projection_state(item) for item in response.get("Items") or []]

    def _flush_batch_write(self, request_items: dict[str, list[dict]]) -> None:
        # EN: Delegate to the shared batch-write helper with exponential backoff retry.
        # CN: 濮旀墭缁欏甫鎸囨暟閫€閬块噸璇曠殑鍏变韩 batch write 杈呭姪鍑芥暟銆?
        flush_batch_write(self._ddb, request_items)


def _chunked(items: list[EmbeddingProjectionStateRecord], size: int) -> list[list[EmbeddingProjectionStateRecord]]:
    # EN: Split a list into fixed-size batches for DynamoDB batch_write_item calls.
    # CN: 灏嗗垪琛ㄦ媶鍒嗕负鍥哄畾澶у皬鐨勬壒娆★紝鐢ㄤ簬 DynamoDB batch_write_item 璋冪敤銆?
    return [items[index : index + size] for index in range(0, len(items), size)]


def _build_projection_pk(*, object_pk: str, version_id: str) -> str:
    """
    EN: Build the composite partition key used by projection state records.
    CN: 构造 projection state record 使用的复合 partition key。
    """
    return f"{object_pk}#{quote(version_id, safe='')}"


def _parse_projection_key(pk: str, sk: str) -> tuple[str, str, str]:
    """
    EN: Decode a projection-state composite key back into object/version/profile parts.
    CN: 将 projection-state 复合 key 解码回 object/version/profile 三部分。
    """
    object_pk, version_id = pk.rsplit("#", 1)
    return object_pk, version_id, sk


def _deserialize_projection_state(item: dict[str, dict[str, str | bool]]) -> EmbeddingProjectionStateRecord:
    """
    EN: Deserialize a DynamoDB item into an EmbeddingProjectionStateRecord domain model.
    CN: 灏?DynamoDB 鏉＄洰鍙嶅簭鍒楀寲涓?EmbeddingProjectionStateRecord 棰嗗煙妯″瀷銆?

    Args:
        item:
            EN: Raw DynamoDB item with typed attribute values.
            CN: 同上。

    Returns:
        EN: Populated EmbeddingProjectionStateRecord instance.
        CN: 同上。
    """
    return EmbeddingProjectionStateRecord(
        pk=item["pk"]["S"],  # type: ignore[index]
        sk=item["sk"]["S"],  # type: ignore[index]
        object_pk=item["object_pk"]["S"],  # type: ignore[index]
        version_id=item["version_id"]["S"],  # type: ignore[index]
        profile_id=item["profile_id"]["S"],  # type: ignore[index]
        provider=item["provider"]["S"],  # type: ignore[index,arg-type]
        model=item["model"]["S"],  # type: ignore[index]
        dimension=int(item["dimension"]["N"]),  # type: ignore[index]
        write_status=item["write_status"]["S"],  # type: ignore[index,arg-type]
        query_status=item["query_status"]["S"],  # type: ignore[index,arg-type]
        manifest_s3_uri=item.get("manifest_s3_uri", {}).get("S"),  # type: ignore[union-attr]
        vector_bucket_name=item.get("vector_bucket_name", {}).get("S"),  # type: ignore[union-attr]
        vector_index_name=item.get("vector_index_name", {}).get("S"),  # type: ignore[union-attr]
        vector_count=int(item["vector_count"]["N"]) if "vector_count" in item else None,  # type: ignore[index]
        last_error=item.get("last_error", {}).get("S"),  # type: ignore[union-attr]
        updated_at=item.get("updated_at", {}).get("S", ""),  # type: ignore[union-attr]
    )
