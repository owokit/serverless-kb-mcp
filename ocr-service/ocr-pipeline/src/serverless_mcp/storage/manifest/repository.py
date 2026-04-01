"""
EN: Manifest repository for persisting chunk manifests and assets to S3 with DynamoDB indexing.
CN: 鐢ㄤ簬灏?chunk manifest 鍜岃祫浜ф寔涔呭寲鍒?S3 骞朵娇鐢?DynamoDB 寤虹储寮曠殑 manifest 浠撳簱銆?

This repository writes manifests to S3 first, then indexes chunk records in DynamoDB.
On index failure it rolls back both S3 writes and DynamoDB records to avoid half-success.
同上。
S3 鍐欏叆鍜?DynamoDB 璁板綍锛岄伩鍏嶅崐鎴愬姛鐘舵€併€?
"""
from __future__ import annotations

import json
import mimetypes
import re
from dataclasses import asdict, replace
from urllib.parse import quote

from botocore.exceptions import ClientError

from serverless_mcp.domain.manifest_schema import validate_chunk_manifest
from serverless_mcp.domain.models import (
    ChunkManifest,
    ChunkManifestRecord,
    ExtractedAsset,
    ExtractedChunk,
    PersistedManifest,
    S3ObjectRef,
)
from serverless_mcp.storage.batch import flush_batch_write
from serverless_mcp.storage.paths import build_asset_key, build_manifest_key, build_s3_uri, parse_s3_uri


_MANIFEST_PERSIST_FAILURE_TYPES = (ClientError, KeyError, OSError, RuntimeError, TypeError, ValueError)
_TEXT_PREVIEW_LIMIT = 512


class ManifestRepository:
    """
    EN: Persist manifests to S3 and maintain DynamoDB index for chunk lookup and neighbor navigation.
    CN: 同上。

    The repository enforces that S3 and DynamoDB stay in sync: new manifest writes are
    rolled back if the corresponding index write fails.
    同上。
    """

    def __init__(
        self,
        *,
        manifest_bucket: str,
        manifest_prefix: str,
        s3_client: object,
        dynamodb_client: object,
        manifest_index_table: str,
    ) -> None:
        # EN: S3 bucket where manifests and assets are stored.
        # CN: 瀛樺偍 manifest 鍜岃祫浜х殑 S3 妗躲€?
        self._manifest_bucket = manifest_bucket
        # EN: Common prefix prepended to all manifest keys.
        # CN: 同上。
        self._manifest_prefix = manifest_prefix.strip("/")
        # EN: Boto3 S3 client for object operations.
        # CN: 同上。
        self._s3 = s3_client
        # EN: Boto3 DynamoDB client for batch writes and consistent reads.
        # CN: 同上。
        self._ddb = dynamodb_client
        # EN: DynamoDB table name for manifest_index chunk records.
        # CN: manifest_index chunk 璁板綍鐨?DynamoDB 琛ㄥ悕銆?
        self._manifest_index_table = manifest_index_table

    def persist_manifest(
        self,
        manifest: ChunkManifest,
        *,
        previous_version_id: str | None = None,
    ) -> PersistedManifest:
        """
        EN: Write S3 first, then index; rollback new writes on index failure to avoid manifest/index half-success.
        CN: 鍏堝啓 S3锛屽啀鍐欑储寮曪紱鑻ョ储寮曞け璐ュ垯鍥炴粴鏂板啓鍏ワ紝閬垮厤 manifest/index 鍗婃垚鍔熴€?

        Args:
            manifest:
                EN: Validated chunk manifest containing source identity, chunks, and assets.
                CN: 同上。
            previous_version_id:
                EN: Optional old version_id whose index records should be demoted to is_latest=False.
                CN: 同上。

        Returns:
            EN: PersistedManifest containing the manifest and its version-aware S3 URI.
            CN: 同上。

        Raises:
            EN: ValueError if the manifest fails schema validation.
            CN: 同上。
        """
        validate_chunk_manifest(manifest)

        # EN: Track created S3 objects for potential rollback.
        # CN: 璺熻釜宸插垱寤虹殑 S3 瀵硅薄锛屼互渚垮彲鑳界殑鍥炴粴銆?
        created_s3_objects: list[tuple[str, str, str | None]] = []
        previous_latest_records: list[ChunkManifestRecord] = []
        current_records: list[ChunkManifestRecord] = []

        # EN: Persist each asset with inline payload to S3.
        # CN: 同上。
        persisted_assets: list[ExtractedAsset] = []
        for asset in manifest.assets:
            persisted_asset, created_object = self._persist_asset(manifest, asset)
            persisted_assets.append(persisted_asset)
            if created_object is not None:
                created_s3_objects.append(created_object)

        persisted_manifest = replace(manifest, assets=persisted_assets)
        manifest_key = self._build_manifest_key(manifest)
        body = json.dumps(asdict(persisted_manifest), ensure_ascii=False).encode("utf-8")
        manifest_put = self._s3.put_object(
            Bucket=self._manifest_bucket,
            Key=manifest_key,
            Body=body,
            ContentType="application/json",
        )
        manifest_version_id = _extract_version_id(manifest_put)
        created_s3_objects.append((self._manifest_bucket, manifest_key, manifest_version_id))
        manifest_s3_uri = build_s3_uri(self._manifest_bucket, manifest_key, version_id=manifest_version_id)

        try:
            # EN: Index new chunks as is_latest=True in DynamoDB.
            # CN: 同上。
            current_records = self._build_manifest_index_records(persisted_manifest, manifest_s3_uri, is_latest=True)
            self._put_manifest_records(current_records)

            # EN: Demote previous version's records to is_latest=False if version_id differs.
            # CN: 鑻?version_id 涓嶅悓锛屽垯灏嗘棫鐗堟湰璁板綍闄嶇骇涓?is_latest=False銆?
            if previous_version_id and previous_version_id != manifest.source.version_id:
                previous_latest_records = self._load_version_records(
                    source=manifest.source,
                    version_id=previous_version_id,
                )
                if previous_latest_records:
                    self._put_manifest_records([replace(record, is_latest=False) for record in previous_latest_records])
        except _MANIFEST_PERSIST_FAILURE_TYPES:
            # EN: Rollback: delete newly created index records and restore previous records.
            # CN: 同上。
            if current_records:
                self._delete_manifest_records(current_records)
            if previous_latest_records:
                self._put_manifest_records(previous_latest_records)
            # EN: Rollback: delete S3 objects created during this persist operation.
            # CN: 鍥炴粴锛氬垹闄ゆ湰娆℃寔涔呭寲鎿嶄綔鍒涘缓鐨?S3 瀵硅薄銆?
            self._delete_s3_objects(created_s3_objects)
            raise

        return PersistedManifest(manifest=persisted_manifest, manifest_s3_uri=manifest_s3_uri)

    def rollback_manifest(
        self,
        manifest: ChunkManifest,
        *,
        manifest_s3_uri: str,
        previous_version_id: str | None = None,
    ) -> None:
        """
        EN: Best-effort rollback for a previously persisted manifest and its index records.
        CN: 对已持久化的 manifest 及其索引记录执行尽力回滚。
        """
        current_records = self._load_version_records(source=manifest.source, version_id=manifest.source.version_id)
        previous_latest_records: list[ChunkManifestRecord] = []
        if previous_version_id and previous_version_id != manifest.source.version_id:
            previous_latest_records = self._load_version_records(source=manifest.source, version_id=previous_version_id)
        if current_records:
            self._delete_manifest_records(current_records)
        if previous_latest_records:
            self._put_manifest_records(previous_latest_records)

        created_s3_objects: list[tuple[str, str, str | None]] = []
        manifest_bucket, manifest_key, manifest_version_id = parse_s3_uri(manifest_s3_uri)
        created_s3_objects.append((manifest_bucket, manifest_key, manifest_version_id))
        for asset in manifest.assets:
            if not asset.asset_s3_uri:
                continue
            asset_bucket, asset_key, asset_version_id = parse_s3_uri(asset.asset_s3_uri)
            created_s3_objects.append((asset_bucket, asset_key, asset_version_id))
        self._delete_s3_objects(created_s3_objects)

    def load_manifest(self, manifest_s3_uri: str) -> ChunkManifest:
        """
        EN: Load chunk manifest from S3 URI for version governance and neighbor expansion.
        CN: 同上。

        Args:
            manifest_s3_uri:
                EN: Version-aware S3 URI pointing to the manifest.json.
                CN: 鎸囧悜 manifest.json 鐨勫甫鐗堟湰淇℃伅鐨?S3 URI銆?

        Returns:
            EN: Deserialized and validated ChunkManifest.
            CN: 宸插弽搴忓垪鍖栧苟楠岃瘉鐨?ChunkManifest銆?

        Raises:
            EN: ClientError if the S3 object does not exist or access is denied.
            CN: 同上。
            EN: ValueError if the manifest fails schema validation.
            CN: 同上。
        """
        bucket, key, version_id = parse_s3_uri(manifest_s3_uri)
        kwargs = {"Bucket": bucket, "Key": key}
        if version_id:
            kwargs["VersionId"] = version_id
        response = self._s3.get_object(**kwargs)
        payload = json.loads(response["Body"].read().decode("utf-8"))
        source_payload = payload["source"]
        manifest = ChunkManifest(
            source=S3ObjectRef(**source_payload),
            doc_type=payload["doc_type"],
            chunks=[
                ExtractedChunk(
                    chunk_id=chunk["chunk_id"],
                    chunk_type=chunk["chunk_type"],
                    text=chunk["text"],
                    doc_type=chunk["doc_type"],
                    token_estimate=chunk["token_estimate"],
                    page_no=chunk.get("page_no"),
                    page_span=tuple(chunk["page_span"]) if chunk.get("page_span") else None,
                    slide_no=chunk.get("slide_no"),
                    section_path=tuple(chunk.get("section_path") or ()),
                    metadata=chunk.get("metadata") or {},
                )
                for chunk in payload["chunks"]
            ],
            assets=[
                ExtractedAsset(
                    asset_id=asset["asset_id"],
                    chunk_type=asset["chunk_type"],
                    mime_type=asset["mime_type"],
                    payload=None,
                    asset_s3_uri=asset.get("asset_s3_uri"),
                    page_no=asset.get("page_no"),
                    slide_no=asset.get("slide_no"),
                    metadata=asset.get("metadata") or {},
                )
                for asset in payload.get("assets") or []
            ],
            metadata=payload.get("metadata") or {},
        )
        validate_chunk_manifest(manifest)
        return manifest

    def find_manifest_s3_uri(self, *, source: S3ObjectRef, version_id: str) -> str | None:
        """
        EN: Resolve the exact manifest S3 URI recorded for one source version.
        CN: 同上。

        Args:
            source:
                EN: S3 object reference used to derive the partition key.
                CN: 同上。
            version_id:
                EN: S3 version_id to look up in the manifest_index table.
                CN: 瑕佸湪 manifest_index 琛ㄤ腑鏌ユ壘鐨?S3 version_id銆?

        Returns:
            EN: The manifest S3 URI if recorded, otherwise None.
            CN: 鑻ュ凡璁板綍鍒欒繑鍥?manifest S3 URI锛屽惁鍒欒繑鍥?None銆?
        """
        records = self._load_version_records(source=source, version_id=version_id)
        for record in records:
            if record.manifest_s3_uri:
                return record.manifest_s3_uri
        return None

    def list_version_records(self, *, source: S3ObjectRef, version_id: str) -> list[ChunkManifestRecord]:
        """
        EN: Return all manifest index records for one source version.
        CN: 返回某个源版本的全部 manifest 索引记录。
        """
        return self._load_version_records(source=source, version_id=version_id)

    def delete_previous_version_artifacts(
        self,
        *,
        source: S3ObjectRef,
        previous_version_id: str | None,
        previous_manifest_s3_uri: str | None = None,
    ) -> None:
        """
        EN: Remove the previous version's manifest objects and manifest index records after the new version is durably written.
        CN: 鍦ㄦ柊鐗堟湰宸茶惤鍦板苟瀹屾垚娌荤悊妫€鏌ュ悗锛屽垹闄ゆ棫鐗堟湰鐨?manifest 瀵硅薄鍜?manifest_index 璁板綍銆?

        Args:
            source:
                EN: Current S3 object reference for identity resolution.
                CN: 同上。
            previous_version_id:
                EN: Old version_id whose artifacts should be cleaned up.
                CN: 瑕佹竻鐞嗗叾浜х墿鐨勬棫 version_id銆?
            previous_manifest_s3_uri:
                EN: Optional known manifest S3 URI to avoid extra DynamoDB lookup.
                CN: 同上。
        """
        if not previous_version_id or previous_version_id == source.version_id:
            return

        candidate_manifest_s3_uri = previous_manifest_s3_uri or self.find_manifest_s3_uri(
            source=source,
            version_id=previous_version_id,
        )
        if candidate_manifest_s3_uri is None:
            return

        manifest = self._load_manifest_if_present(candidate_manifest_s3_uri)
        if manifest is not None:
            objects = self._collect_manifest_s3_objects(candidate_manifest_s3_uri, manifest)
            self._delete_s3_objects(objects)

        previous_records = self._load_version_records(source=source, version_id=previous_version_id)
        if previous_records:
            self._delete_manifest_records(previous_records)

    def build_manifest_s3_uri(self, *, source: S3ObjectRef, version_id: str) -> str:
        """
        EN: Build the deterministic manifest S3 URI for one document version.
        CN: 同上。

        Args:
            source:
                EN: S3 object reference used to derive the manifest path.
                CN: 同上。
            version_id:
                EN: S3 version_id to include in the URI query string.
                CN: 同上。

        Returns:
            EN: Version-aware S3 URI for the manifest.
            CN: manifest 鐨勫甫鐗堟湰淇℃伅鐨?S3 URI銆?
        """
        candidate = self.find_manifest_s3_uri(source=source, version_id=version_id)
        if candidate:
            return candidate
        manifest_key = self._build_manifest_key(source=source)
        return build_s3_uri(self._manifest_bucket, manifest_key)

    def _persist_asset(
        self,
        manifest: ChunkManifest,
        asset: ExtractedAsset,
    ) -> tuple[ExtractedAsset, tuple[str, str, str | None] | None]:
        """
        EN: Persist one asset's inline payload to S3 and return the updated asset with its S3 URI.
        CN: 灏嗗崟涓?asset 鐨勫唴鑱?payload 鎸佷箙鍖栧埌 S3锛屽苟杩斿洖鏇存柊鍚庣殑 asset 鍙婂叾 S3 URI銆?

        Args:
            manifest:
                EN: Parent chunk manifest used to derive the asset S3 key.
                CN: 同上。
            asset:
                EN: Extracted asset that may contain inline payload bytes.
                CN: 同上。

        Returns:
            EN: Tuple of (updated_asset, created_object) where created_object is None when no S3 write occurred.
            CN: 同上。
        """
        if asset.asset_s3_uri:
            return replace(asset, payload=None), None

        if asset.payload is None:
            return asset, None

        asset_key = self._build_asset_key(manifest, asset)
        put_response = self._s3.put_object(
            Bucket=self._manifest_bucket,
            Key=asset_key,
            Body=asset.payload,
            ContentType=asset.mime_type,
        )
        version_id = _extract_version_id(put_response)
        asset_s3_uri = build_s3_uri(self._manifest_bucket, asset_key, version_id=version_id)
        return replace(asset, payload=None, asset_s3_uri=asset_s3_uri), (self._manifest_bucket, asset_key, version_id)

    def _collect_manifest_s3_objects(
        self,
        manifest_s3_uri: str,
        manifest: ChunkManifest,
    ) -> list[tuple[str, str, str | None]]:
        """
        EN: Collect all S3 objects (manifest + assets) that belong to one manifest version.
        CN: 鏀堕泦灞炰簬鏌愪釜 manifest 鐗堟湰鐨勬墍鏈?S3 瀵硅薄锛坢anifest + assets锛夈€?

        Args:
            manifest_s3_uri:
                EN: Version-aware S3 URI of the manifest itself.
                CN: 同上。
            manifest:
                EN: Deserialized chunk manifest whose assets will be enumerated.
                CN: 同上。

        Returns:
            EN: List of (bucket, key, version_id) tuples for deletion during rollback or cleanup.
            CN: 鐢ㄤ簬鍥炴粴鎴栨竻鐞嗘椂鍒犻櫎鐨?(bucket, key, version_id) 鍏冪粍鍒楄〃銆?
        """
        objects: list[tuple[str, str, str | None]] = [parse_s3_uri(manifest_s3_uri)]
        for asset in manifest.assets:
            if not asset.asset_s3_uri:
                continue
            objects.append(parse_s3_uri(asset.asset_s3_uri))
        return objects

    def _build_manifest_key(self, manifest: ChunkManifest | None = None, *, source: S3ObjectRef | None = None) -> str:
        """
        EN: Resolve the S3 manifest key from either a manifest or a source reference.
        CN: 浠?manifest 鎴?source 寮曠敤瑙ｆ瀽 S3 manifest key銆?

        Args:
            manifest:
                EN: Optional chunk manifest whose source is used for key derivation.
                CN: 同上。
            source:
                EN: Fallback S3 object reference when manifest is not provided.
                CN: 褰撴湭鎻愪緵 manifest 鏃剁殑澶囩敤 S3 瀵硅薄寮曠敤銆?

        Returns:
            EN: Full S3 key for the manifest.json object.
            CN: manifest.json 瀵硅薄鐨勫畬鏁?S3 key銆?

        Raises:
            EN: ValueError if neither manifest nor source is provided.
            CN: 褰?manifest 鍜?source 鍧囨湭鎻愪緵鏃舵姏鍑?ValueError銆?
        """
        if manifest is not None:
            source = manifest.source
        if source is None:
            raise ValueError("source is required to build a manifest key")
        return build_manifest_key(source, manifest_prefix=self._manifest_prefix)

    def _build_asset_key(self, manifest: ChunkManifest, asset: ExtractedAsset) -> str:
        """
        EN: Build the S3 key for an asset, preferring metadata relative_path over a generated fallback.
        CN: 同上。

        Args:
            manifest:
                EN: Parent chunk manifest used to derive the manifest root folder.
                CN: 同上。
            asset:
                EN: Extracted asset whose metadata may contain a relative_path hint.
                CN: 同上。

        Returns:
            EN: Full S3 key under the manifest root for this asset.
            CN: 璇?asset 鍦?manifest 鏍圭洰褰曚笅鐨勫畬鏁?S3 key銆?
        """
        source = manifest.source
        relative_path = _sanitize_relative_path(asset.metadata.get("relative_path") if asset.metadata else None)
        if relative_path:
            return build_asset_key(source, relative_path, manifest_prefix=self._manifest_prefix)

        extension = _guess_asset_extension(asset.mime_type)
        fallback_name = _safe_filename(asset.asset_id)
        return build_asset_key(
            source,
            f"assets/{fallback_name}{extension}",
            manifest_prefix=self._manifest_prefix,
        )

    def _build_manifest_index_records(
        self,
        manifest: ChunkManifest,
        manifest_s3_uri: str,
        *,
        is_latest: bool,
    ) -> list[ChunkManifestRecord]:
        """
        EN: Build DynamoDB index records for every chunk in the manifest.
        CN: 同上。

        Args:
            manifest:
                EN: Chunk manifest whose chunks will be indexed.
                CN: 同上。
            manifest_s3_uri:
                EN: Version-aware S3 URI recorded in each index record for back-retrieval.
                CN: 同上。
            is_latest:
                EN: Whether these records represent the latest version for query routing.
                CN: 同上。

        Returns:
            EN: List of ChunkManifestRecord ready for DynamoDB batch write.
            CN: 同上。
        """
        records: list[ChunkManifestRecord] = []
        for chunk in manifest.chunks:
            records.append(
                ChunkManifestRecord(
                    pk=manifest.source.version_pk,
                    sk=f"chunk#{chunk.chunk_id}",
                    tenant_id=manifest.source.tenant_id,
                    bucket=manifest.source.bucket,
                    key=manifest.source.key,
                    version_id=manifest.source.version_id,
                    chunk_id=chunk.chunk_id,
                    chunk_type=chunk.chunk_type,
                    doc_type=manifest.doc_type,
                    is_latest=is_latest,
                    security_scope=manifest.source.security_scope,
                    language=manifest.source.language,
                    page_no=chunk.page_no,
                    page_span=chunk.page_span,
                    slide_no=chunk.slide_no,
                    section_path=chunk.section_path,
                    token_estimate=chunk.token_estimate,
                    text_preview=_build_text_preview(chunk.text),
                    manifest_s3_uri=manifest_s3_uri,
                )
            )
        return records

    def _load_version_records(self, *, source: S3ObjectRef, version_id: str) -> list[ChunkManifestRecord]:
        """
        EN: Query all manifest_index records for one source version using strong consistency.
        CN: 同上。

        Args:
            source:
                EN: S3 object reference used to derive the partition key.
                CN: 同上。
            version_id:
                EN: S3 version_id to query in the manifest_index table.
                CN: 同上。

        Returns:
            EN: List of ChunkManifestRecord for the given version, possibly empty.
            CN: 鎸囧畾鐗堟湰鐨?ChunkManifestRecord 鍒楄〃锛屽彲鑳戒负绌恒€?
        """
        pk = f"{source.object_pk}#{quote(version_id, safe='')}"
        response = self._ddb.query(
            TableName=self._manifest_index_table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": pk}},
            ConsistentRead=True,
        )
        return [_deserialize_manifest_record(item) for item in response.get("Items") or []]

    def _put_manifest_records(self, records: list[ChunkManifestRecord]) -> None:
        """
        EN: Batch-write manifest index records to DynamoDB in chunks of 25.
        CN: 浠?25 鏉′负涓€鎵瑰皢 manifest 绱㈠紩璁板綍鎵归噺鍐欏叆 DynamoDB銆?

        Args:
            records:
                EN: List of ChunkManifestRecord to persist.
                CN: 瑕佹寔涔呭寲鐨?ChunkManifestRecord 鍒楄〃銆?
        """
        for batch in _chunked(records, 25):
            request_items = {
                self._manifest_index_table: [
                    {"PutRequest": {"Item": _serialize_manifest_record(record)}}
                    for record in batch
                ]
            }
            self._flush_batch_write(request_items)

    def _delete_manifest_records(self, records: list[ChunkManifestRecord]) -> None:
        """
        EN: Batch-delete manifest index records from DynamoDB, used during rollback and old-version cleanup.
        CN: 同上。

        Args:
            records:
                EN: List of ChunkManifestRecord to delete by (pk, sk) composite key.
                CN: 同上。
        """
        if not records:
            return
        for batch in _chunked(records, 25):
            request_items = {
                self._manifest_index_table: [
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

    def _flush_batch_write(self, request_items: dict[str, list[dict]]) -> None:
        # EN: Delegate to the shared batch-write helper with exponential backoff retry.
        # CN: 濮旀墭缁欏甫鎸囨暟閫€閬块噸璇曠殑鍏变韩 batch write 杈呭姪鍑芥暟銆?
        flush_batch_write(self._ddb, request_items)

    def _delete_s3_objects(self, objects: list[tuple[str, str, str | None]]) -> None:
        """
        EN: Delete S3 objects in reverse creation order to respect dependency chains during rollback.
        CN: 鎸夊垱寤虹殑閫嗗簭鍒犻櫎 S3 瀵硅薄锛屼互鍦ㄥ洖婊氭湡闂村皧閲嶄緷璧栭摼銆?

        Args:
            objects:
                EN: List of (bucket, key, version_id) tuples to delete.
                CN: 瑕佸垹闄ょ殑 (bucket, key, version_id) 鍏冪粍鍒楄〃銆?
        """
        reversed_objects = list(reversed(objects))
        for batch in _chunked(reversed_objects, 1000):
            if not batch:
                continue
            bucket_name = batch[0][0]
            if any(item[0] != bucket_name for item in batch):
                for bucket, key, version_id in batch:
                    kwargs = {"Bucket": bucket, "Key": key}
                    if version_id:
                        kwargs["VersionId"] = version_id
                    self._s3.delete_object(**kwargs)
                continue
            self._s3.delete_objects(
                Bucket=bucket_name,
                Delete={
                    "Objects": [
                        {"Key": key, **({"VersionId": version_id} if version_id else {})}
                        for _, key, version_id in batch
                    ],
                    "Quiet": True,
                },
            )

    def _load_manifest_if_present(self, manifest_s3_uri: str) -> ChunkManifest | None:
        """
        EN: Attempt to load a manifest from S3, returning None when the object is missing or the URI is stale.
        CN: 灏濊瘯浠?S3 鍔犺浇 manifest锛屽綋瀵硅薄涓嶅瓨鍦ㄦ垨 URI 杩囨湡鏃惰繑鍥?None銆?

        Args:
            manifest_s3_uri:
                EN: Version-aware S3 URI of the manifest to load.
                CN: 瑕佸姞杞界殑 manifest 鐨勫甫鐗堟湰淇℃伅鐨?S3 URI銆?

        Returns:
            EN: Deserialized ChunkManifest if found, otherwise None.
            CN: 鎵惧埌鏃惰繑鍥炲弽搴忓垪鍖栫殑 ChunkManifest锛屽惁鍒欒繑鍥?None銆?
        """
        try:
            return self.load_manifest(manifest_s3_uri)
        except ClientError as exc:
            if _is_missing_object_error(exc):
                return None
            raise


def _serialize_manifest_record(record: ChunkManifestRecord) -> dict[str, dict[str, str | bool]]:
    """
    EN: Serialize a ChunkManifestRecord into a DynamoDB-compatible item dictionary.
    CN: 同上。

    Args:
        record:
            EN: Chunk manifest record to serialize.
            CN: 瑕佸簭鍒楀寲鐨?chunk manifest 璁板綍銆?

    Returns:
        EN: DynamoDB item dictionary with typed attribute values.
        CN: 甯︾被鍨嬪寲灞炴€у€肩殑 DynamoDB 鏉＄洰瀛楀吀銆?
    """
    item: dict[str, dict[str, str | bool]] = {
        "pk": {"S": record.pk},
        "sk": {"S": record.sk},
        "tenant_id": {"S": record.tenant_id},
        "bucket": {"S": record.bucket},
        "key": {"S": record.key},
        "version_id": {"S": record.version_id},
        "chunk_id": {"S": record.chunk_id},
        "chunk_type": {"S": record.chunk_type},
        "doc_type": {"S": record.doc_type},
        "is_latest": {"BOOL": record.is_latest},
        "language": {"S": record.language},
        "created_at": {"S": record.created_at},
    }
    if record.page_no is not None:
        item["page_no"] = {"N": str(record.page_no)}
    if record.slide_no is not None:
        item["slide_no"] = {"N": str(record.slide_no)}
    if record.page_span is not None:
        item["page_span"] = {"S": json.dumps(record.page_span, ensure_ascii=False)}
    if record.section_path:
        item["section_path"] = {"S": json.dumps(record.section_path, ensure_ascii=False)}
    if record.token_estimate is not None:
        item["token_estimate"] = {"N": str(record.token_estimate)}
    if record.text_preview:
        item["text_preview"] = {"S": record.text_preview}
    if record.security_scope:
        item["security_scope"] = {"S": json.dumps(record.security_scope, ensure_ascii=False)}
    if record.manifest_s3_uri:
        item["manifest_s3_uri"] = {"S": record.manifest_s3_uri}
    return item


def _deserialize_manifest_record(item: dict[str, dict[str, str | bool]]) -> ChunkManifestRecord:
    """
    EN: Deserialize a DynamoDB item into a ChunkManifestRecord domain model.
    CN: 灏?DynamoDB 鏉＄洰鍙嶅簭鍒楀寲涓?ChunkManifestRecord 棰嗗煙妯″瀷銆?

    Args:
        item:
            EN: Raw DynamoDB item with typed attribute values.
            CN: 同上。

    Returns:
        EN: Populated ChunkManifestRecord instance.
        CN: 同上。
    """
    return ChunkManifestRecord(
        pk=item["pk"]["S"],  # type: ignore[index]
        sk=item["sk"]["S"],  # type: ignore[index]
        tenant_id=item["tenant_id"]["S"],  # type: ignore[index]
        bucket=item["bucket"]["S"],  # type: ignore[index]
        key=item["key"]["S"],  # type: ignore[index]
        version_id=item["version_id"]["S"],  # type: ignore[index]
        chunk_id=item["chunk_id"]["S"],  # type: ignore[index]
        chunk_type=item["chunk_type"]["S"],  # type: ignore[index,arg-type]
        doc_type=item["doc_type"]["S"],  # type: ignore[index]
        is_latest=bool(item["is_latest"]["BOOL"]),  # type: ignore[index]
        security_scope=tuple(json.loads(item.get("security_scope", {}).get("S", "[]"))),  # type: ignore[union-attr]
        language=item.get("language", {}).get("S", "zh"),  # type: ignore[union-attr]
        page_no=int(item["page_no"]["N"]) if "page_no" in item else None,  # type: ignore[index]
        page_span=tuple(json.loads(item["page_span"]["S"])) if "page_span" in item else None,  # type: ignore[index]
        slide_no=int(item["slide_no"]["N"]) if "slide_no" in item else None,  # type: ignore[index]
        section_path=tuple(json.loads(item["section_path"]["S"])) if "section_path" in item else (),  # type: ignore[index]
        token_estimate=int(item["token_estimate"]["N"]) if "token_estimate" in item else None,  # type: ignore[index]
        text_preview=item.get("text_preview", {}).get("S"),  # type: ignore[union-attr]
        manifest_s3_uri=item.get("manifest_s3_uri", {}).get("S"),  # type: ignore[union-attr]
        created_at=item.get("created_at", {}).get("S", ""),  # type: ignore[union-attr]
    )


def _chunked(items: list[ChunkManifestRecord], size: int) -> list[list[ChunkManifestRecord]]:
    # EN: Split a list into fixed-size batches for DynamoDB batch_write_item calls.
    # CN: 灏嗗垪琛ㄦ媶鍒嗕负鍥哄畾澶у皬鐨勬壒娆★紝鐢ㄤ簬 DynamoDB batch_write_item 璋冪敤銆?
    return [items[index : index + size] for index in range(0, len(items), size)]


def _build_text_preview(text: str) -> str:
    """
    EN: Build a bounded text preview for query-time projection reads.
    CN: 为查询投影层构建有上限的文本预览。
    """
    preview = text.strip()
    if len(preview) <= _TEXT_PREVIEW_LIMIT:
        return preview
    return preview[: _TEXT_PREVIEW_LIMIT - 1].rstrip() + "…"


def _is_missing_object_error(exc: ClientError) -> bool:
    # EN: Check whether a ClientError indicates the S3 object does not exist.
    # CN: 同上。
    error = exc.response.get("Error", {})
    code = str(error.get("Code", ""))
    return code in {"NoSuchKey", "404", "NotFound"}


def _guess_asset_extension(mime_type: str) -> str:
    """
    EN: Guess a file extension from a MIME type, with special handling for JSONL and Markdown.
    CN: 同上。

    Args:
        mime_type:
            EN: MIME type string such as "application/x-ndjson" or "text/markdown".
            CN: 同上。

    Returns:
        EN: Lowercase file extension with leading dot, defaulting to ".bin".
        CN: 甯﹀墠瀵肩偣鐨勫皬鍐欐枃浠舵墿灞曞悕锛岄粯璁や负 ".bin"銆?
    """
    lower = mime_type.lower()
    if lower in {"application/x-ndjson", "application/jsonl"}:
        return ".jsonl"
    if lower.startswith("text/markdown"):
        return ".md"
    return mimetypes.guess_extension(mime_type) or ".bin"


def _sanitize_relative_path(relative_path: str | None) -> str | None:
    """
    EN: Sanitize a relative path by stripping whitespace and leading slashes, rejecting path traversal.
    CN: 鍑€鍖栫浉瀵硅矾寰勶紝鍘婚櫎绌虹櫧鍜屽墠瀵兼枩鏉狅紝骞舵嫆缁濊矾寰勭┛瓒娿€?

    Args:
        relative_path:
            EN: Raw relative path string or None.
            CN: 同上。

    Returns:
        EN: Sanitized relative path without leading slash, or None if empty.
        CN: 同上。
    """
    if not relative_path:
        return None
    value = relative_path.strip().lstrip("/")
    if not value:
        return None
    if ".." in value.split("/"):
        raise ValueError(f"Invalid relative path: {relative_path}")
    return value


def _safe_filename(value: str) -> str:
    """
    EN: Convert an arbitrary string into a safe filename by replacing unsafe characters with hyphens.
    CN: 同上。

    Args:
        value:
            EN: Raw string to sanitize for use as a filename.
            CN: 同上。

    Returns:
        EN: Sanitized filename, defaulting to "asset" when nothing remains.
        CN: 同上。
    """
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip(".-_")
    return sanitized or "asset"


def _extract_version_id(response: dict[str, object]) -> str | None:
    # EN: Extract the S3 VersionId from a put_object response, returning None when versioning is off.
    # CN: 同上。
    version_id = response.get("VersionId")
    if isinstance(version_id, str) and version_id.strip():
        return version_id.strip()
    return None
