"""
EN: Storage protocol interfaces for decoupling boto3 and enabling test doubles.
CN: 用于解耦 boto3 并启用测试替代实现的存储协议接口。

This module defines structural protocols (interfaces) that the concrete repository
implementations satisfy. Consumers should depend on these protocols rather than
the concrete classes to enable dependency injection and testing.
本模块定义了具体仓库实现满足的结构化协议（接口）。消费者应依赖这些协议而非
具体类，以实现依赖注入和测试。
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from serverless_mcp.domain.models import (
        ChunkManifest,
        EmbeddingProfile,
        ObjectStateRecord,
        PersistedManifest,
        S3ObjectRef,
    )


class ManifestStore(Protocol):
    """
    EN: Protocol for persisting and loading chunk manifests with S3 + DynamoDB indexing.
    CN: 用于持久化和加载 chunk manifest 的协议，S3 + DynamoDB 索引。

    Implementations must provide transactional consistency between S3 writes
    and DynamoDB index updates.
    实现必须提供 S3 写入和 DynamoDB 索引更新之间的事务一致性。
    """

    def persist_manifest(
        self,
        manifest: ChunkManifest,
        *,
        previous_version_id: str | None = None,
    ) -> PersistedManifest:
        """
        EN: Persist manifest to S3 and index chunks in DynamoDB atomically.
        CN: 原子地将 manifest 持久化到 S3 并在 DynamoDB 中索引 chunks。

        Args:
            manifest:
                EN: Validated chunk manifest containing source identity, chunks, and assets.
                CN: 包含源身份、chunks 和 assets 的已校验 chunk manifest。
            previous_version_id:
                EN: Optional old version_id whose index records should be demoted to is_latest=False.
                CN: 可选的旧 version_id，其索引记录应降级为 is_latest=False。

        Returns:
            EN: PersistedManifest containing the manifest and its version-aware S3 URI.
            CN: 包含 manifest 及其带版本 S3 URI 的 PersistedManifest。
        """
        ...

    def load_manifest(self, manifest_s3_uri: str) -> ChunkManifest:
        """
        EN: Load chunk manifest from S3 URI for version governance and neighbor expansion.
        CN: 从 S3 URI 加载 chunk manifest，用于版本治理和邻居扩展。

        Args:
            manifest_s3_uri:
                EN: Version-aware S3 URI pointing to the manifest.json.
                CN: 指向 manifest.json 的带版本 S3 URI。

        Returns:
            EN: Deserialized and validated ChunkManifest.
            CN: 反序列化并校验的 ChunkManifest。
        """
        ...

    def find_manifest_s3_uri(self, *, source: S3ObjectRef, version_id: str) -> str | None:
        """
        EN: Resolve the exact manifest S3 URI recorded for one source version.
        CN: 解析为某个源版本记录的准确 manifest S3 URI。

        Args:
            source:
                EN: S3 object reference used to derive the partition key.
                CN: 用于派生 partition key 的 S3 对象引用。
            version_id:
                EN: S3 version_id to look up in the manifest_index table.
                CN: 要在 manifest_index 表中查找的 S3 version_id。

        Returns:
            EN: The manifest S3 URI if recorded, otherwise None.
            CN: 如果已记录则返回 manifest S3 URI，否则返回 None。
        """
        ...

    def rollback_manifest(
        self,
        manifest: ChunkManifest,
        *,
        manifest_s3_uri: str,
        previous_version_id: str | None = None,
    ) -> None:
        """
        EN: Best-effort rollback for a previously persisted manifest and its index records.
        CN: 对先前持久化的 manifest 及其索引记录执行尽力回滚。

        Args:
            manifest:
                EN: The manifest that was previously persisted.
                CN: 先前持久化的 manifest。
            manifest_s3_uri:
                EN: Version-aware S3 URI of the persisted manifest.
                CN: 已持久化 manifest 的带版本 S3 URI。
            previous_version_id:
                EN: Optional previous version_id for index record restoration.
                CN: 用于索引记录恢复的可选 previous_version_id。
        """
        ...

    def delete_previous_version_artifacts(
        self,
        *,
        source: S3ObjectRef,
        previous_version_id: str | None,
        previous_manifest_s3_uri: str | None = None,
    ) -> None:
        """
        EN: Remove the previous version's manifest objects and manifest index records.
        CN: 删除旧版本的 manifest 对象和 manifest_index 记录。

        Args:
            source:
                EN: Current S3 object reference for identity resolution.
                CN: 用于身份解析的当前 S3 对象引用。
            previous_version_id:
                EN: Old version_id whose artifacts should be cleaned up.
                CN: 应清理其构件的旧 version_id。
            previous_manifest_s3_uri:
                EN: Optional known manifest S3 URI to avoid extra DynamoDB lookup.
                CN: 可选的已知 manifest S3 URI，以避免额外的 DynamoDB 查询。
        """
        ...


class StateStore(Protocol):
    """
    EN: Protocol for managing object state records in DynamoDB.
    CN: 用于在 DynamoDB 中管理 object_state 记录的协议。

    Implementations must provide idempotent state transitions with
    sequencer ordering enforcement.
    实现必须提供具有 sequencer 排序强制功能的幂等状态转换。
    """

    def get_state(self, *, object_pk: str) -> ObjectStateRecord | None:
        """
        EN: Load the latest object state record by primary key with consistent reads.
        CN: 通过主键以一致性读取加载最新的 object_state 记录。

        Args:
            object_pk:
                EN: Primary key of the object_state record (tenant_id#bucket#key).
                CN: object_state 记录的主键（tenant_id#bucket#key）。

        Returns:
            EN: The ObjectStateRecord if it exists, otherwise None.
            CN: 存在时返回 ObjectStateRecord，否则返回 None。
        """
        ...

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
        """
        ...

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
        """
        ...

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
        ...

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
        ...

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
        ...

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
        ...

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
        ...

    def mark_embed_cleanup_failed(self, source: S3ObjectRef, error_message: str) -> ObjectStateRecord:
        """
        EN: Record a best-effort cleanup failure without changing the successful embed status.
        CN: 记录 best-effort 清理失败，但不改变已经成功的 embed 状态。

        Args:
            source:
                EN: S3 object reference for the version being cleaned up.
                CN: 正在清理的版本对应的 S3 对象引用。
            error_message:
                EN: Error description, truncated to 1000 characters.
                CN: 错误描述，最多截断为 1000 个字符。

        Returns:
            EN: The updated ObjectStateRecord.
            CN: 更新后的 ObjectStateRecord。
        """
        ...

    def mark_deleted(
        self,
        *,
        bucket: str,
        key: str,
        version_id: str,
        sequencer: str | None,
    ) -> ObjectStateRecord:
        """
        EN: Mark the current object version deleted using bucket/key lookup.
        CN: 使用 bucket/key 查找将当前对象版本标记为已删除。

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
        ...


class ProjectionStateStore(Protocol):
    """
    EN: Protocol for per-profile embedding projection state in DynamoDB.
    CN: 用于 DynamoDB 中按 profile 划分的 embedding 投影状态的协议。

    Used when multiple embedding profiles are active to track per-profile
    indexing status independently from the main object_state.
    当多个 embedding profile 活跃时使用，用于独立于主 object_state 跟踪按 profile
    划分的索引状态。
    """

    def mark_running(
        self,
        *,
        source: S3ObjectRef,
        profile: EmbeddingProfile,
        manifest_s3_uri: str,
    ) -> None:
        """
        EN: Mark embedding as running for a specific profile.
        CN: 将特定 profile 的 embedding 标记为运行中。

        Args:
            source:
                EN: S3 object reference for the version being embedded.
                CN: 正在执行 embedding 的版本对应的 S3 对象引用。
            profile:
                EN: The embedding profile being processed.
                CN: 正在处理的 embedding profile。
            manifest_s3_uri:
                EN: Version-aware S3 URI of the manifest.
                CN: manifest 的带版本 S3 URI。
        """
        ...

    def mark_done(
        self,
        *,
        outcome: object,
        profile: EmbeddingProfile,
    ) -> None:
        """
        EN: Mark embedding as done (INDEXED) for a specific profile.
        CN: 将特定 profile 的 embedding 标记为完成 (INDEXED)。

        Args:
            outcome:
                EN: Embedding outcome with vector count and object state snapshot.
                CN: 包含向量数量和 object_state 快照的 embedding 结果。
            profile:
                EN: The embedding profile that completed.
                CN: 完成的 embedding profile。
        """
        ...

    def mark_failed(
        self,
        *,
        source: S3ObjectRef,
        profile: EmbeddingProfile,
        manifest_s3_uri: str,
        error_message: str,
    ) -> None:
        """
        EN: Mark embedding as failed for a specific profile.
        CN: 将特定 profile 的 embedding 标记为失败。

        Args:
            source:
                EN: S3 object reference for the version that failed.
                CN: 失败的版本对应的 S3 对象引用。
            profile:
                EN: The embedding profile that failed.
                CN: 失败的 embedding profile。
            manifest_s3_uri:
                EN: Version-aware S3 URI of the manifest.
                CN: manifest 的带版本 S3 URI。
            error_message:
                EN: Error description.
                CN: 错误描述。
        """
        ...

    def get_state(
        self,
        *,
        object_pk: str,
        version_id: str,
        profile_id: str,
    ) -> object | None:
        """
        EN: Load the projection state for a specific profile and version.
        CN: 加载特定 profile 和版本的投影状态。

        Args:
            object_pk:
                EN: Primary key of the object (tenant_id#bucket#key).
                CN: 对象的主键（tenant_id#bucket#key）。
            version_id:
                EN: S3 version_id of the object.
                CN: 对象的 S3 version_id。
            profile_id:
                EN: The embedding profile identifier.
                CN: embedding profile 标识符。

        Returns:
            EN: The projection state record if it exists, otherwise None.
            CN: 存在时返回投影状态记录，否则返回 None。
        """
        ...

    def delete_version_records(
        self,
        *,
        source: S3ObjectRef,
        version_id: str,
    ) -> None:
        """
        EN: Delete all projection state records for a specific version.
        CN: 删除特定版本的所有投影状态记录。

        Args:
            source:
                EN: S3 object reference for identity resolution.
                CN: 用于身份解析的 S3 对象引用。
            version_id:
                EN: S3 version_id whose records should be deleted.
                CN: 应删除其记录的 S3 version_id。
        """
        ...


class VectorStore(Protocol):
    """
    EN: Protocol for persisting vectors in S3 Vectors.
    CN: 用于在 S3 Vectors 中持久化和删除向量的协议。

    Implementations must provide atomic vector batch operations with
    eventual consistency guarantees.
    实现必须提供具有最终一致性保证的原子向量批量操作。
    """

    def put_vectors(
        self,
        *,
        job: object,
        profile: EmbeddingProfile,
        vectors: list[object],
    ) -> None:
        """
        EN: Persist a batch of vectors to S3 Vectors for a specific profile.
        CN: 将一批向量持久化到特定 profile 的 S3 Vectors。

        Args:
            job:
                EN: Embedding job message containing source identity and metadata.
                CN: 包含源身份和元数据的 embedding 作业消息。
            profile:
                EN: The embedding profile whose vector space receives the vectors.
                CN: 接收向量的 embedding profile 的向量空间。
            vectors:
                EN: List of VectorRecord to persist.
                CN: 要持久化的 VectorRecord 列表。
        """
        ...

class QueueDispatcher(Protocol):
    """
    EN: Protocol for dispatching embedding jobs to an SQS queue.
    CN: 用于将 embedding 作业分发到 SQS 队列的协议。
    """

    def dispatch_many(self, jobs: list[object]) -> None:
        """
        EN: Dispatch multiple embedding jobs to the queue.
        CN: 将多个 embedding 作业分发到队列。

        Args:
            jobs:
                EN: List of EmbeddingJobMessage to dispatch.
                CN: 要分发的 EmbeddingJobMessage 列表。
        """
        ...
