"""
EN: Historical embedding re-dispatch service for reprocessing old S3 objects with new embedding profiles.
CN: 历史 embedding 重新分发服务，用于使用新的 embedding profile 重新处理旧的 S3 对象。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from botocore.exceptions import ClientError

from serverless_mcp.embed.dispatcher import EmbeddingJobDispatcher, build_jobs_for_profiles
from serverless_mcp.domain.models import EmbeddingProfile, S3ObjectRef
from serverless_mcp.storage.projection.repository import EmbeddingProjectionStateRepository
from serverless_mcp.storage.state.execution_state_repository import ExecutionStateRepository
from serverless_mcp.storage.manifest.repository import ManifestRepository
from serverless_mcp.storage.state.object_state_repository import ObjectStateRepository, ObjectStateLookupRecord

if TYPE_CHECKING:
    from serverless_mcp.extract.application import ExtractionService


_BACKFILL_FAILURE_TYPES = (ClientError, KeyError, OSError, RuntimeError, TypeError, ValueError)


@dataclass(slots=True)
class BackfillSample:
    """
    EN: Structured sample describing one skipped or failed backfill record.
    CN: 描述一条被跳过或失败的 backfill 记录的结构化样本。
    """

    object_pk: str
    version_id: str
    reason: str
    manifest_s3_uri: str | None = None


@dataclass(slots=True)
class EmbeddingBackfillOutcome:
    """
    EN: Aggregated counters summarizing a single-profile backfill pass.
    CN: 针对单个 profile 的回填统计聚合结果。
    """

    profile_id: str
    scanned_count: int
    eligible_count: int
    dispatched_job_count: int
    skipped_deleted_count: int
    skipped_not_ready_count: int
    skipped_stale_count: int
    skipped_projection_count: int
    resume_after_object_pk: str | None = None
    is_truncated: bool = False
    samples: tuple[BackfillSample, ...] = ()


class EmbeddingBackfillService:
    """
    EN: Re-dispatch historical embedding jobs for a single embedding profile so new models can cover old S3 content.
    CN: 重新分发历史嵌入作业，让新模型覆盖旧的 S3 内容。
    """

    def __init__(
        self,
        *,
        extraction_service: ExtractionService,
        object_state_repo: ObjectStateRepository,
        manifest_repo: ManifestRepository,
        embed_dispatcher: EmbeddingJobDispatcher,
        embedding_profiles: dict[str, EmbeddingProfile],
        projection_state_repo: EmbeddingProjectionStateRepository | None = None,
        execution_state_repo: ExecutionStateRepository | None = None,
    ) -> None:
        """
        Args:
            extraction_service:
                EN: Extraction service used to rebuild embedding requests from manifests.
                CN: 用于从 manifest 重建嵌入请求的提取服务。
            object_state_repo:
                EN: Repository for querying object_state records during backfill scanning.
                CN: 用于在回填扫描期间查询 object_state 记录的仓储。
            manifest_repo:
                EN: Repository for loading chunk manifests from the manifest bucket.
                CN: 用于从 manifest bucket 加载 chunk manifest 的仓储。
            embed_dispatcher:
                EN: Dispatcher that sends embedding jobs to the SQS embed queue.
                CN: 将嵌入作业发送到 SQS embed 队列的分发器。
            embedding_profiles:
                EN: Map of profile_id to EmbeddingProfile for profile validation and filtering.
                CN: profile_id 到 EmbeddingProfile 的映射，用于 profile 校验和过滤。
            projection_state_repo:
                EN: Optional repository for checking per-profile projection state; required for multi-profile setups.
                CN: 用于检查按 profile 划分的 projection state 的可选仓储；多 profile 场景必需。
        """
        self._extraction_service = extraction_service
        self._object_state_repo = object_state_repo
        self._execution_state_repo = execution_state_repo
        self._manifest_repo = manifest_repo
        self._embed_dispatcher = embed_dispatcher
        self._embedding_profiles = embedding_profiles
        self._projection_state_repo = projection_state_repo

    def backfill_profile(
        self,
        *,
        profile_id: str,
        trace_id: str,
        force: bool = False,
        resume_after_object_pk: str | None = None,
        max_records: int | None = None,
    ) -> EmbeddingBackfillOutcome:
        """
        EN: Iterate queryable lookup records and re-dispatch embedding jobs for eligible versions under the given profile.
        CN: 遍历可查询的 lookup 记录，并为给定 profile 下满足条件的版本重新分发嵌入作业。

        Args:
            profile_id:
                EN: Target embedding profile identifier.
                CN: 目标 embedding profile 标识符。
            trace_id:
                EN: Trace identifier propagated into each dispatched job.
                CN: 传播到每个分发作业中的追踪标识。
            force:
                EN: When True, skip projection-state readiness checks and re-embed regardless.
                CN: 为 True 时跳过 projection-state 就绪检查，强制重新嵌入。
            resume_after_object_pk:
                EN: Optional object_pk cursor for resumable backfill runs.
                CN: 可选的 object_pk 游标，用于可恢复的 backfill 执行。
            max_records:
                EN: Optional upper bound on scanned lookup records for one invocation.
                CN: 单次调用中可扫描的 lookup 记录上限。

        Returns:
            EN: Outcome counters for the backfill pass.
            CN: 回填过程的结果计数器。
        """
        profile = self._require_profile(profile_id)
        scanned_count = 0
        eligible_count = 0
        dispatched_job_count = 0
        skipped_deleted_count = 0
        skipped_not_ready_count = 0
        skipped_stale_count = 0
        skipped_projection_count = 0
        samples: list[BackfillSample] = []
        processed_records = 0
        truncated = False
        next_resume_after_object_pk: str | None = None
        resume_after_object_pk = (
            resume_after_object_pk.strip()
            if isinstance(resume_after_object_pk, str) and resume_after_object_pk.strip()
            else None
        )

        # EN: Iterate all known objects; skip stale, deleted, not-ready, and already-indexed entries.
        # CN: 遍历所有已知对象，跳过过期、已删除、未就绪和已索引的条目。
        for lookup in self._object_state_repo.iter_lookup_records():
            if resume_after_object_pk is not None and lookup.object_pk <= resume_after_object_pk:
                continue
            if max_records is not None and processed_records >= max_records:
                truncated = True
                break
            processed_records += 1
            next_resume_after_object_pk = lookup.object_pk
            scanned_count += 1
            state = self._load_execution_state(object_pk=lookup.object_pk)
            if state is None or state.latest_version_id != lookup.latest_version_id:
                skipped_stale_count += 1
                self._record_sample(
                    samples,
                    object_pk=lookup.object_pk,
                    version_id=lookup.latest_version_id,
                    reason="stale_or_missing_state",
                    manifest_s3_uri=lookup.latest_manifest_s3_uri,
                )
                continue
            if state.is_deleted:
                skipped_deleted_count += 1
                self._record_sample(
                    samples,
                    object_pk=lookup.object_pk,
                    version_id=lookup.latest_version_id,
                    reason="deleted",
                    manifest_s3_uri=lookup.latest_manifest_s3_uri,
                )
                continue
            if state.extract_status != "EXTRACTED" or not state.latest_manifest_s3_uri:
                skipped_not_ready_count += 1
                self._record_sample(
                    samples,
                    object_pk=lookup.object_pk,
                    version_id=lookup.latest_version_id,
                    reason="not_ready",
                    manifest_s3_uri=state.latest_manifest_s3_uri,
                )
                continue
            if not force and self._is_projection_ready(source=lookup, profile_id=profile_id):
                skipped_projection_count += 1
                self._record_sample(
                    samples,
                    object_pk=lookup.object_pk,
                    version_id=lookup.latest_version_id,
                    reason="projection_ready",
                    manifest_s3_uri=state.latest_manifest_s3_uri,
                )
                continue

            # EN: Load manifest, build embedding requests, and expand into profile-scoped jobs.
            # CN: 加载 manifest，构建嵌入请求，并展开为按 profile 划分的作业。
            try:
                manifest = self._manifest_repo.load_manifest(state.latest_manifest_s3_uri)
                requests = self._extraction_service.build_embedding_requests(
                    manifest,
                    manifest_s3_uri=state.latest_manifest_s3_uri,
                )
                jobs = build_jobs_for_profiles(
                    source=_lookup_to_source(lookup),
                    trace_id=trace_id,
                    manifest_s3_uri=state.latest_manifest_s3_uri,
                    requests=requests,
                    profiles=(profile,),
                    previous_version_id=state.previous_version_id,
                    previous_manifest_s3_uri=state.previous_manifest_s3_uri,
                )
            except _BACKFILL_FAILURE_TYPES:
                self._record_sample(
                    samples,
                    object_pk=lookup.object_pk,
                    version_id=lookup.latest_version_id,
                    reason="manifest_or_job_build_failed",
                    manifest_s3_uri=state.latest_manifest_s3_uri,
                )
                continue
            if not jobs:
                continue

            eligible_count += 1
            dispatched_job_count += len(jobs)
            self._embed_dispatcher.dispatch_many(jobs)

        return EmbeddingBackfillOutcome(
            profile_id=profile_id,
            scanned_count=scanned_count,
            eligible_count=eligible_count,
            dispatched_job_count=dispatched_job_count,
            skipped_deleted_count=skipped_deleted_count,
            skipped_not_ready_count=skipped_not_ready_count,
            skipped_stale_count=skipped_stale_count,
            skipped_projection_count=skipped_projection_count,
            resume_after_object_pk=next_resume_after_object_pk,
            is_truncated=truncated,
            samples=tuple(samples[:5]),
        )

    def _require_profile(self, profile_id: str) -> EmbeddingProfile:
        """
        EN: Validate that the profile exists and is writable.
        CN: 验证指定 profile 存在且可写入。
        """
        profile = self._embedding_profiles.get(profile_id)
        if profile is None:
            raise ValueError(f"Unknown embedding profile: {profile_id}")
        if not profile.enable_write:
            raise ValueError(f"Embedding profile is not writable: {profile_id}")
        return profile

    def _is_projection_ready(self, *, source: ObjectStateLookupRecord, profile_id: str) -> bool:
        """
        EN: Check whether the given profile has already indexed this version.
        CN: 检查给定 profile 是否已经为该版本建立索引。
        """
        if self._projection_state_repo is None:
            return False
        record = self._projection_state_repo.get_state(
            object_pk=source.object_pk,
            version_id=source.latest_version_id,
            profile_id=profile_id,
        )
        return bool(record and record.query_status == "INDEXED")

    def _load_execution_state(self, *, object_pk: str):
        """
        EN: Load execution-state first and fall back to object_state when needed.
        CN: 优先读取 execution-state，必要时回退到 object_state。
        """
        if self._execution_state_repo is not None:
            return self._execution_state_repo.get_state(object_pk=object_pk)
        return self._object_state_repo.get_state(object_pk=object_pk)

    def _record_sample(
        self,
        samples: list[BackfillSample],
        *,
        object_pk: str,
        version_id: str,
        reason: str,
        manifest_s3_uri: str | None,
    ) -> None:
        """
        EN: Capture a bounded sample list for skipped or failed backfill records.
        CN: 为被跳过或失败的 backfill 记录保留一个有上限的样本列表。
        """
        if len(samples) >= 5:
            return
        samples.append(
            BackfillSample(
                object_pk=object_pk,
                version_id=version_id,
                reason=reason,
                manifest_s3_uri=manifest_s3_uri,
            )
        )


def _lookup_to_source(lookup: ObjectStateLookupRecord) -> S3ObjectRef:
    """
    EN: Convert a lookup record into an S3ObjectRef for job dispatch.
    CN: 将 lookup 记录转换为用于作业分发的 S3ObjectRef。
    """
    return S3ObjectRef(
        tenant_id=lookup.tenant_id,
        bucket=lookup.bucket,
        key=lookup.key,
        version_id=lookup.latest_version_id,
        sequencer=lookup.latest_sequencer,
    )
