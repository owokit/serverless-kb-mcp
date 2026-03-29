"""
EN: Embedding job dispatcher that sends jobs to SQS embed queue.
CN: 将作业发送到 SQS embed 队列的嵌入作业分发器。
"""
from __future__ import annotations

import json
from dataclasses import asdict

from serverless_mcp.domain.embedding_schema import validate_embedding_job_message
from serverless_mcp.domain.models import EmbeddingJobMessage, EmbeddingProfile, EmbeddingRequest


class EmbeddingJobDispatcher:
    """
    EN: Dispatch embedding jobs to SQS queue for asynchronous processing.
    CN: 将嵌入作业异步发送到 SQS 队列。
    """

    def __init__(self, *, queue_url: str, sqs_client: object) -> None:
        """
        Args:
            queue_url:
                EN: SQS queue URL for the embed job queue.
                CN: embed 作业队列的 SQS 队列 URL。
            sqs_client:
                EN: Boto3 SQS client used to send messages.
                CN: 用于发送消息的 Boto3 SQS 客户端。
        """
        self._queue_url = queue_url
        self._sqs = sqs_client

    def dispatch(self, job: EmbeddingJobMessage) -> None:
        """
        EN: One document version dispatches only one embed job for stable embed_status convergence.
        CN: 同一文档版本只分发一个嵌入作业，以保证 embed_status 稳定收敛。

        Args:
            job:
                EN: Embedding job message containing all embedding requests for the document version.
                CN: 包含该文档版本全部嵌入请求的作业消息。
        """
        validate_embedding_job_message(job)
        self._sqs.send_message(
            QueueUrl=self._queue_url,
            MessageBody=json.dumps(asdict(job), ensure_ascii=False),
        )

    def dispatch_many(self, jobs: list[EmbeddingJobMessage]) -> None:
        """
        EN: Dispatch multiple embedding jobs, typically one job per document version and embedding profile.
        CN: 分发多个嵌入作业，通常每个文档版本和 embedding profile 各一个作业。

        Args:
            jobs:
                EN: List of embedding job messages to send.
                CN: 待发送的嵌入作业消息列表。
        """
        if not jobs:
            return

        for start in range(0, len(jobs), 10):
            batch = jobs[start : start + 10]
            entries = []
            for index, job in enumerate(batch):
                validate_embedding_job_message(job)
                entries.append(
                    {
                        "Id": f"{start + index}",
                        "MessageBody": json.dumps(asdict(job), ensure_ascii=False),
                    }
                )
            failed_entries = entries
            failures: list[dict[str, object]] = []
            for attempt in range(3):
                response = self._sqs.send_message_batch(QueueUrl=self._queue_url, Entries=failed_entries)
                failed = response.get("Failed") or []
                if not failed:
                    failures = []
                    break
                failures = failed
                failed_ids = {str(item.get("Id")) for item in failed if item.get("Id") is not None}
                failed_entries = [entry for entry in failed_entries if entry["Id"] in failed_ids]
                if not failed_entries:
                    break
            if failures:
                raise RuntimeError(
                    "Failed to dispatch one or more embedding jobs: "
                    + ", ".join(str(item.get("Message")) or str(item.get("Id")) for item in failures)
                )


def build_jobs_for_profiles(
    *,
    source,
    trace_id: str,
    manifest_s3_uri: str,
    requests: list[EmbeddingRequest],
    profiles: tuple[EmbeddingProfile, ...],
    previous_version_id: str | None,
    previous_manifest_s3_uri: str | None,
) -> list[EmbeddingJobMessage]:
    """
    EN: Expand one manifest-level embedding request set into profile-scoped jobs with filtered content kinds.
    CN: 将一个 manifest 级别的嵌入请求集展开为按 profile 划分的作业，并过滤内容类型。

    Args:
        source:
            EN: S3 object reference used as the document identity across jobs.
            CN: 作为跨作业文档身份使用的 S3 对象引用。
        trace_id:
            EN: Correlation identifier propagated into every emitted job.
            CN: 传播到每个发出作业中的关联标识。
        manifest_s3_uri:
            EN: S3 URI of the manifest that produced the embedding requests.
            CN: 产生嵌入请求的 manifest 的 S3 URI。
        requests:
            EN: Raw embedding requests derived from the manifest chunks.
            CN: 从 manifest chunks 派生出的原始嵌入请求。
        profiles:
            EN: Target embedding profiles to fan out to.
            CN: 要扇出的目标 embedding profile。
        previous_version_id:
            EN: Previous object version_id used for stale vector cleanup.
            CN: 用于清理过期向量的上一个对象版本 version_id。
        previous_manifest_s3_uri:
            EN: S3 URI of the previous version's manifest.
            CN: 上一个版本 manifest 的 S3 URI。

    Returns:
        EN: List of profile-scoped embedding job messages.
        CN: 按 profile 划分的嵌入作业消息列表。
    """
    jobs: list[EmbeddingJobMessage] = []
    for profile in profiles:
        if not profile.enabled or not profile.enable_write:
            continue
        # EN: Filter requests by profile-supported content kinds and inject output_dimensionality.
        # CN: 按 profile 支持的内容类型过滤请求，并注入 output_dimensionality。
        scoped_requests = [
            EmbeddingRequest(
                chunk_id=request.chunk_id,
                chunk_type=request.chunk_type,
                content_kind=request.content_kind,
                text=request.text,
                asset_id=request.asset_id,
                asset_s3_uri=request.asset_s3_uri,
                mime_type=request.mime_type,
                output_dimensionality=profile.dimension,
                task_type=request.task_type,
                metadata=dict(request.metadata),
            )
            for request in requests
            if profile.supports_content_kind(request.content_kind)
        ]
        if not scoped_requests:
            continue
        jobs.append(
            EmbeddingJobMessage(
                source=source,
                profile_id=profile.profile_id,
                trace_id=trace_id,
                manifest_s3_uri=manifest_s3_uri,
                requests=scoped_requests,
                previous_version_id=previous_version_id,
                previous_manifest_s3_uri=previous_manifest_s3_uri,
            )
        )
    return jobs
