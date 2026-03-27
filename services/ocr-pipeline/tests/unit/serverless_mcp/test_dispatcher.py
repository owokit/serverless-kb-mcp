"""
EN: Tests for EmbeddingJobDispatcher batching behavior.
CN: 测试 EmbeddingJobDispatcher 的批量发送行为。
"""

from serverless_mcp.embed.dispatcher import EmbeddingJobDispatcher
from serverless_mcp.domain.models import EmbeddingJobMessage, EmbeddingRequest, S3ObjectRef


class _FakeSqsClient:
    def __init__(self) -> None:
        self.batches: list[dict[str, object]] = []

    def send_message_batch(self, **kwargs):
        self.batches.append(kwargs)
        return {"Successful": [{"Id": item["Id"]} for item in kwargs["Entries"]], "Failed": []}


class _RetryingSqsClient:
    def __init__(self) -> None:
        self.batches: list[dict[str, object]] = []
        self.calls = 0

    def send_message_batch(self, **kwargs):
        self.batches.append(kwargs)
        self.calls += 1
        if self.calls == 1:
            return {
                "Successful": [{"Id": kwargs["Entries"][0]["Id"]}],
                "Failed": [{"Id": kwargs["Entries"][1]["Id"], "Message": "throttled"}],
            }
        return {"Successful": [{"Id": item["Id"]} for item in kwargs["Entries"]], "Failed": []}


def _job(index: int) -> EmbeddingJobMessage:
    source = S3ObjectRef(
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="docs/guide.pdf",
        version_id="v1",
    )
    return EmbeddingJobMessage(
        source=source,
        profile_id="openai-text-small",
        trace_id="trace-1",
        manifest_s3_uri="s3://manifest-bucket/manifests/v1.json",
        requests=[
            EmbeddingRequest(
                chunk_id=f"chunk#{index:06d}",
                chunk_type="page_text_chunk",
                content_kind="text",
                text=f"text-{index}",
                metadata={
                    "tenant_id": source.tenant_id,
                    "bucket": source.bucket,
                    "key": source.key,
                    "version_id": source.version_id,
                    "document_uri": source.document_uri,
                    "language": source.language,
                    "doc_type": "pdf",
                    "source_format": "pdf",
                    "manifest_s3_uri": "s3://manifest-bucket/manifests/v1.json",
                    "is_latest": True,
                },
            )
        ],
    )


def test_dispatch_many_uses_batch_sqs_sends() -> None:
    """
    EN: Dispatch-many should use SQS batch sends in groups of at most ten.
    CN: dispatch-many 应当按最多 10 条一组使用 SQS batch send。
    """
    sqs = _FakeSqsClient()
    dispatcher = EmbeddingJobDispatcher(queue_url="https://sqs.example.com/queue", sqs_client=sqs)

    dispatcher.dispatch_many([_job(index) for index in range(12)])

    assert len(sqs.batches) == 2
    assert len(sqs.batches[0]["Entries"]) == 10
    assert len(sqs.batches[1]["Entries"]) == 2


def test_dispatch_many_retries_only_failed_entries() -> None:
    """
    EN: Dispatch-many retries only the SQS entries that failed in the previous batch response.
    CN: dispatch-many 只会重试上一轮 batch response 中失败的 SQS 条目。
    """
    sqs = _RetryingSqsClient()
    dispatcher = EmbeddingJobDispatcher(queue_url="https://sqs.example.com/queue", sqs_client=sqs)

    dispatcher.dispatch_many([_job(index) for index in range(2)])

    assert sqs.calls == 2
    assert len(sqs.batches[0]["Entries"]) == 2
    assert len(sqs.batches[1]["Entries"]) == 1
