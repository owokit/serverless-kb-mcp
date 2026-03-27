"""
EN: S3 document source for fetching versioned objects.
CN: 用于获取带版本对象的 S3 文档源。
"""
from __future__ import annotations

from dataclasses import dataclass
from time import monotonic

from serverless_mcp.runtime.aws_clients import build_aws_client
from serverless_mcp.runtime.observability import emit_trace
from serverless_mcp.domain.models import S3ObjectRef


@dataclass(slots=True)
class S3ObjectPayload:
    """
    EN: Container for fetched S3 object with source reference and body.
    CN: 保存已获取 S3 对象、来源引用和正文的容器。
    """
    source: S3ObjectRef
    body: bytes
    content_length: int


class S3DocumentSource:
    """
    EN: Fetch document bytes from S3 using version_id for immutable retrieval.
    CN: 使用 version_id 从 S3 获取文档字节，以保证不可变读取。
    """

    def __init__(self, *, s3_client: object | None = None) -> None:
        self._s3 = s3_client or build_aws_client("s3")

    def fetch(self, source: S3ObjectRef) -> S3ObjectPayload:
        """
        EN: Fetch S3 object by version_id ensuring immutable document identity.
        CN: 通过 version_id 获取 S3 对象，以保证文档身份不可变。

        Args:
            source:
                EN: S3 object reference with bucket/key/version_id.
                CN: 包含 bucket、key 和 version_id 的 S3 对象引用。

        Returns:
            EN: S3 object payload with document bytes.
            CN: 包含文档字节的 S3 对象载荷。
        """
        start = monotonic()
        # EN: Emit start trace with S3 identity for latency tracking and debugging.
        # CN: 记录起始 trace，带上 S3 身份信息，便于延迟跟踪和排障。
        emit_trace(
            "s3_source.fetch.start",
            document_uri=source.document_uri,
            bucket=source.bucket,
            key=source.key,
            version_id=source.version_id,
        )
        response = self._s3.get_object(
            Bucket=source.bucket,
            Key=source.key,
            VersionId=source.version_id,
        )
        # EN: Emit headers trace after receiving S3 response metadata.
        # CN: 在收到 S3 响应元数据后记录 headers trace。
        emit_trace(
            "s3_source.fetch.headers",
            document_uri=source.document_uri,
            content_length=response.get("ContentLength"),
            content_type=response.get("ContentType"),
            elapsed_ms=round((monotonic() - start) * 1000, 2),
        )
        body = response["Body"].read()
        # EN: Emit done trace with final content length and total elapsed time.
        # CN: 记录结束 trace，包含最终内容长度和总耗时。
        emit_trace(
            "s3_source.fetch.done",
            document_uri=source.document_uri,
            content_length=len(body),
            elapsed_ms=round((monotonic() - start) * 1000, 2),
        )
        return S3ObjectPayload(
            source=source,
            body=body,
            content_length=len(body),
        )

