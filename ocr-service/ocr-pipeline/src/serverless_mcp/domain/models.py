"""
EN: Shared data models for document processing, embedding, and query workflows.
CN: 用于文档处理、嵌入和查询工作流的共享数据模型。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal
from urllib.parse import quote


# EN: Allowed chunk type identifiers for extraction output categorization.
# CN: 用于抽取产物分类的 chunk 类型标识符。
ChunkType = Literal[
    "page_text_chunk",
    "page_image_chunk",
    "document_markdown_chunk",
    "ocr_json_chunk",
    "section_text_chunk",
    "window_pdf_chunk",
    "slide_text_chunk",
    "slide_image_chunk",
    "image_text_chunk",
    "image_chunk",
    "table_text_chunk",
]

# EN: Processing lifecycle states for document extraction and embedding.
# CN: 文档提取和嵌入处理的生命周期状态。
ProcessStatus = Literal[
    "PENDING",
    "QUEUED",
    "EXTRACTING",
    "EXTRACTED",
    "EMBEDDING",
    "INDEXED",
    "DELETED",
    "FAILED",
    "SKIPPED",
]

# EN: Supported embedding provider identifiers.
# CN: 支持的嵌入 provider 标识符。
EmbeddingProvider = Literal[
    "gemini",
    "openai",
]


def utc_now_iso() -> str:
    """
    EN: Generate the current UTC timestamp in ISO format.
    CN: 生成当前 UTC 时间戳的 ISO 格式字符串。
    """
    return datetime.now(UTC).isoformat()


@dataclass(frozen=True, slots=True)
class S3ObjectRef:
    """
    EN: Immutable S3 object reference with version_id as the primary identity.
    CN: 以 version_id 为主标识的不可变 S3 对象引用。
    """

    tenant_id: str
    bucket: str
    key: str
    version_id: str
    sequencer: str | None = None
    etag: str | None = None
    content_type: str | None = None
    security_scope: tuple[str, ...] = ()
    language: str = "zh"

    @property
    def object_pk(self) -> str:
        """
        EN: Composite primary key for the object-level DynamoDB record.
        CN: 对象级 DynamoDB 记录的复合主键。
        """
        return f"{_escape_key_part(self.tenant_id)}#{_escape_key_part(self.bucket)}#{_escape_key_part(self.key)}"

    @property
    def version_pk(self) -> str:
        """
        EN: Composite primary key scoped to a specific object version.
        CN: 限定到特定对象版本的复合主键。
        """
        return f"{self.object_pk}#{_escape_key_part(self.version_id)}"

    @property
    def document_uri(self) -> str:
        """
        EN: S3 URI with URL-encoded key and version_id query parameter.
        CN: 带有 URL 编码 key 和 version_id 查询参数的 S3 URI。
        """
        encoded_key = quote(self.key, safe="/")
        encoded_version_id = quote(self.version_id, safe="")
        return f"s3://{self.bucket}/{encoded_key}?versionId={encoded_version_id}"

    @property
    def extension(self) -> str:
        """
        EN: Lowercase file extension extracted from the S3 key, or empty string if none.
        CN: 从 S3 key 提取的小写文件扩展名，无扩展名时返回空字符串。
        """
        suffix = self.key.rsplit(".", 1)
        return suffix[-1].lower() if len(suffix) == 2 else ""


@dataclass(slots=True)
class ExtractedChunk:
    """
    EN: Extracted text chunk with metadata for embedding and retrieval.
    CN: 带有用于嵌入和检索元数据的提取文本 chunk。
    """

    chunk_id: str
    chunk_type: ChunkType
    text: str
    doc_type: str
    token_estimate: int
    page_no: int | None = None
    page_span: tuple[int, int] | None = None
    slide_no: int | None = None
    section_path: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ExtractedAsset:
    """
    EN: Extracted binary asset such as images for multimodal embedding.
    CN: 提取的二进制资产，例如用于多模态嵌入的图片。
    """

    asset_id: str
    chunk_type: ChunkType
    mime_type: str
    payload: bytes | None = None
    asset_s3_uri: str | None = None
    page_no: int | None = None
    slide_no: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ChunkManifest:
    """
    EN: Complete extraction result with chunks, assets, and metadata.
    CN: 包含 chunk、资产和元数据的完整提取结果。
    """

    source: S3ObjectRef
    doc_type: str
    chunks: list[ExtractedChunk]
    assets: list[ExtractedAsset] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EmbeddingPolicy:
    """
    EN: Embedding configuration policy for OpenAI Embedding and Azure OpenAI compatibility.
    CN: 适用于 OpenAI Embedding 和 Azure OpenAI 兼容接口的嵌入配置策略。
    """

    model: str = "text-embedding-3-small"
    output_dimensionality: int = 1536
    max_input_tokens: int = 2048
    safe_text_tokens: int = 1400
    max_pdf_pages_per_part: int = 6


@dataclass(frozen=True, slots=True)
class EmbeddingProfile:
    """
    EN: Immutable embedding profile describing one provider/model/dimension vector space.
    CN: 描述单一 provider/model/dimension 向量空间的不可变 embedding profile。
    """

    profile_id: str
    provider: EmbeddingProvider
    model: str
    dimension: int
    vector_bucket_name: str
    vector_index_name: str
    supported_content_kinds: tuple[Literal["text", "pdf", "image"], ...]
    enabled: bool = True
    enable_write: bool = True
    enable_query: bool = True

    def supports_content_kind(self, content_kind: Literal["text", "pdf", "image"]) -> bool:
        """
        EN: Check whether the profile accepts the requested content kind.
        CN: 检查当前 profile 是否接受指定内容类型。
        """

        return content_kind in self.supported_content_kinds


@dataclass(slots=True)
class EmbeddingRequest:
    """
    EN: Embedding request for text or multimodal content.
    CN: 用于文本或多模态内容的嵌入请求。
    """

    chunk_id: str
    chunk_type: ChunkType
    content_kind: Literal["text", "pdf", "image"]
    text: str | None = None
    asset_id: str | None = None
    asset_s3_uri: str | None = None
    mime_type: str | None = None
    output_dimensionality: int = 1536
    task_type: str = "RETRIEVAL_DOCUMENT"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class EmbeddingJobMessage:
    """
    EN: Embedding job message dispatched to the SQS embed queue.
    CN: 分发到 SQS embed 队列的嵌入作业消息。
    """

    source: S3ObjectRef
    profile_id: str
    trace_id: str
    manifest_s3_uri: str
    requests: list[EmbeddingRequest]
    previous_version_id: str | None = None
    previous_manifest_s3_uri: str | None = None
    requested_at: str = field(default_factory=utc_now_iso)


@dataclass(slots=True)
class ObjectStateRecord:
    """
    EN: DynamoDB record tracking version progression and processing status.
    CN: 跟踪版本推进和处理状态的 DynamoDB 记录。
    """

    pk: str
    latest_version_id: str
    latest_sequencer: str | None
    extract_status: ProcessStatus
    embed_status: ProcessStatus
    previous_version_id: str | None = None
    previous_manifest_s3_uri: str | None = None
    latest_manifest_s3_uri: str | None = None
    is_deleted: bool = False
    last_error: str | None = None
    updated_at: str = field(default_factory=utc_now_iso)


@dataclass(slots=True)
class ChunkManifestRecord:
    """
    EN: DynamoDB index record for chunk lookup and neighbor navigation.
    CN: 用于 chunk 查找和邻居导航的 DynamoDB 索引记录。
    """

    pk: str
    sk: str
    tenant_id: str
    bucket: str
    key: str
    version_id: str
    chunk_id: str
    chunk_type: ChunkType
    doc_type: str
    is_latest: bool
    security_scope: tuple[str, ...]
    language: str
    page_no: int | None = None
    page_span: tuple[int, int] | None = None
    slide_no: int | None = None
    section_path: tuple[str, ...] = ()
    token_estimate: int | None = None
    text_preview: str | None = None
    manifest_s3_uri: str | None = None
    created_at: str = field(default_factory=utc_now_iso)


@dataclass(slots=True)
class PersistedManifest:
    """
    EN: Persisted manifest with an S3 URI reference.
    CN: 带有 S3 URI 引用的持久化 manifest。
    """

    manifest: ChunkManifest
    manifest_s3_uri: str


@dataclass(slots=True)
class VectorRecord:
    """
    EN: Vector record for S3 Vectors persistence.
    CN: 用于 S3 Vectors 持久化的向量记录。
    """

    key: str
    data: list[float]
    metadata: dict[str, Any]


@dataclass(frozen=True, slots=True)
class VectorCleanupPlan:
    """
    EN: Cleanup plan for deleting previous-version vectors in a dedicated Step Functions workflow.
    CN: 用于独立 Step Functions 工作流删除旧版本向量的清理计划。
    """

    vector_bucket_name: str
    vector_index_name: str
    keys: tuple[str, ...]
    object_pk: str
    previous_version_id: str
    profile_id: str
    manifest_s3_uri: str
    previous_manifest_s3_uri: str | None = None
    requested_at: str = field(default_factory=utc_now_iso)


@dataclass(slots=True)
class ExtractJobMessage:
    """
    EN: Extract job message for the Step Functions workflow.
    CN: 用于 Step Functions 工作流的提取作业消息。
    """

    source: S3ObjectRef
    trace_id: str
    operation: Literal["UPSERT", "DELETE"] = "UPSERT"
    requested_at: str = field(default_factory=utc_now_iso)


@dataclass(slots=True)
class ProcessingOutcome:
    """
    EN: Extraction workflow outcome containing the manifest URI and counters.
    CN: 包含 manifest URI 和计数信息的提取工作流结果。
    """

    source: S3ObjectRef
    manifest_s3_uri: str
    chunk_count: int
    asset_count: int
    embedding_request_count: int
    object_state: ObjectStateRecord


@dataclass(slots=True)
class EmbeddingOutcome:
    """
    EN: Embed workflow outcome with vector count and updated state.
    CN: 包含向量计数和更新状态的嵌入工作流结果。
    """

    source: S3ObjectRef
    profile_id: str
    manifest_s3_uri: str
    vector_count: int
    object_state: ObjectStateRecord


@dataclass(slots=True)
class EmbeddingProjectionStateRecord:
    """
    EN: Projection state record tracking per-profile embedding readiness for one document version.
    CN: 跟踪单个文档版本在每个 profile 上嵌入就绪状态的投影状态记录。
    """

    pk: str
    sk: str
    object_pk: str
    version_id: str
    profile_id: str
    provider: EmbeddingProvider
    model: str
    dimension: int
    write_status: ProcessStatus
    query_status: ProcessStatus
    manifest_s3_uri: str | None = None
    vector_bucket_name: str | None = None
    vector_index_name: str | None = None
    vector_count: int | None = None
    last_error: str | None = None
    updated_at: str = field(default_factory=utc_now_iso)


def _escape_key_part(value: str) -> str:
    """
    EN: Escape a composite-key segment so delimiter collisions cannot corrupt identity.
    CN: 对复合主键片段进行转义，避免分隔符碰撞破坏身份标识。
    """
    return quote(value, safe="")


@dataclass(slots=True)
class QueryResultContext:
    """
    EN: Query result context for a matched chunk or asset.
    CN: 匹配 chunk 或资产的查询结果上下文。
    """

    chunk_id: str
    chunk_type: ChunkType
    text: str | None = None
    asset_s3_uri: str | None = None
    page_no: int | None = None
    page_span: tuple[int, int] | None = None
    slide_no: int | None = None
    section_path: tuple[str, ...] = ()


@dataclass(slots=True)
class QueryResultItem:
    """
    EN: Query result item with match context and neighbors.
    CN: 包含匹配上下文和邻居的查询结果项。
    """

    key: str
    distance: float | None
    source: S3ObjectRef
    manifest_s3_uri: str | None
    metadata: dict[str, Any]
    match: QueryResultContext
    neighbors: list[QueryResultContext] = field(default_factory=list)


@dataclass(slots=True)
class QueryDegradedProfile:
    """
    EN: Query profile failure or timeout surfaced alongside partial results.
    CN: 与部分结果一起返回的 query profile 失败或超时信息。
    """

    profile_id: str
    stage: str
    error: str
    manifest_s3_uri: str | None = None


@dataclass(slots=True)
class QueryResponse:
    """
    EN: Complete query response with all matched results.
    CN: 包含所有匹配结果的完整查询响应。
    """

    query: str
    results: list[QueryResultItem]
    degraded_profiles: tuple[QueryDegradedProfile, ...] = ()
