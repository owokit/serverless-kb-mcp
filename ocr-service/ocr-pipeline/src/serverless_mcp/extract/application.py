"""
EN: Extraction service that coordinates document extraction and embedding request generation.
CN: 协调文档提取并生成 embedding 请求的提取服务。
"""
from __future__ import annotations

from serverless_mcp.extract.extractors import DocumentExtractor
from serverless_mcp.extract.policy import DEFAULT_POLICY
from serverless_mcp.extract.s3_source import S3DocumentSource
from serverless_mcp.domain.models import (
    ChunkManifest,
    EmbeddingPolicy,
    EmbeddingRequest,
    ExtractedAsset,
    ExtractedChunk,
    S3ObjectRef,
)


class ExtractionService:
    """
    EN: Orchestrate document extraction from S3 and build embedding requests for chunks and assets.
    CN: 协调从 S3 提取文档，并为 chunk 和资源生成 embedding 请求。
    """

    def __init__(
        self,
        *,
        source_repo: S3DocumentSource | None = None,
        extractor: DocumentExtractor | None = None,
        embedding_policy: EmbeddingPolicy = DEFAULT_POLICY,
    ) -> None:
        self._source_repo = source_repo or S3DocumentSource()
        self._extractor = extractor or DocumentExtractor()
        self._policy = embedding_policy

    def extract_from_s3(self, source: S3ObjectRef) -> ChunkManifest:
        """
        EN: Fetch a document from S3 and extract chunks using format-specific extractors.
        CN: 从 S3 获取文档，并使用对应格式的提取器生成 chunk。

        Args:
            source:
                EN: S3 object reference identified by bucket, key, and version_id.
                CN: 由 bucket、key 和 version_id 标识的 S3 对象引用。

        Returns:
            EN: Chunk manifest containing extracted text chunks and assets.
            CN: 包含已提取文本 chunk 和资源的 chunk manifest。
        """
        payload = self._source_repo.fetch(source)
        return self._extractor.extract(
            source=payload.source,
            body=payload.body,
            safe_text_tokens=self._policy.safe_text_tokens,
            max_pdf_pages_per_part=self._policy.max_pdf_pages_per_part,
        )

    def build_embedding_requests(self, manifest: ChunkManifest, *, manifest_s3_uri: str) -> list[EmbeddingRequest]:
        """
        EN: Build embedding requests for all text chunks and image assets in the manifest.
        CN: 为 manifest 中的所有文本 chunk 和图片资源构建 embedding 请求。

        Args:
            manifest:
                EN: Chunk manifest containing extracted chunks and assets.
                CN: 包含已提取 chunk 和资源的 chunk manifest。
            manifest_s3_uri:
                EN: S3 URI of the persisted manifest for metadata reference.
                CN: 已持久化 manifest 的 S3 URI，用于元数据引用。

        Returns:
            EN: List of embedding requests ready for embedding processing.
            CN: 可直接进入 embedding 处理流程的请求列表。
        """
        requests: list[EmbeddingRequest] = []

        for chunk in manifest.chunks:
            requests.append(self._build_text_request(manifest, chunk, manifest_s3_uri=manifest_s3_uri))

        for asset in manifest.assets:
            if asset.chunk_type in {"page_image_chunk", "slide_image_chunk", "image_chunk"}:
                requests.append(self._build_asset_request(manifest, asset, manifest_s3_uri=manifest_s3_uri))

        return requests

    def _build_text_request(
        self,
        manifest: ChunkManifest,
        chunk: ExtractedChunk,
        *,
        manifest_s3_uri: str,
    ) -> EmbeddingRequest:
        """
        EN: Build an embedding request for a text chunk with document-level metadata.
        CN: 为带有文档级元数据的文本 chunk 构建 embedding 请求。
        """
        # EN: Merge base metadata with chunk-specific fields like page_no, slide_no, and token estimates.
        # CN: 将基础元数据与 chunk 级字段合并，例如 page_no、slide_no 和 token 估算值。
        metadata = self._base_metadata(manifest, chunk.metadata) | {
            "doc_type": manifest.doc_type,
            "page_no": chunk.page_no,
            "page_span": chunk.page_span,
            "slide_no": chunk.slide_no,
            "token_estimate": chunk.token_estimate,
            "is_latest": True,
            "manifest_s3_uri": manifest_s3_uri,
        }
        if chunk.section_path:
            metadata["section_path"] = list(chunk.section_path)
        return EmbeddingRequest(
            chunk_id=chunk.chunk_id,
            chunk_type=chunk.chunk_type,
            content_kind="text",
            text=chunk.text,
            output_dimensionality=self._policy.output_dimensionality,
            task_type="RETRIEVAL_DOCUMENT",
            metadata=metadata,
        )

    def _build_asset_request(
        self,
        manifest: ChunkManifest,
        asset: ExtractedAsset,
        *,
        manifest_s3_uri: str,
    ) -> EmbeddingRequest:
        """
        EN: Build an embedding request for an image asset extracted from the document.
        CN: 为从文档中提取的图片资源构建 embedding 请求。
        """
        return EmbeddingRequest(
            chunk_id=asset.asset_id,
            chunk_type=asset.chunk_type,
            content_kind="image",
            asset_id=asset.asset_id,
            asset_s3_uri=asset.asset_s3_uri,
            mime_type=asset.mime_type,
            output_dimensionality=self._policy.output_dimensionality,
            task_type="RETRIEVAL_DOCUMENT",
            metadata=self._base_metadata(manifest, asset.metadata)
            | {
                "doc_type": manifest.doc_type,
                "mime_type": asset.mime_type,
                "page_no": asset.page_no,
                "slide_no": asset.slide_no,
                "is_latest": True,
                "manifest_s3_uri": manifest_s3_uri,
            },
        )

    def _base_metadata(self, manifest: ChunkManifest, metadata: dict[str, object]) -> dict[str, object]:
        """
        EN: Build base metadata dict from manifest source fields, merged with chunk/asset metadata.
        CN: 根据 manifest 的来源字段构建基础元数据字典，并与 chunk 或资源元数据合并。

        Args:
            manifest:
                EN: Chunk manifest providing source identity fields.
                CN: 提供来源身份字段的 chunk manifest。
            metadata:
                EN: Chunk-level or asset-level metadata to merge on top.
                CN: 需要叠加合并的 chunk 级或资源级元数据。

        Returns:
            EN: Merged metadata dict containing tenant_id, bucket, key, version_id, and caller metadata.
            CN: 合并后的元数据字典，包含 tenant_id、bucket、key、version_id 和调用方元数据。
        """
        source = manifest.source
        # EN: Include immutable S3 identity fields (tenant_id, bucket, key, version_id) and document-level attributes.
        # CN: 包含不可变的 S3 身份字段（tenant_id、bucket、key、version_id）和文档级属性。
        base_metadata = {
            "tenant_id": source.tenant_id,
            "bucket": source.bucket,
            "key": source.key,
            "version_id": source.version_id,
            "document_uri": source.document_uri,
            "language": source.language,
            **metadata,
        }
        return base_metadata
