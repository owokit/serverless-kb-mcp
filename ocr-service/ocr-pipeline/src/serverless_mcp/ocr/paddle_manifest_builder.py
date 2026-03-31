"""
EN: PaddleOCR manifest builder that converts JSONL + Markdown results into chunk manifests.
CN: 将 PaddleOCR 的 JSONL 和 Markdown 结果转换为 chunk manifest。
"""
from __future__ import annotations

import json
import mimetypes
import re
from pathlib import PurePosixPath
from typing import Any, Callable
from urllib.parse import urlparse

from serverless_mcp.domain.format_specs import get_format_spec
from serverless_mcp.domain.models import ChunkManifest, ExtractedAsset, ExtractedChunk, S3ObjectRef
from serverless_mcp.extract.policy import (
    DEFAULT_POLICY,
    estimate_tokens,
    normalize_text,
)
from serverless_mcp.extract.markdown_chunker import split_markdown_for_embedding
from serverless_mcp.extract.policy import _get_token_encoder


class PaddleOCRManifestBuilder:
    """
    EN: Build markdown-first chunk manifests from PaddleOCR async output.
    CN: 从 PaddleOCR 异步输出构建 markdown-first 的 chunk manifest。
    """

    def build_manifest(
        self,
        *,
        source: S3ObjectRef,
        json_lines: list[dict[str, Any]],
        markdown_text: str,
        binary_loader: Callable[[str], tuple[bytes, str | None]],
    ) -> ChunkManifest:
        """
        EN: Backward-compatible wrapper that keeps the JSONL raw asset while delegating to Markdown-first chunking.
        CN: 兼容旧调用入口，保留 JSONL 原始资产，并委托给 Markdown-first chunking。
        """
        return self.build_manifest_from_markdown(
            source=source,
            markdown_text=markdown_text,
            binary_loader=binary_loader,
            json_lines=json_lines,
        )

    def build_markdown_text_from_json_lines(self, json_lines: list[dict[str, Any]]) -> str:
        page_markdowns: list[str] = []
        for line in json_lines:
            page_markdown = self._extract_page_markdown_text(line)
            if page_markdown:
                page_markdowns.append(page_markdown)
        return "\n\n".join(page_markdowns)

    def build_manifest_from_markdown(
        self,
        *,
        source: S3ObjectRef,
        markdown_text: str,
        binary_loader: Callable[[str], tuple[bytes, str | None]],
        json_lines: list[dict[str, Any]] | None = None,
    ) -> ChunkManifest:
        """
        EN: Build a markdown-first manifest and keep JSONL as a replay/debug asset when available.
        CN: 构建 markdown-first manifest，并在可用时保留 JSONL 作为回放 / 调试资产。
        """
        chunks: list[ExtractedChunk] = []
        assets: list[ExtractedAsset] = []
        asset_index = 0
        image_asset_count = 0
        markdown_asset_count = 0
        document_markdown_asset_count = 0
        page_count = len(json_lines) if json_lines is not None else 0
        spec = get_format_spec(doc_type="pdf", source_format="paddleocr_async")

        # EN: Preserve raw JSONL as an asset for reproducibility and debugging.
        # CN: 同上。
        if json_lines is not None:
            raw_json_payload = _dump_json_lines(json_lines)
            assets.append(
                ExtractedAsset(
                    asset_id="ocr#raw-jsonl",
                    chunk_type="ocr_json_chunk",
                    mime_type="application/x-ndjson",
                    payload=raw_json_payload,
                    page_no=None,
                    metadata=spec.asset_metadata(
                        relative_path="raw.jsonl",
                        source_field="json_lines",
                        page_count=page_count,
                    ),
                )
            )

        image_urls = _collect_markdown_image_urls(markdown_text)
        local_asset_paths: dict[str, str] = {}
        for image_url in image_urls:
            asset_index += 1
            image_asset_count += 1
            image_asset = self._build_image_asset(
                asset_id=f"asset#{asset_index:06d}",
                page_no=None,
                binary_loader=binary_loader,
                url=image_url,
                image_name=PurePosixPath(urlparse(image_url).path).name or f"asset-{asset_index:06d}",
                metadata=spec.asset_metadata(source_field="markdown.images", page_count=page_count),
            )
            assets.append(image_asset)
            local_asset_paths[image_url] = image_asset.metadata["relative_path"]

        rewritten_markdown_text = self._rewrite_markdown_links(markdown_text, local_asset_paths) if markdown_text else ""
        markdown_chunks = split_markdown_for_embedding(
            rewritten_markdown_text,
            soft_token_target=DEFAULT_POLICY.safe_text_tokens,
            hard_token_limit=DEFAULT_POLICY.max_input_tokens,
            token_counter=estimate_tokens,
            tokenizer=_get_token_encoder(),
        )

        for layout_index, markdown_chunk in enumerate(markdown_chunks, start=1):
            assets.append(
                ExtractedAsset(
                    asset_id=f"ocr#markdown-section-{layout_index:06d}",
                    chunk_type="document_markdown_chunk",
                    mime_type="text/markdown",
                    payload=markdown_chunk.text.encode("utf-8"),
                    page_no=None,
                    metadata=spec.asset_metadata(
                        relative_path=f"sections/section-{layout_index:06d}.md",
                        source_field="markdown.text",
                        page_count=len(markdown_chunks),
                        layout_index=layout_index,
                    ),
                )
            )
            markdown_asset_count += 1
            chunks.append(
                ExtractedChunk(
                    chunk_id=f"chunk#{layout_index:06d}",
                    chunk_type="section_text_chunk",
                    text=markdown_chunk.text,
                    doc_type="pdf",
                    token_estimate=markdown_chunk.token_estimate,
                    section_path=markdown_chunk.header_path,
                    metadata=spec.chunk_metadata(layout_index=layout_index)
                    | markdown_chunk.metadata
                    | {
                        "layout_index": layout_index,
                        "section_path": list(markdown_chunk.header_path),
                        "header_path": list(markdown_chunk.header_path),
                    },
                )
            )

        if rewritten_markdown_text:
            assets.append(
                ExtractedAsset(
                    asset_id="ocr#markdown",
                    chunk_type="document_markdown_chunk",
                    mime_type="text/markdown",
                    payload=rewritten_markdown_text.encode("utf-8"),
                    page_no=None,
                    metadata=spec.asset_metadata(
                        relative_path="document.md",
                        source_field="markdownUrl",
                        page_count=len(markdown_chunks) or 1,
                    ),
                )
            )
        document_markdown_asset_count = int(bool(rewritten_markdown_text))

        return ChunkManifest(
            source=source,
            doc_type="pdf",
            chunks=chunks,
            assets=assets,
            metadata={
                "page_count": page_count,
                "page_image_asset_count": image_asset_count,
                "raw_json_asset_count": int(json_lines is not None),
                "layout_markdown_asset_count": markdown_asset_count,
                "document_markdown_asset_count": document_markdown_asset_count,
                "markdown_asset_count": markdown_asset_count + document_markdown_asset_count,
                "ocr_engine": "PaddleOCR-VL-1.5",
                "source_format": spec.source_format,
                "chunking_strategy": "v2_markdown_semchunk",
            },
        )

    def _build_image_asset(
        self,
        *,
        asset_id: str,
        page_no: int | None,
        binary_loader: Callable[[str], tuple[bytes, str | None]],
        url: str,
        image_name: str,
        metadata: dict[str, Any],
    ) -> ExtractedAsset:
        """
        EN: Download image binary, infer MIME type, and construct a page_image_chunk asset.
        CN: 同上。
        """
        payload, content_type = binary_loader(url)
        mime_type = (
            content_type
            or mimetypes.guess_type(urlparse(url).path)[0]
            or mimetypes.guess_type(image_name)[0]
            or "application/octet-stream"
        )
        extension = _infer_asset_extension(url=url, image_name=image_name, content_type=content_type)
        relative_path = f"assets/{_safe_filename(asset_id)}{extension}"
        return ExtractedAsset(
            asset_id=asset_id,
            chunk_type="page_image_chunk",
            mime_type=mime_type,
            payload=payload,
            page_no=page_no,
            metadata=metadata
            | {
                "image_name": image_name,
                "source_url": url,
                "relative_path": relative_path,
            },
        )

    def _rewrite_markdown_links(self, markdown_text: str, local_asset_paths: dict[str, str]) -> str:
        """
        EN: Replace remote image URLs in markdown with local asset relative paths.
        CN: 同上。
        """
        # EN: Sort by URL length descending to avoid partial replacements of longer URLs.
        # CN: 同上。
        rewritten = markdown_text
        for original_url, relative_path in sorted(local_asset_paths.items(), key=lambda item: len(item[0]), reverse=True):
            rewritten = rewritten.replace(original_url, relative_path)
        return normalize_text(rewritten)

    def _extract_page_markdown_text(self, payload: dict[str, Any]) -> str | None:
        result = payload.get("result")
        if not isinstance(result, dict):
            data = payload.get("data")
            result = data if isinstance(data, dict) else None
        if not isinstance(result, dict):
            return None

        layout_results = result.get("layoutParsingResults")
        if isinstance(layout_results, list):
            page_parts: list[str] = []
            for item in layout_results:
                if not isinstance(item, dict):
                    continue
                markdown = item.get("markdown")
                if not isinstance(markdown, dict):
                    continue
                text = markdown.get("text")
                if not isinstance(text, str):
                    continue
                normalized_text = text.strip()
                if normalized_text:
                    page_parts.append(normalized_text)
            if page_parts:
                return "\n\n".join(page_parts)

        markdown = result.get("markdown")
        if isinstance(markdown, dict):
            text = markdown.get("text")
            if isinstance(text, str):
                normalized_text = text.strip()
                if normalized_text:
                    return normalized_text
        return None

def _dump_json_lines(json_lines: list[dict[str, Any]]) -> bytes:
    """
    EN: Serialize a list of JSON objects as newline-delimited JSON bytes.
    CN: 同上。
    """
    return "\n".join(json.dumps(item, ensure_ascii=False) for item in json_lines).encode("utf-8")


def _infer_asset_extension(*, url: str, image_name: str, content_type: str | None) -> str:
    """
    EN: Infer file extension from content type, image name, or URL path, with fallback to .bin.
    CN: 同上。
    """
    # EN: Check candidates in priority order: content_type, image_name extension, URL path extension.
    # CN: 同上。
    for candidate in (
        content_type,
        mimetypes.guess_type(image_name)[0],
        mimetypes.guess_type(urlparse(url).path)[0],
    ):
        if not candidate:
            continue
        if candidate.lower() in {"application/x-ndjson", "application/jsonl"}:
            return ".jsonl"
        if candidate.lower().startswith("image/"):
            extension = mimetypes.guess_extension(candidate)
            if extension:
                return extension
    suffix = PurePosixPath(image_name).suffix
    if suffix:
        return suffix
    suffix = PurePosixPath(urlparse(url).path).suffix
    if suffix:
        return suffix
    return ".bin"


def _safe_filename(value: str) -> str:
    """
    EN: Sanitize a string into a safe filename by replacing non-alphanumeric characters.
    CN: 同上。
    """
    sanitized = "".join(char if char.isalnum() or char in {".", "_", "-"} else "-" for char in value)
    sanitized = sanitized.strip(".-_")
    return sanitized or "asset"


def _collect_markdown_image_urls(markdown_text: str) -> list[str]:
    """
    EN: Collect markdown image URLs in first-seen order while de-duplicating them.
    CN: 按首次出现顺序收集 Markdown 图片 URL 并去重。
    """
    urls: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(r"!\[[^\]]*]\(([^)]+)\)", markdown_text):
        candidate = match.group(1).strip()
        if not candidate:
            continue
        url = candidate.split()[0].strip("<>")
        if not url or url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls
