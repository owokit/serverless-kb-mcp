"""
EN: PaddleOCR manifest builder that converts JSONL results into chunk manifests.
CN: 同上。
"""
from __future__ import annotations

import json
import mimetypes
from pathlib import PurePosixPath
from typing import Any, Callable
from urllib.parse import urlparse

from serverless_mcp.domain.format_specs import get_format_spec
from serverless_mcp.domain.models import ChunkManifest, ExtractedAsset, ExtractedChunk, S3ObjectRef
from serverless_mcp.extract.policy import (
    DEFAULT_POLICY,
    estimate_tokens,
    expand_oversized_chunks,
    normalize_text,
)


class PaddleOCRManifestBuilder:
    """
    EN: Build chunk manifest from PaddleOCR JSONL output with text and image assets.
    CN: 浠?PaddleOCR JSONL 杈撳嚭鏋勫缓鍖呭惈鏂囨湰鍜屽浘鐗囪祫浜х殑 chunk manifest銆?
    """

    def build_manifest(
        self,
        *,
        source: S3ObjectRef,
        json_lines: list[dict[str, Any]],
        binary_loader: Callable[[str], tuple[bytes, str | None]],
    ) -> ChunkManifest:
        """
        EN: Parse PaddleOCR JSONL results and build manifest with split markdown assets.
        CN: 瑙ｆ瀽 PaddleOCR JSONL 缁撴灉骞舵瀯寤哄甫鎷嗗垎 Markdown 璧勪骇鐨?manifest銆?
        """
        # EN: Accumulator variables for chunks, assets, and per-type counters.
        # CN: 同上。
        chunks: list[ExtractedChunk] = []
        assets: list[ExtractedAsset] = []
        chunk_index = 0
        asset_index = 0
        image_asset_count = 0
        layout_markdown_asset_count = 0
        document_markdown_asset_count = 0
        markdown_sections: list[tuple[int, int, str]] = []
        spec = get_format_spec(doc_type="pdf", source_format="paddleocr_async")

        # EN: Preserve raw JSONL as an asset for reproducibility and debugging.
        # CN: 同上。
        raw_json_payload = _dump_json_lines(json_lines)
        assets.append(
            ExtractedAsset(
                asset_id="ocr#raw-jsonl",
                chunk_type="ocr_json_chunk",
                mime_type="application/x-ndjson",
                payload=raw_json_payload,
                page_no=None,
                metadata=spec.asset_metadata(relative_path="raw.jsonl", source_field="json_lines", page_count=len(json_lines)),
            )
        )

        for page_no, item in enumerate(json_lines, start=1):
            result = item.get("result", {})
            layouts = result.get("layoutParsingResults", []) if isinstance(result, dict) else []
            if not isinstance(layouts, list):
                continue

            for layout_index, layout in enumerate(layouts, start=1):
                if not isinstance(layout, dict):
                    continue

                layout_text_parts: list[str] = []
                layout_asset_paths: dict[str, str] = {}
                layout_image_paths: list[str] = []

                markdown = layout.get("markdown") or {}
                if isinstance(markdown, dict):
                    text = markdown.get("text")
                    if isinstance(text, str) and text.strip():
                        layout_text_parts.append(text)

                    markdown_images = markdown.get("images") or {}
                    if isinstance(markdown_images, dict):
                        for image_name, image_url in markdown_images.items():
                            if isinstance(image_name, str) and isinstance(image_url, str):
                                asset_index += 1
                                image_asset_count += 1
                                assets.append(
                                    self._build_image_asset(
                                        asset_id=f"asset#{asset_index:06d}",
                                        page_no=page_no,
                                        binary_loader=binary_loader,
                                        url=image_url,
                                        image_name=image_name,
                                        metadata=spec.asset_metadata(layout_index=layout_index, source_field="markdown.images"),
                                    )
                                )
                                relative_path = assets[-1].metadata["relative_path"]
                                layout_asset_paths[image_url] = relative_path
                                layout_image_paths.append(relative_path)

                output_images = layout.get("outputImages") or {}
                if isinstance(output_images, dict):
                    for image_name, image_url in output_images.items():
                        if isinstance(image_name, str) and isinstance(image_url, str):
                            asset_index += 1
                            image_asset_count += 1
                            assets.append(
                                self._build_image_asset(
                                    asset_id=f"asset#{asset_index:06d}",
                                    page_no=page_no,
                                    binary_loader=binary_loader,
                                    url=image_url,
                                    image_name=image_name,
                                    metadata=spec.asset_metadata(layout_index=layout_index, source_field="outputImages"),
                                )
                            )
                            relative_path = assets[-1].metadata["relative_path"]
                            layout_asset_paths[image_url] = relative_path
                            layout_image_paths.append(relative_path)

                # EN: Normalize text, rewrite remote URLs to local asset paths, and build layout markdown.
                # CN: 同上。
                layout_text = normalize_text("\n\n".join(layout_text_parts))
                rewritten_layout_text = self._rewrite_markdown_links(layout_text, layout_asset_paths) if layout_text else ""
                layout_markdown_payload = rewritten_layout_text
                # EN: Fallback to image-only markdown when no text is available for this layout.
                # CN: 同上。
                if not layout_markdown_payload and layout_image_paths:
                    layout_markdown_payload = "\n\n".join(
                        f"![page-{page_no}-layout-{layout_index}]({path})" for path in layout_image_paths
                    )

                if layout_markdown_payload:
                    markdown_sections.append((page_no, layout_index, layout_markdown_payload))
                    asset_index += 1
                    assets.append(
                        ExtractedAsset(
                            asset_id=f"ocr#markdown-page-{page_no:06d}-layout-{layout_index:03d}",
                            chunk_type="document_markdown_chunk",
                            mime_type="text/markdown",
                            payload=layout_markdown_payload.encode("utf-8"),
                            page_no=page_no,
                            metadata=spec.asset_metadata(
                                relative_path=f"pages/page-{page_no:06d}-layout-{layout_index:03d}.md",
                                source_field="markdown.text" if rewritten_layout_text else "layout.images",
                                page_count=len(json_lines),
                                page_no=page_no,
                                layout_index=layout_index,
                            ),
                        )
                    )
                    layout_markdown_asset_count += 1

                if rewritten_layout_text:
                    chunk_index += 1
                    chunks.append(
                        ExtractedChunk(
                            chunk_id=f"chunk#{chunk_index:06d}",
                            chunk_type="page_text_chunk",
                            text=rewritten_layout_text,
                            doc_type="pdf",
                            token_estimate=estimate_tokens(rewritten_layout_text),
                            page_no=page_no,
                            page_span=(page_no, page_no),
                            section_path=(f"page-{page_no}", f"layout-{layout_index}"),
                            metadata=spec.chunk_metadata(layout_index=layout_index),
                        )
                    )

        markdown_document = self._build_markdown_document(markdown_sections)
        if markdown_document:
            assets.append(
                ExtractedAsset(
                    asset_id="ocr#markdown",
                    chunk_type="document_markdown_chunk",
                    mime_type="text/markdown",
                    payload=markdown_document.encode("utf-8"),
                    page_no=None,
                    metadata=spec.asset_metadata(
                        relative_path="document.md",
                        source_field="markdown.text",
                        page_count=len(json_lines),
                    ),
                )
            )
        document_markdown_asset_count = int(bool(markdown_document))

        # EN: Split oversized chunks to stay within embedding token limits.
        # CN: 鎷嗗垎瓒呭ぇ chunk 浠ヤ繚鎸佸湪 embedding token 闄愬埗鍐呫€?
        chunks = expand_oversized_chunks(chunks, safe_text_tokens=DEFAULT_POLICY.safe_text_tokens)

        return ChunkManifest(
            source=source,
            doc_type="pdf",
            chunks=chunks,
            assets=assets,
            metadata={
                "page_count": len(json_lines),
                "page_image_asset_count": image_asset_count,
                "raw_json_asset_count": 1,
                "layout_markdown_asset_count": layout_markdown_asset_count,
                "document_markdown_asset_count": document_markdown_asset_count,
                "markdown_asset_count": layout_markdown_asset_count + document_markdown_asset_count,
                "ocr_engine": "PaddleOCR-VL-1.5",
                "source_format": spec.source_format,
            },
        )

    def _build_image_asset(
        self,
        *,
        asset_id: str,
        page_no: int,
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

    def _build_markdown_document(self, markdown_sections: list[tuple[int, int, str]]) -> str:
        """
        EN: Assemble a markdown document from page and layout OCR markdown text.
        CN: 同上。
        """
        parts: list[str] = []
        for page_no, layout_index, markdown_text in markdown_sections:
            normalized = markdown_text.strip()
            if not normalized:
                continue
            parts.append(f"<!-- page:{page_no} layout:{layout_index} -->\n\n{normalized}")
        return "\n\n---\n\n".join(parts).strip()


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

