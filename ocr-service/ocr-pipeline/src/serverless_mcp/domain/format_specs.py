"""
EN: Format specification registry for document extraction metadata and schema alignment.
CN: 文档抽取元数据与 schema 对齐用的格式规格注册表。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class FormatSpec:
    """
    EN: Declarative format contract used by extractors and schema validators.
    CN: 供 extractor 和 schema validator 共同使用的声明式格式契约。
    """

    doc_type: str
    source_format: str
    manifest_required_keys: tuple[str, ...]
    chunk_required_keys: tuple[str, ...] = ("source_format",)
    asset_required_keys: tuple[str, ...] = ("source_format",)
    chunk_defaults: tuple[tuple[str, Any], ...] = ()
    manifest_defaults: tuple[tuple[str, Any], ...] = ()
    asset_defaults: tuple[tuple[str, Any], ...] = ()

    def chunk_metadata(self, **metadata: Any) -> dict[str, Any]:
        """
        EN: Build chunk-level metadata dict by merging defaults with caller-supplied overrides.
        CN: 将默认值与调用方覆盖值合并，生成 chunk 级别的元数据字典。

        Args:
            **metadata:
                EN: Caller-supplied metadata fields to override defaults.
                CN: 调用方提供的用于覆盖默认值的元数据字段。

        Returns:
            EN: Merged metadata dict with source_format injected.
            CN: 已注入 source_format 的合并元数据字典。
        """
        return _merge_metadata(self.chunk_defaults, metadata, source_format=self.source_format)

    def asset_metadata(self, **metadata: Any) -> dict[str, Any]:
        """
        EN: Build asset-level metadata dict by merging defaults with caller-supplied overrides.
        CN: 将默认值与调用方覆盖值合并，生成资产级别的元数据字典。

        Args:
            **metadata:
                EN: Caller-supplied metadata fields to override defaults.
                CN: 调用方提供的用于覆盖默认值的元数据字段。

        Returns:
            EN: Merged metadata dict with source_format injected.
            CN: 已注入 source_format 的合并元数据字典。
        """
        return _merge_metadata(self.asset_defaults, metadata, source_format=self.source_format)

    def manifest_metadata(self, **metadata: Any) -> dict[str, Any]:
        """
        EN: Build manifest-level metadata dict by merging defaults with caller-supplied overrides.
        CN: 将默认值与调用方覆盖值合并，生成 manifest 级别的元数据字典。

        Args:
            **metadata:
                EN: Caller-supplied metadata fields to override defaults.
                CN: 调用方提供的用于覆盖默认值的元数据字段。

        Returns:
            EN: Merged metadata dict with source_format injected.
            CN: 已注入 source_format 的合并元数据字典。
        """
        return _merge_metadata(self.manifest_defaults, metadata, source_format=self.source_format)


FORMAT_SPECS: tuple[FormatSpec, ...] = (
    # EN: Markdown — plain text with heading structure.
    # CN: Markdown — 具有标题结构的纯文本。
    FormatSpec(
        doc_type="md",
        source_format="markdown",
        manifest_required_keys=("source_format", "section_count"),
    ),
    # EN: DOCX — converted via python-docx to Markdown, includes converter tracking.
    # CN: DOCX — 通过 python-docx 转换为 Markdown，包含转换器追踪。
    FormatSpec(
        doc_type="docx",
        source_format="python-docx",
        manifest_required_keys=("source_format", "section_count"),
        chunk_required_keys=("source_format", "converter"),
        chunk_defaults=(("converter", "python-docx"),),
    ),
    # EN: PPTX — slides with text and embedded images.
    # CN: PPTX — 包含文本和嵌入图像的幻灯片。
    FormatSpec(
        doc_type="pptx",
        source_format="python-pptx",
        manifest_required_keys=("source_format", "slide_count", "image_asset_count"),
        chunk_required_keys=("source_format", "image_count", "has_notes"),
    ),
    # EN: PDF — native text extraction via pypdf.
    # CN: PDF — 通过 pypdf 进行原生文本提取。
    FormatSpec(
        doc_type="pdf",
        source_format="pdf",
        manifest_required_keys=("source_format", "page_count", "page_image_asset_count", "visual_page_numbers"),
    ),
    # EN: PDF via PaddleOCR — async OCR with layout analysis and multiple markdown outputs.
    # CN: PDF 经 PaddleOCR — 异步 OCR，含版面分析和多种 markdown 输出。
    FormatSpec(
        doc_type="pdf",
        source_format="paddleocr_async",
        manifest_required_keys=(
            "source_format",
            "page_count",
            "page_image_asset_count",
            "raw_json_asset_count",
            "layout_markdown_asset_count",
            "document_markdown_asset_count",
            "markdown_asset_count",
            "ocr_engine",
        ),
        chunk_required_keys=("source_format", "layout_index"),
        asset_required_keys=("source_format", "relative_path", "source_field"),
    ),
)

# EN: Index by (doc_type, source_format) for O(1) lookup in extractors.
# CN: 以 (doc_type, source_format) 为键建立索引，供 extractor 做 O(1) 查找。
FORMAT_SPECS_BY_KEY: dict[tuple[str, str], FormatSpec] = {
    (spec.doc_type, spec.source_format): spec for spec in FORMAT_SPECS
}


def get_format_spec(*, doc_type: str, source_format: str) -> FormatSpec:
    """
    EN: Resolve a registered format spec for one doc_type/source_format pair.
    CN: 为一个 doc_type/source_format 组合解析已注册的格式规格。
    """
    key = (doc_type, source_format)
    try:
        return FORMAT_SPECS_BY_KEY[key]
    except KeyError as exc:  # pragma: no cover - defensive branch
        raise KeyError(f"Unsupported format spec: doc_type={doc_type!r}, source_format={source_format!r}") from exc


def _merge_metadata(defaults: tuple[tuple[str, Any], ...], metadata: dict[str, Any], *, source_format: str) -> dict[str, Any]:
    """
    EN: Merge default key-value pairs with caller metadata, always injecting source_format.
    CN: 将默认键值对与调用方元数据合并，并始终注入 source_format。

    Args:
        defaults:
            EN: Default key-value pairs defined in the format spec.
            CN: 格式规格中定义的默认键值对。
        metadata:
            EN: Caller-supplied metadata overrides.
            CN: 调用方提供的元数据覆盖值。
        source_format:
            EN: Source format identifier to inject into the result.
            CN: 要注入到结果中的源格式标识符。

    Returns:
        EN: Merged metadata dict with defaults as base, metadata on top, and source_format guaranteed.
        CN: 以默认值为基础、调用方元数据叠加、source_format 保证存在的合并字典。
    """
    merged = {key: value for key, value in defaults}
    merged.update(metadata)
    merged["source_format"] = source_format
    return merged

