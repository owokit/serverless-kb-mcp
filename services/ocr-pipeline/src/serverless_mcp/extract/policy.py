"""
EN: Embedding policy and text processing utilities for chunking and token estimation.
CN: 同上。
"""
from __future__ import annotations

import math
import re
from functools import lru_cache
from collections.abc import Iterable
from typing import Any

from markdown_it import MarkdownIt

from serverless_mcp.runtime.observability import emit_trace
from serverless_mcp.domain.models import EmbeddingPolicy, ExtractedChunk


TOKEN_CHAR_RATIO = 4
MARKDOWN_PARSER = MarkdownIt("commonmark")
HTML_STYLE_ATTR_PATTERN = re.compile(r"\sstyle=(?:'[^']*'|\"[^\"]*\")", re.IGNORECASE)


@lru_cache(maxsize=1)
def _get_token_encoder() -> Any | None:
    """
    EN: Load an optional token encoder from the open-source tiktoken package.
    CN: 从开源 tiktoken 包加载可选的 token 编码器。
    """
    try:
        import tiktoken
    except ImportError:
        emit_trace("extract.tiktoken_encoder.unavailable", reason="import_error")
        return None

    try:
        return tiktoken.get_encoding("cl100k_base")
    except (LookupError, OSError, RuntimeError, TypeError, ValueError) as exc:
        emit_trace(
            "extract.tiktoken_encoder.unavailable",
            reason="encoding_load_failed",
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
        return None


def estimate_tokens(text: str) -> int:
    """
    EN: Estimate token count using character-to-token ratio heuristic.
    CN: 同上。

    Args:
        text:
            EN: Input text to estimate.
            CN: 杈撳叆寰呬及绠楃殑鏂囨湰銆?

    Returns:
        EN: Estimated token count, minimum 1 for non-empty text.
        CN: 闈炵┖鏂囨湰鐨勬渶灏忎及绠楀€间负 1銆?
    """
    normalized = re.sub(r"\s+", " ", text).strip()
    if not normalized:
        return 0

    encoder = _get_token_encoder()
    if encoder is not None:
        try:
            return max(1, len(encoder.encode(normalized)))
        except (LookupError, OSError, RuntimeError, TypeError, ValueError):
            pass

    return max(1, math.ceil(len(normalized) / TOKEN_CHAR_RATIO))


def normalize_text(text: str) -> str:
    """
    EN: Normalize text by collapsing excessive newlines, stripping inline style attributes, and trimming whitespace.
    CN: 同上。

    Args:
        text:
            EN: Raw input text possibly containing CR/LF, HTML style attrs, or excessive blank lines.
            CN: 同上。

    Returns:
        EN: Cleaned text with consistent LF line endings and collapsed blank lines.
        CN: 同上。
    """
    normalized = text.replace("\r\n", "\n")
    normalized = HTML_STYLE_ATTR_PATTERN.sub("", normalized)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized.strip()


def section_hint_from_markdown(markdown_text: str) -> list[tuple[tuple[str, ...], str]]:
    """
    EN: Split Markdown into ordered sections using a standards-based parser for heading boundaries.
    CN: 同上。

    Args:
        markdown_text:
            EN: Normalized Markdown text with optional heading structure.
            CN: 同上。

    Returns:
        EN: List of (section_path, section_text) tuples where section_path is the heading breadcrumb.
        CN: 同上。
    """
    text = normalize_text(markdown_text)
    if not text:
        return []

    lines = text.splitlines()
    tokens = MARKDOWN_PARSER.parse(text)
    heading_markers: list[tuple[int, int, str]] = []

    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token.type == "heading_open" and token.map:
            level = int(token.tag[1:]) if token.tag.startswith("h") and token.tag[1:].isdigit() else 1
            heading = ""
            if index + 1 < len(tokens) and tokens[index + 1].type == "inline":
                heading = normalize_text(tokens[index + 1].content)
            heading_markers.append((token.map[0], level, heading))
        index += 1

    if not heading_markers:
        return [((), text)] if text else []

    sections: list[tuple[tuple[str, ...], str]] = []
    stack: list[str] = []
    current_start = 0
    current_path: tuple[str, ...] = ()

    for next_start, level, heading in heading_markers:
        content = normalize_text("\n".join(lines[current_start:next_start]))
        if content:
            sections.append((current_path, content))
        stack[:] = stack[: level - 1]
        if heading:
            stack.append(heading)
        current_path = tuple(stack)
        current_start = next_start

    content = normalize_text("\n".join(lines[current_start:]))
    if content:
        sections.append((current_path, content))

    return [(path, content) for path, content in sections if content]


DEFAULT_POLICY = EmbeddingPolicy()

# EN: Ordered break markers from strongest (heading) to weakest (space) for split boundaries.
# CN: 同上。
STRUCTURAL_BREAK_MARKERS = ("\n# ", "\n## ", "\n### ", "\n#### ", "\n##### ", "\n###### ", "\n```", "\n~~~", "\n\n", "\n", " ")

ATX_HEADING_PATTERN = re.compile(r"^#{1,6}\s+\S")
FENCE_PATTERN = re.compile(r"^(```+|~~~+)")


def split_text_for_embedding(
    text: str,
    *,
    max_tokens: int,
    preferred_breaks: Iterable[str] = STRUCTURAL_BREAK_MARKERS,
) -> list[str]:
    """
    EN: Split text into Markdown-aware chunks, falling back to tight token windows when needed.
    CN: 鍏堟寜 Markdown 缁撴瀯鍒囧潡锛屾棤娉曟弧瓒抽檺鍒舵椂鍥為€€鍒扮揣鍑戠殑 token 绐楀彛銆?

    Args:
        text:
            EN: Input text to split.
            CN: 待拆分的输入文本。
        max_tokens:
            EN: Maximum token count per output chunk.
            CN: 每个输出 chunk 的最大 token 数。
        preferred_breaks:
            EN: Ordered list of break markers to try for split boundaries.
            CN: 用于拆分边界的有序断点标记列表。

    Returns:
        EN: List of text chunks each within the token limit.
        CN: 每个都在 token 限制内的文本 chunk 列表。
    """
    text = normalize_text(text)
    if not text:
        return []

    max_tokens = max(1, max_tokens)
    blocks = _split_markdown_sections(text)
    parts: list[str] = []
    for block in blocks:
        parts.extend(_pack_markdown_block(block, max_tokens=max_tokens, preferred_breaks=tuple(preferred_breaks)))
    return [part for part in parts if part]


def expand_oversized_chunks(
    chunks: Iterable[ExtractedChunk],
    *,
    safe_text_tokens: int,
) -> list[ExtractedChunk]:
    """
    EN: Split oversized chunks repeatedly until every text chunk fits the embedding token limit.
    CN: 同上。

    Args:
        chunks:
            EN: Input chunks from the extraction stage.
            CN: 同上。
        safe_text_tokens:
            EN: Maximum token count per chunk; anything larger is recursively split.
            CN: 同上。

    Returns:
        EN: Expanded list of chunks all within the token limit, preserving metadata lineage.
        CN: 全部都在 token 限制内的扩展 chunk 列表，保留元数据来源链。
    """
    safe_text_tokens = max(1, safe_text_tokens)
    # EN: BFS-style queue: oversized chunks are re-enqueued for further splitting.
    # CN: 同上。
    queue: list[ExtractedChunk] = list(chunks)
    expanded: list[ExtractedChunk] = []

    while queue:
        chunk = queue.pop(0)
        normalized_text = normalize_text(chunk.text)
        token_estimate = chunk.token_estimate if chunk.token_estimate > 0 else estimate_tokens(normalized_text)

        if token_estimate <= safe_text_tokens:
            expanded.append(
                ExtractedChunk(
                    chunk_id=chunk.chunk_id,
                    chunk_type=chunk.chunk_type,
                    text=normalized_text,
                    doc_type=chunk.doc_type,
                    token_estimate=token_estimate,
                    page_no=chunk.page_no,
                    page_span=chunk.page_span,
                    slide_no=chunk.slide_no,
                    section_path=chunk.section_path,
                    metadata=chunk.metadata,
                )
            )
            continue

        split_parts = split_text_for_embedding(normalized_text, max_tokens=safe_text_tokens)
        if not split_parts or (len(split_parts) == 1 and normalize_text(split_parts[0]) == normalized_text):
            split_parts = _force_split_text(normalized_text, max_tokens=safe_text_tokens)

        for index, part in enumerate(split_parts, start=1):
            part = normalize_text(part)
            if not part:
                continue

            part_tokens = estimate_tokens(part)
            part_chunk = ExtractedChunk(
                chunk_id=f"{chunk.chunk_id}#part{index:02d}",
                chunk_type=chunk.chunk_type,
                text=part,
                doc_type=chunk.doc_type,
                token_estimate=part_tokens,
                page_no=chunk.page_no,
                page_span=chunk.page_span,
                slide_no=chunk.slide_no,
                section_path=chunk.section_path,
                metadata={
                    **chunk.metadata,
                    "split_from": chunk.metadata.get("split_from", chunk.chunk_id),
                    "split_index": index,
                },
            )

            if part_tokens > safe_text_tokens and part != normalized_text:
                queue.insert(0, part_chunk)
                continue

            if part_tokens > safe_text_tokens:
                forced_parts = _force_split_text(part, max_tokens=safe_text_tokens)
                if len(forced_parts) <= 1 and normalize_text(forced_parts[0]) == part:
                    expanded.append(part_chunk)
                    continue
                for forced_index, forced_part in enumerate(forced_parts, start=1):
                    forced_part = normalize_text(forced_part)
                    if not forced_part:
                        continue
                    queue.insert(
                        0,
                        ExtractedChunk(
                            chunk_id=f"{part_chunk.chunk_id}#sub{forced_index:02d}",
                            chunk_type=chunk.chunk_type,
                            text=forced_part,
                            doc_type=chunk.doc_type,
                            token_estimate=estimate_tokens(forced_part),
                            page_no=chunk.page_no,
                            page_span=chunk.page_span,
                            slide_no=chunk.slide_no,
                            section_path=chunk.section_path,
                            metadata={
                                **part_chunk.metadata,
                                "split_from": part_chunk.metadata.get("split_from", part_chunk.chunk_id),
                                "split_index": forced_index,
                            },
                        ),
                    )
                continue

            expanded.append(part_chunk)

    return expanded


def _split_markdown_sections(text: str) -> list[str]:
    """
    EN: Split Markdown text into coarse blocks at heading and fenced-code boundaries.
    CN: 按标题和代码围栏边界将 Markdown 文本拆分为粗粒度块。

    Args:
        text:
            EN: Normalized Markdown text.
            CN: 规范化后的 Markdown 文本。

    Returns:
        EN: List of non-empty Markdown blocks split at structural boundaries.
        CN: 同上。
    """
    lines = text.splitlines()
    sections: list[str] = []
    current: list[str] = []
    in_fence = False

    for line in lines:
        stripped = line.lstrip()
        fence = _fence_delimiter(stripped)

        if fence:
            if not in_fence and current and any(part.strip() for part in current):
                sections.append(normalize_text("\n".join(current)))
                current = []
            in_fence = not in_fence
            current.append(line)
            if not in_fence:
                sections.append(normalize_text("\n".join(current)))
                current = []
            continue

        if not in_fence and _is_heading_line(line):
            if current and any(part.strip() for part in current):
                sections.append(normalize_text("\n".join(current)))
            current = [line]
            continue

        current.append(line)

    if current and any(part.strip() for part in current):
        sections.append(normalize_text("\n".join(current)))

    return sections if sections else ([text] if text else [])


def _pack_markdown_block(block: str, *, max_tokens: int, preferred_breaks: tuple[str, ...]) -> list[str]:
    """
    EN: Pack a Markdown block into token-safe chunks by splitting at structural break points.
    CN: 同上。

    Args:
        block:
            EN: A single Markdown block to pack.
            CN: 待打包的单个 Markdown 块。
        max_tokens:
            EN: Maximum token count per output chunk.
            CN: 每个输出 chunk 的最大 token 数。
        preferred_breaks:
            EN: Ordered break markers for split boundary selection.
            CN: 用于拆分边界选择的有序断点标记。

    Returns:
        EN: List of token-safe text chunks.
        CN: token 安全的文本 chunk 列表。
    """
    if estimate_tokens(block) <= max_tokens:
        return [block]

    units = _split_markdown_units(block)
    if len(units) == 1:
        return _split_oversized_unit(units[0], max_tokens=max_tokens, preferred_breaks=preferred_breaks)

    parts: list[str] = []
    current: list[str] = []
    current_tokens = 0

    for unit in units:
        unit = normalize_text(unit)
        if not unit:
            continue

        unit_tokens = estimate_tokens(unit)
        if unit_tokens > max_tokens:
            if current:
                parts.append("\n\n".join(current))
                current = []
                current_tokens = 0

            parts.extend(_split_oversized_unit(unit, max_tokens=max_tokens, preferred_breaks=preferred_breaks))
            continue

        if current and current_tokens + unit_tokens + 1 > max_tokens:
            parts.append("\n\n".join(current))
            current = [unit]
            current_tokens = unit_tokens
            continue

        current.append(unit)
        current_tokens = unit_tokens if len(current) == 1 else current_tokens + unit_tokens + 1

    if current:
        parts.append("\n\n".join(current))

    return [normalize_text(part) for part in parts if normalize_text(part)]


def _split_markdown_units(block: str) -> list[str]:
    """
    EN: Split a Markdown block into atomic units separated by blank lines or fenced code blocks.
    CN: 按空行或围栏代码块将 Markdown 块拆分为原子单元。

    Args:
        block:
            EN: A Markdown block text.
            CN: 同上。

    Returns:
        EN: List of atomic Markdown units (paragraphs or fenced code blocks).
        CN: 同上。
    """
    lines = block.splitlines()
    units: list[str] = []
    current: list[str] = []
    in_fence = False
    fence_marker = ""

    for line in lines:
        stripped = line.strip()
        fence = _fence_delimiter(stripped)

        if fence:
            if not in_fence and current and any(part.strip() for part in current):
                units.append(normalize_text("\n".join(current)))
                current = []
            if not in_fence:
                in_fence = True
                fence_marker = fence
            current.append(line)
            if in_fence and stripped.startswith(fence_marker):
                units.append(normalize_text("\n".join(current)))
                current = []
                in_fence = False
                fence_marker = ""
            continue

        if not in_fence and not stripped:
            if current and any(part.strip() for part in current):
                units.append(normalize_text("\n".join(current)))
                current = []
            continue

        current.append(line)

    if current and any(part.strip() for part in current):
        units.append(normalize_text("\n".join(current)))

    return units if units else ([block] if block else [])


def _split_oversized_unit(unit: str, *, max_tokens: int, preferred_breaks: tuple[str, ...]) -> list[str]:
    """
    EN: Split an oversized atomic unit that exceeds the token limit into smaller fragments.
    CN: 将超过 token 限制的超大原子单元拆成更小片段。

    Args:
        unit:
            EN: A single Markdown unit that exceeds max_tokens.
            CN: 超过 max_tokens 的单个 Markdown 单元。
        max_tokens:
            EN: Maximum token count per output fragment.
            CN: 同上。
        preferred_breaks:
            EN: Ordered break markers for split boundary selection.
            CN: 用于拆分边界选择的有序断点标记。

    Returns:
            EN: List of token-safe text fragments.
            CN: 同上。
    """
    encoder = _get_token_encoder()
    if encoder is not None:
        return _split_text_with_encoder(
            unit,
            max_tokens=max_tokens,
            preferred_breaks=preferred_breaks,
            encoder=encoder,
        )

    remaining = normalize_text(unit)
    if not remaining:
        return []

    parts: list[str] = []
    max_chars = max(1, (max_tokens * TOKEN_CHAR_RATIO) // 2)

    while remaining:
        if estimate_tokens(remaining) <= max_tokens:
            parts.append(remaining)
            break

        window = remaining[:max_chars]
        cut = _find_best_char_cut(window, preferred_breaks)
        if cut <= 0:
            cut = min(max_chars, len(remaining))

        part = normalize_text(remaining[:cut])
        if not part:
            part = normalize_text(remaining[:max_chars])
            cut = min(max_chars, len(remaining))

        parts.append(part)
        remaining = remaining[cut:].lstrip()

    return [part for part in parts if part]


def _split_text_with_encoder(
    text: str,
    *,
    max_tokens: int,
    preferred_breaks: Iterable[str],
    encoder: Any,
) -> list[str]:
    """
    EN: Split text with exact token boundaries when an encoder is available.
    CN: 在编码器可用时，按精确 token 边界拆分文本。
    """
    tokens = encoder.encode(text)
    if len(tokens) <= max_tokens:
        return [text]

    parts: list[str] = []
    start = 0
    total = len(tokens)
    preferred_breaks = tuple(preferred_breaks)

    while start < total:
        end = min(start + max_tokens, total)
        if end == total:
            part = normalize_text(encoder.decode(tokens[start:end]))
            if part:
                parts.append(part)
            break

        candidate = encoder.decode(tokens[start:end])
        cut_end = end
        midpoint = len(candidate) // 2

        for marker in preferred_breaks:
            marker_index = candidate.rfind(marker)
            if marker_index <= midpoint:
                continue

            prefix = candidate[: marker_index + len(marker.rstrip())]
            prefix_tokens = len(encoder.encode(prefix))
            if 0 < prefix_tokens < (end - start):
                cut_end = start + prefix_tokens
                break

        part = normalize_text(encoder.decode(tokens[start:cut_end]))
        if not part:
            part = normalize_text(encoder.decode(tokens[start:end]))
            cut_end = end

        parts.append(part)
        start = cut_end

    return parts


def _force_split_text(text: str, *, max_tokens: int) -> list[str]:
    """
    EN: Force-split text by exact token boundaries when no natural break point exists.
    CN: 同上。

    Args:
        text:
            EN: Text to force-split.
            CN: 待强制拆分的文本。
        max_tokens:
            EN: Maximum token count per output fragment.
            CN: 同上。

    Returns:
        EN: List of text fragments each within the token limit.
        CN: 同上。
    """
    encoder = _get_token_encoder()
    if encoder is not None:
        tokens = encoder.encode(text)
        if len(tokens) <= max_tokens:
            return [normalize_text(text)] if text else []

        parts: list[str] = []
        start = 0
        while start < len(tokens):
            end = min(start + max_tokens, len(tokens))
            part = normalize_text(encoder.decode(tokens[start:end]))
            if part:
                parts.append(part)
            if end <= start:
                end = min(start + 1, len(tokens))
            start = end
        return parts

    safe_chars = max(1, (max_tokens * TOKEN_CHAR_RATIO) // 2)
    if len(text) <= safe_chars:
        return [normalize_text(text)] if text else []

    parts = [normalize_text(text[index:index + safe_chars]) for index in range(0, len(text), safe_chars)]
    return [part for part in parts if part]


def _find_best_char_cut(window: str, preferred_breaks: tuple[str, ...]) -> int:
    """
    EN: Find the best character offset to cut a window, preferring markers after the midpoint.
    CN: 同上。

    Args:
        window:
            EN: Text window to search for a cut point.
            CN: 同上。
        preferred_breaks:
            EN: Ordered break markers to try.
            CN: 同上。

    Returns:
        EN: Character offset after the matched marker, or -1 if no suitable marker found.
            CN: 同上。
    """
    if not window:
        return 0

    cutoff = -1
    half_window = max(1, len(window) // 2)
    for marker in preferred_breaks:
        index = window.rfind(marker)
        if index >= half_window:
            cutoff = index + len(marker.rstrip())
            break
    return cutoff


def _is_heading_line(line: str) -> bool:
    """
    EN: Check whether a line is an ATX-style Markdown heading.
    CN: 同上。

    Args:
        line:
            EN: A single line of text.
            CN: 同上。

    Returns:
        EN: True if the line matches the ATX heading pattern.
        CN: 同上。
    """
    return bool(ATX_HEADING_PATTERN.match(line.strip()))


def _fence_delimiter(line: str) -> str:
    """
    EN: Extract the fence delimiter string if the line opens or closes a fenced code block.
    CN: 同上。

    Args:
        line:
            EN: A single line of text.
            CN: 同上。

    Returns:
        EN: The matched fence delimiter (e.g., "```"), or empty string if not a fence.
        CN: 同上。
    """
    match = FENCE_PATTERN.match(line)
    return match.group(1) if match else ""
