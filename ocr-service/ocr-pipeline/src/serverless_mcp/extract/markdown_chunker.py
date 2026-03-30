"""
EN: Markdown-aware chunking helpers for Lambda-friendly embedding pipelines.
CN: 面向 Lambda 的 Markdown 感知 chunking 辅助工具。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
import re
from collections.abc import Callable, Sequence
from typing import Any

from markdown_it import MarkdownIt


@dataclass(slots=True)
class MarkdownBlock:
    """
    EN: Syntax-aware Markdown block extracted from a parsed token stream.
    CN: 从解析后的 token 流中提取的具备语法感知的 Markdown block。
    """

    block_type: str
    text: str
    header_path: tuple[str, ...]
    start_line: int
    end_line: int
    is_atomic: bool
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class MarkdownChunk:
    """
    EN: Final Markdown chunk ready for manifest materialization and embedding.
    CN: 可直接用于 manifest 落盘和 embedding 的最终 Markdown chunk。
    """

    text: str
    header_path: tuple[str, ...]
    token_estimate: int
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class MarkdownSection:
    """
    EN: Coarse section assembled from Markdown blocks under one breadcrumb path.
    CN: 由同一面包屑路径下的 Markdown block 聚合得到的粗粒度 section。
    """

    header_path: tuple[str, ...]
    blocks: list[MarkdownBlock]
    start_line: int
    end_line: int
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def text(self) -> str:
        """
        EN: Render the section as Markdown text separated by blank lines.
        CN: 用空行分隔后将 section 渲染为 Markdown 文本。
        """
        return "\n\n".join(block.text for block in self.blocks if block.text).strip()


DEFAULT_SOFT_TOKEN_RATIO = 0.82
ATX_HEADING_PATTERN = re.compile(r"^#{1,6}\s+\S")
FENCE_PATTERN = re.compile(r"^(```+|~~~+)")
ATOMIC_OPEN_TYPES = {"blockquote_open", "bullet_list_open", "ordered_list_open", "table_open"}
ATOMIC_BLOCK_TYPES = {"fence", "code_block", "html_block"}


@lru_cache(maxsize=1)
def _markdown_parser() -> MarkdownIt:
    """
    EN: Build a Markdown parser with CommonMark plus table support.
    CN: 构建支持 table 的 CommonMark Markdown 解析器。
    """
    parser = MarkdownIt("commonmark")
    try:
        parser.enable("table")
    except (AttributeError, KeyError, ValueError):
        pass
    return parser


def normalize_markdown_text(text: str) -> str:
    """
    EN: Normalize Markdown text by collapsing CRLF and trimming extra blank lines.
    CN: 规范化 Markdown 文本，折叠 CRLF 并收敛多余空行。
    """
    normalized = text.replace("\r\n", "\n")
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized.strip()


def parse_markdown_blocks(markdown_text: str, *, token_counter: Callable[[str], int] | None = None) -> list[MarkdownBlock]:
    """
    EN: Parse Markdown into syntax-aware blocks with breadcrumb metadata.
    CN: 将 Markdown 解析为带面包屑元数据的语法感知 block。
    """
    text = normalize_markdown_text(markdown_text)
    if not text:
        return []

    lines = text.splitlines()
    tokens = _markdown_parser().parse(text)
    blocks: list[MarkdownBlock] = []
    consumed_spans: list[tuple[int, int]] = []
    header_stack: list[str] = []

    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token.type == "heading_open" and token.map:
            level = _heading_level(token)
            title = _extract_inline_text(tokens, index + 1)
            header_stack[:] = header_stack[: max(0, level - 1)]
            if title:
                header_stack.append(title)
            start_line, end_line = token.map
            block_text = _slice_lines(lines, start_line, end_line)
            if block_text:
                blocks.append(
                    MarkdownBlock(
                        block_type=f"heading_h{level}",
                        text=block_text,
                        header_path=tuple(header_stack),
                        start_line=start_line + 1,
                        end_line=end_line,
                        is_atomic=True,
                        metadata={
                            "level": level,
                            "line_span": (start_line + 1, end_line),
                            "token_estimate": token_counter(block_text) if token_counter else None,
                        },
                    )
                )
                consumed_spans.append((start_line, end_line))
            index += 1
            continue

        if token.type in ATOMIC_BLOCK_TYPES and token.map:
            start_line, end_line = token.map
            if _is_covered_by_span(start_line, end_line, consumed_spans):
                index += 1
                continue
            block_text = _slice_lines(lines, start_line, end_line)
            if block_text:
                blocks.append(
                    MarkdownBlock(
                        block_type=_classify_atomic_token(token.type),
                        text=block_text,
                        header_path=tuple(header_stack),
                        start_line=start_line + 1,
                        end_line=end_line,
                        is_atomic=True,
                        metadata={
                            "token_type": token.type,
                            "line_span": (start_line + 1, end_line),
                            "token_estimate": token_counter(block_text) if token_counter else None,
                        },
                    )
                )
                consumed_spans.append((start_line, end_line))
            index += 1
            continue

        if token.type in ATOMIC_OPEN_TYPES and token.map:
            start_line, end_line = token.map
            if _is_covered_by_span(start_line, end_line, consumed_spans):
                index += 1
                continue
            block_text = _slice_lines(lines, start_line, end_line)
            if block_text:
                blocks.append(
                    MarkdownBlock(
                        block_type=_classify_atomic_token(token.type),
                        text=block_text,
                        header_path=tuple(header_stack),
                        start_line=start_line + 1,
                        end_line=end_line,
                        is_atomic=True,
                        metadata={
                            "token_type": token.type,
                            "line_span": (start_line + 1, end_line),
                            "token_estimate": token_counter(block_text) if token_counter else None,
                        },
                    )
                )
                consumed_spans.append((start_line, end_line))
            index += 1
            continue

        if token.type == "paragraph_open" and token.map:
            start_line, end_line = token.map
            if _is_covered_by_span(start_line, end_line, consumed_spans):
                index += 1
                continue
            block_text = _slice_lines(lines, start_line, end_line)
            if block_text:
                blocks.append(
                    MarkdownBlock(
                        block_type="paragraph",
                        text=block_text,
                        header_path=tuple(header_stack),
                        start_line=start_line + 1,
                        end_line=end_line,
                        is_atomic=False,
                        metadata={
                            "token_type": token.type,
                            "line_span": (start_line + 1, end_line),
                            "token_estimate": token_counter(block_text) if token_counter else None,
                        },
                    )
                )
                consumed_spans.append((start_line, end_line))
            index += 1
            continue

        index += 1

    return [block for block in blocks if block.text]


def group_blocks_into_sections(blocks: Sequence[MarkdownBlock]) -> list[MarkdownSection]:
    """
    EN: Group parsed blocks into breadcrumb-aligned coarse sections.
    CN: 按面包屑路径将解析后的 block 聚合为粗粒度 section。
    """
    sections: list[MarkdownSection] = []
    current_blocks: list[MarkdownBlock] = []
    current_path: tuple[str, ...] = ()
    current_start = 0
    current_end = 0

    for block in blocks:
        if block.block_type.startswith("heading_h"):
            if current_blocks:
                sections.append(
                    MarkdownSection(
                        header_path=current_path,
                        blocks=current_blocks,
                        start_line=current_start,
                        end_line=current_end,
                        metadata={
                            "section_kind": "heading_section",
                            "block_count": len(current_blocks),
                            "line_span": (current_start, current_end),
                        },
                    )
                )
                current_blocks = []

            current_path = block.header_path
            current_blocks = [block]
            current_start = block.start_line
            current_end = block.end_line
            continue

        if not current_blocks:
            current_path = block.header_path
            current_start = block.start_line

        current_blocks.append(block)
        current_end = block.end_line

    if current_blocks:
        sections.append(
            MarkdownSection(
                header_path=current_path,
                blocks=current_blocks,
                start_line=current_start,
                end_line=current_end,
                metadata={
                    "section_kind": "preamble" if not current_path else "heading_section",
                    "block_count": len(current_blocks),
                    "line_span": (current_start, current_end),
                },
            )
        )

    return sections


def split_markdown_for_embedding(
    markdown_text: str,
    *,
    soft_token_target: int,
    hard_token_limit: int,
    token_counter: Callable[[str], int],
    tokenizer: Any | None = None,
) -> list[MarkdownChunk]:
    """
    EN: Split Markdown into token-safe chunks while preserving syntax-aware sections.
    CN: 在保留语法感知 section 的前提下，将 Markdown 拆成 token 安全的 chunks。
    """
    text = normalize_markdown_text(markdown_text)
    if not text:
        return []

    soft_token_target = max(1, soft_token_target)
    hard_token_limit = max(soft_token_target, hard_token_limit)

    blocks = parse_markdown_blocks(text, token_counter=token_counter)
    if not blocks:
        blocks = [
            MarkdownBlock(
                block_type="paragraph",
                text=text,
                header_path=(),
                start_line=1,
                end_line=len(text.splitlines()) or 1,
                is_atomic=True,
                metadata={"token_estimate": token_counter(text)},
            )
        ]

    sections = group_blocks_into_sections(blocks)
    chunks: list[MarkdownChunk] = []
    chunk_index = 0
    chunker = _build_semchunk_chunker(token_counter=token_counter, soft_token_target=soft_token_target)

    for section_index, section in enumerate(sections, start=1):
        section_text = section.text
        if not section_text:
            continue

        section_tokens = token_counter(section_text)
        if section_tokens <= soft_token_target:
            chunk_index += 1
            chunks.append(
                _build_chunk(
                    text=section_text,
                    header_path=section.header_path,
                    token_counter=token_counter,
                    chunk_index=chunk_index,
                    section_index=section_index,
                    part_index=None,
                    total_parts=1,
                    section_metadata=section.metadata,
                )
            )
            continue

        section_parts = _split_section_blocks(
            section,
            token_counter=token_counter,
            soft_token_target=soft_token_target,
            hard_token_limit=hard_token_limit,
            chunker=chunker,
            tokenizer=tokenizer,
        )
        if not section_parts:
            section_parts = [section_text]

        for part_index, part in enumerate(section_parts, start=1):
            normalized_part = normalize_markdown_text(part)
            if not normalized_part:
                continue

            part_tokens = token_counter(normalized_part)
            if part_tokens > hard_token_limit:
                forced_parts = _force_split_markdown_text(
                    normalized_part,
                    hard_token_limit=hard_token_limit,
                    token_counter=token_counter,
                    tokenizer=tokenizer,
                )
            else:
                forced_parts = [normalized_part]

            for forced_index, forced_part in enumerate(forced_parts, start=1):
                forced_part = normalize_markdown_text(forced_part)
                if not forced_part:
                    continue
                chunk_index += 1
                chunks.append(
                    _build_chunk(
                        text=forced_part,
                        header_path=section.header_path,
                        token_counter=token_counter,
                        chunk_index=chunk_index,
                        section_index=section_index,
                        part_index=part_index,
                        total_parts=len(section_parts),
                        section_metadata=section.metadata,
                        split_index=forced_index if len(forced_parts) > 1 else None,
                    )
                )

    return _ensure_hard_limit(chunks, hard_token_limit=hard_token_limit, token_counter=token_counter, tokenizer=tokenizer)


def _split_section_blocks(
    section: MarkdownSection,
    *,
    token_counter: Callable[[str], int],
    soft_token_target: int,
    hard_token_limit: int,
    chunker: Callable[[str], list[str]],
    tokenizer: Any | None,
) -> list[str]:
    """
    EN: Split an oversized section by packing Markdown blocks instead of slicing the whole section text.
    CN: 将超大的 section 按 Markdown block 打包切分，而不是直接切整段 section 文本。
    """
    packed_chunks: list[str] = []
    current_parts: list[str] = []

    def flush_current() -> None:
        """
        EN: Emit the current packed chunk if there is one.
        CN: 如果存在当前打包 chunk，则输出它。
        """
        if current_parts:
            packed_chunks.append(normalize_markdown_text("\n\n".join(current_parts)))
            current_parts.clear()

    def append_unit(unit_text: str) -> None:
        """
        EN: Append one pre-split unit while keeping the packed chunk under the hard limit.
        CN: 追加一个预切分单元，并保持当前打包 chunk 不超过硬限制。
        """
        normalized_unit = normalize_markdown_text(unit_text)
        if not normalized_unit:
            return
        candidate_parts = [*current_parts, normalized_unit]
        candidate_text = normalize_markdown_text("\n\n".join(candidate_parts))
        if current_parts and token_counter(candidate_text) > hard_token_limit:
            flush_current()
            current_parts.append(normalized_unit)
            return
        current_parts.append(normalized_unit)

    for block in section.blocks:
        block_text = normalize_markdown_text(block.text)
        if not block_text:
            continue

        block_tokens = token_counter(block_text)
        if block.block_type == "paragraph" and block_tokens > soft_token_target:
            paragraph_parts = chunker(block_text)
            if not paragraph_parts or (
                len(paragraph_parts) == 1 and normalize_markdown_text(paragraph_parts[0]) == block_text
            ):
                paragraph_parts = _force_split_markdown_text(
                    block_text,
                    hard_token_limit=hard_token_limit,
                    token_counter=token_counter,
                    tokenizer=tokenizer,
                )
            for paragraph_part in paragraph_parts:
                append_unit(paragraph_part)
            continue

        if block.is_atomic and block_tokens > hard_token_limit:
            for forced_part in _force_split_markdown_text(
                block_text,
                hard_token_limit=hard_token_limit,
                token_counter=token_counter,
                tokenizer=tokenizer,
            ):
                append_unit(forced_part)
            continue

        append_unit(block_text)

    flush_current()
    return packed_chunks


def _build_semchunk_chunker(
    *,
    token_counter: Callable[[str], int],
    soft_token_target: int,
) -> Callable[[str], list[str]]:
    """
    EN: Build a semchunk chunker from a token counter and token target.
    CN: 基于 token 计数器和 token 目标构建 semchunk chunker。
    """
    try:
        import semchunk
    except ImportError:
        return lambda text: _naive_sentence_split(text, token_counter=token_counter, soft_token_target=soft_token_target)

    try:
        return semchunk.chunkerify(token_counter, soft_token_target)
    except Exception:
        return lambda text: _naive_sentence_split(text, token_counter=token_counter, soft_token_target=soft_token_target)


def _naive_sentence_split(text: str, *, token_counter: Callable[[str], int], soft_token_target: int) -> list[str]:
    """
    EN: Conservative fallback splitter used when semchunk is unavailable.
    CN: semchunk 不可用时使用的保守回退切分器。
    """
    normalized = normalize_markdown_text(text)
    if not normalized:
        return []

    if token_counter(normalized) <= soft_token_target:
        return [normalized]

    paragraphs = [segment.strip() for segment in normalized.split("\n\n") if segment.strip()]
    if len(paragraphs) <= 1:
        return [normalized]

    parts: list[str] = []
    current: list[str] = []
    for paragraph in paragraphs:
        candidate = "\n\n".join(current + [paragraph]) if current else paragraph
        if current and token_counter(candidate) > soft_token_target:
            parts.append("\n\n".join(current))
            current = [paragraph]
            continue
        current.append(paragraph)
    if current:
        parts.append("\n\n".join(current))
    return parts


def _build_chunk(
    *,
    text: str,
    header_path: tuple[str, ...],
    token_counter: Callable[[str], int],
    chunk_index: int,
    section_index: int,
    part_index: int | None,
    total_parts: int,
    section_metadata: dict[str, Any],
    split_index: int | None = None,
) -> MarkdownChunk:
    """
    EN: Build a chunk wrapper with provenance metadata and token estimate.
    CN: 构建带来源元数据与 token 估算值的 chunk 包装对象。
    """
    metadata: dict[str, Any] = {
        "section_index": section_index,
        "chunk_index": chunk_index,
        "chunking_strategy": "v2_markdown_semchunk",
        "source_format": "markdown",
        "section_path": list(header_path),
        "header_path": list(header_path),
        "section_kind": section_metadata.get("section_kind"),
        "block_count": section_metadata.get("block_count"),
    }
    if part_index is not None:
        metadata["section_part_index"] = part_index
        metadata["section_part_count"] = total_parts
    if split_index is not None:
        metadata["split_index"] = split_index
    token_estimate = token_counter(text)
    metadata["token_estimate"] = token_estimate
    return MarkdownChunk(text=text, header_path=header_path, token_estimate=token_estimate, metadata=metadata)


def _ensure_hard_limit(
    chunks: Sequence[MarkdownChunk],
    *,
    hard_token_limit: int,
    token_counter: Callable[[str], int],
    tokenizer: Any | None,
) -> list[MarkdownChunk]:
    """
    EN: Enforce a hard token ceiling on every chunk with a final exact fallback.
    CN: 对每个 chunk 强制执行硬 token 上限，并提供最终精确回退。
    """
    safe_chunks: list[MarkdownChunk] = []
    for chunk in chunks:
        if token_counter(chunk.text) <= hard_token_limit:
            safe_chunks.append(chunk)
            continue
        for forced_index, forced_text in enumerate(
            _force_split_markdown_text(
                chunk.text,
                hard_token_limit=hard_token_limit,
                token_counter=token_counter,
                tokenizer=tokenizer,
            ),
            start=1,
        ):
            forced_text = normalize_markdown_text(forced_text)
            if not forced_text:
                continue
            safe_chunks.append(
                MarkdownChunk(
                    text=forced_text,
                    header_path=chunk.header_path,
                    token_estimate=token_counter(forced_text),
                    metadata={
                        **chunk.metadata,
                        "split_index": forced_index,
                        "token_estimate": token_counter(forced_text),
                    },
                )
            )
    return safe_chunks


def _force_split_markdown_text(
    text: str,
    *,
    hard_token_limit: int,
    token_counter: Callable[[str], int],
    tokenizer: Any | None,
) -> list[str]:
    """
    EN: Force-split Markdown text when semantic splitting still exceeds the hard limit.
    CN: 当语义切分后仍超出硬限制时，对 Markdown 文本执行强制切分。
    """
    normalized = normalize_markdown_text(text)
    if not normalized:
        return []

    if tokenizer is not None and hasattr(tokenizer, "encode") and hasattr(tokenizer, "decode"):
        try:
            tokens = tokenizer.encode(normalized)
        except Exception:
            tokens = None
        if tokens is not None:
            if len(tokens) <= hard_token_limit:
                return [normalized]
            parts: list[str] = []
            start = 0
            while start < len(tokens):
                end = min(start + hard_token_limit, len(tokens))
                part = normalize_markdown_text(tokenizer.decode(tokens[start:end]))
                if part:
                    parts.append(part)
                if end <= start:
                    end = min(start + 1, len(tokens))
                start = end
            return parts

    approx_chars = max(1, hard_token_limit * 4)
    if len(normalized) <= approx_chars:
        return [normalized]

    parts: list[str] = []
    remaining = normalized
    while remaining:
        if token_counter(remaining) <= hard_token_limit:
            parts.append(remaining)
            break
        cut = _find_text_cut(remaining, approx_chars=approx_chars)
        part = normalize_markdown_text(remaining[:cut])
        if not part:
            part = normalize_markdown_text(remaining[:approx_chars])
            cut = min(approx_chars, len(remaining))
        parts.append(part)
        remaining = remaining[cut:].lstrip()
    return [part for part in parts if part]


def _find_text_cut(text: str, *, approx_chars: int) -> int:
    """
    EN: Find a conservative cut point near the approximate character budget.
    CN: 在近似字符预算附近寻找保守切分点。
    """
    if len(text) <= approx_chars:
        return len(text)
    window = text[:approx_chars]
    candidates = ["\n\n# ", "\n# ", "\n## ", "\n### ", "\n#### ", "\n##### ", "\n###### ", "\n\n", "\n"]
    half_window = max(1, len(window) // 2)
    for marker in candidates:
        index = window.rfind(marker)
        if index >= half_window:
            return index + len(marker.rstrip())
    return approx_chars


def _slice_lines(lines: Sequence[str], start_line: int, end_line: int) -> str:
    """
    EN: Slice source lines into a Markdown block while trimming outer whitespace.
    CN: 从源行中切出 Markdown block，并裁剪外围空白。
    """
    if start_line >= end_line:
        return ""
    return normalize_markdown_text("\n".join(lines[start_line:end_line]))


def _is_covered_by_span(start_line: int, end_line: int, spans: Sequence[tuple[int, int]]) -> bool:
    """
    EN: Check whether a line range is already covered by a previously captured span.
    CN: 检查某个行区间是否已被之前捕获的 span 覆盖。
    """
    for span_start, span_end in spans:
        if start_line >= span_start and end_line <= span_end:
            return True
    return False


def _extract_inline_text(tokens: Sequence[Any], index: int) -> str:
    """
    EN: Extract inline text from a token stream.
    CN: 从 token 流中提取 inline 文本。
    """
    if index < 0 or index >= len(tokens):
        return ""
    token = tokens[index]
    if token.type != "inline":
        return ""
    return normalize_markdown_text(token.content)


def _heading_level(token: Any) -> int:
    """
    EN: Resolve the heading level from a MarkdownIt token.
    CN: 从 MarkdownIt token 中解析 heading 层级。
    """
    if getattr(token, "tag", "").startswith("h") and token.tag[1:].isdigit():
        return int(token.tag[1:])
    return 1


def _classify_atomic_token(token_type: str) -> str:
    """
    EN: Map a MarkdownIt token type to a stable block type label.
    CN: 将 MarkdownIt token 类型映射为稳定的 block 类型标签。
    """
    if token_type == "fence":
        return "code_fence"
    if token_type == "code_block":
        return "code_block"
    if token_type == "html_block":
        return "html_block"
    if token_type == "blockquote_open":
        return "blockquote"
    if token_type in {"bullet_list_open", "ordered_list_open"}:
        return "list"
    if token_type == "table_open":
        return "table"
    return token_type
