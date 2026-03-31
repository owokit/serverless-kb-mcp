"""
EN: DOCX structural extraction helpers that render Word documents as Markdown.
CN: 将 Word 文档按结构提取并渲染为 Markdown 的辅助模块。
"""
from __future__ import annotations

from io import BytesIO
from typing import Iterable
from urllib.parse import urljoin
import re

from docx import Document as load_docx_document
from docx.oxml.ns import qn
from docx.oxml.table import CT_Tbl
from docx.oxml.text.paragraph import CT_P
from docx.table import Table, _Cell
from docx.text.paragraph import Paragraph
from docx.text.run import Run

from serverless_mcp.extract.policy import normalize_text


_BULLET_STYLE_PREFIXES = ("List Bullet", "Bullet", "Unordered List")
_NUMBERED_STYLE_PREFIXES = ("List Number", "Numbered List")
_HEADING_STYLE_RE = re.compile(r"^Heading\s+(\d+)$", flags=re.IGNORECASE)


def convert_docx_to_markdown(body: bytes) -> str:
    """
    EN: Convert DOCX bytes into Markdown while preserving document order.
    CN: 按文档顺序将 DOCX 字节内容转换为 Markdown。
    """
    document = load_docx_document(BytesIO(body))
    lines: list[str] = []
    for block in _iter_docx_blocks(document):
        if isinstance(block, Paragraph):
            rendered = _paragraph_to_markdown(block)
            if rendered:
                lines.append(rendered)
            continue
        if isinstance(block, Table):
            table_lines = _table_to_markdown(block)
            if table_lines:
                lines.extend(table_lines)
    return normalize_text("\n".join(lines))


def _iter_docx_blocks(document) -> Iterable[Paragraph | Table]:
    """
    EN: Yield top-level paragraphs and tables in their original document order.
    CN: 按原始文档顺序产出顶层段落和表格。
    """
    for child in document.element.body.iterchildren():
        if isinstance(child, CT_P):
            yield Paragraph(child, document)
        elif isinstance(child, CT_Tbl):
            yield Table(child, document)


def _paragraph_to_markdown(paragraph: Paragraph) -> str:
    """
    EN: Render a DOCX paragraph as Markdown, preserving headings, lists, links, bold, and italics.
    CN: 将 DOCX 段落渲染为 Markdown，尽量保留标题、列表、链接、粗体和斜体。
    """
    text = _render_paragraph_text(paragraph)
    if not text:
        return ""

    style_name = (paragraph.style.name if paragraph.style is not None else "").strip()
    heading_level = _heading_level_from_style_name(style_name)
    if heading_level:
        return f"{'#' * heading_level} {text}"

    list_prefix = _list_prefix(paragraph, style_name)
    if list_prefix is not None:
        level = _list_level(paragraph)
        indent = "  " * max(0, level)
        return f"{indent}{list_prefix} {text}"

    return text


def _render_paragraph_text(paragraph: Paragraph) -> str:
    """
    EN: Render inline paragraph content, including hyperlinks and run-level emphasis.
    CN: 渲染段落内联内容，包括超链接和运行级强调样式。
    """
    parts: list[str] = []
    for child in paragraph._p.iterchildren():  # type: ignore[attr-defined]
        if child.tag == qn("w:r"):
            run = Run(child, paragraph)
            rendered = _render_run(run)
            if rendered:
                parts.append(rendered)
            continue
        if child.tag == qn("w:hyperlink"):
            rendered = _render_hyperlink(paragraph, child)
            if rendered:
                parts.append(rendered)
    if not parts:
        parts = [normalize_text(paragraph.text)]
    return normalize_text("".join(parts))


def _render_run(run: Run) -> str:
    """
    EN: Render one run with Markdown emphasis when present.
    CN: 在存在强调样式时，将单个 run 渲染为 Markdown。
    """
    text = run.text or ""
    if not text:
        return ""
    text = text.replace("\\", "\\\\").replace("`", "\\`")
    if run.bold and run.italic:
        return f"***{text}***"
    if run.bold:
        return f"**{text}**"
    if run.italic:
        return f"*{text}*"
    return text


def _render_hyperlink(paragraph: Paragraph, hyperlink) -> str:
    """
    EN: Render a DOCX hyperlink by resolving the relationship target.
    CN: 通过解析 relationship target 渲染 DOCX 超链接。
    """
    target = None
    rel_id = hyperlink.get(qn("r:id"))
    if rel_id:
        rel = paragraph.part.rels.get(rel_id)
        if rel is not None:
            target = getattr(rel.target_ref, "strip", lambda: rel.target_ref)()
    text = "".join((child.text or "") for child in hyperlink.iterchildren() if getattr(child, "text", None))
    text = normalize_text(text)
    if not text:
        return ""
    if target:
        return f"[{text}]({urljoin('', target)})"
    return text


def _heading_level_from_style_name(style_name: str) -> int:
    """
    EN: Map Word heading styles to Markdown heading levels.
    CN: 将 Word 标题样式映射为 Markdown 标题级别。
    """
    if style_name.lower() == "title":
        return 1
    match = _HEADING_STYLE_RE.match(style_name)
    if not match:
        return 0
    return max(1, min(6, int(match.group(1))))


def _list_prefix(paragraph: Paragraph, style_name: str) -> str | None:
    """
    EN: Resolve a bullet or numbered list prefix from style or numbering metadata.
    CN: 从样式或编号元数据解析无序/有序列表前缀。
    """
    normalized = style_name.lower()
    if any(normalized.startswith(prefix.lower()) for prefix in _BULLET_STYLE_PREFIXES):
        return "-"
    if any(normalized.startswith(prefix.lower()) for prefix in _NUMBERED_STYLE_PREFIXES):
        return "1."
    num_pr = getattr(getattr(paragraph._p, "pPr", None), "numPr", None)
    if num_pr is not None:
        level = getattr(getattr(num_pr, "ilvl", None), "val", None)
        num_id = getattr(getattr(num_pr, "numId", None), "val", None)
        if num_id is not None or level is not None:
            return "1."
    return None


def _list_level(paragraph: Paragraph) -> int:
    """
    EN: Resolve list indentation depth from numbering metadata.
    CN: 从编号元数据解析列表缩进层级。
    """
    num_pr = getattr(getattr(paragraph._p, "pPr", None), "numPr", None)
    if num_pr is None:
        return 0
    level = getattr(getattr(num_pr, "ilvl", None), "val", None)
    try:
        return int(level) if level is not None else 0
    except (TypeError, ValueError):
        return 0


def _table_to_markdown(table: Table) -> list[str]:
    """
    EN: Render a DOCX table as a Markdown table when rows are present.
    CN: 当表格有行时，将 DOCX 表格渲染为 Markdown 表格。
    """
    rows: list[list[str]] = []
    for row in table.rows:
        cells = [_render_cell_text(cell) for cell in row.cells]
        if any(cell.strip() for cell in cells):
            rows.append(cells)
    if not rows:
        return []

    width = max(len(row) for row in rows)
    normalized_rows = [row + [""] * (width - len(row)) for row in rows]
    lines = ["| " + " | ".join(normalized_rows[0]) + " |"]
    lines.append("| " + " | ".join("---" for _ in range(width)) + " |")
    for row in normalized_rows[1:]:
        lines.append("| " + " | ".join(row) + " |")
    return lines


def _render_cell_text(cell: _Cell) -> str:
    """
    EN: Render cell text using the same inline markdown rules as paragraphs.
    CN: 采用与段落相同的内联 Markdown 规则渲染单元格文本。
    """
    parts: list[str] = []
    for paragraph in cell.paragraphs:
        rendered = _render_paragraph_text(paragraph)
        if rendered:
            parts.append(rendered)
    return normalize_text("<br>".join(parts))
