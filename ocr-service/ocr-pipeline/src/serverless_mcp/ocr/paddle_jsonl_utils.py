"""
EN: Helpers for extracting markdown text from PaddleOCR JSONL payloads.
CN: 从 PaddleOCR JSONL 负载中提取 markdown 文本的辅助函数。
"""

from __future__ import annotations

from typing import Any


def build_markdown_text_from_json_lines(json_lines: list[dict[str, Any]]) -> str:
    page_markdowns: list[str] = []
    for line in json_lines:
        page_markdown = extract_page_markdown_text(line)
        if page_markdown:
            page_markdowns.append(page_markdown)
    return "\n\n".join(page_markdowns)


def extract_page_markdown_text(payload: dict[str, Any]) -> str | None:
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
