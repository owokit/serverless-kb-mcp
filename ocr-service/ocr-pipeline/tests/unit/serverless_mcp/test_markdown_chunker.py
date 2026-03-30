"""
EN: Tests for the Markdown chunker covering structure-aware parsing and token-safe splitting.
CN: 针对 Markdown chunker 的测试，覆盖结构感知解析和 token-safe 拆分。
"""

from __future__ import annotations

import sys
import types

from serverless_mcp.extract.markdown_chunker import parse_markdown_blocks, split_markdown_for_embedding


class _CharTokenizer:
    # EN: Simple tokenizer stub that treats every character as one token.
    # CN: 将每个字符视为一个 token 的简单 tokenizer stub。
    def encode(self, text: str) -> list[str]:
        return list(text)

    def decode(self, tokens: list[str]) -> str:
        return "".join(tokens)


def _count_chars(text: str) -> int:
    return len(text)


def test_parse_markdown_blocks_tracks_header_paths_and_atomic_blocks() -> None:
    """
    EN: Markdown block parsing preserves header breadcrumbs and atomic block types.
    CN: Markdown block 解析需要保留标题面包屑和原子 block 类型。
    """
    markdown_text = (
        "# Title\n\n"
        "Intro paragraph.\n\n"
        "## Details\n\n"
        "> quoted line\n\n"
        "- item one\n"
        "- item two\n\n"
        "```python\n"
        "print('hello')\n"
        "```\n\n"
        "| a | b |\n"
        "|---|---|\n"
        "| 1 | 2 |\n"
    )

    blocks = parse_markdown_blocks(markdown_text, token_counter=_count_chars)

    assert any(block.block_type == "heading_h1" and block.header_path == ("Title",) for block in blocks)
    assert any(block.block_type == "heading_h2" and block.header_path == ("Title", "Details") for block in blocks)
    assert any(block.block_type == "blockquote" and block.is_atomic for block in blocks)
    assert any(block.block_type == "list" and block.is_atomic for block in blocks)
    assert any(block.block_type == "code_fence" and block.is_atomic for block in blocks)
    assert any(block.block_type == "table" and block.is_atomic for block in blocks)
    assert any(block.header_path == ("Title", "Details") for block in blocks if block.block_type in {"blockquote", "list", "code_fence", "table"})


def test_split_markdown_for_embedding_merges_small_sections_and_keeps_atomic_blocks_intact() -> None:
    """
    EN: Small Markdown sections are merged while fenced code, lists, tables, and blockquotes stay intact.
    CN: 小 section 会合并，同时 fenced code、list、table 和 blockquote 不应被切坏。
    """
    markdown_text = (
        "# Intro\n\n"
        "Short paragraph one.\n\n"
        "Short paragraph two.\n\n"
        "> quoted note\n\n"
        "- item one\n"
        "- item two\n\n"
        "```python\n"
        "print('hello')\n"
        "```\n\n"
        "| a | b |\n"
        "|---|---|\n"
        "| 1 | 2 |\n"
    )

    chunks = split_markdown_for_embedding(
        markdown_text,
        soft_token_target=400,
        hard_token_limit=500,
        token_counter=_count_chars,
        tokenizer=_CharTokenizer(),
    )

    assert len(chunks) == 1
    chunk = chunks[0]
    assert chunk.header_path == ("Intro",)
    assert chunk.metadata["source_format"] == "markdown"
    assert chunk.metadata["chunking_strategy"] == "v2_markdown_semchunk"
    assert "```python\nprint('hello')\n```" in chunk.text
    assert "| a | b |\n|---|---|\n| 1 | 2 |" in chunk.text
    assert "- item one\n- item two" in chunk.text
    assert "> quoted note" in chunk.text
    assert chunk.token_estimate <= 500


def test_split_markdown_for_embedding_splits_long_paragraphs_by_semantic_chunks() -> None:
    """
    EN: Long Markdown sections are split into multiple token-safe chunks.
    CN: 长 Markdown section 应被拆成多个 token-safe chunk。
    """
    markdown_text = "# Intro\n\n" + ("alpha beta gamma delta epsilon zeta eta theta iota kappa " * 18)

    chunks = split_markdown_for_embedding(
        markdown_text,
        soft_token_target=80,
        hard_token_limit=120,
        token_counter=_count_chars,
        tokenizer=_CharTokenizer(),
    )

    assert len(chunks) > 1
    assert all(chunk.token_estimate <= 120 for chunk in chunks)
    assert chunks[0].header_path == ("Intro",)
    assert all(chunk.metadata["source_format"] == "markdown" for chunk in chunks)


def test_split_markdown_for_embedding_uses_exact_fallback_when_semchunk_is_noop(monkeypatch) -> None:
    """
    EN: The exact token window fallback activates when semchunk returns an oversized chunk unchanged.
    CN: 当 semchunk 仍返回超大 chunk 时，精确 token 窗口回退必须生效。
    """
    fake_semchunk = types.SimpleNamespace(chunkerify=lambda *_args, **_kwargs: (lambda text: [text]))
    monkeypatch.setitem(sys.modules, "semchunk", fake_semchunk)

    markdown_text = "# Intro\n\n" + ("x" * 260)
    chunks = split_markdown_for_embedding(
        markdown_text,
        soft_token_target=60,
        hard_token_limit=90,
        token_counter=_count_chars,
        tokenizer=_CharTokenizer(),
    )

    assert len(chunks) > 1
    assert all(chunk.token_estimate <= 90 for chunk in chunks)
    assert chunks[0].text.startswith("# Intro")


def test_split_markdown_for_embedding_keeps_atomic_blocks_intact_when_packing_sections() -> None:
    """
    EN: Atomic blocks stay whole even when the section must be split into multiple chunks.
    CN: 即使 section 需要拆成多个 chunk，原子 block 也应保持完整。
    """
    markdown_text = (
        "# Intro\n\n"
        + ("alpha beta gamma delta epsilon zeta eta theta iota kappa " * 8)
        + "\n\n"
        "```python\n"
        "print('hello')\n"
        "print('world')\n"
        "```\n\n"
        + ("lambda mu nu xi omicron pi rho sigma tau upsilon " * 8)
    )

    chunks = split_markdown_for_embedding(
        markdown_text,
        soft_token_target=60,
        hard_token_limit=120,
        token_counter=_count_chars,
        tokenizer=_CharTokenizer(),
    )

    assert len(chunks) > 1
    assert all(chunk.token_estimate <= 120 for chunk in chunks)
    assert any("```python\nprint('hello')\nprint('world')\n```" in chunk.text for chunk in chunks)
