"""
EN: Tests for embedding policy including text splitting, token estimation, and oversized chunk expansion.
CN: 同上。
"""

import sys
import types

from serverless_mcp.extract import policy
from serverless_mcp.extract.policy import estimate_tokens, expand_oversized_chunks, split_text_for_embedding
from serverless_mcp.domain.models import ExtractedChunk


def test_split_text_for_embedding_respects_max_tokens() -> None:
    """
    EN: Split text for embedding respects max tokens.
    CN: 楠岃瘉 split_text_for_embedding 閬靛畧 max_tokens 闄愬埗銆?
    """
    text = "\n\n".join(f"## Section {index}\n" + ("content " * 400) for index in range(1, 5))
    parts = split_text_for_embedding(text, max_tokens=300)
    assert len(parts) > 1
    assert all(estimate_tokens(part) <= 300 for part in parts)


class _FakeEncoder:
    # EN: Stub tokenizer that treats each character as one token.
    # CN: 同上。
    def encode(self, text: str) -> list[str]:
        return list(text)

    def decode(self, tokens: list[str]) -> str:
        return "".join(tokens)


def test_estimate_tokens_uses_available_encoder(monkeypatch) -> None:
    """
    EN: Estimate tokens uses available encoder.
    CN: 同上。
    """
    monkeypatch.setattr(policy, "_get_token_encoder", lambda: _FakeEncoder())

    assert estimate_tokens("alpha beta") == len("alpha beta")


def test_split_text_for_embedding_uses_token_encoder_when_available(monkeypatch) -> None:
    """
    EN: Split text for embedding uses token encoder when available.
    CN: 楠岃瘉鏈?tokenizer 鏃?split_text_for_embedding 浣跨敤瀹冦€?
    """
    monkeypatch.setattr(policy, "_get_token_encoder", lambda: _FakeEncoder())

    parts = split_text_for_embedding("alpha beta gamma delta epsilon", max_tokens=12)

    assert len(parts) > 1
    assert all(estimate_tokens(part) <= 12 for part in parts)


def test_split_text_for_embedding_prefers_markdown_boundaries(monkeypatch) -> None:
    """
    EN: Split text for embedding prefers markdown boundaries.
    CN: 楠岃瘉 split_text_for_embedding 浼樺厛鍦?markdown 杈圭晫澶勬媶鍒嗐€?
    """
    monkeypatch.setattr(policy, "_get_token_encoder", lambda: _FakeEncoder())

    text = (
        "# Intro\n\n"
        "Paragraph one.\n\n"
        "```python\n"
        "print('a')\n"
        "print('b')\n"
        "```\n\n"
        "## Next\n\n"
        "Another paragraph."
    )

    parts = split_text_for_embedding(text, max_tokens=40)

    assert parts == [
        "# Intro\n\nParagraph one.",
        "```python\nprint('a')\nprint('b')\n```",
        "## Next\n\nAnother paragraph.",
    ]


def test_expand_oversized_chunks_recursively_reduces_until_safe(monkeypatch) -> None:
    """
    EN: Expand oversized chunks recursively reduces until safe.
    CN: 楠岃瘉 expand_oversized_chunks 閫掑綊缂╁噺鐩村埌瀹夊叏澶у皬銆?
    """
    monkeypatch.setattr(policy, "estimate_tokens", lambda text: len(text))

    def _split_halves(text: str, *, max_tokens: int, preferred_breaks=()):
        if len(text) <= 1:
            return [text]
        midpoint = max(1, len(text) // 2)
        return [text[:midpoint], text[midpoint:]]

    monkeypatch.setattr(policy, "split_text_for_embedding", _split_halves)

    chunk = ExtractedChunk(
        chunk_id="chunk#000001",
        chunk_type="page_text_chunk",
        text="x" * 32,
        doc_type="pdf",
        token_estimate=32,
        page_no=1,
        page_span=(1, 1),
        section_path=("page-1", "layout-1"),
        metadata={"source": "test"},
    )

    expanded = expand_oversized_chunks([chunk], safe_text_tokens=5)

    assert len(expanded) > 1
    assert all(part.token_estimate <= 5 for part in expanded)
    assert all(part.metadata["split_from"] == "chunk#000001" for part in expanded)


def test_default_embedding_policy_keeps_openai_inputs_below_model_limit() -> None:
    """
    EN: Default embedding policy keeps openai inputs below model limit.
    CN: 同上。
    """
    assert policy.DEFAULT_POLICY.max_input_tokens == 2048
    assert policy.DEFAULT_POLICY.safe_text_tokens == 1400
    assert policy.DEFAULT_POLICY.output_dimensionality == 1536


def test_normalize_text_strips_html_style_attributes() -> None:
    """
    EN: Normalize text strips html style attributes.
    CN: 楠岃瘉 normalize_text 鍘婚櫎 HTML style 灞炴€с€?
    """
    text = "<table border=1 style='margin: auto; word-wrap: break-word;'><tr><td style=\"text-align: center;\">A</td></tr></table>"

    assert policy.normalize_text(text) == "<table border=1><tr><td>A</td></tr></table>"


def test_tiktoken_fallback_emits_a_trace_when_encoder_is_unavailable(monkeypatch, capsys) -> None:
    """
    EN: Verify that token encoder fallback emits a visible trace line.
    CN: 验证 token 编码器回退会输出可见 trace。
    """
    fake_tiktoken = types.ModuleType("tiktoken")

    def _raise(_name):
        raise RuntimeError("boom")

    fake_tiktoken.get_encoding = _raise  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "tiktoken", fake_tiktoken)
    policy._get_token_encoder.cache_clear()

    encoder = policy._get_token_encoder()
    captured = capsys.readouterr().out

    assert encoder is None
    assert "extract.tiktoken_encoder.unavailable" in captured
