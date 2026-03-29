"""
EN: Tests for provider URL normalization helpers.
CN: 提供者 URL 规范化辅助函数的测试。
"""

from __future__ import annotations

import pytest

from serverless_mcp.embed.provider_urls import normalize_openai_base_url


@pytest.mark.parametrize(
    ("base_url", "expected"),
    [
        ("https://api.openai.com", "https://api.openai.com/v1/"),
        ("https://api.openai.com/v1", "https://api.openai.com/v1/"),
        ("https://example.openai.azure.com", "https://example.openai.azure.com/openai/v1/"),
        ("https://openrouter.ai/api/v1", "https://openrouter.ai/api/v1/"),
        ("https://openrouter.ai/api/v1/", "https://openrouter.ai/api/v1/"),
        ("https://custom.example.com", "https://custom.example.com/v1/"),
        ("https://custom.example.com/v1", "https://custom.example.com/v1/"),
    ],
)
def test_normalize_openai_base_url_preserves_custom_path_prefix(base_url: str, expected: str) -> None:
    """
    EN: Verify OpenAI-compatible normalization preserves explicit path prefixes and canonicalizes known hosts.
    CN: 验证 OpenAI 兼容归一化会保留显式路径前缀，并规范化已知主机。
    """
    assert normalize_openai_base_url(base_url) == expected
