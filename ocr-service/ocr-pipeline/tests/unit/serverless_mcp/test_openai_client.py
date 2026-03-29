"""
EN: Tests for OpenAIEmbeddingClient SDK integration.
CN: OpenAIEmbeddingClient SDK 集成测试。
"""

from __future__ import annotations

import json
import types

import httpx

from serverless_mcp.domain.models import EmbeddingRequest
from serverless_mcp.embed import openai_client


class _FakeRawResponse:
    def __init__(self, payload: dict[str, object], *, content_type: str = "text/plain") -> None:
        self.http_response = httpx.Response(
            200,
            headers={"content-type": content_type},
            content=json.dumps(payload).encode("utf-8"),
        )


class _FakeEmbeddings:
    def __init__(self) -> None:
        self.calls = []
        self.with_raw_response = types.SimpleNamespace(create=self.create)

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return _FakeRawResponse({"data": [{"embedding": [0.1, 0.2]}]}, content_type="text/plain")


class _FakeOpenAI:
    def __init__(self) -> None:
        self.embeddings = _FakeEmbeddings()


def test_openai_embedding_client_uses_raw_response_and_float_encoding(monkeypatch) -> None:
    """
    EN: OpenAI embedding client uses raw response parsing and float encoding.
    CN: 验证 OpenAI embedding 客户端使用 raw response 解析和 float 编码。
    """
    captured = {}
    fake = _FakeOpenAI()

    def _factory(**kwargs):
        captured.update(kwargs)
        return fake

    monkeypatch.setattr(openai_client, "OpenAI", _factory)

    client = openai_client.OpenAIEmbeddingClient(
        api_key="secret",
        base_url="https://api.openai.com/v1",
        model="text-embedding-3-small",
        timeout_seconds=45,
    )

    vector = client.embed_text(
        EmbeddingRequest(
            chunk_id="chunk#000001",
            chunk_type="page_text_chunk",
            content_kind="text",
            text="hello world",
            output_dimensionality=1536,
        )
    )

    assert captured == {
        "api_key": "secret",
        "base_url": "https://api.openai.com/v1/",
        "timeout": 45,
    }
    assert fake.embeddings.calls == [
        {
            "model": "text-embedding-3-small",
            "input": "hello world",
            "dimensions": 1536,
            "encoding_format": "float",
            "timeout": 45,
        }
    ]
    assert vector == [0.1, 0.2]
