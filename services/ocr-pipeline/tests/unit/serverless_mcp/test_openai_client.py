"""
EN: Tests for OpenAIEmbeddingClient SDK integration.
CN: 娴嬭瘯 OpenAIEmbeddingClient SDK 闆嗘垚銆?
"""

from __future__ import annotations

from serverless_mcp.embed import openai_client
from serverless_mcp.domain.models import EmbeddingRequest


class _FakeOpenAI:
    def __init__(self) -> None:
        self.embeddings = self
        self.calls = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return type("Response", (), {"data": [type("Embedding", (), {"embedding": [0.1, 0.2]})()]})()


def test_openai_embedding_client_uses_sdk_embeddings(monkeypatch) -> None:
    """
    EN: Openai embedding client uses sdk embeddings.
    CN: 同上。
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
    assert fake.calls == [
        {
            "model": "text-embedding-3-small",
            "input": "hello world",
            "dimensions": 1536,
            "timeout": 45,
        }
    ]
    assert vector == [0.1, 0.2]

