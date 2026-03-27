"""
EN: Tests for GeminiEmbeddingClient including SDK integration and read-timeout retry.
CN: 同上。
"""

from __future__ import annotations

import httpx

from serverless_mcp.embed import gemini_client
from serverless_mcp.domain.models import EmbeddingRequest


class _FakeModels:
    # EN: Stub Gemini models API that records embed_content calls.
    # CN: 璁板綍 embed_content 璋冪敤鐨?Gemini models API 妗┿€?
    def __init__(self) -> None:
        self.calls = []

    def embed_content(self, **kwargs):
        self.calls.append(kwargs)
        return type("Response", (), {"embeddings": [type("Embedding", (), {"values": [0.3, 0.4]})()]})()


class _FakeClient:
    # EN: Stub Gemini client wrapping _FakeModels.
    # CN: 同上。
    def __init__(self) -> None:
        self.models = _FakeModels()


class _RetryingModels:
    # EN: Stub that raises httpx.ReadTimeout on first call and succeeds on the second.
    # CN: 同上。
    def __init__(self) -> None:
        self.calls = 0

    def embed_content(self, **kwargs):
        self.calls += 1
        if self.calls == 1:
            raise httpx.ReadTimeout("timed out", request=httpx.Request("POST", "https://example.invalid"))
        return type("Response", (), {"embeddings": [type("Embedding", (), {"values": [0.9, 0.8]})()]})()


class _RetryingStatusModels:
    # EN: Stub that raises an HTTP 429 response on first call and succeeds on the second.
    # CN: 首次调用抛出 HTTP 429、第二次成功的桩对象。
    def __init__(self) -> None:
        self.calls = 0

    def embed_content(self, **kwargs):
        self.calls += 1
        if self.calls == 1:
            request = httpx.Request("POST", "https://example.invalid")
            response = httpx.Response(429, request=request)
            raise httpx.HTTPStatusError("rate limited", request=request, response=response)
        return type("Response", (), {"embeddings": [type("Embedding", (), {"values": [0.6, 0.7]})()]})()


def test_gemini_embedding_client_uses_sdk_embed_content(monkeypatch) -> None:
    """
    EN: Verify the Gemini client configures the SDK with correct api_key, base_url, timeout, and passes request params.
    CN: 同上。
    """
    captured = {}
    fake_client = _FakeClient()

    def _factory(**kwargs):
        captured.update(kwargs)
        return fake_client

    monkeypatch.setattr(gemini_client.genai, "Client", _factory)

    client = gemini_client.GeminiEmbeddingClient(
        api_key="secret",
        base_url="https://generativelanguage.googleapis.com/v1beta",
        model="gemini-embedding-2-preview",
        timeout_seconds=45,
    )

    vector = client.embed_text(
        EmbeddingRequest(
            chunk_id="chunk#000001",
            chunk_type="page_text_chunk",
            content_kind="text",
            text="hello world",
            output_dimensionality=3072,
            task_type="RETRIEVAL_DOCUMENT",
        )
    )

    assert captured["api_key"] == "secret"
    assert captured["http_options"].base_url == "https://generativelanguage.googleapis.com/"
    assert captured["http_options"].timeout == 45000
    assert fake_client.models.calls[0]["model"] == "gemini-embedding-2-preview"
    assert fake_client.models.calls[0]["contents"] == "hello world"
    assert fake_client.models.calls[0]["config"].http_options.timeout == 45000
    assert fake_client.models.calls[0]["config"].task_type == "RETRIEVAL_DOCUMENT"
    assert fake_client.models.calls[0]["config"].output_dimensionality == 3072
    assert vector == [0.3, 0.4]


def test_gemini_embedding_client_retries_read_timeout(monkeypatch) -> None:
    """
    EN: Verify the Gemini client retries embed_text after a read timeout and returns the successful result.
    CN: 同上。
    """
    captured = {}
    fake_client = _FakeClient()
    fake_client.models = _RetryingModels()

    def _factory(**kwargs):
        captured.update(kwargs)
        return fake_client

    monkeypatch.setattr(gemini_client.genai, "Client", _factory)
    monkeypatch.setattr(gemini_client.time, "sleep", lambda _seconds: None)

    client = gemini_client.GeminiEmbeddingClient(
        api_key="secret",
        base_url="https://generativelanguage.googleapis.com/v1beta",
        model="gemini-embedding-2-preview",
        timeout_seconds=45,
    )

    vector = client.embed_text(
        EmbeddingRequest(
            chunk_id="chunk#000001",
            chunk_type="page_text_chunk",
            content_kind="text",
            text="hello world",
            output_dimensionality=3072,
            task_type="RETRIEVAL_DOCUMENT",
        )
    )

    assert captured["api_key"] == "secret"
    assert fake_client.models.calls == 2
    assert captured["http_options"].timeout == 45000
    assert vector == [0.9, 0.8]


def test_gemini_embedding_client_retries_rate_limit(monkeypatch) -> None:
    """
    EN: Verify that the Gemini client retries transient 429 responses.
    CN: 验证 Gemini 客户端会重试瞬时性的 429 响应。
    """
    captured = {}
    fake_client = _FakeClient()
    fake_client.models = _RetryingStatusModels()

    def _factory(**kwargs):
        captured.update(kwargs)
        return fake_client

    monkeypatch.setattr(gemini_client.genai, "Client", _factory)
    monkeypatch.setattr(gemini_client.time, "sleep", lambda _seconds: None)

    client = gemini_client.GeminiEmbeddingClient(
        api_key="secret",
        base_url="https://generativelanguage.googleapis.com/v1beta",
        model="gemini-embedding-2-preview",
        timeout_seconds=45,
    )

    vector = client.embed_text(
        EmbeddingRequest(
            chunk_id="chunk#000001",
            chunk_type="page_text_chunk",
            content_kind="text",
            text="hello world",
            output_dimensionality=3072,
            task_type="RETRIEVAL_DOCUMENT",
        )
    )

    assert captured["api_key"] == "secret"
    assert fake_client.models.calls == 2
    assert vector == [0.6, 0.7]
