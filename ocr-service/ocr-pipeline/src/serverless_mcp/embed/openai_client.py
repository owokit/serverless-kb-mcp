"""
EN: OpenAI-compatible embedding client for text-only embeddings.
CN: 兼容 OpenAI 的纯文本 embedding 客户端。
"""
from __future__ import annotations

import base64
import json
import struct

import httpx
from openai import OpenAI

from serverless_mcp.domain.models import EmbeddingRequest
from serverless_mcp.embed.provider_urls import normalize_openai_base_url


class OpenAIEmbeddingClient:
    """
    EN: OpenAI-compatible embedding client that delegates request execution to the official OpenAI SDK.
    CN: 通过官方 OpenAI SDK 执行请求的 OpenAI 兼容 embedding 客户端。
    """

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str,
        model: str,
        timeout_seconds: int = 60,
    ) -> None:
        """
        Args:
            api_key:
                EN: API key for authenticating with the OpenAI-compatible endpoint.
                CN: 用于认证 OpenAI 兼容端点的 API key。
            base_url:
                EN: Base URL of the OpenAI-compatible API, normalized by normalize_openai_base_url.
                CN: 经过 normalize_openai_base_url 规范化的 OpenAI 兼容 API 基础地址。
            model:
                EN: Embedding model identifier passed to the API.
                CN: 传给 API 的 embedding 模型标识符。
            timeout_seconds:
                EN: Per-request timeout in seconds.
                CN: 单次请求的超时时间，单位为秒。
        """
        self._client = OpenAI(
            api_key=api_key,
            base_url=normalize_openai_base_url(base_url),
            timeout=timeout_seconds,
        )
        self._model = model
        self._timeout_seconds = timeout_seconds

    def embed_text(self, request: EmbeddingRequest) -> list[float]:
        """
        EN: Generate an embedding vector for the given text-only request.
        CN: 通过 OpenAI API 为给定的纯文本请求生成 embedding 向量。

        Args:
            request:
                EN: Embedding request containing text and output dimensionality.
                CN: 包含文本和输出维度的 embedding 请求。

        Returns:
            EN: Embedding vector as a list of floats.
            CN: 以浮点数列表形式返回 embedding 向量。

        Raises:
            EN: ValueError if the request text is empty or the response contains no embedding.
            CN: 当请求文本为空，或响应中不包含 embedding 时抛出 ValueError。
        """
        if not request.text:
            raise ValueError("Text embedding request requires text")

        response = self._client.embeddings.with_raw_response.create(
            model=self._model,
            input=request.text,
            dimensions=request.output_dimensionality,
            encoding_format="float",
            timeout=self._timeout_seconds,
        )
        payload = self._parse_response_payload(response.http_response)
        data = payload.get("data") or []
        if not data:
            raise ValueError("OpenAI embedding response does not contain data")

        first = data[0]
        embedding = first.get("embedding") if isinstance(first, dict) else getattr(first, "embedding", None)
        if not embedding:
            raise ValueError("OpenAI embedding response does not contain embedding")
        return self._coerce_embedding_values(embedding)

    def embed_bytes(self, *, payload: bytes, mime_type: str, request: EmbeddingRequest) -> list[float]:
        """
        EN: Not supported - OpenAI client in this repository only handles text embeddings.
        CN: 不支持，本仓库里的 OpenAI 客户端只处理文本 embedding。

        Raises:
            EN: ValueError always, because binary embedding is unsupported.
            CN: 始终抛出 ValueError，因为二进制 embedding 不受支持。
        """
        raise ValueError("OpenAI embedding client does not support binary content in this repository")

    @staticmethod
    def _parse_response_payload(response: httpx.Response) -> dict[str, object]:
        """
        EN: Parse an embedding response payload regardless of the response content type.
        CN: 不管响应内容类型如何，都解析 embedding 响应载荷。
        """
        try:
            payload = response.json()
        except ValueError as exc:
            content_type = response.headers.get("content-type", "<missing>")
            raise ValueError(
                f"OpenAI embedding response is not valid JSON (content-type={content_type})"
            ) from exc

        if isinstance(payload, str):
            payload = json.loads(payload)
        if not isinstance(payload, dict):
            raise ValueError("OpenAI embedding response must be a JSON object")
        return payload

    @staticmethod
    def _coerce_embedding_values(embedding: object) -> list[float]:
        """
        EN: Convert embedding values to a list of floats, accepting either numeric lists or base64 payloads.
        CN: 将 embedding 值统一转成 float 列表，同时支持数值列表或 base64 载荷。
        """
        if isinstance(embedding, (list, tuple)):
            return [float(value) for value in embedding]
        if isinstance(embedding, str):
            raw = base64.b64decode(embedding)
            if len(raw) % 4 != 0:
                raise ValueError("OpenAI embedding base64 payload length is invalid")
            return [float(value) for value in struct.unpack(f"<{len(raw) // 4}f", raw)]
        raise ValueError(f"OpenAI embedding response contains unsupported embedding type: {type(embedding).__name__}")
