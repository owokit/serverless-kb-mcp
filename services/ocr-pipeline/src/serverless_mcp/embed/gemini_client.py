"""
EN: Gemini embedding client with text and binary multimodal embedding support.
CN: 支持文本和二进制多模态嵌入的 Gemini 嵌入客户端。
"""
from __future__ import annotations

import time

import httpx
from google import genai
from google.genai import types

from serverless_mcp.domain.models import EmbeddingRequest
from serverless_mcp.embed.provider_urls import normalize_gemini_base_url


class GeminiEmbeddingClient:
    """
    EN: Embedding client backed by the Google Gemini SDK with automatic retry on transient timeouts.
    CN: 基于 Google Gemini SDK 的嵌入客户端，会在瞬时超时时自动重试。
    """

    # EN: Maximum number of retry attempts for transient embedding timeouts.
    # CN: 瞬时超时嵌入操作的最大重试次数。
    _max_retry_attempts = 3

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
                EN: API key for authenticating with the Gemini API.
                CN: 用于 Gemini API 认证的 API 密钥。
            base_url:
                EN: Base URL of the Gemini API, normalized by normalize_gemini_base_url.
                CN: Gemini API 的 base URL，由 normalize_gemini_base_url 规范化。
            model:
                EN: Gemini embedding model identifier.
                CN: Gemini embedding 模型标识符。
            timeout_seconds:
                EN: Per-request timeout in seconds, converted to milliseconds internally.
                CN: 单次请求的超时时间（秒），内部会转换为毫秒。
        """
        self._client = genai.Client(
            api_key=api_key,
            http_options=types.HttpOptions(
                baseUrl=normalize_gemini_base_url(base_url),
                apiVersion="v1beta",
                timeout=_timeout_seconds_to_milliseconds(timeout_seconds),
            ),
        )
        self._model = model
        self._timeout_seconds = timeout_seconds

    def embed_text(self, request: EmbeddingRequest) -> list[float]:
        """
        EN: Generate an embedding vector for the given text-only request.
        CN: 为给定的纯文本请求生成嵌入向量。

        Args:
            request:
                EN: Embedding request containing text and output dimensionality.
                CN: 包含文本和输出维度的嵌入请求。

        Returns:
            EN: Embedding vector as a list of floats.
            CN: 以浮点数列表形式返回的嵌入向量。

        Raises:
            EN: ValueError if the request text is empty.
            CN: 当请求文本为空时抛出 ValueError。
        """
        if not request.text:
            raise ValueError("Text embedding request requires text")

        response = self._embed_content_with_retry(
            contents=request.text,
            task_type=request.task_type,
            output_dimensionality=request.output_dimensionality,
        )
        return self._extract_embedding_values(response)

    def embed_bytes(self, *, payload: bytes, mime_type: str, request: EmbeddingRequest) -> list[float]:
        """
        EN: Generate an embedding vector for binary content such as images or PDF pages.
        CN: 为图片、PDF 页面等二进制内容生成嵌入向量。

        Args:
            payload:
                EN: Raw binary content to embed.
                CN: 需要嵌入的原始二进制内容。
            mime_type:
                EN: MIME type of the binary payload.
                CN: 二进制载荷的 MIME 类型。
            request:
                EN: Embedding request providing task_type and output_dimensionality.
                CN: 提供 task_type 和 output_dimensionality 的嵌入请求。

        Returns:
            EN: Embedding vector as a list of floats.
            CN: 以浮点数列表形式返回的嵌入向量。
        """
        response = self._embed_content_with_retry(
            contents=types.Part.from_bytes(data=payload, mime_type=mime_type),
            task_type=request.task_type,
            output_dimensionality=request.output_dimensionality,
            mime_type=mime_type,
        )
        return self._extract_embedding_values(response)

    def _embed_content_with_retry(
        self,
        *,
        contents: str | types.Part,
        task_type: str,
        output_dimensionality: int,
        mime_type: str | None = None,
    ) -> types.EmbedContentResponse:
        """
        EN: Retry transient embedding read timeouts a few times before bubbling the error.
        CN: 同上。
        """
        last_error: Exception | None = None
        for attempt in range(self._max_retry_attempts):
            try:
                config_kwargs: dict[str, object] = {
                    "httpOptions": types.HttpOptions(
                        timeout=_timeout_seconds_to_milliseconds(self._timeout_seconds),
                    ),
                    "taskType": task_type,
                    "outputDimensionality": output_dimensionality,
                }
                if mime_type is not None:
                    config_kwargs["mimeType"] = mime_type
                return self._client.models.embed_content(
                    model=self._model,
                    contents=contents,
                    config=types.EmbedContentConfig(**config_kwargs),
                )
            except (httpx.TimeoutException, httpx.HTTPStatusError, httpx.NetworkError) as exc:
                if not _is_retryable_embedding_error(exc):
                    raise
                last_error = exc
                if attempt + 1 >= self._max_retry_attempts:
                    raise
                # EN: Exponential backoff before retrying a transient timeout.
                # CN: 在重试瞬时超时之前执行指数退避。
                time.sleep(2**attempt)
        raise last_error or TimeoutError("Gemini embedding request timed out")

    @staticmethod
    def _extract_embedding_values(response: types.EmbedContentResponse) -> list[float]:
        """
        EN: Extract the first embedding values list from a Gemini embed response.
        CN: 同上。
        """
        embeddings = response.embeddings or []
        if not embeddings:
            raise ValueError("Gemini embedding response does not contain embeddings")
        values = embeddings[0].values or []
        if not values:
            raise ValueError("Gemini embedding response does not contain embedding values")
        return [float(value) for value in values]


def _timeout_seconds_to_milliseconds(timeout_seconds: int) -> int:
    """
    EN: Convert the service timeout setting from seconds to the Gemini SDK's millisecond unit.
    CN: 同上。
    """
    return max(1, int(timeout_seconds) * 1000)


def _is_retryable_embedding_error(exc: Exception) -> bool:
    """
    EN: Return True for transient Gemini API failures that should be retried.
    CN: 对可恢复的 Gemini API 失败返回 True，表示应重试。
    """
    if isinstance(exc, httpx.TimeoutException):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        status_code = exc.response.status_code
        return status_code == 408 or status_code == 409 or status_code == 425 or status_code == 429 or 500 <= status_code < 600
    if isinstance(exc, httpx.NetworkError):
        return True
    return False
