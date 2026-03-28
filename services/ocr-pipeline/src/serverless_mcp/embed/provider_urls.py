"""
EN: Provider URL normalization utilities for OpenAI-compatible and Gemini API endpoints.
CN: 用于 OpenAI 兼容和 Gemini API 端点的提供者 URL 规范化工具。
"""
from __future__ import annotations

from urllib.parse import urlparse, urlunparse


_AZURE_OPENAI_HOST_SUFFIXES = (".openai.azure.com", ".services.ai.azure.com")
_GEMINI_VERSION_SUFFIXES = ("/v1beta", "/v1")


def normalize_openai_base_url(base_url: str) -> str:
    """
    EN: Normalize OpenAI-compatible base URLs while preserving explicit custom path prefixes.
    CN: 规范化 OpenAI 兼容 base URL，但保留显式配置的自定义路径前缀。
    """
    parsed = _parse_absolute_url(base_url)
    host = parsed.netloc.lower()
    raw_path = (parsed.path or "").rstrip("/")

    if _is_azure_openai_host(host):
        normalized_path = "/openai/v1"
    elif _is_public_openai_host(host):
        normalized_path = "/v1"
    elif raw_path:
        # EN: Preserve caller-provided prefixes such as /api/v1 for OpenRouter and similar providers.
        # CN: 保留调用方显式提供的路径前缀，例如 OpenRouter 及类似服务的 /api/v1。
        normalized_path = raw_path
    else:
        normalized_path = "/v1"

    return urlunparse(parsed._replace(path=f"{normalized_path.rstrip('/')}/"))


def normalize_gemini_base_url(base_url: str) -> str:
    """
    EN: Normalize Gemini API base URLs so callers can pass either the root host or an explicit /v1beta suffix.
    CN: 规范化 Gemini API base URL，使调用方既可传根地址，也可传显式 /v1beta 后缀。

    Args:
        base_url:
            EN: Gemini API base URL, with or without a version path suffix.
            CN: Gemini API base URL，可带或不带版本路径后缀。

    Returns:
        EN: Normalized base URL with trailing slash and no version suffix.
        CN: 带尾部斜杠且去除版本后缀的规范化 base URL。

    Raises:
        EN: ValueError if the URL is not absolute (missing scheme or host).
        CN: 当 URL 不是绝对路径（缺少 scheme 或 host）时抛出 ValueError。
    """
    parsed = _parse_absolute_url(base_url)
    raw_path = (parsed.path or "").rstrip("/")

    # EN: Strip known Gemini version suffixes so the SDK can layer its own apiVersion.
    # CN: 剥离已知的 Gemini 版本后缀，方便 SDK 自行附加 apiVersion。
    normalized_path = raw_path
    for suffix in _GEMINI_VERSION_SUFFIXES:
        if normalized_path.endswith(suffix):
            normalized_path = normalized_path[: -len(suffix)]
            break

    if not normalized_path:
        normalized_path = "/"

    return urlunparse(parsed._replace(path=f"{normalized_path.rstrip('/')}/"))


def _parse_absolute_url(value: str):
    """
    EN: Parse a string into a URL and validate that it has an absolute scheme and host.
    CN: 将字符串解析为 URL，并验证其具备绝对路径所需的 scheme 和 host。
    """
    parsed = urlparse(value.strip())
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"Invalid base URL: {value!r}")
    return parsed


def _is_azure_openai_host(host: str) -> bool:
    """
    EN: Check whether the host belongs to Azure OpenAI or Azure AI Services.
    CN: 检查 host 是否属于 Azure OpenAI 或 Azure AI Services。
    """
    return any(host.endswith(suffix) for suffix in _AZURE_OPENAI_HOST_SUFFIXES)


def _is_public_openai_host(host: str) -> bool:
    """
    EN: Check whether the host is the public OpenAI API endpoint.
    CN: 检查 host 是否为公共 OpenAI API 端点。
    """
    return host == "api.openai.com"
