"""
EN: Embedding profile selection and provider client construction for runtime composition.
CN: 用于运行时装配的 embedding profile 选择与 provider 客户端构建模块。
"""
from __future__ import annotations

from dataclasses import replace

from serverless_mcp.domain.models import EmbeddingProfile
from serverless_mcp.embed.provider_urls import normalize_gemini_base_url, normalize_openai_base_url
from serverless_mcp.runtime.config import Settings, load_settings


def get_write_profiles(settings: Settings | None = None) -> tuple[EmbeddingProfile, ...]:
    """
    EN: Return embedding profiles that are enabled for vector writes.
    CN: 返回启用向量写入的 embedding profile。
    """
    active_settings = settings or load_settings()
    return tuple(profile for profile in active_settings.embedding_profiles if profile.enabled and profile.enable_write)


def get_query_profiles(settings: Settings | None = None) -> tuple[EmbeddingProfile, ...]:
    """
    EN: Return embedding profiles that are enabled for query-time retrieval.
    CN: 返回启用查询检索的 embedding profile。
    """
    active_settings = settings or load_settings()
    return tuple(profile for profile in active_settings.embedding_profiles if profile.enabled and profile.enable_query)


def build_embedding_clients(
    settings: Settings | None = None,
    *,
    profiles: tuple[EmbeddingProfile, ...] | None = None,
) -> dict[str, object]:
    """
    EN: Build provider clients keyed by profile_id for the supplied profile set.
    CN: 为给定的 profile 集合构建以 profile_id 为键的 provider 客户端。
    """
    active_settings = settings or load_settings()
    active_profiles = profiles if profiles is not None else active_settings.embedding_profiles
    clients: dict[str, object] = {}
    for profile in active_profiles:
        if not profile.enabled:
            continue
        if profile.provider == "gemini":
            from serverless_mcp.embed.gemini_client import GeminiEmbeddingClient

            if not active_settings.gemini_api_key:
                raise ValueError("GEMINI_API_KEY is required for Gemini embedding profiles")
            clients[profile.profile_id] = GeminiEmbeddingClient(
                api_key=active_settings.gemini_api_key,
                base_url=normalize_gemini_base_url(active_settings.gemini_api_base_url),
                model=profile.model,
                timeout_seconds=active_settings.gemini_http_timeout_seconds,
            )
            continue
        if profile.provider == "openai":
            from serverless_mcp.embed.openai_client import OpenAIEmbeddingClient

            if not active_settings.openai_api_key:
                raise ValueError("OPENAI_API_KEY is required for OpenAI embedding profiles")
            if not active_settings.openai_api_base_url:
                raise ValueError("OPENAI_API_BASE_URL is required for OpenAI embedding profiles")
            clients[profile.profile_id] = OpenAIEmbeddingClient(
                api_key=active_settings.openai_api_key,
                base_url=normalize_openai_base_url(active_settings.openai_api_base_url),
                model=profile.model,
                timeout_seconds=active_settings.openai_http_timeout_seconds,
            )
            continue
        raise ValueError(f"Unsupported embedding provider: {profile.provider}")
    return clients


def _clone_profiles(
    profiles: tuple[EmbeddingProfile, ...],
    *,
    gemini_model: str | None,
    openai_model: str | None,
) -> tuple[EmbeddingProfile, ...]:
    """
    EN: Apply provider-level model overrides while preserving vector space identity.
    CN: 应用 provider 级 model 覆盖，但保持向量空间身份不变。
    """
    if not gemini_model and not openai_model:
        return profiles

    resolved: list[EmbeddingProfile] = []
    for profile in profiles:
        if profile.provider == "gemini" and gemini_model:
            resolved.append(replace(profile, model=gemini_model))
            continue
        if profile.provider == "openai" and openai_model:
            resolved.append(replace(profile, model=openai_model))
            continue
        resolved.append(profile)
    return tuple(resolved)
