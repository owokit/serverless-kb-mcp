"""
EN: Tests for embedding runtime client construction including timeout, base URL normalization, and profile filtering.
CN: 同上。
"""

from __future__ import annotations

import sys
import types
from dataclasses import replace

from serverless_mcp.runtime import embedding_profiles
from serverless_mcp.runtime.config import Settings
from serverless_mcp.domain.models import EmbeddingProfile


def test_build_embedding_clients_uses_openai_timeout(monkeypatch) -> None:
    """
    EN: Build embedding clients uses openai timeout.
    CN: 楠岃瘉 build embedding clients uses openai timeout銆?
    """
    captured: dict[str, object] = {}

    class _FakeOpenAIClient:
        # EN: Stub OpenAI embedding client returning fixed vectors.
        # CN: 同上。
        def __init__(self, *, api_key: str, base_url: str, model: str, timeout_seconds: int) -> None:
            captured["api_key"] = api_key
            captured["base_url"] = base_url
            captured["model"] = model
            captured["timeout_seconds"] = timeout_seconds

    fake_module = types.ModuleType("serverless_mcp.embed.openai_client")
    fake_module.OpenAIEmbeddingClient = _FakeOpenAIClient
    monkeypatch.setitem(sys.modules, "serverless_mcp.embed.openai_client", fake_module)
    profile = EmbeddingProfile(
        profile_id="openai-text-small",
        provider="openai",
        model="text-embedding-3-small",
        dimension=1536,
        vector_bucket_name="vector-bucket",
        vector_index_name="index-openai",
        supported_content_kinds=("text",),
    )
    settings = Settings(
        object_state_table="object-state",
        manifest_index_table="manifest-index",
        manifest_bucket="manifest-bucket",
        manifest_prefix="manifests",
        openai_api_key="secret",
        openai_api_base_url="https://openai-text-embedding-lq.openai.azure.com/",
        openai_http_timeout_seconds=45,
        embedding_profiles=(profile,),
    )

    clients = embedding_profiles.build_embedding_clients(settings, profiles=(profile,))

    assert "openai-text-small" in clients
    assert captured["timeout_seconds"] == 45
    assert captured["base_url"] == "https://openai-text-embedding-lq.openai.azure.com/openai/v1/"
    assert captured["model"] == "text-embedding-3-small"


def test_build_embedding_clients_normalizes_azure_openai_root_endpoint(monkeypatch) -> None:
    """
    EN: Build embedding clients normalizes azure openai root endpoint.
    CN: 楠岃瘉 build embedding clients normalizes azure openai root endpoint銆?
    """
    captured: dict[str, object] = {}

    class _FakeOpenAIClient:
        # EN: Stub OpenAI embedding client returning fixed vectors.
        # CN: 同上。
        def __init__(self, *, api_key: str, base_url: str, model: str, timeout_seconds: int) -> None:
            captured["base_url"] = base_url

    fake_module = types.ModuleType("serverless_mcp.embed.openai_client")
    fake_module.OpenAIEmbeddingClient = _FakeOpenAIClient
    monkeypatch.setitem(sys.modules, "serverless_mcp.embed.openai_client", fake_module)
    profile = EmbeddingProfile(
        profile_id="openai-text-small",
        provider="openai",
        model="text-embedding-3-small",
        dimension=1536,
        vector_bucket_name="vector-bucket",
        vector_index_name="index-openai",
        supported_content_kinds=("text",),
    )
    settings = Settings(
        object_state_table="object-state",
        manifest_index_table="manifest-index",
        manifest_bucket="manifest-bucket",
        manifest_prefix="manifests",
        openai_api_key="secret",
        openai_api_base_url="https://openai-text-embedding-lq.openai.azure.com/",
        embedding_profiles=(profile,),
    )

    embedding_profiles.build_embedding_clients(settings, profiles=(profile,))

    assert captured["base_url"] == "https://openai-text-embedding-lq.openai.azure.com/openai/v1/"


def test_get_write_profiles_does_not_require_provider_sdk_imports() -> None:
    """
    EN: Get write profiles does not require provider sdk imports.
    CN: 楠岃瘉 get write profiles does not require provider sdk imports銆?
    """
    profile = EmbeddingProfile(
        profile_id="openai-text-small",
        provider="openai",
        model="text-embedding-3-small",
        dimension=1536,
        vector_bucket_name="vector-bucket",
        vector_index_name="index-openai",
        supported_content_kinds=("text",),
    )
    settings = Settings(
        object_state_table="object-state",
        manifest_index_table="manifest-index",
        manifest_bucket="manifest-bucket",
        manifest_prefix="manifests",
        embedding_profiles=(profile,),
    )

    assert [item.profile_id for item in embedding_profiles.get_write_profiles(settings)] == ["openai-text-small"]


def test_get_write_and_query_profiles_filter_flags() -> None:
    """
    EN: Get write and query profiles filter flags.
    CN: 楠岃瘉 get write and query profiles filter flags銆?
    """
    base = EmbeddingProfile(
        profile_id="openai-text-small",
        provider="openai",
        model="text-embedding-3-small",
        dimension=1536,
        vector_bucket_name="vector-bucket",
        vector_index_name="index-openai",
        supported_content_kinds=("text",),
    )
    write_disabled = replace(base, profile_id="query-only", enable_write=False)
    query_disabled = replace(base, profile_id="write-only", enable_query=False)
    settings = Settings(
        object_state_table="object-state",
        manifest_index_table="manifest-index",
        manifest_bucket="manifest-bucket",
        manifest_prefix="manifests",
        embedding_profiles=(base, write_disabled, query_disabled),
    )

    assert [profile.profile_id for profile in embedding_profiles.get_write_profiles(settings)] == [
        "openai-text-small",
        "write-only",
    ]
    assert [profile.profile_id for profile in embedding_profiles.get_query_profiles(settings)] == [
        "openai-text-small",
        "query-only",
    ]


def test_build_embedding_clients_skips_disabled_profiles(monkeypatch) -> None:
    """
    EN: Build embedding clients skips disabled profiles.
    CN: 楠岃瘉 build embedding clients skips disabled profiles銆?
    """
    captured: dict[str, object] = {}

    class _FakeGeminiClient:
        # EN: Stub Gemini embedding client returning fixed vectors.
        # CN: 同上。
        def __init__(self, *, api_key: str, base_url: str, model: str, timeout_seconds: int) -> None:
            captured["called"] = True

    fake_module = types.ModuleType("serverless_mcp.embed.gemini_client")
    fake_module.GeminiEmbeddingClient = _FakeGeminiClient
    monkeypatch.setitem(sys.modules, "serverless_mcp.embed.gemini_client", fake_module)
    profile = EmbeddingProfile(
        profile_id="gemini-default",
        provider="gemini",
        model="gemini-embedding-2-preview",
        dimension=3072,
        vector_bucket_name="vector-bucket",
        vector_index_name="index-gemini",
        supported_content_kinds=("text", "image"),
        enabled=False,
    )
    settings = Settings(
        object_state_table="object-state",
        manifest_index_table="manifest-index",
        manifest_bucket="manifest-bucket",
        manifest_prefix="manifests",
        gemini_api_key="secret",
        gemini_api_base_url="https://generativelanguage.googleapis.com/",
        embedding_profiles=(profile,),
    )

    clients = embedding_profiles.build_embedding_clients(settings, profiles=(profile,))

    assert clients == {}
    assert "called" not in captured


def test_build_embedding_clients_normalizes_gemini_base_url(monkeypatch) -> None:
    """
    EN: Build embedding clients normalizes gemini base url.
    CN: 楠岃瘉 build embedding clients normalizes gemini base url銆?
    """
    captured: dict[str, object] = {}

    class _FakeGeminiClient:
        # EN: Stub Gemini embedding client returning fixed vectors.
        # CN: 同上。
        def __init__(self, *, api_key: str, base_url: str, model: str, timeout_seconds: int) -> None:
            captured["base_url"] = base_url
            captured["model"] = model

    fake_module = types.ModuleType("serverless_mcp.embed.gemini_client")
    fake_module.GeminiEmbeddingClient = _FakeGeminiClient
    monkeypatch.setitem(sys.modules, "serverless_mcp.embed.gemini_client", fake_module)
    profile = EmbeddingProfile(
        profile_id="gemini-default",
        provider="gemini",
        model="gemini-embedding-2-preview",
        dimension=3072,
        vector_bucket_name="vector-bucket",
        vector_index_name="index-gemini",
        supported_content_kinds=("text", "image"),
    )
    settings = Settings(
        object_state_table="object-state",
        manifest_index_table="manifest-index",
        manifest_bucket="manifest-bucket",
        manifest_prefix="manifests",
        gemini_api_key="secret",
        gemini_api_base_url="https://generativelanguage.googleapis.com/v1beta",
        embedding_profiles=(profile,),
    )

    embedding_profiles.build_embedding_clients(settings, profiles=(profile,))

    assert captured["base_url"] == "https://generativelanguage.googleapis.com/"
    assert captured["model"] == "gemini-embedding-2-preview"
