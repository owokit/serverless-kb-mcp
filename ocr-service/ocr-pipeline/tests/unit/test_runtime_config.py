"""
EN: Runtime configuration contract tests that protect environment parsing and cache behavior.
CN: 保护环境解析与缓存行为的运行时配置契约测试。
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from fixtures.env import override_env, write_pipeline_config
from serverless_mcp.domain.models import EmbeddingProfile
from serverless_mcp.runtime import config as runtime_config
from serverless_mcp.runtime.config import Settings, load_settings


@pytest.fixture(autouse=True)
def _clear_runtime_config_caches() -> None:
    load_settings.cache_clear()
    runtime_config._pipeline_defaults_for_path.cache_clear()
    yield
    load_settings.cache_clear()
    runtime_config._pipeline_defaults_for_path.cache_clear()


def test_settings_from_env_requires_required_tables() -> None:
    with override_env({"OBJECT_STATE_TABLE": None, "EXECUTION_STATE_TABLE": None}):
        with pytest.raises(ValueError, match="OBJECT_STATE_TABLE"):
            Settings.from_env()


def test_settings_from_env_applies_pipeline_defaults_and_embedding_profiles(tmp_path: Path) -> None:
    config_path = write_pipeline_config(
        tmp_path / "pipeline-config.json",
        {
            "defaults": {
                "manifest_prefix": "manifests/v2",
                "gemini_api_base_url": "https://example.test/gemini/",
                "openai_embedding_model": "text-embedding-3-large",
                "paddle_api_base_url": "https://paddle.test/api",
                "cloudfront_url_ttl_seconds": 300,
                "query_tenant_claim": "tenant",
                "remote_mcp_default_tenant_id": "fallback-tenant",
            }
        },
    )
    profiles = [
        {
            "profile_id": "openai-text-small",
            "provider": "openai",
            "model": "text-embedding-3-small",
            "dimension": 1536,
            "vector_bucket_name": "vector-bucket",
            "vector_index_name": "vector-index",
            "supported_content_kinds": ["text"],
            "enabled": True,
            "enable_write": True,
            "enable_query": False,
        }
    ]
    with override_env(
        {
            "SERVERLESS_MCP_PIPELINE_CONFIG_PATH": str(config_path),
            "OBJECT_STATE_TABLE": "object-state-table",
            "EXECUTION_STATE_TABLE": "execution-state-table",
            "EMBEDDING_PROFILES_JSON": json.dumps(profiles),
            "VECTOR_BUCKET_NAME": "vector-bucket",
            "VECTOR_INDEX_NAME": "vector-index",
            "OPENAI_API_KEY": "openai-key",
            "OPENAI_API_BASE_URL": "https://openai.example/v1",
        }
    ):
        settings = Settings.from_env()

    assert settings.object_state_table == "object-state-table"
    assert settings.execution_state_table == "execution-state-table"
    assert settings.manifest_prefix == "manifests/v2"
    assert settings.gemini_api_base_url == "https://example.test/gemini/"
    assert settings.openai_embedding_model == "text-embedding-3-large"
    assert settings.paddle_api_base_url == "https://paddle.test/api"
    assert settings.cloudfront_url_ttl_seconds == 300
    assert settings.query_tenant_claim == "tenant"
    assert settings.remote_mcp_default_tenant_id == "fallback-tenant"
    assert settings.embedding_profiles == (
        EmbeddingProfile(
            profile_id="openai-text-small",
            provider="openai",
            model="text-embedding-3-small",
            dimension=1536,
            vector_bucket_name="vector-bucket",
            vector_index_name="vector-index",
            supported_content_kinds=("text",),
            enabled=True,
            enable_write=True,
            enable_query=False,
        ),
    )


def test_settings_from_env_requires_profiles_json_when_vector_settings_are_present(tmp_path: Path) -> None:
    config_path = write_pipeline_config(tmp_path / "pipeline-config.json", {"defaults": {}})
    with override_env(
        {
            "SERVERLESS_MCP_PIPELINE_CONFIG_PATH": str(config_path),
            "OBJECT_STATE_TABLE": "object-state-table",
            "EXECUTION_STATE_TABLE": "execution-state-table",
            "VECTOR_BUCKET_NAME": "vector-bucket",
            "VECTOR_INDEX_NAME": "vector-index",
        }
    ):
        with pytest.raises(ValueError, match="EMBEDDING_PROFILES_JSON is required"):
            Settings.from_env()


def test_load_settings_reuses_cached_result_until_cleared(tmp_path: Path) -> None:
    config_path = write_pipeline_config(tmp_path / "pipeline-config.json", {"defaults": {"manifest_prefix": "v1"}})
    base_env = {
        "SERVERLESS_MCP_PIPELINE_CONFIG_PATH": str(config_path),
        "OBJECT_STATE_TABLE": "object-state-table",
        "EXECUTION_STATE_TABLE": "execution-state-table",
    }
    with override_env(base_env):
        first = load_settings()
        second = load_settings()
        assert first is second

    load_settings.cache_clear()
    with override_env({**base_env, "MANIFEST_PREFIX": "manual"}):
        refreshed = load_settings()

    assert refreshed.manifest_prefix == "manual"
