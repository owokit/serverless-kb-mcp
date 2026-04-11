"""
EN: Environment contract tests that protect the code-to-runtime boundary used by CI and AWS.
CN: 保护 CI 与 AWS 使用的代码到运行时边界的环境契约测试。
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from fixtures.env import override_env, write_pipeline_config
from serverless_mcp.runtime.config import Settings, load_settings


def test_environment_contract_requires_object_state_and_execution_state_tables() -> None:
    with override_env({"OBJECT_STATE_TABLE": None, "EXECUTION_STATE_TABLE": None}):
        with pytest.raises(ValueError, match="OBJECT_STATE_TABLE"):
            Settings.from_env()


def test_environment_contract_honors_pipeline_config_defaults_and_cache_clear(tmp_path: Path) -> None:
    config_path = write_pipeline_config(tmp_path / "pipeline-config.json", {"defaults": {"manifest_prefix": "manifests/v3", "query_max_top_k": 7}})
    base_env = {
        "SERVERLESS_MCP_PIPELINE_CONFIG_PATH": str(config_path),
        "OBJECT_STATE_TABLE": "object-state",
        "EXECUTION_STATE_TABLE": "execution-state",
    }
    with override_env(base_env):
        first = load_settings()
        assert first.manifest_prefix == "manifests/v3"
        assert first.query_max_top_k == 7

    load_settings.cache_clear()
    with override_env({**base_env, "MANIFEST_PREFIX": "explicit"}):
        second = load_settings()

    assert second.manifest_prefix == "explicit"


def test_environment_contract_accepts_embedding_profiles_json_shape(tmp_path: Path) -> None:
    config_path = write_pipeline_config(tmp_path / "pipeline-config.json", {"defaults": {}})
    payload = [
        {
            "profile_id": "openai-text-small",
            "provider": "openai",
            "model": "text-embedding-3-small",
            "dimension": 1536,
            "vector_bucket_name": "vector-bucket",
            "vector_index_name": "vector-index",
            "supported_content_kinds": ["text"],
        }
    ]
    with override_env(
        {
            "SERVERLESS_MCP_PIPELINE_CONFIG_PATH": str(config_path),
            "OBJECT_STATE_TABLE": "object-state",
            "EXECUTION_STATE_TABLE": "execution-state",
            "VECTOR_BUCKET_NAME": "vector-bucket",
            "VECTOR_INDEX_NAME": "vector-index",
            "EMBEDDING_PROFILES_JSON": json.dumps(payload),
        }
    ):
        settings = Settings.from_env()

    assert settings.embedding_profiles[0].profile_id == "openai-text-small"
    assert settings.embedding_profiles[0].vector_index_name == "vector-index"
