"""
EN: Tests for Settings construction from environment variables and pipeline config defaults.
CN: 同上。
"""

import json
from pathlib import Path
import sys

import pytest

# ruff: noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[4]
SERVICE_SRC = REPO_ROOT / "ocr-pipeline" / "src"
if str(SERVICE_SRC) not in sys.path:
    sys.path.insert(0, str(SERVICE_SRC))

from serverless_mcp.runtime.config import Settings, _resolve_pipeline_config_path

PIPELINE_CONFIG_PATH = REPO_ROOT.parent / "infra" / "pipeline-config.json"


def _load_pipeline_config() -> dict[str, object]:
    return json.loads(PIPELINE_CONFIG_PATH.read_text(encoding="utf-8"))


def _pipeline_embedding_profiles_json() -> str:
    pipeline_config = _load_pipeline_config()
    return json.dumps(pipeline_config["embedding_profiles"], ensure_ascii=False, indent=2)


def test_settings_use_pipeline_config_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify Settings.from_env resolves the checked-in pipeline defaults for runtime knobs.
    CN: 同上。
    """
    config_path = PIPELINE_CONFIG_PATH
    pipeline_config = _load_pipeline_config()
    defaults = pipeline_config["defaults"]
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv("SERVERLESS_MCP_PIPELINE_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("EMBEDDING_PROFILES_JSON", _pipeline_embedding_profiles_json())
    monkeypatch.delenv("MANIFEST_PREFIX", raising=False)
    monkeypatch.delenv("PADDLE_OCR_API_BASE_URL", raising=False)
    monkeypatch.delenv("PADDLE_OCR_MODEL", raising=False)
    monkeypatch.delenv("PADDLE_OCR_POLL_INTERVAL_SECONDS", raising=False)
    monkeypatch.delenv("PADDLE_OCR_MAX_POLL_ATTEMPTS", raising=False)
    monkeypatch.delenv("PADDLE_OCR_HTTP_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("PADDLE_OCR_STATUS_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("GEMINI_API_BASE_URL", raising=False)
    monkeypatch.delenv("GEMINI_EMBEDDING_MODEL", raising=False)
    monkeypatch.delenv("OPENAI_EMBEDDING_MODEL", raising=False)
    monkeypatch.delenv("QUERY_TENANT_CLAIM", raising=False)
    monkeypatch.delenv("QUERY_MAX_TOP_K", raising=False)
    monkeypatch.delenv("QUERY_MAX_NEIGHBOR_EXPAND", raising=False)
    monkeypatch.delenv("CLOUDFRONT_URL_TTL_SECONDS", raising=False)
    monkeypatch.setenv("ALLOW_UNAUTHENTICATED_QUERY", str(defaults["allow_unauthenticated_query"]).lower())
    monkeypatch.delenv("REMOTE_MCP_DEFAULT_TENANT_ID", raising=False)

    settings = Settings.from_env()

    assert settings.manifest_prefix == defaults["manifest_prefix"]
    assert settings.paddle_api_base_url == defaults["paddle_api_base_url"]
    assert settings.paddle_ocr_model == defaults["paddle_ocr_model"]
    assert settings.paddle_poll_interval_seconds == defaults["paddle_poll_interval_seconds"]
    assert settings.paddle_max_poll_attempts == defaults["paddle_max_poll_attempts"]
    assert settings.paddle_http_timeout_seconds == defaults["paddle_http_timeout_seconds"]
    assert settings.paddle_status_timeout_seconds == defaults["paddle_status_timeout_seconds"]
    assert settings.gemini_api_base_url == defaults["gemini_api_base_url"]
    assert settings.gemini_embedding_model == defaults["gemini_embedding_model"]
    assert settings.openai_embedding_model == defaults["openai_embedding_model"]
    assert settings.allow_unauthenticated_query == defaults["allow_unauthenticated_query"]
    assert settings.query_tenant_claim == defaults["query_tenant_claim"]
    assert settings.query_max_top_k == defaults["query_max_top_k"]
    assert settings.query_max_neighbor_expand == defaults["query_max_neighbor_expand"]
    assert settings.cloudfront_url_ttl_seconds == defaults["cloudfront_url_ttl_seconds"]
    assert settings.remote_mcp_default_tenant_id == defaults["remote_mcp_default_tenant_id"]
    assert settings.vector_cleanup_state_machine_arn is None
    assert settings.embedding_profiles[0].model == defaults["openai_embedding_model"]
    assert settings.embedding_profiles[1].model == defaults["gemini_embedding_model"]


def test_runtime_pipeline_config_path_prefers_explicit_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify that the runtime config loader resolves the checked-in root pipeline config.
    CN: 验证运行时配置加载器会解析仓库根目录中的 pipeline config。
    """
    config_path = PIPELINE_CONFIG_PATH
    monkeypatch.setenv("SERVERLESS_MCP_PIPELINE_CONFIG_PATH", str(config_path))
    resolved_path = _resolve_pipeline_config_path()

    assert resolved_path == config_path
    assert resolved_path and resolved_path.exists()


def test_settings_allow_ingest_only_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify Settings.from_env works with the minimal ingest-oriented environment.
    CN: 同上。
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv("STEP_FUNCTIONS_STATE_MACHINE_ARN", "arn:aws:states:ap-southeast-1:123456789012:stateMachine:extract")
    monkeypatch.delenv("MANIFEST_INDEX_TABLE", raising=False)
    monkeypatch.delenv("MANIFEST_BUCKET", raising=False)

    settings = Settings.from_env()

    assert settings.object_state_table == "object-state"
    assert settings.step_functions_state_machine_arn == "arn:aws:states:ap-southeast-1:123456789012:stateMachine:extract"
    assert settings.manifest_index_table is None
    assert settings.manifest_bucket is None
    assert settings.manifest_prefix == ""
    assert settings.paddle_allowed_hosts == ("*.bcebos.com",)


def test_settings_accepts_canonical_state_machine_arn(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify that STEP_FUNCTIONS_STATE_MACHINE_ARN is loaded as the canonical state machine reference.
    CN: 验证 STEP_FUNCTIONS_STATE_MACHINE_ARN 会作为规范 state machine 引用被加载。
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv("STEP_FUNCTIONS_STATE_MACHINE_ARN", "arn:aws:states:ap-southeast-1:123:stateMachine:extract")

    settings = Settings.from_env()

    assert settings.step_functions_state_machine_arn == "arn:aws:states:ap-southeast-1:123:stateMachine:extract"


def test_settings_ignore_legacy_state_machine_name_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify that STEP_FUNCTIONS_STATE_MACHINE_NAME no longer populates the runtime settings.
    CN: 验证 STEP_FUNCTIONS_STATE_MACHINE_NAME 不再填充运行时配置。
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv("STEP_FUNCTIONS_STATE_MACHINE_NAME", "extract")
    monkeypatch.delenv("STEP_FUNCTIONS_STATE_MACHINE_ARN", raising=False)

    settings = Settings.from_env()

    assert settings.step_functions_state_machine_arn is None


def test_settings_load_embedding_profiles_from_pipeline_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify that EMBEDDING_PROFILES_JSON sourced from pipeline config produces two profiles with correct flags.
    CN: 楠岃瘉浠?pipeline config 鍔犺浇鐨?EMBEDDING_PROFILES_JSON 鐢熸垚涓や釜 profile 涓旀爣蹇椾綅姝ｇ‘銆?
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv("SERVERLESS_MCP_PIPELINE_CONFIG_PATH", str(PIPELINE_CONFIG_PATH))
    monkeypatch.setenv("EMBEDDING_PROFILES_JSON", _pipeline_embedding_profiles_json())

    settings = Settings.from_env()

    assert len(settings.embedding_profiles) == 2
    assert settings.embedding_profiles[0].profile_id == "openai-text-small"
    assert settings.embedding_profiles[0].provider == "openai"
    assert settings.embedding_profiles[0].vector_index_name == "openai-text-embedding-3-small-1536"
    assert settings.embedding_profiles[0].model == "text-embedding-3-small"
    assert settings.embedding_profiles[0].enabled is True
    assert settings.embedding_profiles[0].enable_write is True
    assert settings.embedding_profiles[0].enable_query is True
    assert settings.embedding_profiles[1].profile_id == "gemini-default"
    assert settings.embedding_profiles[1].provider == "gemini"
    assert settings.embedding_profiles[1].enabled is False


def test_settings_override_embedding_models_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify that OPENAI_EMBEDDING_MODEL overrides the model name in the first profile.
    CN: 同上。
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv("SERVERLESS_MCP_PIPELINE_CONFIG_PATH", str(PIPELINE_CONFIG_PATH))
    monkeypatch.setenv("EMBEDDING_PROFILES_JSON", _pipeline_embedding_profiles_json())
    monkeypatch.setenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")

    settings = Settings.from_env()

    assert len(settings.embedding_profiles) == 2
    assert settings.embedding_profiles[0].model == "text-embedding-3-small"


def test_settings_build_single_profile_from_explicit_json(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify that a single explicit JSON profile entry is parsed correctly.
    CN: 同上。
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv(
        "EMBEDDING_PROFILES_JSON",
        json.dumps(
            [
                {
                    "profile_id": "openai-text-small",
                    "provider": "openai",
                    "model": "text-embedding-3-small",
                    "dimension": 1536,
                    "vector_bucket_name": "vector-bucket",
                    "vector_index_name": "openai-text-embedding-3-small-1536",
                    "supported_content_kinds": ["text"],
                    "enable_write": True,
                    "enable_query": True,
                }
            ]
        ),
    )

    settings = Settings.from_env()

    assert len(settings.embedding_profiles) == 1
    assert settings.embedding_profiles[0].profile_id == "openai-text-small"
    assert settings.embedding_profiles[0].vector_index_name == "openai-text-embedding-3-small-1536"


def test_settings_accepts_string_boolean_values_in_embedding_profiles(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify that string boolean values are coerced when loading embedding profiles.
    CN: 验证加载 embedding profile 时会将字符串布尔值正确转换。
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv(
        "EMBEDDING_PROFILES_JSON",
        json.dumps(
            [
                {
                    "profile_id": "openai-text-small",
                    "provider": "openai",
                    "model": "text-embedding-3-small",
                    "dimension": "1536",
                    "vector_bucket_name": "vector-bucket",
                    "vector_index_name": "openai-text-embedding-3-small-1536",
                    "supported_content_kinds": ["text"],
                    "enabled": "false",
                }
            ]
        ),
    )

    settings = Settings.from_env()

    assert settings.embedding_profiles[0].dimension == 1536
    assert settings.embedding_profiles[0].enabled is False


def test_settings_reject_invalid_embedding_profile_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify that malformed embedding profile payloads raise clear validation errors.
    CN: 验证格式损坏的 embedding profile 负载会触发清晰的校验错误。
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv(
        "EMBEDDING_PROFILES_JSON",
        json.dumps(
            [
                {
                    "profile_id": "openai-text-small",
                    "provider": "openai",
                    "model": "text-embedding-3-small",
                    "dimension": 1536,
                    "vector_bucket_name": "vector-bucket",
                    "vector_index_name": "openai-text-embedding-3-small-1536",
                }
            ]
        ),
    )

    with pytest.raises(ValueError, match="supported_content_kinds is required"):
        Settings.from_env()


def test_settings_reject_model_override_without_explicit_profiles(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify that model override env vars raise when EMBEDDING_PROFILES_JSON is missing.
    CN: 同上。
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv("MANIFEST_BUCKET", "manifest-bucket")
    monkeypatch.setenv("MANIFEST_INDEX_TABLE", "manifest-index")
    monkeypatch.setenv("VECTOR_BUCKET_NAME", "vector-bucket")
    monkeypatch.setenv("VECTOR_INDEX_NAME", "index-a")
    monkeypatch.setenv("GEMINI_EMBEDDING_MODEL", "gemini-embedding-001")
    monkeypatch.delenv("EMBEDDING_PROFILES_JSON", raising=False)

    with pytest.raises(ValueError, match="EMBEDDING_PROFILES_JSON"):
        Settings.from_env()


def test_settings_parse_security_controls(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify security-related env vars are parsed into correct Settings fields.
    CN: 同上。
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv("PADDLE_OCR_ALLOWED_HOSTS", "results.example.com,*.cdn.example.com")
    monkeypatch.setenv("ALLOW_UNAUTHENTICATED_QUERY", "false")
    monkeypatch.setenv("QUERY_MAX_TOP_K", "15")
    monkeypatch.setenv("QUERY_MAX_NEIGHBOR_EXPAND", "3")

    settings = Settings.from_env()

    assert settings.paddle_allowed_hosts == ("*.bcebos.com", "results.example.com", "*.cdn.example.com")
    assert settings.allow_unauthenticated_query is False
    assert settings.query_max_top_k == 15
    assert settings.query_max_neighbor_expand == 3


def test_settings_parse_vector_cleanup_state_machine_arn(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify VECTOR_CLEANUP_STATE_MACHINE_ARN is loaded into Settings.
    CN: 楠岃瘉 VECTOR_CLEANUP_STATE_MACHINE_ARN 浼氳鍔犺浇鍒癝ettings 涓€?
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv(
        "VECTOR_CLEANUP_STATE_MACHINE_ARN",
        "arn:aws:states:ap-southeast-1:123456789012:stateMachine:vector-cleanup",
    )

    settings = Settings.from_env()

    assert settings.vector_cleanup_state_machine_arn == "arn:aws:states:ap-southeast-1:123456789012:stateMachine:vector-cleanup"


def test_settings_parse_paddle_allowlist_hosts(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify default PaddleOCR allowed hosts is empty when env var is absent.
    CN: 验证环境变量缺失时 PaddleOCR allowed hosts 默认为空。
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.delenv("PADDLE_OCR_ALLOWED_HOSTS", raising=False)

    settings = Settings.from_env()

    assert settings.paddle_allowed_hosts == ("*.bcebos.com",)


def test_settings_parse_cloudfront_delivery(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify CloudFront delivery settings are parsed from environment variables.
    CN: 楠岃瘉 CloudFront 鍒嗗彂璁剧疆浠庣幆澧冨彉閲忎腑姝ｇ‘瑙ｆ瀽銆?
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv("CLOUDFRONT_DISTRIBUTION_DOMAIN", "cdn.example.com")
    monkeypatch.setenv("CLOUDFRONT_KEY_PAIR_ID", "K123")
    monkeypatch.setenv("CLOUDFRONT_PRIVATE_KEY_PEM", "-----BEGIN PRIVATE KEY-----\nFAKE\n-----END PRIVATE KEY-----")
    monkeypatch.setenv("CLOUDFRONT_PRIVATE_KEY_SECRET_ARN", "arn:aws:secretsmanager:us-east-1:123:secret:cloudfront")
    monkeypatch.setenv("CLOUDFRONT_URL_TTL_SECONDS", "600")

    settings = Settings.from_env()

    assert settings.cloudfront_distribution_domain == "cdn.example.com"
    assert settings.cloudfront_key_pair_id == "K123"
    assert settings.cloudfront_url_ttl_seconds == 600
    assert settings.cloudfront_private_key_secret_arn == "arn:aws:secretsmanager:us-east-1:123:secret:cloudfront"


def test_settings_parse_openai_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify OPENAI_HTTP_TIMEOUT_SECONDS is parsed as an integer.
    CN: 同上。
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv("OPENAI_HTTP_TIMEOUT_SECONDS", "45")

    settings = Settings.from_env()

    assert settings.openai_http_timeout_seconds == 45


def test_settings_parse_paddle_status_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify PADDLE_OCR_STATUS_TIMEOUT_SECONDS is parsed as an integer.
    CN: 同上。
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv("PADDLE_OCR_STATUS_TIMEOUT_SECONDS", "12")

    settings = Settings.from_env()

    assert settings.paddle_status_timeout_seconds == 12


def test_settings_default_embedding_http_timeouts(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify default HTTP timeout values for embedding providers.
    CN: 楠岃瘉 embedding provider 鐨勯粯璁?HTTP 瓒呮椂鍊笺€?
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")

    settings = Settings.from_env()

    assert settings.gemini_http_timeout_seconds == 120
    assert settings.openai_http_timeout_seconds == 120


def test_settings_default_openai_embedding_model(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify the default OpenAI embedding model is text-embedding-3-small.
    CN: 同上。
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv("EMBEDDING_PROFILES_JSON", _pipeline_embedding_profiles_json())
    monkeypatch.delenv("OPENAI_EMBEDDING_MODEL", raising=False)

    settings = Settings.from_env()

    assert settings.openai_embedding_model == "text-embedding-3-small"
    assert settings.embedding_profiles[0].model == "text-embedding-3-small"
    assert settings.embedding_profiles[1].enabled is False


def test_settings_parse_openai_embedding_model(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify OPENAI_EMBEDDING_MODEL overrides the default model name.
    CN: 同上。
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv("EMBEDDING_PROFILES_JSON", _pipeline_embedding_profiles_json())
    monkeypatch.setenv("OPENAI_EMBEDDING_MODEL", "my-azure-deployment-name")

    settings = Settings.from_env()

    assert settings.embedding_profiles[0].model == "my-azure-deployment-name"
    assert settings.embedding_profiles[1].enabled is False


def test_settings_ignore_azure_openai_embedding_deployment(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify AZURE_OPENAI_EMBEDDING_DEPLOYMENT no longer overrides the embedding model.
    CN: 楠岃瘉 AZURE_OPENAI_EMBEDDING_DEPLOYMENT 涓嶅啀瑕嗙洊 embedding model銆?
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv("EMBEDDING_PROFILES_JSON", _pipeline_embedding_profiles_json())
    monkeypatch.setenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT", "my-azure-deployment-name")

    settings = Settings.from_env()

    assert settings.openai_embedding_model == "text-embedding-3-small"
    assert settings.embedding_profiles[0].model == "text-embedding-3-small"
    assert settings.embedding_profiles[1].enabled is False


def test_settings_parse_query_profile_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify QUERY_PROFILE_TIMEOUT_SECONDS supports float values.
    CN: 同上。
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv("QUERY_PROFILE_TIMEOUT_SECONDS", "12.5")

    settings = Settings.from_env()

    assert settings.query_profile_timeout_seconds == 12.5


def test_settings_accepts_canonical_openai_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify OPENAI_API_KEY and OPENAI_API_BASE_URL are loaded as canonical OpenAI settings.
    CN: 验证 OPENAI_API_KEY 和 OPENAI_API_BASE_URL 会作为规范 OpenAI 配置被加载。
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv("OPENAI_API_BASE_URL", "https://azure.example.openai.azure.com/")
    monkeypatch.setenv("OPENAI_API_KEY", "azure-secret")
    monkeypatch.delenv("AZURE_OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)

    settings = Settings.from_env()

    assert settings.openai_api_base_url == "https://azure.example.openai.azure.com/openai/v1/"
    assert settings.openai_api_key == "azure-secret"


def test_settings_preserve_openrouter_style_openai_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify that custom OpenAI-compatible path prefixes such as OpenRouter /api/v1 are preserved.
    CN: 验证 OpenRouter 这类带 /api/v1 前缀的 OpenAI 兼容地址会被原样保留。
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv("OPENAI_API_BASE_URL", "https://openrouter.ai/api/v1")
    monkeypatch.setenv("OPENAI_API_KEY", "router-secret")

    settings = Settings.from_env()

    assert settings.openai_api_base_url == "https://openrouter.ai/api/v1/"
    assert settings.openai_api_key == "router-secret"


def test_settings_ignore_legacy_openai_aliases(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify that legacy OpenAI alias variables no longer populate the runtime settings.
    CN: 验证旧 OpenAI 别名变量不再填充运行时配置。
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv("AZURE_OPENAI_API_KEY", "legacy-secret")
    monkeypatch.setenv("OPENAI_BASE_URL", "https://legacy.example.openai.azure.com/")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_BASE_URL", raising=False)

    settings = Settings.from_env()

    assert settings.openai_api_key is None
    assert settings.openai_api_base_url is None


def test_settings_ignore_azure_openai_url_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify AZURE_OPENAI_URL is ignored after the alias contraction.
    CN: 验证别名收敛后会忽略 AZURE_OPENAI_URL。
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv("AZURE_OPENAI_URL", "https://ignored.example.openai.azure.com/")

    settings = Settings.from_env()

    assert settings.openai_api_base_url is None


def test_settings_reject_duplicate_profile_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify that duplicate profile_id values raise a ValueError.
    CN: 同上。
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv(
        "EMBEDDING_PROFILES_JSON",
        """
        [
          {
            "profile_id": "dup",
            "provider": "gemini",
            "model": "gemini-embedding-2-preview",
            "dimension": 3072,
            "vector_bucket_name": "vector-a",
            "vector_index_name": "index-a",
            "supported_content_kinds": ["text"]
          },
          {
            "profile_id": "dup",
            "provider": "openai",
            "model": "text-embedding-3-small",
            "dimension": 1536,
            "vector_bucket_name": "vector-b",
            "vector_index_name": "index-b",
            "supported_content_kinds": ["text"]
          }
        ]
        """,
    )

    with pytest.raises(ValueError, match="Duplicate embedding profile_id"):
        Settings.from_env()


def test_settings_reject_duplicate_vector_bucket_index_pairs(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify that duplicate vector_bucket_name + vector_index_name pairs raise a ValueError.
    CN: 同上。
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv(
        "EMBEDDING_PROFILES_JSON",
        """
        [
          {
            "profile_id": "openai-text-small",
            "provider": "openai",
            "model": "text-embedding-3-small",
            "dimension": 1536,
            "vector_bucket_name": "vector-a",
            "vector_index_name": "index-a",
            "supported_content_kinds": ["text"]
          },
          {
            "profile_id": "openai-text-small-copy",
            "provider": "openai",
            "model": "text-embedding-3-small",
            "dimension": 1536,
            "vector_bucket_name": "vector-a",
            "vector_index_name": "index-a",
            "supported_content_kinds": ["text"]
          }
        ]
        """,
    )

    with pytest.raises(ValueError, match="Duplicate vector bucket/index pair"):
        Settings.from_env()


def test_settings_raise_clear_error_for_missing_required_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify that omitting the required OBJECT_STATE_TABLE raises a clear error.
    CN: 同上。
    """
    monkeypatch.delenv("OBJECT_STATE_TABLE", raising=False)

    with pytest.raises(ValueError, match="OBJECT_STATE_TABLE is required"):
        Settings.from_env()


def test_settings_raise_clear_error_for_missing_execution_state_table(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify that omitting the required EXECUTION_STATE_TABLE raises a clear error.
    CN: 同上。
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.delenv("EXECUTION_STATE_TABLE", raising=False)

    with pytest.raises(ValueError, match="EXECUTION_STATE_TABLE is required"):
        Settings.from_env()
