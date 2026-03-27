"""
EN: Tests for the checked-in .env.example runtime environment reference.
CN: 针对仓库内 .env.example 运行时环境变量参考文件的测试。
"""

from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[5]
ENV_EXAMPLE_PATH = REPO_ROOT / ".env.example"


def _read_env_example_keys() -> set[str]:
    """
    EN: Extract environment variable names from the example file.
    CN: 从示例文件中提取环境变量名称。
    """
    keys: set[str] = set()
    for raw_line in ENV_EXAMPLE_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _value = line.split("=", 1)
        key = key.strip()
        if key:
            keys.add(key)
    return keys


def _read_env_example_values() -> dict[str, str]:
    """
    EN: Extract environment variable values from the example file.
    CN: 从示例文件中提取环境变量值。
    """
    values: dict[str, str] = {}
    for raw_line in ENV_EXAMPLE_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key:
            values[key] = value.strip()
    return values


def test_env_example_covers_runtime_environment_variables() -> None:
    """
    EN: Verify the example file lists the runtime variables used by the serverless_mcp service package.
    CN: 验证示例文件列出 serverless_mcp 服务包实际使用的运行时变量。
    """
    keys = _read_env_example_keys()

    expected_keys = {
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
        "POWERTOOLS_SERVICE_NAME",
        "METRICS_NAMESPACE",
        "SERVERLESS_MCP_PIPELINE_CONFIG_PATH",
        "OBJECT_STATE_TABLE",
        "EXECUTION_STATE_TABLE",
        "MANIFEST_INDEX_TABLE",
        "MANIFEST_BUCKET",
        "MANIFEST_PREFIX",
        "STEP_FUNCTIONS_STATE_MACHINE_ARN",
        "EMBED_QUEUE_URL",
        "EMBEDDING_PROJECTION_STATE_TABLE",
        "VECTOR_BUCKET_NAME",
        "VECTOR_INDEX_NAME",
        "PADDLE_OCR_API_BASE_URL",
        "PADDLE_OCR_API_TOKEN",
        "PADDLE_OCR_MODEL",
        "PADDLE_OCR_POLL_INTERVAL_SECONDS",
        "PADDLE_OCR_MAX_POLL_ATTEMPTS",
        "PADDLE_OCR_HTTP_TIMEOUT_SECONDS",
        "PADDLE_OCR_STATUS_TIMEOUT_SECONDS",
        "PADDLE_OCR_ALLOWED_HOSTS",
        "EMBEDDING_PROFILES_JSON",
        "GEMINI_API_KEY",
        "GEMINI_API_BASE_URL",
        "GEMINI_EMBEDDING_MODEL",
        "GEMINI_HTTP_TIMEOUT_SECONDS",
        "OPENAI_API_KEY",
        "OPENAI_API_BASE_URL",
        "OPENAI_EMBEDDING_MODEL",
        "OPENAI_HTTP_TIMEOUT_SECONDS",
        "FAIL_ON_JOB_ERROR",
        "QUERY_TENANT_CLAIM",
        "REMOTE_MCP_DEFAULT_TENANT_ID",
        "QUERY_MAX_TOP_K",
        "QUERY_MAX_NEIGHBOR_EXPAND",
        "QUERY_PROFILE_TIMEOUT_SECONDS",
        "CLOUDFRONT_DISTRIBUTION_DOMAIN",
        "CLOUDFRONT_KEY_PAIR_ID",
        "CLOUDFRONT_PRIVATE_KEY_PEM",
        "CLOUDFRONT_PRIVATE_KEY_SECRET_ARN",
        "CLOUDFRONT_URL_TTL_SECONDS",
        "S3_LAMBDA_BUCKET_NAME",
        "S3_BUCKET_NAME",
        "S3_PREFIX",
    }

    assert expected_keys.issubset(keys)


def test_env_example_points_pipeline_config_path_at_repo_root() -> None:
    """
    EN: Verify the checked-in example points the pipeline config path at the repository root file.
    CN: 验证仓库内示例把 pipeline config 路径指向仓库根目录文件。
    """
    values = _read_env_example_values()

    assert values["SERVERLESS_MCP_PIPELINE_CONFIG_PATH"] == "pipeline-config.json"
