"""
EN: Shared pytest fixtures for runtime configuration tests.
CN: 运行时配置测试共用的 pytest fixture。
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"

# EN: Add the src directory to sys.path so serverless_mcp package is importable in tests.
# CN: 将 src 目录加入 sys.path，确保 serverless_mcp 包在测试中可导入。
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))


# EN: Environment variables that may leak from the host shell and affect test isolation.
# CN: 可能从宿主机 shell 泄漏并影响测试隔离的环境变量列表。
_NOISY_ENV_VARS = (
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "S3_VECTORS_REGION",
    "TEST_AWS_REGION",
    "VECTOR_BUCKET_NAME",
    "VECTOR_INDEX_NAME",
    "EMBEDDING_PROFILES_JSON",
    "S3_VECTORS_BUCKET_NAME",
    "S3_VECTORS_INDEX_NAME",
    "GEMINI_HTTP_TIMEOUT_SECONDS",
    "OPENAI_HTTP_TIMEOUT_SECONDS",
    "PADDLE_OCR_STATUS_TIMEOUT_SECONDS",
    "QUERY_PROFILE_TIMEOUT_SECONDS",
    "QUERY_MAX_TOP_K",
    "QUERY_MAX_NEIGHBOR_EXPAND",
    "CLOUDFRONT_URL_TTL_SECONDS",
    "OPENAI_EMBEDDING_MODEL",
    "GEMINI_EMBEDDING_MODEL",
    "AZURE_OPENAI_EMBEDDING_DEPLOYMENT",
)


@pytest.fixture(autouse=True)
def _default_execution_state_table(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Provide a default execution-state table for tests that exercise runtime settings.
    CN: 为涉及运行时设置的测试提供默认 execution-state 表名。
    """
    monkeypatch.setenv("EXECUTION_STATE_TABLE", "execution-state")


@pytest.fixture(autouse=True)
def _clear_noisy_embedding_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Auto-use fixture that removes noisy embedding env vars before every test.
    CN: 自动使用的 fixture，在每个测试前移除干扰性的 embedding 环境变量。
    """
    for name in _NOISY_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
