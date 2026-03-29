"""
EN: Smoke-test the Lambda OpenAI-compatible embedding client using the repo-level .env file.
CN: 使用仓库级 .env 文件对 Lambda OpenAI 兼容 embedding 客户端做冒烟测试。
"""
# EN: Reference-only scripts intentionally mutate sys.path before importing service code.
# CN: 参考脚本会在导入服务代码前有意修改 sys.path。
# ruff: noqa: E402
from __future__ import annotations

import os
import sys
from pathlib import Path


REFERENCE_ONLY = True

REPO_ROOT = Path(__file__).resolve().parents[1]
EXTRACT_WORKER_PARENT = REPO_ROOT / "services"
if str(EXTRACT_WORKER_PARENT) not in sys.path:
    sys.path.insert(0, str(EXTRACT_WORKER_PARENT))

from serverless_mcp.embed.openai_client import OpenAIEmbeddingClient
from serverless_mcp.embed.provider_urls import normalize_openai_base_url
from serverless_mcp.domain.models import EmbeddingRequest


def _load_local_env_file(path: Path) -> None:
    """
    EN: Load key-value pairs from the repo-level .env file without overriding existing shell variables.
    CN: 从仓库级 .env 文件加载键值对，但不覆盖已有的 shell 变量。
    """
    if not path.exists():
        raise FileNotFoundError(f"Missing environment file: {path}")

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or key in os.environ:
            continue
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        os.environ[key] = value


def _first_env(*names: str, default: str | None = None, required: bool = False) -> str | None:
    """
    EN: Return the first non-empty environment variable from the provided names.
    CN: 从给定名称中返回第一个非空环境变量。

    Args:
        names: Candidate environment variable names to check in order.
        default: Fallback value when no variable is set.
        required: Raise ValueError if no variable is found.
    Returns:
        The first non-empty value found, or default.
    Raises:
        ValueError: If required is True and no variable is set.
    """
    for name in names:
        value = os.environ.get(name)
        if value is not None and value.strip():
            return value.strip()
    if required:
        joined = ", ".join(names)
        raise ValueError(f"Missing required environment variable: one of {joined}")
    return default


def main() -> int:
    """
    EN: Run a single embedding request against the configured OpenAI-compatible endpoint.
    CN: 向已配置的 OpenAI 兼容端点发起一次嵌入请求。
    """
    _load_local_env_file(REPO_ROOT / ".env")

    api_key = _first_env("OPENAI_API_KEY", "AZURE_OPENAI_API_KEY", required=True)
    base_url_raw = _first_env("OPENAI_API_BASE_URL", "OPENAI_BASE_URL", required=True)
    model = _first_env("OPENAI_EMBEDDING_MODEL", default="text-embedding-3-small") or "text-embedding-3-small"
    timeout_seconds = int(_first_env("OPENAI_HTTP_TIMEOUT_SECONDS", default="120") or "120")

    resolved_base_url = normalize_openai_base_url(base_url_raw)

    print(f"OPENAI_API_BASE_URL (raw): {base_url_raw}")
    print(f"OPENAI_API_BASE_URL (resolved): {resolved_base_url}")
    print(f"OPENAI_EMBEDDING_MODEL: {model}")
    print(f"OPENAI_HTTP_TIMEOUT_SECONDS: {timeout_seconds}")

    client = OpenAIEmbeddingClient(
        api_key=api_key,
        base_url=resolved_base_url,
        model=model,
        timeout_seconds=timeout_seconds,
    )
    request = EmbeddingRequest(
        chunk_id="local#000001",
        chunk_type="page_text_chunk",
        content_kind="text",
        text="Azure OpenAI smoke test from workflow_reference_only.",
        output_dimensionality=1536,
        metadata={"source": "examples/workflows/workflow_reference_only/openai_embedding_smoke.py"},
    )

    try:
        embedding = client.embed_text(request)
    except Exception as exc:
        print(f"Embedding request failed: {type(exc).__name__}: {exc}")
        return 1

    print(f"Embedding request succeeded. Vector length: {len(embedding)}")
    print(f"First five values: {embedding[:5]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
