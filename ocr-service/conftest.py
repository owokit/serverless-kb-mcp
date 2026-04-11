"""
EN: Repository-level pytest bootstrap and isolation fixtures for serverless-kb-mcp.
CN: serverless-kb-mcp 的仓库级 pytest 启动与隔离 fixtures。
"""
from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parent
SERVICE_ROOT = REPO_ROOT / "ocr-pipeline"
SRC_PATH = SERVICE_ROOT / "src"
TESTS_PATH = SERVICE_ROOT / "tests"
PACKAGING_PATH = REPO_ROOT / "tools" / "packaging" / "serverless_mcp"

for path in (SRC_PATH, TESTS_PATH, PACKAGING_PATH):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


if "awslabs.mcp_lambda_handler" not in sys.modules:
    awslabs_module = types.ModuleType("awslabs")
    mcp_lambda_handler_module = types.ModuleType("awslabs.mcp_lambda_handler")
    session_module = types.ModuleType("awslabs.mcp_lambda_handler.session")

    class MCPLambdaHandler:
        def __init__(self, *args, **kwargs) -> None:
            self.args = args
            self.kwargs = kwargs
            self.tools: list[object] = []

        def tool(self):
            def decorator(func):
                self.tools.append(func)
                return func

            return decorator

        def handle_request(self, event, context):
            return {"statusCode": 200, "headers": {"Content-Type": "application/json"}, "body": "{}"}

    class NoOpSessionStore:
        pass

    mcp_lambda_handler_module.MCPLambdaHandler = MCPLambdaHandler
    session_module.NoOpSessionStore = NoOpSessionStore
    mcp_lambda_handler_module.session = session_module
    awslabs_module.mcp_lambda_handler = mcp_lambda_handler_module

    sys.modules["awslabs"] = awslabs_module
    sys.modules["awslabs.mcp_lambda_handler"] = mcp_lambda_handler_module
    sys.modules["awslabs.mcp_lambda_handler.session"] = session_module


def _clear_packaging_staging_cache() -> None:
    """EN: Clear the shared Lambda packaging staging cache between tests.
    CN: 在测试之间清空共享的 Lambda packaging staging 缓存。"""
    try:
        package_lambda = __import__("package_lambda")
    except Exception:
        return

    cache = getattr(package_lambda, "_SHARED_STAGING_CACHE", None)
    if not isinstance(cache, dict):
        return

    for shared in cache.values():
        tempdir = getattr(shared, "tempdir", None)
        cleanup = getattr(tempdir, "cleanup", None)
        if callable(cleanup):
            cleanup()
    cache.clear()


collect_ignore_glob = [
    "ocr-pipeline/tests/integration/*.py",
    "ocr-pipeline/tests/unit/runtime/*.py",
    "ocr-pipeline/tests/unit/serverless_mcp/*.py",
]


@pytest.fixture(autouse=True)
def _runtime_isolation(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Reset process-wide runtime caches and default environment between tests.
    CN: 在每个测试之间重置进程级运行时缓存和默认环境。
    """
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv("EXECUTION_STATE_TABLE", "execution-state")
    monkeypatch.delenv("SERVERLESS_MCP_PIPELINE_CONFIG_PATH", raising=False)
    monkeypatch.delenv("MCP_PIPELINE_CONFIG_PATH", raising=False)
    monkeypatch.delenv("CDK_DEFAULT_ACCOUNT", raising=False)
    monkeypatch.delenv("CDK_DEFAULT_REGION", raising=False)
    monkeypatch.delenv("AWS_ACCOUNT_ID", raising=False)
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    monkeypatch.delenv("OPENAI_API_BASE_URL", raising=False)
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("AWS_S3_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL_S3", raising=False)
    monkeypatch.delenv("AWS_DYNAMODB_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL_DYNAMODB", raising=False)
    monkeypatch.delenv("AWS_SQS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL_SQS", raising=False)
    monkeypatch.delenv("AWS_STEPFUNCTIONS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL_STEPFUNCTIONS", raising=False)
    monkeypatch.delenv("AWS_S3_VECTORS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL_S3_VECTORS", raising=False)
    monkeypatch.delenv("EMBEDDING_PROFILES_JSON", raising=False)

    _clear_packaging_staging_cache()

    for module_path, attribute in (
        ("serverless_mcp.runtime.config", "load_settings"),
        ("serverless_mcp.runtime.config", "_pipeline_defaults_for_path"),
        ("serverless_mcp.runtime.aws_clients", "get_aws_clients"),
        ("serverless_mcp.mcp_gateway.server", "get_mcp_handler"),
    ):
        try:
            module = __import__(module_path, fromlist=[attribute])
        except Exception:
            continue
        target = getattr(module, attribute, None)
        cache_clear = getattr(target, "cache_clear", None)
        if callable(cache_clear):
            cache_clear()

    yield

    _clear_packaging_staging_cache()

