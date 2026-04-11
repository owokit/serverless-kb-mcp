"""
EN: Public entrypoint contract tests that fail when public Lambda modules drift or lose lambda_handler exports.
CN: 当公开 Lambda 模块漂移或丢失 lambda_handler 导出时失败的公开入口契约测试。
"""
from __future__ import annotations

import importlib

from lambda_wrappers import LAMBDA_HANDLER_MODULES

PUBLIC_ENTRYPOINTS = {
    "serverless_mcp.entrypoints.ingest",
    "serverless_mcp.mcp_gateway.handler",
}


def test_lambda_entrypoint_modules_import_cleanly() -> None:
    for module_path in sorted(PUBLIC_ENTRYPOINTS):
        module = importlib.import_module(module_path)
        assert module is not None


def test_public_entrypoints_export_lambda_handler() -> None:
    for module_path in sorted(PUBLIC_ENTRYPOINTS | set(LAMBDA_HANDLER_MODULES.values())):
        module = importlib.import_module(module_path)
        assert hasattr(module, "lambda_handler"), module_path
        assert callable(getattr(module, "lambda_handler")), module_path
