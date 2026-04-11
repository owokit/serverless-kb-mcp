"""
EN: Lambda wrapper registry tests that ensure every public handler can be rendered and imported.
CN: 确保每个公开 handler 都能被渲染并导入的 Lambda wrapper registry 测试。
"""
from __future__ import annotations

import importlib

import lambda_wrappers


def test_registry_covers_all_expected_lambda_keys() -> None:
    assert set(lambda_wrappers.LAMBDA_HANDLER_MODULES) == {
        "ingest",
        "extract_prepare",
        "extract_sync",
        "extract_submit",
        "extract_poll",
        "extract_persist",
        "extract_mark_failed",
        "embed",
        "remote_mcp",
        "backfill",
        "job_status",
    }


def test_registry_modules_exist_and_expose_lambda_handler() -> None:
    for function_key, module_path in lambda_wrappers.LAMBDA_HANDLER_MODULES.items():
        module = importlib.import_module(module_path)
        assert hasattr(module, "lambda_handler"), function_key
        assert callable(getattr(module, "lambda_handler")), function_key


def test_render_lambda_wrapper_contains_one_public_handler_import_per_entry() -> None:
    for function_key, module_path in lambda_wrappers.LAMBDA_HANDLER_MODULES.items():
        rendered = lambda_wrappers.render_lambda_wrapper(function_key)
        assert rendered == f"from {module_path} import lambda_handler\n"
        assert rendered.count("import lambda_handler") == 1
        assert rendered.startswith("from ")
