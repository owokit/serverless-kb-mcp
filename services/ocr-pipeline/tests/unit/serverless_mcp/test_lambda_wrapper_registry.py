"""
EN: Tests verifying Lambda wrapper generation from the handler module registry.
CN: 楠岃瘉浠?handler 妯″潡娉ㄥ唽琛ㄧ敓鎴?Lambda wrapper銆?
"""

from __future__ import annotations

import importlib
import inspect

import pytest

from tools.packaging.serverless_mcp.lambda_wrappers import LAMBDA_HANDLER_MODULES, render_lambda_wrapper


@pytest.mark.parametrize("function_key, module_path", LAMBDA_HANDLER_MODULES.items())
def test_lambda_wrapper_is_generated_from_single_source_of_truth(function_key: str, module_path: str) -> None:
    """
    EN: Lambda wrapper is generated from single source of truth.
    CN: 楠岃瘉 Lambda wrapper 浠庡崟涓€浜嬪疄鏉ユ簮鐢熸垚銆?
    """
    assert render_lambda_wrapper(function_key) == f"from {module_path} import lambda_handler\n"
    module = importlib.import_module(module_path)
    assert hasattr(module, "lambda_handler")


@pytest.mark.parametrize(
    "module_path",
    [
        "serverless_mcp.entrypoints.extract_prepare",
        "serverless_mcp.entrypoints.extract_sync",
        "serverless_mcp.entrypoints.extract_submit",
        "serverless_mcp.entrypoints.extract_poll",
        "serverless_mcp.entrypoints.extract_persist",
        "serverless_mcp.entrypoints.extract_mark_failed",
    ],
)
def test_action_entrypoints_do_not_import_legacy_extract_router(module_path: str) -> None:
    """
    EN: Ensure action-scoped entrypoints do not reintroduce the legacy extract router dependency.
    CN: 确保动作级入口不会重新引入旧的 extract router 依赖。
    """
    module = importlib.import_module(module_path)
    source = inspect.getsource(module)
    assert "entrypoints.extract import" not in source
