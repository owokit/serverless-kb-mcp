"""
EN: Lambda packaging smoke tests that verify ZIP contents and the vendored MCP import contract.
CN: 验证 ZIP 内容和 vendored MCP 导入契约的 Lambda packaging smoke 测试。
"""
from __future__ import annotations

import sys
from pathlib import Path
from zipfile import ZipFile

import package_lambda


def test_lambda_package_smoke_contains_wrapper_service_tree_and_vendored_handler(tmp_path: Path) -> None:
    zip_path = package_lambda.build_lambda_package(function_key="remote_mcp", repo_name="serverless-kb-mcp", output_dir=tmp_path / "dist")

    assert zip_path.is_file()
    with ZipFile(zip_path) as archive:
        names = set(archive.namelist())
        extracted = tmp_path / "unzipped"
        archive.extractall(extracted)
        assert "lambda_function.py" in names
        assert "awslabs/mcp_lambda_handler/__init__.py" in names
        assert archive.read("lambda_function.py").decode("utf-8") == package_lambda.render_lambda_wrapper("remote_mcp")

    saved_modules = {
        name: sys.modules.pop(name)
        for name in list(sys.modules)
        if name == "awslabs" or name.startswith("awslabs.")
    }
    sys.path.insert(0, str(extracted))
    try:
        import lambda_function
        import awslabs.mcp_lambda_handler as handler_module
        from awslabs.mcp_lambda_handler import MCPLambdaHandler

        assert hasattr(lambda_function, "lambda_handler")
        assert handler_module.MCPLambdaHandler is MCPLambdaHandler
    finally:
        sys.path.remove(str(extracted))
        sys.modules.update(saved_modules)
