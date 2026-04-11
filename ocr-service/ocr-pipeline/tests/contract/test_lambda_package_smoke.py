"""
EN: Lambda packaging smoke tests that verify ZIP contents and the vendored MCP import contract.
CN: 验证 ZIP 内容和 vendored MCP 导入契约的 Lambda packaging smoke 测试。
"""
from __future__ import annotations

from pathlib import Path
from zipfile import ZipFile

from fixtures.packaging import make_staged_service_tree
import package_lambda


def test_lambda_package_smoke_contains_wrapper_service_tree_and_vendored_handler(tmp_path: Path, monkeypatch) -> None:
    staging = make_staged_service_tree(tmp_path)
    monkeypatch.setattr(package_lambda, "_ensure_project_staging", lambda *, label: staging)

    zip_path = package_lambda.build_lambda_package(function_key="remote_mcp", repo_name="serverless-kb-mcp", output_dir=tmp_path / "dist")

    assert zip_path.is_file()
    with ZipFile(zip_path) as archive:
        names = set(archive.namelist())
        assert "lambda_function.py" in names
        assert "serverless_mcp/__init__.py" in names
        assert "serverless_mcp/runtime.py" in names
        assert "awslabs/mcp_lambda_handler/__init__.py" in names
        assert archive.read("lambda_function.py").decode("utf-8") == package_lambda.render_lambda_wrapper("remote_mcp")
