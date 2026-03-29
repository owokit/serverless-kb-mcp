"""
EN: Tests for Lambda packaging including zip generation, wrapper injection, and dependency group reuse.
CN: 测试 Lambda 打包，包括 zip 生成、wrapper 注入和依赖组复用。
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from zipfile import ZipFile

import pytest


def _load_lambda_wrappers():
    script_path = Path(__file__).resolve().parents[4] / "tools" / "packaging" / "serverless_mcp" / "lambda_wrappers.py"
    module_name = "lambda_wrappers_test_module"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec and spec.loader, f"unable to load {script_path}"
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


render_lambda_wrapper = _load_lambda_wrappers().render_lambda_wrapper


def _write_fake_staged_packages(target: Path, *, function_name: str) -> None:
    """EN: Create the staged service and vendored awslabs packages used by packaging tests.
    CN: 为打包测试创建 staged service 与 vendored awslabs 包。"""
    package_root = target / "serverless_mcp"
    package_root.mkdir(parents=True, exist_ok=True)
    (package_root / "__init__.py").write_text("", encoding="utf-8")
    handler_file = {
        "ingest": "ingest_handler.py",
        "extract_prepare": "extract_handler.py",
        "extract_sync": "extract_handler.py",
        "extract_submit": "extract_handler.py",
        "extract_poll": "extract_handler.py",
        "extract_persist": "extract_handler.py",
        "extract_mark_failed": "extract_handler.py",
        "embed": "embed_handler.py",
        "remote_mcp": "remote_mcp_handler.py",
        "backfill": "backfill_handler.py",
        "job_status": "job_status_handler.py",
    }[function_name]
    (package_root / handler_file).write_text("lambda_handler = object()\n", encoding="utf-8")

    awslabs_root = target / "awslabs" / "mcp_lambda_handler"
    awslabs_root.mkdir(parents=True, exist_ok=True)
    (awslabs_root / "__init__.py").write_text(
        "from .mcp_lambda_handler import MCPLambdaHandler\n",
        encoding="utf-8",
    )
    (awslabs_root / "mcp_lambda_handler.py").write_text(
        "class MCPLambdaHandler:\n    pass\n",
        encoding="utf-8",
    )


def _load_package_script():
    script_path = Path(__file__).resolve().parents[4] / "tools" / "packaging" / "serverless_mcp" / "package_lambda.py"
    module_name = "package_lambda_test_module"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec and spec.loader, f"unable to load {script_path}"
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "function_name",
    [
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
    ],
)
def test_package_lambda_generates_wrapper_and_project_sources(tmp_path, monkeypatch, function_name: str) -> None:
    """
    EN: Package lambda generates wrapper and project sources.
    CN: 验证 package_lambda 会生成 wrapper 和项目源码。
    """
    module = _load_package_script()
    output_dir = tmp_path / "dist"
    calls: list[tuple[str, ...]] = []

    def fake_run(*args: str) -> None:
        calls.append(args)
        if "--target" not in args:
            return

        target = Path(args[args.index("--target") + 1])
        _write_fake_staged_packages(target, function_name=function_name)

    monkeypatch.setattr(module, "_run", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "package_lambda.py",
            "--function",
            function_name,
            "--repo-name",
            "serverless-kb-mcp",
            "--output-dir",
            str(output_dir),
        ],
    )

    exit_code = module.main()

    zip_path = output_dir / f"serverless-kb-mcp_{function_name}.zip"
    assert exit_code == 0
    assert zip_path.exists()
    assert len(calls) == 1

    with ZipFile(zip_path) as archive:
        names = set(archive.namelist())
        wrapper_source = archive.read("lambda_function.py").decode("utf-8")

    assert "lambda_function.py" in names
    assert "serverless_mcp/__init__.py" in names
    assert "awslabs/mcp_lambda_handler/__init__.py" in names
    assert "awslabs/mcp_lambda_handler/mcp_lambda_handler.py" in names
    assert wrapper_source == render_lambda_wrapper(function_name)


def test_package_lambda_zip_imports_vendored_awslabs_handler(tmp_path, monkeypatch) -> None:
    """
    EN: The staged Lambda ZIP should import the vendored MCP handler successfully.
    CN: staged Lambda ZIP 应能成功导入 vendored MCP handler。
    """
    module = _load_package_script()
    output_dir = tmp_path / "dist"

    def fake_run(*args: str) -> None:
        if "--target" not in args:
            return

        target = Path(args[args.index("--target") + 1])
        _write_fake_staged_packages(target, function_name="remote_mcp")

    monkeypatch.setattr(module, "_run", fake_run)

    zip_path = module.build_lambda_package(
        function_key="remote_mcp",
        repo_name="serverless-kb-mcp",
        output_dir=output_dir,
    )

    with ZipFile(zip_path) as archive:
        extracted = tmp_path / "unzipped"
        archive.extractall(extracted)

    saved_modules = {
        name: sys.modules.pop(name)
        for name in list(sys.modules)
        if name == "awslabs" or name.startswith("awslabs.")
    }
    sys.path.insert(0, str(extracted))
    try:
        import awslabs.mcp_lambda_handler as handler_module
        from awslabs.mcp_lambda_handler import MCPLambdaHandler

        assert handler_module.MCPLambdaHandler is MCPLambdaHandler
    finally:
        sys.path.remove(str(extracted))
        sys.modules.update(saved_modules)


def test_package_lambda_reuses_shared_staging_for_same_dependency_group(tmp_path, monkeypatch) -> None:
    """
    EN: Package lambda reuses shared staging for same dependency group.
    CN: 验证 package_lambda 会为相同依赖组复用共享 staging。
    """
    module = _load_package_script()
    output_dir = tmp_path / "dist"
    calls: list[tuple[str, ...]] = []

    def fake_run(*args: str) -> None:
        calls.append(args)
        if "--target" not in args:
            return

        target = Path(args[args.index("--target") + 1])
        _write_fake_staged_packages(target, function_name="remote_mcp")

    monkeypatch.setattr(module, "_run", fake_run)

    first_zip = module.build_lambda_package(
        function_key="remote_mcp",
        repo_name="serverless-kb-mcp",
        output_dir=output_dir,
    )
    second_zip = module.build_lambda_package(
        function_key="extract_prepare",
        repo_name="serverless-kb-mcp",
        output_dir=output_dir,
    )

    assert first_zip.exists()
    assert second_zip.exists()
    assert len(calls) == 1


def test_build_lambda_packages_reuses_each_dependency_group_once(tmp_path, monkeypatch) -> None:
    """
    EN: Build lambda packages reuses each dependency group once.
    CN: 验证 build_lambda_packages 会让每个依赖组只复用一次。
    """
    module = _load_package_script()
    output_dir = tmp_path / "dist"
    calls: list[tuple[str, ...]] = []

    def fake_run(*args: str) -> None:
        calls.append(args)
        if "--target" not in args:
            return

        target = Path(args[args.index("--target") + 1])
        _write_fake_staged_packages(target, function_name="remote_mcp")
        package_root = target / "serverless_mcp"
        (package_root / "extract_handler.py").write_text("lambda_handler = object()\n", encoding="utf-8")

    monkeypatch.setattr(module, "_run", fake_run)

    zip_paths = module.build_lambda_packages(
        function_keys=["remote_mcp", "extract_prepare"],
        repo_name="serverless-kb-mcp",
        output_dir=output_dir,
    )

    assert [path.name for path in zip_paths] == [
        "serverless-kb-mcp_remote_mcp.zip",
        "serverless-kb-mcp_extract_prepare.zip",
    ]
    assert all(path.exists() for path in zip_paths)
    assert len(calls) == 1
