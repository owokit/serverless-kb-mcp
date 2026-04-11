"""
EN: Lambda packaging tests that protect ZIP composition, staged import validation, and cache semantics.
CN: 保护 ZIP 组成、staged import 校验和缓存语义的 Lambda packaging 测试。
"""
from __future__ import annotations

import subprocess
from pathlib import Path
from zipfile import ZipFile

import pytest

from fixtures.packaging import make_staged_service_tree
import package_lambda


@pytest.fixture(autouse=True)
def _clear_package_cache() -> None:
    package_lambda._SHARED_STAGING_CACHE.clear()
    yield
    package_lambda._SHARED_STAGING_CACHE.clear()


def test_find_repo_root_detects_workspace_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fake_root = tmp_path / "repo"
    service_root = fake_root / "ocr-service" / "ocr-pipeline"
    service_root.mkdir(parents=True)
    fake_file = service_root / "tools" / "packaging" / "serverless_mcp" / "package_lambda.py"
    fake_file.parent.mkdir(parents=True, exist_ok=True)
    fake_file.write_text("# fake\n", encoding="utf-8")
    monkeypatch.setattr(package_lambda, "__file__", str(fake_file))

    assert package_lambda._find_repo_root() == fake_root


def test_build_lambda_packages_write_zip_and_lambda_function(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    staging = make_staged_service_tree(tmp_path)
    monkeypatch.setattr(package_lambda, "_ensure_project_staging", lambda *, label: staging)

    output_dir = tmp_path / "dist"
    packages = package_lambda.build_lambda_packages(function_keys=("ingest", "remote_mcp"), repo_name="demo-repo", output_dir=output_dir)

    assert [path.name for path in packages] == ["demo-repo_ingest.zip", "demo-repo_remote_mcp.zip"]
    for zip_path, function_key in zip(packages, ("ingest", "remote_mcp"), strict=True):
        assert zip_path.is_file()
        with ZipFile(zip_path) as archive:
            names = set(archive.namelist())
            assert "lambda_function.py" in names
            assert "serverless_mcp/__init__.py" in names
            assert "awslabs/mcp_lambda_handler/__init__.py" in names
            assert archive.read("lambda_function.py").decode("utf-8") == package_lambda.render_lambda_wrapper(function_key)


def test_build_lambda_package_deduplicates_staging_cache_and_uses_single_uv_install(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    staging_root = tmp_path / "staging-root"
    staging_root.mkdir()
    run_calls: list[tuple[str, ...]] = []
    validate_calls: list[Path] = []

    def fake_run(*args: str) -> None:
        run_calls.append(args)

    def fake_validate(staging: Path) -> None:
        validate_calls.append(staging)

    monkeypatch.setattr(package_lambda, "_run", fake_run)
    monkeypatch.setattr(package_lambda, "_validate_staged_imports", fake_validate)
    monkeypatch.setattr(package_lambda.tempfile, "TemporaryDirectory", lambda prefix: type("TD", (), {"name": str(staging_root), "cleanup": lambda self: None})())

    first = package_lambda._ensure_project_staging(label="ingest")
    second = package_lambda._ensure_project_staging(label="remote_mcp")

    assert first is second
    assert run_calls == [("uv", "pip", "install", "--no-deps", str(package_lambda.SERVICE_ROOT), "--target", str(staging_root / "staging"))]
    assert validate_calls == [staging_root / "staging"]


def test_validate_staged_imports_executes_vendored_import_check_and_surfaces_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    staging = make_staged_service_tree(tmp_path)
    captured: list[list[str]] = []

    def fake_run(args, check, env):
        captured.append(list(args))
        raise subprocess.CalledProcessError(returncode=1, cmd=args)

    monkeypatch.setattr(package_lambda.subprocess, "run", fake_run)

    with pytest.raises(subprocess.CalledProcessError):
        package_lambda._validate_staged_imports(staging)

    assert captured and captured[0][0] == package_lambda.sys.executable
    assert "awslabs.mcp_lambda_handler" in " ".join(captured[0])


def test_build_lambda_package_retains_service_root_resolution_when_called_from_package_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    staging = make_staged_service_tree(tmp_path)
    monkeypatch.setattr(package_lambda, "_ensure_project_staging", lambda *, label: staging)

    output_dir = tmp_path / "dist"
    zip_path = package_lambda.build_lambda_package(function_key="ingest", repo_name="repo", output_dir=output_dir)

    assert zip_path.name == "repo_ingest.zip"
    assert package_lambda.SERVICE_ROOT.name == "ocr-pipeline"
