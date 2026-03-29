"""
EN: Build individual Lambda ZIP packages by staging service dependencies and injecting handler wrappers.
CN: 通过暂存服务依赖并注入 handler wrapper 来构建单个 Lambda ZIP 产物。
"""
# ruff: noqa: E402
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from collections.abc import Iterable
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


def _find_repo_root() -> Path:
    """EN: Locate the repository root that contains the ocr-service pipeline.
    CN: 寻找包含 ocr-service pipeline 的仓库根目录。"""
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "ocr-service" / "ocr-pipeline"
        if candidate.is_dir():
            return parent
    raise RuntimeError("Unable to locate repository root containing ocr-service/ocr-pipeline")


REPO_ROOT = _find_repo_root()
SERVICE_ROOT = REPO_ROOT / "ocr-service" / "ocr-pipeline"
SERVICE_PARENT = SERVICE_ROOT / "src"
SCRIPT_ROOT = Path(__file__).resolve().parent

# EN: Insert the service parent and script directories into sys.path for internal imports.
# CN: 将服务父目录和脚本目录插入 sys.path 以便导入内部模块。
if str(SERVICE_PARENT) not in sys.path:
    sys.path.insert(0, str(SERVICE_PARENT))
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from lambda_wrappers import LAMBDA_HANDLER_MODULES, render_lambda_wrapper
from lambda_artifacts import build_zip_name


@dataclass(slots=True)
class _SharedStaging:
    """EN: Cache entry that pairs a TemporaryDirectory with its staging path.
    CN: 同上。
    """
    tempdir: tempfile.TemporaryDirectory[str]
    staging: Path


# EN: Cache keyed by label to reuse the project staging across multiple function builds.
# CN: 同上。
_SHARED_STAGING_CACHE: dict[tuple[str, ...], _SharedStaging] = {}


def main() -> int:
    """EN: CLI entry point that builds a single Lambda ZIP for one function entrypoint.
    CN: 同上。
    """
    parser = argparse.ArgumentParser(description="Build a single Lambda ZIP for one function entrypoint.")
    parser.add_argument("--function", required=True, choices=sorted(LAMBDA_HANDLER_MODULES))
    parser.add_argument("--repo-name", required=True)
    parser.add_argument("--output-dir", default=str(SERVICE_ROOT / "dist"))
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    zip_path = build_lambda_package(function_key=args.function, repo_name=args.repo_name, output_dir=output_dir)

    print(zip_path)
    return 0


def build_lambda_package(*, function_key: str, repo_name: str, output_dir: Path) -> Path:
    """EN: Build a single Lambda ZIP and return its path.
    CN: 同上。
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    return _build_lambda_package_group(function_keys=(function_key,), repo_name=repo_name, output_dir=output_dir)[0]


def build_lambda_packages(*, function_keys: Iterable[str], repo_name: str, output_dir: Path) -> list[Path]:
    """EN: Build multiple Lambda ZIPs sharing a single project staging directory.
    CN: 同上。
    """
    if not isinstance(function_keys, Iterable):
        raise TypeError("function_keys must be an iterable of lambda keys")
    selected = tuple(dict.fromkeys(function_keys))
    if not selected:
        return []

    output_dir.mkdir(parents=True, exist_ok=True)
    staging = _ensure_project_staging(label=selected[0])
    return [
        _write_lambda_zip(
            zip_path=output_dir / build_zip_name(repo_name=repo_name, function_key=function_key),
            staging=staging,
            function_key=function_key,
        )
        for function_key in selected
    ]


def _build_lambda_package_group(
    *,
    function_keys: tuple[str, ...],
    repo_name: str,
    output_dir: Path,
) -> list[Path]:
    staging = _ensure_project_staging(label=function_keys[0])
    return [
        _write_lambda_zip(
            zip_path=output_dir / build_zip_name(repo_name=repo_name, function_key=function_key),
            staging=staging,
            function_key=function_key,
        )
        for function_key in function_keys
    ]


def _write_lambda_zip(*, zip_path: Path, staging: Path, function_key: str) -> Path:
    """EN: Write a Lambda ZIP containing staged files plus the generated handler wrapper.
    CN: 同上。
    """
    with ZipFile(zip_path, "w", ZIP_DEFLATED) as archive:
        for path in staging.rglob("*"):
            if path.is_file():
                archive.write(path, path.relative_to(staging))
        archive.writestr("lambda_function.py", render_lambda_wrapper(function_key))
    return zip_path


def _ensure_project_staging(*, label: str) -> Path:
    """EN: Create or reuse a staging directory with project dependencies installed through uv.
    CN: 创建或复用一个通过 uv 安装项目依赖的暂存目录。
    """
    cache_key = ("project",)
    shared = _SHARED_STAGING_CACHE.get(cache_key)
    if shared is not None:
        return shared.staging

    tempdir = tempfile.TemporaryDirectory(prefix=f"lambda-shared-{label}-")
    staging = Path(tempdir.name) / "staging"
    staging.mkdir(parents=True, exist_ok=True)

    _run(
        "uv",
        "pip",
        "install",
        "--no-deps",
        str(SERVICE_ROOT),
        "--target",
        str(staging),
    )

    _prune_transient_files(staging)
    _validate_staged_imports(staging)
    _SHARED_STAGING_CACHE[cache_key] = _SharedStaging(tempdir=tempdir, staging=staging)
    return staging


def _validate_staged_imports(staging: Path) -> None:
    """EN: Fail fast if the staged Lambda payload cannot import the vendored MCP handler.
    CN: 如果暂存的 Lambda 产物无法导入 vendored MCP handler，则尽早失败。"""
    env = os.environ.copy()
    pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = str(staging) if not pythonpath else f"{staging}{os.pathsep}{pythonpath}"
    subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from awslabs.mcp_lambda_handler import MCPLambdaHandler\n"
                "import awslabs.mcp_lambda_handler as module\n"
                "assert module.MCPLambdaHandler is MCPLambdaHandler\n"
                "print(module.__file__)\n"
            ),
        ],
        check=True,
        env=env,
    )


def _run(*args: str) -> None:
    """EN: Run a subprocess and raise on failure.
    CN: 同上。
    """
    subprocess.run(list(args), check=True)


def _prune_transient_files(root: Path) -> None:
    """EN: Remove __pycache__ directories and .pyc/.pyo files from a staging tree.
    CN: 同上。
    """
    for pattern in ("__pycache__", "*.pyc", "*.pyo"):
        for path in root.rglob(pattern):
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
            elif path.is_file():
                path.unlink()


if __name__ == "__main__":
    raise SystemExit(main())
