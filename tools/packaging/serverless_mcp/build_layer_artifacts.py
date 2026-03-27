"""
EN: Build Lambda layer ZIP artifacts by installing dependencies into staging directories.
CN: 通过将依赖安装到暂存目录来构建 Lambda layer ZIP 产物。
"""
# ruff: noqa: E402
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Iterable
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


REPO_ROOT = Path(__file__).resolve().parents[2]
SERVICE_ROOT = REPO_ROOT / "mcp"
SCRIPT_ROOT = Path(__file__).resolve().parent

# EN: Insert script directory into sys.path for sibling module imports.
# CN: 将脚本目录插入 sys.path 以便导入同级模块。
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from layer_artifacts import LAYER_DEPENDENCIES, build_layer_zip_name, parse_layer_keys


@dataclass(slots=True)
class _SharedStaging:
    """EN: Cache entry that pairs a TemporaryDirectory with its staging path.
    CN: 将 TemporaryDirectory 与暂存路径配对的缓存条目。"""
    tempdir: tempfile.TemporaryDirectory[str]
    staging: Path


_SHARED_STAGING_CACHE: dict[tuple[str, ...], _SharedStaging] = {}
# EN: Cache keyed by dependency tuples to avoid reinstalling the same layer dependencies.
# CN: 以依赖元组为 key 的缓存，避免重复安装相同的 layer 依赖。


def main() -> int:
    """EN: CLI entry point that builds selected or all Lambda layer ZIP artifacts.
    CN: CLI 入口，构建选定或全部 Lambda layer ZIP 产物。"""
    parser = argparse.ArgumentParser(description="Build Lambda layer ZIP artifacts for the repository.")
    parser.add_argument("--repo-name", required=True)
    parser.add_argument("--output-dir", default=str(SERVICE_ROOT / "dist" / "layers"))
    parser.add_argument("--layers", default=None, help="Optional space- or comma-separated layer keys.")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    selected_layers = parse_layer_keys(args.layers)
    build_layer_packages(layer_keys=selected_layers, repo_name=args.repo_name, output_dir=output_dir)
    for layer_key in selected_layers:
        print(output_dir / build_layer_zip_name(repo_name=args.repo_name, layer_key=layer_key))
    return 0


def build_layer_packages(*, layer_keys: Iterable[str], repo_name: str, output_dir: Path) -> list[Path]:
    """EN: Build layer ZIP packages, using parallelism when multiple layers are selected.
    CN: 构建 layer ZIP 包，多个 layer 时使用并行构建。"""
    if not isinstance(layer_keys, Iterable):
        raise TypeError("layer_keys must be an iterable of layer keys")
    selected = tuple(dict.fromkeys(layer_keys))
    if not selected:
        return []

    output_dir.mkdir(parents=True, exist_ok=True)
    if len(selected) == 1:
        layer_key = selected[0]
        return [
            _build_layer_package(
                layer_key=layer_key,
                dependencies=tuple(LAYER_DEPENDENCIES[layer_key]),
                repo_name=repo_name,
                output_dir=output_dir,
            )
        ]

    results: list[Path] = []
    max_workers = min(len(selected), max(1, os.cpu_count() or 1), 4)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                _build_layer_package,
                layer_key=layer_key,
                dependencies=tuple(LAYER_DEPENDENCIES[layer_key]),
                repo_name=repo_name,
                output_dir=output_dir,
            )
            for layer_key in selected
        ]
        for future in futures:
            results.append(future.result())
    return results


def _build_layer_package(*, layer_key: str, dependencies: tuple[str, ...], repo_name: str, output_dir: Path) -> Path:
    """EN: Build a single layer ZIP from an ensured staging directory.
    CN: 从已准备好的暂存目录构建单个 layer ZIP。"""
    staging = _ensure_layer_staging(dependencies=dependencies, label=layer_key)
    zip_path = output_dir / build_layer_zip_name(repo_name=repo_name, layer_key=layer_key)
    with ZipFile(zip_path, "w", ZIP_DEFLATED) as archive:
        for path in staging.rglob("*"):
            if path.is_file():
                archive.write(path, path.relative_to(staging))
    return zip_path


def _ensure_layer_staging(*, dependencies: tuple[str, ...], label: str) -> Path:
    """EN: Create or reuse a staging directory with layer dependencies installed through uv.
    CN: 创建或复用一个通过 uv 安装 layer 依赖的暂存目录。"""
    shared = _SHARED_STAGING_CACHE.get(dependencies)
    if shared is not None:
        return shared.staging

    tempdir = tempfile.TemporaryDirectory(prefix=f"lambda-layer-{label}-")
    staging = Path(tempdir.name) / "staging" / "python"
    staging.mkdir(parents=True, exist_ok=True)
    _run(
        "uv",
        "pip",
        "install",
        "--upgrade",
        *dependencies,
        "--target",
        str(staging),
    )
    _prune_transient_files(staging.parent)
    _SHARED_STAGING_CACHE[dependencies] = _SharedStaging(tempdir=tempdir, staging=staging.parent)
    return staging.parent


def _run(*args: str) -> None:
    """EN: Run a subprocess and raise on failure.
    CN: 运行子进程，失败时抛出异常。"""
    subprocess.run(list(args), check=True)


def _prune_transient_files(root: Path) -> None:
    """EN: Remove __pycache__ directories and .pyc/.pyo files from a staging tree.
    CN: 从暂存目录树中移除 __pycache__ 目录和 .pyc/.pyo 文件。"""
    for pattern in ("__pycache__", "*.pyc", "*.pyo"):
        for path in root.rglob(pattern):
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
            elif path.is_file():
                path.unlink()


if __name__ == "__main__":
    raise SystemExit(main())
