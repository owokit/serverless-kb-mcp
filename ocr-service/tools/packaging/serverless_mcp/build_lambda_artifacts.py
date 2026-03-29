"""
EN: Build all Lambda ZIP artifacts for the extract_worker service by delegating to package_lambda.
CN: 通过调用 package_lambda 为 extract_worker 服务构建全部 Lambda ZIP 产物。
"""
# ruff: noqa: E402
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

SCRIPT_ROOT = Path(__file__).resolve().parent
REPO_ROOT = Path(__file__).resolve().parents[2]
SERVICE_ROOT = REPO_ROOT / "mcp"
# EN: Insert script directory into sys.path for sibling module imports.
# CN: 将脚本目录插入 sys.path 以便导入同级模块。
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from lambda_artifacts import parse_lambda_functions
from package_lambda import build_lambda_package


def main() -> int:
    """EN: CLI entry point that builds selected or all Lambda ZIP artifacts.
    CN: CLI 入口，构建选定或全部 Lambda ZIP 产物。"""
    parser = argparse.ArgumentParser(description="Build all Lambda ZIP artifacts for a repository.")
    parser.add_argument("--repo-name", required=True)
    parser.add_argument("--output-dir", default=str(SERVICE_ROOT / "dist"))
    parser.add_argument("--functions", default=None, help="Optional space- or comma-separated function keys.")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    selected_functions = parse_lambda_functions(args.functions)

    for function_key in selected_functions:
        zip_path = build_lambda_package(function_key=function_key, repo_name=args.repo_name, output_dir=output_dir)
        print(zip_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
