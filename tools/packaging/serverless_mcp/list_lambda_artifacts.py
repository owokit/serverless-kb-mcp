"""
EN: CLI tool to render the Lambda function artifact list for CI workflows.
CN: 为 CI 工作流渲染 Lambda 函数产物列表的 CLI 工具。
"""
# ruff: noqa: E402
from __future__ import annotations

import argparse
import sys
from pathlib import Path

SCRIPT_ROOT = Path(__file__).resolve().parent
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from lambda_artifacts import LAMBDA_FUNCTIONS, render_github_env_line


def main() -> int:
    """EN: Print Lambda function keys in github-env or plain format.
    CN: 以 github-env 或 plain 格式输出 Lambda 函数 key。"""
    parser = argparse.ArgumentParser(description="Render the Lambda artifact list for CI workflows.")
    parser.add_argument(
        "--format",
        choices=("github-env", "plain"),
        default="github-env",
        help="Choose whether to emit a GITHUB_ENV assignment or a plain newline list.",
    )
    args = parser.parse_args()

    if args.format == "plain":
        for function_key in LAMBDA_FUNCTIONS:
            print(function_key)
    else:
        print(render_github_env_line())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
