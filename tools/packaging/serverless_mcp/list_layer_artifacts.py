"""
EN: CLI tool to render the Lambda layer artifact list for CI workflows.
CN: 为 CI 工作流渲染 Lambda layer 产物列表的 CLI 工具。
"""
# ruff: noqa: E402
from __future__ import annotations

import argparse
import sys
from pathlib import Path


SCRIPT_ROOT = Path(__file__).resolve().parent
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from layer_artifacts import LAYER_KEYS, render_github_env_line


def main() -> int:
    """EN: Print Lambda layer keys in github-env or plain format.
    CN: 以 github-env 或 plain 格式输出 Lambda layer key。"""
    parser = argparse.ArgumentParser(description="Render the Lambda layer artifact list for CI workflows.")
    parser.add_argument(
        "--format",
        choices=("github-env", "plain"),
        default="github-env",
        help="Choose whether to emit a GITHUB_ENV assignment or a plain newline list.",
    )
    args = parser.parse_args()

    if args.format == "plain":
        for layer_key in LAYER_KEYS:
            print(layer_key)
    else:
        print(render_github_env_line())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
