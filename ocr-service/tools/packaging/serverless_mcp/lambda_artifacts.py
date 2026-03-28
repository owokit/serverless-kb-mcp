"""
EN: Lambda artifact metadata: function list, ZIP naming, S3 key construction, and GitHub env rendering.
CN: Lambda 产物元数据：函数列表、ZIP 命名、S3 key 构建和 GitHub 环境变量渲染。
"""
from __future__ import annotations

from collections.abc import Iterable

# EN: Canonical list of all Lambda function keys in deployment order.
# CN: 按部署顺序排列的所有 Lambda 函数 key 规范列表。
LAMBDA_FUNCTIONS: tuple[str, ...] = (
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
)


def build_zip_name(*, repo_name: str, function_key: str) -> str:
    """EN: Construct the Lambda ZIP filename from repository name and function key.
    CN: 根据仓库名和函数 key 构建 Lambda ZIP 文件名。"""
    return f"{repo_name}_{function_key}.zip"


def build_s3_key(*, repo_name: str, function_key: str, s3_prefix: str | None) -> str:
    """EN: Construct the S3 object key for a Lambda artifact ZIP.
    CN: 构建 Lambda 产物 ZIP 的 S3 对象 key。"""
    zip_name = build_zip_name(repo_name=repo_name, function_key=function_key)
    prefix = (s3_prefix or "").strip("/")
    return f"{prefix}/{zip_name}" if prefix else zip_name


def parse_lambda_functions(raw_value: str | None) -> tuple[str, ...]:
    """EN: Parse a user-supplied function list string or return the full default set.
    CN: 解析用户提供的函数列表字符串，或返回完整的默认集合。"""
    if raw_value is None:
        return LAMBDA_FUNCTIONS

    tokens = [token.strip() for token in raw_value.replace(",", " ").split() if token.strip()]
    if not tokens:
        return LAMBDA_FUNCTIONS

    unknown = tuple(token for token in tokens if token not in LAMBDA_FUNCTIONS)
    if unknown:
        raise ValueError(f"Unknown lambda function keys: {', '.join(unknown)}")

    return tuple(tokens)


def render_github_env_line(functions: Iterable[str] | None = None) -> str:
    """EN: Render a FUNCTIONS=<space-separated> line for GITHUB_ENV assignment.
    CN: 渲染 FUNCTIONS=<空格分隔> 行以写入 GITHUB_ENV。"""
    selected = tuple(functions) if functions is not None else LAMBDA_FUNCTIONS
    return "FUNCTIONS=" + " ".join(selected)
