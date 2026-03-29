"""
EN: Lambda layer artifact metadata: layer keys, dependency mapping, ZIP naming, and S3 key construction.
CN: Lambda layer 产物元数据：layer key、依赖映射、ZIP 命名和 S3 key 构建。
"""
from __future__ import annotations

from collections.abc import Iterable


# EN: Canonical list of all Lambda layer keys.
# CN: 全部 Lambda layer key 的规范列表。
LAYER_KEYS: tuple[str, ...] = ("core", "extract", "embedding")

# EN: Mapping from layer key to its pip dependency specifications.
# CN: 从 layer key 到其 pip 依赖规格的映射。
LAYER_DEPENDENCIES: dict[str, list[str]] = {
    "core": [
        "aws-lambda-powertools>=3.12.0",
        "cryptography>=45.0.0",
        "mangum>=0.19.0",
        "mcp>=1.20.0",
    ],
    "extract": [
        "markdown-it-py>=4.0.0",
        "pypdf>=5.4.0",
        "python-docx>=1.1.2",
        "python-pptx>=1.0.2",
        "pydantic>=2.11.3",
        "requests>=2.32.0",
    ],
    "embedding": [
        "google-genai>=1.68.0",
        "openai>=2.2.0",
    ],
}

# EN: Guard ensuring layer dependency keys stay in sync with declared layer keys.
# CN: 守卫检查，确保 layer 依赖 key 与声明的 layer key 保持同步。
if set(LAYER_DEPENDENCIES) != set(LAYER_KEYS):
    raise RuntimeError("Layer dependency keys and declared layer keys must stay in sync")


def build_layer_zip_name(*, repo_name: str, layer_key: str) -> str:
    """EN: Construct the Lambda layer ZIP filename from repository name and layer key.
    CN: 根据仓库名和 layer key 构建 Lambda layer ZIP 文件名。"""
    return f"{repo_name}_{layer_key}_layer.zip"


def build_layer_s3_key(*, repo_name: str, layer_key: str, s3_prefix: str | None) -> str:
    """EN: Construct the S3 object key for a Lambda layer artifact ZIP.
    CN: 构建 Lambda layer 产物 ZIP 的 S3 对象 key。"""
    zip_name = build_layer_zip_name(repo_name=repo_name, layer_key=layer_key)
    prefix = (s3_prefix or "").strip("/")
    return f"{prefix}/{zip_name}" if prefix else zip_name


def parse_layer_keys(raw_value: str | None) -> tuple[str, ...]:
    """EN: Parse a user-supplied layer key string or return the full default set.
    CN: 解析用户提供的 layer key 字符串，或返回完整的默认集合。"""
    if raw_value is None:
        return LAYER_KEYS

    tokens = [token.strip() for token in raw_value.replace(",", " ").split() if token.strip()]
    if not tokens:
        return LAYER_KEYS

    unknown = tuple(token for token in tokens if token not in LAYER_KEYS)
    if unknown:
        raise ValueError(f"Unknown layer keys: {', '.join(unknown)}")

    return tuple(tokens)


def render_github_env_line(layer_keys: Iterable[str] | None = None) -> str:
    """EN: Render a LAYERS=<space-separated> line for GITHUB_ENV assignment.
    CN: 渲染 LAYERS=<空格分隔> 行以写入 GITHUB_ENV。"""
    selected = tuple(layer_keys) if layer_keys is not None else LAYER_KEYS
    return "LAYERS=" + " ".join(selected)
