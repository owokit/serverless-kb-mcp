"""
EN: Packaging fixtures for Lambda ZIP and staging-tree tests.
CN: Lambda ZIP 与 staging tree 测试的 packaging fixtures。
"""
from __future__ import annotations

from pathlib import Path


def make_staged_service_tree(root: Path) -> Path:
    staging = root / "staging"
    (staging / "serverless_mcp").mkdir(parents=True, exist_ok=True)
    (staging / "awslabs").mkdir(parents=True, exist_ok=True)
    (staging / "awslabs" / "mcp_lambda_handler").mkdir(parents=True, exist_ok=True)
    (staging / "serverless_mcp" / "__init__.py").write_text("SERVICE = 'ok'\n", encoding="utf-8")
    (staging / "serverless_mcp" / "runtime.py").write_text("RUNTIME = 'present'\n", encoding="utf-8")
    (staging / "awslabs" / "__init__.py").write_text("# vendored namespace package marker\n", encoding="utf-8")
    (staging / "awslabs" / "mcp_lambda_handler" / "__init__.py").write_text(
        "class MCPLambdaHandler:\n    pass\n",
        encoding="utf-8",
    )
    return staging
