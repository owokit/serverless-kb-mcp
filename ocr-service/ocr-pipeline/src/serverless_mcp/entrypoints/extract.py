"""
EN: Thin Lambda wrapper that exposes the extract router entrypoint.
CN: 暴露 extract router 入口的薄 Lambda 包装层。
"""
from __future__ import annotations

from serverless_mcp.extract.handlers.router import lambda_handler as lambda_handler  # noqa: F401

__all__ = ["lambda_handler"]
