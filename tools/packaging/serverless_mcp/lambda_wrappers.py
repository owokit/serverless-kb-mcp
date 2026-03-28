"""
EN: Packaging-side Lambda wrapper registry and wrapper source generator.
CN: 打包侧的 Lambda wrapper 注册表与 wrapper 源码生成器。
"""
from __future__ import annotations

LAMBDA_HANDLER_MODULES: dict[str, str] = {
    "ingest": "serverless_mcp.entrypoints.ingest",
    "extract_prepare": "serverless_mcp.entrypoints.extract",
    "extract_sync": "serverless_mcp.entrypoints.extract",
    "extract_submit": "serverless_mcp.entrypoints.extract",
    "extract_poll": "serverless_mcp.entrypoints.extract",
    "extract_persist": "serverless_mcp.entrypoints.extract",
    "extract_mark_failed": "serverless_mcp.entrypoints.extract",
    "embed": "serverless_mcp.entrypoints.embed",
    "remote_mcp": "serverless_mcp.entrypoints.remote_mcp",
    "backfill": "serverless_mcp.entrypoints.backfill",
    "job_status": "serverless_mcp.entrypoints.job_status",
}


def render_lambda_wrapper(function_key: str) -> str:
    """
    EN: Render the minimal Python wrapper that imports one handler for the package build.
    CN: 渲染最小化的 Python wrapper，用于在打包时导入单个 handler。
    """
    module_path = LAMBDA_HANDLER_MODULES[function_key]
    return f"from {module_path} import lambda_handler\n"


__all__ = ["LAMBDA_HANDLER_MODULES", "render_lambda_wrapper"]
