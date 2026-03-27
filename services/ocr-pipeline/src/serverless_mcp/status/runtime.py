"""
EN: Lambda runtime builder for the cached job status service.
CN: 运行时缓存的 job status 服务构建器。
"""
from __future__ import annotations

from functools import lru_cache

from serverless_mcp.runtime.bootstrap import build_runtime_context
from serverless_mcp.status.application import JobStatusService


@lru_cache(maxsize=1)
def build_job_status_service() -> JobStatusService:
    """
    EN: Build and cache the read-only job status service once per Lambda runtime.
    CN: 每个 Lambda 运行环境只构建并缓存一次只读 job status 服务。
    """
    runtime_context = build_runtime_context()
    settings = runtime_context.settings
    clients = runtime_context.clients
    return JobStatusService.from_settings(
        settings,
        s3_client=clients.s3,
        dynamodb_client=clients.dynamodb,
    )
