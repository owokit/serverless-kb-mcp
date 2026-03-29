"""
EN: Shared gateway context for query-side MCP tools.
CN: 查询侧 MCP tools 共享的网关上下文。
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

from serverless_mcp.runtime.bootstrap import build_runtime_context
from serverless_mcp.runtime.config import Settings
from serverless_mcp.runtime.delivery import build_cloudfront_delivery_service
from serverless_mcp.runtime.query_runtime import build_query_service
from serverless_mcp.status.application import JobStatusService
from serverless_mcp.storage.manifest.repository import ManifestRepository
from serverless_mcp.storage.state.execution_state_repository import ExecutionStateRepository
from serverless_mcp.storage.state.object_state_repository import ObjectStateRepository
from serverless_mcp.storage.projection.repository import EmbeddingProjectionStateRepository


@dataclass(frozen=True, slots=True)
class GatewayContext:
    """
    EN: Lazily built query-side service bundle reused by warm Lambda invocations.
    CN: 供热启动 Lambda 复用的懒加载查询侧服务包。
    """

    settings: Settings
    query_service: object
    status_service: JobStatusService
    manifest_repo: ManifestRepository
    s3_client: object
    delivery_service: object | None
    object_state_repo: ObjectStateRepository
    execution_state_repo: ExecutionStateRepository | None
    projection_state_repo: EmbeddingProjectionStateRepository | None


@lru_cache(maxsize=1)
def get_gateway_context() -> GatewayContext:
    """
    EN: Build the query-side gateway context once per Lambda execution environment.
    CN: 每个 Lambda 执行环境只构建一次查询侧网关上下文。
    """
    runtime_context = build_runtime_context()
    settings = runtime_context.settings
    clients = runtime_context.clients

    object_state_repo = ObjectStateRepository(
        table_name=settings.object_state_table,
        dynamodb_client=clients.dynamodb,
    )
    execution_state_repo = None
    if settings.execution_state_table:
        execution_state_repo = ExecutionStateRepository(
            table_name=settings.execution_state_table,
            dynamodb_client=clients.dynamodb,
        )
    projection_state_repo = None
    if settings.embedding_projection_state_table:
        projection_state_repo = EmbeddingProjectionStateRepository(
            table_name=settings.embedding_projection_state_table,
            dynamodb_client=clients.dynamodb,
        )

    manifest_repo = ManifestRepository(
        manifest_bucket=settings.manifest_bucket or "",
        manifest_prefix=settings.manifest_prefix,
        s3_client=clients.s3,
        dynamodb_client=clients.dynamodb,
        manifest_index_table=settings.manifest_index_table or "",
    )

    return GatewayContext(
        settings=settings,
        query_service=build_query_service(settings),
        status_service=JobStatusService(
            settings=settings,
            s3_client=clients.s3,
            object_state_repo=object_state_repo,
            execution_state_repo=execution_state_repo,
            projection_state_repo=projection_state_repo,
            manifest_repo=manifest_repo,
        ),
        manifest_repo=manifest_repo,
        s3_client=clients.s3,
        delivery_service=build_cloudfront_delivery_service(),
        object_state_repo=object_state_repo,
        execution_state_repo=execution_state_repo,
        projection_state_repo=projection_state_repo,
    )
