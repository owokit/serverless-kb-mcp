"""
EN: Shared runtime assembly for the internal retrieval core and MCP gateway.
CN: 内部检索核心和 MCP 网关的共享运行时组装。
"""
from __future__ import annotations

from serverless_mcp.embed.vector_repository import S3VectorRepository
from serverless_mcp.query.application import QueryService
from serverless_mcp.runtime.bootstrap import build_runtime_context
from serverless_mcp.runtime.config import Settings
from serverless_mcp.runtime.embedding_profiles import build_embedding_clients, get_query_profiles
from serverless_mcp.storage.projection.repository import EmbeddingProjectionStateRepository
from serverless_mcp.storage.state.execution_state_repository import ExecutionStateRepository
from serverless_mcp.storage.manifest.repository import ManifestRepository
from serverless_mcp.storage.state.object_state_repository import ObjectStateRepository


def build_query_service(settings: Settings | None = None) -> QueryService:
    """
    EN: Build the retrieval service with enabled profiles and shared repositories.
    CN: 使用启用的 profile 和共享存储构建检索服务。
    """
    runtime_context = build_runtime_context(settings=settings)
    active_settings = runtime_context.settings
    query_profiles = get_query_profiles(active_settings)
    if not query_profiles:
        raise ValueError("At least one embedding profile is required for the retrieval service")
    if not active_settings.manifest_bucket or not active_settings.manifest_index_table:
        raise ValueError("MANIFEST_BUCKET and MANIFEST_INDEX_TABLE are required for the retrieval service")
    if not active_settings.execution_state_table:
        raise ValueError("EXECUTION_STATE_TABLE is required for the retrieval service")
    clients = runtime_context.clients
    projection_state_repo = None
    if active_settings.embedding_projection_state_table:
        projection_state_repo = EmbeddingProjectionStateRepository(
            table_name=active_settings.embedding_projection_state_table,
            dynamodb_client=clients.dynamodb,
        )
    execution_state_repo = ExecutionStateRepository(
        table_name=active_settings.execution_state_table,
        dynamodb_client=clients.dynamodb,
    )

    return QueryService(
        embedding_clients=build_embedding_clients(active_settings, profiles=query_profiles),
        query_profiles=query_profiles,
        vector_repo=S3VectorRepository(s3vectors_client=clients.s3vectors),
        manifest_repo=ManifestRepository(
            manifest_bucket=active_settings.manifest_bucket,
            manifest_prefix=active_settings.manifest_prefix,
            s3_client=clients.s3,
            dynamodb_client=clients.dynamodb,
            manifest_index_table=active_settings.manifest_index_table,
        ),
        object_state_repo=ObjectStateRepository(
            table_name=active_settings.object_state_table,
            dynamodb_client=clients.dynamodb,
        ),
        execution_state_repo=execution_state_repo,
        projection_state_repo=projection_state_repo,
        profile_timeout_seconds=active_settings.query_profile_timeout_seconds,
    )
