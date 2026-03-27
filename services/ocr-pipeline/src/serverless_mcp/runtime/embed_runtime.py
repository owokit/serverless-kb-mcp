"""
EN: Embed worker and backfill service assembly for runtime composition.
CN: 用于运行时装配的 embed worker 与 backfill 服务组装模块。
"""
from __future__ import annotations

from serverless_mcp.embed.application import EmbedWorker
from serverless_mcp.embed.asset_source import EmbedAssetSource
from serverless_mcp.embed.backfill import EmbeddingBackfillService
from serverless_mcp.embed.dispatcher import EmbeddingJobDispatcher
from serverless_mcp.embed.vector_repository import S3VectorRepository
from serverless_mcp.runtime.bootstrap import build_runtime_context
from serverless_mcp.runtime.config import Settings
from serverless_mcp.runtime.embedding_profiles import build_embedding_clients, get_write_profiles
from serverless_mcp.storage.projection.repository import EmbeddingProjectionStateRepository
from serverless_mcp.storage.state.execution_state_repository import ExecutionStateRepository
from serverless_mcp.storage.manifest.repository import ManifestRepository
from serverless_mcp.storage.state.object_state_repository import ObjectStateRepository


def build_embed_worker(settings: Settings | None = None) -> EmbedWorker:
    """
    EN: Build the embed worker with profile-aware provider clients and S3 Vectors repository.
    CN: 使用按 profile 感知的 provider 客户端和 S3 Vectors 仓库构建 embed worker。
    """
    runtime_context = build_runtime_context(settings=settings)
    active_settings = runtime_context.settings
    write_profiles = get_write_profiles(active_settings)
    if not write_profiles:
        raise ValueError("At least one embedding profile is required for embed worker")
    if len(write_profiles) > 1 and not active_settings.embedding_projection_state_table:
        raise ValueError("EMBEDDING_PROJECTION_STATE_TABLE is required when multiple write profiles are active")
    if not active_settings.manifest_bucket or not active_settings.manifest_index_table:
        raise ValueError("MANIFEST_BUCKET and MANIFEST_INDEX_TABLE are required for embed worker")
    if not active_settings.execution_state_table:
        raise ValueError("EXECUTION_STATE_TABLE is required for embed worker")
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

    return EmbedWorker(
        embedding_clients=build_embedding_clients(active_settings, profiles=write_profiles),
        embedding_profiles={profile.profile_id: profile for profile in write_profiles},
        asset_source=EmbedAssetSource(s3_client=clients.s3),
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
    )


def build_backfill_service(settings: Settings | None = None) -> EmbeddingBackfillService:
    """
    EN: Build the historical embedding backfill service.
    CN: 构建历史 embedding 回填服务。
    """
    from serverless_mcp.extract.application import ExtractionService

    runtime_context = build_runtime_context(settings=settings)
    active_settings = runtime_context.settings
    if not active_settings.embed_queue_url:
        raise ValueError("EMBED_QUEUE_URL is required for embedding backfill")
    if not active_settings.manifest_bucket or not active_settings.manifest_index_table:
        raise ValueError("MANIFEST_BUCKET and MANIFEST_INDEX_TABLE are required for embedding backfill")
    if not active_settings.execution_state_table:
        raise ValueError("EXECUTION_STATE_TABLE is required for embedding backfill")
    clients = runtime_context.clients
    profiles = get_write_profiles(active_settings)
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

    return EmbeddingBackfillService(
        extraction_service=ExtractionService(),
        object_state_repo=ObjectStateRepository(
            table_name=active_settings.object_state_table,
            dynamodb_client=clients.dynamodb,
        ),
        execution_state_repo=execution_state_repo,
        manifest_repo=ManifestRepository(
            manifest_bucket=active_settings.manifest_bucket,
            manifest_prefix=active_settings.manifest_prefix,
            s3_client=clients.s3,
            dynamodb_client=clients.dynamodb,
            manifest_index_table=active_settings.manifest_index_table,
        ),
        embed_dispatcher=EmbeddingJobDispatcher(
            queue_url=active_settings.embed_queue_url,
            sqs_client=clients.sqs,
        ),
        embedding_profiles={profile.profile_id: profile for profile in profiles},
        projection_state_repo=projection_state_repo,
    )
