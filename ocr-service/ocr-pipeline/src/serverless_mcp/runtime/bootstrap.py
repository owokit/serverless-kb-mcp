"""
EN: Runtime composition root for the serverless_mcp service package.
CN: serverless_mcp 服务包的运行时组合根。
"""
from __future__ import annotations

from dataclasses import dataclass

from serverless_mcp.runtime.aws_clients import AwsClientBundle, get_aws_clients
from serverless_mcp.runtime.config import Settings, load_settings
from serverless_mcp.storage.manifest.repository import ManifestRepository
from serverless_mcp.storage.projection.repository import EmbeddingProjectionStateRepository
from serverless_mcp.storage.state.execution_state_repository import ExecutionStateRepository
from serverless_mcp.storage.state.object_state_repository import ObjectStateRepository


@dataclass(frozen=True, slots=True)
class RuntimeContext:
    """
    EN: Resolved runtime settings and cached AWS clients for service builders.
    CN: 供服务构建器使用的已解析运行时配置和缓存 AWS 客户端。
    """

    settings: Settings
    clients: AwsClientBundle


@dataclass(frozen=True, slots=True)
class RuntimeRepositories:
    """
    EN: Shared repository bundle built from runtime settings and cached AWS clients.
    CN: 基于运行时配置和缓存 AWS clients 构建的共享 repository 组合。
    """

    object_state_repo: ObjectStateRepository
    execution_state_repo: ExecutionStateRepository | None = None
    manifest_repo: ManifestRepository | None = None
    projection_state_repo: EmbeddingProjectionStateRepository | None = None


def build_runtime_context(*, settings: Settings | None = None, clients: AwsClientBundle | None = None) -> RuntimeContext:
    """
    EN: Resolve settings and AWS clients into a single composition-root context.
    CN: 将 settings 和 AWS clients 解析为单一的组合根上下文。
    """
    active_settings = settings or load_settings()
    active_clients = clients or get_aws_clients()
    return RuntimeContext(settings=active_settings, clients=active_clients)


def build_object_state_repo(*, settings: Settings, clients: AwsClientBundle) -> ObjectStateRepository:
    """
    EN: Build the canonical object_state repository once from shared runtime inputs.
    CN: 基于共享运行时输入构建唯一的 object_state repository。
    """
    return ObjectStateRepository(table_name=settings.object_state_table, dynamodb_client=clients.dynamodb)


def build_execution_state_repo(*, settings: Settings, clients: AwsClientBundle) -> ExecutionStateRepository | None:
    """
    EN: Build the execution_state repository when the table is configured.
    CN: 在配置了表名时构建 execution_state repository。
    """
    if not settings.execution_state_table:
        return None
    return ExecutionStateRepository(table_name=settings.execution_state_table, dynamodb_client=clients.dynamodb)


def build_manifest_repo(*, settings: Settings, clients: AwsClientBundle) -> ManifestRepository | None:
    """
    EN: Build the manifest repository when manifest storage is configured.
    CN: 在配置了 manifest 存储时构建 manifest repository。
    """
    if not settings.manifest_bucket or not settings.manifest_index_table:
        return None
    return ManifestRepository(
        manifest_bucket=settings.manifest_bucket,
        manifest_prefix=settings.manifest_prefix,
        s3_client=clients.s3,
        dynamodb_client=clients.dynamodb,
        manifest_index_table=settings.manifest_index_table,
    )


def build_projection_state_repo(
    *,
    settings: Settings,
    clients: AwsClientBundle,
) -> EmbeddingProjectionStateRepository | None:
    """
    EN: Build the optional projection_state repository when the table is configured.
    CN: 在配置了表名时构建可选的 projection_state repository。
    """
    if not settings.embedding_projection_state_table:
        return None
    return EmbeddingProjectionStateRepository(
        table_name=settings.embedding_projection_state_table,
        dynamodb_client=clients.dynamodb,
    )


__all__ = [
    "RuntimeContext",
    "RuntimeRepositories",
    "build_runtime_context",
    "build_object_state_repo",
    "build_execution_state_repo",
    "build_manifest_repo",
    "build_projection_state_repo",
]
