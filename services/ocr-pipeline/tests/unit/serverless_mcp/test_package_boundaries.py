"""
EN: Tests for package boundary cleanup around the new storage and application layout.
CN: 新 storage 和 application 布局的包边界收口测试。
"""

from __future__ import annotations

from importlib.util import find_spec
from pathlib import Path

import serverless_mcp
from serverless_mcp.embed.application import EmbedWorker as NewEmbedWorker
from serverless_mcp.extract.application import ExtractionService as NewExtractionService
from serverless_mcp.server.fastmcp import FastMCP as NewFastMCP
from serverless_mcp.server.transport_security import TransportSecuritySettings as NewTransportSecuritySettings
from serverless_mcp.query.application import QueryService as NewQueryService
from serverless_mcp.status.application import JobStatusService as NewJobStatusService
from serverless_mcp.storage.batch import flush_batch_write
from serverless_mcp.storage.manifest.repository import ManifestRepository
from serverless_mcp.storage.paths import build_manifest_key, optimize_source_file_name
from serverless_mcp.storage.projection.repository import EmbeddingProjectionStateRepository
from serverless_mcp.storage.state.object_state_repository import ObjectStateRepository


def _missing_spec(name: str) -> bool:
    """
    EN: Return True when a module spec is absent or the parent package no longer exists.
    CN: 当模块 spec 缺失或父包已不存在时返回 True。
    """
    try:
        return find_spec(name) is None
    except ModuleNotFoundError:
        return True


def test_legacy_shell_modules_are_gone() -> None:
    """
    EN: The old shell modules should no longer be importable.
    CN: 旧的壳模块不应再可导入。
    """
    assert _missing_spec("serverless_mcp.storage.manifest_repository")
    assert _missing_spec("serverless_mcp.storage.object_state_repository")
    assert _missing_spec("serverless_mcp.storage.execution_state_repository")
    assert _missing_spec("serverless_mcp.storage.embedding_projection_state_repository")
    assert _missing_spec("serverless_mcp.storage.manifest_paths")
    assert _missing_spec("serverless_mcp.storage.dynamo_batch")
    assert _missing_spec("serverless_mcp.embed.worker")
    assert _missing_spec("serverless_mcp.embed.parser")
    assert _missing_spec("serverless_mcp.events.parser")
    assert _missing_spec("serverless_mcp.query.service")
    assert _missing_spec("serverless_mcp.extract.service")
    assert _missing_spec("serverless_mcp.status.service")
    assert _missing_spec("serverless_mcp.runtime.pipeline")
    assert _missing_spec("serverless_mcp.runtime.step_functions_workflow")
    assert _missing_spec("serverless_mcp.runtime.worker")
    assert _missing_spec("serverless_mcp.runtime.embedding_runtime")
    assert _missing_spec("serverless_mcp.app.bootstrap")
    assert _missing_spec("serverless_mcp.query_api")
    assert _missing_spec("serverless_mcp.lambda_entrypoints")
    assert _missing_spec("serverless_mcp.entrypoints.lambda_entrypoints")


def test_canonical_storage_modules_are_importable() -> None:
    """
    EN: Canonical storage modules should expose the expected implementations directly.
    CN: 正式 storage 模块应直接暴露预期实现。
    """
    assert ObjectStateRepository.__module__ == "serverless_mcp.storage.state.object_state_repository"
    assert ManifestRepository.__module__ == "serverless_mcp.storage.manifest.repository"
    assert EmbeddingProjectionStateRepository.__module__ == "serverless_mcp.storage.projection.repository"
    assert build_manifest_key.__module__ == "serverless_mcp.storage.paths"
    assert optimize_source_file_name.__module__ == "serverless_mcp.storage.paths"
    assert flush_batch_write.__module__ == "serverless_mcp.storage.batch"


def test_application_modules_are_canonical() -> None:
    """
    EN: Canonical application modules should expose the actual implementations directly.
    CN: 正式 application 模块应直接暴露真实实现。
    """
    assert NewExtractionService.__module__ == "serverless_mcp.extract.application"
    assert NewEmbedWorker.__module__ == "serverless_mcp.embed.application"
    assert NewQueryService.__module__ == "serverless_mcp.query.application"
    assert NewJobStatusService.__module__ == "serverless_mcp.status.application"

    assert NewFastMCP.__module__ == "serverless_mcp.server.fastmcp"
    assert NewTransportSecuritySettings.__module__ == "serverless_mcp.server.transport_security"


def test_package_root_is_physical_and_non_namespace() -> None:
    """
    EN: The package root should be the physical serverless_mcp directory only.
    CN: 包根应只指向物理存在的 serverless_mcp 目录。
    """
    package_root = Path(serverless_mcp.__file__).resolve().parent
    assert list(serverless_mcp.__path__) == [str(package_root)]
