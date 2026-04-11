"""
EN: Runtime composition-root tests that verify repository wiring and optional dependency creation.
CN: 验证 repository 装配和可选依赖创建行为的运行时组合根测试。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from serverless_mcp.runtime import bootstrap
from serverless_mcp.runtime.aws_clients import AwsClientBundle
from serverless_mcp.runtime.config import Settings


@dataclass
class _FakeRepo:
    kwargs: dict[str, Any]

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs


@dataclass
class _FakeSettings:
    object_state_table: str = "object-state"
    execution_state_table: str | None = "execution-state"
    manifest_index_table: str | None = "manifest-index"
    manifest_bucket: str | None = "manifest-bucket"
    manifest_prefix: str = "manifests"
    embedding_projection_state_table: str | None = "projection-state"


@dataclass
class _FakeClients:
    s3: object = object()
    dynamodb: object = object()
    sqs: object = object()
    stepfunctions: object = object()
    s3vectors: object = object()


def _make_bundle() -> AwsClientBundle:
    clients = _FakeClients()
    return AwsClientBundle(
        s3=clients.s3,
        dynamodb=clients.dynamodb,
        sqs=clients.sqs,
        stepfunctions=clients.stepfunctions,
        s3vectors=clients.s3vectors,
    )


def test_build_runtime_context_prefers_injected_inputs() -> None:
    settings = Settings.from_env()
    clients = _make_bundle()

    context = bootstrap.build_runtime_context(settings=settings, clients=clients)

    assert context.settings is settings
    assert context.clients is clients


def test_build_runtime_repositories_creates_optional_repositories(monkeypatch) -> None:
    created: list[tuple[str, dict[str, Any]]] = []

    class FakeObjectStateRepository(_FakeRepo):
        def __init__(self, **kwargs: Any) -> None:
            created.append(("object", kwargs))
            super().__init__(**kwargs)

    class FakeExecutionStateRepository(_FakeRepo):
        def __init__(self, **kwargs: Any) -> None:
            created.append(("execution", kwargs))
            super().__init__(**kwargs)

    class FakeManifestRepository(_FakeRepo):
        def __init__(self, **kwargs: Any) -> None:
            created.append(("manifest", kwargs))
            super().__init__(**kwargs)

    class FakeProjectionRepository(_FakeRepo):
        def __init__(self, **kwargs: Any) -> None:
            created.append(("projection", kwargs))
            super().__init__(**kwargs)

    monkeypatch.setattr(bootstrap, "ObjectStateRepository", FakeObjectStateRepository)
    monkeypatch.setattr(bootstrap, "ExecutionStateRepository", FakeExecutionStateRepository)
    monkeypatch.setattr(bootstrap, "ManifestRepository", FakeManifestRepository)
    monkeypatch.setattr(bootstrap, "EmbeddingProjectionStateRepository", FakeProjectionRepository)

    repositories = bootstrap.build_runtime_repositories(settings=_FakeSettings(), clients=_make_bundle())

    assert repositories.object_state_repo.kwargs["table_name"] == "object-state"
    assert repositories.execution_state_repo.kwargs["table_name"] == "execution-state"
    assert repositories.manifest_repo.kwargs["manifest_bucket"] == "manifest-bucket"
    assert repositories.projection_state_repo.kwargs["table_name"] == "projection-state"
    assert [kind for kind, _ in created] == ["object", "execution", "manifest", "projection"]


def test_optional_repositories_are_not_created_when_inputs_are_missing(monkeypatch) -> None:
    created: list[str] = []

    class FakeObjectStateRepository(_FakeRepo):
        def __init__(self, **kwargs: Any) -> None:
            created.append("object")
            super().__init__(**kwargs)

    monkeypatch.setattr(bootstrap, "ObjectStateRepository", FakeObjectStateRepository)
    monkeypatch.setattr(bootstrap, "ExecutionStateRepository", lambda **kwargs: (_ for _ in ()).throw(AssertionError("unexpected execution repo")))
    monkeypatch.setattr(bootstrap, "ManifestRepository", lambda **kwargs: (_ for _ in ()).throw(AssertionError("unexpected manifest repo")))
    monkeypatch.setattr(bootstrap, "EmbeddingProjectionStateRepository", lambda **kwargs: (_ for _ in ()).throw(AssertionError("unexpected projection repo")))

    settings = _FakeSettings(execution_state_table=None, manifest_index_table=None, manifest_bucket=None, embedding_projection_state_table=None)
    repositories = bootstrap.build_runtime_repositories(settings=settings, clients=_make_bundle())

    assert repositories.object_state_repo.kwargs["table_name"] == "object-state"
    assert repositories.execution_state_repo is None
    assert repositories.manifest_repo is None
    assert repositories.projection_state_repo is None
    assert created == ["object"]
