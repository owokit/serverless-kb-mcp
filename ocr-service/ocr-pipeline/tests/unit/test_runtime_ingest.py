"""
EN: Ingest workflow starter tests that pin idempotency, delete cleanup, and execution contracts.
CN: 钉住幂等、删除清理与执行契约的 ingest workflow starter 测试。
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from unittest.mock import ANY

import pytest
from botocore.exceptions import ClientError

from fixtures.events import S3_CREATE_EVENT, S3_DELETE_EVENT, deep_copy_event
from serverless_mcp.domain.models import ChunkManifest, EmbeddingProfile, ExtractedAsset, ExtractedChunk, ObjectStateRecord, S3ObjectRef
from serverless_mcp.runtime import ingest as runtime_ingest
from serverless_mcp.runtime.ingest import DeleteMarkerGovernance, IngestWorkflowStarter, _build_cleanup_execution_name, _build_execution_name, _normalize_sequencer_value


@dataclass
class _Lookup:
    object_pk: str


class _ObjectStateRepo:
    def __init__(self) -> None:
        self.lookup = _Lookup(object_pk="lookup#source-bucket#docs/guide.pdf")
        self.state_by_object_pk: dict[str, ObjectStateRecord] = {}
        self.deleted: list[tuple[str, str, str | None, str | None]] = []

    def get_lookup_for_source(self, source: S3ObjectRef):
        return self.lookup

    def get_state(self, *, object_pk: str):
        return self.state_by_object_pk.get(object_pk)

    def mark_deleted(self, *, bucket: str, key: str, version_id: str, sequencer: str | None):
        self.deleted.append((bucket, key, version_id, sequencer))
        return ObjectStateRecord(
            pk=self.lookup.object_pk,
            latest_version_id=version_id,
            latest_sequencer=sequencer,
            extract_status="EXTRACTED",
            embed_status="INDEXED",
            latest_manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
            is_deleted=True,
        )


class _StepFunctions:
    def __init__(self, *, already_exists: bool = False, error_code: str | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.already_exists = already_exists
        self.error_code = error_code

    def start_execution(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if self.already_exists:
            raise ClientError({"Error": {"Code": "ExecutionAlreadyExists", "Message": "duplicate"}}, "StartExecution")
        if self.error_code:
            raise ClientError({"Error": {"Code": self.error_code, "Message": "boom"}}, "StartExecution")
        return {"executionArn": f"arn:aws:states:region:acct:execution:machine:{len(self.calls)}"}


class _ManifestRepo:
    def load_manifest(self, manifest_s3_uri: str):
        assert manifest_s3_uri == "s3://manifest-bucket/manifests/example.json"
        return ChunkManifest(
            source=S3ObjectRef(tenant_id="tenant-a", bucket="source-bucket", key="docs/guide.pdf", version_id="v1"),
            doc_type="pdf",
            chunks=[ExtractedChunk(chunk_id="chunk-1", chunk_type="page_text_chunk", text="hello", doc_type="pdf", token_estimate=2)],
            assets=[ExtractedAsset(asset_id="asset-1", chunk_type="page_image_chunk", mime_type="image/png")],
            metadata={},
        )


class _DeleteLifecycleManager:
    def __init__(self, plan: dict[str, Any] | None) -> None:
        self.plan = plan
        self.calls: list[str] = []

    def handle_delete(self, *, source: S3ObjectRef):
        self.calls.append(source.document_uri)
        return self.plan


def test_build_execution_name_and_sequencer_normalization() -> None:
    source = S3ObjectRef(tenant_id="tenant-a", bucket="source-bucket", key="docs/guide.pdf", version_id="v1", sequencer=" 1a ")
    another = S3ObjectRef(tenant_id="tenant-b", bucket="source-bucket", key="docs/guide.pdf", version_id="v1", sequencer="1B")

    assert _normalize_sequencer_value(None) is None
    assert _normalize_sequencer_value(" 1a ") == "0000000000000000000000000000001A"
    assert _build_execution_name(source) != _build_execution_name(another)
    assert len(_build_execution_name(source)) <= 80


def test_handle_batch_starts_create_path_and_returns_structured_result() -> None:
    repo = _ObjectStateRepo()
    stepfunctions = _StepFunctions()
    starter = IngestWorkflowStarter(object_state_repo=repo, stepfunctions_client=stepfunctions, state_machine_arn="arn:aws:states:region:acct:stateMachine:extract")

    result = starter.handle_batch(deep_copy_event(S3_CREATE_EVENT))

    assert result["started_count"] == 1
    assert result["deleted_count"] == 0
    assert result["skipped_count"] == 0
    assert result["failed_count"] == 0
    assert result["started"][0]["document_uri"].startswith("s3://source-bucket/")
    assert result["started"][0]["execution_arn"] == "arn:aws:states:region:acct:execution:machine:1"
    payload = json.loads(stepfunctions.calls[0]["input"])
    assert payload["processing_state"]["extract_status"] == "QUEUED"
    assert payload["processing_state"]["previous_version_id"] is None
    assert payload["job"]["operation"] == "UPSERT"


def test_handle_batch_skips_duplicate_or_stale_events_and_execution_already_exists() -> None:
    stale_state = ObjectStateRecord(
        pk="lookup#source-bucket#docs/guide.pdf",
        latest_version_id="v1",
        latest_sequencer="00000000000000000000000000000002",
        extract_status="EXTRACTED",
        embed_status="INDEXED",
    )
    repo = _ObjectStateRepo()
    repo.get_state = lambda *, object_pk: stale_state  # type: ignore[assignment]
    starter = IngestWorkflowStarter(object_state_repo=repo, stepfunctions_client=_StepFunctions(), state_machine_arn="arn:aws:states:region:acct:stateMachine:extract")

    result = starter.handle_batch(deep_copy_event(S3_CREATE_EVENT))

    assert result["skipped_count"] == 1
    assert result["started_count"] == 0
    assert result["skipped"][0]["reason"] == "duplicate_or_stale_event"

    fresh_repo = _ObjectStateRepo()
    stepfunctions = _StepFunctions(already_exists=True)
    fresh_starter = IngestWorkflowStarter(object_state_repo=fresh_repo, stepfunctions_client=stepfunctions, state_machine_arn="arn:aws:states:region:acct:stateMachine:extract")

    result = fresh_starter.handle_batch(deep_copy_event(S3_CREATE_EVENT))

    assert result["skipped_count"] == 1
    assert result["skipped"][0]["reason"] == "execution_already_exists"


def test_build_ingest_workflow_starter_wires_runtime_context_and_optional_cleanup(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = type(
        "Settings",
        (),
        {
            "step_functions_state_machine_arn": "arn:aws:states:region:acct:stateMachine:extract",
            "execution_state_table": "execution-state",
            "manifest_bucket": "manifest-bucket",
            "manifest_index_table": "manifest-index",
            "manifest_prefix": "manifests",
        },
    )()
    clients = type("Clients", (), {"stepfunctions": object(), "dynamodb": object(), "s3": object()})()
    runtime_context = type("RuntimeContext", (), {"settings": settings, "clients": clients})()
    repositories = type(
        "Repositories",
        (),
        {
            "object_state_repo": _ObjectStateRepo(),
            "execution_state_repo": object(),
            "manifest_repo": object(),
            "projection_state_repo": None,
        },
    )()
    captured: dict[str, object] = {}

    monkeypatch.setattr(runtime_ingest, "build_runtime_context", lambda settings=None: runtime_context)
    monkeypatch.setattr(runtime_ingest, "build_runtime_repositories", lambda settings, clients: repositories)
    monkeypatch.setattr(runtime_ingest, "resolve_step_functions_state_machine_arn", lambda state_machine_ref: state_machine_ref)
    monkeypatch.setattr(runtime_ingest, "get_write_profiles", lambda settings: (EmbeddingProfile(
        profile_id="openai-text-small",
        provider="openai",
        model="text-embedding-3-small",
        dimension=1536,
        vector_bucket_name="vector-bucket",
        vector_index_name="vector-index",
        supported_content_kinds=("text",),
    ),))
    monkeypatch.setattr(runtime_ingest, "DeleteMarkerGovernance", lambda **kwargs: captured.setdefault("delete_manager", kwargs) or kwargs)

    starter = runtime_ingest.build_ingest_workflow_starter()

    assert starter._state_machine_arn == "arn:aws:states:region:acct:stateMachine:extract"
    assert starter._object_state_repo is repositories.object_state_repo
    assert starter._stepfunctions is clients.stepfunctions
    assert "delete_manager" in captured
    assert captured["delete_manager"]["execution_state_repo"] is repositories.execution_state_repo
    assert captured["delete_manager"]["manifest_repo"] is repositories.manifest_repo


def test_handle_batch_records_unexpected_stepfunctions_failures_before_raising() -> None:
    repo = _ObjectStateRepo()
    stepfunctions = _StepFunctions(error_code="ThrottlingException")
    starter = IngestWorkflowStarter(object_state_repo=repo, stepfunctions_client=stepfunctions, state_machine_arn="arn:aws:states:region:acct:stateMachine:extract")
    failure_records: list[dict[str, object]] = []

    original_build_failure_record = runtime_ingest._build_failure_record

    def fake_build_failure_record(document_uri: str, stage: str, exc: Exception) -> dict[str, object]:
        failure = original_build_failure_record(document_uri, stage, exc)
        failure_records.append(failure)
        return failure

    runtime_ingest._build_failure_record = fake_build_failure_record  # type: ignore[assignment]

    try:
        with pytest.raises(ClientError):
            starter.handle_batch(deep_copy_event(S3_CREATE_EVENT))
    finally:
        runtime_ingest._build_failure_record = original_build_failure_record  # type: ignore[assignment]

    assert failure_records == [
        {
            "document_uri": "s3://source-bucket/docs/guide.pdf?versionId=v1",
            "stage": "stepfunctions_start",
            "error_type": "ClientError",
            "reason": "unexpected_error",
            "error": ANY,
        }
    ]
    assert "ThrottlingException" in str(failure_records[0]["error"])


def test_handle_batch_delete_path_returns_cleanup_plan_and_cleanup_executions() -> None:
    repo = _ObjectStateRepo()
    cleanup_plan = {
        "document_uri": "s3://source-bucket/docs/guide.pdf?versionId=delete-v1",
        "object_pk": repo.lookup.object_pk,
        "latest_manifest_s3_uri": "s3://manifest-bucket/manifests/example.json",
        "cleanup_targets": [
            {
                "profile_id": "openai-text-small",
                "vector_bucket_name": "vector-bucket",
                "vector_index_name": "vector-index",
                "keys": ["openai-text-small#lookup#source-bucket#docs/guide.pdf#v1#chunk-1"],
            }
        ],
    }
    delete_manager = _DeleteLifecycleManager(cleanup_plan)
    stepfunctions = _StepFunctions()
    starter = IngestWorkflowStarter(
        object_state_repo=repo,
        stepfunctions_client=stepfunctions,
        state_machine_arn="arn:aws:states:region:acct:stateMachine:extract",
        delete_lifecycle_manager=delete_manager,
    )

    result = starter.handle_batch(deep_copy_event(S3_DELETE_EVENT))

    assert result["deleted_count"] == 1
    assert result["deleted"][0]["cleanup_plan"] == cleanup_plan
    assert result["deleted"][0]["cleanup_executions"][0]["profile_id"] == "openai-text-small"
    assert delete_manager.calls == ["s3://source-bucket/docs/guide.pdf?versionId=delete-v1"]
    assert len(stepfunctions.calls) == 1
    expected_name = _build_cleanup_execution_name(
        S3ObjectRef(tenant_id="lookup", bucket="source-bucket", key="docs/guide.pdf", version_id="delete-v1", sequencer="002"),
        cleanup_plan["cleanup_targets"][0],
    )
    assert stepfunctions.calls[0]["name"] == expected_name
    assert json.loads(stepfunctions.calls[0]["input"]) == {
        "cleanup_plan": cleanup_plan,
        "cleanup_target": cleanup_plan["cleanup_targets"][0],
    }


def test_delete_marker_governance_builds_cleanup_targets_for_each_write_enabled_profile() -> None:
    repo = _ObjectStateRepo()
    repo.state_by_object_pk[repo.lookup.object_pk] = ObjectStateRecord(
        pk=repo.lookup.object_pk,
        latest_version_id="v1",
        latest_sequencer="00000000000000000000000000000002",
        extract_status="EXTRACTED",
        embed_status="INDEXED",
        latest_manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
    )
    governance = DeleteMarkerGovernance(
        object_state_repo=repo,
        manifest_repo=_ManifestRepo(),
        profiles=(
            EmbeddingProfile(
                profile_id="openai-text-small",
                provider="openai",
                model="text-embedding-3-small",
                dimension=1536,
                vector_bucket_name="vector-bucket",
                vector_index_name="vector-index",
                supported_content_kinds=("text",),
                enabled=True,
                enable_write=True,
                enable_query=True,
            ),
            EmbeddingProfile(
                profile_id="query-only",
                provider="openai",
                model="text-embedding-3-small",
                dimension=1536,
                vector_bucket_name="vector-bucket",
                vector_index_name="vector-index-2",
                supported_content_kinds=("text",),
                enabled=True,
                enable_write=False,
                enable_query=True,
            ),
        ),
    )

    plan = governance.handle_delete(source=S3ObjectRef(tenant_id="lookup", bucket="source-bucket", key="docs/guide.pdf", version_id="delete-v1"))

    manifest = _ManifestRepo().load_manifest("s3://manifest-bucket/manifests/example.json")
    assert plan is not None
    assert plan["cleanup_targets"] == [
        {
            "profile_id": "openai-text-small",
            "vector_bucket_name": "vector-bucket",
            "vector_index_name": "vector-index",
            "keys": [f"openai-text-small#{manifest.source.version_pk}#chunk-1", f"openai-text-small#{manifest.source.version_pk}#asset-1"],
        }
    ]
