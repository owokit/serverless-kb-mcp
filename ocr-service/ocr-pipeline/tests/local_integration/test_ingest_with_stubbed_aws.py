"""
EN: Local integration tests for ingest that exercise the real Lambda entrypoint and starter wiring with stubbed AWS boundaries.
CN: 使用 stub AWS 边界，覆盖真实 Lambda 入口和 starter 接线的本地集成测试。
"""
from __future__ import annotations

from typing import Any

from fixtures.events import S3_CREATE_EVENT, S3_DELETE_EVENT, deep_copy_event
from fixtures.lambda_context import make_lambda_context
from serverless_mcp.domain.models import ObjectStateRecord, S3ObjectRef
from serverless_mcp.entrypoints import ingest as ingest_entrypoint
from serverless_mcp.runtime.ingest import IngestWorkflowStarter


class _ObjectStateRepo:
    def __init__(self) -> None:
        self.lookup = type("Lookup", (), {"object_pk": "tenant-a#source-bucket#docs/guide.pdf"})()
        self.state = ObjectStateRecord(
            pk=self.lookup.object_pk,
            latest_version_id="v0",
            latest_sequencer="00000000000000000000000000000001",
            extract_status="EXTRACTED",
            embed_status="INDEXED",
            latest_manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
        )
        self.deleted: list[tuple[str, str, str | None, str | None]] = []

    def get_lookup_for_source(self, source: S3ObjectRef):
        return self.lookup

    def get_state(self, *, object_pk: str):
        return self.state if object_pk == self.lookup.object_pk else None

    def mark_deleted(self, *, bucket: str, key: str, version_id: str, sequencer: str | None):
        self.deleted.append((bucket, key, version_id, sequencer))
        return ObjectStateRecord(
            pk=self.lookup.object_pk,
            latest_version_id=version_id,
            latest_sequencer=sequencer,
            extract_status="EXTRACTED",
            embed_status="INDEXED",
            latest_manifest_s3_uri=self.state.latest_manifest_s3_uri,
            is_deleted=True,
        )


class _StepFunctions:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def start_execution(self, **kwargs: Any):
        self.calls.append(kwargs)
        return {"executionArn": f"arn:aws:states:region:acct:execution:machine:{len(self.calls)}"}


class _DeleteLifecycleManager:
    def __init__(self, plan: dict[str, Any]) -> None:
        self.plan = plan

    def handle_delete(self, *, source: S3ObjectRef):
        return self.plan


class _FakeFactory:
    def __init__(self, starter: IngestWorkflowStarter) -> None:
        self.starter = starter
        self.calls: list[Any] = []

    def __call__(self, context=None):
        self.calls.append(context)
        return self.starter


def test_ingest_local_integration_starts_create_execution_and_cleanup_execution(monkeypatch) -> None:
    repo = _ObjectStateRepo()
    stepfunctions = _StepFunctions()
    delete_plan = {
        "document_uri": "s3://source-bucket/docs/guide.pdf?versionId=delete-v1",
        "object_pk": repo.lookup.object_pk,
        "latest_manifest_s3_uri": repo.state.latest_manifest_s3_uri,
        "cleanup_targets": [
            {
                "profile_id": "openai-text-small",
                "vector_bucket_name": "vector-bucket",
                "vector_index_name": "vector-index",
                "keys": ["openai-text-small#tenant-a#source-bucket#docs/guide.pdf#v0#chunk-1"],
            }
        ],
    }
    starter = IngestWorkflowStarter(
        object_state_repo=repo,
        stepfunctions_client=stepfunctions,
        state_machine_arn="arn:aws:states:region:acct:stateMachine:extract",
        delete_lifecycle_manager=_DeleteLifecycleManager(delete_plan),
    )
    factory = _FakeFactory(starter)
    monkeypatch.setattr(ingest_entrypoint, "build_ingest_workflow_starter", factory)
    monkeypatch.setattr(ingest_entrypoint, "emit_trace", lambda *args, **kwargs: None)

    create_result = ingest_entrypoint.lambda_handler(deep_copy_event(S3_CREATE_EVENT), make_lambda_context())
    delete_result = ingest_entrypoint.lambda_handler(deep_copy_event(S3_DELETE_EVENT), make_lambda_context())

    assert factory.calls and factory.calls[0] is not None
    assert create_result["started_count"] == 1
    assert delete_result["deleted_count"] == 1
    assert stepfunctions.calls[0]["stateMachineArn"].endswith(":stateMachine:extract")
    assert stepfunctions.calls[1]["name"].startswith("ingest-")
    assert delete_result["deleted"][0]["cleanup_plan"] == delete_plan
