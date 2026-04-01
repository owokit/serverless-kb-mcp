"""
EN: Tests for ingest workflow starter, idempotency, and delete cleanup planning.
CN: 测试 ingest workflow starter、幂等逻辑和删除清理计划。
"""

from __future__ import annotations

import hashlib
import json

import pytest

from serverless_mcp.domain.models import ChunkManifest, EmbeddingProfile, ExtractedChunk, ObjectStateRecord, S3ObjectRef
from serverless_mcp.entrypoints.ingest import lambda_handler as ingest_lambda_handler
from serverless_mcp.runtime.ingest import DeleteMarkerGovernance, IngestWorkflowStarter, _build_execution_name


class _FakeObjectStateRepo:
    def __init__(self) -> None:
        self.document_uris: list[str] = []
        self.deleted: list[tuple[str, str, str | None, str | None]] = []
        self.lookup_by_bucket_key = {
            ("bucket-a", "docs/guide.pdf"): type("_Lookup", (), {"object_pk": "tenant-a#bucket-a#docs/guide.pdf"})()
        }
        self.state_by_object_pk: dict[str, ObjectStateRecord] = {}

    def mark_deleted(self, *, bucket, key, version_id, sequencer):
        self.deleted.append((bucket, key, version_id, sequencer))
        return ObjectStateRecord(
            pk=f"tenant-a#{bucket}#{key}",
            latest_version_id=version_id,
            latest_sequencer=sequencer,
            extract_status="EXTRACTED",
            embed_status="INDEXED",
            is_deleted=True,
        )

    def get_lookup_record(self, *, bucket, key):
        return self.lookup_by_bucket_key.get((bucket, key))

    def get_lookup_for_source(self, source):
        return self.get_lookup_record(bucket=source.bucket, key=source.key)

    def get_state(self, *, object_pk):
        return self.state_by_object_pk.get(object_pk)


class _FakeStepFunctions:
    def __init__(self, *, fail_first_execution: bool = False) -> None:
        self.executions: list[dict[str, object]] = []
        self.fail_first_execution = fail_first_execution

    def start_execution(self, **kwargs):
        self.executions.append(kwargs)
        if self.fail_first_execution and len(self.executions) == 1:
            raise RuntimeError("transient cleanup execution failure")
        return {"executionArn": f"arn:aws:states:ap-southeast-1:123:execution:extract:{len(self.executions)}"}


class _FakeDeleteLifecycleManager:
    def __init__(self, cleanup_plan: dict | None = None) -> None:
        self.calls: list[str] = []
        self.cleanup_plan = cleanup_plan

    def handle_delete(self, *, source):
        self.calls.append(source.document_uri)
        return self.cleanup_plan


class _FakeManifestRepo:
    def load_manifest(self, manifest_s3_uri):
        assert manifest_s3_uri == "s3://manifest-bucket/manifests/example.json"
        return ChunkManifest(
            source=S3ObjectRef(
                tenant_id="tenant-a",
                bucket="bucket-a",
                key="docs/guide.pdf",
                version_id="v1",
            ),
            doc_type="pdf",
            chunks=[
                ExtractedChunk(
                    chunk_id="chunk#000001",
                    chunk_type="page_text_chunk",
                    text="hello",
                    doc_type="pdf",
                    token_estimate=2,
                )
            ],
            metadata={"source_format": "pdf", "page_count": 0, "visual_page_numbers": [], "page_image_asset_count": 0},
        )


def test_ingest_starts_step_functions_execution_from_sqs_event() -> None:
    """
    EN: Verify the ingest starter parses an SQS-wrapped S3 event and starts Step Functions.
    CN: 验证 ingest starter 能解析 SQS 包装的 S3 事件并启动 Step Functions。
    """
    state_repo = _FakeObjectStateRepo()
    stepfunctions = _FakeStepFunctions()
    starter = IngestWorkflowStarter(
        object_state_repo=state_repo,
        stepfunctions_client=stepfunctions,
        state_machine_arn="arn:aws:states:ap-southeast-1:123:stateMachine:extract",
    )

    result = starter.handle_batch(
        {
            "Records": [
                {
                    "eventSource": "aws:sqs",
                    "body": json.dumps(
                        {
                            "Records": [
                                {
                                    "eventVersion": "2.1",
                                    "eventSource": "aws:s3",
                                    "eventName": "ObjectCreated:Put",
                                    "s3": {
                                        "bucket": {"name": "bucket-a"},
                                        "object": {
                                            "key": "docs%2Fguide.pdf",
                                            "versionId": "v1",
                                            "sequencer": "001",
                                        },
                                    },
                                }
                            ]
                        }
                    ),
                }
            ]
        }
    )

    assert result["started_count"] == 1
    payload = json.loads(stepfunctions.executions[0]["input"])
    assert payload["processing_state"]["extract_status"] == "QUEUED"
    assert payload["processing_state"]["latest_version_id"] == "v1"
    assert payload["processing_state"]["previous_version_id"] is None
    assert payload["job"]["source"]["version_id"] == "v1"
    assert payload["job"]["operation"] == "UPSERT"


def test_execution_name_includes_tenant_and_bucket_identity() -> None:
    """
    EN: Verify that the execution name includes hashes of tenant and bucket identity.
    CN: 验证执行名称里包含 tenant 和 bucket 的哈希标识。
    """
    shared_key = "docs/guide.pdf"
    shared_version = "v1"
    shared_sequencer = "001"
    source_a = S3ObjectRef(
        tenant_id="tenant-a",
        bucket="bucket-a",
        key=shared_key,
        version_id=shared_version,
        sequencer=shared_sequencer,
    )
    source_b = S3ObjectRef(
        tenant_id="tenant-b",
        bucket="bucket-b",
        key=shared_key,
        version_id=shared_version,
        sequencer=shared_sequencer,
    )

    execution_name_a = _build_execution_name(source_a)
    execution_name_b = _build_execution_name(source_b)

    assert execution_name_a != execution_name_b
    assert hashlib.sha1(source_a.tenant_id.encode("utf-8")).hexdigest()[:8] in execution_name_a
    assert hashlib.sha1(source_a.bucket.encode("utf-8")).hexdigest()[:8] in execution_name_a
    assert len(execution_name_a) <= 80


def test_ingest_skips_duplicate_or_stale_events() -> None:
    """
    EN: Verify that duplicate or stale S3 events are skipped.
    CN: 验证重复或过期的 S3 事件会被跳过。
    """

    class _DuplicateStateRepo(_FakeObjectStateRepo):
        def get_state(self, *, object_pk):
            return ObjectStateRecord(
                pk=object_pk,
                latest_version_id="v1",
                latest_sequencer="00000000000000000000000000000001",
                extract_status="EXTRACTING",
                embed_status="PENDING",
            )

    starter = IngestWorkflowStarter(
        object_state_repo=_DuplicateStateRepo(),
        stepfunctions_client=_FakeStepFunctions(),
        state_machine_arn="arn:aws:states:ap-southeast-1:123:stateMachine:extract",
    )

    result = starter.handle_batch(
        {
            "Records": [
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "eventName": "ObjectCreated:Put",
                    "s3": {
                        "bucket": {"name": "bucket-a"},
                        "object": {
                            "key": "docs%2Fguide.pdf",
                            "versionId": "v1",
                        },
                    },
                }
            ]
        }
    )

    assert result["started_count"] == 0
    assert result["skipped_count"] == 1


def test_ingest_handles_delete_marker_and_starts_cleanup_execution() -> None:
    """
    EN: Verify that delete marker events start cleanup executions and return a cleanup plan.
    CN: 验证删除标记事件会启动 cleanup execution 并返回清理计划。
    """
    state_repo = _FakeObjectStateRepo()
    cleanup_plan = {
        "document_uri": "s3://bucket-a/docs/guide.pdf?versionId=delete-v1",
        "object_pk": "tenant-a#bucket-a#docs/guide.pdf",
        "latest_manifest_s3_uri": "s3://manifest-bucket/manifests/example.json",
        "cleanup_targets": [
            {
                "profile_id": "openai-text-small",
                "vector_bucket_name": "vector-bucket",
                "vector_index_name": "vector-index",
                "keys": ["openai-text-small#tenant-a#bucket-a#docs/guide.pdf#v1#chunk#000001"],
            }
        ],
    }
    delete_manager = _FakeDeleteLifecycleManager(cleanup_plan=cleanup_plan)
    stepfunctions = _FakeStepFunctions()
    starter = IngestWorkflowStarter(
        object_state_repo=state_repo,
        stepfunctions_client=stepfunctions,
        state_machine_arn="arn:aws:states:ap-southeast-1:123:stateMachine:extract",
        delete_lifecycle_manager=delete_manager,
    )

    result = starter.handle_batch(
        {
            "Records": [
                {
                    "eventVersion": "2.3",
                    "eventSource": "aws:s3",
                    "eventName": "ObjectRemoved:DeleteMarkerCreated",
                    "s3": {
                        "bucket": {"name": "bucket-a"},
                        "object": {
                            "key": "docs%2Fguide.pdf",
                            "versionId": "delete-v1",
                            "sequencer": "002",
                        },
                    },
                }
            ]
        }
    )

    assert result["deleted_count"] == 1
    assert result["started_count"] == 0
    assert result["deleted"][0]["cleanup_plan"] == cleanup_plan
    assert result["deleted"][0]["cleanup_executions"] == [
        {
            "profile_id": "openai-text-small",
            "execution_arn": "arn:aws:states:ap-southeast-1:123:execution:extract:1",
        }
    ]
    assert state_repo.deleted == [("bucket-a", "docs/guide.pdf", "delete-v1", "002")]
    assert delete_manager.calls == ["s3://bucket-a/docs/guide.pdf?versionId=delete-v1"]
    assert len(stepfunctions.executions) == 1
    cleanup_input = json.loads(stepfunctions.executions[0]["input"])
    assert cleanup_input["cleanup_plan"] == cleanup_plan
    assert cleanup_input["cleanup_target"] == cleanup_plan["cleanup_targets"][0]


def test_ingest_retries_delete_cleanup_execution_after_transient_failure() -> None:
    """
    EN: Verify that a transient cleanup execution failure is retried on the next invocation.
    CN: 验证临时性的 cleanup execution 失败会在下一次调用中重试。
    """

    class _RetryableDeleteStateRepo(_FakeObjectStateRepo):
        def __init__(self) -> None:
            super().__init__()
            self._deleted_state = None

        def mark_deleted(self, *, bucket, key, version_id, sequencer):
            self.deleted.append((bucket, key, version_id, sequencer))
            if self._deleted_state is None:
                self._deleted_state = ObjectStateRecord(
                    pk=f"tenant-a#{bucket}#{key}",
                    latest_version_id=version_id,
                    latest_sequencer=sequencer,
                    extract_status="EXTRACTED",
                    embed_status="INDEXED",
                    latest_manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
                    is_deleted=True,
                )
            return self._deleted_state

    cleanup_plan = {
        "document_uri": "s3://bucket-a/docs/guide.pdf?versionId=delete-v1",
        "object_pk": "tenant-a#bucket-a#docs/guide.pdf",
        "latest_manifest_s3_uri": "s3://manifest-bucket/manifests/example.json",
        "cleanup_targets": [
            {
                "profile_id": "openai-text-small",
                "vector_bucket_name": "vector-bucket",
                "vector_index_name": "vector-index",
                "keys": ["openai-text-small#tenant-a#bucket-a#docs/guide.pdf#v1#chunk#000001"],
            }
        ],
    }

    state_repo = _RetryableDeleteStateRepo()
    delete_manager = _FakeDeleteLifecycleManager(cleanup_plan=cleanup_plan)
    starter = IngestWorkflowStarter(
        object_state_repo=state_repo,
        stepfunctions_client=_FakeStepFunctions(fail_first_execution=True),
        state_machine_arn="arn:aws:states:ap-southeast-1:123:stateMachine:extract",
        delete_lifecycle_manager=delete_manager,
    )
    event = {
        "Records": [
            {
                "eventVersion": "2.3",
                "eventSource": "aws:s3",
                "eventName": "ObjectRemoved:DeleteMarkerCreated",
                "s3": {
                    "bucket": {"name": "bucket-a"},
                    "object": {
                        "key": "docs%2Fguide.pdf",
                        "versionId": "delete-v1",
                        "sequencer": "002",
                    },
                },
            }
        ]
    }

    with pytest.raises(RuntimeError, match="transient cleanup execution failure"):
        starter.handle_batch(event)

    result = starter.handle_batch(event)

    assert result["deleted_count"] == 1
    assert result["skipped_count"] == 0
    assert delete_manager.calls == [
        "s3://bucket-a/docs/guide.pdf?versionId=delete-v1",
        "s3://bucket-a/docs/guide.pdf?versionId=delete-v1",
    ]
    assert state_repo.deleted == [
        ("bucket-a", "docs/guide.pdf", "delete-v1", "002"),
        ("bucket-a", "docs/guide.pdf", "delete-v1", "002"),
    ]


def test_delete_marker_governance_builds_cleanup_targets_for_each_profile() -> None:
    """
    EN: Verify that DeleteMarkerGovernance builds cleanup targets for each embedding profile.
    CN: 验证 DeleteMarkerGovernance 会为每个 embedding profile 构建 cleanup target。
    """
    state_repo = _FakeObjectStateRepo()
    state_repo.state_by_object_pk["tenant-a#bucket-a#docs/guide.pdf"] = ObjectStateRecord(
        pk="tenant-a#bucket-a#docs/guide.pdf",
        latest_version_id="delete-v1",
        latest_sequencer="002",
        extract_status="EXTRACTED",
        embed_status="INDEXED",
        latest_manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
        is_deleted=True,
    )
    governance = DeleteMarkerGovernance(
        object_state_repo=state_repo,
        manifest_repo=_FakeManifestRepo(),
        profiles=(
            EmbeddingProfile(
                profile_id="gemini-default",
                provider="gemini",
                model="gemini-embedding-2-preview",
                dimension=3072,
                vector_bucket_name="vector-bucket",
                vector_index_name="index-gemini",
                supported_content_kinds=("text", "image"),
            ),
            EmbeddingProfile(
                profile_id="openai-text-small",
                provider="openai",
                model="text-embedding-3-small",
                dimension=1536,
                vector_bucket_name="vector-bucket",
                vector_index_name="index-openai",
                supported_content_kinds=("text",),
            ),
        ),
    )

    cleanup_plan = governance.handle_delete(
        source=S3ObjectRef(
            tenant_id="lookup",
            bucket="bucket-a",
            key="docs/guide.pdf",
            version_id="delete-v1",
        )
    )

    assert cleanup_plan == {
        "document_uri": "s3://bucket-a/docs/guide.pdf?versionId=delete-v1",
        "object_pk": "tenant-a#bucket-a#docs/guide.pdf",
        "latest_manifest_s3_uri": "s3://manifest-bucket/manifests/example.json",
        "cleanup_targets": [
            {
                "profile_id": "gemini-default",
                "vector_bucket_name": "vector-bucket",
                "vector_index_name": "index-gemini",
                "keys": ["gemini-default#tenant-a#bucket-a#docs%2Fguide.pdf#v1#chunk#000001"],
            },
            {
                "profile_id": "openai-text-small",
                "vector_bucket_name": "vector-bucket",
                "vector_index_name": "index-openai",
                "keys": ["openai-text-small#tenant-a#bucket-a#docs%2Fguide.pdf#v1#chunk#000001"],
            },
        ],
    }


def test_execution_name_keeps_version_and_sequencer_uniqueness_for_long_keys() -> None:
    """
    EN: Verify that long S3 keys produce execution names within the 80-char limit.
    CN: 验证长 S3 key 生成的执行名称仍然保持在 80 字符限制内。
    """
    source_v1 = S3ObjectRef(
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="docs/" + ("very-long-segment-" * 20) + "guide.pdf",
        version_id="version-001",
        sequencer="0001ABC",
    )
    source_v2 = S3ObjectRef(
        tenant_id="tenant-a",
        bucket="bucket-a",
        key=source_v1.key,
        version_id="version-002",
        sequencer="0001ABD",
    )

    name_v1 = _build_execution_name(source_v1)
    name_v2 = _build_execution_name(source_v2)

    assert len(name_v1) <= 80
    assert len(name_v2) <= 80
    assert name_v1 != name_v2


def test_ingest_handler_exposes_structured_failed_records_for_sqs_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify that the ingest handler returns structured failure diagnostics for SQS retries.
    CN: 验证 ingest handler 会为 SQS 重试返回结构化失败诊断。
    """

    class _FailingStarter:
        def handle_batch(self, event):
            if event["Records"][0]["messageId"] == "msg-2":
                raise ValueError("bad record payload")
            return {"started_count": 1, "skipped_count": 0, "failed_count": 0, "failed": []}

    monkeypatch.setattr("serverless_mcp.entrypoints.ingest.build_ingest_workflow_starter", lambda _context: _FailingStarter())

    result = ingest_lambda_handler(
        {
            "Records": [
                {"eventSource": "aws:sqs", "messageId": "msg-1", "body": "{}"},
                {"eventSource": "aws:sqs", "messageId": "msg-2", "body": "{}"},
            ]
        },
        None,
    )

    assert result["failed_count"] == 1
    assert result["batchItemFailures"] == [{"itemIdentifier": "msg-2"}]
    assert result["failed_records"][0]["error_type"] == "ValueError"
    assert result["failed_records"][0]["reason"] == "validation_error"
