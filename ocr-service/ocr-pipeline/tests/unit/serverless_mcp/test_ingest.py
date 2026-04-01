"""
EN: Tests for the ingest workflow starter and delete cleanup planning.
CN: 测试 ingest workflow starter 和删除清理计划。
"""

from __future__ import annotations

import hashlib
import json

import pytest

from serverless_mcp.domain.models import ChunkManifest, EmbeddingProfile, ExtractedChunk, ObjectStateRecord, S3ObjectRef
from serverless_mcp.entrypoints.ingest import lambda_handler as ingest_lambda_handler
from serverless_mcp.runtime.ingest import DeleteMarkerGovernance, IngestWorkflowStarter, _build_execution_name


class _FakeObjectStateRepo:
    def __init__(self):
        self.document_uris = []
        self.deleted = []
        self.lookup_by_bucket_key = {
            ("bucket-a", "docs/guide.pdf"): type(
                "_Lookup",
                (),
                {"object_pk": "tenant-a#bucket-a#docs/guide.pdf"},
            )()
        }
        self.state_by_object_pk = {}

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
    def __init__(self):
        self.executions = []

    def start_execution(self, **kwargs):
        self.executions.append(kwargs)
        return {"executionArn": "arn:aws:states:ap-southeast-1:123:execution:extract:abc"}


class _FakeDeleteLifecycleManager:
    def __init__(self) -> None:
        self.calls = []

    def handle_delete(self, *, source):
        self.calls.append(source.document_uri)


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


class _FakeProjectionStateRepoForDelete:
    def __init__(self) -> None:
        self.calls = []

    def mark_deleted(self, *, source, profile, manifest_s3_uri, error_message="source object deleted"):
        self.calls.append((source.document_uri, profile.profile_id, manifest_s3_uri, error_message))


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
    CN: 验证执行名称中包含 tenant 和 bucket 身份哈希。
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


def test_ingest_handles_delete_marker_without_starting_step_functions() -> None:
    """
    EN: Verify that delete marker events do not start Step Functions and return a cleanup plan.
    CN: 验证删除标记事件不会启动 Step Functions，而是返回清理计划。
    """
    state_repo = _FakeObjectStateRepo()
    delete_manager = _FakeDeleteLifecycleManager()
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
    assert result["deleted"][0]["cleanup_plan"] is None
    assert state_repo.deleted == [("bucket-a", "docs/guide.pdf", "delete-v1", "002")]
    assert delete_manager.calls == ["s3://bucket-a/docs/guide.pdf?versionId=delete-v1"]
    assert stepfunctions.executions == []


def test_ingest_retries_delete_side_effects_after_transient_failure() -> None:
    """
    EN: Verify that a transient delete governance failure is retried on the next invocation.
    CN: 验证临时性的删除治理失败会在下一次调用中重试。
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

    class _FlakyDeleteLifecycleManager:
        def __init__(self) -> None:
            self.calls = []

        def handle_delete(self, *, source):
            self.calls.append(source.document_uri)
            if len(self.calls) == 1:
                raise RuntimeError("transient delete governance failure")

    state_repo = _RetryableDeleteStateRepo()
    delete_manager = _FlakyDeleteLifecycleManager()
    starter = IngestWorkflowStarter(
        object_state_repo=state_repo,
        stepfunctions_client=_FakeStepFunctions(),
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

    with pytest.raises(RuntimeError, match="transient delete governance failure"):
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


def test_delete_marker_governance_marks_projection_states_deleted_for_each_profile() -> None:
    """
    EN: Verify that DeleteMarkerGovernance marks each embedding profile projection state deleted.
    CN: 验证 DeleteMarkerGovernance 会将每个 embedding profile 的 projection state 标记为 deleted。
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
    projection_state_repo = _FakeProjectionStateRepoForDelete()
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
        projection_state_repo=projection_state_repo,
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
    assert projection_state_repo.calls == [
        (
            "s3://bucket-a/docs/guide.pdf?versionId=v1",
            "gemini-default",
            "s3://manifest-bucket/manifests/example.json",
            "source object deleted",
        ),
        (
            "s3://bucket-a/docs/guide.pdf?versionId=v1",
            "openai-text-small",
            "s3://manifest-bucket/manifests/example.json",
            "source object deleted",
        ),
    ]


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

