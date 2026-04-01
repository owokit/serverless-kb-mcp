"""
EN: Tests for EmbedWorker covering vector writing, timeout tracing, projection state, multi-profile cleanup, and previous version deletion.
CN: 鍚屼笂銆?
"""

import json

import pytest

from serverless_mcp.embed import application as embed_application_module
from serverless_mcp.embed.application import EmbedWorker
from serverless_mcp.domain.models import (
    ChunkManifest,
    ChunkManifestRecord,
    EmbeddingJobMessage,
    EmbeddingProfile,
    EmbeddingRequest,
    ExtractedAsset,
    ExtractedChunk,
    ObjectStateRecord,
    S3ObjectRef,
)
from serverless_mcp.storage.paths import optimize_source_file_name


class _FakeGeminiClient:
    # EN: Stub Gemini embedding client returning fixed vectors.
    # CN: 鍚屼笂銆?
    def embed_text(self, request):
        return [0.1, 0.2]

    def embed_bytes(self, *, payload, mime_type, request):
        assert payload == b"image-bytes"
        assert mime_type == "image/png"
        return [0.3, 0.4]


class _FakeAssetSource:
    # EN: Stub asset source that returns controlled image bytes.
    # CN: 鏉╂柨娲栭崣妤佸付閸ュ墽澧栫€涙濡惃鍕カ娴溠勭爱濡椻斂鈧?
    def load_s3_uri(self, asset_s3_uri):
        assert asset_s3_uri.endswith("/assets/asset-000001.png")
        return b"image-bytes"


class _FailingGeminiClient:
    # EN: Gemini client stub that raises RuntimeError on every call.
    # CN: 鍚屼笂銆?
    def embed_text(self, request):
        raise RuntimeError("timed out")

    def embed_bytes(self, *, payload, mime_type, request):
        raise RuntimeError("timed out")


class _FakeStepFunctionsClient:
    # EN: In-memory stand-in for Step Functions start_execution calls.
    # CN: Step Functions start_execution 鐨勫唴瀛樻浛韬€?
    def __init__(self):
        self.executions = []

    def start_execution(self, **kwargs):
        self.executions.append(kwargs)
        return {"executionArn": f"arn:aws:states:ap-southeast-1:123:execution:{kwargs['name']}"}


class _FailingStepFunctionsClient(_FakeStepFunctionsClient):
    # EN: Step Functions client that fails when a cleanup execution is started.
    # CN: 鍦ㄥ惎鍔?cleanup execution 鏃跺け璐ョ殑 Step Functions 瀹㈡埛绔€?
    def start_execution(self, **kwargs):
        self.executions.append(kwargs)
        raise RuntimeError("cleanup dispatch failed")


def _manifest_root(source: S3ObjectRef, version_id: str) -> str:
    return f"s3://manifest-bucket/{optimize_source_file_name(source)}"


def _request_metadata(source: S3ObjectRef, *, manifest_s3_uri: str) -> dict[str, object]:
    return {
        "tenant_id": source.tenant_id,
        "bucket": source.bucket,
        "key": source.key,
        "version_id": source.version_id,
        "document_uri": source.document_uri,
        "security_scope": list(source.security_scope),
        "language": source.language,
        "doc_type": "pdf",
        "source_format": "pdf",
        "manifest_s3_uri": manifest_s3_uri,
        "is_latest": True,
    }


class _FakeVectorRepo:
    # EN: In-memory stand-in for S3VectorRepository.
    # CN: 鍚屼笂銆?
    def __init__(self):
        self.jobs = []
        self.vectors = []

    def put_vectors(self, *, job, profile, vectors):
        self.jobs.append((job, profile.profile_id))
        self.vectors.extend(vectors)




class _FakeObjectStateRepo:
    # EN: In-memory stand-in for ObjectStateRepository.
    # CN: 鍚屼笂銆?
    def __init__(self):
        self.running = []
        self.done = []
        self.failed = []
        self.cleanup_failed = []
        self.state_by_pk = {}

    def mark_embed_running(self, source):
        self.running.append(source.document_uri)
        record = ObjectStateRecord(
            pk=source.object_pk,
            latest_version_id=source.version_id,
            latest_sequencer=source.sequencer,
            extract_status="EXTRACTED",
            embed_status="EMBEDDING",
            latest_manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
        )
        self.state_by_pk[source.object_pk] = record
        return record

    def mark_embed_done(self, source):
        self.done.append(source.document_uri)
        record = ObjectStateRecord(
            pk=source.object_pk,
            latest_version_id=source.version_id,
            latest_sequencer=source.sequencer,
            extract_status="EXTRACTED",
            embed_status="INDEXED",
            latest_manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
        )
        self.state_by_pk[source.object_pk] = record
        return record

    def mark_embed_failed(self, source, error_message):
        self.failed.append((source.document_uri, error_message))
        record = ObjectStateRecord(
            pk=source.object_pk,
            latest_version_id=source.version_id,
            latest_sequencer=source.sequencer,
            extract_status="EXTRACTED",
            embed_status="FAILED",
            latest_manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
            last_error=error_message,
        )
        self.state_by_pk[source.object_pk] = record
        return record

    def mark_embed_cleanup_failed(self, source, error_message):
        self.cleanup_failed.append((source.document_uri, error_message))
        record = self.state_by_pk.get(source.object_pk)
        if record is None:
            record = ObjectStateRecord(
                pk=source.object_pk,
                latest_version_id=source.version_id,
                latest_sequencer=source.sequencer,
                extract_status="EXTRACTED",
                embed_status="INDEXED",
                latest_manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
            )
        updated = ObjectStateRecord(
            pk=record.pk,
            latest_version_id=record.latest_version_id,
            latest_sequencer=record.latest_sequencer,
            extract_status=record.extract_status,
            embed_status=record.embed_status,
            previous_version_id=record.previous_version_id,
            previous_manifest_s3_uri=record.previous_manifest_s3_uri,
            latest_manifest_s3_uri=record.latest_manifest_s3_uri,
            is_deleted=record.is_deleted,
            last_error=error_message,
            updated_at=record.updated_at,
        )
        self.state_by_pk[source.object_pk] = updated
        return updated

    def get_state(self, *, object_pk):
        return self.state_by_pk.get(object_pk)


class _FakeExecutionStateRepo:
    # EN: In-memory stand-in for ExecutionStateRepository.
    # CN: 閸氬奔绗傞妴?
    def __init__(self):
        self.running = []
        self.done = []
        self.failed = []
        self.state_by_pk = {}

    def mark_embed_running(self, source):
        self.running.append(source.document_uri)
        record = ObjectStateRecord(
            pk=source.object_pk,
            latest_version_id=source.version_id,
            latest_sequencer=source.sequencer,
            extract_status="EXTRACTED",
            embed_status="EMBEDDING",
            latest_manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
        )
        self.state_by_pk[source.object_pk] = record
        return record

    def mark_embed_done(self, source):
        self.done.append(source.document_uri)
        record = ObjectStateRecord(
            pk=source.object_pk,
            latest_version_id=source.version_id,
            latest_sequencer=source.sequencer,
            extract_status="EXTRACTED",
            embed_status="INDEXED",
            latest_manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
        )
        self.state_by_pk[source.object_pk] = record
        return record

    def mark_embed_failed(self, source, error_message):
        self.failed.append((source.document_uri, error_message))
        record = ObjectStateRecord(
            pk=source.object_pk,
            latest_version_id=source.version_id,
            latest_sequencer=source.sequencer,
            extract_status="EXTRACTED",
            embed_status="FAILED",
            latest_manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
            last_error=error_message,
        )
        self.state_by_pk[source.object_pk] = record
        return record

    def get_state(self, *, object_pk):
        return self.state_by_pk.get(object_pk)


class _FakeProjectionStateRepo:
    # EN: Stand-in for EmbeddingProjectionStateRepository.
    # CN: 鍚屼笂銆?
    def __init__(self):
        self.running = []
        self.done = []
        self.failed = []
        self.deleted = []
        self._states = {}

    def mark_running(self, *, source, profile, manifest_s3_uri):
        self.running.append((source.document_uri, profile.profile_id, manifest_s3_uri))
        self._states[(source.object_pk, source.version_id, profile.profile_id)] = "EMBEDDING"

    def mark_done(self, *, outcome, profile):
        self.done.append((outcome.source.document_uri, profile.profile_id, outcome.vector_count))
        self._states[(outcome.source.object_pk, outcome.source.version_id, profile.profile_id)] = "INDEXED"

    def mark_failed(self, *, source, profile, manifest_s3_uri, error_message):
        self.failed.append((source.document_uri, profile.profile_id, manifest_s3_uri, error_message))
        self._states[(source.object_pk, source.version_id, profile.profile_id)] = "FAILED"

    def delete_version_records(self, *, source, version_id):
        self.deleted.append((source.document_uri, version_id))
        for key in list(self._states):
            if key[0] == source.object_pk and key[1] == version_id:
                del self._states[key]

    def get_state(self, *, object_pk, version_id, profile_id):
        class _Record:
            # EN: Stand-in for projection state record.
            # CN: 鍚屼笂銆?
            def __init__(self, query_status):
                self.query_status = query_status

        status = self._states.get((object_pk, version_id, profile_id))
        if status is None:
            return None
        return _Record(status)


class _FakeManifestRepo:
    def __init__(self):
        self.loaded_uris = []
        self.delete_calls = []
        self.version_record_calls = []

    def build_manifest_s3_uri(self, *, source, version_id):
        return f"{_manifest_root(source, version_id)}/manifest.json"

    def find_manifest_s3_uri(self, *, source, version_id):
        return f"{_manifest_root(source, version_id)}/manifest.json"

    def load_manifest(self, manifest_s3_uri):
        self.loaded_uris.append(manifest_s3_uri)
        version_id = "v0" if manifest_s3_uri.endswith("v0.json") else "v2"
        source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/guide.pdf", version_id=version_id)
        return ChunkManifest(
            source=source,
            doc_type="pdf",
            chunks=[
                ExtractedChunk(
                    chunk_id="chunk#000001" if version_id == "v0" else "chunk#current-001",
                    chunk_type="page_text_chunk",
                    text="hello-old" if version_id == "v0" else "hello-current",
                    doc_type="pdf",
                    token_estimate=2,
                )
            ],
            assets=[
                ExtractedAsset(
                    asset_id="asset#000001" if version_id == "v0" else "asset#current-001",
                    chunk_type="page_image_chunk",
                    mime_type="image/png",
                    asset_s3_uri=_manifest_root(source, version_id) + "/assets/asset-000001.png",
                )
            ],
        )

    def list_version_records(self, *, source, version_id):
        self.version_record_calls.append((source.document_uri, version_id))
        return [
            ChunkManifestRecord(
                pk=f"{source.object_pk}#{version_id}",
                sk="chunk#000001",
                tenant_id=source.tenant_id,
                bucket=source.bucket,
                key=source.key,
                version_id=version_id,
                chunk_id="chunk#000001",
                chunk_type="page_text_chunk",
                doc_type="pdf",
                is_latest=False,
                security_scope=tuple(source.security_scope),
                language=source.language,
                page_no=1,
                text_preview="hello-old",
                manifest_s3_uri=f"s3://manifest-bucket/manifests/{version_id}.json",
            )
        ]

    def delete_previous_version_artifacts(self, *, source, previous_version_id=None, previous_manifest_s3_uri=None):
        self.delete_calls.append((source.document_uri, previous_version_id, previous_manifest_s3_uri))


def _build_single_profile_worker(
    *,
    object_state_repo,
    vector_repo,
    manifest_repo,
    projection_state_repo=None,
    embedding_client=None,
    stepfunctions_client=None,
    cleanup_state_machine_arn="arn:aws:states:ap-southeast-1:123:stateMachine:vector-cleanup",
):
    return EmbedWorker(
        embedding_clients={"gemini-default": embedding_client or _FakeGeminiClient()},
        embedding_profiles={
            "gemini-default": EmbeddingProfile(
                profile_id="gemini-default",
                provider="gemini",
                model="gemini-embedding-2-preview",
                dimension=3072,
                vector_bucket_name="vector-bucket",
                vector_index_name="index-gemini",
                supported_content_kinds=("text", "image"),
            )
        },
        asset_source=_FakeAssetSource(),
        vector_repo=vector_repo,
        object_state_repo=object_state_repo,
        manifest_repo=manifest_repo,
        stepfunctions_client=stepfunctions_client or _FakeStepFunctionsClient(),
        cleanup_state_machine_arn=cleanup_state_machine_arn,
        projection_state_repo=projection_state_repo,
    )


def test_embed_worker_writes_vectors_marks_done_and_records_previous_version_cleanup() -> None:
    """
    EN: Embed worker writes vectors marks done and cleans previous version artifacts.
    CN: 濡ょ姴鐭侀惁?embed worker writes vectors marks done and cleans previous version artifacts闁?
    """
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/guide.pdf", version_id="v1")
    previous_manifest_root = _manifest_root(source, "v0")
    vector_repo = _FakeVectorRepo()
    object_state_repo = _FakeObjectStateRepo()
    manifest_repo = _FakeManifestRepo()
    stepfunctions_client = _FakeStepFunctionsClient()
    worker = _build_single_profile_worker(
        object_state_repo=object_state_repo,
        vector_repo=vector_repo,
        manifest_repo=manifest_repo,
        stepfunctions_client=stepfunctions_client,
    )

    outcome = worker.process(
        EmbeddingJobMessage(
            source=source,
            profile_id="gemini-default",
            trace_id="trace-1",
            manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
            previous_version_id="v0",
            previous_manifest_s3_uri="s3://manifest-bucket/manifests/v0.json",
            requests=[
                EmbeddingRequest(
                    chunk_id="chunk#999999",
                    chunk_type="page_text_chunk",
                    content_kind="text",
                    text="hello",
                    metadata=_request_metadata(source, manifest_s3_uri="s3://manifest-bucket/manifests/example.json"),
                ),
                EmbeddingRequest(
                    chunk_id="asset#999999",
                    chunk_type="page_image_chunk",
                    content_kind="image",
                    asset_id="asset#999999",
                    asset_s3_uri=f"{previous_manifest_root}/assets/asset-000001.png",
                    mime_type="image/png",
                    metadata=_request_metadata(source, manifest_s3_uri="s3://manifest-bucket/manifests/example.json"),
                ),
            ],
        )
    )

    assert outcome.vector_count == 2
    assert object_state_repo.running == [source.document_uri]
    assert object_state_repo.done == [source.document_uri]
    assert outcome.object_state.latest_manifest_s3_uri == "s3://manifest-bucket/manifests/example.json"
    assert outcome.profile_id == "gemini-default"
    assert len(vector_repo.vectors) == 2
    assert all(vector.metadata["is_latest"] is True for vector in vector_repo.vectors)
    assert len(stepfunctions_client.executions) == 1
    cleanup_execution = stepfunctions_client.executions[0]
    assert cleanup_execution["stateMachineArn"] == "arn:aws:states:ap-southeast-1:123:stateMachine:vector-cleanup"
    cleanup_payload = json.loads(cleanup_execution["input"])
    assert cleanup_execution["name"].startswith("cleanup-")
    assert cleanup_payload["cleanup_target"]["previous_version_id"] == "v0"
    assert cleanup_payload["cleanup_target"]["keys"] == [
        "gemini-default#tenant-a#bucket-a#docs%2Fguide.pdf#v0#chunk#000001",
        "gemini-default#tenant-a#bucket-a#docs%2Fguide.pdf#v0#asset#000001",
    ]
    assert manifest_repo.version_record_calls == [(source.document_uri, "v0")]
    assert manifest_repo.delete_calls == [(source.document_uri, "v0", "s3://manifest-bucket/manifests/v0.json")]

def test_embed_worker_emits_request_context_on_text_timeout(monkeypatch) -> None:
    """
    EN: Embed worker emits request context on text timeout.
    CN: 妤犲矁鐦?embed worker emits request context on text timeout閵?
    """
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/guide.pdf", version_id="v1")
    vector_repo = _FakeVectorRepo()
    object_state_repo = _FakeObjectStateRepo()
    manifest_repo = _FakeManifestRepo()
    calls = []

    monkeypatch.setattr(embed_application_module, "emit_trace", lambda stage, **fields: calls.append((stage, fields)))

    worker = _build_single_profile_worker(
        object_state_repo=object_state_repo,
        vector_repo=vector_repo,
        manifest_repo=manifest_repo,
        embedding_client=_FailingGeminiClient(),
    )

    with pytest.raises(RuntimeError, match="timed out"):
        worker.process(
            EmbeddingJobMessage(
                source=source,
                profile_id="gemini-default",
                trace_id="trace-1",
                manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
                requests=[
                    EmbeddingRequest(
                        chunk_id="chunk#000001",
                        chunk_type="page_text_chunk",
                        content_kind="text",
                        text="hello world",
                        metadata=_request_metadata(source, manifest_s3_uri="s3://manifest-bucket/manifests/example.json"),
                    )
                ],
            )
        )

    start = next(fields for stage, fields in calls if stage == "embed.request.start")
    failed = next(fields for stage, fields in calls if stage == "embed.request.failed")

    assert start["profile_id"] == "gemini-default"
    assert start["chunk_id"] == "chunk#000001"
    assert start["content_kind"] == "text"
    assert start["request_index"] == 0
    assert start["payload_size_bytes"] == len("hello world".encode("utf-8"))
    assert failed["error_type"] == "RuntimeError"
    assert failed["error_message"] == "timed out"
    assert failed["payload_size_bytes"] == len("hello world".encode("utf-8"))
    assert object_state_repo.failed == [(source.document_uri, "timed out")]


def test_embed_worker_emits_request_context_on_image_timeout(monkeypatch) -> None:
    """
    EN: Embed worker emits request context on image timeout.
    CN: 妤犲矁鐦?embed worker emits request context on image timeout閵?
    """
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/guide.pdf", version_id="v1")
    vector_repo = _FakeVectorRepo()
    object_state_repo = _FakeObjectStateRepo()
    manifest_repo = _FakeManifestRepo()
    calls = []

    monkeypatch.setattr(embed_application_module, "emit_trace", lambda stage, **fields: calls.append((stage, fields)))

    worker = _build_single_profile_worker(
        object_state_repo=object_state_repo,
        vector_repo=vector_repo,
        manifest_repo=manifest_repo,
        embedding_client=_FailingGeminiClient(),
    )

    with pytest.raises(RuntimeError, match="timed out"):
        worker.process(
            EmbeddingJobMessage(
                source=source,
                profile_id="gemini-default",
                trace_id="trace-1",
                manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
                requests=[
                    EmbeddingRequest(
                        chunk_id="asset#000001",
                        chunk_type="page_image_chunk",
                        content_kind="image",
                        asset_id="asset#000001",
                        asset_s3_uri="s3://manifest-bucket/manifests/example.json/assets/asset-000001.png",
                        mime_type="image/png",
                        metadata=_request_metadata(source, manifest_s3_uri="s3://manifest-bucket/manifests/example.json"),
                    )
                ],
            )
        )

    start = next(fields for stage, fields in calls if stage == "embed.request.start")
    failed = next(fields for stage, fields in calls if stage == "embed.request.failed")

    assert start["profile_id"] == "gemini-default"
    assert start["chunk_id"] == "asset#000001"
    assert start["content_kind"] == "image"
    assert start["mime_type"] == "image/png"
    assert start["request_index"] == 0
    assert start["payload_size_bytes"] == len(b"image-bytes")
    assert failed["error_type"] == "RuntimeError"
    assert failed["error_message"] == "timed out"
    assert failed["payload_size_bytes"] == len(b"image-bytes")
    assert object_state_repo.failed == [(source.document_uri, "timed out")]


def test_embed_worker_uses_projection_state_without_mutating_global_embed_status() -> None:
    """
    EN: Embed worker uses projection state without mutating global embed status.
    CN: 妤犲矁鐦?embed worker uses projection state without mutating global embed status閵?
    """
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/guide.pdf", version_id="v1")
    vector_repo = _FakeVectorRepo()
    object_state_repo = _FakeObjectStateRepo()
    object_state_repo.state_by_pk[source.object_pk] = ObjectStateRecord(
        pk=source.object_pk,
        latest_version_id=source.version_id,
        latest_sequencer=source.sequencer,
        extract_status="EXTRACTED",
        embed_status="PENDING",
        latest_manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
    )
    projection_state_repo = _FakeProjectionStateRepo()
    manifest_repo = _FakeManifestRepo()
    worker = _build_single_profile_worker(
        object_state_repo=object_state_repo,
        vector_repo=vector_repo,
        manifest_repo=manifest_repo,
        projection_state_repo=projection_state_repo,
    )

    outcome = worker.process(
        EmbeddingJobMessage(
            source=source,
            profile_id="gemini-default",
            trace_id="trace-1",
            manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
            requests=[
            EmbeddingRequest(
                chunk_id="chunk#000001",
                chunk_type="page_text_chunk",
                content_kind="text",
                text="hello",
                metadata=_request_metadata(source, manifest_s3_uri="s3://manifest-bucket/manifests/example.json"),
            )
            ],
        )
    )

    assert outcome.object_state.embed_status == "PENDING"
    assert object_state_repo.running == []
    assert object_state_repo.done == []
    assert object_state_repo.failed == []
    assert projection_state_repo.running == [
        (source.document_uri, "gemini-default", "s3://manifest-bucket/manifests/example.json")
    ]
    assert projection_state_repo.done == [(source.document_uri, "gemini-default", 1)]
    assert manifest_repo.delete_calls == []


def test_embed_worker_reads_execution_state_when_projection_state_exists() -> None:
    """
    EN: Embed worker reads execution state when projection state exists.
    CN: 当 projection state 存在时，embed worker 读取 execution state。
    """
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/guide.pdf", version_id="v1")
    vector_repo = _FakeVectorRepo()
    object_state_repo = _FakeObjectStateRepo()
    execution_state_repo = _FakeExecutionStateRepo()
    execution_state_repo.state_by_pk[source.object_pk] = ObjectStateRecord(
        pk=source.object_pk,
        latest_version_id=source.version_id,
        latest_sequencer=source.sequencer,
        extract_status="EXTRACTED",
        embed_status="EMBEDDING",
        latest_manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
    )
    projection_state_repo = _FakeProjectionStateRepo()
    manifest_repo = _FakeManifestRepo()
    worker = EmbedWorker(
        embedding_clients={"gemini-default": _FakeGeminiClient()},
        embedding_profiles={
            "gemini-default": EmbeddingProfile(
                profile_id="gemini-default",
                provider="gemini",
                model="gemini-embedding-2-preview",
                dimension=3072,
                vector_bucket_name="vector-bucket",
                vector_index_name="index-gemini",
                supported_content_kinds=("text", "image"),
            )
        },
        asset_source=_FakeAssetSource(),
        vector_repo=vector_repo,
        object_state_repo=object_state_repo,
        manifest_repo=manifest_repo,
        execution_state_repo=execution_state_repo,
        projection_state_repo=projection_state_repo,
    )

    outcome = worker.process(
        EmbeddingJobMessage(
            source=source,
            profile_id="gemini-default",
            trace_id="trace-1",
            manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
            requests=[
                EmbeddingRequest(
                    chunk_id="chunk#000001",
                    chunk_type="page_text_chunk",
                    content_kind="text",
                    text="hello",
                    metadata=_request_metadata(source, manifest_s3_uri="s3://manifest-bucket/manifests/example.json"),
                )
            ],
        )
    )

    assert outcome.object_state.embed_status == "EMBEDDING"
    assert execution_state_repo.running == [source.document_uri]
    assert execution_state_repo.done == []
    assert object_state_repo.running == []
    assert object_state_repo.done == []
    assert projection_state_repo.done == [(source.document_uri, "gemini-default", 1)]


def test_embed_worker_defers_previous_projection_cleanup_before_marking_profile_done() -> None:
    """
    EN: Embed worker deletes previous projection state and vectors before marking profile done.
    CN: 妤犲矁鐦?embed worker deletes previous projection state and vectors before marking profile done閵?
    """
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/guide.pdf", version_id="v1")
    vector_repo = _FakeVectorRepo()
    object_state_repo = _FakeObjectStateRepo()
    object_state_repo.state_by_pk[source.object_pk] = ObjectStateRecord(
        pk=source.object_pk,
        latest_version_id=source.version_id,
        latest_sequencer=source.sequencer,
        extract_status="EXTRACTED",
        embed_status="PENDING",
        latest_manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
    )
    projection_state_repo = _FakeProjectionStateRepo()
    manifest_repo = _FakeManifestRepo()
    worker = _build_single_profile_worker(
        object_state_repo=object_state_repo,
        vector_repo=vector_repo,
        manifest_repo=manifest_repo,
        projection_state_repo=projection_state_repo,
    )

    worker.process(
        EmbeddingJobMessage(
            source=source,
            profile_id="gemini-default",
            trace_id="trace-1",
            manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
            previous_version_id="v0",
            previous_manifest_s3_uri="s3://manifest-bucket/manifests/v0.json",
            requests=[
            EmbeddingRequest(
                chunk_id="chunk#000001",
                chunk_type="page_text_chunk",
                content_kind="text",
                text="hello",
                metadata=_request_metadata(source, manifest_s3_uri="s3://manifest-bucket/manifests/example.json"),
            )
            ],
        )
    )

    assert manifest_repo.delete_calls == [(source.document_uri, "v0", "s3://manifest-bucket/manifests/v0.json")]


def test_embed_worker_uses_derived_previous_manifest_uri_when_not_explicitly_provided() -> None:
    """
    EN: Embed worker uses derived previous manifest uri when not explicitly provided.
    CN: 妤犲矁鐦?embed worker uses derived previous manifest uri when not explicitly provided閵?
    """
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/guide.pdf", version_id="v1")
    vector_repo = _FakeVectorRepo()
    object_state_repo = _FakeObjectStateRepo()
    object_state_repo.state_by_pk[source.object_pk] = ObjectStateRecord(
        pk=source.object_pk,
        latest_version_id=source.version_id,
        latest_sequencer=source.sequencer,
        extract_status="EXTRACTED",
        embed_status="PENDING",
        latest_manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
    )
    projection_state_repo = _FakeProjectionStateRepo()
    manifest_repo = _FakeManifestRepo()
    worker = _build_single_profile_worker(
        object_state_repo=object_state_repo,
        vector_repo=vector_repo,
        manifest_repo=manifest_repo,
        projection_state_repo=projection_state_repo,
    )

    worker.process(
        EmbeddingJobMessage(
            source=source,
            profile_id="gemini-default",
            trace_id="trace-1",
            manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
            previous_version_id="v0",
            requests=[
            EmbeddingRequest(
                chunk_id="chunk#000001",
                chunk_type="page_text_chunk",
                content_kind="text",
                text="hello",
                metadata=_request_metadata(source, manifest_s3_uri="s3://manifest-bucket/manifests/example.json"),
            )
            ],
        )
    )

    assert manifest_repo.delete_calls == [(source.document_uri, "v0", f"{_manifest_root(source, 'v0')}/manifest.json")]


def test_embed_worker_defers_previous_manifest_cleanup_until_all_write_profiles_complete() -> None:
    """
    EN: Embed worker defers previous manifest cleanup until all write profiles complete.
    CN: 妤犲矁鐦?embed worker defers previous manifest cleanup until all write profiles complete閵?
    """
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/guide.pdf", version_id="v1")
    vector_repo = _FakeVectorRepo()
    object_state_repo = _FakeObjectStateRepo()
    object_state_repo.state_by_pk[source.object_pk] = ObjectStateRecord(
        pk=source.object_pk,
        latest_version_id=source.version_id,
        latest_sequencer=source.sequencer,
        extract_status="EXTRACTED",
        embed_status="PENDING",
        latest_manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
    )
    projection_state_repo = _FakeProjectionStateRepo()
    manifest_repo = _FakeManifestRepo()
    stepfunctions_client = _FakeStepFunctionsClient()
    worker = EmbedWorker(
        embedding_clients={
            "gemini-default": _FakeGeminiClient(),
            "openai-text-small": _FakeGeminiClient(),
        },
        embedding_profiles={
            "gemini-default": EmbeddingProfile(
                profile_id="gemini-default",
                provider="gemini",
                model="gemini-embedding-2-preview",
                dimension=3072,
                vector_bucket_name="vector-bucket",
                vector_index_name="index-gemini",
                supported_content_kinds=("text", "image"),
            ),
            "openai-text-small": EmbeddingProfile(
                profile_id="openai-text-small",
                provider="openai",
                model="text-embedding-3-small",
                dimension=1536,
                vector_bucket_name="vector-bucket",
                vector_index_name="index-openai",
                supported_content_kinds=("text",),
            ),
        },
        asset_source=_FakeAssetSource(),
        vector_repo=vector_repo,
        object_state_repo=object_state_repo,
        manifest_repo=manifest_repo,
        stepfunctions_client=stepfunctions_client,
        cleanup_state_machine_arn="arn:aws:states:ap-southeast-1:123:stateMachine:vector-cleanup",
        projection_state_repo=projection_state_repo,
    )

    worker.process(
        EmbeddingJobMessage(
            source=source,
            profile_id="gemini-default",
            trace_id="trace-1",
            manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
            previous_version_id="v0",
            previous_manifest_s3_uri="s3://manifest-bucket/manifests/v0.json",
            requests=[
            EmbeddingRequest(
                chunk_id="chunk#000001",
                chunk_type="page_text_chunk",
                content_kind="text",
                text="hello",
                metadata=_request_metadata(source, manifest_s3_uri="s3://manifest-bucket/manifests/example.json"),
            )
            ],
        )
    )

    assert len(stepfunctions_client.executions) == 1
    assert manifest_repo.delete_calls == []

    worker.process(
        EmbeddingJobMessage(
            source=source,
            profile_id="openai-text-small",
            trace_id="trace-1",
            manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
            previous_version_id="v0",
            previous_manifest_s3_uri="s3://manifest-bucket/manifests/v0.json",
            requests=[
            EmbeddingRequest(
                chunk_id="chunk#000001",
                chunk_type="page_text_chunk",
                content_kind="text",
                text="hello",
                metadata=_request_metadata(source, manifest_s3_uri="s3://manifest-bucket/manifests/example.json"),
            )
            ],
        )
    )

    assert len(stepfunctions_client.executions) == 2
    assert manifest_repo.delete_calls == [(source.document_uri, "v0", "s3://manifest-bucket/manifests/v0.json")]


def test_embed_worker_requires_projection_state_for_multiple_profiles() -> None:
    """
    EN: Embed worker requires projection state for multiple profiles.
    CN: 妤犲矁鐦?embed worker requires projection state for multiple profiles閵?
    """
    try:
        EmbedWorker(
            embedding_clients={
                "gemini-default": _FakeGeminiClient(),
                "openai-text-small": _FakeGeminiClient(),
            },
            embedding_profiles={
                "gemini-default": EmbeddingProfile(
                    profile_id="gemini-default",
                    provider="gemini",
                    model="gemini-embedding-2-preview",
                    dimension=3072,
                    vector_bucket_name="vector-bucket",
                    vector_index_name="index-gemini",
                    supported_content_kinds=("text", "image"),
                ),
                "openai-text-small": EmbeddingProfile(
                    profile_id="openai-text-small",
                    provider="openai",
                    model="text-embedding-3-small",
                    dimension=1536,
                    vector_bucket_name="vector-bucket",
                    vector_index_name="index-openai",
                    supported_content_kinds=("text",),
                ),
            },
            asset_source=_FakeAssetSource(),
            vector_repo=_FakeVectorRepo(),
            object_state_repo=_FakeObjectStateRepo(),
            manifest_repo=_FakeManifestRepo(),
            projection_state_repo=None,
        )
    except ValueError as exc:
        assert "EMBEDDING_PROJECTION_STATE_TABLE" in str(exc)
    else:
        raise AssertionError("multiple write profiles should require projection state governance")


def test_embed_worker_keeps_success_when_previous_version_cleanup_fails() -> None:
    """
    EN: Embed worker keeps success when previous version cleanup fails.
    CN: 妤犲矁鐦?embed worker keeps success when previous version cleanup fails閵?
    """
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/guide.pdf", version_id="v1")

    object_state_repo = _FakeObjectStateRepo()
    manifest_repo = _FakeManifestRepo()
    stepfunctions_client = _FailingStepFunctionsClient()
    worker = _build_single_profile_worker(
        object_state_repo=object_state_repo,
        vector_repo=_FakeVectorRepo(),
        manifest_repo=manifest_repo,
        stepfunctions_client=stepfunctions_client,
    )

    outcome = worker.process(
        EmbeddingJobMessage(
            source=source,
            profile_id="gemini-default",
            trace_id="trace-1",
            manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
            previous_version_id="v0",
            previous_manifest_s3_uri="s3://manifest-bucket/manifests/v0.json",
            requests=[
                EmbeddingRequest(
                    chunk_id="chunk#000001",
                    chunk_type="page_text_chunk",
                    content_kind="text",
                    text="hello",
                    metadata=_request_metadata(source, manifest_s3_uri="s3://manifest-bucket/manifests/example.json"),
                )
            ],
        )
    )

    assert outcome.object_state.embed_status == "INDEXED"
    assert object_state_repo.done == [source.document_uri]
    assert object_state_repo.cleanup_failed == [(source.document_uri, "cleanup dispatch failed")]
    assert len(stepfunctions_client.executions) == 1
