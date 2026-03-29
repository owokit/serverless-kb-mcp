"""
EN: Tests for the EmbeddingBackfillService and backfill Lambda handler.
CN: 娴嬭瘯 EmbeddingBackfillService 鍜?backfill Lambda handler銆?
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from serverless_mcp.entrypoints import backfill as backfill_handler
from serverless_mcp.embed.backfill import EmbeddingBackfillService
from serverless_mcp.extract.application import ExtractionService
from serverless_mcp.domain.models import (
    ChunkManifest,
    EmbeddingProfile,
    ExtractedAsset,
    ExtractedChunk,
    ObjectStateRecord,
)
from serverless_mcp.storage.state.object_state_repository import ObjectStateLookupRecord


class _FakeObjectStateRepo:
    # EN: In-memory stand-in for ObjectStateRepository used by backfill tests.
    # CN: 鐢ㄤ簬 backfill 娴嬭瘯鐨?ObjectStateRepository 鍐呭瓨鏇胯韩銆?
    def __init__(self, lookups, states):
        self._lookups = lookups
        self._states = states

    def iter_lookup_records(self):
        return iter(self._lookups)

    def get_state(self, *, object_pk):
        return self._states.get(object_pk)


class _FakeManifestRepo:
    # EN: In-memory stand-in for ManifestRepository that returns a fixed ChunkManifest.
    # CN: 鐢ㄤ簬杩斿洖鍥哄畾 ChunkManifest 鐨?ManifestRepository 鍐呭瓨鏇胯韩銆?
    def load_manifest(self, manifest_s3_uri):
        return ChunkManifest(
            source=_source("tenant-a", "bucket-a", "docs/guide.pdf", "v2"),
            doc_type="pdf",
            chunks=[
                ExtractedChunk(
                    chunk_id="chunk#000001",
                    chunk_type="page_text_chunk",
                    text="hello",
                    doc_type="pdf",
                    token_estimate=2,
                    metadata={"source_format": "pdf"},
                )
            ],
            assets=[
                ExtractedAsset(
                    asset_id="asset#000001",
                    chunk_type="page_image_chunk",
                    mime_type="image/png",
                    payload=b"binary",
                    metadata={"source_format": "pdf"},
                )
            ],
            metadata={"source_format": "pdf", "page_count": 1, "visual_page_numbers": [], "page_image_asset_count": 1},
        )


class _FakeDispatcher:
    # EN: Captures dispatched embedding jobs for assertion.
    # CN: 同上。
    def __init__(self):
        self.jobs = []

    def dispatch_many(self, jobs):
        self.jobs.extend(jobs)


class _ProjectionStateRepo:
    # EN: Stand-in for embedding_projection_state_repository with pre-seeded ready keys.
    # CN: 棰勭疆灏辩华 key 鐨?embedding_projection_state_repository 鏇胯韩銆?
    def __init__(self, ready_keys=None):
        self._ready_keys = set(ready_keys or [])

    def get_state(self, *, object_pk, version_id, profile_id):
        if (object_pk, version_id, profile_id) not in self._ready_keys:
            return None

        class _State:
            # EN: Stand-in for projection state record with query_status.
            # CN: 同上。
            query_status = "INDEXED"

        return _State()


def _source(tenant_id: str, bucket: str, key: str, version_id: str):
    from serverless_mcp.domain.models import S3ObjectRef

    return S3ObjectRef(tenant_id=tenant_id, bucket=bucket, key=key, version_id=version_id)


def test_backfill_service_dispatches_historical_jobs_for_profile() -> None:
    """
    EN: Verify the backfill service iterates lookup records, skips stale entries, and dispatches eligible embedding jobs.
    CN: 同上。
    """
    lookup = ObjectStateLookupRecord(
        pk="lookup-v2#bucket-a#docs/guide.pdf",
        object_pk="tenant-a#bucket-a#docs/guide.pdf",
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="docs/guide.pdf",
        latest_version_id="v2",
        latest_sequencer="00000000000000000000000000000002",
        latest_manifest_s3_uri="s3://manifest-bucket/manifests/v2.json",
    )
    stale_lookup = ObjectStateLookupRecord(
        pk="lookup-v2#bucket-a#docs/old.pdf",
        object_pk="tenant-a#bucket-a#docs/old.pdf",
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="docs/old.pdf",
        latest_version_id="v1",
        latest_sequencer="00000000000000000000000000000001",
        latest_manifest_s3_uri="s3://manifest-bucket/manifests/v1.json",
    )
    states = {
        lookup.object_pk: ObjectStateRecord(
            pk=lookup.object_pk,
            latest_version_id="v2",
            latest_sequencer=lookup.latest_sequencer,
            extract_status="EXTRACTED",
            embed_status="INDEXED",
            previous_version_id="v1",
            previous_manifest_s3_uri="s3://manifest-bucket/manifests/v1.json",
            latest_manifest_s3_uri="s3://manifest-bucket/manifests/v2.json",
        ),
        stale_lookup.object_pk: ObjectStateRecord(
            pk=stale_lookup.object_pk,
            latest_version_id="v9",
            latest_sequencer=stale_lookup.latest_sequencer,
            extract_status="EXTRACTED",
            embed_status="INDEXED",
            latest_manifest_s3_uri="s3://manifest-bucket/manifests/v9.json",
        ),
    }
    service = EmbeddingBackfillService(
        extraction_service=ExtractionService(),
        object_state_repo=_FakeObjectStateRepo([lookup, stale_lookup], states),
        manifest_repo=_FakeManifestRepo(),
        embed_dispatcher=_FakeDispatcher(),
        embedding_profiles={
            "openai-text-small": EmbeddingProfile(
                profile_id="openai-text-small",
                provider="openai",
                model="text-embedding-3-small",
                dimension=1536,
                vector_bucket_name="vector-bucket",
                vector_index_name="index-openai",
                supported_content_kinds=("text",),
            )
        },
    )

    outcome = service.backfill_profile(profile_id="openai-text-small", trace_id="trace-1")

    assert outcome.scanned_count == 2
    assert outcome.eligible_count == 1
    assert outcome.dispatched_job_count == 1
    assert outcome.skipped_stale_count == 1
    assert outcome.samples[0].reason == "stale_or_missing_state"
    assert service._embed_dispatcher.jobs[0].profile_id == "openai-text-small"
    assert len(service._embed_dispatcher.jobs[0].requests) == 1
    assert service._embed_dispatcher.jobs[0].requests[0].content_kind == "text"


def test_backfill_service_skips_ready_projection_records_unless_forced() -> None:
    """
    EN: Verify that records with an INDEXED projection state are skipped unless force=True is supplied.
    CN: 同上。
    """
    lookup = ObjectStateLookupRecord(
        pk="lookup-v2#bucket-a#docs/guide.pdf",
        object_pk="tenant-a#bucket-a#docs/guide.pdf",
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="docs/guide.pdf",
        latest_version_id="v2",
        latest_sequencer="00000000000000000000000000000002",
        latest_manifest_s3_uri="s3://manifest-bucket/manifests/v2.json",
    )
    states = {
        lookup.object_pk: ObjectStateRecord(
            pk=lookup.object_pk,
            latest_version_id="v2",
            latest_sequencer=lookup.latest_sequencer,
            extract_status="EXTRACTED",
            embed_status="INDEXED",
            latest_manifest_s3_uri="s3://manifest-bucket/manifests/v2.json",
        )
    }
    dispatcher = _FakeDispatcher()
    service = EmbeddingBackfillService(
        extraction_service=ExtractionService(),
        object_state_repo=_FakeObjectStateRepo([lookup], states),
        manifest_repo=_FakeManifestRepo(),
        embed_dispatcher=dispatcher,
        embedding_profiles={
            "openai-text-small": EmbeddingProfile(
                profile_id="openai-text-small",
                provider="openai",
                model="text-embedding-3-small",
                dimension=1536,
                vector_bucket_name="vector-bucket",
                vector_index_name="index-openai",
                supported_content_kinds=("text",),
            )
        },
        projection_state_repo=_ProjectionStateRepo(
            ready_keys={(lookup.object_pk, lookup.latest_version_id, "openai-text-small")}
        ),
    )

    skipped = service.backfill_profile(profile_id="openai-text-small", trace_id="trace-1")
    forced = service.backfill_profile(profile_id="openai-text-small", trace_id="trace-2", force=True)

    assert skipped.skipped_projection_count == 1
    assert skipped.dispatched_job_count == 0
    assert forced.dispatched_job_count == 1
    assert len(dispatcher.jobs) == 1


def test_backfill_service_skips_corrupt_manifest_and_keeps_processing() -> None:
    """
    EN: Verify that a corrupt manifest for one object is skipped without aborting remaining records.
    CN: 同上。
    """
    lookup_ok = ObjectStateLookupRecord(
        pk="lookup-v2#bucket-a#docs/guide.pdf",
        object_pk="tenant-a#bucket-a#docs/guide.pdf",
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="docs/guide.pdf",
        latest_version_id="v2",
        latest_sequencer="00000000000000000000000000000002",
        latest_manifest_s3_uri="s3://manifest-bucket/manifests/v2.json",
    )
    lookup_bad = ObjectStateLookupRecord(
        pk="lookup-v2#bucket-a#docs/bad.pdf",
        object_pk="tenant-a#bucket-a#docs/bad.pdf",
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="docs/bad.pdf",
        latest_version_id="v2",
        latest_sequencer="00000000000000000000000000000003",
        latest_manifest_s3_uri="s3://manifest-bucket/manifests/bad.json",
    )
    states = {
        lookup_ok.object_pk: ObjectStateRecord(
            pk=lookup_ok.object_pk,
            latest_version_id="v2",
            latest_sequencer=lookup_ok.latest_sequencer,
            extract_status="EXTRACTED",
            embed_status="PENDING",
            latest_manifest_s3_uri=lookup_ok.latest_manifest_s3_uri,
        ),
        lookup_bad.object_pk: ObjectStateRecord(
            pk=lookup_bad.object_pk,
            latest_version_id="v2",
            latest_sequencer=lookup_bad.latest_sequencer,
            extract_status="EXTRACTED",
            embed_status="PENDING",
            latest_manifest_s3_uri=lookup_bad.latest_manifest_s3_uri,
        ),
    }

    # EN: Override manifest loader to raise on the corrupt entry.
    # CN: 瑕嗗啓 manifest loader锛屼娇鍏跺湪鎹熷潖鏉＄洰涓婃姏鍑哄紓甯搞€?
    class _CorruptManifestRepo(_FakeManifestRepo):
        # EN: Manifest repo that raises on corrupt entries.
        # CN: 同上。
        def load_manifest(self, manifest_s3_uri):
            if manifest_s3_uri.endswith("bad.json"):
                raise ValueError("manifest is corrupt")
            return super().load_manifest(manifest_s3_uri)

    dispatcher = _FakeDispatcher()
    service = EmbeddingBackfillService(
        extraction_service=ExtractionService(),
        object_state_repo=_FakeObjectStateRepo([lookup_ok, lookup_bad], states),
        manifest_repo=_CorruptManifestRepo(),
        embed_dispatcher=dispatcher,
        embedding_profiles={
            "openai-text-small": EmbeddingProfile(
                profile_id="openai-text-small",
                provider="openai",
                model="text-embedding-3-small",
                dimension=1536,
                vector_bucket_name="vector-bucket",
                vector_index_name="index-openai",
                supported_content_kinds=("text",),
            )
        },
    )

    outcome = service.backfill_profile(profile_id="openai-text-small", trace_id="trace-1")

    assert outcome.scanned_count == 2
    assert outcome.eligible_count == 1
    assert outcome.dispatched_job_count == 1
    assert outcome.samples[0].reason == "manifest_or_job_build_failed"
    assert len(dispatcher.jobs) == 1


def test_backfill_handler_requires_profile_id_for_multiple_writable_profiles(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify the Lambda handler raises when multiple writable profiles exist but no profile_id is provided.
    CN: 同上。
    """
    class _FakeSettings:
        # EN: Frozen dataclass stand-in for Settings.
        # CN: Settings 鐨勫喕缁?dataclass 鏇胯韩銆?
        embedding_profiles = (
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
        )

    monkeypatch.setattr("serverless_mcp.embed.backfill_request.load_settings", lambda: _FakeSettings())

    with pytest.raises(ValueError, match="profile_id is required"):
        backfill_handler.lambda_handler({"force": False}, None)


def test_backfill_handler_parses_force_and_invokes_service(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify the Lambda handler parses profile_id, force, and trace_id and delegates to the backfill service.
    CN: 同上。
    """
    captured = {}

    class _FakeSettings:
        # EN: Frozen dataclass stand-in for Settings.
        # CN: Settings 鐨勫喕缁?dataclass 鏇胯韩銆?
        embedding_profiles = (
            EmbeddingProfile(
                profile_id="openai-text-small",
                provider="openai",
                model="text-embedding-3-small",
                dimension=1536,
                vector_bucket_name="vector-bucket",
                vector_index_name="index-openai",
                supported_content_kinds=("text",),
            ),
        )

    class _FakeService:
        def backfill_profile(
            self,
            *,
            profile_id: str,
            trace_id: str,
            force: bool,
            resume_after_object_pk=None,
            max_records=None,
        ):
            captured["profile_id"] = profile_id
            captured["trace_id"] = trace_id
            captured["force"] = force
            return _Outcome(
                profile_id=profile_id,
                scanned_count=1,
                eligible_count=1,
                dispatched_job_count=1,
                skipped_deleted_count=0,
                skipped_not_ready_count=0,
                skipped_stale_count=0,
                skipped_projection_count=0,
                resume_after_object_pk=None,
                is_truncated=False,
                samples=(),
            )

    @dataclass(slots=True)
    class _Outcome:
        # EN: Dataclass representing a backfill outcome snapshot.
        # CN: 同上。
        profile_id: str
        scanned_count: int
        eligible_count: int
        dispatched_job_count: int
        skipped_deleted_count: int
        skipped_not_ready_count: int
        skipped_stale_count: int
        skipped_projection_count: int
        samples: tuple[object, ...] = ()
        resume_after_object_pk: str | None = None
        is_truncated: bool = False

    monkeypatch.setattr("serverless_mcp.embed.backfill_request.load_settings", lambda: _FakeSettings())
    monkeypatch.setattr(backfill_handler, "build_backfill_service", lambda: _FakeService())

    result = backfill_handler.lambda_handler(
        {"profile_id": "openai-text-small", "force": "true", "trace_id": "trace-99"},
        None,
    )

    assert captured == {
        "profile_id": "openai-text-small",
        "trace_id": "trace-99",
        "force": True,
    }
    assert result["profile_id"] == "openai-text-small"
    assert result["dispatched_job_count"] == 1


def test_backfill_handler_passes_resume_controls(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify the handler forwards resumable backfill controls when supplied.
    CN: 验证 handler 在收到恢复控制参数时会向下游透传。
    """
    captured = {}

    class _FakeSettings:
        embedding_profiles = (
            EmbeddingProfile(
                profile_id="openai-text-small",
                provider="openai",
                model="text-embedding-3-small",
                dimension=1536,
                vector_bucket_name="vector-bucket",
                vector_index_name="index-openai",
                supported_content_kinds=("text",),
            ),
        )

    class _FakeService:
        def backfill_profile(self, **kwargs):
            captured.update(kwargs)
            return _Outcome(
                profile_id=kwargs["profile_id"],
                scanned_count=1,
                eligible_count=1,
                dispatched_job_count=1,
                skipped_deleted_count=0,
                skipped_not_ready_count=0,
                skipped_stale_count=0,
                skipped_projection_count=0,
                resume_after_object_pk=kwargs.get("resume_after_object_pk"),
                is_truncated=True,
                samples=(),
            )

    @dataclass(slots=True)
    class _Outcome:
        profile_id: str
        scanned_count: int
        eligible_count: int
        dispatched_job_count: int
        skipped_deleted_count: int
        skipped_not_ready_count: int
        skipped_stale_count: int
        skipped_projection_count: int
        resume_after_object_pk: str | None
        is_truncated: bool
        samples: tuple[object, ...]

    monkeypatch.setattr("serverless_mcp.embed.backfill_request.load_settings", lambda: _FakeSettings())
    monkeypatch.setattr(backfill_handler, "build_backfill_service", lambda: _FakeService())

    result = backfill_handler.lambda_handler(
        {
            "profile_id": "openai-text-small",
            "force": False,
            "trace_id": "trace-101",
            "resume_after_object_pk": "tenant-a#bucket-a#docs/a.pdf",
            "max_records": 3,
        },
        None,
    )

    assert captured["resume_after_object_pk"] == "tenant-a#bucket-a#docs/a.pdf"
    assert captured["max_records"] == 3
    assert result["resume_after_object_pk"] == "tenant-a#bucket-a#docs/a.pdf"
    assert result["is_truncated"] is True


def test_backfill_handler_rejects_empty_event() -> None:
    """
    EN: Verify the handler returns 400 when the event dict is empty.
    CN: 楠岃瘉浜嬩欢瀛楀吀涓虹┖鏃?handler 杩斿洖 400銆?
    """
    result = backfill_handler.lambda_handler({}, None)

    assert result["statusCode"] == 400
    assert "profile_id is required for backfill worker" in result["body"]


def test_backfill_handler_rejects_non_dict_event() -> None:
    """
    EN: Verify the handler returns 400 when the event is not a dict.
    CN: 楠岃瘉浜嬩欢涓嶆槸瀛楀吀鏃?handler 杩斿洖 400銆?
    """
    result = backfill_handler.lambda_handler(["unexpected"], None)

    assert result["statusCode"] == 400
    assert "profile_id is required for backfill worker" in result["body"]
