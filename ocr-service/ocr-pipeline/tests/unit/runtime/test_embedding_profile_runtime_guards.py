"""
EN: Tests for embedding profile runtime guards ensuring disabled profiles are skipped at dispatch and backfill.
CN: embedding profile 运行时守卫测试，确保禁用的 profile 在分发和回填时被跳过。
"""
from __future__ import annotations

from types import SimpleNamespace

from serverless_mcp.entrypoints import backfill as backfill_handler
from serverless_mcp.entrypoints import embed as embed_handler
from serverless_mcp.runtime.config import Settings
from serverless_mcp.embed.backfill import EmbeddingBackfillOutcome
from serverless_mcp.embed.dispatcher import build_jobs_for_profiles
from serverless_mcp.embed.application import UnknownEmbeddingProfileError
from serverless_mcp.domain.models import EmbeddingProfile, EmbeddingRequest, S3ObjectRef


def _source() -> S3ObjectRef:
    """EN: Create a minimal S3ObjectRef for embedding profile tests.
    CN: 为 embedding profile 测试创建最小的 S3ObjectRef。"""
    return S3ObjectRef(
        tenant_id="tenant-a",
        bucket="source-bucket",
        key="docs/example.pdf",
        version_id="v1",
    )


def _openai_profile() -> EmbeddingProfile:
    """EN: Return a fully enabled OpenAI embedding profile for test fixtures.
    CN: 返回一个完全启用的 OpenAI embedding profile 供测试使用。"""
    return EmbeddingProfile(
        profile_id="openai-text-small",
        provider="openai",
        model="text-embedding-3-small",
        dimension=1536,
        vector_bucket_name="vector-bucket",
        vector_index_name="openai-index",
        supported_content_kinds=("text",),
        enabled=True,
        enable_write=True,
        enable_query=True,
    )


def _disabled_gemini_profile() -> EmbeddingProfile:
    """EN: Return a disabled Gemini embedding profile for guard tests.
    CN: 返回一个禁用的 Gemini embedding profile 供守卫测试使用。"""
    return EmbeddingProfile(
        profile_id="gemini-default",
        provider="gemini",
        model="gemini-embedding-2-preview",
        dimension=3072,
        vector_bucket_name="vector-bucket",
        vector_index_name="gemini-index",
        supported_content_kinds=("text", "image"),
        enabled=False,
        enable_write=True,
        enable_query=True,
    )


def test_build_jobs_for_profiles_skips_disabled_profiles() -> None:
    source = _source()
    request = EmbeddingRequest(
        chunk_id="chunk-1",
        chunk_type="section_text_chunk",
        content_kind="text",
        text="hello",
        metadata={"tenant_id": source.tenant_id},
    )

    jobs = build_jobs_for_profiles(
        source=source,
        trace_id="trace-1",
        manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
        requests=[request],
        profiles=(_openai_profile(), _disabled_gemini_profile()),
        previous_version_id=None,
        previous_manifest_s3_uri=None,
    )

    assert [job.profile_id for job in jobs] == ["openai-text-small"]


def test_process_embed_event_skips_inactive_profile_messages(monkeypatch) -> None:
    job = SimpleNamespace(
        source=_source(),
        profile_id="gemini-default",
        manifest_s3_uri="s3://manifest-bucket/manifests/example.json",
    )

    class _Worker:
        def process(self, _job):
            raise UnknownEmbeddingProfileError("Unknown embedding profile: gemini-default")

    monkeypatch.setattr(embed_handler, "parse_embedding_event", lambda event: [job])
    monkeypatch.setattr(embed_handler, "_get_worker", lambda: _Worker())

    result = embed_handler._process_embed_event({"Records": [{"body": "{}"}]})

    assert result["statusCode"] == 200
    assert result["processed_count"] == 0
    assert result["failed_count"] == 1
    assert result["failed"][0]["profile_id"] == "gemini-default"
    assert result["failed"][0]["disposition"] == "skipped_inactive_profile"


def test_backfill_handler_defaults_to_only_enabled_write_profile(monkeypatch) -> None:
    settings = Settings(
        object_state_table="object-state",
        manifest_index_table="manifest-index",
        manifest_bucket="manifest-bucket",
        manifest_prefix="",
        embed_queue_url="https://sqs.example.invalid/embed",
        embedding_profiles=(_openai_profile(), _disabled_gemini_profile()),
    )
    captured: dict[str, object] = {}

    class _Service:
        def backfill_profile(
            self,
            *,
            profile_id: str,
            trace_id: str,
            force: bool = False,
            resume_after_object_pk=None,
            max_records=None,
        ):
            captured["profile_id"] = profile_id
            captured["trace_id"] = trace_id
            captured["force"] = force
            return EmbeddingBackfillOutcome(
                profile_id=profile_id,
                scanned_count=1,
                eligible_count=1,
                dispatched_job_count=1,
                skipped_deleted_count=0,
                skipped_not_ready_count=0,
                skipped_stale_count=0,
                skipped_projection_count=0,
            )

    monkeypatch.setattr("serverless_mcp.embed.backfill_request.load_settings", lambda: settings)
    monkeypatch.setattr(backfill_handler, "build_backfill_service", lambda: _Service())

    result = backfill_handler.lambda_handler({"force": False}, SimpleNamespace(aws_request_id="req-1"))

    assert result["statusCode"] == 200
    assert captured["profile_id"] == "openai-text-small"
