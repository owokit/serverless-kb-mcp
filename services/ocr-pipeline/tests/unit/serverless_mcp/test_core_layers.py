"""
EN: Tests for the new core and runtime bootstrap boundaries.
CN: 新的 core 和 runtime bootstrap 边界测试。
"""

from __future__ import annotations

import json

import pytest

from serverless_mcp.core.serialization import build_document_id
from serverless_mcp.domain.models import S3ObjectRef
from serverless_mcp.runtime.aws_clients import AwsClientBundle
from serverless_mcp.runtime.bootstrap import RuntimeContext, build_runtime_context
from serverless_mcp.runtime.config import Settings, load_settings
from serverless_mcp.runtime.embedding_profiles import get_query_profiles, get_write_profiles


def test_build_document_id_changes_with_version_id() -> None:
    """
    EN: Verify that the public core document identifier helper remains version-aware.
    CN: 验证公共 core 文档标识生成器仍然按版本区分。
    """
    base = S3ObjectRef(
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="docs/guide.pdf",
        version_id="v1",
    )
    newer = S3ObjectRef(
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="docs/guide.pdf",
        version_id="v2",
    )

    assert build_document_id(base) != build_document_id(newer)


def test_runtime_profile_filters_follow_enabled_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify that the runtime helpers expose the expected profile filtering semantics.
    CN: 验证 runtime 辅助函数暴露的向量 profile 过滤语义。
    """
    load_settings.cache_clear()
    monkeypatch.setenv("OBJECT_STATE_TABLE", "object-state")
    monkeypatch.setenv(
        "EMBEDDING_PROFILES_JSON",
        json.dumps(
            [
                {
                    "profile_id": "openai-text-small",
                    "provider": "openai",
                    "model": "text-embedding-3-small",
                    "dimension": 1536,
                    "vector_bucket_name": "vector-bucket",
                    "vector_index_name": "openai-text-embedding-3-small-1536",
                    "supported_content_kinds": ["text"],
                    "enable_write": True,
                    "enable_query": False,
                },
                {
                    "profile_id": "gemini-default",
                    "provider": "gemini",
                    "model": "gemini-embedding-2-preview",
                    "dimension": 768,
                    "vector_bucket_name": "vector-bucket",
                    "vector_index_name": "gemini-embedding-2-preview-768",
                    "supported_content_kinds": ["text"],
                    "enable_write": False,
                    "enable_query": True,
                },
            ]
        ),
    )

    settings = load_settings()

    assert [profile.profile_id for profile in get_write_profiles(settings)] == ["openai-text-small"]
    assert [profile.profile_id for profile in get_query_profiles(settings)] == ["gemini-default"]
    load_settings.cache_clear()


def test_build_runtime_context_uses_explicit_dependencies() -> None:
    """
    EN: Verify the runtime composition root can reuse injected settings and clients without recomputation.
    CN: 验证运行时组合根可以复用注入的 settings 和 clients，而不重新解析。
    """
    settings = Settings(
        object_state_table="object-state",
        execution_state_table="execution-state",
        manifest_index_table="manifest-index",
        manifest_bucket="manifest-bucket",
        manifest_prefix="",
    )
    clients = AwsClientBundle(
        s3=object(),
        dynamodb=object(),
        sqs=object(),
        stepfunctions=object(),
        s3vectors=object(),
    )

    context = build_runtime_context(settings=settings, clients=clients)

    assert context == RuntimeContext(settings=settings, clients=clients)
