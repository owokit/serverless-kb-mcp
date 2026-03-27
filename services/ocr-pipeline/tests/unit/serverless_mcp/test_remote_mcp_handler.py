"""
EN: Tests for the remote MCP handler including search_documents, tenant resolution, and MCP error mapping.
CN: 远程 MCP 处理器的测试，覆盖 search_documents、tenant 解析和 MCP 错误映射。
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from mcp import McpError

from serverless_mcp.domain.models import QueryResponse, QueryResultContext, QueryResultItem, S3ObjectRef
from serverless_mcp.entrypoints import remote_mcp as remote_mcp_handler
from serverless_mcp.query.request import TenantIdConflictError, build_remote_query_request
from serverless_mcp.runtime.config import Settings


@dataclass(frozen=True)
class _FakeSettings:
    # EN: Frozen dataclass stand-in for Settings.
    # CN: Settings 的冻结 dataclass 替身。
    query_max_top_k: int = 20
    query_max_neighbor_expand: int = 2
    allow_unauthenticated_query: bool = True
    query_tenant_claim: str = "tenant_id"
    remote_mcp_default_tenant_id: str | None = None
    cloudfront_distribution_domain: str | None = "cdn.example.com"
    cloudfront_key_pair_id: str | None = "K123"
    cloudfront_private_key_pem: str | None = "-----BEGIN PRIVATE KEY-----\nFAKE\n-----END PRIVATE KEY-----"
    cloudfront_url_ttl_seconds: int = 900


class _FakeQueryService:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def search(self, **kwargs):
        self.calls.append(kwargs)
        source = S3ObjectRef(
            tenant_id="tenant-a",
            bucket="bucket-a",
            key="docs/guide.pdf",
            version_id="v2",
        )
        return QueryResponse(
            query=kwargs["query"],
            results=[
                QueryResultItem(
                    key="hit-1",
                    distance=0.01,
                    source=source,
                    manifest_s3_uri=None,
                    metadata={"doc_type": "pdf", "__fusion_score__": 0.75, "__profile_hits__": 2},
                    match=QueryResultContext(
                        chunk_id="chunk#1",
                        chunk_type="page_text_chunk",
                        text="hello",
                    ),
                )
            ],
        )


class _FakeDeliveryService:
    def deliver_source_document(self, source):
        class _Delivery:
            # EN: Stand-in for CloudFront delivery result.
            # CN: CloudFront 分发结果替身。
            url = f"https://cdn.example.com/documents/{source.bucket}/{source.key}?versionId={source.version_id}"
            expires_at = "2026-03-17T00:00:00+00:00"

        return _Delivery()


def test_search_documents_returns_structured_results(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Search documents returns structured results.
    CN: 验证 search_documents 返回结构化结果。
    """
    service = _FakeQueryService()
    monkeypatch.setattr(remote_mcp_handler, "_build_service", lambda: service)
    monkeypatch.setattr(remote_mcp_handler, "load_settings", lambda: _FakeSettings())
    monkeypatch.setattr(remote_mcp_handler, "_build_delivery_service", lambda: _FakeDeliveryService())

    payload = remote_mcp_handler.search_documents(
        query="hello",
        tenant_id="tenant-a",
        top_k=5,
        neighbor_expand=1,
    )

    assert payload["query"] == "hello"
    assert payload["results"][0]["delivery"]["url"].startswith("https://cdn.example.com/")
    assert payload["results"][0]["fusion_score"] == 0.75
    assert service.calls[0]["tenant_id"] == "tenant-a"


def test_search_documents_rejects_out_of_range_top_k(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Search documents rejects out of range top k.
    CN: 验证 search_documents 会拒绝越界的 top_k。
    """
    monkeypatch.setattr(remote_mcp_handler, "load_settings", lambda: _FakeSettings(query_max_top_k=3))

    with pytest.raises(ValueError, match="top_k must be between 1 and 3"):
        remote_mcp_handler.search_documents(
            query="hello",
            tenant_id="tenant-a",
            top_k=4,
        )


def test_search_documents_uses_authenticated_request_tenant(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Search documents uses the authenticated request tenant when no explicit tenant is provided.
    CN: 当未显式传入 tenant 时，search_documents 应使用已认证请求租户。
    """
    service = _FakeQueryService()
    monkeypatch.setattr(remote_mcp_handler, "_build_service", lambda: service)
    monkeypatch.setattr(remote_mcp_handler, "load_settings", lambda: _FakeSettings())
    monkeypatch.setattr(remote_mcp_handler, "_build_delivery_service", lambda: _FakeDeliveryService())

    token = remote_mcp_handler._REQUEST_TENANT_ID.set("tenant-a")
    try:
        payload = remote_mcp_handler.search_documents(query="hello")
    finally:
        remote_mcp_handler._REQUEST_TENANT_ID.reset(token)

    assert payload["query"] == "hello"
    assert service.calls[0]["tenant_id"] == "tenant-a"


def test_search_documents_rejects_tenant_override_conflict(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Search documents rejects conflicting explicit tenant overrides with a 403-equivalent MCP error.
    CN: search_documents 应拒绝与认证租户冲突的显式 tenant 覆盖，并返回 403 等价的 MCP 错误。
    """
    service = _FakeQueryService()
    monkeypatch.setattr(remote_mcp_handler, "_build_service", lambda: service)
    monkeypatch.setattr(remote_mcp_handler, "load_settings", lambda: _FakeSettings())
    monkeypatch.setattr(remote_mcp_handler, "_build_delivery_service", lambda: _FakeDeliveryService())

    token = remote_mcp_handler._REQUEST_TENANT_ID.set("tenant-a")
    try:
        with pytest.raises(McpError) as exc_info:
            remote_mcp_handler.search_documents(query="hello", tenant_id="tenant-b")
    finally:
        remote_mcp_handler._REQUEST_TENANT_ID.reset(token)

    assert exc_info.value.error.code == 403
    assert "authenticated request tenant" in exc_info.value.error.message
    assert service.calls == []


def test_build_remote_query_request_allows_matching_explicit_tenant() -> None:
    """
    EN: Matching explicit tenant overrides remain valid for authenticated callers.
    CN: 与认证租户一致的显式 tenant 覆盖仍然有效。
    """
    settings = Settings(
        object_state_table="object-state",
        execution_state_table="execution-state",
        manifest_prefix="",
        manifest_index_table=None,
        manifest_bucket=None,
        allow_unauthenticated_query=False,
    )

    request = build_remote_query_request(
        query="hello",
        tenant_id="tenant-a",
        request_tenant_id="tenant-a",
        top_k=1,
        neighbor_expand=0,
        doc_type=None,
        key=None,
        settings=settings,
    )

    assert request.tenant_id == "tenant-a"


def test_build_remote_query_request_rejects_tenant_conflict() -> None:
    """
    EN: Conflicting explicit tenant overrides are rejected before query execution.
    CN: 在查询执行前拒绝冲突的显式 tenant 覆盖。
    """
    settings = Settings(
        object_state_table="object-state",
        execution_state_table="execution-state",
        manifest_prefix="",
        manifest_index_table=None,
        manifest_bucket=None,
        allow_unauthenticated_query=False,
    )

    with pytest.raises(TenantIdConflictError, match="must match the authenticated request tenant"):
        build_remote_query_request(
            query="hello",
            tenant_id="tenant-b",
            request_tenant_id="tenant-a",
            top_k=1,
            neighbor_expand=0,
            doc_type=None,
            key=None,
            settings=settings,
        )


def test_search_documents_requires_tenant_without_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Search documents requires tenant without default.
    CN: 当没有默认 tenant 时，search_documents 需要显式 tenant。
    """
    monkeypatch.setattr(remote_mcp_handler, "load_settings", lambda: _FakeSettings())

    with pytest.raises(ValueError, match="tenant_id is required"):
        remote_mcp_handler.search_documents(query="hello")


def test_search_documents_rejects_blank_query(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Search documents rejects blank query text.
    CN: 验证 search_documents 会拒绝空白 query。
    """
    monkeypatch.setattr(remote_mcp_handler, "load_settings", lambda: _FakeSettings())

    with pytest.raises(ValueError, match="query is required"):
        remote_mcp_handler.search_documents(query=" ")
