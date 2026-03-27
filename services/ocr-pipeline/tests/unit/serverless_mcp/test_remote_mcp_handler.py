"""
EN: Tests for the remote MCP handler including search_documents, tenant resolution, and ASGI scope normalization.
CN: 同上。
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from serverless_mcp.entrypoints import remote_mcp as remote_mcp_handler
from serverless_mcp.domain.models import QueryResponse, QueryResultContext, QueryResultItem, S3ObjectRef


@dataclass(frozen=True)
class _FakeSettings:
    # EN: Frozen dataclass stand-in for Settings.
    # CN: Settings 的冻结 dataclass 替身。
    query_max_top_k: int = 20
    query_max_neighbor_expand: int = 2
    allow_unauthenticated_query: bool = False
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
    CN: 验证 search_documents 拒绝超出范围的 top_k。
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
    CN: 同上。
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


def test_search_documents_forwards_authenticated_request_security_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Search documents forwards the authenticated request security scope into the query request.
    CN: search_documents 会把已认证请求的 security scope 透传到 query request。
    """
    service = _FakeQueryService()
    monkeypatch.setattr(remote_mcp_handler, "_build_service", lambda: service)
    monkeypatch.setattr(remote_mcp_handler, "load_settings", lambda: _FakeSettings())
    monkeypatch.setattr(remote_mcp_handler, "_build_delivery_service", lambda: _FakeDeliveryService())

    token = remote_mcp_handler._REQUEST_SECURITY_SCOPE.set(("team-a", "team-b"))
    try:
        payload = remote_mcp_handler.search_documents(query="hello", tenant_id="tenant-a")
    finally:
        remote_mcp_handler._REQUEST_SECURITY_SCOPE.reset(token)

    assert payload["query"] == "hello"
    assert service.calls[0]["security_scope"] == ("team-a", "team-b")


def test_search_documents_requires_tenant_without_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Search documents requires tenant without default.
    CN: 同上。
    """
    monkeypatch.setattr(remote_mcp_handler, "load_settings", lambda: _FakeSettings())

    with pytest.raises(ValueError, match="tenant_id is required"):
        remote_mcp_handler.search_documents(query="hello")


def test_extract_request_tenant_id_reads_jwt_claims(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Remote MCP request tenant extraction reads JWT claims from the attached AWS event.
    CN: 同上。
    """
    monkeypatch.setattr(remote_mcp_handler, "load_settings", lambda: _FakeSettings())
    scope = {
        "aws.event": {
            "requestContext": {
                "authorizer": {
                    "jwt": {
                        "claims": {
                            "tenant_id": "tenant-from-claim",
                        }
                    }
                }
            }
        }
    }

    assert remote_mcp_handler._extract_request_tenant_id(scope) == "tenant-from-claim"


def test_extract_request_security_scope_reads_jwt_claims(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Remote MCP request security scope extraction reads JWT claims from the attached AWS event.
    CN: 远程 MCP 请求的 security scope 提取会读取附着 AWS event 中的 JWT 声明。
    """
    scope = {
        "aws.event": {
            "requestContext": {
                "authorizer": {
                    "jwt": {
                        "claims": {
                            "security_scope": "team-a team-b",
                        }
                    }
                }
            }
        }
    }

    assert remote_mcp_handler._extract_request_security_scope(scope) == ("team-a", "team-b")


def test_search_documents_rejects_blank_query(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Search documents rejects blank query text.
    CN: 验证 search_documents 会拒绝空白 query。
    """
    monkeypatch.setattr(remote_mcp_handler, "load_settings", lambda: _FakeSettings())

    with pytest.raises(ValueError, match="query is required"):
        remote_mcp_handler.search_documents(query=" ")


def test_lambda_handler_delegates_to_asgi_bridge(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Lambda handler delegates to asgi bridge.
    CN: 楠岃瘉 Lambda handler 濮旀墭缁?ASGI bridge銆?
    """
    captured: dict[str, object] = {}

    def fake_asgi_handler(event, context):
        captured["event"] = event
        captured["context"] = context
        return {"statusCode": 200}

    monkeypatch.setattr(remote_mcp_handler, "_build_asgi_handler", lambda: fake_asgi_handler)

    event = {"version": "2.0", "requestContext": {"http": {"method": "POST"}}}
    context = object()

    result = remote_mcp_handler.lambda_handler(event, context)

    assert result == {"statusCode": 200}
    assert captured["event"] is event
    assert captured["context"] is context


def test_normalize_mcp_scope_rewrites_root_path() -> None:
    """
    EN: Normalize mcp scope rewrites root path.
    CN: 楠岃瘉 _normalize_mcp_scope 閲嶅啓鏍硅矾寰勩€?
    """
    normalized = remote_mcp_handler._normalize_mcp_scope({"path": "/", "raw_path": b"/"})

    assert normalized["path"] == "/mcp"
    assert normalized["raw_path"] == b"/mcp"


def test_normalize_mcp_scope_rewrites_trailing_slash_path() -> None:
    """
    EN: Normalize mcp scope rewrites trailing slash path.
    CN: 同上。
    """
    normalized = remote_mcp_handler._normalize_mcp_scope({"path": "/mcp/", "raw_path": b"/mcp/"})

    assert normalized["path"] == "/mcp"
    assert normalized["raw_path"] == b"/mcp"


def test_normalize_mcp_scope_keeps_existing_mcp_path() -> None:
    """
    EN: Normalize mcp scope keeps existing mcp path.
    CN: 同上。
    """
    scope = {"path": "/mcp", "raw_path": b"/mcp"}

    normalized = remote_mcp_handler._normalize_mcp_scope(scope)

    assert normalized is scope


def test_build_mcp_server_uses_open_streamable_http() -> None:
    """
    EN: Build mcp server uses open streamable http.
    CN: 同上。
    """
    server = remote_mcp_handler._build_mcp_server()

    assert server.settings.json_response is True
    assert server.settings.stateless_http is True
    assert server.settings.transport_security is not None
    assert server.settings.transport_security.enable_dns_rebinding_protection is True
