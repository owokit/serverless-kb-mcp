"""
EN: Smoke tests for the MCP discovery document, Lambda handler, and tool contract.
CN: MCP discovery 文档、Lambda handler 和 tool 契约的冒烟测试。
"""

from __future__ import annotations

import json
from types import SimpleNamespace

from mcp.types import LATEST_PROTOCOL_VERSION

from serverless_mcp.domain.models import (
    ChunkManifest,
    ExtractedChunk,
    QueryResponse,
    QueryResultContext,
    QueryResultItem,
    S3ObjectRef,
)
from serverless_mcp.entrypoints import remote_mcp as remote_mcp_handler
import serverless_mcp.mcp_gateway.tools.get_document_excerpt as excerpt_tool_module
import serverless_mcp.mcp_gateway.tools.get_ingestion_status as status_tool_module
import serverless_mcp.mcp_gateway.tools.list_document_versions as versions_tool_module
import serverless_mcp.mcp_gateway.tools.search_documents as search_tool_module


class _FakeSettings:
    def __init__(self) -> None:
        self.query_max_top_k = 20
        self.query_max_neighbor_expand = 2
        self.allow_unauthenticated_query = True
        self.query_tenant_claim = "tenant_id"
        self.remote_mcp_default_tenant_id = "lookup"
        self.cloudfront_distribution_domain = "cdn.example.com"
        self.cloudfront_key_pair_id = "K123"
        self.cloudfront_private_key_pem = "-----BEGIN PRIVATE KEY-----\nFAKE\n-----END PRIVATE KEY-----"
        self.cloudfront_url_ttl_seconds = 900


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
                    manifest_s3_uri="s3://manifests/manifests/guide.json",
                    metadata={"doc_type": "pdf", "__fusion_score__": 0.75, "__profile_hits__": 2},
                    match=QueryResultContext(
                        chunk_id="chunk#1",
                        chunk_type="page_text_chunk",
                        text="hello world",
                    ),
                )
            ],
        )


class _FakeDeliveryService:
    def deliver_source_document(self, source):
        return SimpleNamespace(
            url=f"https://cdn.example.com/documents/{source.bucket}/{source.key}?versionId={source.version_id}",
            expires_at="2026-03-17T00:00:00+00:00",
        )


class _FakeManifestRepo:
    def load_manifest(self, manifest_s3_uri: str):
        return ChunkManifest(
            source=S3ObjectRef(
                tenant_id="tenant-a",
                bucket="bucket-a",
                key="docs/guide.pdf",
                version_id="v2",
            ),
            doc_type="pdf",
            chunks=[
                ExtractedChunk(
                    chunk_id="chunk#1",
                    chunk_type="page_text_chunk",
                    text="first chunk text",
                    doc_type="pdf",
                    token_estimate=12,
                ),
                ExtractedChunk(
                    chunk_id="chunk#2",
                    chunk_type="page_text_chunk",
                    text="second chunk text",
                    doc_type="pdf",
                    token_estimate=14,
                ),
            ],
        )


class _FakeStatusService:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def build_status(self, request):
        if isinstance(request, dict):
            bucket = request["bucket"]
            key = request["key"]
            version_id = request.get("version_id")
            tenant_id = request.get("tenant_id")
        else:
            bucket = request.bucket
            key = request.key
            version_id = request.version_id
            tenant_id = request.tenant_id
        self.calls.append(
            {
                "bucket": bucket,
                "key": key,
                "version_id": version_id,
                "tenant_id": tenant_id,
            }
        )
        return {
            "bucket": bucket,
            "key": key,
            "version_id": version_id or "v2",
            "tenant_id": tenant_id,
            "overall_status": "INDEXED",
            "manifest": {"manifest_s3_uri": "s3://manifests/manifests/guide.json"},
            "lookup": {"object_pk": "tenant-a#bucket-a#docs/guide.pdf"},
        }


class _FakeS3Client:
    def list_object_versions(self, **kwargs):
        return {
            "Versions": [
                {
                    "Key": kwargs["Prefix"],
                    "VersionId": "v2",
                    "IsLatest": True,
                    "LastModified": "2026-03-01T00:00:00+00:00",
                    "Size": 111,
                    "ETag": '"etag-2"',
                },
                {
                    "Key": kwargs["Prefix"],
                    "VersionId": "v1",
                    "IsLatest": False,
                    "LastModified": "2026-02-01T00:00:00+00:00",
                    "Size": 90,
                    "ETag": '"etag-1"',
                },
            ],
            "DeleteMarkers": [
                {
                    "Key": kwargs["Prefix"],
                    "VersionId": "v0",
                    "IsLatest": False,
                    "LastModified": "2026-01-01T00:00:00+00:00",
                }
            ],
        }


def _build_gateway_context() -> SimpleNamespace:
    return SimpleNamespace(
        settings=_FakeSettings(),
        query_service=_FakeQueryService(),
        status_service=_FakeStatusService(),
        manifest_repo=_FakeManifestRepo(),
        s3_client=_FakeS3Client(),
        delivery_service=_FakeDeliveryService(),
        object_state_repo=None,
        execution_state_repo=None,
        projection_state_repo=None,
    )


def _install_gateway_context(monkeypatch) -> SimpleNamespace:
    context = _build_gateway_context()
    monkeypatch.setattr(search_tool_module, "get_gateway_context", lambda: context)
    monkeypatch.setattr(excerpt_tool_module, "get_gateway_context", lambda: context)
    monkeypatch.setattr(versions_tool_module, "get_gateway_context", lambda: context)
    monkeypatch.setattr(status_tool_module, "get_gateway_context", lambda: context)
    return context


def _build_api_gateway_event(body: dict[str, object], *, session_id: str | None = None) -> dict[str, object]:
    headers = {
        "accept": "application/json, text/event-stream",
        "content-type": "application/json; charset=utf-8",
        "mcp-protocol-version": LATEST_PROTOCOL_VERSION,
    }
    if session_id:
        headers["mcp-session-id"] = session_id
    return {
        "version": "2.0",
        "routeKey": "POST /mcp",
        "rawPath": "/mcp",
        "rawQueryString": "",
        "headers": headers,
        "requestContext": {
            "accountId": "123456789012",
            "apiId": "test",
            "domainName": "example.execute-api.us-east-1.amazonaws.com",
            "domainPrefix": "example",
            "http": {
                "method": "POST",
                "path": "/mcp",
                "protocol": "HTTP/1.1",
                "sourceIp": "127.0.0.1",
                "userAgent": "pytest",
            },
            "requestId": "request-id",
            "routeKey": "POST /mcp",
            "stage": "$default",
            "time": "28/Mar/2026:00:00:00 +0000",
            "timeEpoch": 1774665600000,
        },
        "body": json.dumps(body),
        "isBase64Encoded": False,
    }


def test_discovery_get_returns_query_gateway_document() -> None:
    """
    EN: Plain GET probes should return the lightweight discovery document.
    CN: 普通 GET 探针应返回轻量 discovery 文档。
    """
    response = remote_mcp_handler.lambda_handler(
        {
            "httpMethod": "GET",
            "path": "/mcp",
            "headers": {"accept": "application/json"},
        },
        None,
    )

    assert response["statusCode"] == 200
    payload = json.loads(response["body"])
    assert payload["protocolVersion"] == LATEST_PROTOCOL_VERSION
    assert payload["endpoint"] == "/mcp"
    assert payload["serverInfo"]["name"] == "mcp-doc-pipeline"
    assert [tool["name"] for tool in payload["tools"]] == [
        "search_documents",
        "get_document_excerpt",
        "list_document_versions",
        "get_ingestion_status",
    ]


def test_lambda_handler_initialize_tools_list_and_call(monkeypatch) -> None:
    """
    EN: The vendored MCP handler should support initialize, tools/list, and tools/call.
    CN: vendored MCP handler 应支持 initialize、tools/list 和 tools/call。
    """
    context = _install_gateway_context(monkeypatch)

    initialize_response = remote_mcp_handler.lambda_handler(
        _build_api_gateway_event(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": LATEST_PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {"name": "pytest", "version": "1.0.0"},
                },
            }
        ),
        None,
    )

    assert initialize_response["statusCode"] == 200
    assert initialize_response["headers"]["MCP-Version"] == "0.6"
    session_id = initialize_response["headers"]["MCP-Session-Id"]
    initialize_payload = json.loads(initialize_response["body"])
    assert initialize_payload["result"]["protocolVersion"] == "2024-11-05"
    assert initialize_payload["result"]["serverInfo"]["name"] == "mcp-doc-pipeline"
    assert initialize_payload["result"]["capabilities"]["tools"] == {"list": True, "call": True}

    tools_list_response = remote_mcp_handler.lambda_handler(
        _build_api_gateway_event(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/list",
                "params": {},
            },
            session_id=session_id,
        ),
        None,
    )

    assert tools_list_response["statusCode"] == 200
    tools_payload = json.loads(tools_list_response["body"])
    tool_names = {tool["name"] for tool in tools_payload["result"]["tools"]}
    assert tool_names == {
        "search_documents",
        "get_document_excerpt",
        "list_document_versions",
        "get_ingestion_status",
    }

    tools_call_response = remote_mcp_handler.lambda_handler(
        _build_api_gateway_event(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "search_documents",
                    "arguments": {
                        "query": "hello",
                        "tenant_id": "tenant-a",
                        "top_k": 5,
                        "neighbor_expand": 1,
                    },
                },
            },
            session_id=session_id,
        ),
        None,
    )

    assert tools_call_response["statusCode"] == 200
    tools_call_payload = json.loads(tools_call_response["body"])
    content = json.loads(tools_call_payload["result"]["content"][0]["text"])
    assert content["query"] == "hello"
    assert content["results"][0]["delivery"]["url"].startswith("https://cdn.example.com/")
    assert context.query_service.calls[0]["tenant_id"] == "tenant-a"
    assert context.query_service.calls[0]["security_scope"] == ()


def test_gateway_tools_return_business_payloads(monkeypatch) -> None:
    """
    EN: Query-side tools should return business payloads without exposing storage internals.
    CN: 查询侧 tools 应返回业务载荷，而不是暴露存储内部细节。
    """
    context = _install_gateway_context(monkeypatch)

    search_payload = json.loads(
        search_tool_module.search_documents(
            query="hello",
            tenant_id="tenant-a",
            top_k=5,
            neighbor_expand=1,
        )
    )
    excerpt_payload = json.loads(
        excerpt_tool_module.get_document_excerpt(
            bucket="bucket-a",
            key="docs/guide.pdf",
            version_id="v2",
            tenant_id="tenant-a",
            max_chunks=2,
            max_chars=4000,
        )
    )
    versions_payload = json.loads(
        versions_tool_module.list_document_versions(
            bucket="bucket-a",
            key="docs/guide.pdf",
            tenant_id="tenant-a",
            limit=2,
        )
    )
    status_payload = json.loads(
        status_tool_module.get_ingestion_status(
            bucket="bucket-a",
            key="docs/guide.pdf",
            version_id="v2",
            tenant_id="tenant-a",
        )
    )

    assert search_payload["query"] == "hello"
    assert search_payload["results"][0]["delivery"]["expires_at"] == "2026-03-17T00:00:00+00:00"
    assert "vector_bucket_name" not in search_payload

    assert excerpt_payload["manifest_s3_uri"] == "s3://manifests/manifests/guide.json"
    assert excerpt_payload["excerpt"] == "first chunk text\n\nsecond chunk text"
    assert [chunk["chunk_id"] for chunk in excerpt_payload["chunks"]] == ["chunk#1", "chunk#2"]
    assert excerpt_payload["status"]["overall_status"] == "INDEXED"

    assert versions_payload["bucket"] == "bucket-a"
    assert [version["version_id"] for version in versions_payload["versions"]] == ["v2", "v1"]
    assert versions_payload["versions"][0]["status"]["overall_status"] == "INDEXED"
    assert versions_payload["versions"][1]["is_delete_marker"] is False

    assert status_payload["overall_status"] == "INDEXED"
    assert status_payload["bucket"] == "bucket-a"

    assert context.status_service.calls[0]["tenant_id"] == "tenant-a"
