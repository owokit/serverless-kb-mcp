"""
EN: Smoke tests for the MCP discovery ASGI app and streamable HTTP routing.
CN: MCP discovery ASGI app 与 streamable HTTP 路由的冒烟测试。
"""

from __future__ import annotations

import asyncio
import json

import pytest
from mcp.types import LATEST_PROTOCOL_VERSION
from starlette.testclient import TestClient

from serverless_mcp.domain.models import QueryResponse, QueryResultContext, QueryResultItem, S3ObjectRef
from serverless_mcp.entrypoints import remote_mcp as remote_mcp_handler


class _RecorderApp:
    # EN: ASGI app that records received scopes.
    # CN: 记录接收到的 scope 的 ASGI app。
    def __init__(self) -> None:
        self.scopes: list[dict[str, object]] = []

    async def __call__(self, scope, receive, send) -> None:
        self.scopes.append(scope)


class _FakeSettings:
    def __init__(
        self,
        *,
        allow_unauthenticated_query: bool = True,
        remote_mcp_default_tenant_id: str | None = None,
    ) -> None:
        self.query_max_top_k = 20
        self.query_max_neighbor_expand = 2
        self.allow_unauthenticated_query = allow_unauthenticated_query
        self.query_tenant_claim = "tenant_id"
        self.remote_mcp_default_tenant_id = remote_mcp_default_tenant_id
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


def _run(app, scope: dict[str, object]) -> list[dict[str, object]]:
    messages: list[dict[str, object]] = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        messages.append(message)

    asyncio.run(app(scope, receive, send))
    return messages


def test_plain_get_returns_mcp_discovery_document() -> None:
    """
    EN: Plain get returns mcp discovery document.
    CN: 普通 GET 应返回 MCP discovery 文档。
    """
    app = remote_mcp_handler._McpGatewayASGIApp(_RecorderApp())

    messages = _run(
        app,
        {
            "type": "http",
            "method": "GET",
            "path": "/mcp",
            "raw_path": b"/mcp",
            "headers": [(b"accept", b"text/html")],
        },
    )

    assert messages[0]["status"] == 200
    payload = json.loads(messages[1]["body"].decode("utf-8"))
    assert payload["protocolVersion"] == LATEST_PROTOCOL_VERSION
    assert payload["endpoint"] == "/mcp"
    assert payload["serverInfo"]["name"] == "mcp-doc-pipeline"
    assert payload["tools"][0]["name"] == "search_documents"


def test_streamable_http_get_passes_through_to_mcp_app() -> None:
    """
    EN: Streamable http get passes through to mcp app.
    CN: streamable HTTP GET 应透传到 MCP app。
    """
    recorder = _RecorderApp()
    app = remote_mcp_handler._McpGatewayASGIApp(recorder)

    _run(
        app,
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "raw_path": b"/",
            "headers": [(b"accept", b"text/event-stream")],
        },
    )

    assert recorder.scopes[0]["path"] == "/mcp"
    assert recorder.scopes[0]["raw_path"] == b"/mcp"


def test_streamable_http_initialize_tools_list_and_call(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Official streamable HTTP supports initialize, tools/list, and tools/call.
    CN: 官方 streamable HTTP 需要支持 initialize、tools/list 和 tools/call。
    """
    service = _FakeQueryService()
    monkeypatch.setattr(remote_mcp_handler, "_build_service", lambda: service)
    monkeypatch.setattr(
        remote_mcp_handler,
        "load_settings",
        lambda: _FakeSettings(allow_unauthenticated_query=True, remote_mcp_default_tenant_id="lookup"),
    )
    monkeypatch.setattr(remote_mcp_handler, "_build_delivery_service", lambda: _FakeDeliveryService())

    app = remote_mcp_handler._build_mcp_server().streamable_http_app()
    with TestClient(app) as client:
        headers = {
            "accept": "application/json, text/event-stream",
            "content-type": "application/json",
            "mcp-protocol-version": LATEST_PROTOCOL_VERSION,
        }

        initialize_response = client.post(
            "/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": LATEST_PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {"name": "test", "version": "1.0.0"},
                },
            },
            headers=headers,
        )
        assert initialize_response.status_code == 200
        initialize_payload = initialize_response.json()
        assert initialize_payload["result"]["protocolVersion"] == LATEST_PROTOCOL_VERSION
        assert initialize_payload["result"]["serverInfo"]["name"] == "mcp-doc-pipeline"

        tools_list_response = client.post(
            "/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/list",
                "params": {},
            },
            headers=headers,
        )
        assert tools_list_response.status_code == 200
        tools_payload = tools_list_response.json()
        tool_names = {tool["name"] for tool in tools_payload["result"]["tools"]}
        assert "search_documents" in tool_names

        tools_call_response = client.post(
            "/mcp",
            json={
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
            headers=headers,
        )
        assert tools_call_response.status_code == 200
        tools_call_payload = tools_call_response.json()
        assert tools_call_payload["result"]["isError"] is False
        search_payload = json.loads(tools_call_payload["result"]["content"][0]["text"])
        assert search_payload["query"] == "hello"
        assert search_payload["results"][0]["delivery"]["url"].startswith("https://cdn.example.com/")
        assert service.calls[0]["tenant_id"] == "tenant-a"

        anonymous_tools_call_response = client.post(
            "/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "search_documents",
                    "arguments": {
                        "query": "hello",
                        "top_k": 5,
                        "neighbor_expand": 1,
                    },
                },
            },
            headers=headers,
        )
        assert anonymous_tools_call_response.status_code == 200
        anonymous_tools_call_payload = anonymous_tools_call_response.json()
        assert anonymous_tools_call_payload["result"]["isError"] is False
        anonymous_search_payload = json.loads(anonymous_tools_call_payload["result"]["content"][0]["text"])
        assert anonymous_search_payload["query"] == "hello"
        assert service.calls[1]["tenant_id"] == "lookup"


def test_lambda_handler_can_initialize_twice_without_reusing_stale_streamable_session() -> None:
    """
    EN: Lambda handler can initialize twice without reusing a stale streamable HTTP session manager.
    CN: Lambda 处理器可以连续初始化两次，而不会复用已失效的 streamable HTTP session manager。
    """
    event = {
        "version": "2.0",
        "routeKey": "POST /mcp",
        "rawPath": "/mcp",
        "rawQueryString": "",
        "headers": {
            "accept": "application/json, text/event-stream",
            "content-type": "application/json",
            "mcp-protocol-version": LATEST_PROTOCOL_VERSION,
        },
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
        "body": json.dumps(
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
        "isBase64Encoded": False,
    }

    first_response = remote_mcp_handler.lambda_handler(event, None)
    second_response = remote_mcp_handler.lambda_handler(event, None)

    assert first_response["statusCode"] == 200
    assert second_response["statusCode"] == 200

