"""
EN: Smoke tests for the MCP discovery ASGI app and streamable HTTP routing.
CN: 同上。
"""

from __future__ import annotations

import asyncio
import json

from serverless_mcp.entrypoints import remote_mcp as remote_mcp_handler


class _RecorderApp:
    # EN: ASGI app that records received scopes.
    # CN: 璁板綍鎺ユ敹 scope 鐨?ASGI app銆?
    def __init__(self) -> None:
        self.scopes: list[dict[str, object]] = []

    async def __call__(self, scope, receive, send) -> None:
        self.scopes.append(scope)


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
    CN: 同上。
    """
    app = remote_mcp_handler._McpDiscoveryASGIApp(_RecorderApp())

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
    assert payload["protocolVersion"] == "2024-11-05"
    assert payload["endpoint"] == "/mcp"
    assert payload["serverInfo"]["name"] == "mcp-doc-pipeline"
    assert payload["tools"][0]["name"] == "search_documents"


def test_streamable_http_get_passes_through_to_mcp_app() -> None:
    """
    EN: Streamable http get passes through to mcp app.
    CN: 楠岃瘉 streamable HTTP GET 閫忎紶鍒?MCP app銆?
    """
    recorder = _RecorderApp()
    app = remote_mcp_handler._McpDiscoveryASGIApp(recorder)

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

