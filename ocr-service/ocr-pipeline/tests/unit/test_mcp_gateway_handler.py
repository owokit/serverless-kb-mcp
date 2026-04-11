"""
EN: MCP gateway handler tests that protect discovery routing and request-context propagation.
CN: 保护 discovery 路由和 request-context 传播的 MCP gateway handler 测试。
"""
from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Any

import pytest

from fixtures.events import APIGW_HTTP_GET_MCP, APIGW_POST, APIGW_REST_GET_MCP, APIGW_REST_GET_ROOT, deep_copy_event
from serverless_mcp.mcp_gateway import handler as gateway_handler


class _FakeMcpHandler:
    def __init__(self) -> None:
        self.calls: list[tuple[dict[str, Any], Any]] = []

    def handle_request(self, event: dict[str, Any], context: Any) -> dict[str, Any]:
        self.calls.append((event, context))
        return {
            "statusCode": 201,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"handled": True}),
        }


@contextmanager
def _fake_request_context(event: dict[str, Any]):
    yield


def test_discovery_get_routes_for_root_and_mcp_variants(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gateway_handler, "push_request_context", lambda event: _fake_request_context(event))
    monkeypatch.setattr(gateway_handler, "get_mcp_handler", lambda: _FakeMcpHandler())
    monkeypatch.setattr(gateway_handler, "build_discovery_document", lambda: {"protocolVersion": "2024-11-05", "endpoint": "/mcp"})

    root = gateway_handler.lambda_handler(deep_copy_event(APIGW_REST_GET_ROOT), None)
    mcp = gateway_handler.lambda_handler(deep_copy_event(APIGW_REST_GET_MCP), None)
    http = gateway_handler.lambda_handler(deep_copy_event(APIGW_HTTP_GET_MCP), None)

    for response in (root, mcp, http):
        assert response["statusCode"] == 200
        assert response["headers"]["Content-Type"] == "application/json; charset=utf-8"
        assert response["headers"]["Cache-Control"] == "no-store"
        assert json.loads(response["body"])["endpoint"] == "/mcp"


def test_non_get_requests_do_not_use_discovery_branch(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = _FakeMcpHandler()
    monkeypatch.setattr(gateway_handler, "push_request_context", lambda event: _fake_request_context(event))
    monkeypatch.setattr(gateway_handler, "get_mcp_handler", lambda: handler)

    response = gateway_handler.lambda_handler(deep_copy_event(APIGW_POST), {"request": "context"})

    assert response["statusCode"] == 201
    assert handler.calls == [(APIGW_POST, {"request": "context"})]


def test_request_context_and_http_method_based_discovery_detection(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gateway_handler, "push_request_context", lambda event: _fake_request_context(event))
    handler = _FakeMcpHandler()
    monkeypatch.setattr(gateway_handler, "get_mcp_handler", lambda: handler)

    event = {"requestContext": {"http": {"method": "GET"}}, "rawPath": "/mcp/"}
    response = gateway_handler.lambda_handler(event, None)

    assert response["statusCode"] == 200
    assert handler.calls == []


def test_gateway_handler_preserves_headers_and_body_contract_for_vendored_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = _FakeMcpHandler()
    monkeypatch.setattr(gateway_handler, "push_request_context", lambda event: _fake_request_context(event))
    monkeypatch.setattr(gateway_handler, "get_mcp_handler", lambda: handler)

    response = gateway_handler.lambda_handler(deep_copy_event(APIGW_POST), None)

    assert response["statusCode"] == 201
    assert response["headers"]["Content-Type"] == "application/json"
    assert json.loads(response["body"]) == {"handled": True}
