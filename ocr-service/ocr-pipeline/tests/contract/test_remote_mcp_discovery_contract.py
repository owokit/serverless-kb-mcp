"""
EN: Remote MCP discovery contract tests that pin public GET probe semantics.
CN: 锁定公开 GET 探针语义的 remote MCP discovery 契约测试。
"""
from __future__ import annotations

import json

from fixtures.events import APIGW_HTTP_GET_MCP, APIGW_REST_GET_MCP, APIGW_REST_GET_ROOT, deep_copy_event
from serverless_mcp.mcp_gateway import handler as gateway_handler


def test_discovery_response_contract_contains_stable_headers_and_document_shape() -> None:
    response = gateway_handler.lambda_handler(deep_copy_event(APIGW_REST_GET_ROOT), None)
    document = json.loads(response["body"])

    assert response["statusCode"] == 200
    assert response["headers"] == {
        "Content-Type": "application/json; charset=utf-8",
        "Cache-Control": "no-store",
    }
    assert document["protocolVersion"] == "2024-11-05"
    assert document["serverInfo"]["name"] == "mcp-doc-pipeline"
    assert document["endpoint"] == "/mcp"
    assert {tool["name"] for tool in document["tools"]} == {
        "search_documents",
        "get_document_excerpt",
        "list_document_versions",
        "get_ingestion_status",
    }


def test_discovery_accepts_rest_and_http_api_get_variants() -> None:
    for event in (deep_copy_event(APIGW_REST_GET_MCP), deep_copy_event(APIGW_HTTP_GET_MCP)):
        response = gateway_handler.lambda_handler(event, None)
        assert response["statusCode"] == 200
        assert json.loads(response["body"])["endpoint"] == "/mcp"
