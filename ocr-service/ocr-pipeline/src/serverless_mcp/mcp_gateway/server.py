"""
EN: MCP server assembly for the query-side gateway.
CN: 查询侧网关的 MCP server 装配。
"""
from __future__ import annotations

import os
from functools import lru_cache
from typing import Any

from awslabs.mcp_lambda_handler import MCPLambdaHandler
from awslabs.mcp_lambda_handler.session import NoOpSessionStore

from serverless_mcp.mcp_gateway.tools.get_document_excerpt import get_document_excerpt
from serverless_mcp.mcp_gateway.tools.get_ingestion_status import get_ingestion_status
from serverless_mcp.mcp_gateway.tools.list_document_versions import list_document_versions
from serverless_mcp.mcp_gateway.tools.search_documents import search_documents

SERVER_NAME = "mcp-doc-pipeline"
SERVER_VERSION = "1.0.0"
# EN: Mirror the vendored handler's initialize response protocolVersion.
# CN: 该常量必须与 vendored handler 的 initialize 响应 protocolVersion 保持一致。
MCP_PROTOCOL_VERSION = "2024-11-05"


@lru_cache(maxsize=1)
def get_mcp_handler() -> MCPLambdaHandler:
    """
    EN: Build and cache the vendored AWS Labs MCP Lambda handler.
    CN: 构建并缓存 vendored 的 AWS Labs MCP Lambda handler。
    """
    handler = MCPLambdaHandler(name=SERVER_NAME, version=SERVER_VERSION, session_store=_build_session_store())
    handler.tool()(search_documents)
    handler.tool()(get_document_excerpt)
    handler.tool()(list_document_versions)
    handler.tool()(get_ingestion_status)
    return handler


def build_discovery_document() -> dict[str, Any]:
    """
    EN: Build a compact human-readable discovery document for GET probes.
    CN: 为 GET 探针构建紧凑的人类可读 discovery 文档。
    """
    return {
        "protocolVersion": MCP_PROTOCOL_VERSION,
        "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
        "capabilities": {
            "tools": {"list": True, "call": True},
        },
        "transport": "streamable-http",
        "endpoint": "/mcp",
        "instructions": (
            "Query-side MCP gateway for semantic document search, document excerpts, "
            "document version discovery, and ingestion status."
        ),
        "tools": [
            {
                "name": "search_documents",
                "description": "Search documents through the retrieval pipeline.",
            },
            {
                "name": "get_document_excerpt",
                "description": "Return a manifest-backed excerpt for one document version.",
            },
            {
                "name": "list_document_versions",
                "description": "List known versions for one source object and attach status snapshots.",
            },
            {
                "name": "get_ingestion_status",
                "description": "Return the ingestion status snapshot for one source object version.",
            },
        ],
    }


def _build_session_store() -> object:
    """
    EN: Choose the stateless-first session store, with optional DynamoDB support.
    CN: 选择 stateless-first 的 session store，并保留可选 DynamoDB 支持。
    """
    session_table = (os.environ.get("MCP_SESSION_TABLE") or os.environ.get("REMOTE_MCP_SESSION_TABLE") or "").strip()
    if session_table:
        return session_table
    return NoOpSessionStore()
