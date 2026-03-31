"""
EN: Tests for query-side tenant resolution in the MCP gateway.
CN: 测试 MCP 网关查询侧的 tenant 解析逻辑。
"""

from __future__ import annotations

import pytest

from serverless_mcp.mcp_gateway import auth


def test_resolve_effective_tenant_id_falls_back_to_default_tenant_for_unauthenticated_queries(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Unauthenticated query mode should fall back to the configured default tenant.
    CN: 未认证查询模式应回退到配置的默认 tenant。
    """
    monkeypatch.setattr(auth, "get_request_tenant_id", lambda: None)

    resolved = auth.resolve_effective_tenant_id(
        None,
        allow_unauthenticated_query=True,
        default_tenant_id="lookup",
    )

    assert resolved == "lookup"


def test_resolve_effective_tenant_id_still_requires_tenant_when_unauthenticated_query_is_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Without unauthenticated query mode, tenant resolution should still fail when no tenant is present.
    CN: 禁用未认证查询时，若未提供 tenant，tenant 解析仍应失败。
    """
    monkeypatch.setattr(auth, "get_request_tenant_id", lambda: None)

    with pytest.raises(ValueError, match="tenant_id is required"):
        auth.resolve_effective_tenant_id(
            None,
            allow_unauthenticated_query=False,
            default_tenant_id="lookup",
        )
