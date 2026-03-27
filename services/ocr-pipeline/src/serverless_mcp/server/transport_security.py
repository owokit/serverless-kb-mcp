"""
EN: Transport security settings for the local FastMCP implementation.
CN: 本地 FastMCP 实现的传输安全配置对象。
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class TransportSecuritySettings:
    """EN: Minimal transport security settings container used by tests and local routing.
    CN: 供测试和本地路由使用的最小传输安全配置容器。"""

    enable_dns_rebinding_protection: bool = False
