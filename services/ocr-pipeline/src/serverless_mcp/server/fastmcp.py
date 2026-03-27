"""
EN: Lightweight FastMCP implementation for the local repository layout.
CN: 适配当前仓库布局的轻量 FastMCP 实现。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable

from .transport_security import TransportSecuritySettings


@dataclass(slots=True)
class FastMCPSettings:
    """EN: Configuration snapshot exposed to the remote MCP handler tests.
    CN: 暴露给远程 MCP 处理器测试的配置快照。"""

    json_response: bool
    stateless_http: bool
    transport_security: TransportSecuritySettings | None


class FastMCP:
    """EN: Minimal in-repo FastMCP stand-in that preserves the tested surface.
    CN: 保留被测试表面的最小仓库内 FastMCP 替身。"""

    def __init__(
        self,
        name: str,
        *,
        instructions: str | None = None,
        json_response: bool = False,
        stateless_http: bool = False,
        transport_security: TransportSecuritySettings | None = None,
    ) -> None:
        self.name = name
        self.instructions = instructions
        self.settings = FastMCPSettings(
            json_response=json_response,
            stateless_http=stateless_http,
            transport_security=transport_security,
        )
        self._tools: dict[str, dict[str, Any]] = {}

    def tool(self, name: str | None = None, description: str | None = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """EN: Register a callable as an MCP tool and return it unchanged.
        CN: 将可调用对象注册为 MCP 工具，并保持原样返回。"""

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            tool_name = name or getattr(fn, "__name__", "tool")
            self._tools[tool_name] = {
                "callable": fn,
                "description": description,
            }
            return fn

        return decorator

    def streamable_http_app(self):
        """EN: Return a minimal ASGI app for streamable HTTP probes.
        CN: 返回一个用于 streamable HTTP 探测的最小 ASGI 应用。"""

        async def app(scope: dict[str, Any], receive, send) -> None:
            if scope.get("type") != "http":
                raise RuntimeError("streamable_http_app only handles HTTP scopes")

            body = json.dumps(
                {
                    "name": self.name,
                    "instructions": self.instructions,
                    "tools": sorted(self._tools),
                },
                ensure_ascii=False,
            ).encode("utf-8")
            headers = [
                (b"content-type", b"application/json; charset=utf-8"),
                (b"content-length", str(len(body)).encode("ascii")),
            ]
            await send({"type": "http.response.start", "status": 200, "headers": headers})
            await send({"type": "http.response.body", "body": body, "more_body": False})

        return app
