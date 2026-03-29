"""
EN: Shared payload helpers for MCP gateway tool responses.
CN: MCP 网关 tool 响应的共享载荷辅助工具。
"""
from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from typing import Any


def dump_json_payload(payload: Any) -> str:
    """
    EN: Serialize a tool payload to UTF-8 JSON text.
    CN: 将 tool 载荷序列化为 UTF-8 JSON 文本。
    """
    return json.dumps(payload, ensure_ascii=False, default=_json_default)


def _json_default(value: Any) -> Any:
    """
    EN: Convert dataclass payloads and sets into JSON-safe values.
    CN: 将 dataclass 载荷和集合转换为 JSON 安全值。
    """
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, set):
        return sorted(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")

