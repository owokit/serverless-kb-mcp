"""
EN: Shared request parsing helpers for the backfill service entrypoint.
CN: backfill 服务入口共享的请求解析辅助工具。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from serverless_mcp.core.serialization import coerce_bounded_int
from serverless_mcp.runtime.config import Settings, load_settings
from serverless_mcp.runtime.embedding_profiles import get_write_profiles


@dataclass(frozen=True, slots=True)
class BackfillRequest:
    """
    EN: Normalized backfill request including optional resume controls.
    CN: 包含可选恢复控制项的规范化 backfill 请求。
    """

    profile_id: str
    trace_id: str
    force: bool = False
    resume_after_object_pk: str | None = None
    max_records: int | None = None


def build_backfill_request(event: dict[str, Any], *, settings: Settings | None = None) -> BackfillRequest:
    """
    EN: Parse the backfill invocation payload into a stable job request contract.
    CN: 将 backfill 调用负载解析为稳定的作业请求契约。
    """
    active_settings = settings or load_settings()
    profile_id = str(event.get("profile_id") or "").strip()
    if not profile_id:
        profiles = list(get_write_profiles(active_settings))
        if len(profiles) != 1:
            raise ValueError("profile_id is required when more than one writable embedding profile is configured")
        profile_id = profiles[0].profile_id

    trace_id = str(event.get("trace_id") or f"backfill-{profile_id}")
    resume_after_object_pk = _optional_str(event.get("resume_after_object_pk") or event.get("resume_after"))
    max_records = _optional_int(event.get("max_records"))
    return BackfillRequest(
        profile_id=profile_id,
        trace_id=trace_id,
        force=_parse_bool(event.get("force", False)),
        resume_after_object_pk=resume_after_object_pk,
        max_records=max_records,
    )


def _parse_bool(value: object) -> bool:
    """
    EN: Parse a boolean-like value from various input types with safe defaults.
    CN: 使用安全默认值解析各种输入类型中的布尔风格值。
    """
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _optional_str(value: object) -> str | None:
    """
    EN: Convert a value to a stripped string or return None when absent.
    CN: 将值转换为去除首尾空白的字符串，缺失时返回 None。
    """
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _optional_int(value: object) -> int | None:
    """
    EN: Convert a value to a positive integer or return None when absent.
    CN: 将值转换为正整数，缺失时返回 None。
    """
    if value in {None, ""}:
        return None
    return coerce_bounded_int(value, field_name="max_records", minimum=1, maximum=100_000)
