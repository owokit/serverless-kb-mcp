"""
EN: Status mapping helpers for stage, profile, and overall progress calculation.
CN: 用于阶段、profile 和整体进度计算的状态映射辅助函数。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any


def build_profile_rows(projection_records: list[dict[str, Any]], manifest_summary: dict[str, Any] | None) -> list[dict[str, Any]]:
    """
    EN: Build per-profile status rows with mapped status and progress percentage.
    CN: 构建按 profile 分组的状态行，并附带映射后的状态与进度百分比。
    """
    if not projection_records:
        return []
    total_items = int(manifest_summary.get("embedding_item_count", 0)) if manifest_summary else 0
    rows: list[dict[str, Any]] = []
    for record in projection_records:
        rows.append(
            {
                **record,
                "status": map_profile_status(record),
                "progress_percent": profile_progress(record, total_items),
            }
        )
    rows.sort(key=lambda item: (str(item.get("profile_id") or ""), str(item.get("provider") or "")))
    return rows


def build_stage_rows(
    *,
    lookup_present: bool,
    current_state: object | None,
    manifest_summary: dict[str, Any] | None,
    profile_rows: list[dict[str, Any]],
    source_info: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """
    EN: Build pipeline stage rows for source, ingest, extract, manifest, and embedding.
    CN: 构建 source、ingest、extract、manifest 和 embedding 的流水线阶段行。
    """
    current_extract_status = str(getattr(current_state, "extract_status", "") or "") if current_state else ""
    current_embed_status = str(getattr(current_state, "embed_status", "") or "") if current_state else ""

    source_stage = {
        "name": "source",
        "status": "UPLOADED" if source_info else "NOT_FOUND",
        "progress_percent": 100 if source_info else 0,
    }
    ingest_stage = {
        "name": "ingest",
        "status": "DONE" if lookup_present else "PENDING",
        "progress_percent": 100 if lookup_present else 0,
    }
    extract_stage = {
        "name": "extract",
        "status": map_extract_status(current_extract_status, lookup_present),
        "progress_percent": extract_progress(current_extract_status, manifest_summary is not None),
    }
    manifest_stage = {
        "name": "manifest",
        "status": map_manifest_status(manifest_summary, lookup_present),
        "progress_percent": 100 if manifest_summary and not manifest_summary.get("load_failed") else (50 if current_extract_status == "EXTRACTED" else 0),
    }
    embedding_stage = {
        "name": "embedding",
        "status": map_embedding_status(current_embed_status, profile_rows),
        "progress_percent": embedding_progress(profile_rows, current_embed_status),
    }
    return [source_stage, ingest_stage, extract_stage, manifest_stage, embedding_stage]


def resolve_overall_status(
    *,
    lookup_present: bool,
    current_state: object | None,
    manifest_summary: dict[str, Any] | None,
    profile_rows: list[dict[str, Any]],
    source_info: dict[str, Any] | None,
) -> str:
    """
    EN: Compute the overall pipeline status from state, manifest, and profile rows.
    CN: 根据状态、manifest 和 profile 行计算整体流水线状态。
    """
    if current_state is not None and bool(getattr(current_state, "is_deleted", False)):
        return "DELETED"
    if not lookup_present and source_info is None:
        return "NOT_FOUND"
    if manifest_summary and manifest_summary.get("load_failed"):
        return "MANIFEST_FAILED"
    if profile_rows and any(row.get("status") == "FAILED" for row in profile_rows):
        return "FAILED"
    if profile_rows and all(row.get("status") == "INDEXED" for row in profile_rows):
        return "DONE"
    extract_status = str(getattr(current_state, "extract_status", "") or "") if current_state else ""
    embed_status = str(getattr(current_state, "embed_status", "") or "") if current_state else ""
    if extract_status == "QUEUED":
        return "QUEUED"
    if extract_status == "EXTRACTING":
        return "EXTRACTING"
    if extract_status == "EXTRACTED" and manifest_summary is None:
        return "MANIFESTING"
    if manifest_summary is not None and (profile_rows or embed_status in {"EMBEDDING", "INDEXED"}):
        if profile_rows:
            return "EMBEDDING" if any(row.get("status") != "INDEXED" for row in profile_rows) else "DONE"
        return "DONE" if embed_status == "INDEXED" else "EMBEDDING"
    if lookup_present:
        if manifest_summary is not None:
            return "EMBEDDING"
        return "UPLOADED" if current_state is None else "QUEUED"
    return "UPLOADED"


def resolve_progress_percent(stage_rows: list[dict[str, Any]]) -> int:
    """
    EN: Compute a weighted composite progress percent across all pipeline stages.
    CN: 计算所有流水线阶段的加权综合进度百分比。
    """
    weights = {"source": 5, "ingest": 10, "extract": 35, "manifest": 10, "embedding": 40}
    total = 0.0
    for row in stage_rows:
        stage = str(row.get("name") or "")
        weight = weights.get(stage, 0)
        total += weight * (int(row.get("progress_percent") or 0) / 100.0)
    return int(round(min(total, 100.0)))


def resolve_current_stage(stage_rows: list[dict[str, Any]]) -> str:
    """
    EN: Find the first stage that is not yet completed.
    CN: 找到第一个尚未完成的阶段。
    """
    for stage in ("source", "ingest", "extract", "manifest", "embedding"):
        for row in stage_rows:
            if row.get("name") == stage:
                status = str(row.get("status") or "")
                if status not in {"DONE", "UPLOADED"}:
                    return stage
    return "done"


def latest_timestamp(*values: str | None) -> str | None:
    """
    EN: Return the latest ISO 8601 timestamp from a set of candidate values.
    CN: 从一组候选值中返回最新的 ISO 8601 时间戳。
    """
    parsed: list[datetime] = []
    for value in values:
        if not value:
            continue
        try:
            parsed.append(datetime.fromisoformat(value.replace("Z", "+00:00")))
        except ValueError:
            continue
    if not parsed:
        return next((value for value in values if value), None)
    return max(parsed).isoformat()


def extract_progress(extract_status: str, has_manifest: bool) -> int:
    """
    EN: Map extract_status to a progress percentage for the extract stage.
    CN: 将 extract_status 映射为 extract 阶段的进度百分比。
    """
    if has_manifest or extract_status == "EXTRACTED":
        return 100
    if extract_status == "EXTRACTING":
        return 55
    if extract_status == "QUEUED":
        return 15
    if extract_status == "FAILED":
        return 100
    return 0


def embedding_progress(profile_rows: list[dict[str, Any]], embed_status: str) -> int:
    """
    EN: Compute embedding stage progress, averaging per-profile progress or using embed_status.
    CN: 计算 embedding 阶段进度，优先平均各 profile 进度，否则使用 embed_status。
    """
    if profile_rows:
        total = sum(int(row.get("progress_percent") or 0) for row in profile_rows)
        return int(round(total / max(len(profile_rows), 1)))
    if embed_status == "INDEXED":
        return 100
    if embed_status == "EMBEDDING":
        return 55
    if embed_status == "FAILED":
        return 100
    return 0


def profile_progress(record: dict[str, Any], total_items: int) -> int:
    """
    EN: Compute per-profile embedding progress from vector_count or mapped status.
    CN: 根据 vector_count 或映射后的状态计算单个 profile 的 embedding 进度。
    """
    status = str(record.get("status") or "")
    vector_count = record.get("vector_count")
    if status in {"INDEXED", "FAILED", "DELETED"}:
        return 100
    if total_items > 0 and isinstance(vector_count, int):
        return max(0, min(100, int(round(vector_count / total_items * 100))))
    if status == "EMBEDDING":
        return 60
    if status == "PENDING":
        return 0
    return 20 if vector_count else 0


def map_extract_status(extract_status: str, lookup_present: bool) -> str:
    """
    EN: Map internal extract_status to a display-friendly stage status string.
    CN: 将内部 extract_status 映射为对外显示的阶段状态字符串。
    """
    if extract_status == "EXTRACTING":
        return "EXTRACTING"
    if extract_status == "EXTRACTED":
        return "DONE"
    if extract_status == "FAILED":
        return "FAILED"
    if extract_status == "QUEUED":
        return "QUEUED" if lookup_present else "PENDING"
    return "PENDING" if lookup_present else "WAITING"


def map_embedding_status(embed_status: str, profile_rows: list[dict[str, Any]]) -> str:
    """
    EN: Map embedding status from profile rows or legacy embed_status field.
    CN: 从 profile 行或旧版 embed_status 字段映射 embedding 状态。
    """
    if profile_rows:
        if any(row.get("status") == "FAILED" for row in profile_rows):
            return "FAILED"
        if all(row.get("status") == "INDEXED" for row in profile_rows):
            return "DONE"
        if any(row.get("status") == "EMBEDDING" for row in profile_rows):
            return "EMBEDDING"
        return "PENDING"
    if embed_status == "INDEXED":
        return "DONE"
    if embed_status == "EMBEDDING":
        return "EMBEDDING"
    if embed_status == "FAILED":
        return "FAILED"
    return "PENDING"


def map_profile_status(record: dict[str, Any]) -> str:
    """
    EN: Derive a unified status string from write_status and query_status fields.
    CN: 从 write_status 和 query_status 字段派生统一状态字符串。
    """
    write_status = str(record.get("write_status") or "")
    query_status = str(record.get("query_status") or "")
    if query_status == "FAILED" or write_status == "FAILED":
        return "FAILED"
    if query_status == "INDEXED" and write_status == "INDEXED":
        return "INDEXED"
    if write_status == "DELETED" or query_status == "DELETED":
        return "DELETED"
    if write_status == "EMBEDDING":
        return "EMBEDDING"
    if write_status == "PENDING":
        return "PENDING"
    return query_status or write_status or "PENDING"


def map_manifest_status(manifest_summary: dict[str, Any] | None, lookup_present: bool) -> str:
    """
    EN: Map manifest summary presence and load failures to a user-facing stage status.
    CN: 将 manifest 摘要存在情况和加载失败映射为对外可见状态。
    """
    if manifest_summary is None:
        return "PENDING" if lookup_present else "WAITING"
    if manifest_summary.get("load_failed"):
        return "FAILED"
    return "DONE"
