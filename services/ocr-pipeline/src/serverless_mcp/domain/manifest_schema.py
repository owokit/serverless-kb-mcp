"""
EN: Manifest schema validation for persisted extraction outputs.
CN: 用于已持久化提取产物的 manifest schema 校验。
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from serverless_mcp.domain.format_specs import FormatSpec, get_format_spec
from serverless_mcp.domain.models import ChunkManifest, ExtractedAsset, ExtractedChunk
from serverless_mcp.domain.schema_errors import SchemaValidationError


def validate_chunk_manifest(manifest: ChunkManifest) -> FormatSpec:
    """
    EN: Validate one manifest and return the resolved format spec.
    CN: 验证一个 manifest，并返回解析后的格式规范。
    """
    if not isinstance(manifest, ChunkManifest):
        raise SchemaValidationError(f"Expected ChunkManifest, got {type(manifest)!r}")

    spec = _resolve_manifest_spec(manifest)
    _require_mapping(manifest.metadata, f"manifest metadata for {manifest.doc_type}/{spec.source_format}")
    _require_keys(manifest.metadata, spec.manifest_required_keys, context="manifest metadata")

    for index, chunk in enumerate(manifest.chunks, start=1):
        _validate_chunk(chunk, spec, index=index)

    for index, asset in enumerate(manifest.assets, start=1):
        _validate_asset(asset, spec, index=index)

    return spec


def _resolve_manifest_spec(manifest: ChunkManifest) -> FormatSpec:
    """
    EN: Resolve the FormatSpec from manifest metadata by source_format.
    CN: 根据 manifest metadata 中的 source_format 解析对应的 FormatSpec。
    """
    source_format = manifest.metadata.get("source_format")
    if not isinstance(source_format, str) or not source_format.strip():
        raise SchemaValidationError("manifest.metadata.source_format is required")
    try:
        return get_format_spec(doc_type=manifest.doc_type, source_format=source_format)
    except KeyError as exc:
        raise SchemaValidationError(
            f"Unsupported manifest schema: doc_type={manifest.doc_type!r}, source_format={source_format!r}"
        ) from exc


def _validate_chunk(chunk: ExtractedChunk, spec: FormatSpec, *, index: int) -> None:
    """
    EN: Validate a single chunk against the resolved format spec.
    CN: 根据解析后的格式规范验证单个 chunk。
    """
    if not isinstance(chunk, ExtractedChunk):
        raise SchemaValidationError(f"Expected ExtractedChunk at index {index}, got {type(chunk)!r}")
    _require_mapping(chunk.metadata, f"chunk.metadata[{chunk.chunk_id}]")
    _require_keys(chunk.metadata, spec.chunk_required_keys, context=f"chunk.metadata[{chunk.chunk_id}]")
    source_format = chunk.metadata.get("source_format")
    if source_format != spec.source_format:
        raise SchemaValidationError(
            f"chunk.metadata[{chunk.chunk_id}].source_format must be {spec.source_format!r}, got {source_format!r}"
        )
    if chunk.page_span is not None and len(chunk.page_span) != 2:
        raise SchemaValidationError(f"chunk.page_span[{chunk.chunk_id}] must contain exactly two integers")


def _validate_asset(asset: ExtractedAsset, spec: FormatSpec, *, index: int) -> None:
    """
    EN: Validate a single asset against the resolved format spec.
    CN: 根据解析后的格式规范验证单个 asset。
    """
    if not isinstance(asset, ExtractedAsset):
        raise SchemaValidationError(f"Expected ExtractedAsset at index {index}, got {type(asset)!r}")
    _require_mapping(asset.metadata, f"asset.metadata[{asset.asset_id}]")
    _require_keys(asset.metadata, spec.asset_required_keys, context=f"asset.metadata[{asset.asset_id}]")
    source_format = asset.metadata.get("source_format")
    if source_format != spec.source_format:
        raise SchemaValidationError(
            f"asset.metadata[{asset.asset_id}].source_format must be {spec.source_format!r}, got {source_format!r}"
        )
    if asset.payload is None and not asset.asset_s3_uri:
        raise SchemaValidationError(
            f"asset {asset.asset_id} must include either payload or asset_s3_uri"
        )


def _require_mapping(value: Any, context: str) -> Mapping[str, Any]:
    """
    EN: Ensure the value is a mapping, raising SchemaValidationError otherwise.
    CN: 确保值是映射类型，否则抛出 SchemaValidationError。
    """
    if not isinstance(value, Mapping):
        raise SchemaValidationError(f"{context} must be a mapping")
    return value


def _require_keys(payload: Mapping[str, Any], required_keys: tuple[str, ...], *, context: str) -> None:
    """
    EN: Ensure all required keys are present and non-empty in the payload.
    CN: 确保 payload 中包含所有必需键且值非空。
    """
    missing = [key for key in required_keys if key not in payload or payload[key] is None or payload[key] == ""]
    if missing:
        raise SchemaValidationError(f"{context} missing required keys: {', '.join(missing)}")

