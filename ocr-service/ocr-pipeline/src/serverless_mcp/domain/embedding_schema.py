"""
EN: Embedding request schema validation for dispatch and worker boundaries.
CN: 用于分发层和 worker 边界的 embedding request schema 校验。
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from serverless_mcp.domain.models import EmbeddingJobMessage, EmbeddingRequest, S3ObjectRef
from serverless_mcp.domain.schema_errors import SchemaValidationError


def validate_embedding_request(request: EmbeddingRequest) -> EmbeddingRequest:
    """
    EN: Validate a single embedding request before it is queued or executed.
CN: 在单个 embedding request 入队或执行前进行校验。
    """
    if not isinstance(request, EmbeddingRequest):
        raise SchemaValidationError(f"Expected EmbeddingRequest, got {type(request)!r}")
    if not request.chunk_id.strip():
        raise SchemaValidationError("embedding request chunk_id is required")
    if not request.chunk_type:
        raise SchemaValidationError(f"embedding request {request.chunk_id} chunk_type is required")
    if request.output_dimensionality <= 0:
        raise SchemaValidationError(f"embedding request {request.chunk_id} output_dimensionality must be positive")

    _require_metadata(request.metadata, request.chunk_id)
    _require_keys(
        request.metadata,
        (
            "tenant_id",
            "bucket",
            "key",
            "version_id",
            "document_uri",
            "language",
            "doc_type",
            "source_format",
            "manifest_s3_uri",
            "is_latest",
        ),
        context=f"embedding request {request.chunk_id}",
    )
    if request.content_kind == "text":
        if request.text is None or not request.text.strip():
            raise SchemaValidationError(f"embedding request {request.chunk_id} requires text content")
    elif request.content_kind == "image":
        if not request.asset_s3_uri:
            raise SchemaValidationError(f"embedding request {request.chunk_id} requires asset_s3_uri")
        if not request.mime_type:
            raise SchemaValidationError(f"embedding request {request.chunk_id} requires mime_type")
    elif request.content_kind == "pdf":
        if request.text is None and not request.asset_s3_uri:
            raise SchemaValidationError(f"embedding request {request.chunk_id} requires text or asset_s3_uri")
    else:  # pragma: no cover - defensive guard for future literals
        raise SchemaValidationError(f"Unsupported embedding content kind: {request.content_kind!r}")
    return request


def validate_embedding_requests(requests: list[EmbeddingRequest]) -> list[EmbeddingRequest]:
    """
    EN: Validate and return a list of embedding requests, enforcing chunk_id uniqueness.
CN: 校验并返回 embedding request 列表，同时强制 chunk_id 唯一。
    """
    seen_chunk_ids: set[str] = set()
    validated: list[EmbeddingRequest] = []
    for request in requests:
        validated_request = validate_embedding_request(request)
        if validated_request.chunk_id in seen_chunk_ids:
            raise SchemaValidationError(f"Duplicate embedding request chunk_id: {validated_request.chunk_id}")
        seen_chunk_ids.add(validated_request.chunk_id)
        validated.append(validated_request)
    return validated


def validate_embedding_job_message(job: EmbeddingJobMessage) -> EmbeddingJobMessage:
    """
    EN: Validate one embedding job message before dispatch or worker execution.
CN: 在分发或 worker 执行前校验一条 embedding job 消息。
    """
    if not isinstance(job, EmbeddingJobMessage):
        raise SchemaValidationError(f"Expected EmbeddingJobMessage, got {type(job)!r}")
    if not job.profile_id.strip():
        raise SchemaValidationError("embedding job profile_id is required")
    if not job.trace_id.strip():
        raise SchemaValidationError("embedding job trace_id is required")
    if not job.manifest_s3_uri.strip():
        raise SchemaValidationError("embedding job manifest_s3_uri is required")
    if not isinstance(job.source, S3ObjectRef):
        raise SchemaValidationError(f"embedding job source must be an S3ObjectRef, got {type(job.source)!r}")
    if not job.requests:
        raise SchemaValidationError(f"embedding job {job.profile_id} must contain at least one request")
    validate_embedding_requests(job.requests)
    return job


def _require_metadata(metadata: Any, chunk_id: str) -> Mapping[str, Any]:
    """
    EN: Ensure metadata is a mapping, raising SchemaValidationError otherwise.
CN: 确保 metadata 是映射类型，否则抛出 SchemaValidationError。
    """
    if not isinstance(metadata, Mapping):
        raise SchemaValidationError(f"embedding request {chunk_id} metadata must be a mapping")
    return metadata


def _require_keys(payload: Mapping[str, Any], required_keys: tuple[str, ...], *, context: str) -> None:
    """
    EN: Ensure all required keys are present and non-empty in the payload.
CN: 确保载荷中的所有必需键都存在且非空。
    """
    missing = [key for key in required_keys if key not in payload or payload[key] is None or payload[key] == ""]
    if missing:
        raise SchemaValidationError(f"{context} missing required keys: {', '.join(missing)}")

