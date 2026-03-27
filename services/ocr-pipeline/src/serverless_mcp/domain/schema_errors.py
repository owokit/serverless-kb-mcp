"""
EN: Shared schema validation errors for manifest and embedding request boundaries.
CN: 用于 manifest 和 embedding request 边界的共享 schema 校验错误。
"""
from __future__ import annotations


class SchemaValidationError(ValueError):
    """
    EN: Raised when a manifest or embedding request violates the expected schema.
    CN: 当 manifest 或 embedding request 违反预期 schema 时抛出。
    """

