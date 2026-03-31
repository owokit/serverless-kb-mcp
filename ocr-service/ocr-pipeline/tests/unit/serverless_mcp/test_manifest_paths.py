"""
EN: Tests for manifest S3 key layout, source file name optimization, and document URI encoding.
CN: 测试 manifest S3 key 布局、源文件名优化和 document URI 编码。
"""

from __future__ import annotations

import hashlib

from serverless_mcp.domain.models import S3ObjectRef
from serverless_mcp.storage.paths import (
    build_manifest_key,
    build_manifest_root,
    build_source_named_asset_path,
    optimize_source_file_name,
)


def test_optimize_source_file_name_keeps_readable_unicode_and_hashes_identity() -> None:
    """
    EN: Optimize source file name keeps readable unicode and hashes identity.
    CN: 验证 optimize_source_file_name 会保留可读 Unicode 并附带哈希。
    """
    source = S3ObjectRef(
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="资料/中文 名称（最终版）.PDF",
        version_id="v1",
    )

    optimized = optimize_source_file_name(source)
    expected_digest = hashlib.sha1(source.object_pk.encode("utf-8")).hexdigest()[:12]

    assert optimized.endswith(f"--{expected_digest}")
    assert " " not in optimized
    assert "/" not in optimized
    assert optimized.split("--", 1)[0]


def test_manifest_key_uses_source_name_only_not_source_version_folder() -> None:
    """
    EN: Manifest key uses source name only not source version folder.
    CN: 验证 manifest key 只使用源文件名而不是版本目录。
    """
    source = S3ObjectRef(
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="docs/guide.pdf",
        version_id="v123",
    )

    root = build_manifest_root(source)
    key = build_manifest_key(source)

    assert key == f"{root}/manifest.json"
    assert "v123" not in key


def test_document_uri_encodes_reserved_key_and_version_chars() -> None:
    """
    EN: Document uri encodes reserved key and version chars.
    CN: 验证 document_uri 会对保留字符进行 URL 编码。
    """
    source = S3ObjectRef(
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="docs/guide 1+2.pdf",
        version_id="v/123",
    )

    assert source.document_uri == "s3://bucket-a/docs/guide%201%2B2.pdf?versionId=v%2F123"


def test_source_named_asset_path_uses_input_filename_verbatim() -> None:
    """
    EN: Source-named assets should keep the original filename and append the requested suffix.
    CN: 以源文件名命名的资产应保留原始文件名并追加指定后缀。
    """
    source = S3ObjectRef(
        tenant_id="tenant-a",
        bucket="bucket-a",
        key="incoming/reports/Attention Is All You Need.pdf",
        version_id="v1",
    )

    assert build_source_named_asset_path(source, "json") == "Attention Is All You Need.pdf.json"
    assert build_source_named_asset_path(source, ".md") == "Attention Is All You Need.pdf.md"
