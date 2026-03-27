"""
EN: Helpers for readable, version-aware manifest and asset S3 key generation.
CN: 可读、版本感知的 manifest 与 asset S3 key 生成辅助工具。

All manifest and asset paths are deterministic given source object identity, enabling
idempotent reprocessing and consistent CloudFront signed URL generation.
CN: 在给定源对象身份后，manifest 和 asset 路径都是确定性的，因此可以幂等重处理，并保持 CloudFront 签名 URL 生成一致。
"""
from __future__ import annotations

import hashlib
import re
import unicodedata
from pathlib import PurePosixPath
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from serverless_mcp.domain.models import S3ObjectRef


def optimize_source_file_name(source: S3ObjectRef) -> str:
    """
    EN: Build a readable, collision-resistant folder name from the source file name.
    CN: 根据源文件名构造一个可读且具备抗冲突能力的文件夹名。
    """
    raw_name = PurePosixPath(source.key.rstrip("/")).name or "document"
    normalized = unicodedata.normalize("NFKC", raw_name).strip()
    if not normalized or normalized in {".", ".."}:
        normalized = "document"

    suffix = PurePosixPath(normalized).suffix
    stem = normalized[: -len(suffix)] if suffix and normalized.endswith(suffix) else normalized
    safe_stem = _sanitize_segment(stem) or "document"
    safe_suffix = _sanitize_suffix(suffix)
    digest = hashlib.sha1(source.object_pk.encode("utf-8")).hexdigest()[:12]
    return f"{safe_stem}{safe_suffix}--{digest}"


def build_manifest_root(source: S3ObjectRef, *, manifest_prefix: str = "") -> str:
    """
    EN: Build the manifest root folder for one source object identity.
    CN: 为某个源对象身份构建 manifest 根目录。
    """
    prefix = manifest_prefix.strip("/")
    folder = optimize_source_file_name(source)
    parts = [part for part in (prefix, folder) if part]
    return "/".join(parts)


def build_manifest_key(source: S3ObjectRef, *, manifest_prefix: str = "") -> str:
    """
    EN: Build the manifest.json object key for one source object identity.
    CN: 为某个源对象身份构建 manifest.json 的对象 key。
    """
    return f"{build_manifest_root(source, manifest_prefix=manifest_prefix)}/manifest.json"


def build_asset_key(source: S3ObjectRef, relative_path: str, *, manifest_prefix: str = "") -> str:
    """
    EN: Build an asset object key relative to the manifest root folder.
    CN: 基于 manifest 根目录构建 asset 的 object key。
    """
    relative_path = _sanitize_relative_path(relative_path)
    return f"{build_manifest_root(source, manifest_prefix=manifest_prefix)}/{relative_path}"


def build_s3_uri(bucket: str, key: str, *, version_id: str | None = None) -> str:
    """
    EN: Render a version-aware S3 URI.
    CN: 生成带版本信息的 S3 URI。
    """
    query = urlencode({"versionId": version_id}) if version_id else ""
    return urlunparse(("s3", bucket, f"/{key.lstrip('/')}", "", query, ""))


def parse_s3_uri(uri: str) -> tuple[str, str, str | None]:
    """
    EN: Parse an S3 URI and preserve the optional versionId query parameter.
    CN: 解析 S3 URI 并保留可选的 versionId 查询参数。
    """
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
        raise ValueError(f"Invalid S3 URI: {uri}")
    query = parse_qs(parsed.query)
    version_id = query.get("versionId", [None])[0]
    return parsed.netloc, parsed.path.lstrip("/"), version_id


def _sanitize_relative_path(relative_path: str) -> str:
    """
    EN: Sanitize a relative path by stripping whitespace, leading slashes, and rejecting path traversal segments.
    CN: 清理相对路径，去除空白和前导斜杠，并拒绝路径穿越片段。
    """
    value = relative_path.strip().lstrip("/")
    if not value:
        raise ValueError("relative_path is required")
    segments = value.split("/")
    if any(segment in {"", ".", ".."} for segment in segments):
        raise ValueError(f"Invalid relative path: {relative_path}")
    return value


def _sanitize_segment(value: str) -> str:
    """
    EN: Sanitize a path segment by replacing unsafe characters and collapsing consecutive separators.
    CN: 清理路径片段，替换不安全字符并折叠连续分隔符。
    """
    sanitized = []
    for char in value:
        if char.isalnum() or char in {".", "_", "-", " "}:
            sanitized.append(char)
        else:
            sanitized.append("-")
    text = "".join(sanitized).strip().replace(" ", "-")
    text = re.sub(r"[-_.]{2,}", "-", text)
    return text.strip(".-_")


def _sanitize_suffix(suffix: str) -> str:
    """
    EN: Normalize a file extension suffix to lowercase.
    CN: 将文件扩展名后缀规范化为小写。
    """
    if not suffix:
        return ""
    return suffix.lower()
