"""
EN: Asset source for loading binary assets from S3 for embedding.
CN: 用于从 S3 加载二进制资产以进行嵌入的资产源。
"""
from __future__ import annotations

from urllib.parse import parse_qs, urlparse


class EmbedAssetSource:
    """
    EN: Load binary assets from S3 URIs with optional version_id support.
    CN: 从带有可选 version_id 支持的 S3 URI 加载二进制资产。
    """

    def __init__(self, *, s3_client: object) -> None:
        """
        Args:
            s3_client:
                EN: Boto3 S3 client used to fetch binary assets.
                CN: 用于获取二进制资产的 Boto3 S3 客户端。
        """
        self._s3 = s3_client

    def load_s3_uri(self, asset_s3_uri: str) -> bytes:
        """
        EN: Load binary asset from S3 URI, extracting bucket/key/version_id from URI.
        CN: 从 S3 URI 加载二进制资产，从 URI 提取 bucket/key/version_id。

        Args:
            asset_s3_uri:
                EN: S3 URI in format s3://bucket/key?versionId=xxx.
                CN: 格式为 s3://bucket/key?versionId=xxx 的 S3 URI。

        Returns:
            EN: Binary asset content.
            CN: 二进制资产内容。

        Raises:
            EN: ValueError if URI format is invalid.
            CN: 当 URI 格式无效时抛出 ValueError。
        """
        parsed = urlparse(asset_s3_uri)
        if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
            raise ValueError(f"Unsupported asset uri: {asset_s3_uri}")

        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        version_id = parse_qs(parsed.query).get("versionId", [None])[0]
        kwargs = {"Bucket": bucket, "Key": key}
        if version_id:
            kwargs["VersionId"] = version_id
        response = self._s3.get_object(**kwargs)
        return response["Body"].read()
