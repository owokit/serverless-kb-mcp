"""
EN: CloudFront delivery helpers for distributing source documents without exposing S3 URLs.
CN: 用于分发源文档且不暴露 S3 URL 的 CloudFront 交付辅助模块。
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Protocol
from urllib.parse import quote

from botocore.signers import CloudFrontSigner

from .aws_clients import build_aws_client
from serverless_mcp.domain.models import S3ObjectRef
from serverless_mcp.runtime.config import Settings, load_settings


class _CloudFrontRsaSigner(Protocol):
    """
    EN: Structural protocol for signing CloudFront canned policy payloads.
    CN: 同上。
    """

    def sign(self, message: bytes) -> bytes: ...


@dataclass(frozen=True, slots=True)
class DeliveredDocument:
    """
    EN: Public delivery representation for one source document.
    CN: 同上。
    """

    url: str
    expires_at: str


class CloudFrontDeliveryService:
    """
    EN: Build signed CloudFront URLs for source documents and derived assets.
    CN: 为源文档和派生资产生成签名 CloudFront URL。
    """

    def __init__(
        self,
        *,
        distribution_domain: str,
        key_pair_id: str,
        private_key_pem: str,
        url_ttl_seconds: int = 900,
        rsa_signer: _CloudFrontRsaSigner | None = None,
    ) -> None:
        if not distribution_domain.strip():
            raise ValueError("distribution_domain is required")
        if not key_pair_id.strip():
            raise ValueError("key_pair_id is required")
        if not private_key_pem.strip():
            raise ValueError("private_key_pem is required")
        if url_ttl_seconds <= 0:
            raise ValueError("url_ttl_seconds must be greater than 0")

        self._distribution_domain = distribution_domain.strip()
        self._url_ttl_seconds = url_ttl_seconds
        self._signer = CloudFrontSigner(
            key_pair_id=key_pair_id.strip(),
            rsa_signer=(rsa_signer or _CryptographyRsaSigner(private_key_pem)).sign,
        )

    def deliver_source_document(self, source: S3ObjectRef) -> DeliveredDocument:
        """
        EN: Sign a version-aware source document URL behind CloudFront.
        CN: 同上。

        Args:
            source:
                EN: S3 object reference with bucket, key, and version_id.
                CN: 包含 bucket、key 和 version_id 的 S3 对象引用。

        Returns:
            EN: DeliveredDocument with the signed URL and ISO-8601 expiry timestamp.
            CN: 包含签名 URL 和 ISO-8601 过期时间戳的 DeliveredDocument。
        """
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=self._url_ttl_seconds)
        # EN: Encode bucket and key into the CloudFront resource path with versionId query param.
        # CN: 同上。
        resource = (
            f"https://{self._distribution_domain}/documents/"
            f"{quote(source.bucket, safe='')}/{quote(source.key, safe='/')}"
            f"?versionId={quote(source.version_id, safe='')}"
        )
        signed_url = self._signer.generate_presigned_url(resource, date_less_than=expires_at)
        return DeliveredDocument(url=signed_url, expires_at=expires_at.isoformat())

def build_cloudfront_delivery_service(settings: Settings | None = None) -> CloudFrontDeliveryService | None:
    """
    EN: Build the CloudFront delivery service from runtime settings.
    CN: 根据运行时设置构建 CloudFront delivery service。
    """
    active_settings = settings or load_settings()
    if not active_settings.cloudfront_distribution_domain:
        return None
    if not active_settings.cloudfront_key_pair_id:
        raise ValueError("CLOUDFRONT_KEY_PAIR_ID is required for CloudFront delivery")
    private_key_pem = active_settings.cloudfront_private_key_pem
    if not private_key_pem and active_settings.cloudfront_private_key_secret_arn:
        private_key_pem = load_cloudfront_private_key_from_secret(active_settings.cloudfront_private_key_secret_arn)
    if not private_key_pem:
        raise ValueError(
            "CLOUDFRONT_PRIVATE_KEY_PEM or CLOUDFRONT_PRIVATE_KEY_SECRET_ARN is required for CloudFront delivery"
        )

    return CloudFrontDeliveryService(
        distribution_domain=active_settings.cloudfront_distribution_domain,
        key_pair_id=active_settings.cloudfront_key_pair_id,
        private_key_pem=private_key_pem,
        url_ttl_seconds=active_settings.cloudfront_url_ttl_seconds,
    )


class _CryptographyRsaSigner:
    """
    EN: RSA-SHA1 signer compatible with CloudFront signed URL generation.
    CN: 同上。
    """

    def __init__(self, private_key_pem: str) -> None:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding

        self._hashes = hashes
        self._padding = padding
        self._private_key = serialization.load_pem_private_key(
            private_key_pem.encode("utf-8"),
            password=None,
        )

    def sign(self, message: bytes) -> bytes:
        return self._private_key.sign(
            message,
            self._padding.PKCS1v15(),
            self._hashes.SHA1(),
        )


@lru_cache(maxsize=8)
def load_cloudfront_private_key_from_secret(secret_arn: str) -> str:
    """
    EN: Load and cache the CloudFront private key from AWS Secrets Manager.
    CN: 从 AWS Secrets Manager 读取并缓存 CloudFront 私钥。

    Args:
        secret_arn:
            EN: ARN of the Secrets Manager secret containing the private key.
            CN: 包含私钥的 Secrets Manager secret ARN。

    Returns:
        EN: Normalized PEM private key string.
        CN: 同上。

    Raises:
        EN: ValueError if the secret does not contain a recognizable private key.
        CN: 同上。
    """
    client = build_aws_client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_arn)
    # EN: Secrets Manager may return either SecretString or SecretBinary depending on how the secret was stored.
    # CN: 同上。
    secret_string = response.get("SecretString")
    if secret_string:
        return _normalize_private_key_secret(secret_string)

    secret_binary = response.get("SecretBinary")
    if secret_binary:
        if isinstance(secret_binary, bytes):
            payload = secret_binary.decode("utf-8")
        else:
            payload = bytes(secret_binary).decode("utf-8")
        return _normalize_private_key_secret(payload)

    raise ValueError(f"Secrets Manager secret {secret_arn} does not contain a private key")


def _normalize_private_key_secret(raw_secret: str) -> str:
    """
    EN: Accept either a raw PEM payload or a JSON object containing private_key_pem.
    CN: 同上。
    """
    candidate = raw_secret.strip()
    if not candidate:
        raise ValueError("CloudFront private key secret is empty")

    # EN: Attempt JSON parsing first; fall through to raw PEM if the structure does not match.
    # CN: 先尝试 JSON 解析；如果结构不匹配，则退回原始 PEM。
    if candidate.startswith("{"):
        payload = json.loads(candidate)
        if isinstance(payload, dict):
            pem = payload.get("private_key_pem") or payload.get("private_key")
            if isinstance(pem, str) and pem.strip():
                return pem.strip()
    return candidate
