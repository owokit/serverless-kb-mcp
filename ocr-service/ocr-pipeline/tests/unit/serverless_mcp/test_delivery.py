"""
EN: Tests for CloudFront signed URL delivery and Secrets Manager private key loading.
CN: 娴嬭瘯 CloudFront 绛惧悕 URL 鍒嗗彂鍜?Secrets Manager 绉侀挜鍔犺浇銆?
"""

from __future__ import annotations

import json
from datetime import datetime
from urllib.parse import parse_qs, urlparse

import pytest

from serverless_mcp.runtime import delivery
from serverless_mcp.runtime.delivery import CloudFrontDeliveryService, load_cloudfront_private_key_from_secret
from serverless_mcp.domain.models import S3ObjectRef


class _FakeCloudFrontSigner:
    # EN: Stand-in CloudFrontSigner that appends expiry and key pair id to the URL.
    # CN: CloudFrontSigner 鏇胯韩锛屽湪 URL 鍚庤拷鍔犺繃鏈熸椂闂村拰 key pair id銆?
    def __init__(self, *, key_pair_id: str, rsa_signer) -> None:
        self.key_pair_id = key_pair_id
        self.rsa_signer = rsa_signer

    def generate_presigned_url(self, resource: str, date_less_than) -> str:
        return f"{resource}&Expires={int(date_less_than.timestamp())}&Key-Pair-Id={self.key_pair_id}"


class _FakeRsaSigner:
    # EN: Stub RSA signer returning a fixed signature payload.
    # CN: 杩斿洖鍥哄畾绛惧悕杞借嵎鐨?RSA 绛惧悕鍣ㄦ々銆?
    def sign(self, message: bytes) -> bytes:
        return b"signed"


def test_cloudfront_delivery_service_builds_version_aware_document_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify CloudFront delivery produces a version-aware URL with correct encoding.
    CN: 同上。
    """
    monkeypatch.setattr(delivery, "CloudFrontSigner", _FakeCloudFrontSigner)
    service = CloudFrontDeliveryService(
        distribution_domain="cdn.example.com",
        key_pair_id="K123",
        private_key_pem="-----BEGIN PRIVATE KEY-----\nFAKE\n-----END PRIVATE KEY-----",
        url_ttl_seconds=600,
        rsa_signer=_FakeRsaSigner(),
    )

    result = service.deliver_source_document(
        S3ObjectRef(
            tenant_id="tenant-a",
            bucket="bucket a",
            key="docs/guide 1.pdf",
            version_id="v/2",
        )
    )

    parsed = urlparse(result.url)
    query = parse_qs(parsed.query)

    assert parsed.scheme == "https"
    assert parsed.netloc == "cdn.example.com"
    assert parsed.path == "/documents/bucket%20a/docs/guide%201.pdf"
    assert query["versionId"] == ["v/2"]
    assert query["Key-Pair-Id"] == ["K123"]
    assert datetime.fromisoformat(result.expires_at)


def test_cloudfront_delivery_service_rejects_invalid_configuration() -> None:
    """
    EN: Verify CloudFrontDeliveryService validates required configuration fields at construction.
    CN: 同上。
    """
    with pytest.raises(ValueError, match="distribution_domain is required"):
        CloudFrontDeliveryService(
            distribution_domain=" ",
            key_pair_id="K123",
            private_key_pem="pem",
        )

    with pytest.raises(ValueError, match="url_ttl_seconds must be greater than 0"):
        CloudFrontDeliveryService(
            distribution_domain="cdn.example.com",
            key_pair_id="K123",
            private_key_pem="pem",
            url_ttl_seconds=0,
        )

    with pytest.raises(ValueError, match="key_pair_id is required"):
        CloudFrontDeliveryService(
            distribution_domain="cdn.example.com",
            key_pair_id=" ",
            private_key_pem="pem",
        )

    with pytest.raises(ValueError, match="private_key_pem is required"):
        CloudFrontDeliveryService(
            distribution_domain="cdn.example.com",
            key_pair_id="K123",
            private_key_pem=" ",
        )


def test_load_cloudfront_private_key_from_secret_accepts_plain_and_json_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify that secrets manager loader handles both plain PEM strings and JSON-wrapped keys, including rotation.
    CN: 同上。
    """
    # EN: Fake SecretsManager client that returns different values on successive calls for rotation testing.
    # CN: 同上。
    class _FakeSecretsManager:
        # EN: Stub SecretsManager client with rotation support.
        # CN: 同上。
        def __init__(self) -> None:
            self.calls = []
            self.responses = {
                "arn:aws:secretsmanager:us-east-1:123:secret:plain": [
                    {"SecretString": "-----BEGIN PRIVATE KEY-----\nPLAIN-1\n-----END PRIVATE KEY-----"},
                    {"SecretString": "-----BEGIN PRIVATE KEY-----\nPLAIN-2\n-----END PRIVATE KEY-----"},
                ],
                "arn:aws:secretsmanager:us-east-1:123:secret:json": [
                    {"SecretString": json.dumps({"private_key_pem": "-----BEGIN PRIVATE KEY-----\nJSON\n-----END PRIVATE KEY-----"})}
                ],
            }

        def get_secret_value(self, **kwargs):
            self.calls.append(kwargs)
            secret_id = kwargs["SecretId"]
            responses = self.responses[secret_id]
            if len(responses) > 1:
                return responses.pop(0)
            return responses[0]

    client = _FakeSecretsManager()
    monkeypatch.setattr(delivery, "build_aws_client", lambda service_name, **kwargs: client)

    load_cloudfront_private_key_from_secret.cache_clear()
    plain = load_cloudfront_private_key_from_secret("arn:aws:secretsmanager:us-east-1:123:secret:plain")
    json_secret = load_cloudfront_private_key_from_secret("arn:aws:secretsmanager:us-east-1:123:secret:json")
    cached_plain = load_cloudfront_private_key_from_secret("arn:aws:secretsmanager:us-east-1:123:secret:plain")

    assert plain.startswith("-----BEGIN PRIVATE KEY-----")
    assert "PLAIN-1" in plain
    assert "JSON" in json_secret
    assert cached_plain == plain
    assert "PLAIN-2" not in cached_plain
    assert len(client.calls) == 2
