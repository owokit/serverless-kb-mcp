"""
EN: Runtime AWS client contract tests that pin endpoint and cache semantics.
CN: 锁定 endpoint 与缓存语义的运行时 AWS 客户端契约测试。
"""
from __future__ import annotations

from typing import Any

import boto3
import pytest
from botocore.config import Config

from serverless_mcp.runtime import aws_clients
from serverless_mcp.runtime.aws_clients import AwsClientBundle


class _FakeSession:
    def __init__(self, region_name: str | None = None) -> None:
        self.region_name = region_name
        self.calls: list[dict[str, Any]] = []

    def client(self, service_name: str, **kwargs: Any) -> object:
        self.calls.append({"service_name": service_name, **kwargs})
        return {"service_name": service_name, **kwargs}


@pytest.fixture(autouse=True)
def _clear_client_cache() -> None:
    aws_clients.get_aws_clients.cache_clear()
    yield
    aws_clients.get_aws_clients.cache_clear()


def test_build_aws_client_prefers_highest_priority_endpoint_env(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[dict[str, Any]] = []

    def fake_client(service_name: str, **kwargs: Any) -> object:
        captured.append({"service_name": service_name, **kwargs})
        return captured[-1]

    monkeypatch.setattr(boto3, "client", fake_client)
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "  https://should-not-win.test  ")
    monkeypatch.setenv("AWS_S3_ENDPOINT_URL", "https://winner.test")

    client = aws_clients.build_aws_client("s3", region_name="ap-southeast-1")

    assert client is captured[0]
    assert captured[0]["service_name"] == "s3"
    assert captured[0]["region_name"] == "ap-southeast-1"
    assert captured[0]["endpoint_url"] == "https://winner.test"
    assert isinstance(captured[0]["config"], Config)
    assert captured[0]["config"].retries["mode"] == "adaptive"


def test_build_session_client_applies_s3_path_style_config(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_session = _FakeSession(region_name="us-east-1")
    monkeypatch.setattr(boto3, "Session", lambda region_name=None: fake_session)
    monkeypatch.setenv("AWS_S3_ENDPOINT_URL", "https://s3.test")

    client = aws_clients.build_session_client("s3", region_name="us-west-2")

    assert client["service_name"] == "s3"
    assert client["endpoint_url"] == "https://s3.test"
    assert client["config"].s3["addressing_style"] == "path"
    assert client["config"].retries["mode"] == "adaptive"


def test_build_aws_client_supports_s3vectors_local_endpoint_override(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[dict[str, Any]] = []

    def fake_client(service_name: str, **kwargs: Any) -> object:
        captured.append({"service_name": service_name, **kwargs})
        return captured[-1]

    monkeypatch.setattr(boto3, "client", fake_client)
    monkeypatch.setenv("AWS_S3_VECTORS_ENDPOINT_URL", "https://vectors.test")

    client = aws_clients.build_aws_client("s3vectors")

    assert client is captured[0]
    assert captured[0]["endpoint_url"] == "https://vectors.test"
    assert isinstance(captured[0]["config"], Config)


def test_get_aws_clients_is_process_cached_until_explicitly_cleared(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def fake_build_aws_client(service_name: str, *, region_name: str | None = None) -> object:
        calls.append(service_name)
        return {"service_name": service_name, "region_name": region_name}

    monkeypatch.setattr(aws_clients, "build_aws_client", fake_build_aws_client)

    first = aws_clients.get_aws_clients()
    second = aws_clients.get_aws_clients()

    assert first is second
    assert calls == ["s3", "dynamodb", "sqs", "stepfunctions", "s3vectors"]
    assert isinstance(first, AwsClientBundle)
