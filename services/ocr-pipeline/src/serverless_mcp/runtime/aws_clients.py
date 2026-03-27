"""
EN: Shared AWS client factory helpers with optional local endpoint overrides.
CN: 支持可选本地 endpoint 覆盖的共享 AWS 客户端工厂辅助函数。
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

import boto3
from botocore.config import Config


def build_aws_client(service_name: str, *, region_name: str | None = None) -> Any:
    """
    EN: Build a boto3 client with repository-friendly local endpoint overrides.
    CN: 构建支持仓库级本地 endpoint 覆盖的 boto3 客户端。
    """
    endpoint_url = _resolve_endpoint_url(service_name)
    config = _resolve_client_config(service_name)
    kwargs: dict[str, object] = {}
    if region_name:
        kwargs["region_name"] = region_name
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    if config is not None:
        kwargs["config"] = config
    return boto3.client(service_name, **kwargs)


def build_session_client(service_name: str, *, region_name: str | None = None) -> Any:
    """
    EN: Build a boto3 session client with the same endpoint resolution policy.
    CN: 使用相同 endpoint 解析策略构建 boto3 session 客户端。
    """
    endpoint_url = _resolve_endpoint_url(service_name)
    config = _resolve_client_config(service_name)
    session = boto3.Session(region_name=region_name)
    kwargs: dict[str, object] = {}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    if config is not None:
        kwargs["config"] = config
    return session.client(service_name, **kwargs)


@dataclass(frozen=True, slots=True)
class AwsClientBundle:
    """
    EN: Cached AWS client bundle shared across runtime builders.
    CN: 在运行时构建器之间共享的 AWS 客户端缓存集合。
    """

    s3: Any
    dynamodb: Any
    sqs: Any
    stepfunctions: Any
    s3vectors: Any


@lru_cache(maxsize=1)
def get_aws_clients() -> AwsClientBundle:
    """
    EN: Build and cache boto3 clients once per Lambda runtime.
    CN: 每个 Lambda 运行环境只构建一次 boto3 客户端并缓存。
    """
    return AwsClientBundle(
        s3=build_aws_client("s3"),
        dynamodb=build_aws_client("dynamodb"),
        sqs=build_aws_client("sqs"),
        stepfunctions=build_aws_client("stepfunctions"),
        s3vectors=build_aws_client("s3vectors"),
    )


def _resolve_endpoint_url(service_name: str) -> str | None:
    """
    EN: Resolve endpoint URL from environment variables for local development overrides.
    CN: 从环境变量解析 endpoint URL，用于本地开发覆盖。

    Returns:
        EN: Stripped endpoint URL if any candidate env var is set, otherwise None.
        CN: 若任一候选环境变量已设置则返回去除空白的 endpoint URL，否则返回 None。
    """
    # EN: Walk candidates in priority order; first non-blank value wins.
    # CN: 按优先级遍历候选变量，第一个非空白值生效。
    candidates = _endpoint_env_names(service_name)
    for name in candidates:
        value = os.environ.get(name)
        if value is not None and value.strip():
            return value.strip()
    return None


def _resolve_client_config(service_name: str) -> Config | None:
    """
    EN: Build boto3 Config with retries and S3 path-style addressing for compatible services.
    CN: 为兼容的 AWS 服务构建带重试和 S3 路径寻址的 boto3 Config。

    Args:
        service_name:
            EN: AWS service identifier such as "s3", "s3vectors", "dynamodb", etc.
            CN: AWS 服务标识符，如 "s3"、"s3vectors"、"dynamodb" 等。

    Returns:
        EN: Config instance with adaptive retries for S3-family services, or None for others.
        CN: 对 S3 系列服务返回带自适应重试的 Config 实例，其他服务返回 None。
    """
    if service_name not in {"s3", "s3vectors"}:
        return None
    # EN: Adaptive retries help absorb transient API throttling in high-throughput extract scenarios.
    # CN: 自适应重试有助于在高吞吐提取场景中吸收瞬态 API 限流。
    kwargs: dict[str, object] = {
        "retries": {"max_attempts": 5, "mode": "adaptive"},
    }
    # EN: Path-style addressing is required for local S3-compatible backends like LocalStack.
    # CN: 路径寻址对于 LocalStack 等本地 S3 兼容后端是必需的。
    if service_name == "s3":
        kwargs["s3"] = {"addressing_style": "path"}
    return Config(**kwargs)


def _endpoint_env_names(service_name: str) -> tuple[str, ...]:
    """
    EN: Return the ordered set of environment variable names for a given AWS service endpoint.
    CN: 返回给定 AWS 服务 endpoint 对应的有序环境变量名称集合。
    """
    mapping: dict[str, tuple[str, ...]] = {
        "s3": (
            "AWS_S3_ENDPOINT_URL",
            "AWS_ENDPOINT_URL_S3",
            "S3_ENDPOINT_URL",
            "LOCALSTACK_S3_ENDPOINT_URL",
        ),
        "dynamodb": (
            "AWS_DYNAMODB_ENDPOINT_URL",
            "AWS_ENDPOINT_URL_DYNAMODB",
            "DYNAMODB_ENDPOINT_URL",
            "LOCALSTACK_DYNAMODB_ENDPOINT_URL",
        ),
        "sqs": (
            "AWS_SQS_ENDPOINT_URL",
            "AWS_ENDPOINT_URL_SQS",
            "SQS_ENDPOINT_URL",
            "LOCALSTACK_SQS_ENDPOINT_URL",
        ),
        "stepfunctions": (
            "AWS_STEPFUNCTIONS_ENDPOINT_URL",
            "AWS_ENDPOINT_URL_STEPFUNCTIONS",
            "STEPFUNCTIONS_ENDPOINT_URL",
            "LOCALSTACK_STEPFUNCTIONS_ENDPOINT_URL",
        ),
        "secretsmanager": (
            "AWS_SECRETSMANAGER_ENDPOINT_URL",
            "AWS_ENDPOINT_URL_SECRETSMANAGER",
            "SECRETSMANAGER_ENDPOINT_URL",
        ),
        "s3vectors": (
            "AWS_S3_VECTORS_ENDPOINT_URL",
            "AWS_ENDPOINT_URL_S3_VECTORS",
            "S3_VECTORS_ENDPOINT_URL",
        ),
    }
    return mapping.get(service_name, ())
