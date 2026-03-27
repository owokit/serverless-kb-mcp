"""
EN: Check whether a configured S3 Vectors index contains any vectors.
CN: 检查已配置的 S3 Vectors index 中是否存在向量内容。
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


REFERENCE_ONLY = True

REPO_ROOT = Path(__file__).resolve().parents[1]
# EN: Common boto3 retry configuration for S3 Vectors clients.
# CN: S3 Vectors 客户端的通用 boto3 重试配置。
COMMON_BOTO_CONFIG = Config(retries={"max_attempts": 5, "mode": "adaptive"})

# EN: Accepted environment variable names for the S3 Vectors bucket.
# CN: S3 Vectors bucket 对应的环境变量名称列表。
VECTOR_BUCKET_ENV_NAMES = (
    "S3_VECTORS_BUCKET_NAME",
    "VECTOR_BUCKET_NAME",
    "VECTOR_BUCKET",
)
# EN: Accepted environment variable names for the S3 Vectors index.
# CN: S3 Vectors index 对应的环境变量名称列表。
VECTOR_INDEX_ENV_NAMES = (
    "S3_VECTORS_INDEX_NAME",
    "VECTOR_INDEX_NAME",
    "VECTOR_INDEX",
)
# EN: Accepted environment variable names for the AWS region.
# CN: AWS region 对应的环境变量名称列表。
REGION_ENV_NAMES = (
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "S3_VECTORS_REGION",
)


def _load_local_env_file(path: Path) -> None:
    """
    EN: Load key-value pairs from the repository-root .env file without overriding shell variables.
    CN: 从仓库根目录的 .env 文件加载键值对，但不覆盖当前 shell 中已经存在的变量。
    """
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or key in os.environ:
            continue
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        os.environ[key] = value


def _first_env(*names: str, default: str | None = None, required: bool = False) -> str | None:
    """
    EN: Return the first non-empty environment variable from the provided aliases.
    CN: 从给定别名中返回第一个非空的环境变量值。
    """
    for name in names:
        value = os.environ.get(name)
        if value is not None and value.strip():
            return value.strip()
    if required:
        raise ValueError(f"Missing required environment variable: one of {', '.join(names)}")
    return default


def _client_error_code(exc: ClientError) -> str:
    """
    EN: Extract a stable AWS error code from a ClientError.
    CN: 从 ClientError 中提取稳定的 AWS 错误码。
    """
    error = exc.response.get("Error", {})
    return str(error.get("Code") or "")


def _build_client(region: str):
    """
    EN: Create an S3 Vectors client for the configured AWS region.
    CN: 为指定的 AWS Region 创建 S3 Vectors 客户端。
    """
    session = boto3.Session(region_name=region)
    return session.client("s3vectors", config=COMMON_BOTO_CONFIG)


def _normalize_vectors(response: dict[str, Any]) -> list[dict[str, Any]]:
    """
    EN: Convert a ListVectors response into a filtered list of vector records.
    CN: 将 ListVectors 的返回值整理成过滤后的向量记录列表。
    """
    vectors = response.get("vectors")
    if not isinstance(vectors, list):
        return []
    return [item for item in vectors if isinstance(item, dict)]


def _list_all_vectors(client: Any, bucket_name: str, index_name: str) -> list[dict[str, Any]]:
    """
    EN: Collect every vector record from the target index by following pagination tokens.
    CN: 通过分页 token 收集目标 index 中的全部向量记录。
    """
    vectors: list[dict[str, Any]] = []
    next_token: str | None = None

    while True:
        request: dict[str, Any] = {
            "vectorBucketName": bucket_name,
            "indexName": index_name,
            "maxResults": 500,
            "returnData": False,
            "returnMetadata": False,
        }
        if next_token:
            request["nextToken"] = next_token

        response = client.list_vectors(**request)
        page_vectors = _normalize_vectors(response)
        vectors.extend(page_vectors)

        next_token = response.get("nextToken") or None
        if not next_token:
            break

    return vectors


def resolve_runtime_settings() -> tuple[str, str, str]:
    """
    EN: Load local env files and resolve the bucket, index, and region settings.
    CN: 加载本地 env 文件，并解析 bucket、index 和 region 配置。
    """
    _load_local_env_file(REPO_ROOT / ".env")

    bucket_name = _first_env(*VECTOR_BUCKET_ENV_NAMES, required=True)
    index_name = _first_env(*VECTOR_INDEX_ENV_NAMES, required=True)
    region = _first_env(*REGION_ENV_NAMES, default="us-east-1") or "us-east-1"
    return bucket_name, index_name, region


def detect_vector_content(client: Any, bucket_name: str, index_name: str) -> dict[str, Any]:
    """
    EN: Check whether the target vector index exists and contains at least one vector.
    CN: 检查目标 vector index 是否存在，并且至少包含一个向量。
    """
    try:
        client.get_vector_bucket(vectorBucketName=bucket_name)
    except ClientError as exc:
        if _client_error_code(exc) in {"NotFoundException", "NoSuchBucket", "ResourceNotFoundException"}:
            return {
                "status": "missing_bucket",
                "has_vectors": False,
                "bucket_name": bucket_name,
                "index_name": index_name,
            }
        raise

    try:
        client.get_index(vectorBucketName=bucket_name, indexName=index_name)
    except ClientError as exc:
        if _client_error_code(exc) in {"NotFoundException", "ResourceNotFoundException"}:
            return {
                "status": "missing_index",
                "has_vectors": False,
                "bucket_name": bucket_name,
                "index_name": index_name,
            }
        raise

    vectors = _list_all_vectors(client, bucket_name, index_name)
    if not vectors:
        return {
            "status": "empty",
            "has_vectors": False,
            "bucket_name": bucket_name,
            "index_name": index_name,
            "total_vectors": 0,
            "vector_keys": [],
            "sample_vector_key": None,
        }

    vector_keys = [str(item.get("key") or "") for item in vectors if item.get("key")]
    return {
        "status": "has_vectors",
        "has_vectors": True,
        "bucket_name": bucket_name,
        "index_name": index_name,
        "total_vectors": len(vectors),
        "vector_keys": vector_keys,
        "sample_vector_key": vector_keys[0] if vector_keys else None,
    }


def main() -> int:
    """
    EN: Run the local S3 Vectors content check and print a JSON summary.
    CN: 运行本地 S3 Vectors 内容检测，并输出 JSON 汇总。
    """
    try:
        bucket_name, index_name, region = resolve_runtime_settings()
    except ValueError as exc:
        print(
            json.dumps(
                {
                    "status": "config_error",
                    "has_vectors": False,
                    "error": str(exc),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1

    try:
        client = _build_client(region)
        result = detect_vector_content(client, bucket_name, index_name)
    except ClientError as exc:
        print(
            json.dumps(
                {
                    "status": "aws_error",
                    "has_vectors": False,
                    "error_code": _client_error_code(exc),
                    "error": str(exc),
                    "bucket_name": bucket_name,
                    "index_name": index_name,
                    "region": region,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1

    result["region"] = region
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
