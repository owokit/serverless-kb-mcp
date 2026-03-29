"""
EN: Tests for the S3 Vectors check reference module covering env loading, alias resolution, and vector detection.
CN: S3 Vectors 检查参考模块的测试，覆盖环境变量加载、别名解析和向量检测。
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from examples.workflows.workflow_reference_only.s3_vectors_check import _load_local_env_file, detect_vector_content, resolve_runtime_settings


pytestmark = pytest.mark.requires_aws


def test_load_local_env_file_reads_aliases_without_overriding_existing_env(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "S3_VECTORS_BUCKET_NAME=file-bucket\n"
        "S3_VECTORS_INDEX_NAME=file-index\n"
        "AWS_REGION=ap-southeast-2\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("S3_VECTORS_BUCKET_NAME", "shell-bucket")
    monkeypatch.delenv("S3_VECTORS_INDEX_NAME", raising=False)
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    monkeypatch.delenv("S3_VECTORS_REGION", raising=False)

    _load_local_env_file(env_file)

    assert os.environ["S3_VECTORS_BUCKET_NAME"] == "shell-bucket"
    assert os.environ["S3_VECTORS_INDEX_NAME"] == "file-index"
    assert os.environ["AWS_REGION"] == "ap-southeast-2"


def test_resolve_runtime_settings_prefers_known_aliases(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "VECTOR_BUCKET_NAME=alias-bucket\n"
        "VECTOR_INDEX_NAME=alias-index\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("S3_VECTORS_BUCKET_NAME", raising=False)
    monkeypatch.delenv("VECTOR_BUCKET_NAME", raising=False)
    monkeypatch.delenv("VECTOR_BUCKET", raising=False)
    monkeypatch.delenv("S3_VECTORS_INDEX_NAME", raising=False)
    monkeypatch.delenv("VECTOR_INDEX_NAME", raising=False)
    monkeypatch.delenv("VECTOR_INDEX", raising=False)
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    monkeypatch.delenv("S3_VECTORS_REGION", raising=False)
    monkeypatch.setattr("examples.workflows.workflow_reference_only.s3_vectors_check.REPO_ROOT", tmp_path)

    bucket_name, index_name, region = resolve_runtime_settings()

    assert bucket_name == "alias-bucket"
    assert index_name == "alias-index"
    assert region == "us-east-1"


def test_detect_vector_content_reports_empty_index() -> None:
    class FakeClient:
        def get_vector_bucket(self, **kwargs):
            return {"vectorBucket": kwargs}

        def get_index(self, **kwargs):
            return {"index": kwargs}

        def list_vectors(self, **kwargs):
            return {"vectors": [], "nextToken": None}

    result = detect_vector_content(FakeClient(), "bucket-a", "index-a")

    assert result["status"] == "empty"
    assert result["has_vectors"] is False
    assert result["total_vectors"] == 0
    assert result["vector_keys"] == []
    assert result["sample_vector_key"] is None


def test_detect_vector_content_reports_present_vector() -> None:
    class FakeClient:
        def get_vector_bucket(self, **kwargs):
            return {"vectorBucket": kwargs}

        def get_index(self, **kwargs):
            return {"index": kwargs}

        def list_vectors(self, **kwargs):
            return {"vectors": [{"key": "doc#v1#chunk#0001"}], "nextToken": None}

    result = detect_vector_content(FakeClient(), "bucket-a", "index-a")

    assert result["status"] == "has_vectors"
    assert result["has_vectors"] is True
    assert result["total_vectors"] == 1
    assert result["vector_keys"] == ["doc#v1#chunk#0001"]
    assert result["sample_vector_key"] == "doc#v1#chunk#0001"


def test_detect_vector_content_follows_pagination_tokens() -> None:
    class FakeClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def get_vector_bucket(self, **kwargs):
            return {"vectorBucket": kwargs}

        def get_index(self, **kwargs):
            return {"index": kwargs}

        def list_vectors(self, **kwargs):
            self.calls.append(kwargs)
            if "nextToken" in kwargs:
                return {"vectors": [{"key": "vector#2"}], "nextToken": None}
            return {"vectors": [], "nextToken": "token-1"}

    client = FakeClient()
    result = detect_vector_content(client, "bucket-a", "index-a")

    assert result["status"] == "has_vectors"
    assert result["total_vectors"] == 1
    assert result["vector_keys"] == ["vector#2"]
    assert len(client.calls) == 2
