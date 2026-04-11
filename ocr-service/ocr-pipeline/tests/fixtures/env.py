"""
EN: Environment helpers that isolate settings and cache-sensitive tests.
CN: 隔离设置与缓存敏感测试的环境辅助工具。
"""
from __future__ import annotations

from contextlib import contextmanager
from collections.abc import Iterator
import json
import os
from pathlib import Path


@contextmanager
def override_env(mapping: dict[str, str | None]) -> Iterator[None]:
    previous: dict[str, str | None] = {}
    try:
        for key, value in mapping.items():
            previous[key] = os.environ.get(key)
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def write_pipeline_config(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
