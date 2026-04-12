"""
EN: Environment isolation for reference workflow tests.
CN: 参考工作流测试的环境隔离。
"""
from __future__ import annotations

import os

import pytest


@pytest.fixture(autouse=True)
def _restore_environment() -> None:
    """
    EN: Snapshot the process environment and restore it after each reference test.
    CN: 快照进程环境，并在每个参考测试结束后恢复。
    """
    original_environ = os.environ.copy()
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(original_environ)
