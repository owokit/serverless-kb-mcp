"""
EN: Tests for lazy import boundaries in the embed runtime composition root.
CN: embed runtime 组装根的延迟导入边界测试。
"""
from __future__ import annotations

import builtins
import importlib
import sys


def _drop_modules(prefixes: tuple[str, ...]) -> None:
    """
    EN: Remove cached modules so the next import exercises the real import graph.
    CN: 清理缓存模块，确保下一次导入会真正走一遍导入图。
    """
    for name in tuple(sys.modules):
        if name.startswith(prefixes):
            sys.modules.pop(name, None)


def test_embed_runtime_import_does_not_pull_extract_modules(monkeypatch) -> None:
    """
    EN: Importing the embed runtime should not require extract-layer modules or docx.
    CN: 导入 embed runtime 时不应依赖 extract 层模块或 docx。
    """
    _drop_modules(
        (
            "serverless_mcp.runtime.embed_runtime",
            "serverless_mcp.embed",
            "serverless_mcp.runtime.bootstrap",
            "serverless_mcp.runtime.embedding_profiles",
            "serverless_mcp.storage.",
            "serverless_mcp.extract.",
        )
    )

    real_import = builtins.__import__

    def _guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "serverless_mcp.extract.application" or name.startswith("serverless_mcp.extract."):
            raise AssertionError(f"unexpected import during embed runtime init: {name}")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)

    module = importlib.import_module("serverless_mcp.runtime.embed_runtime")

    assert hasattr(module, "build_embed_worker")
    assert "serverless_mcp.extract.application" not in sys.modules
