"""
EN: Tests for the embed runtime composition root and its lazy extraction import boundary.
CN: 测试 embed runtime 组合根及其 extraction 延迟导入边界。
"""

from __future__ import annotations

import builtins
import importlib
import sys


def test_embed_runtime_can_import_without_docx(monkeypatch) -> None:
    """
    EN: The embed runtime should import without requiring docx at module import time.
    CN: embed runtime 在模块导入时不应依赖 docx。
    """
    original_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "docx" or name.startswith("docx."):
            raise ModuleNotFoundError("No module named 'docx'")
        return original_import(name, globals, locals, fromlist, level)

    for module_name in (
        "serverless_mcp.runtime.embed_runtime",
        "serverless_mcp.extract.application",
        "serverless_mcp.extract.extractors",
    ):
        monkeypatch.delitem(sys.modules, module_name, raising=False)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    module = importlib.import_module("serverless_mcp.runtime.embed_runtime")

    assert hasattr(module, "build_embed_worker")
    assert hasattr(module, "build_backfill_service")
