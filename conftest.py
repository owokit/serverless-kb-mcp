"""
EN: Make the repository root and service source tree importable during pytest collection.
CN: 在 pytest 收集期间让仓库根目录和服务源码树可导入。
"""

from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SERVICE_SRC = REPO_ROOT / "services" / "ocr-pipeline" / "src"

# EN: Keep the repository root importable so namespace packages under tools/ and examples/ resolve in tests.
# CN: 保持仓库根目录可导入，确保 tests 中的 tools/ 和 examples/ 命名空间包可以解析。
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# EN: Add the packaged service source tree so serverless_mcp imports continue to work after the directory move.
# CN: 加入已打包的服务源码树，确保目录迁移后 serverless_mcp 导入仍然可用。
if str(SERVICE_SRC) not in sys.path:
    sys.path.insert(0, str(SERVICE_SRC))
