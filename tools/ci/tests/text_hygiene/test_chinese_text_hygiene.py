"""
EN: Tests for the shared Chinese text hygiene scanner used by local pytest and CI.
CN: 本地 pytest 与 CI 共用的中文文本卫生扫描器测试。
"""
from __future__ import annotations

from pathlib import Path

from tools.ci.chinese_text_hygiene import scan_repo, scan_text


REPO_ROOT = Path(__file__).resolve().parents[4]


def test_repository_has_no_placeholder_cn_or_mojibake() -> None:
    """EN: Fail the test suite when the real repository contains hygiene issues.
    CN: 当真实仓库里存在卫生问题时让测试套件直接失败。
    """
    assert scan_repo(REPO_ROOT) == []


def test_scan_text_flags_placeholder_cn_text() -> None:
    """EN: The scanner must flag placeholder Chinese text on its own.
    CN: 扫描器必须能够单独识别中文占位句。
    """
    placeholder_cn = "".join(
        [
            "\u89c1",
            "\u4e0a",
            "\u65b9",
            "\u82f1",
            "\u6587",
            "\u8bf4",
            "\u660e",
        ]
    )
    findings = scan_text(
        f"EN: Rewrite the CN line below.\nCN: {placeholder_cn}\n",
        repo_root=REPO_ROOT,
        path=Path("sample.md"),
    )

    assert any(finding["signal"] == "placeholder_cn_text" for finding in findings)
    assert findings[0]["suggested_fix"] == "Rewrite the CN line as readable Chinese prose instead of a placeholder."


def test_scan_text_flags_private_use_character() -> None:
    """EN: The scanner must flag private-use Unicode code points.
    CN: 扫描器必须能够识别私用区 Unicode 字符。
    """
    findings = scan_text(
        "EN: Keep the Chinese line readable.\nCN: \ue000\n",
        repo_root=REPO_ROOT,
        path=Path("sample.md"),
    )

    assert any(finding["signal"] == "private_use_character" for finding in findings)
