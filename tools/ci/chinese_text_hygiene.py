"""
EN: Shared scanner for Simplified Chinese comment hygiene and mojibake detection.
CN: 用于简体中文注释卫生与乱码检测的共享扫描器。
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path

PRIVATE_USE_RE = re.compile(r"[\ue000-\uf8ff]")
QUESTION_RUN_RE = re.compile(r"\?{3,}")
PLACEHOLDER_PHRASES = ("\u89c1\u4e0a\u65b9\u82f1\u6587\u8bf4\u660e",)
REPLACEMENT_CHAR = "\ufffd"
SCANNED_SUFFIXES = {".py", ".yml", ".yaml", ".js", ".mjs", ".ts", ".toml", ".ini", ".md"}
IGNORED_DIRS = {"node_modules", ".venv", "__pycache__"}


def iter_target_files(repo_root: Path, base_ref: str) -> list[Path]:
    """EN: Return the PR diff files as paths relative to the repo root.
    CN: 返回相对于仓库根目录的 PR 差异文件路径列表。
    """
    candidates: set[Path] = set()
    git_commands = [
        ["git", "diff", "--name-only", "--diff-filter=ACMRT", f"{base_ref}...HEAD"],
        ["git", "diff", "--name-only", "--diff-filter=ACMRT", "--cached"],
        ["git", "diff", "--name-only", "--diff-filter=ACMRT"],
        ["git", "ls-files", "--others", "--exclude-standard"],
    ]

    try:
        for command in git_commands:
            completed = subprocess.run(
                command,
                cwd=repo_root,
                check=True,
                capture_output=True,
                text=True,
            )
            for line in completed.stdout.splitlines():
                rel_path = Path(line)
                if line and rel_path.suffix in SCANNED_SUFFIXES and not any(part in IGNORED_DIRS for part in rel_path.parts):
                    candidates.add(rel_path)
    except (OSError, subprocess.CalledProcessError):
        return [
            path.relative_to(repo_root)
            for path in repo_root.rglob("*")
            if path.is_file()
            and ".git" not in path.parts
            and path.suffix in SCANNED_SUFFIXES
            and not any(part in IGNORED_DIRS for part in path.parts)
        ]

    return sorted(candidates, key=lambda path: path.as_posix())


def classify_line(line: str) -> str | None:
    """EN: Classify a line if it contains a hygiene issue.
    CN: 如果行内包含卫生问题，则返回对应的分类标识。
    """
    if any(marker in line for marker in PLACEHOLDER_PHRASES):
        return "placeholder_cn_text"
    if PRIVATE_USE_RE.search(line):
        return "private_use_character"
    if REPLACEMENT_CHAR in line:
        return "replacement_character"
    if QUESTION_RUN_RE.search(line):
        return "question_mark_run"
    return None


def suggest_fix(previous_non_empty: str, signal: str) -> str:
    """EN: Suggest a direct remediation for the detected signal.
    CN: 针对检测到的信号给出直接的修复建议。
    """
    if signal == "placeholder_cn_text":
        return "Rewrite the CN line as readable Chinese prose instead of a placeholder."
    if previous_non_empty.startswith("EN:"):
        return "Rewrite the matching CN line from the EN source above, then save the file as UTF-8."
    return "Re-enter the intended Simplified Chinese text and save the file as UTF-8."


def display_path(repo_root: Path, path: Path) -> str:
    """EN: Render a stable display path for reports.
    CN: 为报告渲染稳定的展示路径。
    """
    try:
        return path.relative_to(repo_root).as_posix()
    except ValueError:
        return path.as_posix()


def scan_text(text: str, *, repo_root: Path, path: Path) -> list[dict[str, object]]:
    """EN: Scan one UTF-8 text blob and return all hygiene findings.
    CN: 扫描一段 UTF-8 文本并返回全部卫生问题。
    """
    findings: list[dict[str, object]] = []
    previous_non_empty = ""

    for lineno, line in enumerate(text.splitlines(), start=1):
        signal = classify_line(line)
        if signal is not None:
            finding: dict[str, object] = {
                "file": display_path(repo_root, path),
                "line": lineno,
                "signal": signal,
                "severity": "high",
                "snippet": line.strip(),
                "suggested_fix": suggest_fix(previous_non_empty, signal),
            }
            if previous_non_empty.startswith("EN:"):
                finding["source_en"] = previous_non_empty[3:].strip()
            findings.append(finding)

        if line.strip():
            previous_non_empty = line.strip()

    return findings


def scan_file(repo_root: Path, rel_path: Path) -> list[dict[str, object]]:
    """EN: Load a tracked file and scan it as UTF-8 text.
    CN: 读取已跟踪文件并按 UTF-8 文本扫描。
    """
    path = repo_root / rel_path
    try:
        text = path.read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        return []
    return scan_text(text, repo_root=repo_root, path=path)


def scan_repo(repo_root: Path, base_ref: str = "origin/main") -> list[dict[str, object]]:
    """EN: Scan the current PR diff against the given base reference.
    CN: 按给定基线扫描当前 PR 的差异文件。
    """
    findings: list[dict[str, object]] = []
    for rel_path in iter_target_files(repo_root, base_ref):
        if rel_path.as_posix() == ".github/workflows/guardrails.yml":
            continue
        findings.extend(scan_file(repo_root, rel_path))
    return findings


def build_report(findings: list[dict[str, object]], check_name: str) -> dict[str, object]:
    """EN: Build the JSON report consumed by CI.
    CN: 构建供 CI 消费的 JSON 报告。
    """
    return {
        "workflow": "Guardrails",
        "check": check_name,
        "status": "passed" if not findings else "failed",
        "summary": {
            "files": len({finding["file"] for finding in findings}),
            "findings": len(findings),
        },
        "findings": findings,
    }


def parse_args() -> argparse.Namespace:
    """EN: Parse command-line arguments for the scanner.
    CN: 解析扫描器的命令行参数。
    """
    parser = argparse.ArgumentParser(description="Scan Simplified Chinese text hygiene issues.")
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Repository root to scan.",
    )
    parser.add_argument(
        "--base-ref",
        default="origin/main",
        help="Base reference used to compute the PR diff.",
    )
    parser.add_argument(
        "--report-path",
        type=Path,
        default=Path("guardrails-chinese-report.json"),
        help="JSON report path to write.",
    )
    parser.add_argument(
        "--check-name",
        default="chinese_mojibake",
        help="Logical check name to record in the report.",
    )
    return parser.parse_args()


def main() -> int:
    """EN: Run the scanner, print the report, and fail on findings.
    CN: 运行扫描器，打印报告，并在发现问题时返回失败。
    """
    args = parse_args()
    repo_root = args.repo_root.resolve()
    findings = scan_repo(repo_root, base_ref=args.base_ref)
    report = build_report(findings, args.check_name)
    args.report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 1 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main())
