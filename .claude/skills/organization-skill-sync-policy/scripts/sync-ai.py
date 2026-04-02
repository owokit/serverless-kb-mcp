#!/usr/bin/env python3
"""
Synchronize organization skill source trees into tool-specific generated views.

This repository keeps the authoritative skill sources under `skills-src/organization/`.
The generated views are committed so Codex and Claude Code can consume them
without knowing about the source layout:

- `.agents/skills/`
- `.claude/skills/`

This generator only accepts skill packs whose directory names start with
`organization`. That keeps the script scoped to the organization-owned skill
namespace and prevents it from being used for unrelated skills.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path


ORG_PREFIX = "organization"


def find_repo_root(start_dir: Path) -> Path:
    """Find the repository root by walking upward until `skills-src/organization/` exists."""
    for candidate in [start_dir, *start_dir.parents]:
        if (candidate / "skills-src" / "organization").is_dir():
            return candidate
    raise FileNotFoundError(
        f"Unable to locate repository root from {start_dir}; expected a parent containing skills-src/organization/"
    )


def find_skill_roots(src_dir: Path) -> list[Path]:
    """Return directories that contain a top-level SKILL.md."""
    roots: list[Path] = []
    for skill_file in sorted(src_dir.rglob("SKILL.md")):
        roots.append(skill_file.parent)
    return roots


def validate_skill_roots(skill_roots: list[Path]) -> None:
    """Ensure only organization-prefixed skill packs are synchronized."""
    invalid_roots = [root for root in skill_roots if not root.name.startswith(ORG_PREFIX)]
    if invalid_roots:
        invalid_list = ", ".join(sorted(root.name for root in invalid_roots))
        raise RuntimeError(
            "This sync script only supports organization-prefixed skills; "
            f"found unsupported skill roots: {invalid_list}"
        )


def clear_generated_dir(dst_dir: Path) -> None:
    """Remove and recreate a generated destination directory."""
    if dst_dir.exists():
        shutil.rmtree(dst_dir)
    dst_dir.mkdir(parents=True, exist_ok=True)


def copy_skill_tree(src_root: Path, dst_root: Path, repo_root: Path) -> None:
    """Copy one skill tree from the source root into a generated view."""
    relative_root = src_root.relative_to(repo_root / "skills-src" / "organization")
    destination = dst_root / relative_root
    shutil.copytree(src_root, destination, dirs_exist_ok=True)
    print(f"Copied {relative_root.as_posix()} -> {destination.relative_to(repo_root)}")


def sync_views(repo_root: Path, src_dir: Path, view_dirs: list[Path]) -> None:
    """Sync all generated views from the source skill tree."""
    if not src_dir.exists():
        raise FileNotFoundError(f"Source directory not found: {src_dir}")

    skill_roots = find_skill_roots(src_dir)
    if not skill_roots:
        raise RuntimeError(f"No SKILL.md files found under {src_dir}")

    validate_skill_roots(skill_roots)

    for view_dir in view_dirs:
        clear_generated_dir(view_dir)

    for src_root in skill_roots:
        for view_dir in view_dirs:
            copy_skill_tree(src_root, view_dir, repo_root)


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repo-root",
        default=None,
        help="Repository root. Defaults to the nearest parent that contains skills-src/organization/.",
    )
    return parser


def main() -> int:
    """Entry point."""
    args = build_parser().parse_args()
    script_root = Path(__file__).resolve().parent
    repo_root = Path(args.repo_root).resolve() if args.repo_root else find_repo_root(script_root)

    src_dir = repo_root / "skills-src" / "organization"
    agents_dir = repo_root / ".agents" / "skills"
    claude_dir = repo_root / ".claude" / "skills"

    print(f"Repository root: {repo_root}")
    print(f"Source tree: {src_dir}")
    print(f"Generated view: {agents_dir}")
    print(f"Generated view: {claude_dir}")

    sync_views(repo_root, src_dir, [agents_dir, claude_dir])
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
