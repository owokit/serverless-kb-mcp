#!/usr/bin/env python3
"""
AI Skills Synchronization Script

Reads skills from ai/skills-src/ and generates tool-specific skill directories:
- .agents/skills/  (Codex)
- .claude/skills/  (Claude Code)

Only the common frontmatter subset (name, description) is preserved in generated
SKILL.md files to ensure cross-tool compatibility.
"""

import sys
import shutil
from pathlib import Path


# Common frontmatter fields to preserve in generated SKILL.md
COMMON_FRONTMAITER_FIELDS = {"name", "description"}


def extract_frontmatter(content: str) -> tuple[dict, str]:
    """Extract YAML frontmatter from markdown content."""
    lines = content.split("\n")
    if len(lines) < 3 or lines[0] != "---":
        return {}, content

    frontmatter_end = -1
    for i, line in enumerate(lines[1:], start=1):
        if line == "---":
            frontmatter_end = i
            break

    if frontmatter_end == -1:
        return {}, content

    frontmatter = {}
    for line in lines[1:frontmatter_end]:
        if ":" in line:
            key, _, value = line.partition(":")
            frontmatter[key.strip()] = value.strip().strip('"').strip("'")

    body = "\n".join(lines[frontmatter_end + 1:])
    return frontmatter, body


def build_frontmatter(fields: dict) -> str:
    """Build YAML frontmatter from common fields only."""
    parts = []
    for key in sorted(fields):
        if key in COMMON_FRONTMAITER_FIELDS and fields[key]:
            parts.append(f"{key}: {fields[key]}")
    return "---\n" + "\n".join(parts) + "\n---\n\n"


def generate_skill(skill_path: Path, output_dir: Path) -> None:
    """Generate a skill SKILL.md in the output directory."""
    skill_name = skill_path.parent.name
    skill_file = skill_path

    if not skill_file.exists():
        return

    content = skill_file.read_text(encoding="utf-8")
    frontmatter, body = extract_frontmatter(content)

    # Filter to common fields only
    filtered_frontmatter = {
        k: v for k, v in frontmatter.items()
        if k in COMMON_FRONTMAITER_FIELDS and v
    }

    # Build new SKILL.md with filtered frontmatter + body
    new_content = build_frontmatter(filtered_frontmatter) + body

    output_file = output_dir / skill_name / "SKILL.md"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(new_content, encoding="utf-8")
    print(f"  Generated: {output_file.relative_to(Path.cwd())}")


def sync_skills(src_dir: Path, dst_dir: Path) -> None:
    """Sync all skills from src to dst."""
    if not src_dir.exists():
        print(f"Source directory not found: {src_dir}")
        return

    skill_count = 0
    for skill_path in src_dir.rglob("SKILL.md"):
        # Only process top-level skill SKILL.md, not nested ones
        if skill_path.parent == src_dir / skill_path.parent.name:
            generate_skill(skill_path, dst_dir)
            skill_count += 1

    print(f"Synced {skill_count} skills to {dst_dir}")


def main():
    repo_root = Path(__file__).parent.parent.parent.resolve()
    src_dir = repo_root / "ai" / "skills-src"
    agents_dir = repo_root / ".agents" / "skills"
    claude_dir = repo_root / ".claude" / "skills"

    print(f"Repository root: {repo_root}")
    print(f"Source skills: {src_dir}")

    # Clean existing generated directories
    for d in [agents_dir, claude_dir]:
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True)

    print("\nGenerating .agents/skills/...")
    sync_skills(src_dir, agents_dir)

    print("\nGenerating .claude/skills/...")
    sync_skills(src_dir, claude_dir)

    print("\nGenerating .codex/skills/...")
    codex_dir = repo_root / ".codex" / "skills"
    if codex_dir.exists():
        shutil.rmtree(codex_dir)
    codex_dir.mkdir(parents=True)
    sync_skills(src_dir, codex_dir)

    print("\nDone.")


if __name__ == "__main__":
    main()
