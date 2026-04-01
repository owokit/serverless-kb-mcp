from __future__ import annotations

import importlib.util
from pathlib import Path


def load_sync_ai():
    script_path = Path(__file__).resolve().parents[5] / "ai" / "scripts" / "sync-ai.py"
    spec = importlib.util.spec_from_file_location("sync_ai", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_skill(root: Path, relative: str, name: str, description: str) -> None:
    skill_dir = root / relative
    skill_dir.mkdir(parents=True, exist_ok=True)
    (skill_dir / "SKILL.md").write_text(
        f"---\nname: {name}\ndescription: {description}\n---\n\nbody\n",
        encoding="utf-8",
    )


def test_sync_skills_recurses_into_nested_skill_trees(tmp_path: Path) -> None:
    src_dir = tmp_path / "src"
    dst_dir = tmp_path / "dst"

    write_skill(src_dir, "plain-skill", "plain-skill", "plain description")
    write_skill(
        src_dir,
        "organization/nested-skill",
        "organization-nested-skill",
        "nested description",
    )

    sync_ai = load_sync_ai()
    sync_ai.sync_skills(src_dir, dst_dir)

    plain_output = dst_dir / "plain-skill" / "SKILL.md"
    nested_output = dst_dir / "nested-skill" / "SKILL.md"

    assert plain_output.exists()
    assert nested_output.exists()
    assert "name: organization-nested-skill" in nested_output.read_text(encoding="utf-8")
    assert "description: nested description" in nested_output.read_text(encoding="utf-8")
