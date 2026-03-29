# Skill Sync Notice

## Do Not Edit `.agents/skills/` Directly

All skills in `.agents/skills/` are **generated** from `ai/skills-src/`.

- **Source of truth**: `ai/skills-src/`
- **Generator**: `python ai/scripts/sync-ai.py`

To modify any skill, edit the source in `ai/skills-src/` and run the sync script, then commit both the source changes and the generated output.
