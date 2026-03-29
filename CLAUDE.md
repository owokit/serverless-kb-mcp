@AGENTS.md

## Claude Code Only

- Use `.claude/rules/` for path-scoped policies.
- Skills are synced from `ai/skills-src/` to `.claude/skills/`.
  Do not edit `.claude/skills/` directly; modify the source and run `python ai/scripts/sync-ai.py`.
- **禁止直接修改** `.claude/skills/` 或 `.agents/skills/`，所有 skill 修改必须通过 `ai/skills-src/` + `python ai/scripts/sync-ai.py`。
