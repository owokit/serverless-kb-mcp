@AGENTS.md

## Claude Code Only

- Use `.claude/rules/` for path-scoped policies.
- Skills are synced from `ai/skills-src/` to `.claude/skills/`.
  Do not edit `.claude/skills/` directly; modify the source and run `python ai/scripts/sync-ai.py`.
