# 组织级 skills 入口说明

这里存放组织级 Codex skills 的权威源文件说明，以及 fan-out 需要同步的根级说明内容。

## 当前内容

- `organization-github-cli`
- `organization-github-issue-workflow`
- `organization-mainline-worktree`
- `organization-check-pr`
- `organization-docs-mcp-router`
- `organization-bilingual-programming`
- `organization-skill-sync-policy`
- `organization-skills-subtree-mappings.json`
- `ORGANIZATION.SKILLS.md`

## 目录约定

- 每个 skill 独立一个目录。
- skill 目录内保留 `SKILL.md`、`references/`、`agents/`、`scripts/` 等配套文件。
- 组织级源文件通过下游仓库的同步脚本或 subtree fan-out 消费。
- 下游生成视图必须由 `skills-src/organization/organization-skill-sync-policy/scripts/sync-ai.py` 生成，不得手工编辑 `.agents/skills/` 或 `.claude/skills/`。
- `ORGANIZATION.SKILLS.md` 是根级说明文件，也会通过 fan-out 映射同步到目标仓库。

## 说明

- 这里是组织级 skills 和映射文件的源头，不是生成后的安装产物。
- 需要同步下游时，优先保持与这里一致的目录结构和命名。
