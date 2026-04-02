# 组织级 skills 源

这里存放组织级 Codex skills 的唯一权威源文件。

## 当前内容

- `organization-github-cli`
- `organization-github-issue-workflow`
- `organization-mainline-worktree`
- `organization-check-pr`
- `organization-docs-mcp-router`
- `organization-bilingual-programming`
- `organization-skill-sync-policy`

## 目录约定

- 每个 skill 独立一个目录。
- skill 目录内保留 `SKILL.md`、`references/`、`agents/`、`scripts/` 等配套文件。
- 组织级源文件通过下游仓库的同步脚本或 subtree 引用消费。
- 下游生成视图必须由 `skills-src/organization/organization-skill-sync-policy/scripts/sync-ai.py` 从这里生成，不得手工编辑 `.agents/skills/` 或 `.claude/skills/`。

## 说明

- 这里是组织级 skills 的源头，不是生成后的安装产物。
- 需要下游同步时，优先保持与这里一致的目录结构和命名。

