# organization-skills

这个仓库是组织级技能包的独立源仓库，保存完整的技能目录树，而不是只存 `SKILL.md`。

## 目录

- `organization-github-cli`
- `organization-github-issue-workflow`
- `organization-mainline-worktree`
- `organization-check-pr`
- `organization-docs-mcp-router`
- `organization-bilingual-programming`

## 说明

- `references/`、`agents/`、`scripts/` 等配套文件会一起保留。
- 消费仓库应通过 `git subtree` 或同步脚本引入，然后再生成 `.agents/skills` 与 `.claude/skills`。
