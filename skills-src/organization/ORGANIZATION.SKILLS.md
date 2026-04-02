# 组织级 skills 入口说明

这里存放组织级 Codex skills 的权威源文件说明，以及 fan-out 需要同步的根级说明内容。这个目录本身属于组织级共享仓库，因此这里的文案要围绕“模板、同步、worktree、GitHub 流程、生成视图”来写，不要写成单一产品仓库的功能说明。

## 术语约定

- **组织级共享仓库**：持有 `skills-src/organization/`、`AGENTS.md`、issue / PR 模板和同步规则的权威仓库。
- **消费仓库**：同步或消费这些组织级 skills、模板和规则的下游仓库。
- **源目录**：`skills-src/organization/`。
- **生成视图**：`.agents/skills/` 与 `.claude/skills/`。
- **模板入口**：`.github/ISSUE_TEMPLATE/README.md` 和 `.github/pull_request_template.md`。
- **同步约束**：`skills-src/organization/organization-skill-sync-policy/` 及其同步脚本。

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
- 根目录 `AGENTS.md`
- `.github/ISSUE_TEMPLATE/README.md`
- `.github/pull_request_template.md`

## 目录约定

- 每个 skill 独立一个目录。
- skill 目录内保留 `SKILL.md`、`references/`、`agents/`、`scripts/` 等配套文件。
- 组织级源文件通过下游仓库的同步脚本或 subtree fan-out 消费。
- 下游生成视图必须由 `skills-src/organization/organization-skill-sync-policy/scripts/sync-ai.py` 生成，不得手工编辑 `.agents/skills/` 或 `.claude/skills/`。
- `ORGANIZATION.SKILLS.md` 是根级说明文件，也会通过 fan-out 映射同步到目标仓库。

## 说明

- 这里是组织级 skills 和映射文件的源头，不是生成后的安装产物。
- 需要同步下游时，优先保持与这里一致的目录结构和命名。
- 如果技能文案、模板文案或 AGENTS 里的说法出现冲突，优先修正源文件，再重新同步。
- 如果文案里必须出现仓库身份，优先使用“当前组织级共享仓库”“消费仓库”或 `<owner>/<repo>` 占位符，不要写死某个具体仓库名。
