---
name: organization-skill-sync-policy
description: 说明 skills-src/organization 到 .agents/.claude 的生成约束，以及 `skills-src/organization/organization-skill-sync-policy/scripts/sync-ai.py` 的标准用法。
---

# Skill Sync Policy

## 何时使用

当你要新增、修改、删除组织级 skill，或者需要理解 skill 如何从 `skills-src/organization/` 同步到 `.agents/skills/` 和 `.claude/skills/` 时，使用这个 skill。

## 核心约束

- `skills-src/organization/` 是唯一权威源。
- 不要直接编辑 `.agents/skills/`。
- 不要直接编辑 `.claude/skills/`。
- 所有 skill 变更必须先落到 `skills-src/organization/`，然后由 `skills-src/organization/organization-skill-sync-policy/scripts/sync-ai.py` 生成下游视图。
- 这个脚本只允许同步以 `organization*` 开头的 skill 目录。
- 新增 skill 目录时，必须保证顶层存在 `SKILL.md`。

## 标准流程

1. 在 `skills-src/organization/` 下创建或修改 skill。
2. 检查目录结构是否完整，包含需要被同步的 `references/`、`agents/`、`scripts/` 等内容。
3. 运行：

```bash
python skills-src/organization/organization-skill-sync-policy/scripts/sync-ai.py
```

4. 确认 `.agents/skills/` 和 `.claude/skills/` 的生成结果与 `skills-src/organization/` 一致。

## 脚本用法

- 默认会从仓库根目录的 `skills-src/organization/` 读取。
- 默认会刷新 `.agents/skills/` 和 `.claude/skills/`。
- 如果需要从别的目录调用，可以显式传入 `--repo-root`。

```bash
python skills-src/organization/organization-skill-sync-policy/scripts/sync-ai.py --repo-root .
```

## 不要做的事

- 不要把非 `organization*` 的 skill 直接交给这个脚本。
- 不要把新 skill 直接写进 `.agents/skills/` 或 `.claude/skills/`。
- 不要把生成目录当作手工维护目录。
- 不要跳过同步脚本，除非你只是在临时本地验证且不打算提交。

## 结果校验

- 生成视图里应当保留与源目录一致的文件树。
- 新增或修改 skill 后，`SKILL.md`、引用文档和脚本都应同步到两个生成视图。
- 如果生成结果和源目录不一致，优先修正 `skills-src/organization/`，再重新运行脚本。
