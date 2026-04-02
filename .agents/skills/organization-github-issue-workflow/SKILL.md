---
name: organization-github-issue-workflow
description: 组织级 GitHub issue/PR 工作流，围绕组织级共享仓库的模板与 skills 源文件，先建 issue 再改动，并强制使用仓库内模板。
---

# GitHub Issue 工作流

## 目的

- 把 issue 作为工作入口，先记录问题，再实现和验证。
- 当前组织级共享仓库的 issue 通常围绕模板、skills、同步脚本、fan-out 和流程约束，而不是产品功能本身。
- 所有可归类的 issue 优先使用 `.github/ISSUE_TEMPLATE/`。
- 创建 issue 时必须先选模板，再按模板字段逐项填写正文；禁止用自定义的“摘要 / 影响 / 建议 / 验证”骨架替代仓库模板。
- 创建 issue 时要先核对仓库当前标签体系与模板 frontmatter 是否一致；如果标签缺失或命名不一致，先修正模板与标签体系，再继续处理。
- 创建 issue 后要再打开 GitHub 页面或用 MCP 回看一次标题、正文、标签和模板字段；如果发现乱码、错码、字符丢失或明显编码异常，先修复再继续。
- issue 标题统一使用 `[type] 简短摘要`。
- 提交 PR 时必须使用 `.github/pull_request_template.md`。

## 标准流程

1. 先判断是否应该建 issue
   - 代码、配置、迁移、回归、发布、文档变更、skills 变更都优先先建 issue。
   - 信息不足时，先建 `triage` issue。
2. 先用 GitHub MCP
   - 创建、更新、评论、关闭 issue 优先使用 GitHub MCP。
   - MCP 不可用或结果不完整时，再用 `gh`。
3. 再选模板
   - 优先从 `.github/ISSUE_TEMPLATE/README.md` 查看模板总览。
   - 直接使用 `.github/ISSUE_TEMPLATE/` 下的模板，不要新造空白 issue。
   - issue 正文必须按模板字段填写，不得把模板改写成另一套固定骨架。
   - 标题固定为 `[type] 简短摘要`。
4. 再填默认标签
   - 先按模板 frontmatter 里的 `labels` 预期处理。
   - 再核对 GitHub 实际标签是否存在且已写入 issue。
   - 如果当前仓库标签体系与模板 frontmatter 不一致，先修模板或标签，不要靠记忆补写一套新映射。
5. 必要时拆 sub issue
   - 复杂问题可以拆成多个 sub issue。
   - 父 issue 只有在所有 sub issue 关闭后才允许关闭。
6. 记录节点和进度
   - 保持 issue 进度评论简短、连续、可追踪。
   - 统一使用 `Checkpoint`、`Change`、`Remaining`、`Blocker`、`Evidence`。
7. 关联 PR
   - PR 正文前部必须以普通文本写明 `Closes #123`、`Fixes #123`、`Resolves #123` 或跨仓库引用，不能放进反引号、代码块或引用块。
   - 如果一个 PR 同时覆盖多个 issue，正文必须逐条列出每个 issue 的处理状态，并区分“已被本 PR 完全关闭”和“仅部分覆盖、仍需后续 issue 处理”。
   - 对当前仓库来说，如果 PR 改了 `skills-src/organization/`，正文里还要说明是否同步了下游视图、是否运行了同步脚本，以及是否还需要人工刷新生成物。
   - 创建 PR 前先填满 `.github/pull_request_template.md`。
   - PR 正文必须按模板逐字段完整填写，不接受空白 PR、只有一句话摘要，或自定义的"摘要 / 影响 / 验证"骨架。
   - PR 打开后要再回看一次 GitHub 上实际显示的标题、正文和模板字段；如果发现乱码、错码、字符丢失或明显编码异常，先修复再请求 review。
   - PR 打开后如果缺模板字段，先补齐再请求 review。
   - 如果当前仓库存在自动关闭 workflow，手工流程只负责把关闭关键词和覆盖范围写正确，不要额外发明“补关规则”。
   - 对于模板仓库，若 PR 只覆盖了某个大改动的一部分，必须在 PR 正文里写清楚剩余部分由哪个 issue 或后续 PR 继续处理，避免让变更范围看起来比实际更大。

## 模板总览

优先以 `.github/ISSUE_TEMPLATE/README.md` 为索引。编号只是文件排序，不代表优先级；实际选择按问题语义匹配。

> 说明：表中的 `labels` 是模板预期值，提交前仍要核对仓库里实际可用的标签是否已经对齐。

| 类型 | 位置 | 适用场景 | 标题前缀 | 默认标签 |
| --- | --- | --- | --- | --- |
| 缺陷模板 | `.github/ISSUE_TEMPLATE/01-bug-report.md` | 回归、异常、验证失败、行为偏差 | `[bug]` | `type: bug` + `status: needs-triage` |
| 功能模板 | `.github/ISSUE_TEMPLATE/02-feature-request.md` | 新能力、增强、产品诉求 | `[feature]` | `type: feature` + `status: needs-triage` |
| 任务模板 | `.github/ISSUE_TEMPLATE/03-task.md` | 清理、迁移、跟进、拆解、明确交付 | `[task]` | `type: task` + `status: needs-triage` |
| 梳理模板 | `.github/ISSUE_TEMPLATE/04-triage.md` | 信息不全、需要补充上下文、先定性再处理 | `[triage]` | `status: needs-triage` |
| 文档模板 | `.github/ISSUE_TEMPLATE/05-docs.md` | 文档补充、修订、同步、术语统一 | `[docs]` | `type: docs` + `status: needs-triage` |
| 安全模板 | `.github/ISSUE_TEMPLATE/06-security.md` | 漏洞、风险、加固、审计、权限问题 | `[security]` | `type: security` + `status: needs-triage` |

## 模板选择规则

- 能明确归类时，直接用最贴近语义的模板。
- 信息不全时，先用 `triage`。
- 涉及安全风险、攻击面、权限、泄漏、加固时，优先 `security`。
- 涉及文档和规范同步时，优先 `docs`。
- `bug`、`feature`、`task` 不要混用到同一个 issue 标题里。

## 创建清单

1. 先确认问题类型。
2. 再选模板。
3. 填最少必填项。
4. 加默认标签。
5. 判断是否需要拆 sub issue。
6. 补一条 `triage` 或进度评论。
7. 记录 issue 编号和 sub issue 编号。
8. 再开始实现。

## PR 清单

1. 先读 `.github/pull_request_template.md`。
2. 把模板内容完整映射到 PR 正文。
3. 填写变更摘要、关联 issue、验证、风险和回滚。
4. 在 PR 正文前部写清 `Closes #123` / `Fixes #123`。
5. 如果涉及文档、workflow 或配置，也要写明影响范围。
6. 提交前确认没有残留占位符。
7. 进入 review 前再次核对模板完整性。

## 评论格式

统一使用 `Checkpoint`、`Change`、`Remaining`、`Blocker`、`Evidence`。

## 关闭规则

- 只有所有明确关联的子 issue 都关闭后，主 issue 才能关闭。
- 若仍有未关闭的关联 issue，主 issue 必须保持打开。
- 关闭前先检查关联链路，必要时用 workflow 或脚本做状态校验。
- 如果这个仓库里的 PR 只是修了模板、skills 或同步规则的一部分，必须在对应 issue 里保留剩余工作，不要把尚未完成的工作误判为已经关闭。
- 如果一个 issue 同时被多个 PR 影响，必须在 issue 评论里写清楚当前到底哪些部分已完成、哪些部分仍在等待同步或复核。
