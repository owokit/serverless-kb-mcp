---
name: organization-github-issue-workflow
description: ??? GitHub issue ???????? issue ????????????????
---

# GitHub Issue 工作流

## 目的

- 把 issue 作为工作入口，先记录问题，再实现和验证。
- 所有可归类的 issue 优先使用 `.github/ISSUE_TEMPLATE/`。
- 创建 issue 时必须先选模板，再按模板字段逐项填写正文；禁止用自定义的“摘要 / 影响 / 建议 / 验证”骨架替代仓库模板。
- 创建 issue 时必须同步附上对应默认标签，创建后要复核标签是否已经真正写入 GitHub issue。
- 创建 issue 后要再打开 GitHub 页面或用 MCP 回看一次标题、正文、标签和模板字段；如果发现乱码、错码、字符丢失或明显编码异常，先修复再继续。
- issue 标题统一使用 `[type] 简短摘要`。
- 提交 PR 时必须使用 `.github/pull_request_template.md`。

## 标准流程

1. 先判断是否应该建 issue
   - 代码、配置、迁移、回归、发布、文档变更都优先先建 issue。
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
   - `bug` -> `type: bug`, `status: needs-triage`
   - `feature` -> `type: feature`, `status: needs-triage`
   - `task` -> `type: task`, `status: needs-triage`
   - `docs` -> `type: docs`, `status: needs-triage`
   - `security` -> `type: security`, `status: needs-triage`
   - `triage` -> `status: needs-triage`
   - 创建后必须复核 labels 是否已经写入 GitHub issue，不能只在本地正文里写规则。
5. 必要时拆 sub issue
   - 复杂问题可以拆成多个 sub issue。
   - 父 issue 只有在所有 sub issue 关闭后才允许关闭。
6. 记录节点和进度
   - 保持 issue 进度评论简短、连续、可追踪。
   - 统一使用 `Checkpoint`、`Change`、`Remaining`、`Blocker`、`Evidence`。
7. 关联 PR
   - PR 正文前部必须以普通文本写明 `Closes #123`、`Fixes #123`、`Resolves #123` 或跨仓库引用，不能放进反引号、代码块或引用块。
   - 如果一个 PR 同时覆盖多个 issue，正文必须逐条列出每个 issue 的处理状态，并区分“已被本 PR 完全关闭”和“仅部分覆盖、仍需后续 issue 处理”。
   - 自动关闭只针对 GitHub 实际识别、且被该 PR 完全覆盖的 issue；如果父 issue 下还有兄弟 leaf 未被覆盖，不能因为父 issue 合并就默认一并关闭，必须继续保持打开并在后续 issue / PR 中追踪。
   - 创建 PR 前先填满 `.github/pull_request_template.md`。
   - PR 正文必须按模板逐字段完整填写，不接受空白 PR、只有一句话摘要，或自定义的"摘要 / 影响 / 验证"骨架。
   - PR 打开后要再回看一次 GitHub 上实际显示的标题、正文和模板字段；如果发现乱码、错码、字符丢失或明显编码异常，先修复再请求 review。
   - PR 打开后如果缺模板字段，先补齐再请求 review。
   - issue 合并后的自动关闭检查由对应 workflow 负责，不再要求在手工流程里额外复核或补关；如果 workflow 暴露出“未自动关闭”的兄弟 issue，先检查 PR 描述是否把关闭关键词写成了普通文本，再检查层级覆盖范围，最后决定是否拆分或补写后续 issue。
   - 若 PR 只覆盖一个父 issue 下的部分 leaf，必须在合并前把剩余 leaf 拆成 sub issue 或后续 issue，并保持原父 issue 打开，不能让一个 PR 误导性地吞掉整棵树。

## 模板总览

优先以 `.github/ISSUE_TEMPLATE/README.md` 为索引。编号只是文件排序，不代表优先级；实际选择按问题语义匹配。

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

- 只有所有 sub issue 都关闭后，父 issue 才能关闭。
- 若仍有未关闭 sub issue，父 issue 必须保持打开。
- 关闭前先检查关联链路，必要时用 workflow 或脚本做状态校验。
- `Issue Hierarchy Guard` 负责校验父子关闭关系，并在最后一个 sub issue 关闭后自动关闭父 issue。
- sub issue 只能挂一个主父 issue；如果同一个 leaf 需要出现在多个父类语境里，不要重复挂载，而是采用“主父 issue + 共享中间父 issue / 跨引用”的方式。
- 其他父类如果只是需要可见性或追踪，不要重新收编该 leaf，而应创建链接、检查清单或 tracking issue 指向主父 issue。
