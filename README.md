# Issue 模板索引

本目录下的模板是 issue 的唯一入口。`blank_issues_enabled: false` 已启用，新增 issue 时请直接选择下面的模板之一，不要新建空白 issue。

## 选择原则

- 先按问题语义选模板，再看编号。
- 标题统一使用 `[type] 简短摘要`。
- 信息不全时优先用 `triage`。
- 安全、文档、任务、功能、缺陷分别走对应模板，不要混用。
- issue 正文必须直接填写模板字段，不要用自定义的“摘要 / 影响 / 建议 / 验证”骨架替代模板。
- issue 创建后要再回看一次 GitHub 上实际显示的标题、正文和模板字段；如果发现乱码、错码、字符丢失或明显编码异常，先修复再继续。

## 模板清单

| 文件 | 简要 | 位置 | 适用场景 | 标题前缀 | 默认标签 |
| --- | --- | --- | --- | --- | --- |
| `01-bug-report.md` | 缺陷报告 | `.github/ISSUE_TEMPLATE/01-bug-report.md` | 回归、异常、验证失败、行为偏差 | `[bug]` | `type: bug`, `status: needs-triage` |
| `02-feature-request.md` | 功能需求 | `.github/ISSUE_TEMPLATE/02-feature-request.md` | 新能力、增强、产品诉求 | `[feature]` | `type: feature`, `status: needs-triage` |
| `03-task.md` | 任务模板 | `.github/ISSUE_TEMPLATE/03-task.md` | 清理、迁移、跟进、拆解、明确交付 | `[task]` | `type: task`, `status: needs-triage` |
| `04-triage.md` | 梳理模板 | `.github/ISSUE_TEMPLATE/04-triage.md` | 信息不足，需要先补充上下文 | `[triage]` | `status: needs-triage` |
| `05-docs.md` | 文档模板 | `.github/ISSUE_TEMPLATE/05-docs.md` | 文档补充、修订、同步、术语统一 | `[docs]` | `type: docs`, `status: needs-triage` |
| `06-security.md` | 安全模板 | `.github/ISSUE_TEMPLATE/06-security.md` | 漏洞、风险、加固、审计、权限问题 | `[security]` | `type: security`, `status: needs-triage` |

## 快速用法

1. 先判断是不是缺陷、功能、任务、梳理、文档或安全问题。
2. 打开对应模板，按模板字段填写。
3. 标题用 `[type] 简短摘要`。
4. 如果信息还不够，先用 `04-triage.md`。
5. 创建后补齐必要标签，再进入处理流程。

