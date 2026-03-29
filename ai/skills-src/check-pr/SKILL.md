---
name: check-pr
description: 检查 GitHub PR 的 review comments、状态检查和描述完整性，并核对是否使用仓库 PR 模板。
license: MIT
compatibility: Requires git and gh (GitHub CLI) installed and authenticated.
metadata:
  author: owokit
  version: "1.0"
allowed-tools: Bash(gh:*) Bash(git:*)
---

# Check PR

分析一个 PR 的 review comments、status checks 和正文完整性，并帮助补齐问题。

## 输入

- **PR number**（可选）：如果不传，检测当前分支对应的 PR。

## 说明

### 1. 识别 PR

```bash
gh pr view --json number,headRefName -q '{number: .number, branch: .headRefName}'
```

如果不在 PR 分支上，先切到对应分支。

### 2. 获取 PR 详情

```bash
gh pr view <PR_NUMBER> --json title,body,state,reviews,comments,headRefName,statusCheckRollup
gh api repos/{owner}/{repo}/pulls/<PR_NUMBER>/comments
```

### 3. 等待检查完成

分析前先确认所有 status checks 都已完成。如果存在 `PENDING` 或 `IN_PROGRESS`，每 30 秒轮询一次，直到全部进入终态。这能确保 bot 评论和 lint 结果都已就绪。

### 4. 分析 PR

完成后检查以下几项：

#### A. Status Checks

- CI 是否全部通过
- 如果失败，记录失败项和原因

#### B. PR Description

- PR 正文是否完整
- 是否使用了仓库的 `.github/pull_request_template.md`
- 是否保留了模板要求的核心段落
- 是否存在 TODO、占位符或空白段落
- 是否在正文前部以普通文本写明 `Closes #123` / `Fixes #123` / `Resolves #123`，而不是放进反引号、代码块或引用块
- 是否在提交后重新查看过 GitHub 上实际显示的标题、正文和模板字段，确认没有乱码、错码、字符丢失或明显编码异常
- 如果是多 issue PR，是否逐条区分“已被本 PR 完全覆盖”与“仅部分覆盖、仍需继续保持打开”的 issue；凡是没有明确分层的描述，都视为可能掩盖实际关闭范围的可操作问题

#### C. Review Comments

- 需要处理的 inline review comments
- bot review comments，例如 `lint bot` 等
- 人类 reviewer 评论

#### D. General Comments

- 普通讨论评论
- 仅供参考的 bot 评论，例如预览部署信息

### 5. 分类问题

对每个问题分类：

| Category | Meaning |
|---|---|
| **Actionable** | 需要改代码、补测试或修正文档 |
| **Informational** | 仅供参考，不需要改动 |
| **Already addressed** | 后续提交已经解决 |

### 6. 输出结果

用表格总结：

| Area | Issue | Status | Action Needed |
|------|-------|--------|---------------|
| Status Checks | CI build failing | Failing | Fix type error in `src/api.ts` |
| Review | "Add null check" - @reviewer | Actionable | Add guard clause |
| Description | Missing template section | Actionable | Fill in PR template |
| Review | "Looks good" - @teammate | Informational | None |

### 7. 需要时修复

如果有可执行项：

1. 切到 PR 分支
2. 询问是否需要修复
3. 如果需要，直接修改、提交、推送

### 8. 解决 review thread

修复完后，关闭对应线程。

先拉取未解决线程 ID：

```bash
gh api graphql -f query='
query($cursor: String) {
  repository(owner: "OWNER", name: "REPO") {
    pullRequest(number: PR_NUMBER) {
      reviewThreads(first: 100, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          isResolved
          comments(first: 1) {
            nodes { body path author { login } }
          }
        }
      }
    }
  }
}'
```

如果 `hasNextPage` 为 true，继续用 `cursor=ENDCURSOR` 拉取剩余线程。

然后批量 resolve：

```bash
gh api graphql -f query='
mutation {
  t1: resolveReviewThread(input: {threadId: "ID1"}) { thread { isResolved } }
  t2: resolveReviewThread(input: {threadId: "ID2"}) { thread { isResolved } }
}'
```

### 9. 多个 PR

如果要检查一串 PR，按顺序处理。

## 输出格式

总结：

- PR 标题和状态
- status checks 结果
- 发现的问题数量
- 需要处理的事项
- 可以忽略的事项
- 需要继续保持打开的兄弟 issue
- 下一步建议
