# Dependabot Auto Merge Note

本仓库的 Dependabot 自动合并流程只负责开启 GitHub auto-merge，不会尝试让 GitHub Actions 直接 approve PR review。

原因很简单：`GITHUB_TOKEN` 不能调用 `addPullRequestReview` 提交 `approve` 类型的 review。即使 workflow 运行在 GitHub runner 上，这个权限边界也不会自动放宽。

如果后续确实需要自动审批，必须改用独立的 GitHub App token 或 PAT，并重新评估分支保护规则。
