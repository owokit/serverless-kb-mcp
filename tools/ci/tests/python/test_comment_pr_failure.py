"""
EN: Tests for the workflow failure comment relay helper.
CN: 测试工作流失败回评 relay helper。
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[4] / "tools" / "ci" / "comment_pr_failure.py"


def _load_module():
    """EN: Load the helper module without requiring package installation.
    CN: 不依赖包安装直接加载 helper 模块。
    """
    spec = importlib.util.spec_from_file_location("comment_pr_failure", MODULE_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_list_failed_jobs_paginates_until_all_jobs_are_read(monkeypatch) -> None:
    """EN: Ensure failed job collection keeps fetching later pages.
    CN: 确认失败 job 收集会继续抓取后续分页。
    """
    module = _load_module()
    pages = [
        {
            "total_count": 3,
            "jobs": [
                {"name": "job-a", "conclusion": "failure"},
                {"name": "job-b", "conclusion": "failure"},
            ],
        },
        {
            "total_count": 3,
            "jobs": [
                {"name": "job-c", "conclusion": "failure"},
            ],
        },
    ]
    seen_urls: list[str] = []

    def fake_request_json(url: str, *, token: str, method: str = "GET", data=None):
        seen_urls.append(url)
        return pages[len(seen_urls) - 1]

    monkeypatch.setattr(module, "_request_json", fake_request_json)

    result = module._list_failed_jobs(token="token", repository="owokit/serverless-ocr-s3vectors-mcp", run_id="123")

    assert result == ["job-a", "job-b", "job-c"]
    assert seen_urls == [
        "https://api.github.com/repos/owokit/serverless-ocr-s3vectors-mcp/actions/runs/123/jobs?per_page=100&page=1",
        "https://api.github.com/repos/owokit/serverless-ocr-s3vectors-mcp/actions/runs/123/jobs?per_page=100&page=2",
    ]


def test_main_raises_when_required_pr_comment_fails(monkeypatch) -> None:
    """EN: Required PR comments must fail the workflow when GitHub returns an API error.
    CN: 当 GitHub 返回 API 错误时，必需的 PR 评论必须让 workflow 失败。
    """
    module = _load_module()
    event = {"workflow_run": {"event": "pull_request"}}
    monkeypatch.setattr(module, "_load_event", lambda: event)
    monkeypatch.setattr(module, "_find_pr_number", lambda event, token, repository: 321)
    monkeypatch.setattr(module, "_list_failed_jobs", lambda **kwargs: ["job-a"])
    monkeypatch.setenv("GITHUB_TOKEN", "token")
    monkeypatch.setenv("GITHUB_REPOSITORY", "owokit/serverless-ocr-s3vectors-mcp")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    monkeypatch.setenv("GITHUB_SERVER_URL", "https://github.com")
    monkeypatch.setenv("GITHUB_WORKFLOW", "Docs CI")

    def fake_post_or_update_comment(**kwargs):
        raise module.GitHubRequestError("HTTP 403", status=403)

    monkeypatch.setattr(module, "_post_or_update_comment", fake_post_or_update_comment)

    with pytest.raises(module.GitHubRequestError, match="HTTP 403"):
        module.main(["--workflow-name", "Docs CI"])


def test_main_skips_non_pr_workflow_runs_without_pr_number(monkeypatch) -> None:
    """EN: Non-PR workflow runs may skip comments when no PR exists.
    CN: 非 PR 的 workflow run 在没有 PR 时可以跳过评论。
    """
    module = _load_module()
    event = {"workflow_run": {"event": "push"}}
    monkeypatch.setattr(module, "_load_event", lambda: event)
    monkeypatch.setattr(module, "_find_pr_number", lambda event, token, repository: None)
    monkeypatch.setenv("GITHUB_TOKEN", "token")
    monkeypatch.setenv("GITHUB_REPOSITORY", "owokit/serverless-ocr-s3vectors-mcp")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    monkeypatch.setenv("GITHUB_SERVER_URL", "https://github.com")
    monkeypatch.setenv("GITHUB_WORKFLOW", "Security CI")

    assert module.main(["--workflow-name", "Security CI"]) == 0


def test_main_retries_transient_comment_api_errors(monkeypatch) -> None:
    """EN: Transient API failures should be retried before the helper gives up.
    CN: 瞬时 API 失败应先重试，再决定是否放弃。
    """
    module = _load_module()
    event = {"pull_request": {"number": 17}}
    monkeypatch.setattr(module, "_load_event", lambda: event)
    monkeypatch.setattr(module, "_find_pr_number", lambda event, token, repository: 17)
    monkeypatch.setattr(module, "_list_failed_jobs", lambda **kwargs: [])
    monkeypatch.setattr(module.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setenv("GITHUB_TOKEN", "token")
    monkeypatch.setenv("GITHUB_REPOSITORY", "owokit/serverless-ocr-s3vectors-mcp")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    monkeypatch.setenv("GITHUB_SERVER_URL", "https://github.com")
    monkeypatch.setenv("GITHUB_WORKFLOW", "Docs CI")

    attempts: list[int] = []

    def fake_post_or_update_comment(**kwargs):
        attempts.append(1)
        if len(attempts) < 3:
            raise module.GitHubRequestError("HTTP 503", status=503)
        return "created"

    monkeypatch.setattr(module, "_post_or_update_comment", fake_post_or_update_comment)

    assert module.main(["--workflow-name", "Docs CI"]) == 0
    assert len(attempts) == 3
