#!/usr/bin/env python3
"""
EN: Post or refresh a PR comment when a workflow run fails.
CN: 当 workflow 失败时，在对应 PR 上发布或刷新评论。
"""
from __future__ import annotations

import argparse
import json
import os
import urllib.error
import urllib.parse
import urllib.request
import time
from pathlib import Path
from typing import Any


COMMENT_MARKER = "<!-- codex-workflow-failure -->"
RETRYABLE_HTTP_STATUS_CODES = {429, 500, 502, 503, 504}


class GitHubRequestError(RuntimeError):
    """EN: Raised when a GitHub API request fails.
    CN: 当 GitHub API 请求失败时抛出。"""

    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


def _load_event() -> dict[str, Any]:
    """
    EN: Load the GitHub Actions event payload from the runner environment.
    CN: 从 runner 环境中读取 GitHub Actions 的事件载荷。
    """
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not event_path:
        return {}
    path = Path(event_path)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _request_json(url: str, *, token: str, method: str = "GET", data: dict[str, Any] | None = None) -> Any:
    """
    EN: Perform an authenticated GitHub REST request and decode JSON response bodies.
    CN: 执行带认证的 GitHub REST 请求，并解析 JSON 响应体。
    """
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "codex-workflow-failure-commenter",
    }
    body = None
    if data is not None:
        headers["Content-Type"] = "application/json"
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")

    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        payload = exc.read().decode("utf-8", errors="replace")
        raise GitHubRequestError(
            f"GitHub API request failed with HTTP {exc.code} for {method} {url}: {payload.strip() or exc.reason}",
            status=exc.code,
        ) from exc
    except urllib.error.URLError as exc:
        raise GitHubRequestError(f"GitHub API request failed for {method} {url}: {exc.reason}") from exc
    if not raw.strip():
        return None
    return json.loads(raw)


def _repo_api_base(repository: str) -> str:
    """
    EN: Build the REST API root for the current repository.
    CN: 为当前仓库构造 REST API 根地址。
    """
    return f"https://api.github.com/repos/{repository}"


def _find_pr_number(event: dict[str, Any], *, token: str, repository: str) -> int | None:
    """
    EN: Resolve the PR number associated with the failing workflow run.
    CN: 收集 workflow run 中失败的 job 名称，让 relay 评论在没有报告产物时仍然有用。
    """
    pull_request = event.get("pull_request")
    if isinstance(pull_request, dict) and isinstance(pull_request.get("number"), int):
        return int(pull_request["number"])

    workflow_run = event.get("workflow_run")
    if isinstance(workflow_run, dict):
        pull_requests = workflow_run.get("pull_requests")
        if isinstance(pull_requests, list):
            for item in pull_requests:
                if isinstance(item, dict) and isinstance(item.get("number"), int):
                    return int(item["number"])

        head_sha = workflow_run.get("head_sha")
        if isinstance(head_sha, str) and head_sha.strip():
            url = f"{_repo_api_base(repository)}/commits/{urllib.parse.quote(head_sha)}/pulls"
            response = _request_json(url, token=token)
            if isinstance(response, list):
                for item in response:
                    if isinstance(item, dict) and isinstance(item.get("number"), int):
                        return int(item["number"])

    return None


def _load_existing_marker_comment(*, token: str, repository: str, pr_number: int, workflow_name: str) -> dict[str, Any] | None:
    """
    EN: Find the latest existing workflow failure comment for the same PR and workflow name.
    CN: 为同一个 PR 和 workflow 名称找到最新的失败评论。
    """
    url = f"{_repo_api_base(repository)}/issues/{pr_number}/comments?per_page=100"
    response = _request_json(url, token=token)
    if not isinstance(response, list):
        return None

    for item in reversed(response):
        if not isinstance(item, dict):
            continue
        body = item.get("body")
        if not isinstance(body, str):
            continue
        if COMMENT_MARKER in body and f"Workflow: `{workflow_name}`" in body:
            return item
    return None


def _list_failed_jobs(*, token: str, repository: str, run_id: str) -> list[str]:
    """
    EN: Collect failed job names for a workflow run so relay comments stay useful without a report artifact.
    CN: 收集 workflow run 中失败的 job 名称，让 relay 评论在没有报告产物时仍然有用。
    """
    failed_jobs: list[str] = []
    page = 1

    while True:
        url = f"{_repo_api_base(repository)}/actions/runs/{run_id}/jobs?per_page=100&page={page}"
        response = _request_json(url, token=token)
        if not isinstance(response, dict):
            return failed_jobs

        jobs = response.get("jobs")
        if not isinstance(jobs, list):
            return failed_jobs

        for item in jobs:
            if not isinstance(item, dict):
                continue
            conclusion = str(item.get("conclusion") or "").lower()
            if conclusion in {"success", "skipped"}:
                continue
            name = item.get("name")
            if isinstance(name, str) and name.strip():
                failed_jobs.append(name.strip())

        total_count = response.get("total_count")
        if isinstance(total_count, int):
            if len(failed_jobs) >= total_count:
                break
        elif len(jobs) < 100:
            break
        page += 1

    return failed_jobs


def _build_comment_body(
    *,
    workflow_name: str,
    run_url: str,
    run_id: str,
    run_attempt: str,
    report_paths: list[str],
    failed_jobs: list[str],
) -> str:
    """
    EN: Render a compact PR comment body with enough evidence for triage.
    CN: 渲染一份简洁但足够 triage 的 PR 评论内容。
    """
    lines = [
        COMMENT_MARKER,
        "Checkpoint",
        f"- Workflow: `{workflow_name}` failed on run `{run_id}` attempt `{run_attempt}`.",
        f"- Run: {run_url}",
        "Change",
        "- The workflow detected a blocking failure and stopped the gate.",
        "Remaining",
        "- Inspect the linked workflow logs and fix the failing step before re-running.",
    ]
    if report_paths:
        lines.append("Evidence")
        for path in report_paths:
            lines.append(f"- Report artifact: `{path}`")
    elif failed_jobs:
        lines.append("Evidence")
        for job in failed_jobs[:10]:
            lines.append(f"- Failed job: `{job}`")
    else:
        lines.append("Evidence")
        lines.append("- No report artifact was available at comment time.")
    return "\n".join(lines)


def _should_require_pr_comment(event: dict[str, Any]) -> bool:
    """EN: Detect whether this run originated from a PR context that must receive a comment.
    CN: 判断本次运行是否源自必须写评论的 PR 上下文。"""
    if isinstance(event.get("pull_request"), dict):
        return True

    workflow_run = event.get("workflow_run")
    if isinstance(workflow_run, dict):
        source_event = str(workflow_run.get("event") or "").strip().lower()
        return source_event == "pull_request"

    return False


def _post_or_update_comment(*, token: str, repository: str, pr_number: int, workflow_name: str, body: str) -> str:
    """EN: Create or update the workflow failure comment and retry transient API failures.
    CN: 创建或更新 workflow 失败评论，并对瞬时 API 故障重试。"""
    existing = _load_existing_marker_comment(
        token=token,
        repository=repository,
        pr_number=pr_number,
        workflow_name=workflow_name,
    )
    if existing and isinstance(existing.get("id"), int):
        comment_id = int(existing["id"])
        url = f"{_repo_api_base(repository)}/issues/comments/{comment_id}"
        _request_json(url, token=token, method="PATCH", data={"body": body})
        return "updated"

    url = f"{_repo_api_base(repository)}/issues/{pr_number}/comments"
    _request_json(url, token=token, method="POST", data={"body": body})
    return "created"


def main(argv: list[str] | None = None) -> int:
    """
    EN: Post or refresh a failure comment for the current workflow event.
    CN: 为当前 workflow 事件发布或刷新失败评论。
    """
    parser = argparse.ArgumentParser(description="Comment on the PR linked to a failed workflow run")
    parser.add_argument("--workflow-name", required=True)
    parser.add_argument("--report-path", action="append", default=[])
    args = parser.parse_args(argv)

    token = os.environ.get("GITHUB_TOKEN")
    repository = os.environ.get("GITHUB_REPOSITORY")
    run_id = os.environ.get("GITHUB_RUN_ID", "unknown")
    run_attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    server_url = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    workflow = os.environ.get("GITHUB_WORKFLOW", args.workflow_name)

    if not token or not repository:
        raise GitHubRequestError("Missing GITHUB_TOKEN or GITHUB_REPOSITORY; cannot post PR failure comment.")

    event = _load_event()
    pr_number = _find_pr_number(event, token=token, repository=repository)
    require_pr_comment = _should_require_pr_comment(event)
    if pr_number is None:
        if require_pr_comment:
            raise GitHubRequestError("No associated pull request was found; cannot post required PR failure comment.")
        print("No associated pull request was found for a non-PR workflow run; skipping PR comment.")
        return 0

    run_url = f"{server_url}/{repository}/actions/runs/{run_id}"
    failed_jobs: list[str] = []
    if not args.report_path:
        failed_jobs = _list_failed_jobs(token=token, repository=repository, run_id=run_id)
    comment_body = _build_comment_body(
        workflow_name=workflow,
        run_url=run_url,
        run_id=run_id,
        run_attempt=run_attempt,
        report_paths=[path for path in args.report_path if path],
        failed_jobs=failed_jobs,
    )

    last_error: GitHubRequestError | None = None
    for attempt in range(3):
        try:
            outcome = _post_or_update_comment(
                token=token,
                repository=repository,
                pr_number=pr_number,
                workflow_name=workflow,
                body=comment_body,
            )
            print(f"{outcome.capitalize()} workflow failure comment on PR #{pr_number}.")
            return 0
        except GitHubRequestError as exc:
            last_error = exc
            if exc.status not in RETRYABLE_HTTP_STATUS_CODES or attempt == 2:
                raise
            wait_seconds = 2**attempt
            print(
                f"Transient GitHub API error on PR failure comment attempt {attempt + 1}/3; "
                f"retrying in {wait_seconds}s: {exc}"
            )
            time.sleep(wait_seconds)

    if last_error is not None:
        raise last_error
    raise GitHubRequestError("Unable to post PR failure comment for an unknown reason.")


if __name__ == "__main__":
    raise SystemExit(main())
