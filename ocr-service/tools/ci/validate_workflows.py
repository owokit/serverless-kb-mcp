"""
EN: Validate all GitHub Actions workflow files against the project's naming, trigger, and layering conventions.
CN: 根据项目的命名、触发器和分层约定校验全部 GitHub Actions 工作流文件。
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import yaml


# EN: Repository root resolved three levels above this script.
# CN: 仓库根目录，从本脚本向上三级解析。
REPO_ROOT = Path(__file__).resolve().parents[3]
# EN: Service project root, the physical home of ocr-pipeline, infra, tools, docs, and examples.
# CN: 服务项目根目录，是 ocr-pipeline、infra、tools、docs 和 examples 的实际位置。
SERVICE_ROOT = REPO_ROOT / "ocr-service"
# EN: Directory that contains all GitHub Actions workflow YAML files.
# CN: 存放全部 GitHub Actions 工作流 YAML 文件的目录。
WORKFLOWS_DIR = REPO_ROOT / ".github" / "workflows"
# EN: Shared CI runtime files that all workflows must converge on.
# CN: 所有 workflow 都必须收敛到的共享 CI runtime 文件。
SHARED_RUNTIME_ACTION = REPO_ROOT / ".github" / "actions" / "setup-runtime" / "action.yml"
SHARED_RUNTIME_CONFIG = REPO_ROOT / ".github" / "config" / "ci-runtime.json"
# EN: Explicit allowlist of pinned commit SHA uses, keyed by workflow filename.
# CN: 按工作流文件名索引的 pinned commit SHA 使用白名单。
ALLOWED_PINNED_USES = {
    "dependabot-auto-merge.yml": {
        "dependabot/fetch-metadata@d7267f607e9d3fb96fc2fbe83e0af444713e90b7",
    },
}
# EN: Set of workflow filenames that must exist in .github/workflows/.
# CN: .github/workflows/ 目录下必须存在的工作流文件名集合。
EXPECTED_WORKFLOWS = {
    "workflow-sanity.yml",
    "guardrails.yml",
    "ci-failure-comment-relay.yml",
    "pr-path-conflict-guard.yml",
    "issue-hierarchy-guard.yml",
    "issue-similarity-triage.yml",
    "issue-similarity-closure.yml",
    "logic-ci.yml",
    "contract-ci.yml",
    "local-integration-ci.yml",
    "codeql.yml",
    "stale-issues.yml",
    "merged-branch-cleanup.yml",
    "dependabot-auto-merge.yml",
    "external-validation.yml",
    "docs-ci.yml",
    "security-ci.yml",
    "package-release.yml",
    "prod-deploy.yml",
    "destroy.yml",
}
# EN: Mapping from workflow filename to its required display name.
# CN: 工作流文件名到其必须使用的显示名称的映射。
EXPECTED_NAMES = {
    "workflow-sanity.yml": "Workflow Sanity",
    "guardrails.yml": "Guardrails",
    "ci-failure-comment-relay.yml": "CI Failure Comment Relay",
    "pr-path-conflict-guard.yml": "PR Path Conflict Guard",
    "issue-hierarchy-guard.yml": "Issue Hierarchy Guard",
    "issue-similarity-triage.yml": "Issue Similarity Triage",
    "issue-similarity-closure.yml": "Issue Similarity Closure",
    "logic-ci.yml": "Logic CI",
    "contract-ci.yml": "Contract CI",
    "local-integration-ci.yml": "Local Integration CI",
    "codeql.yml": "CodeQL JavaScript / TypeScript / Python",
    "stale-issues.yml": "Stale Issues",
    "merged-branch-cleanup.yml": "Branch Lifecycle Cleanup",
    "dependabot-auto-merge.yml": "Dependabot Auto Merge",
    "external-validation.yml": "External Validation",
    "docs-ci.yml": "Docs CI",
    "security-ci.yml": "Security CI",
    "package-release.yml": "Package Release",
    "prod-deploy.yml": "Prod Deploy",
    "destroy.yml": "Destroy",
}
# EN: Workflow files that must depend on specific parent workflows via workflow_run triggers.
# CN: 必须通过 workflow_run 触发器依赖特定父工作流的文件。
EXPECTED_WORKFLOW_RUN_PARENTS = {
    "ci-failure-comment-relay.yml": [
        "Workflow Sanity",
        "Guardrails",
        "Logic CI",
        "Contract CI",
        "Local Integration CI",
        "Security CI",
    ],
    "guardrails.yml": ["Workflow Sanity"],
    "logic-ci.yml": ["Guardrails"],
    "contract-ci.yml": ["Logic CI"],
    "local-integration-ci.yml": ["Logic CI"],
    "security-ci.yml": ["Workflow Sanity"],
    "package-release.yml": ["Local Integration CI"],
}
# EN: Required trigger types per workflow filename.
# CN: 每个工作流文件必须包含的触发器类型集合。
EXPECTED_TRIGGER_REQUIREMENTS = {
    "workflow-sanity.yml": {"pull_request", "push", "workflow_dispatch"},
    "guardrails.yml": {"workflow_run", "workflow_dispatch"},
    "ci-failure-comment-relay.yml": {"workflow_run"},
    "pr-path-conflict-guard.yml": {"pull_request_target", "workflow_dispatch"},
    "issue-hierarchy-guard.yml": {"issues", "workflow_dispatch"},
    "issue-similarity-triage.yml": {"issues", "workflow_dispatch"},
    "issue-similarity-closure.yml": {"schedule", "workflow_dispatch"},
    "logic-ci.yml": {"workflow_run", "workflow_dispatch"},
    "contract-ci.yml": {"workflow_run", "workflow_dispatch"},
    "local-integration-ci.yml": {"pull_request", "workflow_run", "workflow_dispatch"},
    "codeql.yml": {"pull_request", "push", "schedule", "workflow_dispatch"},
    "stale-issues.yml": {"schedule", "workflow_dispatch"},
    "merged-branch-cleanup.yml": {"pull_request_target", "schedule", "workflow_dispatch"},
    "dependabot-auto-merge.yml": {"pull_request"},
    "external-validation.yml": {"workflow_dispatch"},
    "docs-ci.yml": {"pull_request", "workflow_dispatch"},
    "security-ci.yml": {"workflow_run", "workflow_dispatch"},
    "package-release.yml": {"workflow_run", "workflow_dispatch"},
    "prod-deploy.yml": {"workflow_dispatch"},
    "destroy.yml": {"workflow_dispatch"},
}


class _WorkflowLoader(yaml.SafeLoader):
    """EN: Custom YAML loader that disables implicit boolean parsing for workflow files.
    CN: 自定义 YAML 加载器，禁用工作流文件中的隐式布尔值解析。"""

    pass


for key, resolvers in list(_WorkflowLoader.yaml_implicit_resolvers.items()):
    _WorkflowLoader.yaml_implicit_resolvers[key] = [
        resolver
        for resolver in resolvers
        if resolver[0] != "tag:yaml.org,2002:bool"
    ]


def main() -> int:
    report: dict[str, object] = {
        "workflows": [],
        "errors": [],
    }
    errors: list[str] = report["errors"]  # type: ignore[assignment]

    discovered = {path.name for path in WORKFLOWS_DIR.glob("*.yml")}
    missing = sorted(EXPECTED_WORKFLOWS - discovered)
    extra = sorted(discovered - EXPECTED_WORKFLOWS)
    if missing:
        errors.append(f"Missing workflow files: {', '.join(missing)}")
    if extra:
        errors.append(f"Unexpected workflow files: {', '.join(extra)}")

    for support_file in (SHARED_RUNTIME_ACTION, SHARED_RUNTIME_CONFIG):
        if not support_file.is_file():
            errors.append(f"Missing shared CI runtime file: {support_file.relative_to(REPO_ROOT)}")

    _assert_shared_runtime_config(errors)

    for path in sorted(WORKFLOWS_DIR.glob("*.yml")):
        text = path.read_text(encoding="utf-8")
        pinned_uses = _find_pinned_uses(text)
        allowed_pinned_uses = ALLOWED_PINNED_USES.get(path.name, set())
        unexpected_pinned_uses = sorted(use for use in pinned_uses if use not in allowed_pinned_uses)
        if unexpected_pinned_uses:
            errors.append(
                f"{path.name} contains a pinned commit SHA in uses: {', '.join(unexpected_pinned_uses)}"
            )
        _assert_bilingual_comments(text, path.name, errors)
        _assert_shared_runtime_usage(text, path.name, errors)
        _assert_python_project_usage(text, path.name, errors)

        workflow = yaml.load(text, Loader=_WorkflowLoader)
        if not isinstance(workflow, dict):
            errors.append(f"{path.name} is not a mapping")
            continue

        workflow_report: dict[str, object] = {
            "file": path.name,
            "name": workflow.get("name"),
            "triggers": sorted(_trigger_names(workflow.get("on"))),
            "runs_on": [],
        }
        report["workflows"].append(workflow_report)

        expected_name = EXPECTED_NAMES.get(path.name)
        if expected_name and workflow.get("name") != expected_name:
            errors.append(f"{path.name} must be named {expected_name!r}")
        if isinstance(workflow.get("name"), str) and re.match(r"^\d", workflow["name"]):
            errors.append(f"{path.name} must not use a numeric workflow name prefix")

        for job_id, job in (workflow.get("jobs") or {}).items():
            runs_on = job.get("runs-on")
            workflow_report["runs_on"].append({job_id: runs_on})
            if _contains_self_hosted(runs_on):
                errors.append(f"{path.name}:{job_id} uses self-hosted runner")

        if path.name in EXPECTED_WORKFLOWS:
            required_trigger = _expected_trigger(path.name)
            if required_trigger and required_trigger not in _trigger_names(workflow.get("on")):
                errors.append(f"{path.name} is missing required trigger {required_trigger!r}")
            required_triggers = EXPECTED_TRIGGER_REQUIREMENTS.get(path.name, set())
            if required_triggers and not required_triggers.issubset(_trigger_names(workflow.get("on"))):
                errors.append(
                    f"{path.name} is missing required triggers: "
                    f"{', '.join(sorted(required_triggers - _trigger_names(workflow.get('on'))))}"
                )
            parents = EXPECTED_WORKFLOW_RUN_PARENTS.get(path.name)
            if parents:
                workflow_run = (workflow.get("on") or {}).get("workflow_run") or {}
                actual_parents = workflow_run.get("workflows") or []
                if list(actual_parents) != parents:
                    errors.append(f"{path.name} must depend on workflow_run parents {parents!r}")

    _assert_reference_only(SERVICE_ROOT / "examples" / "workflows" / "workflow_reference_only" / "__init__.py", errors)
    _assert_reference_only(SERVICE_ROOT / "examples" / "workflows" / "workflow_reference_only" / "gemini_local_pipeline.py", errors)
    _assert_reference_only(SERVICE_ROOT / "examples" / "workflows" / "workflow_reference_only" / "openai_embedding_smoke.py", errors)
    _assert_reference_only(SERVICE_ROOT / "examples" / "workflows" / "workflow_reference_only" / "s3_vectors_check.py", errors)
    _assert_destroy_alignment(errors)
    _assert_labeler_alignment(errors)
    _assert_codeql_alignment(errors)
    _assert_docs_alignment(errors)

    pytest_ini_path = SERVICE_ROOT / "pytest.ini"
    if not pytest_ini_path.is_file():
        errors.append("Missing pytest.ini file")
        pytest_ini = ""
    else:
        pytest_ini = pytest_ini_path.read_text(encoding="utf-8")
    for marker in ("logic", "contract", "integration", "requires_network", "requires_aws"):
        if marker not in pytest_ini:
            errors.append(f"pytest.ini is missing marker {marker!r}")

    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 1 if errors else 0


def _assert_reference_only(path: Path, errors: list[str]) -> None:
    """EN: Assert that a reference-only file contains the required marker string.
    CN: 断言 reference-only 文件包含必要的标记字符串。"""
    text = path.read_text(encoding="utf-8")
    if "REFERENCE_ONLY = True" not in text and "Reference-only" not in text:
        errors.append(f"{path.relative_to(REPO_ROOT)} must be marked as reference-only")


def _trigger_names(on_field: object) -> set[str]:
    """EN: Extract trigger type names from a workflow's 'on' field.
    CN: 从工作流的 'on' 字段提取触发器类型名称集合。"""
    if isinstance(on_field, str):
        return {on_field}
    if isinstance(on_field, list):
        return {str(item) for item in on_field}
    if isinstance(on_field, dict):
        return {str(key) for key in on_field}
    return set()


def _contains_self_hosted(runs_on: object) -> bool:
    """EN: Check whether a runs-on value references a self-hosted runner.
    CN: 检查 runs-on 值是否引用了 self-hosted runner。"""
    if isinstance(runs_on, str):
        return runs_on == "self-hosted"
    if isinstance(runs_on, list):
        return any(str(item) == "self-hosted" for item in runs_on)
    return False


def _expected_trigger(filename: str) -> str | None:
    """EN: Return the primary expected trigger type for a given workflow filename.
    CN: 返回指定工作流文件的主要期望触发器类型。"""
    mapping = {
        "workflow-sanity.yml": "pull_request",
        "guardrails.yml": "workflow_run",
        "ci-failure-comment-relay.yml": "workflow_run",
        "pr-path-conflict-guard.yml": "pull_request_target",
        "issue-hierarchy-guard.yml": "issues",
        "issue-similarity-triage.yml": "issues",
        "issue-similarity-closure.yml": "schedule",
        "logic-ci.yml": "workflow_run",
        "contract-ci.yml": "workflow_run",
        "local-integration-ci.yml": "pull_request",
        "stale-issues.yml": "schedule",
        "merged-branch-cleanup.yml": "schedule",
        "dependabot-auto-merge.yml": "pull_request",
        "external-validation.yml": "workflow_dispatch",
        "docs-ci.yml": "pull_request",
        "security-ci.yml": "workflow_run",
        "package-release.yml": "workflow_run",
        "prod-deploy.yml": "workflow_dispatch",
        "destroy.yml": "workflow_dispatch",
    }
    return mapping.get(filename)


def _assert_shared_runtime_usage(text: str, filename: str, errors: list[str]) -> None:
    """EN: Require workflows to consume the shared runtime bootstrap instead of direct setup actions.
    CN: 要求 workflow 通过共享 runtime bootstrap，而不是直接调用 setup action。"""
    if "actions/setup-node@" in text or "actions/setup-python@" in text:
        errors.append(f"{filename} must use .github/actions/setup-runtime instead of direct setup actions")
    if "node-version:" in text or "python-version:" in text:
        errors.append(f"{filename} must source runtime versions from .github/config/ci-runtime.json")


def _assert_python_project_usage(text: str, filename: str, errors: list[str]) -> None:
    """EN: Require uv-based Python workflows to point at the mcp project explicitly.
    CN: 要求基于 uv 的 Python workflow 显式指向 mcp project。"""
    for lineno, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if "uv sync --locked" in stripped and "--project ocr-service" not in stripped:
            errors.append(f"{filename}:{lineno} must run uv sync with --project ocr-service")
        if "uv run" in stripped:
            if "--no-project" in stripped or "--no-sync" in stripped:
                continue
            if "--project ocr-service" not in stripped:
                errors.append(f"{filename}:{lineno} must run uv with --project ocr-service")


def _assert_shared_runtime_config(errors: list[str]) -> None:
    """EN: Validate the shared runtime config file so the bootstrap action has a single source of truth.
    CN: 校验共享 runtime 配置文件，确保 bootstrap action 拥有唯一事实来源。"""
    try:
        config = json.loads(SHARED_RUNTIME_CONFIG.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return

    node = config.get("node") if isinstance(config, dict) else None
    python = config.get("python") if isinstance(config, dict) else None
    if not isinstance(node, dict):
        errors.append("Shared CI runtime config must define a node object")
        node = {}
    if not isinstance(python, dict):
        errors.append("Shared CI runtime config must define a python object")
        python = {}

    node_version = str(node.get("version", "")).strip()
    python_version = str(python.get("version", "")).strip()
    node_cache = str(node.get("cache", "")).strip()

    if not node_version:
        errors.append("Shared CI runtime config must define node.version")
    if not python_version:
        errors.append("Shared CI runtime config must define python.version")
    if not node_cache:
        errors.append("Shared CI runtime config must define node.cache")


def _assert_docs_alignment(errors: list[str]) -> None:
    """EN: Ensure CI strategy docs and AGENTS.md contain references to all expected workflows.
    CN: 确保 CI 策略文档和 AGENTS.md 引用了全部预期的工作流。"""
    docs_text = (SERVICE_ROOT / "docs" / "open-source-ci-strategy.md").read_text(encoding="utf-8")
    agents_text = (REPO_ROOT / "AGENTS.md").read_text(encoding="utf-8")

    for workflow_name in EXPECTED_NAMES.values():
        if workflow_name not in docs_text:
            errors.append(f"docs/open-source-ci-strategy.md is missing workflow name {workflow_name!r}")

    if "docs/open-source-ci-strategy.md" not in agents_text:
        errors.append("AGENTS.md must reference docs/open-source-ci-strategy.md")
    if "examples/workflows/workflow_reference_only/*" not in agents_text or "reference-only" not in agents_text:
        errors.append("AGENTS.md must mark examples/workflows/workflow_reference_only/* as reference-only")
    if "冲突优先规则" not in agents_text and "与现有设计或既有规则冲突" not in agents_text:
        errors.append("AGENTS.md must preserve the user-preference conflict rule")


def _assert_labeler_alignment(errors: list[str]) -> None:
    """EN: Ensure the labeler still points at the renamed workflow reference directory.
    CN: 确保 labeler 仍然指向已重命名的 workflow reference 目录。"""
    labeler_text = (REPO_ROOT / ".github" / "labeler.yml").read_text(encoding="utf-8")
    if "ocr-service/ocr-pipeline/**" not in labeler_text:
        errors.append(".github/labeler.yml must target ocr-service/ocr-pipeline/** for the service source tree")
    if "examples/workflows/workflow_reference_only/**" not in labeler_text:
        errors.append(".github/labeler.yml must target examples/workflows/workflow_reference_only/**")
    if "workflow_reference/**" in labeler_text:
        errors.append(".github/labeler.yml must not reference the old workflow_reference/** path")


def _assert_codeql_alignment(errors: list[str]) -> None:
    """EN: Ensure the JavaScript/TypeScript CodeQL config matches the real JS/TS surfaces.
    CN: 确保 JavaScript/TypeScript CodeQL 配置与真实的 JS/TS 作用面一致。"""
    codeql_text = (REPO_ROOT / ".github" / "codeql" / "codeql-javascript-typescript.yml").read_text(encoding="utf-8")
    if "examples/workflows/workflow_reference_only" not in codeql_text:
        errors.append(".github/codeql/codeql-javascript-typescript.yml must ignore examples/workflows/workflow_reference_only")
    if "workflow_reference" in codeql_text and "examples/workflows/workflow_reference_only" not in codeql_text:
        errors.append(".github/codeql/codeql-javascript-typescript.yml must not reference workflow_reference")


def _assert_destroy_alignment(errors: list[str]) -> None:
    """EN: Ensure the destroy workflow reads the infra/ pipeline config instead of the old root path.
    CN: 确保 destroy workflow 读取 infra/ 下的 pipeline 配置，而不是旧的根目录路径。"""
    destroy_text = (REPO_ROOT / ".github" / "workflows" / "destroy.yml").read_text(encoding="utf-8")
    if "ocr-service/infra/pipeline-config.json" not in destroy_text:
        errors.append(".github/workflows/destroy.yml must read ocr-service/infra/pipeline-config.json")
    if "scripts/deploy/pipeline-config.json" in destroy_text:
        errors.append(".github/workflows/destroy.yml must not reference scripts/deploy/pipeline-config.json")


def _assert_bilingual_comments(text: str, filename: str, errors: list[str]) -> None:
    """EN: Assert that a workflow file contains both EN and CN comment markers.
    CN: 断言工作流文件同时包含 EN 和 CN 注释标记。"""
    if "# EN:" not in text:
        errors.append(f"{filename} must include English workflow comments")
    if "# CN:" not in text:
        errors.append(f"{filename} must include Chinese workflow comments")


def _find_pinned_uses(text: str) -> set[str]:
    """EN: Find all action references pinned to a full 40-character commit SHA.
    CN: 查找所有固定到 40 位完整 commit SHA 的 action 引用。"""
    pinned: set[str] = set()
    for line in text.splitlines():
        for match in re.finditer(r"([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+@[0-9a-f]{40})", line):
            pinned.add(match.group(1))
    return pinned


if __name__ == "__main__":
    raise SystemExit(main())
