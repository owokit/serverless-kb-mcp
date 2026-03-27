"""
EN: Tests for the CI workflow validation script covering trigger rules, names, and matrix structure.
CN: 用于验证 CI workflow 规则的测试，覆盖触发条件、名称和矩阵结构。
"""
from __future__ import annotations

import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[4] / "tools" / "ci" / "validate_workflows.py"
# EN: Path to the workflow-sanity.yml file used by inline assertions.
# CN: 这里是内联断言所用的 workflow-sanity.yml 路径。
SANITY_WORKFLOW_PATH = Path(__file__).resolve().parents[4] / ".github" / "workflows" / "workflow-sanity.yml"


def _load_module():
    """EN: Dynamically load validate_workflows.py without importing it as a package.
    CN: 动态加载 validate_workflows.py，而不把它当作 package 导入。"""
    spec = importlib.util.spec_from_file_location("validate_workflows", MODULE_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_local_integration_ci_is_a_pr_and_workflow_run_gate() -> None:
    validate_workflows = _load_module()
    assert validate_workflows._expected_trigger("local-integration-ci.yml") == "pull_request"
    assert validate_workflows.EXPECTED_TRIGGER_REQUIREMENTS["local-integration-ci.yml"] == {
        "pull_request",
        "workflow_run",
        "workflow_dispatch",
    }
    assert validate_workflows.EXPECTED_WORKFLOW_RUN_PARENTS["local-integration-ci.yml"] == ["Logic CI"]


def test_pr_path_conflict_guard_is_an_optional_path_drift_gate() -> None:
    validate_workflows = _load_module()
    assert validate_workflows.EXPECTED_NAMES["pr-path-conflict-guard.yml"] == "PR Path Conflict Guard"
    assert validate_workflows._expected_trigger("pr-path-conflict-guard.yml") == "pull_request_target"
    assert validate_workflows.EXPECTED_TRIGGER_REQUIREMENTS["pr-path-conflict-guard.yml"] == {
        "pull_request_target",
        "workflow_dispatch",
    }


def test_issue_hierarchy_guard_is_an_issue_lifecycle_gate() -> None:
    validate_workflows = _load_module()
    assert validate_workflows.EXPECTED_NAMES["issue-hierarchy-guard.yml"] == "Issue Hierarchy Guard"
    assert validate_workflows._expected_trigger("issue-hierarchy-guard.yml") == "issues"
    assert validate_workflows.EXPECTED_TRIGGER_REQUIREMENTS["issue-hierarchy-guard.yml"] == {
        "issues",
        "workflow_dispatch",
    }


def test_workflow_sanity_is_the_earliest_hygiene_gate() -> None:
    validate_workflows = _load_module()
    assert validate_workflows.EXPECTED_NAMES["workflow-sanity.yml"] == "Workflow Sanity"
    assert validate_workflows._expected_trigger("workflow-sanity.yml") == "pull_request"
    assert validate_workflows.EXPECTED_TRIGGER_REQUIREMENTS["workflow-sanity.yml"] == {
        "pull_request",
        "push",
        "workflow_dispatch",
    }


def test_issue_hierarchy_guard_is_an_issue_hygiene_job() -> None:
    validate_workflows = _load_module()
    assert validate_workflows.EXPECTED_NAMES["issue-hierarchy-guard.yml"] == "Issue Hierarchy Guard"
    assert validate_workflows._expected_trigger("issue-hierarchy-guard.yml") == "issues"
    assert validate_workflows.EXPECTED_TRIGGER_REQUIREMENTS["issue-hierarchy-guard.yml"] == {
        "issues",
        "workflow_dispatch",
    }


def test_release_workflows_still_depend_on_local_integration_ci() -> None:
    validate_workflows = _load_module()
    assert validate_workflows.EXPECTED_WORKFLOW_RUN_PARENTS["package-release.yml"] == ["Local Integration CI"]
    assert validate_workflows.EXPECTED_WORKFLOW_RUN_PARENTS["local-integration-ci.yml"] == ["Logic CI"]


def test_package_release_only_publishes_for_main_branch_workflow_runs() -> None:
    workflow_path = Path(__file__).resolve().parents[4] / ".github" / "workflows" / "package-release.yml"
    text = workflow_path.read_text(encoding="utf-8")

    assert "name: Package Release" in text
    assert "github.event.workflow_run.head_branch == 'main'" in text


def test_failure_comment_relay_is_workflow_run_based() -> None:
    validate_workflows = _load_module()
    assert validate_workflows.EXPECTED_NAMES["ci-failure-comment-relay.yml"] == "CI Failure Comment Relay"
    assert validate_workflows._expected_trigger("ci-failure-comment-relay.yml") == "workflow_run"
    assert validate_workflows.EXPECTED_TRIGGER_REQUIREMENTS["ci-failure-comment-relay.yml"] == {
        "workflow_run",
    }
    assert validate_workflows.EXPECTED_WORKFLOW_RUN_PARENTS["ci-failure-comment-relay.yml"] == [
        "Workflow Sanity",
        "Guardrails",
        "Logic CI",
        "Contract CI",
        "Local Integration CI",
        "Security CI",
    ]


def test_failure_comment_relay_shell_step_passes_workflow_name() -> None:
    workflow_path = Path(__file__).resolve().parents[4] / ".github" / "workflows" / "ci-failure-comment-relay.yml"
    text = workflow_path.read_text(encoding="utf-8")

    assert "uv run --project services python tools/ci/comment_pr_failure.py" in text
    assert 'GITHUB_WORKFLOW: ${{ github.event.workflow_run.name }}' in text
    assert '--workflow-name "$GITHUB_WORKFLOW"' in text


def test_failure_comment_workflows_request_pr_lookup_permissions() -> None:
    docs_workflow = Path(__file__).resolve().parents[4] / ".github" / "workflows" / "docs-ci.yml"
    relay_workflow = Path(__file__).resolve().parents[4] / ".github" / "workflows" / "ci-failure-comment-relay.yml"

    docs_text = docs_workflow.read_text(encoding="utf-8")
    relay_text = relay_workflow.read_text(encoding="utf-8")

    assert "pull-requests: read" in docs_text
    assert "pull-requests: read" in relay_text


def test_open_source_ci_strategy_documents_relay_section() -> None:
    docs_path = Path(__file__).resolve().parents[4] / "docs" / "open-source-ci-strategy.md"
    text = docs_path.read_text(encoding="utf-8")

    assert "### `ci-failure-comment-relay.yml`" in text
    assert "CI Failure Comment Relay" in text
    assert "workflow_run" in text
    assert "tools/ci/comment_pr_failure.py" in text
    assert "Docs CI" not in text.split("### `ci-failure-comment-relay.yml`", 1)[1].split("### `dependabot-auto-merge.yml`", 1)[0]


def test_open_source_ci_strategy_documents_branch_lifecycle_cleanup() -> None:
    docs_path = Path(__file__).resolve().parents[4] / "docs" / "open-source-ci-strategy.md"
    text = docs_path.read_text(encoding="utf-8")

    assert "### `merged-branch-cleanup.yml`" in text
    assert "Branch Lifecycle Cleanup" in text
    assert "branch:protected" in text
    assert "branch:deletable" in text


def test_labeler_tracks_the_renamed_workflow_reference_directory() -> None:
    labeler_path = Path(__file__).resolve().parents[4] / ".github" / "labeler.yml"
    text = labeler_path.read_text(encoding="utf-8")

    assert "apps/console-web/api/**" not in text
    assert "services/ocr-pipeline/**" in text
    assert "examples/workflows/workflow_reference_only/**" in text
    assert "workflow_reference/**" not in text


def test_codeql_tracks_the_real_js_ts_surfaces() -> None:
    codeql_path = Path(__file__).resolve().parents[4] / ".github" / "codeql" / "codeql-javascript-typescript.yml"
    text = codeql_path.read_text(encoding="utf-8")
    lines = [line.strip() for line in text.splitlines()]

    assert "apps/console-web" not in text
    assert "- examples/workflows/workflow_reference_only" in lines
    assert "- workflow_reference" not in lines


def test_agents_documents_branch_lifecycle_cleanup() -> None:
    agents_path = Path(__file__).resolve().parents[4] / "AGENTS.md"
    text = agents_path.read_text(encoding="utf-8")

    assert "Branch Lifecycle Cleanup" in text


def test_docs_ci_is_a_direct_pull_request_gate() -> None:
    validate_workflows = _load_module()
    assert validate_workflows.EXPECTED_NAMES["docs-ci.yml"] == "Docs CI"
    assert validate_workflows._expected_trigger("docs-ci.yml") == "pull_request"
    assert validate_workflows.EXPECTED_TRIGGER_REQUIREMENTS["docs-ci.yml"] == {
        "pull_request",
        "workflow_dispatch",
    }


def test_stale_issues_is_a_scheduled_hygiene_job() -> None:
    validate_workflows = _load_module()
    assert validate_workflows.EXPECTED_NAMES["stale-issues.yml"] == "Stale Issues"
    assert validate_workflows._expected_trigger("stale-issues.yml") == "schedule"
    assert validate_workflows.EXPECTED_TRIGGER_REQUIREMENTS["stale-issues.yml"] == {
        "schedule",
        "workflow_dispatch",
    }


def test_branch_lifecycle_cleanup_is_a_label_reconciliation_gate() -> None:
    validate_workflows = _load_module()
    assert validate_workflows.EXPECTED_NAMES["merged-branch-cleanup.yml"] == "Branch Lifecycle Cleanup"
    assert validate_workflows._expected_trigger("merged-branch-cleanup.yml") == "schedule"
    assert validate_workflows.EXPECTED_TRIGGER_REQUIREMENTS["merged-branch-cleanup.yml"] == {
        "pull_request_target",
        "schedule",
        "workflow_dispatch",
    }


def test_workflow_sanity_enforces_tabs_and_actionlint() -> None:
    text = SANITY_WORKFLOW_PATH.read_text(encoding="utf-8")

    assert "name: Workflow Sanity" in text
    assert "name: Sanity (${{ matrix.check }})" in text
    assert "check: tabs" in text
    assert "check: actionlint" in text
    assert "check: inventory" in text
    assert "workflow-tabs-report.json" in text
    assert "workflow-actionlint-report.json" in text
    assert "workflow-inventory-report.json" in text
    assert '"workflow": "Workflow Sanity"' in text
    assert "workflow_dispatch" in text


def test_guardrails_is_matrix_driven() -> None:
    workflow_path = Path(__file__).resolve().parents[4] / ".github" / "workflows" / "guardrails.yml"
    text = workflow_path.read_text(encoding="utf-8")

    assert "name: Guardrails" in text
    assert "name: Guardrails (${{ matrix.check }})" in text
    assert "check: secret_shapes" in text
    assert "check: chinese_mojibake" in text
    assert "guardrails-secrets-report.json" in text
    assert "guardrails-chinese-report.json" in text
    assert "tools/ci/chinese_text_hygiene.py" in text
    assert '"workflow": "Guardrails"' in text
    assert "chinese_text_hygiene" in text


def test_pr_path_conflict_guard_scans_parallel_prs() -> None:
    workflow_path = Path(__file__).resolve().parents[4] / ".github" / "workflows" / "pr-path-conflict-guard.yml"
    text = workflow_path.read_text(encoding="utf-8")

    assert "name: PR Path Conflict Guard" in text
    assert "pull_request_target" in text
    assert "workflow_dispatch" in text
    assert "actions/github-script@v8" in text
    assert "pr-path-conflict-report.json" in text
    assert "codex-pr-path-conflict-guard" in text


def test_destroy_workflow_uses_root_pipeline_config() -> None:
    workflow_path = Path(__file__).resolve().parents[4] / ".github" / "workflows" / "destroy.yml"
    text = workflow_path.read_text(encoding="utf-8")

    assert "name: Destroy" in text
    assert "workflow_dispatch" in text
    assert "pipeline-config.json" in text
    assert "scripts/deploy/pipeline-config.json" not in text


def test_branch_lifecycle_cleanup_reconciles_labels_and_deletes_eligible_branches() -> None:
    workflow_path = Path(__file__).resolve().parents[4] / ".github" / "workflows" / "merged-branch-cleanup.yml"
    text = workflow_path.read_text(encoding="utf-8")

    assert "name: Branch Lifecycle Cleanup" in text
    assert "0 */3 * * *" in text
    assert "pull_request" in text
    assert "opened, reopened, synchronize, closed, labeled, unlabeled" in text
    assert "issues: write" in text
    assert "pull-requests: write" in text
    assert "branch:protected" in text
    assert "branch:deletable" in text
    assert "GitHub labels are attached to the PR record" in text


def test_logic_ci_is_matrix_driven() -> None:
    workflow_path = Path(__file__).resolve().parents[4] / ".github" / "workflows" / "logic-ci.yml"
    text = workflow_path.read_text(encoding="utf-8")

    assert "name: Logic CI" in text
    assert "name: Logic CI (${{ matrix.suite }})" in text
    assert "suite: python_quality" in text
    assert "suite: python_logic_tests" in text
    assert "ruff check services/ocr-pipeline/src tools/ci services/ocr-pipeline/tests" in text
    assert "python -m pytest -q" in text
    assert "logic-ci-summary" in text
    assert "logic-ci-report" in text
    assert "guardrails-*-report" in text
    assert '"workflow": "Logic CI"' in text


def test_contract_ci_is_matrix_driven() -> None:
    workflow_path = Path(__file__).resolve().parents[4] / ".github" / "workflows" / "contract-ci.yml"
    text = workflow_path.read_text(encoding="utf-8")

    assert "name: Contract CI" in text
    assert "name: Contract CI (${{ matrix.suite }})" in text
    assert "suite: policy" in text
    assert "suite: storage" in text
    assert "suite: providers" in text
    assert "suite: orchestration" in text
    assert "suite: packaging" in text
    assert "services/ocr-pipeline/tests/unit/serverless_mcp/test_dynamo_batch.py" in text
    assert "contract-ci-${{ matrix.suite }}-report" in text


def test_python_workflows_target_the_mcp_project_explicitly() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    required_snippets = {
        "docs-ci.yml": [
            "uv sync --locked --project services",
            "uv run --project services python tools/ci/validate_workflows.py",
            "uv run --project services python <<'PY'",
            "uv run --project services python tools/ci/comment_pr_failure.py",
        ],
        "workflow-sanity.yml": [
            "uv sync --locked --project services",
            "uv run --project services python -c \"$PYTHON_SCRIPT\"",
        ],
        "logic-ci.yml": [
            "uv sync --locked --project services",
            "uv run --project services ruff check services/ocr-pipeline/src tools/ci services/ocr-pipeline/tests",
            "uv run --project services python -m compileall services/ocr-pipeline/src tools/ci services/ocr-pipeline/tests",
            "uv run --project services python -m pytest -q",
        ],
        "contract-ci.yml": [
            "uv sync --locked --project services",
            "uv run --project services python -m pytest -q",
            "uv run --project services python <<'PY'",
        ],
        "local-integration-ci.yml": [
            "uv sync --locked --project services",
            "uv run --project services python <<'PY'",
            "uv run --project services python -m pytest -q services/ocr-pipeline/tests/integration/test_hosted_runner_storage_roundtrip.py --maxfail=1",
            "uv run --project services python -m pytest -q services/ocr-pipeline/tests/integration/test_hosted_runner_aws_roundtrip.py --maxfail=1",
        ],
        "security-ci.yml": [
            "uv sync --locked --project services",
            "uv run --project services python -m pip check",
            "uv run --project services --locked pip-audit",
            "uv run --project services python <<'PY'",
        ],
        "external-validation.yml": [
            "uv sync --locked --project services",
            "uv run --project services python -m pytest -q examples/workflows/tests/reference/test_gemini_local_pipeline.py --maxfail=1",
            "uv run --project services python -m pytest -q examples/workflows/tests/reference/test_s3_vectors_check.py --maxfail=1",
        ],
        "package-release.yml": [
            "uv sync --locked --project services",
            "uv run --project services python ./tools/packaging/serverless_mcp/list_lambda_artifacts.py",
            "uv run --project services python ./tools/packaging/serverless_mcp/build_layer_artifacts.py",
            "uv run --project services python <<'PY'",
        ],
    }

    for filename, snippets in required_snippets.items():
        text = (repo_root / ".github" / "workflows" / filename).read_text(encoding="utf-8")
        for snippet in snippets:
            assert snippet in text


def test_local_integration_ci_is_matrix_driven() -> None:
    workflow_path = Path(__file__).resolve().parents[4] / ".github" / "workflows" / "local-integration-ci.yml"
    text = workflow_path.read_text(encoding="utf-8")

    assert "name: Local Integration CI" in text
    assert "name: Local Integration CI (${{ matrix.scenario }})" in text
    assert "scenario: storage_roundtrip" in text
    assert "scenario: aws_roundtrip" in text
    assert "scenario: sam_job_status" in text
    assert "scenario: remote_mcp_frontdoor" in text
    assert "scenario: stepfunctions_local" in text
    assert "scenario: vector_backend" in text
    assert 'str(repo_root / "services" / "ocr-pipeline")' in text
    assert "local-integration-summary" in text
    assert "pattern: local-integration-ci-*-report" in text
    assert "local-integration-ci-report" in text
