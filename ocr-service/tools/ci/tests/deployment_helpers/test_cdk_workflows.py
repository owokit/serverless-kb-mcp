"""
EN: Tests for the CDK-based deployment workflows.
CN: 针对基于 CDK 的部署工作流的测试。
"""

from __future__ import annotations

from pathlib import Path


def test_prod_deploy_workflow_uses_cdk_deploy_from_release_assets() -> None:
    workflow_path = Path(__file__).resolve().parents[5] / ".github" / "workflows" / "prod-deploy.yml"
    workflow_text = workflow_path.read_text(encoding="utf-8")
    script_path = Path(__file__).resolve().parents[5] / "scripts" / "prod-deploy.sh"
    script_text = script_path.read_text(encoding="utf-8")

    assert "name: Prod Deploy" in workflow_text
    assert 'ENTRYPOINT="scripts/prod-deploy.sh"' in workflow_text
    assert "serverless-kb-mcp/scripts/prod-deploy.sh" in workflow_text
    assert 'bash "$ENTRYPOINT" --release-tag' in workflow_text
    assert "aws-actions/configure-aws-credentials@v6" in workflow_text
    assert "MCP_CDK_ASSET_DIR:" not in workflow_text
    assert "Validate release asset manifest" not in workflow_text
    assert "Deploy production backend" not in workflow_text
    assert "Upload prod deploy report" not in workflow_text
    assert "Set up runtime" not in workflow_text
    assert "working-directory: serverless-kb-mcp" not in workflow_text
    assert "release_tag confirmation does not match" not in workflow_text
    assert "gh release download" not in workflow_text
    assert "npm ci --prefix infra/cdk" not in workflow_text

    assert "resolve_repo_root" in script_text
    assert "MCP_CDK_ASSET_DIR" in script_text
    assert "MCP_PIPELINE_CONFIG_PATH" in script_text
    assert 'export RELEASE_TAG="$release_tag"' in script_text
    assert "gh release download" in script_text
    assert "uv sync --locked --project ocr-service" in script_text
    assert "npm ci --prefix infra/cdk" in script_text
    assert "npm --prefix infra/cdk run deploy" in script_text
    assert "prod-deploy-report.json" in script_text
    assert "Restore the missing stack resources before re-running prod deploy." in script_text


def test_destroy_workflow_uses_cdk_destroy_with_placeholder_assets() -> None:
    workflow_path = Path(__file__).resolve().parents[5] / ".github" / "workflows" / "destroy.yml"
    workflow_text = workflow_path.read_text(encoding="utf-8")

    assert "name: Destroy" in workflow_text
    assert "AWS_REGION: ${{ inputs.region || secrets.AWS_REGION }}" in workflow_text
    assert 'MCP_ALLOW_PLACEHOLDER_ASSETS: "true"' in workflow_text
    assert "embedding_profiles_json" not in workflow_text
    assert "setup-node: true" in workflow_text
    assert "npm ci --prefix infra/cdk" in workflow_text
    assert "aws sts get-caller-identity" in workflow_text
    assert "Validate destroy confirmation" in workflow_text
    assert "npm --prefix infra/cdk run destroy" in workflow_text
    assert "confirm_destroy does not match name_prefix" in workflow_text


def test_cdk_app_instantiates_three_top_level_stacks_and_regional_api() -> None:
    app_path = Path(__file__).resolve().parents[5] / "infra" / "cdk" / "bin" / "app.ts"
    api_path = Path(__file__).resolve().parents[5] / "infra" / "cdk" / "lib" / "pipeline" / "api.ts"
    app_text = app_path.read_text(encoding="utf-8")
    api_text = api_path.read_text(encoding="utf-8")

    assert "new FoundationStack(" in app_text
    assert "new ComputeStack(" in app_text
    assert "new ApiStack(" in app_text
    assert "EndpointType.REGIONAL" in api_text
    assert "EndpointType.EDGE" not in api_text
