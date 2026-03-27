"""
EN: Tests for the CDK-based deployment workflows.
CN: 针对基于 CDK 的部署工作流的测试。
"""

from __future__ import annotations

from pathlib import Path


def test_prod_deploy_workflow_uses_cdk_deploy_from_release_assets() -> None:
    workflow_path = Path(__file__).resolve().parents[4] / ".github" / "workflows" / "prod-deploy.yml"
    workflow_text = workflow_path.read_text(encoding="utf-8")

    assert "name: Prod Deploy" in workflow_text
    assert "gh release download \"${{ inputs.release_tag }}\"" in workflow_text
    assert "MCP_CDK_ASSET_DIR: ./release-assets" in workflow_text
    assert "setup-node: true" in workflow_text
    assert "npm ci --prefix infra/cdk" in workflow_text
    assert "aws sts get-caller-identity" in workflow_text
    assert "Validate release asset manifest" in workflow_text
    assert "npm --prefix infra/cdk run deploy -- --all --progress events" in workflow_text
    assert "Record production deployment summary" in workflow_text
    assert workflow_text.index("Validate release tag confirmation") < workflow_text.index("Deploy production backend")


def test_destroy_workflow_uses_cdk_destroy_with_placeholder_assets() -> None:
    workflow_path = Path(__file__).resolve().parents[4] / ".github" / "workflows" / "destroy.yml"
    workflow_text = workflow_path.read_text(encoding="utf-8")

    assert "name: Destroy" in workflow_text
    assert "AWS_REGION: ${{ inputs.region || secrets.AWS_REGION }}" in workflow_text
    assert 'MCP_ALLOW_PLACEHOLDER_ASSETS: "true"' in workflow_text
    assert "embedding_profiles_json" not in workflow_text
    assert "setup-node: true" in workflow_text
    assert "npm ci --prefix infra/cdk" in workflow_text
    assert "aws sts get-caller-identity" in workflow_text
    assert "Validate destroy confirmation" in workflow_text
    assert "npm --prefix infra/cdk run destroy -- --all --progress events" in workflow_text
    assert "confirm_destroy does not match name_prefix" in workflow_text
