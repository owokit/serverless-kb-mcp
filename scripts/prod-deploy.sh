#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: prod-deploy.sh [--release-tag TAG] [--confirm-release-tag TAG]

Deploy the production backend from a single, repository-root-aware entrypoint.
EOF
}

die() {
  printf '::error::%s\n' "$*" >&2
  exit 1
}

json_read() {
  local path="$1"
  local key="$2"
  python3 - "$path" "$key" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
key = sys.argv[2]
data = json.loads(path.read_text(encoding="utf-8"))
value = data[key]
if not isinstance(value, str):
    value = str(value)
print(value)
PY
}

resolve_repo_root() {
  local script_dir repo_root
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  repo_root="$(cd "$script_dir/.." && git rev-parse --show-toplevel 2>/dev/null || true)"
  if [[ -n "$repo_root" ]]; then
    printf '%s\n' "$repo_root"
    return 0
  fi
  printf '%s\n' "$(cd "$script_dir/.." && pwd)"
}

ensure_uv() {
  if command -v uv >/dev/null 2>&1; then
    return 0
  fi

  python3 -m pip install --user --upgrade uv
  export PATH="$HOME/.local/bin:$PATH"
}

build_release_assets_from_source() {
  local functions layers report_path source_branch source_sha

  ensure_uv
  mkdir -p "$ASSET_DIR/layers"
  uv sync --locked --project ocr-service

  functions="$(uv run --project ocr-service python ./ocr-service/tools/packaging/serverless_mcp/list_lambda_artifacts.py --format plain | xargs)"
  layers="$(uv run --project ocr-service python ./ocr-service/tools/packaging/serverless_mcp/list_layer_artifacts.py --format plain | xargs)"

  uv run --project ocr-service python ./ocr-service/tools/packaging/serverless_mcp/build_lambda_artifacts.py \
    --repo-name "$REPO_NAME" \
    --output-dir "$ASSET_DIR" \
    --functions "$functions"
  uv run --project ocr-service python ./ocr-service/tools/packaging/serverless_mcp/build_layer_artifacts.py \
    --repo-name "$REPO_NAME" \
    --output-dir "$ASSET_DIR/layers" \
    --layers "$layers"

  report_path="$ASSET_DIR/package-release-report.json"
  source_branch="${GITHUB_REF_NAME:-main}"
  source_sha="${GITHUB_SHA:-unknown}"
  python3 - "$report_path" "$RELEASE_TAG" "$source_branch" "$source_sha" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
report = {
    "workflow": "Package Release",
    "release_tag": sys.argv[2],
    "source_branch": sys.argv[3],
    "source_sha": sys.argv[4],
    "status": "prepared",
}
path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps(report, ensure_ascii=False, indent=2))
PY
}

download_release_assets() {
  mkdir -p "$ASSET_DIR/layers"
  gh release download "$RELEASE_TAG" --dir "$ASSET_DIR" --pattern '*.zip' --pattern 'package-release-report.json'
  shopt -s nullglob
  local layer_zips=("$ASSET_DIR"/*_layer.zip)
  if (( ${#layer_zips[@]} > 0 )); then
    mv "${layer_zips[@]}" "$ASSET_DIR/layers/"
  fi
  shopt -u nullglob
}

validate_release_manifest() {
  local report_path="$ASSET_DIR/package-release-report.json"
  [[ -f "$report_path" ]] || die "Missing release manifest: $report_path"

  python3 - "$report_path" "$RELEASE_TAG" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
expected_release_tag = sys.argv[2]
report = json.loads(path.read_text(encoding="utf-8"))
actual_release_tag = report.get("release_tag")
if actual_release_tag != expected_release_tag:
    raise SystemExit(
        f"Release manifest does not match requested tag: expected {expected_release_tag!r}, got {actual_release_tag!r}"
    )
print(json.dumps(report, ensure_ascii=False, indent=2))
PY
}

collect_rollback_skip_resources() {
  local stack_name="$1"

  aws cloudformation describe-stack-events --stack-name "$stack_name" --output json | python3 -c '
from __future__ import annotations

import json
import sys

payload = json.load(sys.stdin)
ids: list[str] = []
seen: set[str] = set()
for event in payload.get("StackEvents", []):
    reason = event.get("ResourceStatusReason") or ""
    if "could not be found" not in reason and "HandlerErrorCode: NotFound" not in reason:
        continue
    logical_id = event.get("LogicalResourceId")
    if logical_id and logical_id not in seen:
        seen.add(logical_id)
        ids.append(logical_id)
print(" ".join(ids))
'
}

recover_failed_stack() {
  local stack_name="$1"
  local stack_status skip_resources

  stack_status="$(aws cloudformation describe-stacks --stack-name "$stack_name" --query 'Stacks[0].StackStatus' --output text 2>/dev/null || true)"
  if [[ "$stack_status" != "UPDATE_ROLLBACK_FAILED" ]]; then
    return 0
  fi

  printf '::warning::Stack %s is %s; attempting rollback recovery.\n' "$stack_name" "$stack_status"
  skip_resources="$(collect_rollback_skip_resources "$stack_name")"
  if [[ -n "$skip_resources" ]]; then
    printf '::warning::Skipping CloudFormation resources for %s: %s\n' "$stack_name" "$skip_resources"
    aws cloudformation continue-update-rollback --stack-name "$stack_name" --resources-to-skip $skip_resources
  else
    printf '::warning::No explicit skip list found for %s; retrying rollback without skips.\n' "$stack_name"
    aws cloudformation continue-update-rollback --stack-name "$stack_name"
  fi

  local attempts current_status
  for attempts in {1..60}; do
    current_status="$(aws cloudformation describe-stacks --stack-name "$stack_name" --query 'Stacks[0].StackStatus' --output text 2>/dev/null || true)"
    case "$current_status" in
      UPDATE_ROLLBACK_COMPLETE|UPDATE_COMPLETE|CREATE_COMPLETE)
        printf '::notice::Stack %s recovered with status %s.\n' "$stack_name" "$current_status"
        return 0
        ;;
      UPDATE_ROLLBACK_FAILED|UPDATE_ROLLBACK_IN_PROGRESS|UPDATE_IN_PROGRESS|CREATE_IN_PROGRESS|ROLLBACK_IN_PROGRESS|ROLLBACK_FAILED)
        sleep 10
        ;;
      "")
        printf '::warning::Stack %s disappeared while recovering; continuing with deploy.\n' "$stack_name"
        return 0
        ;;
      *)
        sleep 10
        ;;
    esac
  done

  die "Timed out waiting for CloudFormation stack recovery: $stack_name"
}

main() {
  local arg release_tag confirm_release_tag config_path deploy_report
  release_tag="main"
  confirm_release_tag="main"

  while (($#)); do
    arg="$1"
    case "$arg" in
      --release-tag)
        [[ $# -ge 2 ]] || die "--release-tag requires a value"
        release_tag="$2"
        shift 2
        ;;
      --confirm-release-tag)
        [[ $# -ge 2 ]] || die "--confirm-release-tag requires a value"
        confirm_release_tag="$2"
        shift 2
        ;;
      -h|--help)
        usage
        return 0
        ;;
      *)
        die "Unknown argument: $arg"
        ;;
    esac
  done

  [[ "$release_tag" == "$confirm_release_tag" ]] || die "release_tag confirmation does not match"
  export RELEASE_TAG="$release_tag"

  ROOT="$(resolve_repo_root)"
  CONFIG_PATH="$ROOT/infra/pipeline-config.json"
  ASSET_DIR="$ROOT/release-assets"
  DEPLOY_REPORT="$ROOT/prod-deploy-report.json"

  [[ -f "$CONFIG_PATH" ]] || die "Missing pipeline config: $CONFIG_PATH"

  cd "$ROOT"

  REPO_NAME="$(json_read "$CONFIG_PATH" repo_name)"
  STACK_PREFIX="$(json_read "$CONFIG_PATH" name_prefix)"
  export REPO_NAME
  export MCP_CDK_ASSET_DIR="$ASSET_DIR"
  export MCP_PIPELINE_CONFIG_PATH="$CONFIG_PATH"
  export GH_TOKEN="${GH_TOKEN:-${GITHUB_TOKEN:-}}"

  recover_failed_stack "$STACK_PREFIX-foundation"
  recover_failed_stack "$STACK_PREFIX-compute"
  recover_failed_stack "$STACK_PREFIX-api"

  if gh release view "$release_tag" >/dev/null 2>&1; then
    download_release_assets
  elif [[ "$release_tag" == "main" ]]; then
    printf '::notice::Release %s does not exist yet. Building release assets from the checked-out source.\n' "$release_tag"
    build_release_assets_from_source
  else
    die "Release '$release_tag' does not exist. Publish the release first or pass an existing release tag."
  fi

  validate_release_manifest

  npm ci --prefix infra/cdk

  if [[ -n "${AWS_ROLE_TO_ASSUME:-}" || -n "${AWS_ACCESS_KEY_ID:-}" || -n "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
    : # AWS credentials are expected to be configured by the workflow.
  else
    die "AWS credentials are not configured"
  fi

  CDK_DEFAULT_ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
  CDK_DEFAULT_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
  export CDK_DEFAULT_ACCOUNT
  export CDK_DEFAULT_REGION

  npm --prefix infra/cdk run deploy

  python3 - "$DEPLOY_REPORT" "$release_tag" "${GITHUB_REF_NAME:-main}" "${GITHUB_SHA:-unknown}" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
report = {
    "workflow": "Prod Deploy",
    "release_tag": sys.argv[2],
    "source_branch": sys.argv[3],
    "source_sha": sys.argv[4],
    "status": "deployed",
}
path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps(report, ensure_ascii=False, indent=2))
PY
}

main "$@"
