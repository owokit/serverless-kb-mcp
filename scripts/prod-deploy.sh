#!/usr/bin/env bash
set -euo pipefail
trap 'die "Command failed at line ${LINENO}: ${BASH_COMMAND}"' ERR

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

log() {
  printf '[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"
}

join_unique_words() {
  python3 - "$@" <<'PY'
from __future__ import annotations

import sys

seen: set[str] = set()
ordered: list[str] = []
for value in sys.argv[1:]:
    for item in value.split():
        if item and item not in seen:
            seen.add(item)
            ordered.append(item)
print(" ".join(ordered))
PY
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

  log "Building release assets from the checked-out source"
  ensure_uv
  mkdir -p "$ASSET_DIR/layers"
  log "Synchronizing uv environment for ocr-service"
  uv sync --locked --project ocr-service

  log "Enumerating Lambda package artifacts"
  functions="$(uv run --project ocr-service python ./ocr-service/tools/packaging/serverless_mcp/list_lambda_artifacts.py --format plain | xargs)"
  log "Enumerating Lambda layer artifacts"
  layers="$(uv run --project ocr-service python ./ocr-service/tools/packaging/serverless_mcp/list_layer_artifacts.py --format plain | xargs)"

  log "Building Lambda package artifacts"
  uv run --project ocr-service python ./ocr-service/tools/packaging/serverless_mcp/build_lambda_artifacts.py \
    --repo-name "$REPO_NAME" \
    --output-dir "$ASSET_DIR" \
    --functions "$functions"
  log "Building Lambda layer artifacts"
  uv run --project ocr-service python ./ocr-service/tools/packaging/serverless_mcp/build_layer_artifacts.py \
    --repo-name "$REPO_NAME" \
    --output-dir "$ASSET_DIR/layers" \
    --layers "$layers"

  report_path="$ASSET_DIR/package-release-report.json"
  source_branch="${GITHUB_REF_NAME:-main}"
  source_sha="${GITHUB_SHA:-unknown}"
  log "Writing prepared package release manifest to $report_path"
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
  log "Downloading release assets for $RELEASE_TAG"
  gh release download "$RELEASE_TAG" --dir "$ASSET_DIR" --pattern '*.zip' --pattern 'package-release-report.json'
  shopt -s nullglob
  local layer_zips=("$ASSET_DIR"/*_layer.zip)
  if (( ${#layer_zips[@]} > 0 )); then
    log "Moving layer assets into $ASSET_DIR/layers"
    mv "${layer_zips[@]}" "$ASSET_DIR/layers/"
  fi
  shopt -u nullglob
}

validate_release_manifest() {
  local report_path="$ASSET_DIR/package-release-report.json"
  [[ -f "$report_path" ]] || die "Missing release manifest: $report_path"

  log "Validating release manifest at $report_path"
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

  aws cloudformation describe-stack-resources --stack-name "$stack_name" --output json | python3 -c '
from __future__ import annotations

import json
import time
import sys

stack_name = sys.argv[1]
payload = json.load(sys.stdin)
ids: list[str] = []
seen: set[str] = set()
for resource in payload.get("StackResources", []):
    if resource.get("ResourceStatus") != "UPDATE_FAILED":
        continue
    reason = resource.get("ResourceStatusReason") or ""
    if (
        "could not be found" not in reason
        and "HandlerErrorCode: NotFound" not in reason
    ):
        continue
    logical_id = resource.get("LogicalResourceId")
    if logical_id and logical_id not in seen and logical_id != stack_name:
        seen.add(logical_id)
        ids.append(logical_id)
print(" ".join(ids))
' "$stack_name"
}

describe_current_stack_failures() {
  local stack_name="$1"

  aws cloudformation describe-stack-resources --stack-name "$stack_name" --output json | python3 -c '
from __future__ import annotations

import json
import time
import sys

payload = json.load(sys.stdin)
resources = payload.get("StackResources", [])
failed = [r for r in resources if r.get("ResourceStatus") in {"UPDATE_FAILED", "DELETE_FAILED"}]
for resource in failed[:15]:
    logical_id = resource.get("LogicalResourceId") or "unknown"
    resource_type = resource.get("ResourceType") or "unknown"
    status = resource.get("ResourceStatus") or "unknown"
    reason = (resource.get("ResourceStatusReason") or "").replace("\n", " ").strip()
    physical_id = resource.get("PhysicalResourceId") or "unknown"
    print(f"{logical_id} | {resource_type} | {status} | {physical_id} | {reason}")
' 
}

describe_recent_stack_events() {
  local stack_name="$1"

  aws cloudformation describe-stack-events --stack-name "$stack_name" --output json | python3 -c '
from __future__ import annotations

import json
import time
import sys

payload = json.load(sys.stdin)
events = payload.get("StackEvents", [])[:10]
for event in events:
    timestamp = event.get("Timestamp") or "unknown"
    logical_id = event.get("LogicalResourceId") or "unknown"
    status = event.get("ResourceStatus") or "unknown"
    reason = (event.get("ResourceStatusReason") or "").replace("\n", " ").strip()
    print(f"{timestamp} | {logical_id} | {status} | {reason}")
' 
}

restore_missing_lambda_functions() {
  local stack_name="$1"

  log "Rehydrating missing Lambda functions for $stack_name if needed"
  ensure_uv
  uv run --project ocr-service python - "$stack_name" "$CONFIG_PATH" "$ASSET_DIR" "$REPO_NAME" <<'PY'
from __future__ import annotations

import json
import time
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

stack_name = sys.argv[1]
config_path = Path(sys.argv[2])
artifact_dir = Path(sys.argv[3])
repo_name = sys.argv[4]

config = json.loads(config_path.read_text(encoding="utf-8"))
names = config["resource_names"]
defaults = config["defaults"]
lambda_settings = config.get("lambda_settings", {})

functions = [
    {"function_key": "ingest", "function_name": names["ingest_lambda"], "role_key": "ingest", "layer_keys": ["core"]},
    {"function_key": "extract_prepare", "function_name": names["extract_prepare_lambda"], "role_key": "extract", "layer_keys": ["core", "extract"]},
    {"function_key": "extract_sync", "function_name": names["extract_sync_lambda"], "role_key": "extract", "layer_keys": ["core", "extract"]},
    {"function_key": "extract_submit", "function_name": names["extract_submit_lambda"], "role_key": "extract", "layer_keys": ["core", "extract"]},
    {"function_key": "extract_poll", "function_name": names["extract_poll_lambda"], "role_key": "extract", "layer_keys": ["core", "extract"]},
    {"function_key": "extract_persist", "function_name": names["extract_persist_lambda"], "role_key": "extract", "layer_keys": ["core", "extract"]},
    {"function_key": "extract_mark_failed", "function_name": names["extract_mark_failed_lambda"], "role_key": "extract", "layer_keys": ["core", "extract"]},
    {"function_key": "embed", "function_name": names["embed_lambda"], "role_key": "embed", "layer_keys": ["core", "embedding"]},
    {"function_key": "remote_mcp", "function_name": names["remote_mcp_lambda"], "role_key": "query", "layer_keys": ["core", "embedding"]},
    {"function_key": "backfill", "function_name": names["backfill_lambda"], "role_key": "backfill", "layer_keys": ["core", "extract", "embedding"]},
    {"function_key": "job_status", "function_name": names["job_status_lambda"], "role_key": "status", "layer_keys": ["core"]},
]

cf = boto3.client("cloudformation")
iam = boto3.client("iam")
lambda_client = boto3.client("lambda")
sts = boto3.client("sts")
account_id = sts.get_caller_identity()["Account"]
region = (boto3.session.Session().region_name or "us-east-1")
stack_resources = cf.describe_stack_resources(StackName=stack_name).get("StackResources", [])
resource_map = {resource["LogicalResourceId"]: resource for resource in stack_resources}
layer_arns = {}
for layer_key in ("core", "extract", "embedding"):
    logical_prefix = f"{layer_key.capitalize()}Layer"
    resource = next(
        (item for item in stack_resources if (item.get("LogicalResourceId") or "").startswith(logical_prefix)),
        None,
    )
    if not resource or not resource.get("PhysicalResourceId"):
        raise SystemExit(f"Missing layer resource {logical_prefix} for {stack_name}")
    layer_arns[layer_key] = resource["PhysicalResourceId"]

def ensure_role_exists(role_name: str, service_principal: str, managed_policies: list[str]) -> str:
    try:
        return iam.get_role(RoleName=role_name)["Role"]["Arn"]
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code not in {"NoSuchEntity", "NoSuchEntityException", "ResourceNotFoundException"} and "not found" not in str(exc).lower():
            raise
    iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": service_principal},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        ),
        Description=f"Rehydrated execution role for {role_name}",
    )
    for policy_name in managed_policies:
        iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn=f"arn:aws:iam::aws:policy/{policy_name}",
        )
    print(f"Created missing IAM role {role_name}")
    time.sleep(10)
    return iam.get_role(RoleName=role_name)["Role"]["Arn"]

def create_lambda_function_with_retry(function_name: str, kwargs: dict[str, object]) -> None:
    for attempt in range(1, 6):
        try:
            lambda_client.create_function(**kwargs)
            print(f"Created missing Lambda function {function_name}")
            return
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            message = str(exc)
            if error_code == "InvalidParameterValueException" and "cannot be assumed by Lambda" in message:
                wait_seconds = 10 * attempt
                print(f"Waiting {wait_seconds}s for IAM trust propagation before retrying {function_name} (attempt {attempt}/5)")
                time.sleep(wait_seconds)
                continue
            raise
    raise SystemExit(f"Failed to create Lambda function {function_name} after IAM trust retries")

created = []
already_present = []
for spec in functions:
    function_name = spec["function_name"]
    try:
        lambda_client.get_function(FunctionName=function_name)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code not in {"ResourceNotFoundException", "ResourceNotFoundExceptionException"} and "not found" not in str(exc).lower():
            raise
        role_name = names["lambda_role"] if spec["role_key"] == "query" else f"{names['lambda_role']}-{spec['role_key']}"
        role_arn = ensure_role_exists(role_name, "lambda.amazonaws.com", ["service-role/AWSLambdaBasicExecutionRole", "AWSXRayDaemonWriteAccess"])
        zip_path = artifact_dir / f"{repo_name}_{spec['function_key']}.zip"
        if not zip_path.exists():
            raise SystemExit(f"Missing Lambda zip asset: {zip_path}")
        settings = lambda_settings.get(spec["function_key"], {})
        layer_list = [layer_arns[layer_key] for layer_key in spec["layer_keys"]]
        kwargs = {
            "FunctionName": function_name,
            "Runtime": defaults["runtime"],
            "Role": role_arn,
            "Handler": "lambda_function.lambda_handler",
            "Code": {"ZipFile": zip_path.read_bytes()},
            "Description": f"{repo_name}:{spec['function_key']}",
            "Timeout": int(settings.get("timeout_seconds", defaults["lambda_timeout_seconds"])),
            "MemorySize": int(settings.get("memory_size", defaults["lambda_memory_size"])),
            "TracingConfig": {"Mode": "Active"},
            "Architectures": [defaults["architecture"]],
            "Publish": True,
        }
        if layer_list:
            kwargs["Layers"] = layer_list
        create_lambda_function_with_retry(function_name, kwargs)
        created.append(function_name)
    else:
        already_present.append(function_name)

print(json.dumps({"already_present": already_present, "created": created}, ensure_ascii=False, indent=2))
PY
}

recover_failed_stack() {
  local stack_name="$1"
  local stack_status skip_resources current_skip_resources merged_skip_resources attempts current_status stagnant_failed_polls=0

  log "Checking CloudFormation stack status for $stack_name"
  stack_status="$(aws cloudformation describe-stacks --stack-name "$stack_name" --query 'Stacks[0].StackStatus' --output text 2>/dev/null || true)"
  if [[ "$stack_status" != "UPDATE_ROLLBACK_FAILED" ]]; then
    return 0
  fi

  printf '::warning::Stack %s is %s; attempting rollback recovery.\n' "$stack_name" "$stack_status"
  skip_resources="$(collect_rollback_skip_resources "$stack_name")"
  if [[ -n "$skip_resources" ]]; then
    printf '::warning::Skipping CloudFormation resources for %s: %s\n' "$stack_name" "$skip_resources"
    log "Suggested one-click repair command:"
    log "  aws cloudformation continue-update-rollback --stack-name $stack_name --resources-to-skip $skip_resources"
    if ! aws cloudformation continue-update-rollback --stack-name "$stack_name" --resources-to-skip $skip_resources; then
      die "CloudFormation continue-update-rollback failed for $stack_name. Repair the skipped resources before rerunning prod deploy."
    fi
  else
    printf '::warning::No explicit skip list found for %s; retrying rollback without skips.\n' "$stack_name"
    log "Suggested one-click repair command:"
    log "  aws cloudformation continue-update-rollback --stack-name $stack_name"
    if ! aws cloudformation continue-update-rollback --stack-name "$stack_name"; then
      die "CloudFormation continue-update-rollback failed for $stack_name. Inspect stack events before rerunning prod deploy."
    fi
  fi
  log "Rollback recovery request submitted for $stack_name"

  for attempts in {1..60}; do
    current_status="$(aws cloudformation describe-stacks --stack-name "$stack_name" --query 'Stacks[0].StackStatus' --output text 2>/dev/null || true)"
    log "Waiting for CloudFormation stack recovery ($stack_name): attempt $attempts/60, status=${current_status:-unknown}"
    case "$current_status" in
      UPDATE_ROLLBACK_COMPLETE|UPDATE_COMPLETE|CREATE_COMPLETE)
        printf '::notice::Stack %s recovered with status %s.\n' "$stack_name" "$current_status"
        if [[ -n "$skip_resources" ]]; then
          printf '::warning::CloudFormation recovery for %s required skipping resources: %s. Continuing deploy so CloudFormation can reconcile them.\n' "$stack_name" "$skip_resources"
        fi
        return 0
        ;;
      UPDATE_ROLLBACK_FAILED|UPDATE_ROLLBACK_IN_PROGRESS|UPDATE_IN_PROGRESS|CREATE_IN_PROGRESS|ROLLBACK_IN_PROGRESS|ROLLBACK_FAILED)
        if [[ "$current_status" == "UPDATE_ROLLBACK_FAILED" ]]; then
          log "Current CloudFormation failures for $stack_name:"
          describe_current_stack_failures "$stack_name" | while IFS= read -r line; do
            log "  $line"
          done
          restore_missing_lambda_functions "$stack_name"
          stagnant_failed_polls=$((stagnant_failed_polls + 1))
          current_skip_resources="$(collect_rollback_skip_resources "$stack_name")"
          merged_skip_resources="$(join_unique_words "${skip_resources:-}" "${current_skip_resources:-}")"
          if [[ -n "$merged_skip_resources" && "$merged_skip_resources" != "${skip_resources:-}" ]]; then
            log "Refreshed rollback skip resources for $stack_name: $merged_skip_resources"
            skip_resources="$merged_skip_resources"
            printf '::warning::Retrying rollback recovery for %s with updated skip list: %s\n' "$stack_name" "$skip_resources"
            log "Suggested one-click repair command:"
            log "  aws cloudformation continue-update-rollback --stack-name $stack_name --resources-to-skip $skip_resources"
            if ! aws cloudformation continue-update-rollback --stack-name "$stack_name" --resources-to-skip $skip_resources; then
              die "CloudFormation continue-update-rollback retry failed for $stack_name. Repair the skipped resources before rerunning prod deploy."
            fi
            stagnant_failed_polls=0
          fi
          if (( stagnant_failed_polls >= 6 )); then
            die "CloudFormation stack recovery for $stack_name is not progressing after ${stagnant_failed_polls} failed polls. Repair the reported resource errors before rerunning prod deploy."
          fi
        fi
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

  log "Resolving repository root"
  ROOT="$(resolve_repo_root)"
  CONFIG_PATH="$ROOT/infra/pipeline-config.json"
  ASSET_DIR="$ROOT/release-assets"
  DEPLOY_REPORT="$ROOT/prod-deploy-report.json"

  [[ -f "$CONFIG_PATH" ]] || die "Missing pipeline config: $CONFIG_PATH"

  cd "$ROOT"

  log "Loading deployment metadata from $CONFIG_PATH"
  REPO_NAME="$(json_read "$CONFIG_PATH" repo_name)"
  STACK_PREFIX="$(json_read "$CONFIG_PATH" name_prefix)"
  export REPO_NAME
  export MCP_CDK_ASSET_DIR="$ASSET_DIR"
  export MCP_PIPELINE_CONFIG_PATH="$CONFIG_PATH"
  export GH_TOKEN="${GH_TOKEN:-${GITHUB_TOKEN:-}}"
  export PYTHONUNBUFFERED=1

  log "Recovering production stacks for prefix $STACK_PREFIX"
  recover_failed_stack "$STACK_PREFIX-foundation"
  recover_failed_stack "$STACK_PREFIX-compute"
  recover_failed_stack "$STACK_PREFIX-api"

  log "Checking release tag $release_tag"
  if gh release view "$release_tag" >/dev/null 2>&1; then
    log "Release $release_tag exists; downloading assets"
    download_release_assets
  elif [[ "$release_tag" == "main" ]]; then
    printf '::notice::Release %s does not exist yet. Building release assets from the checked-out source.\n' "$release_tag"
    build_release_assets_from_source
  else
    die "Release '$release_tag' does not exist. Publish the release first or pass an existing release tag."
  fi

  validate_release_manifest
  restore_missing_lambda_functions "$STACK_PREFIX-compute"

  log "Installing CDK dependencies"
  npm ci --prefix infra/cdk

  if [[ -n "${AWS_ROLE_TO_ASSUME:-}" || -n "${AWS_ACCESS_KEY_ID:-}" || -n "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
    : # AWS credentials are expected to be configured by the workflow.
  else
    die "AWS credentials are not configured"
  fi

  log "Resolving AWS identity for the deploy account"
  CDK_DEFAULT_ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
  CDK_DEFAULT_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
  export CDK_DEFAULT_ACCOUNT
  export CDK_DEFAULT_REGION

  log "Running CDK deploy"
  npm --prefix infra/cdk run deploy

  log "Writing deploy report to $DEPLOY_REPORT"
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
