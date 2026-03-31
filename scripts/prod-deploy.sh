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

count_deleted_stack_drifts() {
  local stack_name="$1"

  python3 - "$stack_name" <<'PY'
from __future__ import annotations

import json
import subprocess
import sys

stack_name = sys.argv[1]
payload = json.loads(
    subprocess.check_output(
        [
            "aws",
            "cloudformation",
            "describe-stack-resource-drifts",
            "--stack-name",
            stack_name,
            "--output",
            "json",
        ],
        text=True,
    )
)
count = sum(1 for drift in payload.get("StackResourceDrifts", []) if drift.get("StackResourceDriftStatus") == "DELETED")
print(count)
PY
}

log_deleted_stack_drifts() {
  local stack_name="$1"

  python3 - "$stack_name" <<'PY'
from __future__ import annotations

import json
import subprocess
import sys

stack_name = sys.argv[1]
payload = json.loads(
    subprocess.check_output(
        [
            "aws",
            "cloudformation",
            "describe-stack-resource-drifts",
            "--stack-name",
            stack_name,
            "--output",
            "json",
        ],
        text=True,
    )
)
deleted = []
for drift in payload.get("StackResourceDrifts", []):
    if drift.get("StackResourceDriftStatus") != "DELETED":
        continue
    deleted.append(
        {
            "logical_id": drift.get("LogicalResourceId") or "unknown",
            "resource_type": drift.get("ResourceType") or "unknown",
            "physical_id": drift.get("PhysicalResourceId") or "unknown",
            "reason": (drift.get("StackResourceDriftStatusReason") or "").replace("\n", " ").strip(),
        }
    )

print(json.dumps({"stack_name": stack_name, "deleted": deleted}, ensure_ascii=False, indent=2))
PY
}

delete_stack_and_wait() {
  local stack_name="$1"

  log "Deleting drifted stack $stack_name so CloudFormation can recreate missing resources from the template"
  log "Suggested one-click repair command:"
  log "  aws cloudformation delete-stack --stack-name $stack_name"
  if ! aws cloudformation delete-stack --stack-name "$stack_name"; then
    die "Failed to submit delete-stack for $stack_name"
  fi
  log "Waiting for stack deletion to finish: $stack_name"
  if ! aws cloudformation wait stack-delete-complete --stack-name "$stack_name"; then
    die "Timed out waiting for stack deletion: $stack_name"
  fi
  log "Stack $stack_name deleted"
}

rebuild_drifted_stack_if_needed() {
  local stack_name="$1"
  local stack_kind="$2"
  local drift_detection_id detection_status stack_drift_status deleted_count

  log "Detecting CloudFormation drift for $stack_name"
  drift_detection_id="$(aws cloudformation detect-stack-drift --stack-name "$stack_name" --query 'StackDriftDetectionId' --output text)"
  log "Started drift detection for $stack_name: $drift_detection_id"

  for attempt in {1..60}; do
    detection_status="$(aws cloudformation describe-stack-drift-detection-status --stack-drift-detection-id "$drift_detection_id" --query 'DetectionStatus' --output text 2>/dev/null || true)"
    stack_drift_status="$(aws cloudformation describe-stack-drift-detection-status --stack-drift-detection-id "$drift_detection_id" --query 'StackDriftStatus' --output text 2>/dev/null || true)"
    log "Waiting for drift detection ($stack_name): attempt $attempt/60, detection_status=${detection_status:-unknown}, stack_drift_status=${stack_drift_status:-unknown}"
    case "$detection_status" in
      DETECTION_COMPLETE)
        break
        ;;
      DETECTION_FAILED)
        die "CloudFormation drift detection failed for $stack_name"
        ;;
      *)
        sleep 5
        ;;
    esac
  done

  if [[ "${detection_status:-}" != "DETECTION_COMPLETE" ]]; then
    die "Timed out waiting for drift detection: $stack_name"
  fi

  if [[ "${stack_drift_status:-}" != "DRIFTED" ]]; then
    log "Stack $stack_name is not drifted (status=${stack_drift_status:-unknown})"
    return 0
  fi

  log "Stack $stack_name is drifted; collecting deleted resource details"
  log "Recent CloudFormation events for $stack_name:"
  describe_recent_stack_events "$stack_name" | while IFS= read -r line; do
    log "  $line"
  done
  log "Deleted resources for $stack_name:"
  log_deleted_stack_drifts "$stack_name" | while IFS= read -r line; do
    log "  $line"
  done

  deleted_count="$(count_deleted_stack_drifts "$stack_name")"
  if [[ "$deleted_count" == "0" ]]; then
    log "Stack $stack_name is drifted but has no deleted resources; leaving it for a normal CDK update"
    return 0
  fi

  if [[ "$stack_kind" != "foundation" && "$stack_kind" != "api" ]]; then
    log "Stack $stack_name has deleted resources, but $stack_kind is not auto-recreated by this repair path"
    return 0
  fi

  printf '::warning::Stack %s has %s deleted resource(s); deleting and recreating it so CloudFormation can restore the missing resources.\n' "$stack_name" "$deleted_count"
  delete_stack_and_wait "$stack_name"
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
account_id = boto3.client("sts").get_caller_identity()["Account"]
region = boto3.session.Session().region_name or "us-east-1"

def resolve_name_suffix(suffix: str | None, account: str, region_name: str) -> str:
    if not suffix or suffix == "" or suffix == "none":
        return ""
    if suffix == "auto":
        return f"{account}-{region_name}"
    return suffix

suffix = resolve_name_suffix(config.get("name_suffix"), account_id, region)
base_names = config["resource_names"]
names = {
    key: (f"{value}-{suffix}" if suffix else value)
    for key, value in base_names.items()
}
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

print(
    json.dumps(
        {
            "stack_name": stack_name,
            "account_id": account_id,
            "region": region,
            "name_suffix": suffix,
            "resolved_lambda_role": names["lambda_role"],
            "resolved_functions": {
                spec["function_key"]: spec["function_name"]
                for spec in functions
            },
        },
        ensure_ascii=False,
        indent=2,
    )
)

def ensure_role_exists(role_name: str, service_principal: str, managed_policies: list[str]) -> str:
    def get_role_arn() -> str | None:
        try:
            return iam.get_role(RoleName=role_name)["Role"]["Arn"]
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            message = str(exc).lower()
            if error_code in {"NoSuchEntity", "NoSuchEntityException", "ResourceNotFoundException"} or "not found" in message:
                return None
            raise

    existing_role_arn = get_role_arn()
    if existing_role_arn:
        return existing_role_arn

    try:
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
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        message = str(exc).lower()
        if error_code not in {"EntityAlreadyExists", "EntityAlreadyExistsException", "EntityAlreadyExistsExceptionException"} and "already exists" not in message:
            raise

    for policy_name in managed_policies:
        iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn=f"arn:aws:iam::aws:policy/{policy_name}",
        )
    print(f"Created missing IAM role {role_name}")
    for attempt in range(1, 7):
        time.sleep(10)
        role_arn = get_role_arn()
        if role_arn:
            return role_arn
        print(f"Waiting for IAM role visibility for {role_name} (attempt {attempt}/6)")
    return get_role_arn() or sys.exit(f"Failed to observe IAM role {role_name} after creation")

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

def ensure_state_machine_role_exists(role_name: str, lambda_function_arns: list[str], log_group_arn: str) -> str:
    def get_role_arn() -> str | None:
        try:
            return iam.get_role(RoleName=role_name)["Role"]["Arn"]
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            message = str(exc).lower()
            if error_code in {"NoSuchEntity", "NoSuchEntityException", "ResourceNotFoundException"} or "not found" in message:
                return None
            raise

    existing_role_arn = get_role_arn()
    if existing_role_arn:
        return existing_role_arn

    try:
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "states.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }
                    ],
                }
            ),
            Description=f"Rehydrated Step Functions execution role for {role_name}",
        )
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        message = str(exc).lower()
        if error_code not in {"EntityAlreadyExists", "EntityAlreadyExistsException", "EntityAlreadyExistsExceptionException"} and "already exists" not in message:
            raise

    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="RehydratedStateMachineAccess",
        PolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["lambda:InvokeFunction"],
                        "Resource": lambda_function_arns,
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "logs:CreateLogDelivery",
                            "logs:CreateLogStream",
                            "logs:GetLogDelivery",
                            "logs:PutLogEvents",
                            "logs:UpdateLogDelivery",
                            "logs:DeleteLogDelivery",
                            "logs:ListLogDeliveries",
                            "logs:PutResourcePolicy",
                            "logs:DescribeResourcePolicies",
                            "logs:DescribeLogGroups",
                        ],
                        "Resource": log_group_arn,
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["xray:PutTraceSegments", "xray:PutTelemetryRecords"],
                        "Resource": "*",
                    },
                ],
            }
        ),
    )
    print(f"Created missing IAM role {role_name}")
    for attempt in range(1, 7):
        time.sleep(10)
        role_arn = get_role_arn()
        if role_arn:
            return role_arn
        print(f"Waiting for IAM role visibility for {role_name} (attempt {attempt}/6)")
    return get_role_arn() or sys.exit(f"Failed to observe IAM role {role_name} after creation")

def restore_missing_state_machine() -> None:
    sfn = boto3.client("stepfunctions")
    state_machine_name = names["state_machine"]
    state_machine_arn = f"arn:aws:states:{region}:{account_id}:stateMachine:{state_machine_name}"
    state_machine_role_name = names["state_machine_role"]
    state_machine_log_group_arn = f"arn:aws:logs:{region}:{account_id}:log-group:{names['state_machine_log_group']}"

    lambda_arns_by_key: dict[str, str] = {}
    for spec in functions:
        function_name = spec["function_name"]
        lambda_arns_by_key[spec["function_key"]] = lambda_client.get_function(FunctionName=function_name)["Configuration"]["FunctionArn"]

    try:
        sfn.describe_state_machine(stateMachineArn=state_machine_arn)
        return
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code not in {"StateMachineDoesNotExist", "StateMachineDoesNotExistException", "StateMachineDoesNotExistExceptionException"} and "does not exist" not in str(exc).lower():
            raise

    role_arn = ensure_state_machine_role_exists(
        state_machine_role_name,
        [lambda_arns_by_key[key] for key in [
            "extract_prepare",
            "extract_sync",
            "extract_submit",
            "extract_poll",
            "extract_persist",
            "extract_mark_failed",
        ]],
        state_machine_log_group_arn,
    )

    template_path = Path("ocr-service/ocr-pipeline/src/serverless_mcp/workflows/extract_state_machine.asl.json")
    template = template_path.read_text(encoding="utf-8")
    placeholders = [
        ("${PREPARE_LAMBDA_ARN}", "extract_prepare"),
        ("${SYNC_LAMBDA_ARN}", "extract_sync"),
        ("${SUBMIT_LAMBDA_ARN}", "extract_submit"),
        ("${POLL_LAMBDA_ARN}", "extract_poll"),
        ("${PERSIST_LAMBDA_ARN}", "extract_persist"),
        ("${MARK_FAILED_LAMBDA_ARN}", "extract_mark_failed"),
    ]
    for placeholder, key in placeholders:
        template = template.replace(placeholder, lambda_arns_by_key[key])
    json.loads(template)

    for attempt in range(1, 6):
        try:
            sfn.create_state_machine(
                name=state_machine_name,
                definition=template,
                roleArn=role_arn,
                type="STANDARD",
            )
            print(f"Created missing Step Functions state machine {state_machine_name}")
            return
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            message = str(exc)
            if error_code == "InvalidParameterValueException" and "cannot be assumed" in message:
                wait_seconds = 10 * attempt
                print(f"Waiting {wait_seconds}s for IAM trust propagation before retrying state machine {state_machine_name} (attempt {attempt}/5)")
                time.sleep(wait_seconds)
                continue
            raise
    raise SystemExit(f"Failed to create Step Functions state machine {state_machine_name} after IAM trust retries")

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

restore_missing_state_machine()

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
  export PYTHONUNBUFFERED=1

  log "Recovering production stacks for prefix $STACK_PREFIX"
  recover_failed_stack "$STACK_PREFIX-foundation"
  recover_failed_stack "$STACK_PREFIX-compute"
  recover_failed_stack "$STACK_PREFIX-api"

  log "Inspecting drift on production stacks"
  rebuild_drifted_stack_if_needed "$STACK_PREFIX-foundation" "foundation"
  rebuild_drifted_stack_if_needed "$STACK_PREFIX-compute" "compute"
  rebuild_drifted_stack_if_needed "$STACK_PREFIX-api" "api"

  log "Building release assets from the current checkout"
  build_release_assets_from_source
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
