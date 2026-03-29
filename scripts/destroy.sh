#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: destroy.sh [--name-prefix PREFIX] [--confirm-destroy PREFIX]

Destroy the deployed CDK resources from a single repository-root-aware entrypoint.
EOF
}

die() {
  printf '::error::%s\n' "$*" >&2
  exit 1
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

main() {
  local arg name_prefix confirm_destroy config_path
  name_prefix="mcp-doc-pipeline-prod"
  confirm_destroy="mcp-doc-pipeline-prod"

  while (($#)); do
    arg="$1"
    case "$arg" in
      --name-prefix)
        [[ $# -ge 2 ]] || die "--name-prefix requires a value"
        name_prefix="$2"
        shift 2
        ;;
      --confirm-destroy)
        [[ $# -ge 2 ]] || die "--confirm-destroy requires a value"
        confirm_destroy="$2"
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

  [[ "$name_prefix" == "$confirm_destroy" ]] || die "confirm_destroy does not match name_prefix"

  ROOT="$(resolve_repo_root)"
  CONFIG_PATH="$ROOT/infra/pipeline-config.json"

  [[ -f "$CONFIG_PATH" ]] || die "Missing pipeline config: $CONFIG_PATH"

  cd "$ROOT"

  if [[ -n "${AWS_ROLE_TO_ASSUME:-}" || -n "${AWS_ACCESS_KEY_ID:-}" || -n "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
    : # AWS credentials are expected to be configured by the workflow.
  else
    die "AWS credentials are not configured"
  fi

  REPO_NAME="$(json_read "$CONFIG_PATH" repo_name)"
  export REPO_NAME
  export MCP_CDK_ASSET_DIR="$ROOT/release-assets"
  export MCP_PIPELINE_CONFIG_PATH="$CONFIG_PATH"
  export MCP_ALLOW_PLACEHOLDER_ASSETS="${MCP_ALLOW_PLACEHOLDER_ASSETS:-true}"

  python3 - "$CONFIG_PATH" "$name_prefix" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
expected_name_prefix = sys.argv[2]
config = json.loads(path.read_text(encoding="utf-8"))
actual_name_prefix = config.get("name_prefix")
if actual_name_prefix != expected_name_prefix:
    raise SystemExit(
        f"name_prefix does not match infra/pipeline-config.json: expected {expected_name_prefix!r}, got {actual_name_prefix!r}"
    )
PY

  npm ci --prefix infra/cdk

  CDK_DEFAULT_ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
  CDK_DEFAULT_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
  export CDK_DEFAULT_ACCOUNT
  export CDK_DEFAULT_REGION

  npm --prefix infra/cdk run destroy -- --all --progress events
}

main "$@"
