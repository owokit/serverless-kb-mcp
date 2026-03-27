"""
EN: Upload built Lambda ZIP artifacts from local dist to an S3 bucket.
CN: 将本地 dist 目录中构建好的 Lambda ZIP 产物上传到 S3 bucket。
"""
# ruff: noqa: E402
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import boto3

SCRIPT_ROOT = Path(__file__).resolve().parent
REPO_ROOT = Path(__file__).resolve().parents[2]
SERVICE_ROOT = REPO_ROOT / "services" / "ocr-pipeline"
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from lambda_artifacts import build_s3_key, build_zip_name, parse_lambda_functions


def main() -> int:
    """EN: CLI entry point that uploads selected or all Lambda ZIP artifacts to S3.
    CN: CLI 入口，将选定或全部 Lambda ZIP 产物上传到 S3。"""
    parser = argparse.ArgumentParser(description="Upload built Lambda ZIP artifacts to S3.")
    parser.add_argument("--repo-name", required=True)
    parser.add_argument("--artifact-dir", default=str(SERVICE_ROOT / "dist"))
    parser.add_argument("--s3-bucket", required=True)
    parser.add_argument("--s3-prefix", required=True)
    parser.add_argument("--region", default=None)
    parser.add_argument("--functions", default=None, help="Optional space- or comma-separated function keys.")
    args = parser.parse_args()

    artifact_dir = Path(args.artifact_dir)
    selected_functions = parse_lambda_functions(args.functions)
    s3_client = boto3.client("s3", region_name=args.region)

    for function_key in selected_functions:
        zip_name = build_zip_name(repo_name=args.repo_name, function_key=function_key)
        zip_path = artifact_dir / zip_name
        s3_key = build_s3_key(repo_name=args.repo_name, function_key=function_key, s3_prefix=args.s3_prefix)
        s3_client.upload_file(str(zip_path), args.s3_bucket, s3_key)
        print(f"uploaded {zip_path} -> s3://{args.s3_bucket}/{s3_key}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
