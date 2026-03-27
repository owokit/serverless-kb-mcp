"""
EN: Preview-only command-line interface for extracting version-aware S3 documents into chunk manifests.
CN: 用于预览版本化 S3 文档抽取结果的只读命令行入口。
"""
from __future__ import annotations

import argparse
import json
from dataclasses import asdict

from serverless_mcp.extract.application import ExtractionService
from serverless_mcp.domain.models import S3ObjectRef


def main() -> None:
    """
    EN: Parse CLI arguments and render a preview manifest for a single S3 object version.
    CN: 解析 CLI 参数并输出单个 S3 对象版本的预览 manifest。
    """
    parser = argparse.ArgumentParser(description="预览版本化 S3 文档的 chunk manifest。")
    # EN: S3 identity arguments - all required for immutable version-specific retrieval.
    # CN: 同上。
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--key", required=True)
    parser.add_argument("--version-id", required=True)
    parser.add_argument("--sequencer")
    parser.add_argument("--security-scope", action="append", default=[])
    parser.add_argument("--language", default="zh")
    args = parser.parse_args()

    service = ExtractionService()
    manifest = service.extract_from_s3(
        S3ObjectRef(
            tenant_id=args.tenant_id,
            bucket=args.bucket,
            key=args.key,
            version_id=args.version_id,
            sequencer=args.sequencer,
            security_scope=tuple(args.security_scope),
            language=args.language,
        )
    )
    # EN: Output manifest as JSON preview; embedding_requests require persisted manifest_s3_uri first.
    # CN: 同上。
    print(
        json.dumps(
            {
                "manifest": asdict(manifest),
                "note": "此命令仅用于预览；embedding_requests 仍需先持久化 manifest，再通过 manifest_s3_uri 和 asset_s3_uri 解析内容。",
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
