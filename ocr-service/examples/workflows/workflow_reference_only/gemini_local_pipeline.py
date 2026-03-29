"""
EN: Run a local manifest -> markdown -> Gemini embedding pipeline with serverless_mcp core code.
CN: 使用 serverless_mcp 核心代码在本地运行 manifest -> markdown -> Gemini embedding 流水线。
"""
# EN: Reference-only scripts intentionally mutate sys.path before importing service code.
# CN: 参考脚本会在导入服务代码前有意修改 sys.path。
# ruff: noqa: E402
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any


REFERENCE_ONLY = True

REPO_ROOT = Path(__file__).resolve().parents[1]
EXTRACT_WORKER_PARENT = REPO_ROOT / "services"
if str(EXTRACT_WORKER_PARENT) not in sys.path:
    sys.path.insert(0, str(EXTRACT_WORKER_PARENT))

from serverless_mcp.embed.gemini_client import GeminiEmbeddingClient
from serverless_mcp.extract.policy import DEFAULT_POLICY, estimate_tokens, split_text_for_embedding
from serverless_mcp.ocr.paddle_manifest_builder import PaddleOCRManifestBuilder
from serverless_mcp.domain.models import ChunkManifest, EmbeddingRequest, ExtractedAsset, ExtractedChunk, S3ObjectRef


def _load_local_env_file(path: Path) -> None:
    """
    EN: Load key-value pairs from a local .env file without overriding shell variables.
    CN: 同上。
    """
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or key in os.environ:
            continue
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        os.environ[key] = value


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """
    EN: Parse CLI arguments for the local manifest-to-embedding pipeline.
    CN: 同上。
    """
    _load_local_env_file(REPO_ROOT / ".env")

    parser = argparse.ArgumentParser(description="Local manifest -> markdown -> Gemini embedding runner")
    reference_root = REPO_ROOT / "workflow_reference_only"
    parser.add_argument(
        "input_json",
        nargs="?",
        type=Path,
        default=reference_root / "manifest.json",
        help="Path to a manifest JSON or OCR JSON input; defaults to ./examples/workflows/workflow_reference_only/manifest.json",
    )
    parser.add_argument("--tenant-id", default="local", help="Tenant identifier used if OCR JSON fallback is used")
    parser.add_argument("--source-bucket", default="local-bucket", help="Source bucket name for OCR JSON fallback")
    parser.add_argument("--source-key", default="local/ocr.pdf", help="Source object key for OCR JSON fallback")
    parser.add_argument("--version-id", default="local", help="Source version_id for OCR JSON fallback")
    parser.add_argument("--gemini-api-key", default=os.environ.get("GEMINI_API_KEY"), help="Gemini API key")
    parser.add_argument(
        "--gemini-api-base-url",
        default=os.environ.get("GEMINI_API_BASE_URL", "https://generativelanguage.googleapis.com/"),
        help="Gemini API base URL",
    )
    parser.add_argument(
        "--gemini-model",
        default=os.environ.get("GEMINI_EMBEDDING_MODEL", DEFAULT_POLICY.model),
        help="Gemini embedding model name",
    )
    parser.add_argument(
        "--gemini-http-timeout-seconds",
        type=int,
        default=int(os.environ.get("GEMINI_HTTP_TIMEOUT_SECONDS", "120")),
        help="Per-request Gemini timeout in seconds",
    )
    parser.add_argument(
        "--safe-text-tokens",
        type=int,
        default=DEFAULT_POLICY.safe_text_tokens,
        help="Markdown split threshold used before embedding",
    )
    parser.add_argument(
        "--output-dimensionality",
        type=int,
        default=DEFAULT_POLICY.output_dimensionality,
        help="Embedding vector dimensionality",
    )
    parser.add_argument(
        "--output",
        dest="embedding_output",
        type=Path,
        default=reference_root / "embedding_result.json",
        help="Output file for the full JSON embedding result payload",
    )
    parser.add_argument(
        "--markdown-output",
        type=Path,
        default=reference_root / "merged_markdown.md",
        help="Output file for the merged markdown text",
    )
    parser.add_argument(
        "--split-output-dir",
        type=Path,
        default=reference_root / "split_parts",
        help="Directory that stores the split markdown parts used for embedding",
    )
    return parser.parse_args(argv)


def _looks_like_manifest_payload(payload: Any) -> bool:
    """
    EN: Check whether a JSON document already matches the serialized manifest shape.
    CN: 同上。
    """
    return isinstance(payload, dict) and "source" in payload and "chunks" in payload and "assets" in payload


def _source_from_payload(payload: dict[str, Any]) -> S3ObjectRef:
    """
    EN: Convert serialized source metadata back into an immutable S3 reference.
    CN: 同上。
    """
    security_scope = payload.get("security_scope")
    if isinstance(security_scope, list):
        security_scope = tuple(str(item) for item in security_scope if isinstance(item, str))
    elif not isinstance(security_scope, tuple):
        security_scope = ()

    return S3ObjectRef(
        tenant_id=str(payload.get("tenant_id") or "local"),
        bucket=str(payload.get("bucket") or "local-bucket"),
        key=str(payload.get("key") or "local/ocr.pdf"),
        version_id=str(payload.get("version_id") or "local"),
        sequencer=payload.get("sequencer") if isinstance(payload.get("sequencer"), str) else None,
        etag=payload.get("etag") if isinstance(payload.get("etag"), str) else None,
        content_type=payload.get("content_type") if isinstance(payload.get("content_type"), str) else None,
        security_scope=security_scope,
        language=str(payload.get("language") or "zh"),
    )


def _chunk_from_payload(payload: dict[str, Any]) -> ExtractedChunk:
    """
    EN: Restore an extracted chunk from serialized JSON.
    CN: 同上。
    """
    page_span = payload.get("page_span")
    if isinstance(page_span, list) and len(page_span) == 2 and all(isinstance(item, int) for item in page_span):
        page_span_value = (page_span[0], page_span[1])
    else:
        page_span_value = None

    section_path = payload.get("section_path")
    if isinstance(section_path, list):
        section_path_value = tuple(str(item) for item in section_path if isinstance(item, str))
    else:
        section_path_value = ()

    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    return ExtractedChunk(
        chunk_id=str(payload.get("chunk_id") or "chunk#000000"),
        chunk_type=str(payload.get("chunk_type") or "page_text_chunk"),
        text=str(payload.get("text") or ""),
        doc_type=str(payload.get("doc_type") or "pdf"),
        token_estimate=int(payload.get("token_estimate") or 0),
        page_no=payload.get("page_no") if isinstance(payload.get("page_no"), int) else None,
        page_span=page_span_value,
        slide_no=payload.get("slide_no") if isinstance(payload.get("slide_no"), int) else None,
        section_path=section_path_value,
        metadata=metadata,
    )


def _asset_from_payload(payload: dict[str, Any]) -> ExtractedAsset:
    """
    EN: Restore an extracted asset from serialized JSON.
    CN: 同上。
    """
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    asset_payload = payload.get("payload")
    if isinstance(asset_payload, str):
        payload_bytes = asset_payload.encode("utf-8")
    elif isinstance(asset_payload, bytes):
        payload_bytes = asset_payload
    else:
        payload_bytes = None

    return ExtractedAsset(
        asset_id=str(payload.get("asset_id") or "asset#000000"),
        chunk_type=str(payload.get("chunk_type") or "page_image_chunk"),
        mime_type=str(payload.get("mime_type") or "application/octet-stream"),
        payload=payload_bytes,
        asset_s3_uri=payload.get("asset_s3_uri") if isinstance(payload.get("asset_s3_uri"), str) else None,
        page_no=payload.get("page_no") if isinstance(payload.get("page_no"), int) else None,
        slide_no=payload.get("slide_no") if isinstance(payload.get("slide_no"), int) else None,
        metadata=metadata,
    )


def _manifest_from_payload(payload: dict[str, Any]) -> ChunkManifest:
    """
    EN: Convert a serialized manifest payload back into core dataclasses.
    CN: 将序列化后的 manifest JSON 还原为核心 dataclass 对象。
    """
    source = _source_from_payload(payload["source"])
    chunks = [_chunk_from_payload(item) for item in payload.get("chunks", []) if isinstance(item, dict)]
    assets = [_asset_from_payload(item) for item in payload.get("assets", []) if isinstance(item, dict)]
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    return ChunkManifest(
        source=source,
        doc_type=str(payload.get("doc_type") or "unknown"),
        chunks=chunks,
        assets=assets,
        metadata=metadata,
    )


def load_pipeline_manifest(path: Path, *, fallback_source: S3ObjectRef) -> ChunkManifest:
    """
    EN: Load an existing manifest directly, or build one from OCR JSON as a fallback.
    CN: 同上。
    """
    payload = json.loads(path.read_text(encoding="utf-8"))
    if _looks_like_manifest_payload(payload):
        return _manifest_from_payload(payload)

    json_lines = normalize_ocr_payload(payload)
    builder = PaddleOCRManifestBuilder()
    return builder.build_manifest(
        source=fallback_source,
        json_lines=json_lines,
        binary_loader=_placeholder_binary_loader,
    )


def normalize_ocr_payload(payload: Any) -> list[dict[str, Any]]:
    """
    EN: Normalize common PaddleOCR JSON shapes into the builder's JSONL-style list.
    CN: 把常见的 PaddleOCR JSON 形态统一成 builder 所需的 JSONL 风格列表。
    """
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if isinstance(payload, dict):
        for key in ("json_lines", "records", "pages", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return [payload]

    raise TypeError(f"Unsupported OCR payload type: {type(payload)!r}")


def _placeholder_binary_loader(url: str) -> tuple[bytes, str | None]:
    """
    EN: Provide a local placeholder for remote assets so the text pipeline can run offline.
    CN: 同上。
    """
    return b"", None


def select_document_markdown(manifest: Any) -> str:
    """
    EN: Select the merged markdown document from the manifest, then fall back to markdown assets or chunks.
    CN: 同上。
    """
    assets_by_path = {
        str(asset.metadata.get("relative_path") or ""): asset
        for asset in manifest.assets
        if isinstance(asset.metadata, dict)
    }
    document_asset = assets_by_path.get("document.md")
    if document_asset and document_asset.payload:
        return document_asset.payload.decode("utf-8")

    markdown_assets = [
        asset
        for asset in manifest.assets
        if getattr(asset, "mime_type", None) == "text/markdown" and getattr(asset, "payload", None)
    ]
    if markdown_assets:
        markdown_assets.sort(key=lambda asset: str(asset.metadata.get("relative_path") or ""))
        return "\n\n---\n\n".join(asset.payload.decode("utf-8") for asset in markdown_assets if asset.payload)

    return "\n\n".join(chunk.text for chunk in manifest.chunks if chunk.text)


def split_markdown_for_embedding(markdown_text: str, *, safe_text_tokens: int) -> list[str]:
    """
    EN: Split merged markdown into Gemini-safe embedding parts using the repository policy helper.
    CN: 使用仓库内的 policy helper 将合并后的 markdown 拆分成适合 Gemini embedding 的片段。
    """
    return split_text_for_embedding(markdown_text, max_tokens=safe_text_tokens)


def embed_markdown_parts(
    parts: list[str],
    *,
    api_key: str,
    base_url: str,
    model: str,
    timeout_seconds: int,
    output_dimensionality: int,
) -> tuple[GeminiEmbeddingClient, list[dict[str, Any]]]:
    """
    EN: Send split markdown parts to Gemini embedding and collect the resulting vectors.
    CN: 将拆分后的 markdown 发送给 Gemini embedding，并收集向量结果。
    """
    client = GeminiEmbeddingClient(
        api_key=api_key,
        base_url=base_url,
        model=model,
        timeout_seconds=timeout_seconds,
    )
    results: list[dict[str, Any]] = []
    for index, part in enumerate(parts, start=1):
        request = EmbeddingRequest(
            chunk_id=f"local#part{index:06d}",
            chunk_type="document_markdown_chunk",
            content_kind="text",
            text=part,
            output_dimensionality=output_dimensionality,
            task_type="RETRIEVAL_DOCUMENT",
            metadata={
                "source": "workflow_reference_only.gemini_local_pipeline",
                "part_index": index,
                "token_estimate": estimate_tokens(part),
            },
        )
        vector = client.embed_text(request)
        results.append(
            {
                "chunk_id": request.chunk_id,
                "token_estimate": estimate_tokens(part),
                "text": part,
                "vector_length": len(vector),
                "vector": vector,
            }
    )
    return client, results


def _write_local_outputs(*, markdown_text: str, markdown_output: Path, result: dict[str, Any], embedding_output: Path) -> None:
    """
    EN: Persist the merged markdown text and the full embedding result to local files.
    CN: 同上。
    """
    markdown_output.parent.mkdir(parents=True, exist_ok=True)
    embedding_output.parent.mkdir(parents=True, exist_ok=True)
    markdown_output.write_text(markdown_text, encoding="utf-8")
    embedding_output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_split_markdown_parts(parts: list[str], split_output_dir: Path) -> list[Path]:
    """
    EN: Persist every embedding split part as a standalone markdown file.
    CN: 同上。
    """
    split_output_dir.mkdir(parents=True, exist_ok=True)
    written_paths: list[Path] = []
    for index, part in enumerate(parts, start=1):
        path = split_output_dir / f"part-{index:04d}.md"
        path.write_text(part, encoding="utf-8")
        written_paths.append(path)
    return written_paths


def _write_split_embedding_results(parts: list[dict[str, Any]], split_output_dir: Path) -> list[Path]:
    """
    EN: Persist one embedding JSON sidecar per split markdown file.
    CN: 同上。
    """
    split_output_dir.mkdir(parents=True, exist_ok=True)
    written_paths: list[Path] = []
    for index, item in enumerate(parts, start=1):
        path = split_output_dir / f"part-{index:04d}.embedding.json"
        payload = {
            "chunk_id": item["chunk_id"],
            "token_estimate": item["token_estimate"],
            "vector_length": item["vector_length"],
            "vector": item["vector"],
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        written_paths.append(path)
    return written_paths


def run_pipeline(args: argparse.Namespace) -> dict[str, Any]:
    """
    EN: Execute the local manifest merge, markdown split, and Gemini embedding flow.
    CN: 同上。
    """
    if not args.gemini_api_key:
        raise ValueError("GEMINI_API_KEY is required unless --gemini-api-key is provided")

    fallback_source = S3ObjectRef(
        tenant_id=args.tenant_id,
        bucket=args.source_bucket,
        key=args.source_key,
        version_id=args.version_id,
    )
    manifest = load_pipeline_manifest(args.input_json, fallback_source=fallback_source)
    merged_markdown = select_document_markdown(manifest)
    split_parts = split_markdown_for_embedding(merged_markdown, safe_text_tokens=args.safe_text_tokens)
    split_part_paths = _write_split_markdown_parts(split_parts, args.split_output_dir)
    _, embeddings = embed_markdown_parts(
        split_parts,
        api_key=args.gemini_api_key,
        base_url=args.gemini_api_base_url,
        model=args.gemini_model,
        timeout_seconds=args.gemini_http_timeout_seconds,
        output_dimensionality=args.output_dimensionality,
    )
    split_embedding_paths = _write_split_embedding_results(embeddings, args.split_output_dir)

    assets_summary = [
        {
            "asset_id": asset.asset_id,
            "chunk_type": asset.chunk_type,
            "mime_type": asset.mime_type,
            "relative_path": asset.metadata.get("relative_path"),
            "size_bytes": len(asset.payload or b""),
        }
        for asset in manifest.assets
    ]

    result = {
        "source": asdict(manifest.source),
        "manifest": {
            "doc_type": manifest.doc_type,
            "chunk_count": len(manifest.chunks),
            "asset_count": len(manifest.assets),
            "metadata": manifest.metadata,
            "chunks": [asdict(chunk) for chunk in manifest.chunks],
            "assets": assets_summary,
        },
        "merged_markdown": {
            "token_estimate": estimate_tokens(merged_markdown),
            "char_count": len(merged_markdown),
            "text": merged_markdown,
        },
        "split_parts": [
            {
                "chunk_id": item["chunk_id"],
                "token_estimate": item["token_estimate"],
                "vector_length": item["vector_length"],
                "text": item["text"],
            }
            for item in embeddings
        ],
        "embedding": {
            "model": args.gemini_model,
            "timeout_seconds": args.gemini_http_timeout_seconds,
            "output_dimensionality": args.output_dimensionality,
            "part_count": len(split_parts),
        },
        "output_paths": {
            "markdown_output": str(args.markdown_output),
            "embedding_output": str(args.embedding_output),
            "split_output_dir": str(args.split_output_dir),
            "split_part_files": [str(path) for path in split_part_paths],
            "split_embedding_files": [str(path) for path in split_embedding_paths],
        },
    }

    _write_local_outputs(
        markdown_text=merged_markdown,
        markdown_output=args.markdown_output,
        result=result,
        embedding_output=args.embedding_output,
    )
    return result


def main(argv: list[str] | None = None) -> int:
    """
    EN: CLI entry point for the local pipeline runner.
    CN: 同上。
    """
    args = parse_args(argv)
    result = run_pipeline(args)
    rendered = json.dumps(result, ensure_ascii=False, indent=2)
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
