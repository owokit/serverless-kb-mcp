"""
EN: Tests for the Gemini local pipeline reference module covering manifest loading, markdown selection, and embedding output.
CN: Gemini 本地流水线参考模块的测试，覆盖 manifest 加载、markdown 选择和 embedding 输出。
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from examples.workflows.workflow_reference_only.gemini_local_pipeline import (
    REPO_ROOT,
    _load_local_env_file,
    normalize_ocr_payload,
    parse_args,
    load_pipeline_manifest,
    run_pipeline,
    select_document_markdown,
)
from serverless_mcp.domain.models import ChunkManifest, ExtractedAsset, ExtractedChunk, S3ObjectRef


pytestmark = pytest.mark.requires_network


def test_normalize_ocr_payload_accepts_list_and_wrappers() -> None:
    payload = normalize_ocr_payload(
        {
            "records": [
                {"result": {"layoutParsingResults": []}},
                {"result": {"layoutParsingResults": []}},
            ]
        }
    )

    assert len(payload) == 2
    assert payload[0]["result"]["layoutParsingResults"] == []


def test_select_document_markdown_prefers_document_asset() -> None:
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/scan.pdf", version_id="v1")
    manifest = ChunkManifest(
        source=source,
        doc_type="pdf",
        chunks=[
            ExtractedChunk(
                chunk_id="chunk#000001",
                chunk_type="page_text_chunk",
                text="chunk markdown",
                doc_type="pdf",
                token_estimate=10,
                page_no=1,
                page_span=(1, 1),
                section_path=("page-1", "layout-1"),
                metadata={},
            )
        ],
        assets=[
            ExtractedAsset(
                asset_id="ocr#markdown",
                chunk_type="document_markdown_chunk",
                mime_type="text/markdown",
                payload=b"# Intro\n\nhello world",
                metadata={"relative_path": "document.md"},
            )
        ],
        metadata={},
    )

    assert select_document_markdown(manifest) == "# Intro\n\nhello world"


def test_load_local_env_file_reads_values_without_overriding_existing_env(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "GEMINI_API_KEY=from-file\n"
        "GEMINI_HTTP_TIMEOUT_SECONDS=180\n"
        "export GEMINI_API_BASE_URL=https://example.invalid/\n"
        "UNRELATED=value-from-file\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("GEMINI_API_KEY", "from-shell")
    monkeypatch.delenv("GEMINI_HTTP_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("GEMINI_API_BASE_URL", raising=False)
    monkeypatch.delenv("UNRELATED", raising=False)

    _load_local_env_file(env_file)

    assert os.environ["GEMINI_API_KEY"] == "from-shell"
    assert os.environ["GEMINI_HTTP_TIMEOUT_SECONDS"] == "180"
    assert os.environ["GEMINI_API_BASE_URL"] == "https://example.invalid/"
    assert os.environ["UNRELATED"] == "value-from-file"


def test_parse_args_defaults_input_json_to_manifest_file(monkeypatch) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "local-key")

    args = parse_args([])

    assert args.input_json == REPO_ROOT / "workflow_reference_only" / "manifest.json"
    assert args.markdown_output == REPO_ROOT / "workflow_reference_only" / "merged_markdown.md"
    assert args.embedding_output == REPO_ROOT / "workflow_reference_only" / "embedding_result.json"
    assert args.split_output_dir == REPO_ROOT / "workflow_reference_only" / "split_parts"


def test_load_pipeline_manifest_reads_existing_manifest_file(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "local-key")

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "source": {
                    "tenant_id": "lookup",
                    "bucket": "bucket",
                    "key": "docs/scan.pdf",
                    "version_id": "v1",
                    "language": "zh",
                },
                "doc_type": "pdf",
                "chunks": [
                    {
                        "chunk_id": "chunk#000001",
                        "chunk_type": "page_text_chunk",
                        "text": "# Intro\n\nhello world",
                        "doc_type": "pdf",
                        "token_estimate": 8,
                        "page_no": 1,
                        "page_span": [1, 1],
                        "section_path": ["page-1", "layout-1"],
                        "metadata": {},
                    }
                ],
                "assets": [
                    {
                        "asset_id": "ocr#markdown",
                        "chunk_type": "document_markdown_chunk",
                        "mime_type": "text/markdown",
                        "payload": "# Intro\n\nhello world",
                        "metadata": {"relative_path": "document.md"},
                    }
                ],
                "metadata": {},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    fallback_source = S3ObjectRef(tenant_id="fallback", bucket="bucket", key="docs/scan.pdf", version_id="v1")
    manifest = load_pipeline_manifest(manifest_path, fallback_source=fallback_source)
    markdown = select_document_markdown(manifest)

    assert manifest.source.tenant_id == "lookup"
    assert manifest.chunks
    assert manifest.assets
    assert markdown.strip()


def test_run_pipeline_writes_markdown_and_embedding_outputs(tmp_path: Path, monkeypatch) -> None:
    manifest = ChunkManifest(
        source=S3ObjectRef(tenant_id="lookup", bucket="bucket", key="docs/scan.pdf", version_id="v1"),
        doc_type="pdf",
        chunks=[
            ExtractedChunk(
                chunk_id="chunk#000001",
                chunk_type="page_text_chunk",
                text="# Title\n\nhello world",
                doc_type="pdf",
                token_estimate=8,
                page_no=1,
                page_span=(1, 1),
                section_path=("page-1", "layout-1"),
                metadata={},
            )
        ],
        assets=[],
        metadata={},
    )

    markdown_output = tmp_path / "merged_markdown.md"
    embedding_output = tmp_path / "embedding_result.json"
    split_output_dir = tmp_path / "split_parts"
    args = type(
        "Args",
        (),
        {
            "input_json": REPO_ROOT / "workflow_reference_only" / "manifest.json",
            "tenant_id": "lookup",
            "source_bucket": "bucket",
            "source_key": "docs/scan.pdf",
            "version_id": "v1",
            "gemini_api_key": "local-key",
            "gemini_api_base_url": "https://example.invalid/",
            "gemini_model": "gemini-embedding-2-preview",
            "gemini_http_timeout_seconds": 120,
            "safe_text_tokens": 1400,
            "output_dimensionality": 3072,
            "markdown_output": markdown_output,
            "embedding_output": embedding_output,
            "split_output_dir": split_output_dir,
        },
    )()

    monkeypatch.setattr("examples.workflows.workflow_reference_only.gemini_local_pipeline.load_pipeline_manifest", lambda *_, **__: manifest)
    monkeypatch.setattr(
        "examples.workflows.workflow_reference_only.gemini_local_pipeline.embed_markdown_parts",
        lambda parts, **_: (
            None,
            [
                {
                    "chunk_id": "local#part000001",
                    "token_estimate": 8,
                    "text": parts[0],
                    "vector_length": 3,
                    "vector": [0.1, 0.2, 0.3],
                }
            ],
        ),
    )

    result = run_pipeline(args)

    assert markdown_output.read_text(encoding="utf-8") == "# Title\n\nhello world"
    assert (split_output_dir / "part-0001.md").read_text(encoding="utf-8") == "# Title\n\nhello world"
    assert json.loads((split_output_dir / "part-0001.embedding.json").read_text(encoding="utf-8"))["vector_length"] == 3
    saved_result = json.loads(embedding_output.read_text(encoding="utf-8"))
    assert saved_result["merged_markdown"]["text"] == "# Title\n\nhello world"
    assert saved_result["split_parts"][0]["vector_length"] == 3
    assert result["output_paths"]["markdown_output"] == str(markdown_output)
    assert result["output_paths"]["embedding_output"] == str(embedding_output)
    assert result["output_paths"]["split_output_dir"] == str(split_output_dir)
    assert result["output_paths"]["split_embedding_files"][0] == str(split_output_dir / "part-0001.embedding.json")
