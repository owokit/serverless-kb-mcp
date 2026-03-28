"""
EN: Tests for PaddleOCRManifestBuilder including layout markdown splitting and oversized chunk recursion.
CN: 娴嬭瘯 PaddleOCRManifestBuilder锛屽寘鎷?layout markdown 鎷嗗垎鍜岃秴澶?chunk 閫掑綊銆?
"""

from types import SimpleNamespace

from serverless_mcp.extract import policy
from serverless_mcp.ocr.paddle_manifest_builder import PaddleOCRManifestBuilder
from serverless_mcp.ocr import paddle_manifest_builder as builder_module
from serverless_mcp.domain.models import S3ObjectRef


def test_paddle_manifest_builder_splits_layout_markdown_into_multiple_assets() -> None:
    """
    EN: Paddle manifest builder splits layout markdown into multiple assets.
    CN: 同上。
    """
    builder = PaddleOCRManifestBuilder()
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/scan.pdf", version_id="v1")

    manifest = builder.build_manifest(
        source=source,
        json_lines=[
            {
                "result": {
                    "layoutParsingResults": [
                        {
                            "markdown": {
                                "text": "# Page 1 A\nhello world\n\n![inline](https://example.com/crop-1.png)",
                                "images": {"crop-1.png": "https://example.com/crop-1.png"},
                            },
                            "outputImages": {"rendered-a.jpg": "https://example.com/rendered-a.jpg"},
                        },
                        {
                            "markdown": {
                                "text": "## Page 1 B\nsecond block\n\n![inline-2](https://example.com/crop-2.png)",
                                "images": {"crop-2.png": "https://example.com/crop-2.png"},
                            },
                            "outputImages": {},
                        },
                    ]
                }
            }
        ],
        binary_loader=lambda url: (f"binary:{url}".encode("utf-8"), "image/png"),
    )

    assert manifest.doc_type == "pdf"
    assert len(manifest.chunks) == 2
    assert [chunk.section_path for chunk in manifest.chunks] == [("page-1", "layout-1"), ("page-1", "layout-2")]
    assert manifest.chunks[0].text.startswith("# Page 1 A")
    assert "assets/asset-000001.png" in manifest.chunks[0].text
    assert "assets/asset-000004.png" in manifest.chunks[1].text
    assert all(chunk.metadata["source_format"] == "paddleocr_async" for chunk in manifest.chunks)

    assert len(manifest.assets) == 7
    assert manifest.metadata["raw_json_asset_count"] == 1
    assert manifest.metadata["layout_markdown_asset_count"] == 2
    assert manifest.metadata["document_markdown_asset_count"] == 1
    assert manifest.metadata["markdown_asset_count"] == 3

    assets_by_path = {asset.metadata["relative_path"]: asset for asset in manifest.assets}
    raw_json_asset = assets_by_path["raw.jsonl"]
    layout_md_asset_1 = assets_by_path["pages/page-000001-layout-001.md"]
    layout_md_asset_2 = assets_by_path["pages/page-000001-layout-002.md"]
    document_md_asset = assets_by_path["document.md"]

    assert raw_json_asset.chunk_type == "ocr_json_chunk"
    assert raw_json_asset.payload.startswith(b"{")
    assert assets_by_path["assets/asset-000002.png"].chunk_type == "page_image_chunk"
    assert layout_md_asset_1.chunk_type == "document_markdown_chunk"
    assert layout_md_asset_1.mime_type == "text/markdown"
    assert layout_md_asset_1.payload == b"# Page 1 A\nhello world\n\n![inline](assets/asset-000001.png)"
    assert layout_md_asset_2.chunk_type == "document_markdown_chunk"
    assert layout_md_asset_2.payload == b"## Page 1 B\nsecond block\n\n![inline-2](assets/asset-000004.png)"
    assert document_md_asset.chunk_type == "document_markdown_chunk"
    assert document_md_asset.mime_type == "text/markdown"
    assert document_md_asset.payload == (
        b"<!-- page:1 layout:1 -->\n\n# Page 1 A\nhello world\n\n![inline](assets/asset-000001.png)"
        b"\n\n---\n\n"
        b"<!-- page:1 layout:2 -->\n\n## Page 1 B\nsecond block\n\n![inline-2](assets/asset-000004.png)"
    )


def test_paddle_manifest_builder_recursively_splits_oversized_layout_markdown(monkeypatch) -> None:
    """
    EN: Paddle manifest builder recursively splits oversized layout markdown.
    CN: 楠岃瘉 PaddleOCR manifest builder 閫掑綊鎷嗗垎瓒呭ぇ layout markdown銆?
    """
    class _FakeEncoder:
        # EN: Stub tokenizer that treats each character as one token.
        # CN: 同上。
        def encode(self, text: str) -> list[str]:
            return list(text)

        def decode(self, tokens: list[str]) -> str:
            return "".join(tokens)

    monkeypatch.setattr(policy, "_get_token_encoder", lambda: _FakeEncoder())
    monkeypatch.setattr(builder_module, "DEFAULT_POLICY", SimpleNamespace(safe_text_tokens=40))

    builder = PaddleOCRManifestBuilder()
    source = S3ObjectRef(tenant_id="tenant-a", bucket="bucket-a", key="docs/scan.pdf", version_id="v1")

    manifest = builder.build_manifest(
        source=source,
        json_lines=[
            {
                "result": {
                    "layoutParsingResults": [
                        {
                            "markdown": {
                                "text": (
                                    "# Intro\n\n"
                                    "Paragraph one.\n\n"
                                    "```python\n"
                                    "print('a')\n"
                                    "print('b')\n"
                                    "```\n\n"
                                    "## Next\n\n"
                                    "Another paragraph."
                                ),
                                "images": {},
                            },
                            "outputImages": {},
                        }
                    ]
                }
            }
        ],
        binary_loader=lambda url: (f"binary:{url}".encode("utf-8"), "image/png"),
    )

    assert len(manifest.chunks) > 1
    assert all(chunk.token_estimate <= 40 for chunk in manifest.chunks)
    assert manifest.chunks[0].text.startswith("# Intro")
    assert any("```python" in chunk.text for chunk in manifest.chunks)
    assert any(chunk.text.startswith("## Next") for chunk in manifest.chunks)

