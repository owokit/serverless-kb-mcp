"""
EN: Tests for Lambda packaging scripts including artifact listing, building, and uploading.
CN: 测试 Lambda 打包脚本，包括产物列表、构建和上传。
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_script_module(script_name: str):
    script_path = Path(__file__).resolve().parents[5] / "tools" / "packaging" / "serverless_mcp" / f"{script_name}.py"
    module_name = f"{script_name}_test_module"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec and spec.loader, f"unable to load {script_path}"
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_list_lambda_artifacts_renders_github_env_line(capsys, monkeypatch) -> None:
    """
    EN: List lambda artifacts renders github env line.
    CN: 验证 list_lambda_artifacts 会输出 GitHub env 行。
    """
    module = _load_script_module("list_lambda_artifacts")
    monkeypatch.setattr(sys, "argv", ["list_lambda_artifacts.py"])

    exit_code = module.main()

    captured = capsys.readouterr()
    assert exit_code == 0
    assert (
        captured.out.strip()
        == "FUNCTIONS=ingest extract_prepare extract_sync extract_submit extract_poll extract_persist extract_mark_failed embed remote_mcp backfill job_status"
    )


def test_list_layer_artifacts_renders_github_env_line(capsys, monkeypatch) -> None:
    """
    EN: List layer artifacts renders github env line.
    CN: 验证 list_layer_artifacts 会输出 GitHub env 行。
    """
    module = _load_script_module("list_layer_artifacts")
    monkeypatch.setattr(sys, "argv", ["list_layer_artifacts.py"])

    exit_code = module.main()

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.out.strip() == "LAYERS=core extract embedding"


def test_layer_dependencies_keep_pydantic_and_requests_out_of_core() -> None:
    """
    EN: Layer dependencies keep pydantic and requests out of core.
    CN: 验证 layer 依赖把 pydantic 和 requests 排除在 core 之外。
    """
    module = _load_script_module("layer_artifacts")

    assert "aws-lambda-powertools>=3.12.0" in module.LAYER_DEPENDENCIES["core"]
    assert "cryptography>=45.0.0" in module.LAYER_DEPENDENCIES["core"]
    assert "mangum>=0.19.0" in module.LAYER_DEPENDENCIES["core"]
    assert "mcp>=1.20.0" in module.LAYER_DEPENDENCIES["core"]
    assert "pydantic>=2.11.3" not in module.LAYER_DEPENDENCIES["core"]
    assert "pydantic>=2.11.3" in module.LAYER_DEPENDENCIES["extract"]
    assert "requests>=2.32.0" in module.LAYER_DEPENDENCIES["extract"]


def test_build_s3_key_uses_prefix_when_present() -> None:
    """
    EN: Build s3 key uses prefix when present.
    CN: 验证在存在 prefix 时 build_s3_key 使用前缀。
    """
    module = _load_script_module("lambda_artifacts")

    assert module.build_s3_key(
        repo_name="demo",
        function_key="ingest",
        s3_prefix="prefix",
    ) == "prefix/demo_ingest.zip"


def test_build_s3_key_uses_zip_name_without_prefix() -> None:
    """
    EN: Build s3 key uses zip name without prefix.
    CN: 验证在没有 prefix 时 build_s3_key 使用 zip 名称。
    """
    module = _load_script_module("lambda_artifacts")

    assert module.build_s3_key(
        repo_name="demo",
        function_key="ingest",
        s3_prefix="",
    ) == "demo_ingest.zip"


def test_build_lambda_artifacts_cleans_output_and_builds_selected_functions(tmp_path, capsys, monkeypatch) -> None:
    """
    EN: Build lambda artifacts cleans output and builds selected functions.
    CN: 验证 build_lambda_artifacts 会清理输出目录并构建指定函数。
    """
    module = _load_script_module("build_lambda_artifacts")
    output_dir = tmp_path / "dist"
    output_dir.mkdir()
    stale_file = output_dir / "stale.txt"
    stale_file.write_text("stale", encoding="utf-8")
    calls: list[tuple[str, str, Path]] = []

    def fake_build_lambda_package(*, function_key: str, repo_name: str, output_dir: Path) -> Path:
        calls.append((function_key, repo_name, output_dir))
        zip_path = output_dir / f"{repo_name}_{function_key}.zip"
        zip_path.write_text(function_key, encoding="utf-8")
        return zip_path

    monkeypatch.setattr(module, "build_lambda_package", fake_build_lambda_package)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_lambda_artifacts.py",
            "--repo-name",
            "serverless-ocr-s3vectors-mcp",
            "--output-dir",
            str(output_dir),
            "--functions",
            "ingest,extract_prepare",
        ],
    )

    exit_code = module.main()

    captured = capsys.readouterr()
    assert exit_code == 0
    assert not stale_file.exists()
    assert calls == [
        ("ingest", "serverless-ocr-s3vectors-mcp", output_dir),
        ("extract_prepare", "serverless-ocr-s3vectors-mcp", output_dir),
    ]
    assert "serverless-ocr-s3vectors-mcp_ingest.zip" in captured.out
    assert "serverless-ocr-s3vectors-mcp_extract_prepare.zip" in captured.out


def test_build_layer_artifacts_builds_selected_layers(tmp_path, capsys, monkeypatch) -> None:
    """
    EN: Build layer artifacts builds selected layers.
    CN: 验证 build_layer_artifacts 会构建指定 layer。
    """
    module = _load_script_module("build_layer_artifacts")
    output_dir = tmp_path / "dist"
    calls: list[tuple[str, ...]] = []

    def fake_run(*args: str) -> None:
        calls.append(args)
        if "--target" not in args:
            return

        target = Path(args[args.index("--target") + 1])
        package_root = target / "serverless_mcp"
        package_root.mkdir(parents=True, exist_ok=True)
        (package_root / "__init__.py").write_text("", encoding="utf-8")

    monkeypatch.setattr(module, "_run", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_layer_artifacts.py",
            "--repo-name",
            "serverless-ocr-s3vectors-mcp",
            "--output-dir",
            str(output_dir),
            "--layers",
            "core,extract",
        ],
    )

    exit_code = module.main()

    captured = capsys.readouterr()
    assert exit_code == 0
    assert (output_dir / "serverless-ocr-s3vectors-mcp_core_layer.zip").exists()
    assert (output_dir / "serverless-ocr-s3vectors-mcp_extract_layer.zip").exists()
    assert len(calls) == 2
    assert "serverless-ocr-s3vectors-mcp_core_layer.zip" in captured.out
    assert "serverless-ocr-s3vectors-mcp_extract_layer.zip" in captured.out


def test_upload_lambda_artifacts_uses_expected_s3_keys(tmp_path, capsys, monkeypatch) -> None:
    """
    EN: Upload lambda artifacts uses expected s3 keys.
    CN: 验证 upload_lambda_artifacts 使用预期的 S3 key。
    """
    module = _load_script_module("upload_lambda_artifacts")
    artifact_dir = tmp_path / "dist"
    artifact_dir.mkdir()
    for function_key in ("ingest", "extract_prepare"):
        (artifact_dir / f"demo_{function_key}.zip").write_text(function_key, encoding="utf-8")

    uploads: list[tuple[str, str, str]] = []

    class _FakeS3Client:
        # EN: In-memory stand-in for S3 client with versioned storage.
        # CN: 带版本化存储的 S3 客户端内存替身。
        def upload_file(self, filename: str, bucket: str, key: str) -> None:
            uploads.append((Path(filename).name, bucket, key))

    monkeypatch.setattr(module.boto3, "client", lambda service_name, region_name=None: _FakeS3Client())
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "upload_lambda_artifacts.py",
            "--repo-name",
            "demo",
            "--artifact-dir",
            str(artifact_dir),
            "--s3-bucket",
            "bucket",
            "--s3-prefix",
            "prefix",
            "--functions",
            "ingest extract_prepare",
        ],
    )

    exit_code = module.main()

    captured = capsys.readouterr()
    assert exit_code == 0
    assert uploads == [
        ("demo_ingest.zip", "bucket", "prefix/demo_ingest.zip"),
        ("demo_extract_prepare.zip", "bucket", "prefix/demo_extract_prepare.zip"),
    ]
    assert "uploaded" in captured.out
