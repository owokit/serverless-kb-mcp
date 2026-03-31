"""
Local PaddleOCR contract check.

This script submits one PDF to PaddleOCR, polls until the job completes, then
prints and verifies both result URLs:

- jsonUrl -> downloaded as JSONL
- markdownUrl -> downloaded as Markdown

It intentionally lives under tests/integration but is not named test_*.py so it
does not run in the default pytest suite.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from time import sleep, monotonic
from typing import Any
from urllib.parse import urlparse

import requests


DEFAULT_PDF_NAME = "Attention Is All You Need.pdf"
DEFAULT_JOB_URL = "https://paddleocr.aistudio-app.com/api/v2/ocr/jobs"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _candidate_env_files() -> list[Path]:
    root = _repo_root()
    return [
        root / ".env",
        root / "ocr-service" / "ocr-pipeline" / ".env",
        Path.cwd() / ".env",
    ]


def _load_env_file(path: Path) -> None:
    if not path.exists() or not path.is_file():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        os.environ.setdefault(key, value)


def load_env() -> None:
    for path in _candidate_env_files():
        _load_env_file(path)


def require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise SystemExit(f"Missing required env var: {name}")
    return value.strip()


def normalize_optional(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def submit_job(*, session: requests.Session, job_url: str, token: str, pdf_path: Path, model: str) -> str:
    headers = {"Authorization": f"bearer {token}"}
    with pdf_path.open("rb") as fh:
        response = session.post(
            job_url,
            headers=headers,
            data={
                "model": model,
                "optionalPayload": json.dumps(
                    {
                        "useDocOrientationClassify": False,
                        "useDocUnwarping": False,
                        "useChartRecognition": False,
                    },
                    ensure_ascii=False,
                ),
            },
            files={"file": (pdf_path.name, fh, "application/pdf")},
            timeout=120,
        )
    response.raise_for_status()
    payload = response.json()
    job_id = payload.get("data", {}).get("jobId")
    if not isinstance(job_id, str) or not job_id.strip():
        raise SystemExit(f"Submit response missing data.jobId: {payload}")
    return job_id


def poll_job(*, session: requests.Session, job_url: str, token: str, job_id: str, timeout_seconds: int = 600) -> dict[str, Any]:
    headers = {"Authorization": f"bearer {token}"}
    start = monotonic()
    while True:
        response = session.get(f"{job_url}/{job_id}", headers=headers, timeout=60)
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data", {})
        state = data.get("state")
        if state == "done":
            return payload
        if state == "failed":
            raise SystemExit(f"Job failed: {json.dumps(payload, ensure_ascii=False, indent=2)}")
        if monotonic() - start > timeout_seconds:
            raise SystemExit(f"Timed out waiting for job {job_id}: {json.dumps(payload, ensure_ascii=False, indent=2)}")
        sleep(5)


def download_text(*, session: requests.Session, url: str, timeout_seconds: int = 120) -> str:
    response = session.get(url, timeout=timeout_seconds)
    response.raise_for_status()
    return response.text


def main() -> int:
    load_env()

    token = require_env("PADDLE_OCR_API_TOKEN")
    job_url = (normalize_optional(os.getenv("PADDLE_OCR_API_BASE_URL")) or DEFAULT_JOB_URL).rstrip("/")
    model = normalize_optional(os.getenv("PADDLE_OCR_MODEL")) or "PaddleOCR-VL-1.5"
    pdf_name = normalize_optional(os.getenv("PADDLE_OCR_TEST_PDF_NAME")) or DEFAULT_PDF_NAME
    pdf_path = Path(
        normalize_optional(os.getenv("PADDLE_OCR_TEST_PDF_PATH"))
        or (_repo_root() / "ocr-service" / "ocr-pipeline" / "tests" / "pdf" / pdf_name)
    )
    if not pdf_path.exists():
        alt_candidates = [
            _repo_root() / pdf_name,
            _repo_root() / "ocr-service" / "ocr-pipeline" / pdf_name,
            _repo_root() / "ocr-service" / "ocr-pipeline" / "tests" / pdf_name,
        ]
        for alt_path in alt_candidates:
            if alt_path.exists():
                pdf_path = alt_path
                break
    if not pdf_path.exists():
        raise SystemExit(f"PDF not found: {pdf_path}")

    output_dir = Path(__file__).resolve().parent / "_artifacts" / pdf_path.stem
    output_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    job_id = submit_job(session=session, job_url=job_url, token=token, pdf_path=pdf_path, model=model)
    print(f"job_id={job_id}")

    result_payload = poll_job(session=session, job_url=job_url, token=token, job_id=job_id)
    print("status_payload=")
    print(json.dumps(result_payload, ensure_ascii=False, indent=2))

    data = result_payload.get("data", {})
    result_url = data.get("resultUrl") if isinstance(data, dict) else {}
    json_url = result_url.get("jsonUrl") if isinstance(result_url, dict) else None
    markdown_url = result_url.get("markdownUrl") if isinstance(result_url, dict) else None

    print(f"json_url={json_url}")
    print(f"markdown_url={markdown_url}")

    missing: list[str] = []
    if not isinstance(json_url, str) or not json_url.strip():
        missing.append("jsonUrl")
    if not isinstance(markdown_url, str) or not markdown_url.strip():
        missing.append("markdownUrl")

    if missing:
        print(f"missing_result_urls={','.join(missing)}", file=sys.stderr)
        return 2

    json_text = download_text(session=session, url=json_url)
    markdown_text = download_text(session=session, url=markdown_url)

    (output_dir / "result.jsonl").write_text(json_text, encoding="utf-8")
    (output_dir / "result.md").write_text(markdown_text, encoding="utf-8")

    print(f"saved_jsonl={output_dir / 'result.jsonl'}")
    print(f"saved_markdown={output_dir / 'result.md'}")
    print(f"jsonl_bytes={len(json_text.encode('utf-8'))}")
    print(f"markdown_chars={len(markdown_text)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
