"""
EN: Tests for PaddleOCRAsyncClient covering host allowlisting, timeout handling, and download normalization.
CN: 同上。
"""

from __future__ import annotations

import pytest
import requests

from serverless_mcp.ocr.paddle_async_client import (
    PaddleOCRAsyncClient,
    PaddleOCRClientError,
    PaddleOCRClientPermanentError,
)


class _FakeResponse:
    # EN: Stub HTTP response with configurable text, content, and headers.
    # CN: 同上。
    def __init__(self, text: str = "", content: bytes = b"", headers: dict | None = None) -> None:
        self.text = text
        self.content = content
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        return None


class _FakeSession:
    # EN: Stub HTTP session returning canned responses.
    # CN: 同上。
    def __init__(self) -> None:
        self.get_calls: list[tuple[str, int | None]] = []

    def get(self, url, headers=None, timeout=None):
        self.get_calls.append((url, timeout))
        if url.endswith(".jsonl"):
            return _FakeResponse(text='{"ok": true}\n')
        return _FakeResponse(content=b"binary", headers={"Content-Type": "image/png"})


class _TimeoutSession:
    # EN: Stub HTTP session that raises Timeout on every request.
    # CN: 同上。
    def __init__(self) -> None:
        self.get_calls: list[tuple[str, int | None]] = []

    def get(self, url, headers=None, timeout=None):
        self.get_calls.append((url, timeout))
        raise requests.Timeout("status poll timed out")


class _DownloadTimeoutSession:
    # EN: Stub HTTP session that raises Timeout on download requests.
    # CN: 涓嬭浇璇锋眰鏃舵姏鍑?Timeout 鐨?HTTP session 妗┿€?
    def get(self, url, headers=None, timeout=None):
        raise requests.Timeout("download timed out")


class _InvalidUrlSession:
    # EN: Stub HTTP session that raises InvalidURL for permanent configuration failure coverage.
    # CN: 同上。
    def get(self, url, headers=None, timeout=None):
        raise requests.exceptions.InvalidURL("bad base url")


def test_paddle_async_client_rejects_non_allowlisted_download_host() -> None:
    """
    EN: Paddle async client rejects non allowlisted download host.
    CN: 同上。
    """
    client = PaddleOCRAsyncClient(
        token="token",
        base_url="https://paddleocr.aistudio-app.com/api/v2/ocr/jobs",
        allowed_hosts=("results.example.com",),
        session=_FakeSession(),
    )

    with pytest.raises(PaddleOCRClientError, match="allow-listed"):
        client.download_json_lines("https://evil.example.com/result.jsonl")


def test_paddle_async_client_accepts_https_allowlisted_download_host() -> None:
    """
    EN: Paddle async client accepts https allowlisted download host.
    CN: 同上。
    """
    client = PaddleOCRAsyncClient(
        token="token",
        base_url="https://paddleocr.aistudio-app.com/api/v2/ocr/jobs",
        allowed_hosts=("results.example.com",),
        session=_FakeSession(),
    )

    rows = client.download_json_lines("https://results.example.com/result.jsonl")

    assert rows == [{"ok": True}]


def test_paddle_async_client_accepts_wildcard_allowlisted_download_host() -> None:
    """
    EN: Paddle async client accepts wildcard allowlisted download host.
    CN: 同上。
    """
    client = PaddleOCRAsyncClient(
        token="token",
        base_url="https://paddleocr.aistudio-app.com/api/v2/ocr/jobs",
        allowed_hosts=("*.bcebos.com",),
        session=_FakeSession(),
    )

    rows = client.download_json_lines("https://pplines-online.bj.bcebos.com/result.jsonl")

    assert rows == [{"ok": True}]


def test_paddle_async_client_accepts_global_allowlisted_download_host() -> None:
    """
    EN: Paddle async client accepts global allowlisted download host.
    CN: 同上。
    """
    client = PaddleOCRAsyncClient(
        token="token",
        base_url="https://paddleocr.aistudio-app.com/api/v2/ocr/jobs",
        allowed_hosts=("*",),
        session=_FakeSession(),
    )

    content, content_type = client.download_binary("https://any.example.com/image.png")

    assert content == b"binary"
    assert content_type == "image/png"


def test_paddle_async_client_defaults_to_base_host_only() -> None:
    """
    EN: Paddle async client should default to the configured base host without a wildcard.
    CN: Paddle async client 默认只允许配置的 base host，不应带通配符。
    """
    client = PaddleOCRAsyncClient(
        token="token",
        base_url="https://paddleocr.aistudio-app.com/api/v2/ocr/jobs",
        session=_FakeSession(),
    )

    rows = client.download_json_lines("https://paddleocr.aistudio-app.com/result.jsonl")

    assert rows == [{"ok": True}]
    with pytest.raises(PaddleOCRClientError, match="allow-listed"):
        client.download_json_lines("https://results.example.com/result.jsonl")


def test_paddle_async_client_rejects_insecure_submit_base_url() -> None:
    """
    EN: Paddle async client rejects insecure submit base URLs up front.
    CN: 同上。
    """
    with pytest.raises(PaddleOCRClientPermanentError, match="HTTPS OCR submit URLs"):
        PaddleOCRAsyncClient(
            token="token",
            base_url="http://paddleocr.aistudio-app.com/api/v2/ocr/jobs",
            session=_FakeSession(),
        )


def test_paddle_async_client_uses_status_timeout_for_job_status_polling() -> None:
    """
    EN: Paddle async client uses status timeout for job status polling.
    CN: 同上。
    """
    session = _TimeoutSession()
    client = PaddleOCRAsyncClient(
        token="token",
        base_url="https://paddleocr.aistudio-app.com/api/v2/ocr/jobs",
        timeout_seconds=60,
        status_timeout_seconds=7,
        session=session,
    )

    status = client.get_job_status("job-123")

    assert session.get_calls == [("https://paddleocr.aistudio-app.com/api/v2/ocr/jobs/job-123", 7)]
    assert status.job_id == "job-123"
    assert status.state == "running"
    assert status.error_message is not None
    assert "timed out" in status.error_message


def test_paddle_async_client_surfaces_permanent_status_poll_failures() -> None:
    """
    EN: Paddle async client surfaces permanent status poll failures instead of retrying them.
    CN: 同上。
    """
    client = PaddleOCRAsyncClient(
        token="token",
        base_url="https://paddleocr.aistudio-app.com/api/v2/ocr/jobs",
        timeout_seconds=60,
        status_timeout_seconds=7,
        session=_InvalidUrlSession(),
    )

    with pytest.raises(PaddleOCRClientPermanentError, match="Permanent PaddleOCR status poll error"):
        client.get_job_status("job-123")


def test_paddle_async_client_normalizes_download_timeouts() -> None:
    """
    EN: Paddle async client normalizes download timeouts.
    CN: 同上。
    """
    client = PaddleOCRAsyncClient(
        token="token",
        base_url="https://paddleocr.aistudio-app.com/api/v2/ocr/jobs",
        allowed_hosts=("*",),
        session=_DownloadTimeoutSession(),
    )

    with pytest.raises(PaddleOCRClientError, match="PaddleOCR download failed"):
        client.download_json_lines("https://results.example.com/result.jsonl")

    with pytest.raises(PaddleOCRClientError, match="PaddleOCR download failed"):
        client.download_binary("https://results.example.com/image.png")
