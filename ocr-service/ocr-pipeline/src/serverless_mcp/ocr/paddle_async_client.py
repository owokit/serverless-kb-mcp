"""
EN: PaddleOCR async API client using submit -> wait -> query -> fetch pattern.
CN: PaddleOCR 异步 API 客户端，采用 submit -> wait -> query -> fetch 的调用流程。
"""
from __future__ import annotations

import json
import mimetypes
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlparse
from time import monotonic

import requests

from serverless_mcp.runtime.observability import emit_trace


class PaddleOCRClientError(RuntimeError):
    """
    EN: Raised when PaddleOCR async API returns an invalid response.
    CN: 当 PaddleOCR 异步 API 返回无效响应时抛出。
    """


class PaddleOCRClientPermanentError(PaddleOCRClientError):
    """
    EN: Raised when the PaddleOCR client is misconfigured or the server response is permanently invalid.
    CN: 当 PaddleOCR 客户端配置错误或服务端响应永久无效时抛出。
    """


@dataclass(frozen=True, slots=True)
class PaddleOCRJobSubmission:
    """
EN: OCR job submission result containing job_id for status polling.
CN: 包含用于状态轮询的 job_id 的 OCR 作业提交结果。
    """
    job_id: str


@dataclass(frozen=True, slots=True)
class PaddleOCRJobStatus:
    """
    EN: OCR job status with state, result URLs, and progress information.
    CN: 包含状态、结果地址和进度信息的 OCR 作业状态。
    """
    job_id: str
    state: str
    json_url: str | None = None
    markdown_url: str | None = None
    error_message: str | None = None
    extracted_pages: int | None = None
    total_pages: int | None = None


class PaddleOCRAsyncClient:
    """
    EN: HTTP client for PaddleOCR async API following submit -> wait -> query -> fetch workflow.
    CN: 遵循 submit -> wait -> query -> fetch 流程的 PaddleOCR 异步 API HTTP 客户端。
    """

    def __init__(
        self,
        *,
        token: str,
        base_url: str = "https://paddleocr.aistudio-app.com/api/v2/ocr/jobs",
        model: str = "PaddleOCR-VL-1.5",
        optional_payload: dict[str, Any] | None = None,
        timeout_seconds: int = 60,
        status_timeout_seconds: int | None = None,
        allowed_hosts: tuple[str, ...] = (),
        session: requests.Session | None = None,
    ) -> None:
        # EN: Initialize HTTP client with Bearer auth, optional payload defaults, and host allow-list.
        # CN: 使用 Bearer 鉴权、可选载荷默认值和主机白名单初始化 HTTP 客户端。
        self._base_url = base_url.rstrip("/")
        self._model = model
        self._optional_payload = optional_payload or {
            "useDocOrientationClassify": False,
            "useDocUnwarping": False,
            "useChartRecognition": False,
        }
        self._timeout_seconds = timeout_seconds
        self._status_timeout_seconds = status_timeout_seconds if status_timeout_seconds is not None else timeout_seconds
        self._session = session or requests.Session()
        self._headers = {"Authorization": f"bearer {token}"}
        configured_hosts = tuple(host.strip().lower() for host in allowed_hosts if host.strip())
        base_host = urlparse(self._base_url).hostname
        if base_host:
            configured_hosts = tuple(dict.fromkeys((*configured_hosts, base_host.lower())))
        self._allowed_hosts = configured_hosts
        self._validate_service_url(self._base_url)

    def submit_job(self, *, payload: bytes, key: str) -> PaddleOCRJobSubmission:
        """
        EN: Submit OCR job to PaddleOCR async API and return job_id for polling.
        CN: 向 PaddleOCR 异步 API 提交 OCR 作业，并返回用于轮询的 job_id。

        Args:
            payload:
                EN: Document bytes to process.
                CN: 要处理的文档字节内容。
            key:
                EN: S3 key used to derive filename for multipart upload.
                CN: 用于派生 multipart 上传文件名的 S3 key。

        Returns:
            EN: Job submission result with job_id.
            CN: 包含 job_id 的作业提交结果。

        Raises:
            EN: PaddleOCRClientError if submission fails or response is invalid.
            CN: 当提交失败或响应无效时抛出 PaddleOCRClientError。
        """
        start = monotonic()
        filename = PurePosixPath(key).name or "document.pdf"
        emit_trace(
            "paddleocr.submit_job.start",
            key=key,
            filename=filename,
            payload_bytes=len(payload),
        )
        response = self._session.post(
            self._base_url,
            headers=self._headers,
            data={
                "model": self._model,
                "optionalPayload": json.dumps(self._optional_payload, ensure_ascii=False),
            },
            files={"file": (filename, payload, _guess_mime_type(filename))},
            timeout=self._timeout_seconds,
        )
        body = self._json_or_raise(response)
        job_id = body.get("data", {}).get("jobId")
        if not isinstance(job_id, str) or not job_id:
            raise PaddleOCRClientError("PaddleOCR submit response missing data.jobId")
        emit_trace(
            "paddleocr.submit_job.done",
            key=key,
            filename=filename,
            job_id=job_id,
            elapsed_ms=round((monotonic() - start) * 1000, 2),
        )
        return PaddleOCRJobSubmission(job_id=job_id)

    def get_job_status(self, job_id: str) -> PaddleOCRJobStatus:
        """
        EN: Query OCR job status by job_id, returns state and result URLs when done.
        CN: 根据 job_id 查询 OCR 作业状态，完成时返回状态和结果地址。

        Args:
            job_id:
                EN: Job identifier returned from submit_job.
                CN: submit_job 返回的作业标识。

        Returns:
            EN: Job status with state, json_url, and progress information.
            CN: 包含状态、json_url 和进度信息的作业状态。

        Raises:
            EN: PaddleOCRClientError if query fails or response is invalid.
            CN: 当查询失败或响应无效时抛出 PaddleOCRClientError。
        """
        start = monotonic()
        emit_trace("paddleocr.get_job_status.start", job_id=job_id)
        try:
            response = self._session.get(
                f"{self._base_url}/{job_id}",
                headers=self._headers,
                timeout=self._status_timeout_seconds,
            )
        # EN: Return "running" state on transient network errors so Step Functions can retry.
        # CN: 遇到瞬时网络错误时返回 running，方便 Step Functions 继续重试。
        except requests.RequestException as exc:
            if _is_permanent_request_error(exc):
                raise PaddleOCRClientPermanentError(f"Permanent PaddleOCR status poll error: {exc}") from exc
            emit_trace(
                "paddleocr.get_job_status.retryable_error",
                job_id=job_id,
                timeout_seconds=self._status_timeout_seconds,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            return PaddleOCRJobStatus(
                job_id=job_id,
                state="running",
                error_message=f"Retryable PaddleOCR status poll error after {self._status_timeout_seconds}s: {exc}",
            )
        body = self._json_or_raise(response)
        data = body.get("data", {})
        state = data.get("state")
        if not isinstance(state, str) or not state:
            raise PaddleOCRClientError("PaddleOCR job status missing data.state")

        progress = data.get("extractProgress") or {}
        result_url = data.get("resultUrl") or {}
        json_url = result_url.get("jsonUrl") if isinstance(result_url, dict) else None
        markdown_url = result_url.get("markdownUrl") if isinstance(result_url, dict) else None
        error_message = data.get("errorMsg")
        emit_trace(
            "paddleocr.get_job_status.done",
            job_id=job_id,
            state=state,
            json_url=bool(json_url),
            markdown_url=bool(markdown_url),
            extracted_pages=_int_or_none(progress.get("extractedPages")),
            total_pages=_int_or_none(progress.get("totalPages")),
            elapsed_ms=round((monotonic() - start) * 1000, 2),
        )
        return PaddleOCRJobStatus(
            job_id=job_id,
            state=state,
            json_url=json_url if isinstance(json_url, str) else None,
            markdown_url=markdown_url if isinstance(markdown_url, str) else None,
            error_message=error_message if isinstance(error_message, str) else None,
            extracted_pages=_int_or_none(progress.get("extractedPages")),
            total_pages=_int_or_none(progress.get("totalPages")),
        )

    def download_json_lines(self, json_url: str) -> list[dict[str, Any]]:
        """
        EN: Download and parse JSONL result from PaddleOCR resultUrl.
        CN: 从 PaddleOCR resultUrl 下载并解析 JSONL 结果。

        Args:
            json_url:
                EN: Result URL from job status response.
                CN: 来自作业状态响应的结果 URL。

        Returns:
            EN: List of parsed JSON objects, one per line.
            CN: 按行解析得到的 JSON 对象列表。

        Raises:
            EN: PaddleOCRClientError if download fails or content is not valid JSONL.
            CN: 下载失败或内容不是有效 JSONL 时抛出 PaddleOCRClientError。
        """
        start = monotonic()
        self._validate_download_url(json_url)
        parsed = urlparse(json_url)
        emit_trace(
            "paddleocr.download_json_lines.start",
            url_host=parsed.hostname,
            url_path=parsed.path,
        )
        try:
            response = self._session.get(json_url, timeout=self._timeout_seconds)
            response.raise_for_status()
            rows: list[dict[str, Any]] = []
            for line in response.text.splitlines():
                stripped = line.strip()
                if not stripped:
                    continue
                parsed = json.loads(stripped)
                if not isinstance(parsed, dict):
                    raise PaddleOCRClientError("PaddleOCR json line is not a JSON object")
                rows.append(parsed)
        except requests.RequestException as exc:
            raise PaddleOCRClientError(f"PaddleOCR download failed: {exc}") from exc
        emit_trace(
            "paddleocr.download_json_lines.done",
            url_host=urlparse(json_url).hostname,
            json_line_count=len(rows),
            elapsed_ms=round((monotonic() - start) * 1000, 2),
        )
        return rows

    def download_markdown(self, markdown_url: str) -> str:
        """
        EN: Download the Markdown result from PaddleOCR resultUrl.
        CN: 从 PaddleOCR resultUrl 下载 Markdown 结果。

        Args:
            markdown_url:
                EN: Markdown result URL from job status response.
                CN: 来自作业状态响应的 Markdown 结果 URL。

        Returns:
            EN: Markdown document text.
            CN: Markdown 文档文本。

        Raises:
            EN: PaddleOCRClientError if download fails.
            CN: 下载失败时抛出 PaddleOCRClientError。
        """
        start = monotonic()
        self._validate_download_url(markdown_url)
        parsed = urlparse(markdown_url)
        emit_trace(
            "paddleocr.download_markdown.start",
            url_host=parsed.hostname,
            url_path=parsed.path,
        )
        try:
            response = self._session.get(markdown_url, timeout=self._timeout_seconds)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise PaddleOCRClientError(f"PaddleOCR download failed: {exc}") from exc
        emit_trace(
            "paddleocr.download_markdown.done",
            url_host=urlparse(markdown_url).hostname,
            markdown_char_count=len(response.text),
            elapsed_ms=round((monotonic() - start) * 1000, 2),
        )
        return response.text

    def download_binary(self, url: str) -> tuple[bytes, str | None]:
        """
        EN: Download binary asset from URL, typically for images embedded in OCR results.
        CN: 从 URL 下载二进制资源，通常用于 OCR 结果里嵌入的图片。

        Args:
            url:
                EN: Asset URL from OCR result.
                CN: 来自 OCR 结果的资产 URL。

        Returns:
            EN: Tuple of (binary content, content-type header).
            CN: （二进制内容、content-type 头）元组。
        """
        start = monotonic()
        self._validate_download_url(url)
        parsed = urlparse(url)
        emit_trace(
            "paddleocr.download_binary.start",
            url_host=parsed.hostname,
            url_path=parsed.path,
        )
        try:
            response = self._session.get(url, timeout=self._timeout_seconds)
            response.raise_for_status()
            content = response.content
        except requests.RequestException as exc:
            raise PaddleOCRClientError(f"PaddleOCR download failed: {exc}") from exc
        emit_trace(
            "paddleocr.download_binary.done",
            url_host=urlparse(url).hostname,
            content_length=len(content),
            content_type=response.headers.get("Content-Type"),
            elapsed_ms=round((monotonic() - start) * 1000, 2),
        )
        return content, response.headers.get("Content-Type")

    def _validate_download_url(self, url: str) -> None:
        """
        EN: Validate OCR result URLs before Lambda performs outbound requests.
        CN: 在 Lambda 发起出站请求前校验 OCR 结果 URL。
        """
        parsed = urlparse(url)
        hostname = parsed.hostname.lower() if parsed.hostname else None
        if parsed.scheme != "https":
            raise PaddleOCRClientError(f"Only HTTPS OCR result URLs are allowed: {url}")
        if not hostname:
            raise PaddleOCRClientError(f"OCR result URL is missing hostname: {url}")
        if self._allowed_hosts and not self._is_allowed_download_host(hostname):
            raise PaddleOCRClientError(f"OCR result URL host is not allow-listed: {hostname}")

    def _validate_service_url(self, url: str) -> None:
        """
        EN: Validate the submission endpoint before any OCR job is sent.
        CN: 在发送任何 OCR 作业前校验提交端点。
        """
        parsed = urlparse(url)
        hostname = parsed.hostname.lower() if parsed.hostname else None
        if parsed.scheme != "https":
            raise PaddleOCRClientPermanentError(f"Only HTTPS OCR submit URLs are allowed: {url}")
        if not hostname:
            raise PaddleOCRClientPermanentError(f"OCR submit URL is missing hostname: {url}")
        if self._allowed_hosts and not self._is_allowed_download_host(hostname):
            raise PaddleOCRClientPermanentError(f"OCR submit URL host is not allow-listed: {hostname}")

    def _is_allowed_download_host(self, hostname: str) -> bool:
        """
        EN: Match a download host against exact hosts, suffix wildcards, or a global wildcard.
        CN: 将下载主机与精确主机名、后缀通配符或全局通配符进行匹配。
        """
        for allowed_host in self._allowed_hosts:
            if allowed_host == "*":
                return True
            if allowed_host.startswith("*."):
                suffix = allowed_host[2:]
                if hostname.endswith(f".{suffix}") and hostname != suffix:
                    return True
                continue
            if hostname == allowed_host:
                return True
        return False

    def _json_or_raise(self, response: requests.Response) -> dict[str, Any]:
        """
        EN: Validate HTTP response and parse JSON body, raising on non-2xx or invalid JSON.
        CN: 校验 HTTP 响应并解析 JSON body，遇到非 2xx 或无效 JSON 时抛错。
        """
        try:
            response.raise_for_status()
            body = response.json()
        except requests.RequestException as exc:
            raise PaddleOCRClientError(f"PaddleOCR request failed: {exc}") from exc
        except ValueError as exc:
            raise PaddleOCRClientError("PaddleOCR response is not valid JSON") from exc
        if not isinstance(body, dict):
            raise PaddleOCRClientError("PaddleOCR response is not a JSON object")
        return body


def _guess_mime_type(filename: str) -> str:
    """
    EN: Guess MIME type from filename extension, falling back to application/octet-stream.
    CN: 根据文件扩展名猜测 MIME 类型，失败则回退为 application/octet-stream。
    """
    guessed, _ = mimetypes.guess_type(filename)
    return guessed or "application/octet-stream"


def _int_or_none(value: Any) -> int | None:
    """
    EN: Safely convert value to int, returning None on TypeError or ValueError.
    CN: 安全地将值转换为整数，遇到 TypeError 或 ValueError 时返回 None。
    """
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _is_permanent_request_error(exc: requests.RequestException) -> bool:
    """
    EN: Identify request errors that are configuration or authorization problems rather than transient failures.
    CN: 识别那些属于配置或授权问题、而不是瞬时故障的请求错误。
    """
    permanent_types = (
        requests.exceptions.InvalidURL,
        requests.exceptions.InvalidSchema,
        requests.exceptions.MissingSchema,
        requests.exceptions.InvalidHeader,
        requests.exceptions.ProxyError,
        requests.exceptions.SSLError,
    )
    if isinstance(exc, permanent_types):
        return True
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    if isinstance(status_code, int) and status_code in {401, 403, 404, 405, 422}:
        return True
    return False
