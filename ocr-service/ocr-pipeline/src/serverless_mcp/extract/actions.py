"""
EN: Action-scoped extract services for Step Functions orchestration.
CN: 面向 Step Functions 编排的动作级 extract 服务。
"""
from __future__ import annotations

from dataclasses import asdict
from time import monotonic
from urllib.parse import urlparse

from serverless_mcp.domain.models import ExtractJobMessage, ObjectStateRecord
from serverless_mcp.extract.contracts import ExtractFailureDetails
from serverless_mcp.extract.pipeline import ExtractionResultPersister
from serverless_mcp.extract.s3_source import S3DocumentSource
from serverless_mcp.ocr.paddle_async_client import PaddleOCRAsyncClient
from serverless_mcp.ocr.paddle_manifest_builder import PaddleOCRManifestBuilder
from serverless_mcp.runtime.observability import emit_metric, emit_trace
from serverless_mcp.storage.state.execution_state_repository import ExecutionStateRepository
from serverless_mcp.extract.worker import ExtractWorker


_MAX_OCR_POLL_BUDGET_SECONDS = 600


class PrepareJobAction:
    """
    EN: Prepare an extract job by promoting queued execution state.
    CN: 通过提升 queued execution state 来准备 extract 作业。
    """

    def __init__(self, *, execution_state_repo: ExecutionStateRepository, poll_interval_seconds: int, max_poll_attempts: int) -> None:
        self._execution_state_repo = execution_state_repo
        self._poll_interval_seconds = poll_interval_seconds
        self._max_poll_attempts = max_poll_attempts

    def prepare_job(self, *, job: ExtractJobMessage, processing_state: ObjectStateRecord | None = None) -> dict:
        start = monotonic()
        emit_trace(
            "prepare_job.start",
            document_uri=job.source.document_uri,
            trace_id=job.trace_id,
            has_processing_state=processing_state is not None,
        )
        state = processing_state
        if state is None:
            state = self._execution_state_repo.start_processing(job.source)
        else:
            state = self._execution_state_repo.activate_ingest_state(job.source, processing_state)
        result = {
            "job": asdict(job),
            "processing_state": asdict(state),
            "document_extension": job.source.extension,
            "poll_interval_seconds": self._poll_interval_seconds,
            "max_poll_attempts": self._resolve_max_poll_attempts(),
            "poll_attempt": 0,
        }
        emit_trace(
            "prepare_job.done",
            document_uri=job.source.document_uri,
            trace_id=job.trace_id,
            object_pk=state.pk,
            extract_status=state.extract_status,
            elapsed_ms=round((monotonic() - start) * 1000, 2),
        )
        return result

    def _resolve_max_poll_attempts(self) -> int:
        budget_limited_attempts = max(1, _MAX_OCR_POLL_BUDGET_SECONDS // max(1, self._poll_interval_seconds))
        return min(self._max_poll_attempts, budget_limited_attempts)


class SyncExtractAction:
    """
    EN: Run the synchronous extract path for non-PDF documents.
    CN: 为非 PDF 文档运行同步 extract 路径。
    """

    def __init__(self, *, extract_worker: ExtractWorker) -> None:
        self._extract_worker = extract_worker

    def sync_extract(self, *, job: ExtractJobMessage, processing_state: ObjectStateRecord) -> dict:
        start = monotonic()
        emit_trace(
            "sync_extract.start",
            document_uri=job.source.document_uri,
            trace_id=job.trace_id,
            processing_state_pk=processing_state.pk,
        )
        outcome = self._extract_worker.process(job, processing_state=processing_state)
        result = asdict(outcome)
        emit_trace(
            "sync_extract.done",
            document_uri=job.source.document_uri,
            trace_id=job.trace_id,
            manifest_s3_uri=outcome.manifest_s3_uri,
            chunk_count=outcome.chunk_count,
            asset_count=outcome.asset_count,
            embedding_request_count=outcome.embedding_request_count,
            elapsed_ms=round((monotonic() - start) * 1000, 2),
        )
        return result


class SubmitOcrJobAction:
    """
    EN: Submit a PDF document to PaddleOCR.
    CN: 将 PDF 文档提交到 PaddleOCR。
    """

    def __init__(self, *, source_repo: S3DocumentSource, ocr_client: PaddleOCRAsyncClient) -> None:
        self._source_repo = source_repo
        self._ocr_client = ocr_client

    def submit_ocr_job(self, *, job: ExtractJobMessage) -> dict:
        start = monotonic()
        emit_trace(
            "submit_ocr_job.start",
            document_uri=job.source.document_uri,
            trace_id=job.trace_id,
            version_id=job.source.version_id,
            key=job.source.key,
        )
        fetch_start = monotonic()
        payload = self._source_repo.fetch(job.source)
        content_length = getattr(payload, "content_length", None)
        if content_length is None and hasattr(payload, "body"):
            body = getattr(payload, "body")
            content_length = len(body) if isinstance(body, (bytes, bytearray)) else None
        emit_trace(
            "submit_ocr_job.fetch_done",
            document_uri=job.source.document_uri,
            trace_id=job.trace_id,
            content_length=content_length,
            elapsed_ms=round((monotonic() - fetch_start) * 1000, 2),
        )
        submit_start = monotonic()
        submission = self._ocr_client.submit_job(payload=payload.body, key=job.source.key)
        emit_trace(
            "submit_ocr_job.submit_done",
            document_uri=job.source.document_uri,
            trace_id=job.trace_id,
            job_id=submission.job_id,
            elapsed_ms=round((monotonic() - submit_start) * 1000, 2),
            total_elapsed_ms=round((monotonic() - start) * 1000, 2),
        )
        return asdict(submission)


class PollOcrJobAction:
    """
    EN: Poll OCR job status once and increment the consumed attempt count.
    CN: 轮询 OCR 作业状态一次，并递增已消耗的尝试次数。
    """

    def __init__(self, *, ocr_client: PaddleOCRAsyncClient) -> None:
        self._ocr_client = ocr_client

    def poll_ocr_job(self, *, job_id: str, poll_attempt: int = 0, max_poll_attempts: int | None = None) -> dict:
        start = monotonic()
        emit_trace("poll_ocr_job.start", job_id=job_id, poll_attempt=poll_attempt)
        payload = asdict(self._ocr_client.get_job_status(job_id))
        payload["poll_attempt"] = poll_attempt + 1
        if max_poll_attempts is not None:
            payload["max_poll_attempts"] = max_poll_attempts
        emit_trace(
            "poll_ocr_job.done",
            job_id=job_id,
            poll_attempt=payload["poll_attempt"],
            state=payload.get("state"),
            json_url=bool(payload.get("json_url")),
            markdown_url=bool(payload.get("markdown_url")),
            elapsed_ms=round((monotonic() - start) * 1000, 2),
        )
        return payload


class PersistOcrResultAction:
    """
    EN: Download OCR outputs, build the manifest, and persist the extraction outcome.
    CN: 下载 OCR 输出、构建 manifest，并持久化提取结果。
    """

    def __init__(
        self,
        *,
        result_persister: ExtractionResultPersister,
        ocr_client: PaddleOCRAsyncClient,
        manifest_builder: PaddleOCRManifestBuilder,
    ) -> None:
        self._result_persister = result_persister
        self._ocr_client = ocr_client
        self._manifest_builder = manifest_builder

    def persist_ocr_result(
        self,
        *,
        job: ExtractJobMessage,
        processing_state: ObjectStateRecord,
        json_url: str | None = None,
        markdown_url: str,
    ) -> dict:
        if not isinstance(markdown_url, str) or not markdown_url.strip():
            raise ValueError("markdown_url is required when persisting OCR output")
        if json_url is not None and not json_url.strip():
            raise ValueError("json_url must be blank-free when provided")
        normalized_json_url = json_url.strip() if json_url is not None else None
        normalized_markdown_url = markdown_url.strip()
        start = monotonic()
        trace_payload = {
            "document_uri": job.source.document_uri,
            "trace_id": job.trace_id,
            "markdown_url_host": urlparse(normalized_markdown_url).hostname,
            "markdown_url_path": urlparse(normalized_markdown_url).path,
            "previous_version_id": processing_state.previous_version_id,
            "json_url_present": normalized_json_url is not None,
        }
        if normalized_json_url is not None:
            trace_payload.update(
                json_url_host=urlparse(normalized_json_url).hostname,
                json_url_path=urlparse(normalized_json_url).path,
            )
        emit_trace("persist_ocr_result.start", **trace_payload)
        json_lines = None
        if normalized_json_url is not None:
            download_start = monotonic()
            json_lines = self._ocr_client.download_json_lines(normalized_json_url)
            emit_trace(
                "persist_ocr_result.download_done",
                document_uri=job.source.document_uri,
                trace_id=job.trace_id,
                json_line_count=len(json_lines),
                elapsed_ms=round((monotonic() - download_start) * 1000, 2),
            )
        markdown_download_start = monotonic()
        markdown_text = self._ocr_client.download_markdown(normalized_markdown_url)
        emit_trace(
            "persist_ocr_result.markdown_download_done",
            document_uri=job.source.document_uri,
            trace_id=job.trace_id,
            markdown_char_count=len(markdown_text),
            elapsed_ms=round((monotonic() - markdown_download_start) * 1000, 2),
        )
        build_start = monotonic()
        manifest = self._manifest_builder.build_manifest_from_markdown(
            source=job.source,
            markdown_text=markdown_text,
            json_lines=json_lines,
            binary_loader=self._ocr_client.download_binary,
        )
        emit_trace(
            "persist_ocr_result.manifest_built",
            document_uri=job.source.document_uri,
            trace_id=job.trace_id,
            chunk_count=len(manifest.chunks),
            asset_count=len(manifest.assets),
            elapsed_ms=round((monotonic() - build_start) * 1000, 2),
        )
        persist_start = monotonic()
        outcome = self._result_persister.persist(
            source=job.source,
            manifest=manifest,
            trace_id=job.trace_id,
            previous_version_id=processing_state.previous_version_id,
            previous_manifest_s3_uri=processing_state.previous_manifest_s3_uri,
        )
        emit_trace(
            "persist_ocr_result.done",
            document_uri=job.source.document_uri,
            trace_id=job.trace_id,
            manifest_s3_uri=outcome.manifest_s3_uri,
            chunk_count=outcome.chunk_count,
            asset_count=outcome.asset_count,
            embedding_request_count=outcome.embedding_request_count,
            elapsed_ms=round((monotonic() - persist_start) * 1000, 2),
            total_elapsed_ms=round((monotonic() - start) * 1000, 2),
        )
        return asdict(outcome)


class MarkFailedAction:
    """
    EN: Mark an extract execution as failed.
    CN: 将 extract 执行标记为失败。
    """

    def __init__(self, *, execution_state_repo: ExecutionStateRepository) -> None:
        self._execution_state_repo = execution_state_repo

    def mark_failed(self, *, job: ExtractJobMessage, failure: ExtractFailureDetails) -> dict:
        start = monotonic()
        emit_trace(
            "mark_failed.start",
            document_uri=job.source.document_uri,
            trace_id=job.trace_id,
            error=failure.error,
            cause=failure.cause,
            error_message=failure.message,
            failure_domain=failure.domain,
        )
        emit_metric("extract.failure", action="mark_failed", failure_domain=failure.domain, error=failure.error)
        record = self._execution_state_repo.mark_extract_failed(job.source, failure.message)
        result = {
            "document_uri": job.source.document_uri,
            "error_message": failure.message,
            "failure_domain": failure.domain,
            "failure": {
                "error": failure.error,
                "cause": failure.cause,
            },
            "object_state": asdict(record),
        }
        emit_trace(
            "mark_failed.done",
            document_uri=job.source.document_uri,
            trace_id=job.trace_id,
            error=failure.error,
            cause=failure.cause,
            failure_domain=failure.domain,
            extract_status=record.extract_status,
            elapsed_ms=round((monotonic() - start) * 1000, 2),
        )
        return result
