"""
EN: Extract worker that coordinates extraction and result persistence.
CN: 协调提取和结果持久化的提取 worker。
"""
from __future__ import annotations

from serverless_mcp.extract.application import ExtractionService
from .pipeline import ExtractionResultPersister
from serverless_mcp.domain.models import ExtractJobMessage, ObjectStateRecord, ProcessingOutcome
from serverless_mcp.storage.state.execution_state_repository import ExecutionStateRepository
from serverless_mcp.storage.state.object_state_repository import ObjectStateRepository


_WORKER_FAILURE_TYPES = (KeyError, OSError, RuntimeError, TypeError, ValueError)


class ExtractWorker:
    """
    EN: Orchestrate extraction from S3 source and persist manifest with state progression.
CN: 编排从 S3 源的提取，并持久化 manifest 与状态推进。
    """

    def __init__(
        self,
        *,
        extraction_service: ExtractionService,
        object_state_repo: ObjectStateRepository,
        result_persister: ExtractionResultPersister,
        execution_state_repo: ExecutionStateRepository | None = None,
    ) -> None:
        self._extraction_service = extraction_service
        self._object_state_repo = object_state_repo
        self._execution_state_repo = execution_state_repo
        self._result_persister = result_persister

    def process(
        self,
        job: ExtractJobMessage,
        *,
        processing_state: ObjectStateRecord | None = None,
    ) -> ProcessingOutcome:
        """
        EN: Execute extraction workflow and persist results with version progression.
        CN: 同上。

        Args:
            job:
                EN: Extract job message containing source reference and trace_id.
CN: 包含来源引用和 trace_id 的提取作业消息。
            processing_state:
                EN: Optional pre-loaded object state for idempotency enforcement.
CN: 可选的预加载对象状态，用于幂等约束。

        Returns:
            EN: Processing outcome with manifest URI and chunk counts.
CN: 包含 manifest URI 和 chunk 计数的处理结果。

        Raises:
            EN: Exception if extraction or persistence fails, marks object_state as failed.
CN: 当提取或持久化失败时抛出异常，并将 object_state 标记为失败。
        """
        if processing_state is not None:
            current_state = processing_state
        elif self._execution_state_repo is not None:
            current_state = self._execution_state_repo.start_processing(job.source)
        else:
            current_state = self._object_state_repo.start_processing(job.source)
        try:
            manifest = self._extraction_service.extract_from_s3(job.source)
            return self._result_persister.persist(
                source=job.source,
                manifest=manifest,
                trace_id=job.trace_id,
                previous_version_id=current_state.previous_version_id,
                previous_manifest_s3_uri=current_state.previous_manifest_s3_uri,
            )
        except _WORKER_FAILURE_TYPES as exc:
            # EN: Preserve the original exception even if failure bookkeeping also fails.
            # CN: 即使失败记录本身也出错，仍保留原始异常。
            try:
                if self._execution_state_repo is not None:
                    self._execution_state_repo.mark_failed(job.source, str(exc))
                else:
                    self._object_state_repo.mark_failed(job.source, str(exc))
            except Exception as mark_exc:  # noqa: BLE001
                # EN: Attach the bookkeeping failure for diagnostics and keep the root cause visible.
                # CN: 附加失败记录写入异常用于诊断，同时保留根因可见。
                exc.add_note(f"mark_failed also failed: {mark_exc!r}")
            raise
