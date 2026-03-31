"""
EN: Commit extraction state after manifest persistence.
CN: 在 manifest 持久化后提交提取状态。
"""
from __future__ import annotations

from serverless_mcp.domain.models import ObjectStateRecord, S3ObjectRef
from serverless_mcp.storage.state.execution_state_repository import ExecutionStateRepository
from serverless_mcp.storage.state.object_state_repository import DuplicateOrStaleEventError, ObjectStateRepository


class StaleExtractionStateError(ValueError):
    """
    EN: Raised when extraction state is stale or already complete during commit.
    CN: 当在提交过程中提取状态已过时或已完成时抛出。
    """


class ExtractionStateCommitter:
    """
    EN: Own the state commit boundary for extract result persistence.
    CN: 拥有提取结果持久化的状态提交边界。
    """

    def __init__(
        self,
        *,
        object_state_repo: ObjectStateRepository,
        execution_state_repo: ExecutionStateRepository | None = None,
    ) -> None:
        self._object_state_repo = object_state_repo
        self._execution_state_repo = execution_state_repo

    def get_state(self, *, object_pk: str) -> ObjectStateRecord | None:
        """
        EN: Read the current object state for preflight and stale recovery.
        CN: 读取当前对象状态，用于预检和过期状态恢复。
        """
        return self._object_state_repo.get_state(object_pk=object_pk)

    def commit(
        self,
        *,
        source: S3ObjectRef,
        manifest_s3_uri: str,
        current_state: ObjectStateRecord | None = None,
        embed_status: str = "PENDING",
    ) -> ObjectStateRecord:
        """
        EN: Mark extraction complete in object_state and execution_state.
        CN: 在 object_state 和 execution_state 中标记提取完成。
        """
        state = current_state if current_state is not None else self.get_state(object_pk=source.object_pk)
        if state is not None and (state.latest_version_id != source.version_id or state.extract_status != "EXTRACTING"):
            raise StaleExtractionStateError(
                f"Extraction state is stale or complete for {source.document_uri}: "
                f"version_id={state.latest_version_id}, "
                f"extract_status={state.extract_status}"
            )

        try:
            object_state = self._object_state_repo.mark_extract_done(
                source,
                manifest_s3_uri,
                embed_status=embed_status,
            )
            if self._execution_state_repo is not None:
                self._execution_state_repo.mark_extract_done(
                    source,
                    manifest_s3_uri,
                    embed_status=embed_status,
                )
        except DuplicateOrStaleEventError:
            latest_state = self.get_state(object_pk=source.object_pk) or state
            raise StaleExtractionStateError(
                f"State became stale during commit for {source.document_uri}: "
                f"version_id={latest_state.latest_version_id if latest_state else 'unknown'}"
            ) from None

        return object_state
