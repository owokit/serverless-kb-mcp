"""
EN: Shared payload validation and lazy workflow assembly for extract handlers.
CN: extract handler 共享的负载校验与懒加载工作流装配。
"""
from __future__ import annotations

from functools import cached_property, lru_cache

from pydantic import BaseModel, ConfigDict, field_validator

from serverless_mcp.extract.application import ExtractionService
from serverless_mcp.extract.result_persister import ExtractionResultPersister
from serverless_mcp.extract.state_commit import ExtractionStateCommitter
from serverless_mcp.extract.s3_source import S3DocumentSource
from serverless_mcp.extract.worker import ExtractWorker
from serverless_mcp.ocr.paddle_async_client import PaddleOCRAsyncClient
from serverless_mcp.ocr.paddle_manifest_builder import PaddleOCRManifestBuilder
from serverless_mcp.runtime.bootstrap import RuntimeContext, build_runtime_context, build_runtime_repositories
from serverless_mcp.runtime.aws_clients import AwsClientBundle
from serverless_mcp.runtime.config import Settings
from serverless_mcp.domain.models import ExtractJobMessage, ObjectStateRecord, S3ObjectRef


class _S3ObjectRefPayload(BaseModel):
    """
    EN: Validate the S3 object identity payload before building immutable domain models.
    CN: 在构建不可变领域模型前校验 S3 对象标识负载。
    """

    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    bucket: str
    key: str
    version_id: str
    sequencer: str | None = None
    etag: str | None = None
    content_type: str | None = None
    security_scope: tuple[str, ...] = ()
    language: str = "zh"

    @field_validator("security_scope", mode="before")
    @classmethod
    def _normalize_security_scope(cls, value: object) -> object:
        """
        EN: Accept Step Functions list payloads while preserving tuple semantics in the domain layer.
        CN: 接受 Step Functions 列表负载，同时保留领域层的元组语义。
        """
        if isinstance(value, list):
            return tuple(value)
        return value

    def to_domain(self) -> S3ObjectRef:
        """
        EN: Convert the validated payload into the immutable S3ObjectRef model.
        CN: 将校验后的负载转换为不可变的 S3ObjectRef 模型。
        """
        return S3ObjectRef(**self.model_dump())


class _ExtractJobPayload(BaseModel):
    """
    EN: Validate Step Functions job payloads at the handler boundary.
    CN: 在 handler 边界校验 Step Functions 作业负载。
    """

    model_config = ConfigDict(extra="forbid")

    source: _S3ObjectRefPayload
    trace_id: str
    operation: str = "UPSERT"
    requested_at: str | None = None

    def to_domain(self) -> ExtractJobMessage:
        """
        EN: Convert the validated payload into the ExtractJobMessage domain model.
        CN: 将校验后的负载转换为 ExtractJobMessage 领域模型。
        """
        return ExtractJobMessage(
            source=self.source.to_domain(),
            trace_id=self.trace_id,
            operation=self.operation,
            requested_at=self.requested_at,
        )


class _ObjectStatePayload(BaseModel):
    """
    EN: Validate object_state payloads so stale or malformed Step Functions state fails fast.
    CN: 校验 object_state 负载，让过期或错误的 Step Functions 状态尽快失败。
    """

    model_config = ConfigDict(extra="forbid")

    pk: str
    latest_version_id: str
    latest_sequencer: str | None
    extract_status: str
    embed_status: str
    previous_version_id: str | None = None
    previous_manifest_s3_uri: str | None = None
    latest_manifest_s3_uri: str | None = None
    is_deleted: bool = False
    last_error: str | None = None
    updated_at: str | None = None

    def to_domain(self) -> ObjectStateRecord:
        """
        EN: Convert the validated payload into the ObjectStateRecord domain model.
        CN: 将校验后的负载转换为 ObjectStateRecord 领域模型。
        """
        payload = self.model_dump(exclude_none=True)
        return ObjectStateRecord(**payload)


class _WorkflowComponents:
    """
    EN: Lazily assemble only the repositories and clients required by each Step Functions action.
    CN: 仅为每个 Step Functions 动作懒加载装配所需的仓库和客户端。
    """

    def __init__(self, settings: Settings, clients: AwsClientBundle | None = None) -> None:
        runtime_context: RuntimeContext = build_runtime_context(settings=settings, clients=clients)
        self._settings = runtime_context.settings
        self._clients = runtime_context.clients

    @cached_property
    def runtime_repositories(self):
        return build_runtime_repositories(settings=self._settings, clients=self._clients)

    @cached_property
    def source_repo(self):
        return S3DocumentSource(s3_client=self._clients.s3)

    @cached_property
    def extraction_service(self):
        return ExtractionService(source_repo=self.source_repo)

    @cached_property
    def object_state_repo(self):
        return self.runtime_repositories.object_state_repo

    @cached_property
    def execution_state_repo(self):
        repositories = self.runtime_repositories
        if repositories.execution_state_repo is None:
            raise ValueError("EXECUTION_STATE_TABLE is required for extract workflow")
        return repositories.execution_state_repo

    @cached_property
    def manifest_repo(self):
        repositories = self.runtime_repositories
        if repositories.manifest_repo is None:
            raise ValueError("MANIFEST_BUCKET and MANIFEST_INDEX_TABLE are required for extract workflow")
        return repositories.manifest_repo

    @cached_property
    def state_committer(self):
        return ExtractionStateCommitter(
            object_state_repo=self.object_state_repo,
            execution_state_repo=self.execution_state_repo,
        )

    @cached_property
    def result_persister(self):
        return ExtractionResultPersister(
            extraction_service=self.extraction_service,
            state_committer=self.state_committer,
            manifest_repo=self.manifest_repo,
            embed_dispatcher=self.embed_dispatcher,
            embedding_profiles=self._settings.embedding_profiles,
        )

    @cached_property
    def embed_dispatcher(self):
        if not self._settings.embed_queue_url:
            raise ValueError("EMBED_QUEUE_URL is required for extract workflow")
        from serverless_mcp.embed.dispatcher import EmbeddingJobDispatcher

        return EmbeddingJobDispatcher(queue_url=self._settings.embed_queue_url, sqs_client=self._clients.sqs)

    @cached_property
    def extract_worker(self):
        return ExtractWorker(
            extraction_service=self.extraction_service,
            object_state_repo=self.object_state_repo,
            result_persister=self.result_persister,
            execution_state_repo=self.execution_state_repo,
        )

    @cached_property
    def ocr_client(self):
        if not self._settings.paddle_api_token:
            raise ValueError("PADDLE_OCR_API_TOKEN is required for extract workflow")
        return PaddleOCRAsyncClient(
            token=self._settings.paddle_api_token,
            base_url=self._settings.paddle_api_base_url,
            model=self._settings.paddle_ocr_model,
            timeout_seconds=self._settings.paddle_http_timeout_seconds,
            status_timeout_seconds=self._settings.paddle_status_timeout_seconds,
            allowed_hosts=self._settings.paddle_allowed_hosts,
        )

    @cached_property
    def manifest_builder(self):
        return PaddleOCRManifestBuilder()

    def workflow_for(self, action: str | None):
        """
        EN: Build the dedicated action object for one Step Functions step.
        CN: 为单个 Step Functions 步骤构建专用 action 对象。
        """
        from serverless_mcp.extract.actions import (
            MarkFailedAction,
            PersistOcrResultAction,
            PollOcrJobAction,
            PrepareJobAction,
            SyncExtractAction,
            SubmitOcrJobAction,
        )
        match action:
            case None:
                raise ValueError("action is required for extract workflow")
            case "prepare_job":
                return PrepareJobAction(
                    execution_state_repo=self.execution_state_repo,
                    poll_interval_seconds=self._settings.paddle_poll_interval_seconds,
                    max_poll_attempts=self._settings.paddle_max_poll_attempts,
                )
            case "sync_extract":
                return SyncExtractAction(extract_worker=self.extract_worker)
            case "submit_ocr_job":
                return SubmitOcrJobAction(source_repo=self.source_repo, ocr_client=self.ocr_client)
            case "poll_ocr_job":
                return PollOcrJobAction(ocr_client=self.ocr_client)
            case "persist_ocr_result":
                return PersistOcrResultAction(
                    result_persister=self.result_persister,
                    ocr_client=self.ocr_client,
                    manifest_builder=self.manifest_builder,
                )
            case "mark_failed":
                return MarkFailedAction(execution_state_repo=self.execution_state_repo)
            case _:
                raise ValueError(f"Unsupported extract workflow action: {action}")


@lru_cache(maxsize=1)
def _get_settings() -> Settings:
    """
    EN: Cache environment settings once per execution environment.
    CN: 在每个执行环境中缓存一次环境配置。
    """
    return Settings.from_env()


@lru_cache(maxsize=1)
def _get_components() -> _WorkflowComponents:
    """
    EN: Cache workflow components so repositories and clients survive Lambda warm starts.
    CN: 缓存工作流组件，让仓库和客户端跨 Lambda warm start 复用。
    """
    return _WorkflowComponents(settings=_get_settings())


def validate_job(payload: object, *, required_for: str) -> ExtractJobMessage:
    """
    EN: Validate and convert a Step Functions job payload, surfacing schema drift as a clear boundary error.
    CN: 校验并转换 Step Functions 作业负载，将 schema 漂移暴露为清晰的边界错误。
    """
    if payload is None:
        raise ValueError(f"job is required for {required_for}")
    return _ExtractJobPayload.model_validate(payload).to_domain()


def validate_processing_state(payload: object, *, required_for: str) -> ObjectStateRecord:
    """
    EN: Validate and convert object_state payloads before workflow execution.
    CN: 在工作流执行前校验并转换 object_state 负载。
    """
    if payload is None:
        raise ValueError(f"processing_state is required for {required_for}")
    return _ObjectStatePayload.model_validate(payload).to_domain()


def parse_error_message(event: dict) -> str:
    """
    EN: Extract error message from Step Functions error payload, truncated to 1000 chars.
    CN: 从 Step Functions 错误负载中提取错误信息，并截断到 1000 个字符。
    """
    error = event.get("error")
    cause = event.get("cause")
    if isinstance(error, str) and isinstance(cause, str):
        return f"{error}: {cause}"[:1000]
    if isinstance(error, str):
        return error[:1000]
    if isinstance(cause, str):
        return cause[:1000]
    return "extract workflow failed"
