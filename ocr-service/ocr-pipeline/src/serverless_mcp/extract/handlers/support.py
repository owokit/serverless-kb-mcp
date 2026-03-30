"""
EN: Shared payload validation and lazy workflow assembly for extract handlers.
CN: extract handler 共享的负载校验与懒加载工作流装配。
"""
from __future__ import annotations

from functools import cached_property, lru_cache
from types import SimpleNamespace

from pydantic import BaseModel, ConfigDict, field_validator

from serverless_mcp.runtime.aws_clients import build_aws_client
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


def _build_source_repo(s3_client):
    """
    EN: Lazily construct the S3 document source used by submit and legacy workflow paths.
    CN: 懒加载构建供 submit 和旧工作流路径使用的 S3 文档源。
    """
    from serverless_mcp.extract.s3_source import S3DocumentSource

    return S3DocumentSource(s3_client=s3_client)


def _build_extraction_service(source_repo):
    """
    EN: Lazily construct the extraction service only when sync or legacy paths need it.
    CN: 仅在 sync 或旧路径需要时懒加载构建 extraction service。
    """
    from serverless_mcp.extract.application import ExtractionService

    return ExtractionService(source_repo=source_repo)


def _build_object_state_repo(table_name: str, dynamodb_client):
    """
    EN: Lazily construct the object state repository for the workflow state boundary.
    CN: 为工作流状态边界懒加载构建 object state repository。
    """
    from serverless_mcp.runtime.bootstrap import build_object_state_repo
    from serverless_mcp.runtime.config import Settings

    settings = Settings(object_state_table=table_name)
    clients = SimpleNamespace(dynamodb=dynamodb_client)
    return build_object_state_repo(settings=settings, clients=clients)


def _build_execution_state_repo(table_name: str, dynamodb_client):
    """
    EN: Lazily construct the execution state repository for Step Functions bookkeeping.
    CN: 为 Step Functions 记账懒加载构建 execution state repository。
    """
    from serverless_mcp.runtime.bootstrap import build_execution_state_repo
    from serverless_mcp.runtime.config import Settings

    settings = Settings(object_state_table="unused", execution_state_table=table_name)
    clients = SimpleNamespace(dynamodb=dynamodb_client)
    return build_execution_state_repo(settings=settings, clients=clients)


def _build_manifest_repo(*, manifest_bucket: str, manifest_prefix: str, s3_client, dynamodb_client, manifest_index_table: str):
    """
    EN: Lazily construct the manifest repository for OCR persistence.
    CN: 为 OCR 持久化懒加载构建 manifest repository。
    """
    from serverless_mcp.runtime.bootstrap import build_manifest_repo
    from serverless_mcp.runtime.config import Settings

    settings = Settings(
        object_state_table="unused",
        manifest_bucket=manifest_bucket,
        manifest_prefix=manifest_prefix,
        manifest_index_table=manifest_index_table,
    )
    clients = SimpleNamespace(s3=s3_client, dynamodb=dynamodb_client)
    return build_manifest_repo(settings=settings, clients=clients)


def _build_embed_dispatcher(queue_url: str, sqs_client):
    """
    EN: Lazily construct the embedding dispatcher used after manifest persistence.
    CN: 在 manifest 持久化后懒加载构建 embedding dispatcher。
    """
    from serverless_mcp.embed.dispatcher import EmbeddingJobDispatcher

    return EmbeddingJobDispatcher(queue_url=queue_url, sqs_client=sqs_client)


def _build_result_persister(
    *,
    extraction_service,
    object_state_repo,
    manifest_repo,
    embed_dispatcher,
    embedding_profiles,
    execution_state_repo,
):
    """
    EN: Lazily construct the result persister only for actions that need OCR result persistence.
    CN: 仅在需要 OCR 结果持久化时懒加载构建 result persister。
    """
    from serverless_mcp.extract.pipeline import ExtractionResultPersister

    return ExtractionResultPersister(
        extraction_service=extraction_service,
        object_state_repo=object_state_repo,
        manifest_repo=manifest_repo,
        embed_dispatcher=embed_dispatcher,
        embedding_profiles=embedding_profiles,
        execution_state_repo=execution_state_repo,
    )


def _build_extract_worker(*, extraction_service, object_state_repo, result_persister, execution_state_repo):
    """
    EN: Lazily construct the extract worker used by the synchronous extract path.
    CN: 为同步 extract 路径懒加载构建 extract worker。
    """
    from serverless_mcp.extract.worker import ExtractWorker

    return ExtractWorker(
        extraction_service=extraction_service,
        object_state_repo=object_state_repo,
        result_persister=result_persister,
        execution_state_repo=execution_state_repo,
    )


def _build_ocr_client(*, token: str, base_url: str, model: str, timeout_seconds: int, status_timeout_seconds: int, allowed_hosts: tuple[str, ...]):
    """
    EN: Lazily construct the PaddleOCR async client used by submit, poll, and persist paths.
    CN: 为 submit、poll 和 persist 路径懒加载构建 PaddleOCR 异步客户端。
    """
    from serverless_mcp.ocr.paddle_async_client import PaddleOCRAsyncClient

    return PaddleOCRAsyncClient(
        token=token,
        base_url=base_url,
        model=model,
        timeout_seconds=timeout_seconds,
        status_timeout_seconds=status_timeout_seconds,
        allowed_hosts=allowed_hosts,
    )


def _build_manifest_builder():
    """
    EN: Lazily construct the OCR manifest builder used during persistence.
    CN: 为持久化流程懒加载构建 OCR manifest builder。
    """
    from serverless_mcp.ocr.paddle_manifest_builder import PaddleOCRManifestBuilder

    return PaddleOCRManifestBuilder()


class _AwsClientRegistry:
    """
    EN: Cache boto3 clients at module scope so Lambda warm starts reuse TCP connection pools.
    CN: 在模块级缓存 boto3 客户端，让 Lambda warm start 复用 TCP 连接池。
    """

    @cached_property
    def s3(self):
        return build_aws_client("s3")

    @cached_property
    def dynamodb(self):
        return build_aws_client("dynamodb")

    @cached_property
    def sqs(self):
        return build_aws_client("sqs")


class _WorkflowComponents:
    """
    EN: Lazily assemble only the repositories and clients required by each Step Functions action.
    CN: 仅为每个 Step Functions 动作懒加载装配所需的仓库和客户端。
    """

    def __init__(self, settings: Settings, clients: _AwsClientRegistry) -> None:
        self._settings = settings
        self._clients = clients

    @cached_property
    def source_repo(self):
        return _build_source_repo(self._clients.s3)

    @cached_property
    def extraction_service(self):
        return _build_extraction_service(self.source_repo)

    @cached_property
    def object_state_repo(self):
        return _build_object_state_repo(
            table_name=self._settings.object_state_table,
            dynamodb_client=self._clients.dynamodb,
        )

    @cached_property
    def execution_state_repo(self):
        if not self._settings.execution_state_table:
            raise ValueError("EXECUTION_STATE_TABLE is required for extract workflow")
        return _build_execution_state_repo(
            table_name=self._settings.execution_state_table,
            dynamodb_client=self._clients.dynamodb,
        )

    @cached_property
    def manifest_repo(self):
        if not self._settings.manifest_bucket or not self._settings.manifest_index_table:
            raise ValueError("MANIFEST_BUCKET and MANIFEST_INDEX_TABLE are required for extract workflow")
        return _build_manifest_repo(
            manifest_bucket=self._settings.manifest_bucket,
            manifest_prefix=self._settings.manifest_prefix,
            s3_client=self._clients.s3,
            dynamodb_client=self._clients.dynamodb,
            manifest_index_table=self._settings.manifest_index_table,
        )

    @cached_property
    def embed_dispatcher(self):
        if not self._settings.embed_queue_url:
            raise ValueError("EMBED_QUEUE_URL is required for extract workflow")
        return _build_embed_dispatcher(queue_url=self._settings.embed_queue_url, sqs_client=self._clients.sqs)

    @cached_property
    def result_persister(self):
        return _build_result_persister(
            extraction_service=self.extraction_service,
            object_state_repo=self.object_state_repo,
            manifest_repo=self.manifest_repo,
            embed_dispatcher=self.embed_dispatcher,
            embedding_profiles=self._settings.embedding_profiles,
            execution_state_repo=self.execution_state_repo,
        )

    @cached_property
    def extract_worker(self):
        return _build_extract_worker(
            extraction_service=self.extraction_service,
            object_state_repo=self.object_state_repo,
            result_persister=self.result_persister,
            execution_state_repo=self.execution_state_repo,
        )

    @cached_property
    def ocr_client(self):
        if not self._settings.paddle_api_token:
            raise ValueError("PADDLE_OCR_API_TOKEN is required for extract workflow")
        return _build_ocr_client(
            token=self._settings.paddle_api_token,
            base_url=self._settings.paddle_api_base_url,
            model=self._settings.paddle_ocr_model,
            timeout_seconds=self._settings.paddle_http_timeout_seconds,
            status_timeout_seconds=self._settings.paddle_status_timeout_seconds,
            allowed_hosts=self._settings.paddle_allowed_hosts,
        )

    @cached_property
    def manifest_builder(self):
        return _build_manifest_builder()

    def workflow_for(self, action: str | None):
        """
        EN: Build an action-scoped workflow so each Lambda step pays only for the dependencies it uses.
        CN: 构建动作级工作流，让每个 Lambda 只为自己使用的依赖付费。
        """
        from serverless_mcp.extract.workflow import StepFunctionsExtractWorkflow

        common_kwargs = {
            "poll_interval_seconds": self._settings.paddle_poll_interval_seconds,
            "max_poll_attempts": self._settings.paddle_max_poll_attempts,
        }
        match action:
            case None:
                return StepFunctionsExtractWorkflow(
                    extract_worker=self.extract_worker,
                    result_persister=self.result_persister,
                    object_state_repo=self.object_state_repo,
                    execution_state_repo=self.execution_state_repo,
                    source_repo=self.source_repo,
                    ocr_client=self.ocr_client,
                    manifest_builder=self.manifest_builder,
                    **common_kwargs,
                )
            case "prepare_job" | "sync_extract" | "mark_failed":
                return StepFunctionsExtractWorkflow(
                    extract_worker=self.extract_worker if action == "sync_extract" else None,
                    object_state_repo=self.object_state_repo,
                    execution_state_repo=self.execution_state_repo,
                    **common_kwargs,
                )
            case "submit_ocr_job":
                return StepFunctionsExtractWorkflow(
                    source_repo=self.source_repo,
                    ocr_client=self.ocr_client,
                    **common_kwargs,
                )
            case "poll_ocr_job":
                return StepFunctionsExtractWorkflow(
                    ocr_client=self.ocr_client,
                    **common_kwargs,
                )
            case "persist_ocr_result":
                return StepFunctionsExtractWorkflow(
                    result_persister=self.result_persister,
                    execution_state_repo=self.execution_state_repo,
                    ocr_client=self.ocr_client,
                    manifest_builder=self.manifest_builder,
                    **common_kwargs,
                )
            case _:
                raise ValueError(f"Unsupported extract workflow action: {action}")


_AWS_CLIENTS = _AwsClientRegistry()


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
    return _WorkflowComponents(settings=_get_settings(), clients=_AWS_CLIENTS)


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
