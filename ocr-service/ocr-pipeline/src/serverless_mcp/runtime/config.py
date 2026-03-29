"""
EN: Configuration settings for the serverless_mcp service package.
CN: serverless_mcp 服务包的配置设置。
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, replace
from functools import lru_cache
from pathlib import Path

from serverless_mcp.embed.provider_urls import normalize_openai_base_url
from serverless_mcp.domain.models import EmbeddingProfile


# EN: PaddleOCR result URLs are hosted on BCE BOS regional subdomains by default.
# CN: PaddleOCR 缁撴灟URL榛樿?涓庡尯鍩熷瓙鍩熷悕 BCE BOS 銆?
DEFAULT_PADDLE_OCR_ALLOWED_HOSTS: tuple[str, ...] = ("*.bcebos.com",)
PIPELINE_CONFIG_PATH_ENV_VAR = "SERVERLESS_MCP_PIPELINE_CONFIG_PATH"


def _resolve_pipeline_config_path() -> Path | None:
    """
    EN: Resolve the explicit pipeline config path from the environment.
    CN: 从环境变量解析显式 pipeline 配置路径。
    """
    raw_path = os.environ.get(PIPELINE_CONFIG_PATH_ENV_VAR)
    if not raw_path:
        return None
    return Path(raw_path).expanduser()


@lru_cache(maxsize=1)
def _pipeline_defaults_for_path(pipeline_config_path: str | None) -> dict[str, object]:
    """
    EN: Load pipeline defaults from the explicit pipeline config path when it is set.
    CN: 在设置了显式 pipeline 配置路径时，从该路径加载默认值。
    """
    if pipeline_config_path is None:
        return {}
    pipeline_config_path = Path(pipeline_config_path)
    if not pipeline_config_path.exists():
        return {}
    payload = json.loads(pipeline_config_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Pipeline config must be a JSON object: {pipeline_config_path}")
    defaults = payload.get("defaults")
    if defaults is None:
        return {}
    if not isinstance(defaults, dict):
        raise ValueError("Pipeline config defaults must be an object")
    return defaults


def _pipeline_defaults() -> dict[str, object]:
    """
    EN: Load pipeline defaults from the explicit environment-driven path.
    CN: 从显式环境驱动路径加载 pipeline 默认值。
    """
    resolved_path = _resolve_pipeline_config_path()
    return _pipeline_defaults_for_path(str(resolved_path) if resolved_path else None)


def _pipeline_default(name: str, fallback: object) -> object:
    """
    EN: Resolve a runtime default from pipeline-config.json with a safe code fallback.
    CN: 从 pipeline-config.json 中解析运行时默认值，并在必要时使用安全的代码兜底。
    """
    value = _pipeline_defaults().get(name)
    return fallback if value is None else value


DEFAULT_MANIFEST_PREFIX = ""
DEFAULT_GEMINI_API_BASE_URL = "https://generativelanguage.googleapis.com/"
DEFAULT_GEMINI_EMBEDDING_MODEL = "gemini-embedding-2-preview"
DEFAULT_GEMINI_HTTP_TIMEOUT_SECONDS = 120
DEFAULT_OPENAI_EMBEDDING_MODEL = "text-embedding-3-small"
DEFAULT_OPENAI_HTTP_TIMEOUT_SECONDS = 120
DEFAULT_QUERY_TENANT_CLAIM = "tenant_id"
DEFAULT_REMOTE_MCP_DEFAULT_TENANT_ID = "lookup"
DEFAULT_QUERY_MAX_TOP_K = 20
DEFAULT_QUERY_MAX_NEIGHBOR_EXPAND = 2
DEFAULT_CLOUDFRONT_URL_TTL_SECONDS = 900
DEFAULT_PADDLE_API_BASE_URL = "https://paddleocr.aistudio-app.com/api/v2/ocr/jobs"
DEFAULT_PADDLE_OCR_MODEL = "PaddleOCR-VL-1.5"
DEFAULT_PADDLE_POLL_INTERVAL_SECONDS = 10
DEFAULT_PADDLE_MAX_POLL_ATTEMPTS = 180
DEFAULT_PADDLE_HTTP_TIMEOUT_SECONDS = 60
DEFAULT_PADDLE_STATUS_TIMEOUT_SECONDS = 10


# =============================================================================
# EN: Individual configuration dataclasses for focused responsibility areas.
# CN: 面向职责领域的独立配置 dataclass。
# =============================================================================


@dataclass(frozen=True, slots=True)
class AWSSettings:
    """
    EN: AWS resource names for DynamoDB tables and S3 buckets.
    CN: DynamoDB 表和 S3 bucket 的 AWS 资源名称。
    """

    object_state_table: str
    execution_state_table: str | None = None
    manifest_index_table: str | None = None
    manifest_bucket: str | None = None
    manifest_prefix: str = DEFAULT_MANIFEST_PREFIX
    embedding_projection_state_table: str | None = None
    vector_bucket_name: str | None = None
    vector_index_name: str | None = None
    step_functions_state_machine_arn: str | None = None
    embed_queue_url: str | None = None


@dataclass(frozen=True, slots=True)
class EmbeddingSettings:
    """
    EN: Embedding provider API keys, base URLs, models, and HTTP timeouts.
    CN: Embedding provider API keys、base URLs、models 和 HTTP 超时配置。
    """

    gemini_api_key: str | None = None
    gemini_api_base_url: str = DEFAULT_GEMINI_API_BASE_URL
    gemini_embedding_model: str = DEFAULT_GEMINI_EMBEDDING_MODEL
    gemini_http_timeout_seconds: int = DEFAULT_GEMINI_HTTP_TIMEOUT_SECONDS
    openai_api_key: str | None = None
    openai_api_base_url: str | None = None
    openai_embedding_model: str = DEFAULT_OPENAI_EMBEDDING_MODEL
    openai_http_timeout_seconds: int = DEFAULT_OPENAI_HTTP_TIMEOUT_SECONDS
    embedding_profiles: tuple[EmbeddingProfile, ...] = ()


@dataclass(frozen=True, slots=True)
class OCRSettings:
    """
    EN: PaddleOCR API configuration including base URL, model, polling, and timeouts.
    CN: PaddleOCR API 配置，包括 base URL、model、轮询和超时设置。
    """

    paddle_api_base_url: str = DEFAULT_PADDLE_API_BASE_URL
    paddle_api_token: str | None = None
    paddle_ocr_model: str = DEFAULT_PADDLE_OCR_MODEL
    paddle_poll_interval_seconds: int = DEFAULT_PADDLE_POLL_INTERVAL_SECONDS
    paddle_max_poll_attempts: int = DEFAULT_PADDLE_MAX_POLL_ATTEMPTS
    paddle_http_timeout_seconds: int = DEFAULT_PADDLE_HTTP_TIMEOUT_SECONDS
    paddle_status_timeout_seconds: int = DEFAULT_PADDLE_STATUS_TIMEOUT_SECONDS
    paddle_allowed_hosts: tuple[str, ...] = DEFAULT_PADDLE_OCR_ALLOWED_HOSTS


@dataclass(frozen=True, slots=True)
class SecuritySettings:
    """
    EN: Authentication, CloudFront delivery, and query access control settings.
    CN: 认证、CloudFront 分发和查询访问控制设置。
    """

    allow_unauthenticated_query: bool = False
    query_tenant_claim: str = DEFAULT_QUERY_TENANT_CLAIM
    remote_mcp_default_tenant_id: str | None = DEFAULT_REMOTE_MCP_DEFAULT_TENANT_ID
    query_max_top_k: int = DEFAULT_QUERY_MAX_TOP_K
    query_max_neighbor_expand: int = DEFAULT_QUERY_MAX_NEIGHBOR_EXPAND
    query_profile_timeout_seconds: float = 15.0
    cloudfront_distribution_domain: str | None = None
    cloudfront_key_pair_id: str | None = None
    cloudfront_private_key_pem: str | None = None
    cloudfront_private_key_secret_arn: str | None = None
    cloudfront_url_ttl_seconds: int = DEFAULT_CLOUDFRONT_URL_TTL_SECONDS


@dataclass(frozen=True, slots=True)
class RuntimeSettings:
    """
    EN: Runtime observability and service identification settings.
    CN: 运行时可观察性和服务标识设置。
    """

    metrics_namespace: str = "McpKnowledgeS3Vectors"
    service_name: str = "serverless-mcp-service"


# =============================================================================
# EN: Combined settings container for backward compatibility.
# CN: 保持向后兼容的组合 settings 容器。
# =============================================================================


@dataclass(frozen=True, slots=True)
class Settings:
    """
    EN: Immutable configuration container loaded from environment variables.
    CN: 从环境变量加载的不可变配置容器。

    This class maintains backward compatibility by aggregating all smaller settings
    classes. New code should prefer injecting only the settings sub-classes needed.
    此类通过聚合所有较小的 settings 类来保持向后兼容性。新代码应优先注入所需的 settings 子类。
    """

    # EN: AWS resource names for DynamoDB tables and S3 buckets.
    # CN: DynamoDB 表和 S3 bucket 的 AWS 资源名称。
    object_state_table: str
    manifest_index_table: str | None = None
    manifest_bucket: str | None = None
    manifest_prefix: str = DEFAULT_MANIFEST_PREFIX
    execution_state_table: str | None = None
    step_functions_state_machine_arn: str | None = None
    embed_queue_url: str | None = None
    embedding_projection_state_table: str | None = None
    vector_bucket_name: str | None = None
    vector_index_name: str | None = None

    # EN: Embedding provider settings.
    # CN: Embedding provider 设置。
    gemini_api_key: str | None = None
    gemini_api_base_url: str = DEFAULT_GEMINI_API_BASE_URL
    gemini_embedding_model: str = DEFAULT_GEMINI_EMBEDDING_MODEL
    gemini_http_timeout_seconds: int = 120
    openai_api_key: str | None = None
    openai_api_base_url: str | None = None
    openai_embedding_model: str = DEFAULT_OPENAI_EMBEDDING_MODEL
    openai_http_timeout_seconds: int = 120

    # EN: Runtime observability settings.
    # CN: 运行时可观察性设置。
    metrics_namespace: str = "McpKnowledgeS3Vectors"
    service_name: str = "serverless-mcp-service"

    # EN: PaddleOCR settings.
    # CN: PaddleOCR 设置。
    paddle_api_base_url: str = DEFAULT_PADDLE_API_BASE_URL
    paddle_api_token: str | None = None
    paddle_ocr_model: str = DEFAULT_PADDLE_OCR_MODEL
    paddle_poll_interval_seconds: int = DEFAULT_PADDLE_POLL_INTERVAL_SECONDS
    paddle_max_poll_attempts: int = DEFAULT_PADDLE_MAX_POLL_ATTEMPTS
    paddle_http_timeout_seconds: int = DEFAULT_PADDLE_HTTP_TIMEOUT_SECONDS
    paddle_status_timeout_seconds: int = DEFAULT_PADDLE_STATUS_TIMEOUT_SECONDS
    paddle_allowed_hosts: tuple[str, ...] = DEFAULT_PADDLE_OCR_ALLOWED_HOSTS

    # EN: Security and query access control settings.
    # CN: 安全和查询访问控制设置。
    allow_unauthenticated_query: bool = False
    query_tenant_claim: str = DEFAULT_QUERY_TENANT_CLAIM
    remote_mcp_default_tenant_id: str | None = DEFAULT_REMOTE_MCP_DEFAULT_TENANT_ID
    query_max_top_k: int = DEFAULT_QUERY_MAX_TOP_K
    query_max_neighbor_expand: int = DEFAULT_QUERY_MAX_NEIGHBOR_EXPAND
    query_profile_timeout_seconds: float = 15.0
    cloudfront_distribution_domain: str | None = None
    cloudfront_key_pair_id: str | None = None
    cloudfront_private_key_pem: str | None = None
    cloudfront_private_key_secret_arn: str | None = None
    cloudfront_url_ttl_seconds: int = DEFAULT_CLOUDFRONT_URL_TTL_SECONDS

    # EN: Embedding profiles configuration.
    # CN: Embedding profiles 配置。
    embedding_profiles: tuple[EmbeddingProfile, ...] = ()

    @classmethod
    def from_env(cls) -> "Settings":
        """
        EN: Load configuration from environment variables with defaults.
        CN: 从环境变量加载配置并应用默认值。

        Returns:
            EN: Fully initialized Settings instance.
            CN: 完整初始化的 Settings 实例。
        """
        return cls(
            object_state_table=_required_env("OBJECT_STATE_TABLE"),
            execution_state_table=_required_env("EXECUTION_STATE_TABLE"),
            manifest_index_table=os.environ.get("MANIFEST_INDEX_TABLE"),
            manifest_bucket=os.environ.get("MANIFEST_BUCKET"),
            manifest_prefix=_env_or_default(
                "MANIFEST_PREFIX",
                str(_pipeline_default("manifest_prefix", DEFAULT_MANIFEST_PREFIX)),
            ),
            step_functions_state_machine_arn=_optional_env("STEP_FUNCTIONS_STATE_MACHINE_ARN"),
            embed_queue_url=os.environ.get("EMBED_QUEUE_URL"),
            embedding_projection_state_table=os.environ.get("EMBEDDING_PROJECTION_STATE_TABLE"),
            vector_bucket_name=os.environ.get("VECTOR_BUCKET_NAME"),
            vector_index_name=os.environ.get("VECTOR_INDEX_NAME"),
            gemini_api_key=os.environ.get("GEMINI_API_KEY"),
            gemini_api_base_url=_env_or_default(
                "GEMINI_API_BASE_URL",
                str(_pipeline_default("gemini_api_base_url", DEFAULT_GEMINI_API_BASE_URL)),
            ),
            gemini_embedding_model=_env_or_default(
                "GEMINI_EMBEDDING_MODEL",
                str(_pipeline_default("gemini_embedding_model", DEFAULT_GEMINI_EMBEDDING_MODEL)),
            ),
            gemini_http_timeout_seconds=int(
                os.environ.get("GEMINI_HTTP_TIMEOUT_SECONDS", str(DEFAULT_GEMINI_HTTP_TIMEOUT_SECONDS))
            ),
            openai_api_key=_optional_env("OPENAI_API_KEY"),
            openai_api_base_url=(
                normalize_openai_base_url(openai_base_url)
                if (openai_base_url := _optional_env("OPENAI_API_BASE_URL"))
                else None
            ),
            openai_embedding_model=_env_or_default(
                "OPENAI_EMBEDDING_MODEL",
                str(_pipeline_default("openai_embedding_model", DEFAULT_OPENAI_EMBEDDING_MODEL)),
            ),
            openai_http_timeout_seconds=int(
                os.environ.get("OPENAI_HTTP_TIMEOUT_SECONDS", str(DEFAULT_OPENAI_HTTP_TIMEOUT_SECONDS))
            ),
            metrics_namespace=os.environ.get("METRICS_NAMESPACE", "McpKnowledgeS3Vectors"),
            service_name=os.environ.get("POWERTOOLS_SERVICE_NAME", "serverless-mcp-service"),
            paddle_api_base_url=_env_or_default(
                "PADDLE_OCR_API_BASE_URL",
                str(_pipeline_default("paddle_api_base_url", DEFAULT_PADDLE_API_BASE_URL)),
            ),
            paddle_api_token=os.environ.get("PADDLE_OCR_API_TOKEN"),
            paddle_ocr_model=_env_or_default(
                "PADDLE_OCR_MODEL",
                str(_pipeline_default("paddle_ocr_model", DEFAULT_PADDLE_OCR_MODEL)),
            ),
            paddle_poll_interval_seconds=int(
                _env_or_default(
                    "PADDLE_OCR_POLL_INTERVAL_SECONDS",
                    str(_pipeline_default("paddle_poll_interval_seconds", DEFAULT_PADDLE_POLL_INTERVAL_SECONDS)),
                )
            ),
            paddle_max_poll_attempts=int(
                _env_or_default(
                    "PADDLE_OCR_MAX_POLL_ATTEMPTS",
                    str(_pipeline_default("paddle_max_poll_attempts", DEFAULT_PADDLE_MAX_POLL_ATTEMPTS)),
                )
            ),
            paddle_http_timeout_seconds=int(
                _env_or_default(
                    "PADDLE_OCR_HTTP_TIMEOUT_SECONDS",
                    str(_pipeline_default("paddle_http_timeout_seconds", DEFAULT_PADDLE_HTTP_TIMEOUT_SECONDS)),
                )
            ),
            paddle_status_timeout_seconds=int(
                _env_or_default(
                    "PADDLE_OCR_STATUS_TIMEOUT_SECONDS",
                    str(_pipeline_default("paddle_status_timeout_seconds", DEFAULT_PADDLE_STATUS_TIMEOUT_SECONDS)),
                )
            ),
            paddle_allowed_hosts=_load_csv_env(
                "PADDLE_OCR_ALLOWED_HOSTS",
                default=DEFAULT_PADDLE_OCR_ALLOWED_HOSTS,
            ),
            allow_unauthenticated_query=_load_bool_env("ALLOW_UNAUTHENTICATED_QUERY", default=False),
            query_tenant_claim=_env_or_default(
                "QUERY_TENANT_CLAIM",
                str(_pipeline_default("query_tenant_claim", DEFAULT_QUERY_TENANT_CLAIM)),
            ),
            remote_mcp_default_tenant_id=_env_or_default(
                "REMOTE_MCP_DEFAULT_TENANT_ID",
                str(_pipeline_default("remote_mcp_default_tenant_id", DEFAULT_REMOTE_MCP_DEFAULT_TENANT_ID)),
            ),
            query_max_top_k=int(
                _env_or_default("QUERY_MAX_TOP_K", str(_pipeline_default("query_max_top_k", DEFAULT_QUERY_MAX_TOP_K)))
            ),
            query_max_neighbor_expand=int(
                _env_or_default(
                    "QUERY_MAX_NEIGHBOR_EXPAND",
                    str(_pipeline_default("query_max_neighbor_expand", DEFAULT_QUERY_MAX_NEIGHBOR_EXPAND)),
                )
            ),
            query_profile_timeout_seconds=float(os.environ.get("QUERY_PROFILE_TIMEOUT_SECONDS", "15")),
            cloudfront_distribution_domain=os.environ.get("CLOUDFRONT_DISTRIBUTION_DOMAIN"),
            cloudfront_key_pair_id=os.environ.get("CLOUDFRONT_KEY_PAIR_ID"),
            cloudfront_private_key_pem=os.environ.get("CLOUDFRONT_PRIVATE_KEY_PEM"),
            cloudfront_private_key_secret_arn=os.environ.get("CLOUDFRONT_PRIVATE_KEY_SECRET_ARN"),
            cloudfront_url_ttl_seconds=int(
                _env_or_default(
                    "CLOUDFRONT_URL_TTL_SECONDS",
                    str(_pipeline_default("cloudfront_url_ttl_seconds", DEFAULT_CLOUDFRONT_URL_TTL_SECONDS)),
                )
            ),
            embedding_profiles=_load_explicit_embedding_profiles_from_env(
                vector_bucket_name=os.environ.get("VECTOR_BUCKET_NAME"),
                vector_index_name=os.environ.get("VECTOR_INDEX_NAME"),
            ),
        )

    def to_aws(self) -> AWSSettings:
        """
        EN: Extract AWS resource names into a focused settings class.
        CN: 将 AWS 资源名称提取到独立的 settings 类中。

        Returns:
            EN: AWSSettings instance with AWS resource names.
            CN: 包含 AWS 资源名称的 AWSSettings 实例。
        """
        return AWSSettings(
            object_state_table=self.object_state_table,
            execution_state_table=self.execution_state_table,
            manifest_index_table=self.manifest_index_table,
            manifest_bucket=self.manifest_bucket,
            manifest_prefix=self.manifest_prefix,
            embedding_projection_state_table=self.embedding_projection_state_table,
            vector_bucket_name=self.vector_bucket_name,
            vector_index_name=self.vector_index_name,
            step_functions_state_machine_arn=self.step_functions_state_machine_arn,
            embed_queue_url=self.embed_queue_url,
        )

    def to_embedding(self) -> EmbeddingSettings:
        """
        EN: Extract embedding provider settings into a focused settings class.
        CN: 将 embedding provider 设置提取到独立的 settings 类中。

        Returns:
            EN: EmbeddingSettings instance with embedding configuration.
            CN: 包含 embedding 配置的 EmbeddingSettings 实例。
        """
        return EmbeddingSettings(
            gemini_api_key=self.gemini_api_key,
            gemini_api_base_url=self.gemini_api_base_url,
            gemini_embedding_model=self.gemini_embedding_model,
            gemini_http_timeout_seconds=self.gemini_http_timeout_seconds,
            openai_api_key=self.openai_api_key,
            openai_api_base_url=self.openai_api_base_url,
            openai_embedding_model=self.openai_embedding_model,
            openai_http_timeout_seconds=self.openai_http_timeout_seconds,
            embedding_profiles=self.embedding_profiles,
        )

    def to_ocr(self) -> OCRSettings:
        """
        EN: Extract PaddleOCR settings into a focused settings class.
        CN: 将 PaddleOCR 设置提取到独立的 settings 类中。

        Returns:
            EN: OCRSettings instance with PaddleOCR configuration.
            CN: 包含 PaddleOCR 配置的 OCRSettings 实例。
        """
        return OCRSettings(
            paddle_api_base_url=self.paddle_api_base_url,
            paddle_api_token=self.paddle_api_token,
            paddle_ocr_model=self.paddle_ocr_model,
            paddle_poll_interval_seconds=self.paddle_poll_interval_seconds,
            paddle_max_poll_attempts=self.paddle_max_poll_attempts,
            paddle_http_timeout_seconds=self.paddle_http_timeout_seconds,
            paddle_status_timeout_seconds=self.paddle_status_timeout_seconds,
            paddle_allowed_hosts=self.paddle_allowed_hosts,
        )

    def to_security(self) -> SecuritySettings:
        """
        EN: Extract security and access control settings into a focused settings class.
        CN: 将安全和访问控制设置提取到独立的 settings 类中。

        Returns:
            EN: SecuritySettings instance with security configuration.
            CN: 包含安全配置的 SecuritySettings 实例。
        """
        return SecuritySettings(
            allow_unauthenticated_query=self.allow_unauthenticated_query,
            query_tenant_claim=self.query_tenant_claim,
            remote_mcp_default_tenant_id=self.remote_mcp_default_tenant_id,
            query_max_top_k=self.query_max_top_k,
            query_max_neighbor_expand=self.query_max_neighbor_expand,
            query_profile_timeout_seconds=self.query_profile_timeout_seconds,
            cloudfront_distribution_domain=self.cloudfront_distribution_domain,
            cloudfront_key_pair_id=self.cloudfront_key_pair_id,
            cloudfront_private_key_pem=self.cloudfront_private_key_pem,
            cloudfront_private_key_secret_arn=self.cloudfront_private_key_secret_arn,
            cloudfront_url_ttl_seconds=self.cloudfront_url_ttl_seconds,
        )

    def to_runtime(self) -> RuntimeSettings:
        """
        EN: Extract runtime observability settings into a focused settings class.
        CN: 将运行时可观察性设置提取到独立的 settings 类中。

        Returns:
            EN: RuntimeSettings instance with runtime configuration.
            CN: 包含运行时配置的 RuntimeSettings 实例。
        """
        return RuntimeSettings(
            metrics_namespace=self.metrics_namespace,
            service_name=self.service_name,
        )


@lru_cache(maxsize=1)
def load_settings() -> Settings:
    """
    EN: Load and cache process-wide settings from environment variables.
    CN: 从环境变量加载并缓存进程范围的 settings。
    """
    return Settings.from_env()


def _load_explicit_embedding_profiles_from_env(
    *,
    vector_bucket_name: str | None,
    vector_index_name: str | None,
) -> tuple[EmbeddingProfile, ...]:
    """
    EN: Load embedding profiles from EMBEDDING_PROFILES_JSON with validation and model overrides.
    CN: 从 EMBEDDING_PROFILES_JSON 加载 embedding profile，并执行校验和模型覆盖。

    Args:
        vector_bucket_name:
            EN: Legacy vector bucket name from env, triggers requirement for profiles JSON.
            CN: 来自环境变量的旧版 vector bucket 名称，会触发必须提供 profiles JSON。
        vector_index_name:
            EN: Legacy vector index name from env, triggers requirement for profiles JSON.
            CN: 来自环境变量的旧版 vector index 名称，会触发必须提供 profiles JSON。

    Returns:
        EN: Tuple of validated EmbeddingProfile instances with optional model overrides applied.
        CN: 已校验的 EmbeddingProfile 元组，并应用可选的模型覆盖。
    """
    raw = os.environ.get("EMBEDDING_PROFILES_JSON")
    if not raw:
        if vector_bucket_name or vector_index_name:
            raise ValueError("EMBEDDING_PROFILES_JSON is required when vector storage settings are configured")
        return ()

    # EN: Parse JSON array where each item defines a full embedding profile with vector space isolation.
    # CN: 解析 JSON 数组，其中每一项都定义一个完整的 embedding profile 及其向量空间隔离关系。
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("EMBEDDING_PROFILES_JSON must be a JSON array of embedding profile objects") from exc
    if not isinstance(payload, list):
        raise ValueError("EMBEDDING_PROFILES_JSON must be a JSON array of embedding profile objects")

    profiles = tuple(
        _build_embedding_profile(item, index=index)
        for index, item in enumerate(payload)
    )
    _validate_embedding_profiles(profiles)
    openai_model = _optional_env("OPENAI_EMBEDDING_MODEL")
    return _apply_embedding_model_overrides(
        profiles,
        gemini_model=_optional_env("GEMINI_EMBEDDING_MODEL"),
        openai_model=openai_model,
    )


def _build_embedding_profile(item: object, *, index: int) -> EmbeddingProfile:
    """
    EN: Validate and convert one raw profile object into an EmbeddingProfile.
    CN: 校验并将单个原始 profile 对象转换为 EmbeddingProfile。
    """
    if not isinstance(item, dict):
        raise ValueError(f"EMBEDDING_PROFILES_JSON[{index}] must be an object")

    profile_id = _require_profile_string(item, "profile_id", index=index)
    provider = _require_profile_string(item, "provider", index=index)
    if provider not in {"gemini", "openai"}:
        raise ValueError(f"EMBEDDING_PROFILES_JSON[{index}].provider must be 'gemini' or 'openai'")
    model = _require_profile_string(item, "model", index=index)
    vector_bucket_name = _require_profile_string(item, "vector_bucket_name", index=index)
    vector_index_name = _require_profile_string(item, "vector_index_name", index=index)
    dimension_raw = _require_profile_value(item, "dimension", index=index)
    try:
        dimension = int(dimension_raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"EMBEDDING_PROFILES_JSON[{index}].dimension must be an integer") from exc

    supported_content_kinds_raw = _require_profile_value(item, "supported_content_kinds", index=index)
    if isinstance(supported_content_kinds_raw, str) or not isinstance(supported_content_kinds_raw, (list, tuple)):
        raise ValueError(
            f"EMBEDDING_PROFILES_JSON[{index}].supported_content_kinds must be a list of strings"
        )
    supported_content_kinds: list[str] = []
    for kind_index, content_kind in enumerate(supported_content_kinds_raw):
        if not isinstance(content_kind, str) or not content_kind.strip():
            raise ValueError(
                f"EMBEDDING_PROFILES_JSON[{index}].supported_content_kinds[{kind_index}] must be a non-empty string"
            )
        supported_content_kinds.append(content_kind.strip())

    return EmbeddingProfile(
        profile_id=profile_id,
        provider=provider,
        model=model,
        dimension=dimension,
        vector_bucket_name=vector_bucket_name,
        vector_index_name=vector_index_name,
        supported_content_kinds=tuple(supported_content_kinds),
        enabled=_coerce_profile_bool(item.get("enabled", True), field_name=f"EMBEDDING_PROFILES_JSON[{index}].enabled"),
        enable_write=_coerce_profile_bool(
            item.get("enable_write", True),
            field_name=f"EMBEDDING_PROFILES_JSON[{index}].enable_write",
        ),
        enable_query=_coerce_profile_bool(
            item.get("enable_query", True),
            field_name=f"EMBEDDING_PROFILES_JSON[{index}].enable_query",
        ),
    )


def _require_profile_value(item: dict[str, object], field_name: str, *, index: int) -> object:
    """
    EN: Extract a required field from one profile payload.
    CN: 从单个 profile 负载中提取必填字段。
    """
    if field_name not in item:
        raise ValueError(f"EMBEDDING_PROFILES_JSON[{index}].{field_name} is required")
    return item[field_name]


def _require_profile_string(item: dict[str, object], field_name: str, *, index: int) -> str:
    """
    EN: Extract a required non-empty string field from one profile payload.
    CN: 从单个 profile 负载中提取必填的非空字符串字段。
    """
    value = _require_profile_value(item, field_name, index=index)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"EMBEDDING_PROFILES_JSON[{index}].{field_name} must be a non-empty string")
    return value.strip()


def _coerce_profile_bool(value: object, *, field_name: str) -> bool:
    """
    EN: Coerce a profile flag field into a boolean with descriptive errors.
    CN: 将 profile 标志字段转换为布尔值，并在失败时给出明确错误。
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    raise ValueError(f"{field_name} must be a boolean value")


def _validate_embedding_profiles(profiles: tuple[EmbeddingProfile, ...]) -> None:
    """
    EN: Validate that embedding profiles have unique IDs and no duplicate vector spaces.
    CN: 校验 embedding profile 的 ID 唯一，且不存在重复的向量空间。
    """
    seen_profile_ids: set[str] = set()
    seen_vector_spaces: set[tuple[str, str]] = set()
    for profile in profiles:
        if profile.profile_id in seen_profile_ids:
            raise ValueError(f"Duplicate embedding profile_id: {profile.profile_id}")
        seen_profile_ids.add(profile.profile_id)

        # EN: Each vector bucket/index pair must be owned by exactly one profile to prevent cross-profile contamination.
        # CN: 每个 vector bucket/index 组合都必须只归属一个 profile，以避免跨 profile 污染。
        vector_space = (profile.vector_bucket_name, profile.vector_index_name)
        if vector_space in seen_vector_spaces:
            raise ValueError(
                "Duplicate vector bucket/index pair is not allowed across embedding profiles: "
                f"{profile.vector_bucket_name}/{profile.vector_index_name}"
            )
        seen_vector_spaces.add(vector_space)


def _apply_embedding_model_overrides(
    profiles: tuple[EmbeddingProfile, ...],
    *,
    gemini_model: str | None,
    openai_model: str | None,
) -> tuple[EmbeddingProfile, ...]:
    """
    EN: Apply provider-level embedding model overrides from environment variables without changing vector spaces.
    CN: 从环境变量应用 provider 级 embedding 模型覆盖，但不改变向量空间。
    """
    if not gemini_model and not openai_model:
        return profiles

    resolved: list[EmbeddingProfile] = []
    for profile in profiles:
        if profile.provider == "gemini" and gemini_model:
            resolved.append(replace(profile, model=gemini_model))
            continue
        if profile.provider == "openai" and openai_model:
            resolved.append(replace(profile, model=openai_model))
            continue
        resolved.append(profile)
    return tuple(resolved)


def _load_bool_env(name: str, *, default: bool) -> bool:
    """
    EN: Parse a boolean environment variable with a secure default fallback.
    CN: 以安全的默认值解析布尔类型环境变量。
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _load_csv_env(name: str, *, default: tuple[str, ...] = ()) -> tuple[str, ...]:
    """
    EN: Parse a comma-separated environment variable into a normalized tuple.
    CN: 将逗号分隔的环境变量解析为规范化元组。
    """
    raw = os.environ.get(name, "")
    if not raw.strip():
        return default
    values = [item.strip().lower() for item in raw.split(",")]
    normalized = tuple(item for item in values if item)
    return tuple(dict.fromkeys((*default, *normalized)))


def _env_or_default(name: str, default: str) -> str:
    """
    EN: Load an environment variable and fall back to the provided default when blank.
    CN: 加载环境变量，并在其为空白时回退到给定默认值。
    """
    value = os.environ.get(name)
    if value is None:
        return default
    text = value.strip()
    return text if text else default


def _optional_env(name: str) -> str | None:
    """
    EN: Load an optional environment variable and normalize blank values to None.
    CN: 加载可选环境变量，并把空白值规范化为 None。
    """
    value = os.environ.get(name)
    if value is None:
        return None
    text = value.strip()
    return text or None


def _required_env(name: str) -> str:
    """
    EN: Load a required environment variable and raise a descriptive error when it is missing.
    CN: 加载必需环境变量，缺失时抛出带说明的错误。
    """
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise ValueError(f"{name} is required")
    return value
