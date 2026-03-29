import * as fs from 'node:fs';
import * as path from 'node:path';

export interface PipelineDefaults {
  runtime: string;
  architecture: string;
  lambda_memory_size: number;
  lambda_timeout_seconds: number;
  source_intelligent_tiering_days: number;
  source_noncurrent_days: number;
  manifest_prefix: string;
  require_ingest_object_tags: boolean;
  ingest_tenant_tag_key: string;
  ingest_language_tag_key: string;
  paddle_api_base_url: string;
  paddle_ocr_model: string;
  paddle_poll_interval_seconds: number;
  paddle_max_poll_attempts: number;
  paddle_http_timeout_seconds: number;
  paddle_status_timeout_seconds: number;
  gemini_api_base_url: string;
  gemini_embedding_model: string;
  openai_embedding_model: string;
  allow_unauthenticated_query: boolean;
  query_tenant_claim: string;
  remote_mcp_default_tenant_id: string;
  query_max_top_k: number;
  query_max_neighbor_expand: number;
  cloudfront_url_ttl_seconds: number;
  api_gateway_stage_name: string;
  fail_on_job_error: boolean;
}

export interface ResourceNames {
  source_bucket: string;
  manifest_bucket: string;
  vector_bucket: string;
  ingest_queue: string;
  embed_queue: string;
  embed_dlq: string;
  object_state_table: string;
  execution_state_table: string;
  manifest_index_table: string;
  embedding_projection_state_table: string;
  state_machine: string;
  ingest_lambda: string;
  extract_prepare_lambda: string;
  extract_sync_lambda: string;
  extract_submit_lambda: string;
  extract_poll_lambda: string;
  extract_persist_lambda: string;
  extract_mark_failed_lambda: string;
  embed_lambda: string;
  remote_mcp_lambda: string;
  remote_mcp_api_gateway: string;
  backfill_lambda: string;
  job_status_lambda: string;
  core_dependency_layer: string;
  extract_dependency_layer: string;
  embedding_dependency_layer: string;
  lambda_role: string;
  state_machine_role: string;
  state_machine_log_group: string;
}

export interface EmbeddingProfileConfig {
  profile_id: string;
  provider: string;
  model: string;
  dimension: number;
  vector_bucket_name: string;
  vector_index_name: string;
  supported_content_kinds: string[];
  enabled?: boolean;
  enable_write?: boolean;
  enable_query?: boolean;
  non_filterable_metadata_keys?: string[];
}

export interface LambdaRuntimeSettings {
  memory_size: number;
  timeout_seconds: number;
}

export interface PipelineConfig {
  name_prefix: string;
  name_suffix?: string;
  repo_name: string;
  defaults: PipelineDefaults;
  resource_names: ResourceNames;
  embedding_profiles: EmbeddingProfileConfig[];
  lambda_settings: Record<string, LambdaRuntimeSettings>;
}

export interface DeploymentInputs {
  paddleApiToken?: string;
  geminiApiKey?: string;
  openaiApiKey?: string;
  openaiApiBaseUrl?: string;
  openaiEmbeddingModel?: string;
  paddleAllowedHosts?: string;
  remoteMcpDefaultTenantId?: string;
  cloudfrontDistributionDomain?: string;
  cloudfrontKeyPairId?: string;
  cloudfrontPrivateKeyPem?: string;
}

export function loadPipelineConfig(repoRoot: string, configPath: string): PipelineConfig {
  const resolvedPath = path.isAbsolute(configPath) ? configPath : path.resolve(repoRoot, configPath);
  const payload = JSON.parse(fs.readFileSync(resolvedPath, 'utf8')) as PipelineConfig;
  if (!payload || typeof payload !== 'object') {
    throw new Error(`Pipeline config must be a JSON object: ${resolvedPath}`);
  }
  return payload;
}

export function resolveDeploymentInputs(env: NodeJS.ProcessEnv): DeploymentInputs {
  return {
    paddleApiToken: optionalEnv(env, 'PADDLE_OCR_API_TOKEN'),
    geminiApiKey: optionalEnv(env, 'GEMINI_API_KEY'),
    openaiApiKey: optionalEnv(env, 'OPENAI_API_KEY') ?? optionalEnv(env, 'AZURE_OPENAI_API_KEY'),
    openaiApiBaseUrl:
      optionalEnv(env, 'OPENAI_API_BASE_URL') ??
      optionalEnv(env, 'OPENAI_BASE_URL'),
    openaiEmbeddingModel: optionalEnv(env, 'OPENAI_EMBEDDING_MODEL'),
    paddleAllowedHosts: optionalEnv(env, 'PADDLE_OCR_ALLOWED_HOSTS'),
    remoteMcpDefaultTenantId: optionalEnv(env, 'REMOTE_MCP_DEFAULT_TENANT_ID'),
    cloudfrontDistributionDomain: optionalEnv(env, 'CLOUDFRONT_DISTRIBUTION_DOMAIN'),
    cloudfrontKeyPairId: optionalEnv(env, 'CLOUDFRONT_KEY_PAIR_ID'),
    cloudfrontPrivateKeyPem: optionalEnv(env, 'CLOUDFRONT_PRIVATE_KEY_PEM'),
  };
}

function optionalEnv(env: NodeJS.ProcessEnv, key: string): string | undefined {
  const value = env[key]?.trim();
  return value ? value : undefined;
}

// EN: Resolve the name_suffix config value into the actual suffix string appended to every AWS resource name.
// CN: 将 name_suffix 配置值解析为追加到每个 AWS 资源名称后的实际后缀字符串。
export function resolveNameSuffix(suffix: string | undefined, account: string, region: string): string {
  if (!suffix || suffix === '' || suffix === 'none') {
    return '';
  }
  if (suffix === 'auto') {
    return `${account}-${region}`;
  }
  return suffix;
}

// EN: AWS service-specific maximum name lengths used to validate suffixed resource names.
// CN: AWS 服务级资源名称最大长度，用于校验追加后缀后的名称是否合规。
const NAME_LENGTH_LIMITS: Record<string, number> = {
  source_bucket: 63,
  manifest_bucket: 63,
  vector_bucket: 63,
  ingest_queue: 80,
  embed_queue: 80,
  embed_dlq: 80,
  object_state_table: 255,
  execution_state_table: 255,
  manifest_index_table: 255,
  embedding_projection_state_table: 255,
  state_machine: 80,
  ingest_lambda: 64,
  extract_prepare_lambda: 64,
  extract_sync_lambda: 64,
  extract_submit_lambda: 64,
  extract_poll_lambda: 64,
  extract_persist_lambda: 64,
  extract_mark_failed_lambda: 64,
  embed_lambda: 64,
  remote_mcp_lambda: 64,
  remote_mcp_api_gateway: 255,
  backfill_lambda: 64,
  job_status_lambda: 64,
  core_dependency_layer: 140,
  extract_dependency_layer: 140,
  embedding_dependency_layer: 140,
  lambda_role: 64,
  state_machine_role: 64,
  state_machine_log_group: 512,
};

// EN: Mutate pipelineConfig.resource_names and embedding_profiles in-place so every downstream consumer sees the suffixed names.
// CN: 原地修改 pipelineConfig.resource_names 和 embedding_profiles，让所有下游消费者直接看到带后缀的名称。
export function applyNameSuffix(config: PipelineConfig, account: string, region: string): void {
  const suffix = resolveNameSuffix(config.name_suffix, account, region);
  if (!suffix) {
    return;
  }

  const names = config.resource_names as unknown as Record<string, string>;
  for (const key of Object.keys(names)) {
    names[key] = `${names[key]}-${suffix}`;
  }

  for (const profile of config.embedding_profiles) {
    profile.vector_bucket_name = `${profile.vector_bucket_name}-${suffix}`;
  }

  // EN: Validate that no suffixed name exceeds its service-specific length limit.
  // CN: 校验追加后缀后的名称是否超出对应服务的长度限制。
  const violations: string[] = [];
  for (const [key, limit] of Object.entries(NAME_LENGTH_LIMITS)) {
    const name = names[key];
    if (name && name.length > limit) {
      violations.push(`  ${key}: "${name}" (${name.length} chars, limit: ${limit})`);
    }
  }
  if (violations.length > 0) {
    throw new Error(
      `Resource name(s) exceed service limits after applying suffix "-${suffix}":\n${violations.join('\n')}\n` +
      `Consider shortening "name_prefix" or using a custom shorter "name_suffix".`,
    );
  }
}
