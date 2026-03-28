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
  /** EN: Removal policy for data buckets (source/manifest). CN: 数据 bucket（source/manifest）的移除策略。 */
  data_bucket_removal_policy?: 'RETAIN' | 'DESTROY';
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
