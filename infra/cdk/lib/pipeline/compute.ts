import * as cdk from 'aws-cdk-lib';
import { Duration, RemovalPolicy } from 'aws-cdk-lib';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import type { Construct } from 'constructs';
import { buildLayerZipPath, buildLambdaZipPath, LAYER_KEYS, type LayerKey, type LambdaFunctionKey } from '../artifacts';
import type { DeploymentInputs, PipelineConfig } from '../config';
import { defaultRuntimeSettings, pascal, renderStateMachineDefinition, renderVectorCleanupStateMachineDefinition, resolveAssetPath } from './helpers';
import { createPipelineRoles, type LambdaRoleBundle, type LambdaRoleKey } from './roles';
import type { PipelineResourceBindings } from './bindings';

export interface LambdaDefinition {
  functionKey: LambdaFunctionKey;
  functionName: string;
  roleKey: LambdaRoleKey;
  layerKeys: LayerKey[];
}

export interface PipelineComputeResources {
  lambdaFunctions: Map<LambdaFunctionKey, lambda.Function>;
  stateMachine: sfn.StateMachine;
  cleanupStateMachine: sfn.StateMachine;
  remoteMcpLambda: lambda.Function;
}

export interface PipelineComputeParams {
  stack: cdk.Stack;
  pipelineConfig: PipelineConfig;
  artifactDir: string;
  deploymentInputs: DeploymentInputs;
  allowPlaceholderAssets: boolean;
  bindings: PipelineResourceBindings;
}

// EN: Build the Lambda, state machine, and API layer separately from the shared data-plane foundation.
// CN: 将 Lambda、状态机和 API 层与共享数据平面基础设施分开构建。
export function createPipelineCompute(params: PipelineComputeParams): PipelineComputeResources {
  const { stack, pipelineConfig, artifactDir, deploymentInputs, allowPlaceholderAssets, bindings } = params;
  const names = pipelineConfig.resource_names;
  const defaultSettings = pipelineConfig.defaults;
  const runtime = new lambda.Runtime(defaultSettings.runtime, lambda.RuntimeFamily.PYTHON);
  const architecture = defaultSettings.architecture === 'x86_64' ? lambda.Architecture.X86_64 : lambda.Architecture.ARM_64;
  const roles = createPipelineRoles({
    stack,
    names,
    bindings,
  });

  // EN: Stage layer assets first so every Lambda can reuse the same layer map and placeholder logic.
  // CN: 先准备 layer 产物，确保每个 Lambda 都复用同一份 layer 映射和占位逻辑。
  const layerArtifacts = createLayerArtifacts(stack, {
    artifactDir,
    pipelineConfig,
    runtime,
    architecture,
    allowPlaceholderAssets,
  });

  // EN: Keep the Lambda declarations data-driven so queue wiring and role assignment stay in one place.
  // CN: 保持 Lambda 声明数据驱动，让队列绑定和角色分配集中在一处。
  const lambdaDefinitions: LambdaDefinition[] = createLambdaDefinitions(names);

  const lambdaFunctions = new Map<LambdaFunctionKey, lambda.Function>();
  for (const definition of lambdaDefinitions) {
    const zipPath = buildLambdaZipPath(artifactDir, pipelineConfig.repo_name, definition.functionKey);
    const assetPath = resolveAssetPath(zipPath, allowPlaceholderAssets);
    const runtimeSettings = pipelineConfig.lambda_settings[definition.functionKey] ?? defaultRuntimeSettings(defaultSettings);
    const fn = new lambda.Function(stack, pascal(definition.functionKey), {
      functionName: definition.functionName,
      runtime,
      architecture,
      handler: 'lambda_function.lambda_handler',
      code: lambda.Code.fromAsset(assetPath),
      role: roles.lambdaRoles.get(definition.roleKey),
      memorySize: runtimeSettings.memory_size,
      timeout: Duration.seconds(runtimeSettings.timeout_seconds),
      environment: buildLambdaEnvironment({
        pipelineConfig,
        deploymentInputs,
        names,
        bindings,
        defaultSettings,
        functionKey: definition.functionKey,
        allowPlaceholderAssets,
      }),
      layers: definition.layerKeys.map((key) => layerArtifacts.get(key)!),
      tracing: lambda.Tracing.ACTIVE,
    });
    fn.applyRemovalPolicy(RemovalPolicy.DESTROY);
    lambdaFunctions.set(definition.functionKey, fn);
  }

  // EN: Ingest and embed are queue consumers; keep the event-source mappings explicit instead of hiding them in the definition array.
  // CN: Ingest 和 embed 是队列消费者；保持 event source mapping 显式可见，不把它藏进定义数组里。
  lambdaFunctions.get('ingest')?.addEventSourceMapping('IngestQueueMapping', {
    eventSourceArn: bindings.ingestQueueArn,
    batchSize: 10,
    enabled: true,
    reportBatchItemFailures: true,
  });
  lambdaFunctions.get('embed')?.addEventSourceMapping('EmbedQueueMapping', {
    eventSourceArn: bindings.embedQueueArn,
    batchSize: 1,
    enabled: true,
    reportBatchItemFailures: true,
  });

  // EN: Keep the state machine log group close to the state machine so the failure surface stays obvious.
  // CN: 将状态机日志组放在状态机附近，方便一眼看出故障面。
  const stateMachineLogGroup = new logs.LogGroup(stack, 'StateMachineLogGroup', {
    logGroupName: names.state_machine_log_group,
    retention: logs.RetentionDays.ONE_MONTH,
    removalPolicy: RemovalPolicy.DESTROY,
  });
  const definition = renderStateMachineDefinition(lambdaFunctions);
  const stateMachine = new sfn.StateMachine(stack, 'ExtractStateMachine', {
    stateMachineName: names.state_machine,
    definitionBody: sfn.DefinitionBody.fromString(definition),
    stateMachineType: sfn.StateMachineType.STANDARD,
    role: roles.stateMachineRole,
    logs: {
      destination: stateMachineLogGroup,
      level: sfn.LogLevel.ERROR,
      includeExecutionData: false,
    },
    tracingEnabled: true,
  });
  stateMachine.applyRemovalPolicy(RemovalPolicy.DESTROY);

  // EN: Keep vector cleanup in its own state machine so the delete path can evolve independently from extraction.
  // CN: 灏?vector cleanup 鍒嗗埌鐙珛 state machine锛岃鍒犻櫎璺緞鑳藉拰鎻愬彇閾炬潯鐙珛婕旇繘銆?
  const cleanupStateMachineLogGroup = new logs.LogGroup(stack, 'CleanupStateMachineLogGroup', {
    logGroupName: names.cleanup_state_machine_log_group,
    retention: logs.RetentionDays.ONE_MONTH,
    removalPolicy: RemovalPolicy.DESTROY,
  });
  const cleanupDefinition = renderVectorCleanupStateMachineDefinition();
  const cleanupStateMachine = new sfn.StateMachine(stack, 'CleanupStateMachine', {
    stateMachineName: names.cleanup_state_machine,
    definitionBody: sfn.DefinitionBody.fromString(cleanupDefinition),
    stateMachineType: sfn.StateMachineType.STANDARD,
    role: roles.cleanupStateMachineRole,
    logs: {
      destination: cleanupStateMachineLogGroup,
      level: sfn.LogLevel.ERROR,
      includeExecutionData: false,
    },
    tracingEnabled: true,
  });
  cleanupStateMachine.applyRemovalPolicy(RemovalPolicy.DESTROY);

  // EN: Grant the state machine only the invoke/logging permissions it needs for extract orchestration.
  // CN: 只给状态机授予 extract 编排所需的 invoke 和日志权限。
  for (const arn of [
    'extract_prepare',
    'extract_sync',
    'extract_submit',
    'extract_poll',
    'extract_persist',
    'extract_mark_failed',
  ].map((key) => lambdaFunctions.get(key as LambdaFunctionKey)!.functionArn)) {
    roles.stateMachineRole.addToPrincipalPolicy(
      new iam.PolicyStatement({
        actions: ['lambda:InvokeFunction'],
        resources: [arn],
      }),
    );
  }
  roles.stateMachineRole.addToPrincipalPolicy(
    new iam.PolicyStatement({
      actions: [
        'logs:CreateLogDelivery',
        'logs:CreateLogStream',
        'logs:GetLogDelivery',
        'logs:PutLogEvents',
        'logs:UpdateLogDelivery',
        'logs:DeleteLogDelivery',
        'logs:ListLogDeliveries',
        'logs:PutResourcePolicy',
        'logs:DescribeResourcePolicies',
        'logs:DescribeLogGroups',
      ],
      resources: [stateMachineLogGroup.logGroupArn],
    }),
  );
  roles.stateMachineRole.addToPrincipalPolicy(
    new iam.PolicyStatement({
      actions: ['xray:PutTraceSegments', 'xray:PutTelemetryRecords'],
      resources: ['*'],
    }),
  );
  roles.cleanupStateMachineRole.addToPrincipalPolicy(
    new iam.PolicyStatement({
      actions: ['s3vectors:DeleteVectors'],
      resources: bindings.vectorIndexArns,
    }),
  );
  roles.cleanupStateMachineRole.addToPrincipalPolicy(
    new iam.PolicyStatement({
      actions: [
        'logs:CreateLogDelivery',
        'logs:CreateLogStream',
        'logs:GetLogDelivery',
        'logs:PutLogEvents',
        'logs:UpdateLogDelivery',
        'logs:DeleteLogDelivery',
        'logs:ListLogDeliveries',
        'logs:PutResourcePolicy',
        'logs:DescribeResourcePolicies',
        'logs:DescribeLogGroups',
      ],
      resources: [cleanupStateMachineLogGroup.logGroupArn],
    }),
  );
  roles.cleanupStateMachineRole.addToPrincipalPolicy(
    new iam.PolicyStatement({
      actions: ['xray:PutTraceSegments', 'xray:PutTelemetryRecords'],
      resources: ['*'],
    }),
  );

  return {
    lambdaFunctions,
    stateMachine,
    cleanupStateMachine,
    remoteMcpLambda: lambdaFunctions.get('remote_mcp')!,
  };
}

// EN: Keep the function inventory data-driven so the main stack only needs to know names and roles.
// CN: 保持函数清单数据驱动，让主 stack 只需要知道名字和角色。
function createLambdaDefinitions(names: PipelineConfig['resource_names']): LambdaDefinition[] {
  return [
    { functionKey: 'ingest', functionName: names.ingest_lambda, roleKey: 'ingest', layerKeys: ['core'] },
    { functionKey: 'extract_prepare', functionName: names.extract_prepare_lambda, roleKey: 'extract', layerKeys: ['core', 'extract'] },
    { functionKey: 'extract_sync', functionName: names.extract_sync_lambda, roleKey: 'extract', layerKeys: ['core', 'extract'] },
    { functionKey: 'extract_submit', functionName: names.extract_submit_lambda, roleKey: 'extract', layerKeys: ['core', 'extract'] },
    { functionKey: 'extract_poll', functionName: names.extract_poll_lambda, roleKey: 'extract', layerKeys: ['core', 'extract'] },
    { functionKey: 'extract_persist', functionName: names.extract_persist_lambda, roleKey: 'extract', layerKeys: ['core', 'extract'] },
    { functionKey: 'extract_mark_failed', functionName: names.extract_mark_failed_lambda, roleKey: 'extract', layerKeys: ['core', 'extract'] },
    { functionKey: 'embed', functionName: names.embed_lambda, roleKey: 'embed', layerKeys: ['core', 'embedding'] },
    { functionKey: 'remote_mcp', functionName: names.remote_mcp_lambda, roleKey: 'query', layerKeys: ['core', 'embedding'] },
    { functionKey: 'backfill', functionName: names.backfill_lambda, roleKey: 'backfill', layerKeys: ['core', 'extract', 'embedding'] },
    { functionKey: 'job_status', functionName: names.job_status_lambda, roleKey: 'status', layerKeys: ['core'] },
  ];
}

// EN: Build Lambda environment variables in one place so the deployment and runtime contract stays visible.
// CN: 把 Lambda 环境变量集中在一处构建，方便同时看清部署和运行时契约。
function buildLambdaEnvironment(params: {
  pipelineConfig: PipelineConfig;
  deploymentInputs: DeploymentInputs;
  names: PipelineConfig['resource_names'];
  bindings: PipelineResourceBindings;
  defaultSettings: PipelineConfig['defaults'];
  functionKey: LambdaFunctionKey;
  allowPlaceholderAssets: boolean;
}): Record<string, string> {
  const { pipelineConfig, deploymentInputs, names, bindings, defaultSettings, functionKey, allowPlaceholderAssets } = params;
  const env: Record<string, string> = {
    POWERTOOLS_SERVICE_NAME: pipelineConfig.repo_name,
    OBJECT_STATE_TABLE: names.object_state_table,
    EXECUTION_STATE_TABLE: names.execution_state_table,
    MANIFEST_INDEX_TABLE: names.manifest_index_table,
    MANIFEST_BUCKET: names.manifest_bucket,
    MANIFEST_PREFIX: defaultSettings.manifest_prefix,
    VECTOR_BUCKET_NAME: names.vector_bucket,
    EMBEDDING_PROJECTION_STATE_TABLE: names.embedding_projection_state_table,
    EMBEDDING_PROFILES_JSON: JSON.stringify(pipelineConfig.embedding_profiles),
    QUERY_TENANT_CLAIM: defaultSettings.query_tenant_claim,
    QUERY_MAX_TOP_K: String(defaultSettings.query_max_top_k),
    QUERY_MAX_NEIGHBOR_EXPAND: String(defaultSettings.query_max_neighbor_expand),
    REQUIRE_INGEST_OBJECT_TAGS: String(defaultSettings.require_ingest_object_tags),
    INGEST_TENANT_TAG_KEY: defaultSettings.ingest_tenant_tag_key,
    INGEST_LANGUAGE_TAG_KEY: defaultSettings.ingest_language_tag_key,
    PADDLE_OCR_API_BASE_URL: defaultSettings.paddle_api_base_url,
    PADDLE_OCR_MODEL: defaultSettings.paddle_ocr_model,
    PADDLE_OCR_POLL_INTERVAL_SECONDS: String(defaultSettings.paddle_poll_interval_seconds),
    PADDLE_OCR_MAX_POLL_ATTEMPTS: String(defaultSettings.paddle_max_poll_attempts),
    PADDLE_OCR_HTTP_TIMEOUT_SECONDS: String(defaultSettings.paddle_http_timeout_seconds),
    PADDLE_OCR_STATUS_TIMEOUT_SECONDS: String(defaultSettings.paddle_status_timeout_seconds),
    PADDLE_OCR_ALLOWED_HOSTS: deploymentInputs.paddleAllowedHosts ?? '',
    OPENAI_EMBEDDING_MODEL: deploymentInputs.openaiEmbeddingModel ?? defaultSettings.openai_embedding_model,
    CLOUDFRONT_URL_TTL_SECONDS: String(defaultSettings.cloudfront_url_ttl_seconds),
    ALLOW_UNAUTHENTICATED_QUERY: String(defaultSettings.allow_unauthenticated_query),
  };
  if (functionKey === 'embed') {
    env.VECTOR_CLEANUP_STATE_MACHINE_ARN = bindings.cleanupStateMachineArn;
  }

  const geminiEnabled = pipelineConfig.embedding_profiles.some(
    (profile) => profile.provider === 'gemini' && profile.enabled !== false && (profile.enable_query !== false || profile.enable_write !== false),
  );
  const openAiEnabled = pipelineConfig.embedding_profiles.some(
    (profile) => profile.provider === 'openai' && profile.enabled !== false && (profile.enable_query !== false || profile.enable_write !== false),
  );
  if (geminiEnabled) {
    if (!deploymentInputs.geminiApiKey && !allowPlaceholderAssets) {
      throw new Error('GEMINI_API_KEY is required for enabled Gemini embedding profiles.');
    }
    env.GEMINI_API_BASE_URL = defaultSettings.gemini_api_base_url;
    env.GEMINI_EMBEDDING_MODEL = defaultSettings.gemini_embedding_model;
    if (deploymentInputs.geminiApiKey) {
      env.GEMINI_API_KEY = deploymentInputs.geminiApiKey;
    }
  }
  if (openAiEnabled) {
    if (!deploymentInputs.openaiApiKey && !allowPlaceholderAssets) {
      throw new Error('OPENAI_API_KEY or AZURE_OPENAI_API_KEY is required for enabled OpenAI embedding profiles.');
    }
    if (!deploymentInputs.openaiApiBaseUrl && !allowPlaceholderAssets) {
      throw new Error('OPENAI_API_BASE_URL or OPENAI_BASE_URL is required for enabled OpenAI embedding profiles.');
    }
    if (deploymentInputs.openaiApiKey) {
      env.OPENAI_API_KEY = deploymentInputs.openaiApiKey;
    }
    if (deploymentInputs.openaiApiBaseUrl) {
      env.OPENAI_API_BASE_URL = deploymentInputs.openaiApiBaseUrl;
    }
  }

  if (functionKey === 'ingest') {
    env.STEP_FUNCTIONS_STATE_MACHINE_ARN = bindings.stateMachineArn;
  }
  if (functionKey === 'remote_mcp') {
    env.REMOTE_MCP_DEFAULT_TENANT_ID = deploymentInputs.remoteMcpDefaultTenantId ?? defaultSettings.remote_mcp_default_tenant_id;
  }
  if (deploymentInputs.paddleApiToken) {
    env.PADDLE_OCR_API_TOKEN = deploymentInputs.paddleApiToken;
  }
  if (deploymentInputs.cloudfrontDistributionDomain) {
    env.CLOUDFRONT_DISTRIBUTION_DOMAIN = deploymentInputs.cloudfrontDistributionDomain;
  }
  if (deploymentInputs.cloudfrontKeyPairId) {
    env.CLOUDFRONT_KEY_PAIR_ID = deploymentInputs.cloudfrontKeyPairId;
  }
  if (deploymentInputs.cloudfrontPrivateKeyPem) {
    env.CLOUDFRONT_PRIVATE_KEY_PEM = deploymentInputs.cloudfrontPrivateKeyPem;
  }
  if (functionKey === 'embed') {
    env.FAIL_ON_JOB_ERROR = String(defaultSettings.fail_on_job_error);
  }
  const needsEmbedQueueUrl = functionKey === 'extract_sync' || functionKey === 'extract_persist' || functionKey === 'backfill';
  if (needsEmbedQueueUrl) {
    env.EMBED_QUEUE_URL = bindings.embedQueueUrl;
  }
  return env;
}

// EN: Stage layer zips and create Lambda layer versions in the same scope as the stack.
// CN: 在与 stack 相同的 scope 中准备 layer zip 并创建 Lambda layer version。
function createLayerArtifacts(
  scope: Construct,
  params: {
    artifactDir: string;
    pipelineConfig: PipelineConfig;
    runtime: lambda.Runtime;
    architecture: lambda.Architecture;
    allowPlaceholderAssets: boolean;
  },
): Map<LayerKey, lambda.LayerVersion> {
  const { artifactDir, pipelineConfig, runtime, architecture, allowPlaceholderAssets } = params;
  const layerArtifacts = new Map<LayerKey, lambda.LayerVersion>();
  for (const layerKey of LAYER_KEYS) {
    const layerZip = buildLayerZipPath(artifactDir, pipelineConfig.repo_name, layerKey);
    const layerAssetPath = resolveAssetPath(layerZip, allowPlaceholderAssets);
    const layerName = pipelineConfig.resource_names[`${layerKey}_dependency_layer` as keyof PipelineConfig['resource_names']] as string;
    layerArtifacts.set(
      layerKey,
      new lambda.LayerVersion(scope, `${pascal(layerKey)}Layer`, {
        layerVersionName: layerName,
        description: `${pipelineConfig.repo_name}:${layerKey}`,
        code: lambda.Code.fromAsset(layerAssetPath),
        compatibleRuntimes: [runtime],
        compatibleArchitectures: [architecture],
        removalPolicy: RemovalPolicy.DESTROY,
      }),
    );
  }
  return layerArtifacts;
}
