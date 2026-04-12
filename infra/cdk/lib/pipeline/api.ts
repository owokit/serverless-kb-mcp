import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import type { Construct } from 'constructs';
import type { DeploymentInputs, PipelineConfig } from '../config';
import type { PipelineResourceBindings } from './bindings';

export interface PipelineApiResources {
  remoteMcpApi: apigateway.RestApi;
  remoteMcpLambda: lambda.IFunction;
}

export interface PipelineApiParams {
  stack: Construct;
  pipelineConfig: PipelineConfig;
  bindings: PipelineResourceBindings;
  deploymentInputs: DeploymentInputs;
}

// EN: Keep the REST API isolated so the edge-to-regional choice and the invoke surface can evolve independently.
// CN: 将 REST API 独立出来，方便 API 入口类型和调用面单独演进。
export function createPipelineApi(params: PipelineApiParams): PipelineApiResources {
  const { stack, pipelineConfig, bindings, deploymentInputs } = params;
  const names = pipelineConfig.resource_names;
  const defaults = pipelineConfig.defaults;
  const remoteMcpLambda = lambda.Function.fromFunctionAttributes(stack, 'RemoteMcpLambda', {
    functionArn: bindings.remoteMcpLambdaArn,
    sameEnvironment: true,
  });
  const apiKeyProtectionEnabled = defaults.remote_mcp_api_key_protection_enabled !== false;
  const methodOptions: apigateway.MethodOptions = apiKeyProtectionEnabled
    ? { apiKeyRequired: true }
    : {};

  const remoteMcpApi = new apigateway.RestApi(stack, 'RemoteMcpApi', {
    restApiName: names.remote_mcp_api_gateway,
    description: `Remote MCP REST API for ${pipelineConfig.repo_name}`,
    endpointTypes: [apigateway.EndpointType.REGIONAL],
    defaultMethodOptions: methodOptions,
    deployOptions: {
      stageName: pipelineConfig.defaults.api_gateway_stage_name,
      throttlingRateLimit: defaults.remote_mcp_api_throttle_rate_limit,
      throttlingBurstLimit: defaults.remote_mcp_api_throttle_burst_limit,
    },
  });
  const remoteMcpIntegration = new apigateway.LambdaIntegration(remoteMcpLambda, {
    proxy: true,
  });
  remoteMcpApi.root.addMethod('ANY', remoteMcpIntegration, methodOptions);
  remoteMcpApi.root.addResource('{proxy+}').addMethod('ANY', remoteMcpIntegration, methodOptions);

  if (apiKeyProtectionEnabled) {
    const apiKeyValue = deploymentInputs.remoteMcpApiKeyValue?.trim();
    if (!apiKeyValue) {
      throw new Error(
        'REMOTE_MCP_API_KEY_VALUE is required when defaults.remote_mcp_api_key_protection_enabled is true.',
      );
    }
    if (!/^[A-Za-z0-9]{20,128}$/.test(apiKeyValue)) {
      throw new Error(
        'REMOTE_MCP_API_KEY_VALUE must be an alphanumeric string between 20 and 128 characters so API Gateway can accept it.',
      );
    }

    const remoteMcpUsagePlan = new apigateway.UsagePlan(stack, 'RemoteMcpUsagePlan', {
      usagePlanName: names.remote_mcp_usage_plan,
      description: `Usage plan for ${pipelineConfig.repo_name} remote MCP access`,
      throttle: {
        rateLimit: defaults.remote_mcp_api_throttle_rate_limit,
        burstLimit: defaults.remote_mcp_api_throttle_burst_limit,
      },
      quota: {
        limit: defaults.remote_mcp_api_quota_limit,
        period: defaults.remote_mcp_api_quota_period as apigateway.Period,
      },
    });
    const remoteMcpApiKey = new apigateway.ApiKey(stack, 'RemoteMcpApiKey', {
      apiKeyName: names.remote_mcp_api_key,
      description: `API key for ${pipelineConfig.repo_name} remote MCP access`,
      enabled: true,
      value: apiKeyValue,
    });
    remoteMcpUsagePlan.addApiStage({
      api: remoteMcpApi,
      stage: remoteMcpApi.deploymentStage,
    });
    remoteMcpUsagePlan.addApiKey(remoteMcpApiKey);
  }

  return {
    remoteMcpApi,
    remoteMcpLambda,
  };
}
