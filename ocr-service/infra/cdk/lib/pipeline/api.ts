import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import type { Construct } from 'constructs';
import type { PipelineConfig } from '../config';
import type { PipelineResourceBindings } from './bindings';

export interface PipelineApiResources {
  remoteMcpApi: apigateway.RestApi;
  remoteMcpLambda: lambda.IFunction;
}

export interface PipelineApiParams {
  stack: Construct;
  pipelineConfig: PipelineConfig;
  bindings: PipelineResourceBindings;
}

// EN: Keep the REST API isolated so the edge-to-regional choice and the invoke surface can evolve independently.
// CN: 将 REST API 独立出来，方便 API 入口类型和调用面单独演进。
export function createPipelineApi(params: PipelineApiParams): PipelineApiResources {
  const { stack, pipelineConfig, bindings } = params;
  const names = pipelineConfig.resource_names;
  const remoteMcpLambda = lambda.Function.fromFunctionAttributes(stack, 'RemoteMcpLambda', {
    functionArn: bindings.remoteMcpLambdaArn,
    sameEnvironment: true,
  });

  const remoteMcpApi = new apigateway.RestApi(stack, 'RemoteMcpApi', {
    restApiName: names.remote_mcp_api_gateway,
    description: `Remote MCP REST API for ${pipelineConfig.repo_name}`,
    endpointTypes: [apigateway.EndpointType.REGIONAL],
    deployOptions: {
      stageName: pipelineConfig.defaults.api_gateway_stage_name,
    },
  });
  const remoteMcpIntegration = new apigateway.LambdaIntegration(remoteMcpLambda, {
    proxy: true,
  });
  remoteMcpApi.root.addMethod('ANY', remoteMcpIntegration);
  remoteMcpApi.root.addResource('{proxy+}').addMethod('ANY', remoteMcpIntegration);

  return {
    remoteMcpApi,
    remoteMcpLambda,
  };
}
