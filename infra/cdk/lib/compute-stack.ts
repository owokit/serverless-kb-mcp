import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import type { DeploymentInputs, PipelineConfig } from './config';
import { buildPipelineResourceBindings } from './pipeline/bindings';
import { createPipelineCompute } from './pipeline/compute';

export interface ComputeStackProps extends cdk.StackProps {
  pipelineConfig: PipelineConfig;
  artifactDir: string;
  deploymentInputs: DeploymentInputs;
  allowPlaceholderAssets?: boolean;
}

// EN: Keep Lambda, Step Functions, and execution roles in a separate compute stack so their assets can evolve independently.
// CN: 将 Lambda、Step Functions 和执行角色放入独立的计算栈，方便它们独立演进。
export class ComputeStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: ComputeStackProps) {
    super(scope, id, props);

    const { pipelineConfig, artifactDir, deploymentInputs, allowPlaceholderAssets = false } = props;
    const bindings = buildPipelineResourceBindings(this, pipelineConfig);
    const compute = createPipelineCompute({
      stack: this,
      pipelineConfig,
      artifactDir,
      deploymentInputs,
      allowPlaceholderAssets,
      bindings,
    });

    new cdk.CfnOutput(this, 'StateMachineArn', {
      value: compute.stateMachine.stateMachineArn,
    });
    new cdk.CfnOutput(this, 'RemoteMcpLambdaArn', {
      value: compute.remoteMcpLambda.functionArn,
    });

    cdk.Tags.of(this).add('app', pipelineConfig.repo_name);
  }
}
