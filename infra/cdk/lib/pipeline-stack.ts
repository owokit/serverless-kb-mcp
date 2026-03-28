import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { ApiStack } from './api-stack';
import { ComputeStack } from './compute-stack';
import { FoundationStack } from './foundation-stack';
import type { DeploymentInputs, PipelineConfig } from './config';

export interface PipelineStackProps extends cdk.StackProps {
  pipelineConfig: PipelineConfig;
  artifactDir: string;
  deploymentInputs: DeploymentInputs;
  allowPlaceholderAssets?: boolean;
}

// EN: Keep a compatibility wrapper for the historical single-stack entry while the app now instantiates the split stacks directly.
// CN: 保留历史单栈入口的兼容包装，但实际 app 已经直接实例化拆分后的多个 stack。
export class PipelineStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: PipelineStackProps) {
    super(scope, id, props);

    const { pipelineConfig, artifactDir, deploymentInputs, allowPlaceholderAssets = false } = props;
    const stackEnvironment = {
      env: {
        account: props.env?.account ?? process.env.CDK_DEFAULT_ACCOUNT ?? process.env.AWS_ACCOUNT_ID,
        region: props.env?.region ?? process.env.CDK_DEFAULT_REGION ?? process.env.AWS_REGION,
      },
    };

    const foundationStack = new FoundationStack(scope, `${id}-foundation`, {
      ...stackEnvironment,
      pipelineConfig,
    });
    const computeStack = new ComputeStack(scope, `${id}-compute`, {
      ...stackEnvironment,
      pipelineConfig,
      artifactDir,
      deploymentInputs,
      allowPlaceholderAssets,
    });
    const apiStack = new ApiStack(scope, `${id}-api`, {
      ...stackEnvironment,
      pipelineConfig,
    });

    computeStack.addDependency(foundationStack);
    apiStack.addDependency(computeStack);

    cdk.Tags.of(this).add('app', pipelineConfig.repo_name);
  }
}
