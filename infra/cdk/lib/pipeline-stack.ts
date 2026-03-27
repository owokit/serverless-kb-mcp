import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import type { DeploymentInputs, PipelineConfig } from './config';
import { createPipelineCompute } from './pipeline/compute';
import { createPipelineFoundation } from './pipeline/foundation';
import { createPipelineRoles } from './pipeline/roles';

export interface PipelineStackProps extends cdk.StackProps {
  pipelineConfig: PipelineConfig;
  artifactDir: string;
  deploymentInputs: DeploymentInputs;
  allowPlaceholderAssets?: boolean;
}

export class PipelineStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: PipelineStackProps) {
    super(scope, id, props);

    const { pipelineConfig, artifactDir, deploymentInputs, allowPlaceholderAssets = false } = props;
    const names = pipelineConfig.resource_names;

    // EN: Build shared buckets, tables, queues, and vector indexes first so later compute resources can reuse them.
    // CN: 先构建共享的 bucket、table、queue 和 vector index，让后续计算资源直接复用。
    const foundation = createPipelineFoundation(this, {
      pipelineConfig,
    });

    // EN: Bind least-privilege execution roles to the already created shared resources.
    // CN: 把最小权限的执行角色绑定到已经创建好的共享资源上。
    const roles = createPipelineRoles({
      stack: this,
      names: pipelineConfig.resource_names,
      pipelineConfig,
      sourceBucket: foundation.sourceBucket,
      manifestBucket: foundation.manifestBucket,
      embedQueue: foundation.embedQueue,
      ingestQueue: foundation.ingestQueue,
      objectStateTable: foundation.objectStateTable,
      executionStateTable: foundation.executionStateTable,
      manifestIndexTable: foundation.manifestIndexTable,
      embeddingProjectionStateTable: foundation.embeddingProjectionStateTable,
    });

    // EN: Layer Lambda, state machine, and API resources on top of the shared foundation.
    // CN: 在共享基础设施之上叠加 Lambda、状态机和 API 资源。
    const compute = createPipelineCompute({
      stack: this,
      pipelineConfig,
      artifactDir,
      deploymentInputs,
      allowPlaceholderAssets,
      foundation,
      roles,
    });

    // EN: Publish only the outputs that deployment and smoke-test workflows actually consume.
    // CN: 只公开部署和冒烟测试工作流真正会消费的输出。
    new cdk.CfnOutput(this, 'RemoteMcpApiUrl', {
      value: compute.remoteMcpApi.url ?? '',
    });
    new cdk.CfnOutput(this, 'StateMachineArn', {
      value: compute.stateMachine.stateMachineArn,
    });
    new cdk.CfnOutput(this, 'VectorBucketName', {
      value: names.vector_bucket,
    });
    new cdk.CfnOutput(this, 'SourceBucketName', {
      value: foundation.sourceBucket.bucketName,
    });

    cdk.Tags.of(this).add('app', pipelineConfig.repo_name);
  }
}
