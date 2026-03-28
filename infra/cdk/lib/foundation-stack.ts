import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import type { PipelineConfig } from './config';
import { buildPipelineResourceBindings } from './pipeline/bindings';
import { createPipelineFoundation } from './pipeline/foundation';

export interface FoundationStackProps extends cdk.StackProps {
  pipelineConfig: PipelineConfig;
}

// EN: Keep the shared data plane in a dedicated top-level stack so destroy can tear it down separately from compute.
// CN: 将共享数据平面放入独立的顶层 stack，方便销毁时与计算层分开处理。
export class FoundationStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: FoundationStackProps) {
    super(scope, id, props);

    const { pipelineConfig } = props;
    const names = pipelineConfig.resource_names;
    const bindings = buildPipelineResourceBindings(this, pipelineConfig);
    const foundation = createPipelineFoundation(this, {
      pipelineConfig,
    });

    new cdk.CfnOutput(this, 'SourceBucketName', {
      value: foundation.sourceBucket.bucketName,
    });
    new cdk.CfnOutput(this, 'ManifestBucketName', {
      value: foundation.manifestBucket.bucketName,
    });
    new cdk.CfnOutput(this, 'VectorBucketName', {
      value: names.vector_bucket,
    });
    new cdk.CfnOutput(this, 'IngestQueueArn', {
      value: bindings.ingestQueueArn,
    });
    new cdk.CfnOutput(this, 'EmbedQueueArn', {
      value: bindings.embedQueueArn,
    });

    cdk.Tags.of(this).add('app', pipelineConfig.repo_name);
  }
}
