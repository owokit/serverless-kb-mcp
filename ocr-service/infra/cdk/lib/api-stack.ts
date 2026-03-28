import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import type { PipelineConfig } from './config';
import { buildPipelineResourceBindings } from './pipeline/bindings';
import { createPipelineApi } from './pipeline/api';

export interface ApiStackProps extends cdk.StackProps {
  pipelineConfig: PipelineConfig;
}

// EN: Keep the public REST API in its own stack so endpoint type and gateway lifecycle do not drag the compute layer with them.
// CN: 将对外 REST API 单独拆成一个栈，避免入口类型和网关生命周期牵连计算层。
export class ApiStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: ApiStackProps) {
    super(scope, id, props);

    const { pipelineConfig } = props;
    const bindings = buildPipelineResourceBindings(this, pipelineConfig);
    const api = createPipelineApi({
      stack: this,
      pipelineConfig,
      bindings,
    });

    new cdk.CfnOutput(this, 'RemoteMcpApiUrl', {
      value: api.remoteMcpApi.url ?? '',
    });

    cdk.Tags.of(this).add('app', pipelineConfig.repo_name);
  }
}
