import * as cdk from 'aws-cdk-lib';
import type { PipelineConfig } from '../config';

export interface PipelineResourceBindings {
  sourceBucketArn: string;
  sourceBucketObjectArn: string;
  manifestBucketArn: string;
  manifestBucketObjectArn: string;
  vectorBucketArn: string;
  vectorIndexArns: string[];
  ingestQueueArn: string;
  ingestQueueUrl: string;
  embedQueueArn: string;
  embedQueueUrl: string;
  objectStateTableArn: string;
  executionStateTableArn: string;
  manifestIndexTableArn: string;
  embeddingProjectionStateTableArn: string;
  stateMachineArn: string;
  cleanupStateMachineArn: string;
  ingestLambdaArn: string;
  embedLambdaArn: string;
  remoteMcpLambdaArn: string;
}

export function buildPipelineResourceBindings(scope: cdk.Stack, pipelineConfig: PipelineConfig): PipelineResourceBindings {
  const { resource_names: names, embedding_profiles } = pipelineConfig;
  const region = scope.region;
  const account = scope.account;
  const enabledProfiles = embedding_profiles.filter((profile) => profile.enabled !== false);

  return {
    sourceBucketArn: `arn:aws:s3:::${names.source_bucket}`,
    sourceBucketObjectArn: `arn:aws:s3:::${names.source_bucket}/*`,
    manifestBucketArn: `arn:aws:s3:::${names.manifest_bucket}`,
    manifestBucketObjectArn: `arn:aws:s3:::${names.manifest_bucket}/*`,
    vectorBucketArn: `arn:aws:s3vectors:${region}:${account}:bucket/${names.vector_bucket}`,
    vectorIndexArns: enabledProfiles.map(
      (profile) => `arn:aws:s3vectors:${region}:${account}:bucket/${profile.vector_bucket_name}/index/${profile.vector_index_name}`,
    ),
    ingestQueueArn: `arn:aws:sqs:${region}:${account}:${names.ingest_queue}`,
    ingestQueueUrl: `https://sqs.${region}.amazonaws.com/${account}/${names.ingest_queue}`,
    embedQueueArn: `arn:aws:sqs:${region}:${account}:${names.embed_queue}`,
    embedQueueUrl: `https://sqs.${region}.amazonaws.com/${account}/${names.embed_queue}`,
    objectStateTableArn: `arn:aws:dynamodb:${region}:${account}:table/${names.object_state_table}`,
    executionStateTableArn: `arn:aws:dynamodb:${region}:${account}:table/${names.execution_state_table}`,
    manifestIndexTableArn: `arn:aws:dynamodb:${region}:${account}:table/${names.manifest_index_table}`,
    embeddingProjectionStateTableArn: `arn:aws:dynamodb:${region}:${account}:table/${names.embedding_projection_state_table}`,
    stateMachineArn: `arn:aws:states:${region}:${account}:stateMachine:${names.state_machine}`,
    cleanupStateMachineArn: `arn:aws:states:${region}:${account}:stateMachine:${names.cleanup_state_machine}`,
    ingestLambdaArn: `arn:aws:lambda:${region}:${account}:function:${names.ingest_lambda}`,
    embedLambdaArn: `arn:aws:lambda:${region}:${account}:function:${names.embed_lambda}`,
    remoteMcpLambdaArn: `arn:aws:lambda:${region}:${account}:function:${names.remote_mcp_lambda}`,
  };
}
