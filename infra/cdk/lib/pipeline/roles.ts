import * as cdk from 'aws-cdk-lib';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import type { PipelineConfig } from '../config';
import { pascal } from './helpers';

export type LambdaRoleKey = 'query' | 'status' | 'backfill' | 'ingest' | 'extract' | 'embed';

export interface LambdaRoleBundle {
  lambdaRoles: Map<LambdaRoleKey, iam.Role>;
  stateMachineRole: iam.Role;
}

export interface PipelineRoleParams {
  stack: cdk.Stack;
  names: PipelineConfig['resource_names'];
  pipelineConfig: PipelineConfig;
  sourceBucket: s3.Bucket;
  manifestBucket: s3.Bucket;
  embedQueue: sqs.Queue;
  ingestQueue: sqs.Queue;
  objectStateTable: dynamodb.Table;
  executionStateTable: dynamodb.Table;
  manifestIndexTable: dynamodb.Table;
  embeddingProjectionStateTable: dynamodb.Table;
}

const LAMBDA_ROLE_KEYS: LambdaRoleKey[] = ['query', 'status', 'backfill', 'ingest', 'extract', 'embed'];

// EN: Create one execution role per Lambda family so the policy surface stays narrow and easy to reason about.
// CN: 为每个 Lambda 家族创建独立执行角色，让权限面保持最小且更容易推理。
export function createPipelineRoles(params: PipelineRoleParams): LambdaRoleBundle {
  const lambdaRoles = new Map<LambdaRoleKey, iam.Role>();
  for (const roleKey of LAMBDA_ROLE_KEYS) {
    const roleName = roleKey === 'query' ? params.names.lambda_role : `${params.names.lambda_role}-${roleKey}`;
    const role = new iam.Role(params.stack, `LambdaRole${pascal(roleKey)}`, {
      roleName,
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      description: `Execution role for the document pipeline Lambda ${roleKey} family`,
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AWSXRayDaemonWriteAccess'),
      ],
    });
    attachLambdaDataPlanePolicy(role, roleKey, params);
    lambdaRoles.set(roleKey, role);
  }

  const stateMachineRole = new iam.Role(params.stack, 'StateMachineRole', {
    roleName: params.names.state_machine_role,
    assumedBy: new iam.ServicePrincipal('states.amazonaws.com'),
    description: 'Execution role for the extract Step Functions state machine',
  });

  return { lambdaRoles, stateMachineRole };
}

// EN: Keep data-plane permissions role-specific so each Lambda only gets the queue and table access it actually needs.
// CN: 把数据平面权限限定到具体角色，确保每个 Lambda 只拿到自己真正需要的 queue 和 table 访问。
function attachLambdaDataPlanePolicy(
  role: iam.Role,
  roleKey: LambdaRoleKey,
  params: PipelineRoleParams,
): void {
  const profiles = params.pipelineConfig.embedding_profiles.filter((profile) => profile.enabled !== false);
  const vectorResources = profiles.map(
    (profile) => `arn:aws:s3vectors:${params.stack.region}:${params.stack.account}:bucket/${profile.vector_bucket_name}/index/${profile.vector_index_name}`,
  );
  const statements: iam.PolicyStatement[] = [];

  if (roleKey === 'query') {
    statements.push(
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:DescribeTable'],
        resources: [params.objectStateTable.tableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:DescribeTable'],
        resources: [params.executionStateTable.tableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:Query', 'dynamodb:DescribeTable'],
        resources: [params.manifestIndexTable.tableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:Query', 'dynamodb:DescribeTable'],
        resources: [params.embeddingProjectionStateTable.tableArn],
      }),
      new iam.PolicyStatement({
        actions: ['s3:GetObject', 's3:GetObjectVersion'],
        resources: [params.manifestBucket.arnForObjects('*')],
      }),
      new iam.PolicyStatement({
        actions: ['s3vectors:QueryVectors', 's3vectors:GetVectors'],
        resources: vectorResources,
      }),
    );
  } else if (roleKey === 'status') {
    statements.push(
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:DescribeTable'],
        resources: [params.objectStateTable.tableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:DescribeTable'],
        resources: [params.executionStateTable.tableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:Query', 'dynamodb:DescribeTable'],
        resources: [params.manifestIndexTable.tableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:Query', 'dynamodb:DescribeTable'],
        resources: [params.embeddingProjectionStateTable.tableArn],
      }),
      new iam.PolicyStatement({
        actions: ['s3:GetObject', 's3:GetObjectVersion'],
        resources: [params.manifestBucket.arnForObjects('*')],
      }),
    );
  } else if (roleKey === 'backfill') {
    statements.push(
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:Query', 'dynamodb:Scan', 'dynamodb:DescribeTable'],
        resources: [params.objectStateTable.tableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:DescribeTable'],
        resources: [params.executionStateTable.tableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:Query', 'dynamodb:DescribeTable'],
        resources: [params.manifestIndexTable.tableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:Query', 'dynamodb:DescribeTable'],
        resources: [params.embeddingProjectionStateTable.tableArn],
      }),
      new iam.PolicyStatement({
        actions: ['s3:GetObject', 's3:GetObjectVersion'],
        resources: [params.manifestBucket.arnForObjects('*')],
      }),
      new iam.PolicyStatement({
        actions: ['sqs:SendMessage'],
        resources: [params.embedQueue.queueArn],
      }),
    );
  } else if (roleKey === 'ingest') {
    statements.push(
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:TransactWriteItems', 'dynamodb:UpdateItem', 'dynamodb:DescribeTable'],
        resources: [params.objectStateTable.tableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:DescribeTable'],
        resources: [params.executionStateTable.tableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:Query', 'dynamodb:DescribeTable'],
        resources: [params.manifestIndexTable.tableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:PutItem', 'dynamodb:DescribeTable'],
        resources: [params.embeddingProjectionStateTable.tableArn],
      }),
      new iam.PolicyStatement({
        actions: ['s3:GetObject', 's3:GetObjectVersion', 's3:GetObjectTagging', 's3:GetObjectVersionTagging'],
        resources: [params.sourceBucket.arnForObjects('*')],
      }),
      new iam.PolicyStatement({
        actions: ['s3:GetObject', 's3:GetObjectVersion'],
        resources: [params.manifestBucket.arnForObjects('*')],
      }),
      new iam.PolicyStatement({
        actions: ['s3vectors:GetVectors', 's3vectors:PutVectors'],
        resources: vectorResources,
      }),
      new iam.PolicyStatement({
        actions: ['sqs:ReceiveMessage', 'sqs:DeleteMessage', 'sqs:GetQueueAttributes', 'sqs:ChangeMessageVisibility'],
        resources: [params.ingestQueue.queueArn],
      }),
      new iam.PolicyStatement({
        actions: ['states:StartExecution'],
        resources: [`arn:aws:states:${params.stack.region}:${params.stack.account}:stateMachine:${params.names.state_machine}`],
      }),
    );
  } else if (roleKey === 'extract') {
    statements.push(
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:BatchWriteItem', 'dynamodb:DeleteItem', 'dynamodb:Query', 'dynamodb:TransactWriteItems', 'dynamodb:UpdateItem', 'dynamodb:DescribeTable'],
        resources: [params.objectStateTable.tableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:DescribeTable'],
        resources: [params.executionStateTable.tableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:BatchWriteItem', 'dynamodb:DeleteItem', 'dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:Query', 'dynamodb:DescribeTable'],
        resources: [params.manifestIndexTable.tableArn],
      }),
      new iam.PolicyStatement({
        actions: ['s3:GetObject', 's3:GetObjectVersion', 's3:GetObjectTagging', 's3:GetObjectVersionTagging'],
        resources: [params.sourceBucket.arnForObjects('*')],
      }),
      new iam.PolicyStatement({
        actions: ['s3:GetObject', 's3:GetObjectVersion', 's3:PutObject', 's3:DeleteObject'],
        resources: [params.manifestBucket.arnForObjects('*')],
      }),
      new iam.PolicyStatement({
        actions: ['sqs:SendMessage'],
        resources: [params.embedQueue.queueArn],
      }),
    );
  } else if (roleKey === 'embed') {
    statements.push(
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:BatchWriteItem', 'dynamodb:DeleteItem', 'dynamodb:Query', 'dynamodb:TransactWriteItems', 'dynamodb:UpdateItem', 'dynamodb:DescribeTable'],
        resources: [params.objectStateTable.tableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:DescribeTable'],
        resources: [params.executionStateTable.tableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:BatchWriteItem', 'dynamodb:DeleteItem', 'dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:Query', 'dynamodb:DescribeTable'],
        resources: [params.manifestIndexTable.tableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:BatchWriteItem', 'dynamodb:DeleteItem', 'dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:Query', 'dynamodb:DescribeTable'],
        resources: [params.embeddingProjectionStateTable.tableArn],
      }),
      new iam.PolicyStatement({
        actions: ['s3:GetObject', 's3:GetObjectVersion', 's3:DeleteObject'],
        resources: [params.manifestBucket.arnForObjects('*')],
      }),
      new iam.PolicyStatement({
        actions: ['s3vectors:GetVectors', 's3vectors:PutVectors', 's3vectors:DeleteVectors'],
        resources: vectorResources,
      }),
      new iam.PolicyStatement({
        actions: ['sqs:ReceiveMessage', 'sqs:DeleteMessage', 'sqs:GetQueueAttributes', 'sqs:ChangeMessageVisibility'],
        resources: [params.embedQueue.queueArn],
      }),
    );
  }

  for (const statement of statements) {
    role.addToPrincipalPolicy(statement);
  }
}
