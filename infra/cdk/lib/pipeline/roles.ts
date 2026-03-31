import * as cdk from 'aws-cdk-lib';
import * as iam from 'aws-cdk-lib/aws-iam';
import type { PipelineConfig } from '../config';
import { pascal } from './helpers';
import type { PipelineResourceBindings } from './bindings';

export type LambdaRoleKey = 'query' | 'status' | 'backfill' | 'ingest' | 'extract' | 'embed';

export interface LambdaRoleBundle {
  lambdaRoles: Map<LambdaRoleKey, iam.Role>;
  stateMachineRole: iam.Role;
}

export interface PipelineRoleParams {
  stack: cdk.Stack;
  names: PipelineConfig['resource_names'];
  bindings: PipelineResourceBindings;
}

const LAMBDA_ROLE_KEYS: LambdaRoleKey[] = ['query', 'status', 'backfill', 'ingest', 'extract', 'embed'];

// EN: Create one execution role per Lambda family so the policy surface stays narrow and easy to reason about.
// CN: 为每个 Lambda 家族创建独立执行角色，让权限面保持最小且更容易推理。
export function createPipelineRoles(params: PipelineRoleParams): LambdaRoleBundle {
  const lambdaRoles = new Map<LambdaRoleKey, iam.Role>();
  for (const roleKey of LAMBDA_ROLE_KEYS) {
    const roleName = roleKey === 'query' ? params.names.lambda_role : `${params.names.lambda_role}-${roleKey}`;
    if (roleName.length > 64) {
      throw new Error(
        `IAM role name "${roleName}" exceeds 64-char limit (${roleName.length} chars). ` +
        `Shorten "name_prefix" or "lambda_role" in pipeline-config.json, or use a custom shorter "name_suffix".`,
      );
    }
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
  const vectorResources = params.bindings.vectorIndexArns;
  const statements: iam.PolicyStatement[] = [];

  if (roleKey === 'query') {
    statements.push(
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:BatchGetItem', 'dynamodb:DescribeTable'],
        resources: [params.bindings.objectStateTableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:BatchGetItem', 'dynamodb:DescribeTable'],
        resources: [params.bindings.executionStateTableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:Query', 'dynamodb:DescribeTable'],
        resources: [params.bindings.manifestIndexTableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:BatchGetItem', 'dynamodb:Query', 'dynamodb:DescribeTable'],
        resources: [params.bindings.embeddingProjectionStateTableArn],
      }),
      new iam.PolicyStatement({
        actions: ['s3:GetObject', 's3:GetObjectVersion'],
        resources: [params.bindings.manifestBucketObjectArn],
      }),
      new iam.PolicyStatement({
        actions: ['s3vectors:QueryVectors', 's3vectors:GetVectors'],
        resources: vectorResources,
      }),
    );
  } else if (roleKey === 'status') {
    statements.push(
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:BatchGetItem', 'dynamodb:DescribeTable'],
        resources: [params.bindings.objectStateTableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:BatchGetItem', 'dynamodb:DescribeTable'],
        resources: [params.bindings.executionStateTableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:Query', 'dynamodb:DescribeTable'],
        resources: [params.bindings.manifestIndexTableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:BatchGetItem', 'dynamodb:Query', 'dynamodb:DescribeTable'],
        resources: [params.bindings.embeddingProjectionStateTableArn],
      }),
      new iam.PolicyStatement({
        actions: ['s3:GetObject', 's3:GetObjectVersion'],
        resources: [params.bindings.manifestBucketObjectArn],
      }),
    );
  } else if (roleKey === 'backfill') {
    statements.push(
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:Query', 'dynamodb:Scan', 'dynamodb:DescribeTable'],
        resources: [params.bindings.objectStateTableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:DescribeTable'],
        resources: [params.bindings.executionStateTableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:Query', 'dynamodb:DescribeTable'],
        resources: [params.bindings.manifestIndexTableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:Query', 'dynamodb:DescribeTable'],
        resources: [params.bindings.embeddingProjectionStateTableArn],
      }),
      new iam.PolicyStatement({
        actions: ['s3:GetObject', 's3:GetObjectVersion'],
        resources: [params.bindings.manifestBucketObjectArn],
      }),
      new iam.PolicyStatement({
        actions: ['sqs:SendMessage'],
        resources: [params.bindings.embedQueueArn],
      }),
    );
  } else if (roleKey === 'ingest') {
    statements.push(
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:TransactWriteItems', 'dynamodb:UpdateItem', 'dynamodb:DescribeTable'],
        resources: [params.bindings.objectStateTableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:DescribeTable'],
        resources: [params.bindings.executionStateTableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:Query', 'dynamodb:DescribeTable'],
        resources: [params.bindings.manifestIndexTableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:PutItem', 'dynamodb:DescribeTable'],
        resources: [params.bindings.embeddingProjectionStateTableArn],
      }),
      new iam.PolicyStatement({
        actions: ['s3:GetObject', 's3:GetObjectVersion', 's3:GetObjectTagging', 's3:GetObjectVersionTagging'],
        resources: [params.bindings.sourceBucketObjectArn],
      }),
      new iam.PolicyStatement({
        actions: ['s3:GetObject', 's3:GetObjectVersion'],
        resources: [params.bindings.manifestBucketObjectArn],
      }),
      new iam.PolicyStatement({
        actions: ['s3vectors:GetVectors', 's3vectors:PutVectors'],
        resources: vectorResources,
      }),
      new iam.PolicyStatement({
        actions: ['sqs:ReceiveMessage', 'sqs:DeleteMessage', 'sqs:GetQueueAttributes', 'sqs:ChangeMessageVisibility'],
        resources: [params.bindings.ingestQueueArn],
      }),
      new iam.PolicyStatement({
        actions: ['states:StartExecution'],
        resources: [params.bindings.stateMachineArn],
      }),
    );
  } else if (roleKey === 'extract') {
    statements.push(
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:BatchWriteItem', 'dynamodb:DeleteItem', 'dynamodb:Query', 'dynamodb:TransactWriteItems', 'dynamodb:UpdateItem', 'dynamodb:DescribeTable'],
        resources: [params.bindings.objectStateTableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:DescribeTable'],
        resources: [params.bindings.executionStateTableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:BatchWriteItem', 'dynamodb:DeleteItem', 'dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:Query', 'dynamodb:DescribeTable'],
        resources: [params.bindings.manifestIndexTableArn],
      }),
      new iam.PolicyStatement({
        actions: ['s3:GetObject', 's3:GetObjectVersion', 's3:GetObjectTagging', 's3:GetObjectVersionTagging'],
        resources: [params.bindings.sourceBucketObjectArn],
      }),
      new iam.PolicyStatement({
        actions: ['s3:GetObject', 's3:GetObjectVersion', 's3:PutObject', 's3:DeleteObject'],
        resources: [params.bindings.manifestBucketObjectArn],
      }),
      new iam.PolicyStatement({
        actions: ['sqs:SendMessage'],
        resources: [params.bindings.embedQueueArn],
      }),
    );
  } else if (roleKey === 'embed') {
    statements.push(
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:BatchWriteItem', 'dynamodb:DeleteItem', 'dynamodb:Query', 'dynamodb:TransactWriteItems', 'dynamodb:UpdateItem', 'dynamodb:DescribeTable'],
        resources: [params.bindings.objectStateTableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:DescribeTable'],
        resources: [params.bindings.executionStateTableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:BatchWriteItem', 'dynamodb:DeleteItem', 'dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:Query', 'dynamodb:DescribeTable'],
        resources: [params.bindings.manifestIndexTableArn],
      }),
      new iam.PolicyStatement({
        actions: ['dynamodb:BatchWriteItem', 'dynamodb:DeleteItem', 'dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:Query', 'dynamodb:DescribeTable'],
        resources: [params.bindings.embeddingProjectionStateTableArn],
      }),
      new iam.PolicyStatement({
        actions: ['s3:GetObject', 's3:GetObjectVersion', 's3:DeleteObject'],
        resources: [params.bindings.manifestBucketObjectArn],
      }),
      new iam.PolicyStatement({
        actions: ['s3vectors:GetVectors', 's3vectors:PutVectors', 's3vectors:DeleteVectors'],
        resources: vectorResources,
      }),
      new iam.PolicyStatement({
        actions: ['sqs:ReceiveMessage', 'sqs:DeleteMessage', 'sqs:GetQueueAttributes', 'sqs:ChangeMessageVisibility'],
        resources: [params.bindings.embedQueueArn],
      }),
    );
  }

  for (const statement of statements) {
    role.addToPrincipalPolicy(statement);
  }
}
