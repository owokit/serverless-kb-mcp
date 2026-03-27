import { Duration, RemovalPolicy } from 'aws-cdk-lib';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import * as s3vectors from 'aws-cdk-lib/aws-s3vectors';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import type { Construct } from 'constructs';
import type { EmbeddingProfileConfig, PipelineConfig } from '../config';
import { pascal } from './helpers';

export interface PipelineFoundationResources {
  sourceBucket: s3.Bucket;
  manifestBucket: s3.Bucket;
  vectorBucket: s3vectors.CfnVectorBucket;
  ingestQueue: sqs.Queue;
  embedDlq: sqs.Queue;
  embedQueue: sqs.Queue;
  objectStateTable: dynamodb.Table;
  executionStateTable: dynamodb.Table;
  manifestIndexTable: dynamodb.Table;
  embeddingProjectionStateTable: dynamodb.Table;
}

export interface PipelineFoundationParams {
  pipelineConfig: PipelineConfig;
}

// EN: Build the shared buckets, tables, queues, and vector bucket once so every later component reuses the same names.
// CN: 一次性构建共享的 bucket、table、queue 和 vector bucket，让后续组件复用同一套命名。
export function createPipelineFoundation(scope: Construct, params: PipelineFoundationParams): PipelineFoundationResources {
  const { pipelineConfig } = params;
  const names = pipelineConfig.resource_names;
  const defaultSettings = pipelineConfig.defaults;
  const enabledProfiles = pipelineConfig.embedding_profiles.filter((profile) => profile.enabled !== false);

  const sourceBucket = new s3.Bucket(scope, 'SourceBucket', {
    bucketName: names.source_bucket,
    blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
    encryption: s3.BucketEncryption.S3_MANAGED,
    versioned: true,
    removalPolicy: RemovalPolicy.DESTROY,
    autoDeleteObjects: true,
    lifecycleRules: [
      {
        id: 'expire-noncurrent-source-versions',
        transitions: [
          {
            storageClass: s3.StorageClass.INTELLIGENT_TIERING,
            transitionAfter: Duration.days(defaultSettings.source_intelligent_tiering_days),
          },
        ],
        noncurrentVersionExpiration: Duration.days(defaultSettings.source_noncurrent_days),
      },
    ],
  });

  const manifestBucket = new s3.Bucket(scope, 'ManifestBucket', {
    bucketName: names.manifest_bucket,
    blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
    encryption: s3.BucketEncryption.S3_MANAGED,
    versioned: true,
    removalPolicy: RemovalPolicy.DESTROY,
    autoDeleteObjects: true,
  });

  const vectorBucket = new s3vectors.CfnVectorBucket(scope, 'VectorBucket', {
    vectorBucketName: names.vector_bucket,
    tags: [{ key: 'app', value: pipelineConfig.repo_name }],
  });
  vectorBucket.applyRemovalPolicy(RemovalPolicy.DESTROY);

  for (const profile of enabledProfiles) {
    createVectorIndex(scope, profile, vectorBucket, names.vector_bucket);
  }

  const ingestQueue = new sqs.Queue(scope, 'IngestQueue', {
    queueName: names.ingest_queue,
    visibilityTimeout: Duration.seconds(180),
    retentionPeriod: Duration.days(4),
  });
  const embedDlq = new sqs.Queue(scope, 'EmbedDlq', {
    queueName: names.embed_dlq,
    visibilityTimeout: Duration.seconds(60),
    retentionPeriod: Duration.days(4),
  });
  const embedQueue = new sqs.Queue(scope, 'EmbedQueue', {
    queueName: names.embed_queue,
    visibilityTimeout: Duration.seconds(180),
    retentionPeriod: Duration.days(4),
    deadLetterQueue: {
      queue: embedDlq,
      maxReceiveCount: 5,
    },
  });

  sourceBucket.addEventNotification(
    s3.EventType.OBJECT_CREATED,
    new s3n.SqsDestination(ingestQueue),
  );
  sourceBucket.addEventNotification(
    s3.EventType.OBJECT_REMOVED,
    new s3n.SqsDestination(ingestQueue),
  );

  const objectStateTable = createTable(scope, names.object_state_table, true, [
    { name: 'pk', type: dynamodb.AttributeType.STRING },
    { name: 'record_type', type: dynamodb.AttributeType.STRING },
  ], [
    {
      indexName: 'lookup-record-type-index',
      partitionKey: { name: 'record_type', type: dynamodb.AttributeType.STRING },
    },
  ]);
  const executionStateTable = createTable(scope, names.execution_state_table, false, [
    { name: 'pk', type: dynamodb.AttributeType.STRING },
  ]);
  const manifestIndexTable = createTable(scope, names.manifest_index_table, false, [
    { name: 'pk', type: dynamodb.AttributeType.STRING },
    { name: 'sk', type: dynamodb.AttributeType.STRING },
  ]);
  const embeddingProjectionStateTable = createTable(scope, names.embedding_projection_state_table, false, [
    { name: 'pk', type: dynamodb.AttributeType.STRING },
    { name: 'sk', type: dynamodb.AttributeType.STRING },
  ]);

  return {
    sourceBucket,
    manifestBucket,
    vectorBucket,
    ingestQueue,
    embedDlq,
    embedQueue,
    objectStateTable,
    executionStateTable,
    manifestIndexTable,
    embeddingProjectionStateTable,
  };
}

// EN: Create one DynamoDB table with optional GSIs so the stack file does not need to repeat table boilerplate.
// CN: 创建一个可选带 GSI 的 DynamoDB table，避免 stack 文件重复 table 样板代码。
function createTable(
  scope: Construct,
  tableName: string,
  withGsi: boolean,
  attributes: { name: string; type: dynamodb.AttributeType }[],
  indexes: { indexName: string; partitionKey: { name: string; type: dynamodb.AttributeType } }[] = [],
): dynamodb.Table {
  const table = new dynamodb.Table(scope, pascal(tableName), {
    tableName,
    billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
    partitionKey: attributes[0],
    sortKey: attributes[1] ?? undefined,
    pointInTimeRecoverySpecification: {
      pointInTimeRecoveryEnabled: true,
    },
    encryption: dynamodb.TableEncryption.AWS_MANAGED,
    removalPolicy: RemovalPolicy.DESTROY,
  });
  if (withGsi) {
    for (const index of indexes) {
      table.addGlobalSecondaryIndex({
        indexName: index.indexName,
        partitionKey: index.partitionKey,
        projectionType: dynamodb.ProjectionType.ALL,
      });
    }
  }
  return table;
}

// EN: Keep each embedding profile on its own S3 Vectors index so vector spaces never mix.
// CN: 让每个 embedding profile 维持独立的 S3 Vectors index，避免向量空间混写。
function createVectorIndex(
  scope: Construct,
  profile: EmbeddingProfileConfig,
  vectorBucket: s3vectors.CfnVectorBucket,
  vectorBucketName: string,
): s3vectors.CfnIndex {
  const index = new s3vectors.CfnIndex(scope, `VectorIndex${pascal(profile.profile_id)}`, {
    dataType: 'float32',
    dimension: profile.dimension,
    distanceMetric: 'cosine',
    indexName: profile.vector_index_name,
    vectorBucketName,
    metadataConfiguration: profile.non_filterable_metadata_keys?.length
      ? {
          nonFilterableMetadataKeys: profile.non_filterable_metadata_keys,
        }
      : undefined,
  });
  index.addDependency(vectorBucket);
  index.applyRemovalPolicy(RemovalPolicy.DESTROY);
  return index;
}
