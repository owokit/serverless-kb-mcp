import * as cdk from 'aws-cdk-lib';
import { loadPipelineConfig, resolveDeploymentInputs } from '../lib/config';
import { PipelineStack } from '../lib/pipeline-stack';

const app = new cdk.App();
const repoRoot = process.cwd();
const configPath =
  process.env.MCP_PIPELINE_CONFIG_PATH ??
  app.node.tryGetContext('configPath') ??
  'pipeline-config.json';
const artifactDir =
  process.env.MCP_CDK_ASSET_DIR ??
  app.node.tryGetContext('artifactDir') ??
  'services/ocr-pipeline/dist';

const deploymentInputs = resolveDeploymentInputs(process.env);
const pipelineConfig = loadPipelineConfig(repoRoot, String(configPath));
const allowPlaceholderAssets = /^(1|true|yes)$/i.test(process.env.MCP_ALLOW_PLACEHOLDER_ASSETS ?? '');

const stack = new PipelineStack(app, pipelineConfig.name_prefix, {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT ?? process.env.AWS_ACCOUNT_ID,
    region: process.env.CDK_DEFAULT_REGION ?? process.env.AWS_REGION,
  },
  pipelineConfig,
  artifactDir: String(artifactDir),
  deploymentInputs,
  allowPlaceholderAssets,
});

cdk.Tags.of(stack).add('app', pipelineConfig.repo_name);
