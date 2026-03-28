import * as cdk from 'aws-cdk-lib';
import { ApiStack } from '../lib/api-stack';
import { ComputeStack } from '../lib/compute-stack';
import { FoundationStack } from '../lib/foundation-stack';
import { loadPipelineConfig, resolveDeploymentInputs } from '../lib/config';

const app = new cdk.App();
const repoRoot = process.cwd();
const configPath =
  process.env.MCP_PIPELINE_CONFIG_PATH ??
  app.node.tryGetContext('configPath') ??
  'infra/pipeline-config.json';
const artifactDir =
  process.env.MCP_CDK_ASSET_DIR ??
  app.node.tryGetContext('artifactDir') ??
  'ocr-service/ocr-pipeline/dist';

const deploymentInputs = resolveDeploymentInputs(process.env);
const pipelineConfig = loadPipelineConfig(repoRoot, String(configPath));
const allowPlaceholderAssets = /^(1|true|yes)$/i.test(process.env.MCP_ALLOW_PLACEHOLDER_ASSETS ?? '');

const stackPrefix = pipelineConfig.name_prefix;
const stackEnvironment = {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT ?? process.env.AWS_ACCOUNT_ID,
    region: process.env.CDK_DEFAULT_REGION ?? process.env.AWS_REGION,
  },
};

const foundationStack = new FoundationStack(app, `${stackPrefix}-foundation`, {
  ...stackEnvironment,
  pipelineConfig,
});

const computeStack = new ComputeStack(app, `${stackPrefix}-compute`, {
  ...stackEnvironment,
  pipelineConfig,
  artifactDir: String(artifactDir),
  deploymentInputs,
  allowPlaceholderAssets,
});

const apiStack = new ApiStack(app, `${stackPrefix}-api`, {
  ...stackEnvironment,
  pipelineConfig,
});

computeStack.addDependency(foundationStack);
apiStack.addDependency(computeStack);

cdk.Tags.of(foundationStack).add('app', pipelineConfig.repo_name);
cdk.Tags.of(computeStack).add('app', pipelineConfig.repo_name);
cdk.Tags.of(apiStack).add('app', pipelineConfig.repo_name);
