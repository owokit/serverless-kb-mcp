// @ts-nocheck

declare const require: any;
declare const __dirname: string;

const assert = require('node:assert/strict');
const childProcess = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const cdk = require('aws-cdk-lib');
const { ApiStack } = require('../lib/api-stack');
const { ComputeStack } = require('../lib/compute-stack');
const { FoundationStack } = require('../lib/foundation-stack');
const { applyNameSuffix, loadPipelineConfig } = require('../lib/config');

function buildAssembly(account: string, region: string) {
  const app = new cdk.App();
  const repoRoot = path.resolve(__dirname, '../../..');
  const pipelineConfig = loadPipelineConfig(repoRoot, 'infra/pipeline-config.json');
  applyNameSuffix(pipelineConfig, account, region);

  const stackEnvironment = {
    env: {
      account,
      region,
    },
  };

  const foundationStack = new FoundationStack(app, `${pipelineConfig.name_prefix}-foundation`, {
    ...stackEnvironment,
    pipelineConfig,
  });
  const computeStack = new ComputeStack(app, `${pipelineConfig.name_prefix}-compute`, {
    ...stackEnvironment,
    pipelineConfig,
    artifactDir: 'ocr-service/ocr-pipeline/dist',
    deploymentInputs: {},
    allowPlaceholderAssets: true,
  });
  const apiStack = new ApiStack(app, `${pipelineConfig.name_prefix}-api`, {
    ...stackEnvironment,
    pipelineConfig,
  });

  (computeStack as any).addDependency(foundationStack);
  (apiStack as any).addDependency(computeStack);

  const assembly = app.synth();
  return { assembly, foundationStack, computeStack, apiStack, pipelineConfig };
}

test('app synth fails fast when CDK_DEFAULT_ACCOUNT and CDK_DEFAULT_REGION are missing', () => {
  const repoRoot = path.resolve(__dirname, '../../..');
  const appPath = path.join(__dirname, '../bin/app.ts');
  const result = childProcess.spawnSync(
    process.execPath,
    ['-r', 'ts-node/register/transpile-only', appPath],
    {
      cwd: path.join(repoRoot, 'infra/cdk'),
      env: {
        ...process.env,
        CDK_DEFAULT_ACCOUNT: '',
        CDK_DEFAULT_REGION: '',
        AWS_ACCOUNT_ID: '',
        AWS_REGION: '',
        MCP_ALLOW_PLACEHOLDER_ASSETS: 'true',
      },
      encoding: 'utf8',
    },
  );

  assert.notEqual(result.status, 0);
  assert.match(`${result.stderr}${result.stdout}`, /CDK_DEFAULT_ACCOUNT and CDK_DEFAULT_REGION must be set/);
});

test('app synth emits the public stack outputs and stable stack artifact names', () => {
  const { assembly, foundationStack, computeStack, apiStack, pipelineConfig } = buildAssembly('111111111111', 'us-east-1');

  const foundationArtifact: any = assembly.getStackArtifact((foundationStack as any).artifactId);
  const computeArtifact: any = assembly.getStackArtifact((computeStack as any).artifactId);
  const apiArtifact: any = assembly.getStackArtifact((apiStack as any).artifactId);

  const foundationTemplate = JSON.parse(fs.readFileSync(path.join(assembly.directory, foundationArtifact.templateFile), 'utf8'));
  const computeTemplate = JSON.parse(fs.readFileSync(path.join(assembly.directory, computeArtifact.templateFile), 'utf8'));
  const apiTemplate = JSON.parse(fs.readFileSync(path.join(assembly.directory, apiArtifact.templateFile), 'utf8'));

  assert.equal(foundationTemplate.Outputs.VectorBucketName.Value, pipelineConfig.resource_names.vector_bucket);
  assert.ok(foundationTemplate.Outputs.SourceBucketName.Value.Ref);
  assert.ok(foundationTemplate.Outputs.ManifestBucketName.Value.Ref);
  assert.ok(computeTemplate.Outputs.StateMachineArn.Value);
  assert.ok(computeTemplate.Outputs.CleanupStateMachineArn.Value);
  assert.ok(computeTemplate.Outputs.RemoteMcpLambdaArn.Value);
  assert.ok(apiTemplate.Outputs.RemoteMcpApiUrl.Value);

  assert.deepEqual(
    computeArtifact.dependencies.map((dependency: any) => dependency.id),
    [foundationArtifact.id],
  );
  assert.deepEqual(
    apiArtifact.dependencies.map((dependency: any) => dependency.id),
    [computeArtifact.id],
  );
});

export {};
