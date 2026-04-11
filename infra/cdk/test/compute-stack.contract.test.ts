// @ts-nocheck

declare const require: any;
declare const __dirname: string;

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const cdk = require('aws-cdk-lib');
const { ComputeStack } = require('../lib/compute-stack');
const { FoundationStack } = require('../lib/foundation-stack');
const { applyNameSuffix, loadPipelineConfig } = require('../lib/config');

function synthesizeComputeStack() {
  const app = new cdk.App();
  const repoRoot = path.resolve(__dirname, '../../..');
  const pipelineConfig = loadPipelineConfig(repoRoot, 'infra/pipeline-config.json');
  applyNameSuffix(pipelineConfig, '111111111111', 'us-east-1');

  const stackEnvironment = {
    env: {
      account: '111111111111',
      region: 'us-east-1',
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
  (computeStack as any).addDependency(foundationStack);

  const assembly = app.synth();
  const artifact: any = assembly.getStackArtifact((computeStack as any).artifactId);
  const template = JSON.parse(fs.readFileSync(path.join(assembly.directory, artifact.templateFile), 'utf8'));
  return { artifact, template };
}

test('compute stack exposes only the contract outputs needed by deploy and smoke jobs', () => {
  const { artifact, template } = synthesizeComputeStack();

  assert.deepEqual(Object.keys(template.Outputs).sort(), ['CleanupStateMachineArn', 'RemoteMcpLambdaArn', 'StateMachineArn']);
  assert.ok(template.Outputs.StateMachineArn.Value);
  assert.ok(template.Outputs.CleanupStateMachineArn.Value);
  assert.ok(template.Outputs.RemoteMcpLambdaArn.Value);
  assert.equal(artifact.dependencies.length, 1);
});

export {};
