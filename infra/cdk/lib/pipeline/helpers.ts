import * as crypto from 'node:crypto';
import * as fs from 'node:fs';
import * as path from 'node:path';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import type { LambdaFunctionKey } from '../artifacts';
import type { PipelineConfig } from '../config';

// EN: Keep stack-local helper logic out of the main stack file so the orchestration stays readable.
// CN: 将栈内辅助逻辑从主 stack 文件中拆出去，让编排层保持可读。

export function defaultRuntimeSettings(defaults: PipelineConfig['defaults']): { memory_size: number; timeout_seconds: number } {
  return {
    memory_size: defaults.lambda_memory_size,
    timeout_seconds: defaults.lambda_timeout_seconds,
  };
}

export function pascal(value: string): string {
  return value
    .split(/[^a-zA-Z0-9]/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join('');
}

// EN: Stage a valid empty zip when synth is allowed to proceed without packaged Lambda assets.
// CN: 当 synth 允许在没有已打包 Lambda 产物时继续时，生成一个合法的空 zip。
export function resolveAssetPath(assetPath: string, allowPlaceholderAssets: boolean): string {
  if (fs.existsSync(assetPath)) {
    return assetPath;
  }
  if (!allowPlaceholderAssets) {
    throw new Error(`Missing Lambda asset: ${assetPath}`);
  }

  const placeholderDir = path.resolve(process.cwd(), '.cdk-placeholder-assets');
  const placeholderName = `${crypto.createHash('sha256').update(assetPath).digest('hex')}-${path.basename(assetPath)}`;
  const placeholderPath = path.join(placeholderDir, placeholderName);
  if (!fs.existsSync(placeholderPath)) {
    fs.mkdirSync(placeholderDir, { recursive: true });
    const emptyZip = Buffer.from('504b0506000000000000000000000000000000000000', 'hex');
    fs.writeFileSync(placeholderPath, emptyZip);
  }
  return placeholderPath;
}

// EN: Render the extract workflow definition by replacing placeholder Lambda ARNs with the real stack outputs.
// CN: 通过把占位 Lambda ARN 替换成真实栈输出，渲染 extract workflow 定义。
export function renderStateMachineDefinition(lambdaFunctions: Map<LambdaFunctionKey, lambda.Function>): string {
  const templatePath = path.resolve(process.cwd(), '../../ocr-service/ocr-pipeline/src/serverless_mcp/workflows/extract_state_machine.asl.json');
  let template = fs.readFileSync(templatePath, 'utf8');
  const placeholders: Array<[string, LambdaFunctionKey]> = [
    ['${PREPARE_LAMBDA_ARN}', 'extract_prepare'],
    ['${SYNC_LAMBDA_ARN}', 'extract_sync'],
    ['${SUBMIT_LAMBDA_ARN}', 'extract_submit'],
    ['${POLL_LAMBDA_ARN}', 'extract_poll'],
    ['${PERSIST_LAMBDA_ARN}', 'extract_persist'],
    ['${MARK_FAILED_LAMBDA_ARN}', 'extract_mark_failed'],
  ];
  for (const [placeholder, key] of placeholders) {
    const fn = lambdaFunctions.get(key);
    if (!fn) {
      throw new Error(`Missing lambda function for state machine placeholder: ${key}`);
    }
    template = template.replaceAll(placeholder, fn.functionArn);
  }
  JSON.parse(template);
  return template;
}
