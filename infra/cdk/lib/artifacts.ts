import * as path from 'node:path';

export const LAMBDA_FUNCTION_KEYS = [
  'ingest',
  'extract_prepare',
  'extract_sync',
  'extract_submit',
  'extract_poll',
  'extract_persist',
  'extract_mark_failed',
  'embed',
  'remote_mcp',
  'backfill',
  'job_status',
] as const;

export const LAYER_KEYS = ['core', 'extract', 'embedding'] as const;

export type LambdaFunctionKey = (typeof LAMBDA_FUNCTION_KEYS)[number];
export type LayerKey = (typeof LAYER_KEYS)[number];

export function buildLambdaZipName(repoName: string, functionKey: LambdaFunctionKey): string {
  return `${repoName}_${functionKey}.zip`;
}

export function buildLayerZipName(repoName: string, layerKey: LayerKey): string {
  return `${repoName}_${layerKey}_layer.zip`;
}

export function buildLambdaZipPath(artifactDir: string, repoName: string, functionKey: LambdaFunctionKey): string {
  return path.join(artifactDir, buildLambdaZipName(repoName, functionKey));
}

export function buildLayerZipPath(artifactDir: string, repoName: string, layerKey: LayerKey): string {
  return path.join(artifactDir, 'layers', buildLayerZipName(repoName, layerKey));
}
