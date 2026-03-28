# AWS CDK 部署与销毁说明

本文件说明当前 TypeScript CDK 入口的职责分工。部署和销毁都以 `infra/cdk/bin/app.ts` 里拆分后的三个顶层 stack 为准，不再依赖旧的 boto3 辅助面作为主入口。

## 部署入口

生产发布通过 GitHub Actions 的 `Prod Deploy` 工作流执行，底层命令如下：

```bash
npx cdk deploy --all --method direct --concurrency 3 --require-approval never --progress events
```

部署前会先下载 release assets，并将 `MCP_CDK_ASSET_DIR` 指向 `release-assets/`，这样 CDK 栈可以直接消费发布产物。
当前部署命令优先使用 `--method direct`，因为它比 change set 路径更直接；`--asset-parallelism` 与 `--method direct` 不兼容，所以这里不再同时开启。

## 销毁入口

销毁通过 GitHub Actions 的 `Destroy` 工作流执行，底层命令如下：

```bash
npx cdk destroy --all --force --progress events
```

销毁流程会启用 `MCP_ALLOW_PLACEHOLDER_ASSETS=true`，允许在没有真实 Lambda zip 的情况下完成 synth。这样 destroy 不再依赖打包产物，但仍然沿用同一份 CDK 定义。

## 配置优先级

1. GitHub Actions 输入参数
2. 仓库根目录的 `pipeline-config.json`
3. 环境变量和密钥

## 资源范围

- 资源命名仍由 `pipeline-config.json` 的 `resource_names` 统一控制
- 运行时参数仍由 `defaults` 和 `lambda_settings` 控制
- `embedding_profiles` 决定要创建哪些 `S3 Vectors` index

## 常见障碍

- 如果 `cdk synth` 找不到产物，先确认 `MCP_CDK_ASSET_DIR` 是否指向正确目录
- 如果 `cdk destroy` 失败但不涉及业务资源，先确认是否启用了占位资产模式
- 如果输入的 `name_prefix` 和配置不一致，工作流会直接失败，避免误删
- 如果需要调试部署性能，优先观察三个顶层 stack 的顺序和资源 teardown，而不是继续把重点放在 TypeScript 里做 Promise 并行
