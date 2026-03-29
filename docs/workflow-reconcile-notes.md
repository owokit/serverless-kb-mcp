# 工作流对齐说明

这份说明用于记录当前部署与销毁 workflow 的真实执行顺序，便于排障和后续调整。

## 当前流程

1. `package-release.yml` 在本地集成通过后打包 Lambda 和 Layer zip
2. `Prod Deploy` 调用 `scripts/prod-deploy.sh`，由脚本统一解析仓库根目录、读取 `infra/pipeline-config.json`、获取 release assets，并执行 `npm --prefix infra/cdk run deploy`
3. `Destroy` 在需要时执行 `npx cdk destroy --all --force --progress events`
4. `Destroy` 会启用占位资产模式，避免在没有 release 产物时无法 synth

## 对齐原则

- 部署入口统一使用 TypeScript CDK
- 生产部署入口统一使用 `scripts/prod-deploy.sh`，避免 workflow、submodule 和脚本之间重复计算路径
- 资源顺序由 CDK 依赖图控制，当前 app 由 Foundation / Compute / Api 三个顶层 stack 组成
- release 产物只负责提供 Lambda 和 Layer 代码，不负责表达基础设施顺序
- 销毁入口不再依赖 boto3 删除顺序，而是依赖同一份 CDK 定义

## 调整时需要同步的地方

- `.github/workflows/prod-deploy.yml`
- `.github/workflows/destroy.yml`
- `infra/cdk/bin/app.ts`
- `infra/cdk/lib/foundation-stack.ts`
- `infra/cdk/lib/compute-stack.ts`
- `infra/cdk/lib/api-stack.ts`
- `docs/README.md`
