# AWS CDK 销毁说明

本文件说明当前 `Destroy` 工作流的行为。销毁入口已经切换到 `cdk destroy`，不再依赖旧的 boto3 销毁脚本作为主路径。

## 当前工作流

- GitHub Actions 销毁工作流：[`destroy.yml`](../.github/workflows/destroy.yml)
- 触发方式：`workflow_dispatch`
- 运行前必须显式确认 `name_prefix`
- 运行前还会校验 `name_prefix` 与 `pipeline-config.json` 保持一致

## 销毁行为

- 继续沿用同一份 `pipeline-config.json` 作为资源命名依据
- 资源删除交给 `cdk destroy --all --force`
- 当没有真实打包产物时，CDK 栈会使用占位资产完成 synth
- 销毁只做栈级清理，不再手动维护旧的 boto3 删除顺序

## 手动执行

GitHub Actions 的销毁 workflow 支持 `workflow_dispatch`，常用输入如下：

- `name_prefix`
- `region`
- `confirm_destroy`

其中 `confirm_destroy` 必须与 `name_prefix` 完全一致，工作流才会继续执行。
如果需要销毁自定义向量索引，请先更新 `pipeline-config.json`，再执行销毁。

## 安全建议

- 销毁 workflow 只允许在受保护环境里触发
- 不要把销毁命令放进默认 PR 门禁
- 如果需要销毁多 profile 资源，先确认仓库配置和当前部署配置一致
