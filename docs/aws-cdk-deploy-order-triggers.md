# AWS CDK 部署顺序与触发器说明

本文件对应当前 TypeScript CDK 栈。资源顺序由 `cdk synth` 决定，`cdk deploy` 会按照依赖图自动处理创建顺序，不再依赖固定的 boto3 脚本顺序。

## 部署顺序

当前栈的核心顺序如下：

1. `S3` 源桶、清单桶和 `S3 Vectors` 资源
2. `SQS` ingest 队列、embed 队列和 DLQ
3. `DynamoDB` 状态表和投影表
4. `Lambda Layer` 和所有 `Lambda function`
5. `SQS -> Lambda` 事件映射
6. `Step Functions Standard`
7. `API Gateway REST API`

## 触发器规则

- `source bucket` 继续通过 `S3 Event Notification` 投递到 `SQS ingest queue`
- `ingest lambda` 再启动 `Step Functions Standard`
- `embed lambda` 继续通过 `SQS embed queue` 消费任务
- `Step Functions` 负责串联 `extract` 阶段的多个 Lambda，并把结果写回清单桶和状态表

## 为什么这样拆

- 让 `S3`、`SQS`、`DynamoDB`、`Lambda` 和 `Step Functions` 保持清晰的职责边界
- 让部署流程更贴近 CDK 的依赖图，而不是一段固定的脚本顺序
- 让 release 产物和 destroy 逻辑可以复用同一份 CDK 定义
