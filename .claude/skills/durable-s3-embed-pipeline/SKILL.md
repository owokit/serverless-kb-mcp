---
description: 用于设计、解释或审查本仓库默认的文档处理链路，覆盖 S3 Event Notification、SQS ingest queue、Ingest Lambda、Step Functions Standard、extract workflow lambdas、SQS embed queue 和 Embed Lambda。
name: durable-s3-embed-pipeline
---


# Durable S3 Embed Pipeline

## 适用场景

当任务涉及下列内容时使用本 skill：

- 默认文档处理链路
- Step Functions 提取编排
- OCR 轮询式异步流程
- 资源拆分与职责边界
- 失败治理与重试预算

## 默认链路

```text
S3 source bucket
-> S3 Event Notification
-> SQS ingest queue
-> Ingest Lambda
-> Step Functions Standard
-> extract workflow lambdas
-> manifest bucket
-> SQS embed queue
-> Embed Lambda
-> S3 Vectors
-> Query Service
```

## 外部暴露原则

- 对外只暴露 `Ingest Lambda`
- `extract workflow lambdas` 和 `Embed Lambda` 都是内部运行时资源
- 不要把 OCR 轮询、manifest 持久化和 embedding 长期合并成单个 Lambda

## 职责边界

### Ingest Lambda

- 接收 S3 事件
- 解析 `bucket/key/version_id/sequencer`
- 做幂等与乱序预检查
- 启动 Step Functions

### Step Functions Standard

- 编排 `prepare -> sync -> submit -> wait -> poll -> persist`
- 处理 OCR 轮询预算
- 拉取 OCR 结果并写 manifest
- 推进 `object_state` / `manifest_index`
- 投递 embedding job

### Embed Lambda

- 消费 embedding job
- 调用 embedding provider
- 写入 `S3 Vectors`
- 更新 `embedding_projection_state`
- 失败后进入 DLQ

## 关键约束

- `PaddleOCR` 只按 `submit -> poll -> fetch result` 建模
- 不要假设 webhook、callback token 或 event resume
- 轮询超时要短，提交超时要单独设置
- 向量桶和 vector index 必须按 profile 隔离
- 不同 embedding space 不允许混写
- `OpenAI Embedding` 的文本 chunk 默认应留出安全余量，避免单条请求接近模型输入上限导致 embed Lambda 读超时

## 设计输出

如果你在用这个 skill 产出方案，优先给出：

1. 资源拆分清单
2. 状态机时序图
3. 超时与重试预算
4. 失败处理和补偿路径
5. 与 deploy / destroy / test 的联动点
