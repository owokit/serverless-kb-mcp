---
name: versioned-s3-ingest
description: 适用于 S3 Versioning、S3 Event Notification、SQS ingest queue、对象版本身份、幂等、object_state 和入口治理。
---

# Versioned S3 Ingest

## 适用范围

当任务涉及以下内容时使用本 skill：

- S3 版本化对象入口
- 事件去重和乱序治理
- object_state 推进
- 旧版本处理
- Ingest Lambda 入口边界

## 默认入口

```text
source bucket
-> S3 Event Notification
-> SQS ingest queue
-> Ingest Lambda
-> Step Functions Standard
```

## 强制规则

- `S3 Versioning` 必开
- 所有下游记录都必须带 `version_id`
- `sequencer` 必须保留并参与乱序拦截
- 同一个 `version_id` 的重复事件必须幂等
- `Ingest Lambda` 只负责启动流程，不负责 OCR 或 embedding

## object_state 约定

- `start_processing()` 之类的入口必须返回或保存 `previous_version_id`
- 后续状态推进必须绑定当前 `latest_version_id`
- `previous_manifest_s3_uri` 是旧 manifest 清理的首选定位
- 找不到旧记录时，清理逻辑一律按幂等 `no-op` 处理

## 资源边界

- `SQS ingest queue` 只负责接收 S3 事件
- 不要把它当成版本主状态存储
- 源文件历史版本只靠 S3 Lifecycle 删除
- 不要在应用层硬删 source bucket 的历史对象版本

## 与提取链路的关系

- `Ingest Lambda` 启动 Step Functions
- Step Functions 再去驱动 extract workflow lambdas
- 该 skill 不负责 OCR 细节，只负责入口和版本治理

