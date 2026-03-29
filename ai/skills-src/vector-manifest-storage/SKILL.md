---
name: vector-manifest-storage
description: 适用于 OpenAI Embedding / Azure OpenAI、S3 Vectors、普通 S3 manifest、DynamoDB 状态与索引设计。
---

# 向量与 Manifest 存储

当任务涉及 embedding 产出、`S3 Vectors` metadata、`manifest.json` / `raw.jsonl` / `document.md` / `assets/*`、向量反查或状态索引时，使用本 skill。

## 先分清三层存储

```text
源文件 S3
-> OCR / 提取
-> 普通 S3 manifest
-> Embedding provider
-> S3 Vectors
```

- 源文件 `S3`：保存用户上传的原始文件。
- 普通 `S3 manifest`：保存结构化提取结果、派生资产、原始 OCR sidecar 和可回放材料，图片和 Markdown 链接优先改写为本地相对路径。
- `S3 Vectors`：保存 embedding 后的向量和检索 metadata。

## 强制规则

- 向量 metadata 必须包含 `tenant_id`、`bucket`、`key`、`version_id`、`chunk_id`、`manifest_s3_uri`
- 多模型并存时，必须先抽象成 `embedding profile`
- `embedding profile` 至少绑定 `profile_id`、`provider`、`model`、`dimension`、`supported_content_kinds`、`vector_bucket`、`vector_index`、query/write 开关
- 同一个 `S3 Vectors index` 只允许承载同一 `embedding profile` 的向量，不允许混存不同 provider、model、dimension 或 embedding space
- 向量 metadata 除文档身份字段外，还必须包含 `profile_id`、`provider`、`model`、`dimension`
- `manifest_index` 必须按 `version_id` 隔离边界
- `OpenAI Embedding` 文本输入默认要控制在模型输入上限以下，实际拆块建议留安全余量，不要把单条文本块放大到接近模型 token 上限
- 新版本 manifest 持久化成功后，上一个版本 `manifest_index.is_latest` 必须回写为 `false`
- 新版本向量写入成功后，上一个版本 `S3 Vectors.is_latest` 必须回写为 `false`
- manifest/index 写入失败时必须补偿，不能留下半成功状态
- 旧版本清理优先复用 `previous_version_id` / `previous_manifest_s3_uri`，并且必须在最接近旧版本写入层的地方做；缺失记录一律按幂等 `no-op` 处理
- `source bucket` 的历史对象版本只走 S3 Lifecycle，不在本 skill 里设计源文件物理删除逻辑

## 推荐实现口径

- `Step Functions` 提取阶段先写 `manifest.json`、`raw.jsonl`、`document.md` 和 `assets/*`，再写 `manifest_index`
- 多 profile 模式下，推荐按“每个文档版本 × 每个 profile 一条消息”投递 embed job
- 文本 profile 与多模态 profile 可以共享同一份 manifest，但不能共享同一个向量索引
- 旧版本向量治理仍依赖上一个版本 manifest 精确推导旧向量 key
- Query 层仍要再用 `object_state` 做最新版本过滤，作为治理兜底而不是唯一手段
- 如果需要使用 DynamoDB 二级索引，只能补充访问路径，不能取代 `object_state`、`manifest_index`、`embedding_projection_state` 的分层职责

## 推荐职责分层

- `S3 Vectors`：相似度召回
- `object_state`：幂等、对象版本推进、主状态，并保留旧版本清理输入
- `embedding_projection_state` 或等价状态层：按 `version_id + profile_id` 跟踪写入就绪、查询就绪、失败和补偿；单写入 profile 时可以省略，多 profile 时不要并回 `object_state`
- `manifest_index`：chunk 定位与版本级反查，不建议靠 GSI 取代；不要把 previewer / text_preview / neighbors 之类的展示内容写进索引表
- `普通 S3 manifest`：完整结构化结果和大 payload 资产

## 禁止事项

- 不要把 `S3 Vectors` 当成完整 manifest 主存储
- 不要因为源文件已经在 `S3` 里就删除普通 `S3 manifest`
- 不要写入缺少 `version_id` 的向量或索引记录
- 不要把不同 embedding profile 的向量写进同一个 `S3 Vectors index`
- 不要把 provider 切换、model 升级或 dimension 变更当成简单环境变量变更
