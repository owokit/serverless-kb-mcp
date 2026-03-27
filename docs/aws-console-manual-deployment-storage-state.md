## 1. 资源清单总表

下面这张表列出当前部署要创建的主要资源。

| 资源类型 | 推荐名称 | 作用 |
| --- | --- | --- |
| S3 bucket | `mcp-doc-pipeline-prod-s3-source` | 源文档桶 |
| S3 bucket | `mcp-doc-pipeline-prod-s3-manifest` | manifest、切片资产、回放材料 |
| S3 vector bucket | `mcp-doc-pipeline-prod-vectors` | 向量桶 |
| SQS queue | `mcp-doc-pipeline-prod-sqs-ingest` | 入口队列 |
| SQS queue | `mcp-doc-pipeline-prod-sqs-embed` | 向量写入队列 |
| SQS queue | `mcp-doc-pipeline-prod-sqs-embed-dlq` | 向量写入死信队列 |
| DynamoDB table | `mcp-doc-pipeline-prod-ddb-object-state` | 对象主状态 |
| DynamoDB table | `mcp-doc-pipeline-prod-ddb-manifest-index` | manifest 级索引 |
| DynamoDB table | `mcp-doc-pipeline-prod-ddb-embedding-projection-state` | 按 profile 的 projection 状态 |
| Step Functions state machine | `mcp-doc-pipeline-prod-sfn-extract` | OCR / manifest / embed 编排 |
| Lambda function | `mcp-doc-pipeline-prod-lambda-ingest` | 对外入口 |
| Lambda function | `mcp-doc-pipeline-prod-lambda-extract-prepare` | 提取准备 |
| Lambda function | `mcp-doc-pipeline-prod-lambda-extract-sync` | 非 PDF 同步抽取 |
| Lambda function | `mcp-doc-pipeline-prod-lambda-extract-submit` | OCR 提交 |
| Lambda function | `mcp-doc-pipeline-prod-lambda-extract-poll` | OCR 轮询 |
| Lambda function | `mcp-doc-pipeline-prod-lambda-extract-persist` | OCR 结果持久化 |
| Lambda function | `mcp-doc-pipeline-prod-lambda-embed` | 向量写入 |
| Lambda function | `mcp-doc-pipeline-prod-lambda-remote-mcp` | 远程 MCP 入口 |
| Lambda function | `mcp-doc-pipeline-prod-lambda-embedding-backfill` | 历史回填 |

---

## 2. 创建 S3 Bucket

### 2.1 创建 `source bucket`

1. 进入 `Amazon S3` 控制台。
2. 点击 `Create bucket`。
3. 填写：
   - `Bucket name`: `mcp-doc-pipeline-prod-s3-source`
   - `AWS Region`: 选择目标 Region
   - `Object Ownership`: 保持默认 `Bucket owner enforced`
   - `Block Public Access settings for this bucket`: 保持四项全开
   - `Default encryption`: 选择 `SSE-S3`
   - `Tags`: 可选
4. 点击 `Create bucket`。
5. 创建完成后，立刻进入 `Properties`，打开 `Bucket Versioning`。

`source bucket` 必须开版本控制，因为同名文件覆盖的治理全靠版本号来支撑。

### 2.2 创建 `manifest bucket`

1. 再创建一个 bucket。
2. 名称填写：
   - `mcp-doc-pipeline-prod-s3-manifest`
3. 其他配置和 `source bucket` 一致：
   - `Bucket owner enforced`
   - 四项 Block Public Access 全开
   - `SSE-S3`
4. 这个 bucket 主要用来放：
   - `manifest.json`
   - `raw.jsonl`
   - `document.md`
   - `assets/*`
   - 切片资产
   - 回放材料

`manifest bucket` 里的图片和 Markdown 内部链接都会改写成相对本地路径，例如 `assets/asset-000001.png`，方便后续回放、排障和二次处理。

如果你只有 `manifest bucket` 里的路径，不要把它当成 source file 的直接反向索引。真正的关联关系在两处：

- `manifest.json.source` 里保存了 `tenant_id`、`bucket`、`key`、`version_id`
- `object_state.latest_manifest_s3_uri` 保存了 source version 对应的 manifest 入口

所以从 source 找 manifest，走的是确定性 hash + `manifest.json`；从 manifest 找 source，走的是 manifest 内容本身，不是目录名。

`manifest bucket` 不要求靠版本控制做主治理。历史版本不是主清理机制，真正的版本治理仍然围绕 `source bucket` 和业务写入逻辑。

### 2.3 配置 `source bucket` 的版本生命周期与智能分层

`source bucket` 的当前版本文件上传后 `1` 天进入 `S3 Intelligent-Tiering`，旧版本清理仍然只靠生命周期规则，不要让业务代码逐条删除历史 `S3 version`。

#### 目标

- 只删除旧版本
- 永远保留最新版本
- 不影响当前可见对象

#### 控制台路径

1. 打开 `Amazon S3`
2. 进入 `mcp-doc-pipeline-prod-s3-source`
3. 打开 `Properties`
4. 确认 `Bucket Versioning` 已启用
5. 找到 `Lifecycle rules`
6. 点击 `Create lifecycle rule`

#### 推荐填写

- `Lifecycle rule name`：`expire-noncurrent-source-versions`
- `Rule scope`：`Apply to all objects in the bucket`
- `Lifecycle rule actions`：同时配置两项
  - 当前版本转储：`Transition current versions of objects between storage classes`
  - 删除非当前版本：勾选 `Permanently delete noncurrent versions of objects`
- `Transition days`：填 `1`
- `Storage class`：选 `S3 Intelligent-Tiering`
- `Noncurrent days`：建议先填 `1` 或 `7`

#### 这条规则的含义

- 新上传的 `ABCD.docs` 会成为当前版本
- 当前版本会在上传后第 `1` 天进入 `S3 Intelligent-Tiering`
- 旧版本会自动变成 `noncurrent`
- 到达 `Noncurrent days` 指定天数后，S3 才会自动清理旧版本

#### 重要提醒

- S3 Lifecycle 不是实时生效，通常会有延迟
- 如果你想立刻删除某个旧版本，要走单独的手工或业务清理流程
- 如果 bucket 没有开启 versioning，这条规则无法达到“保留最新、删除旧版”的目的

---

## 3. 创建 SQS Queue

你需要三个标准队列：

- `mcp-doc-pipeline-prod-sqs-ingest`
- `mcp-doc-pipeline-prod-sqs-embed`
- `mcp-doc-pipeline-prod-sqs-embed-dlq`

### 3.1 创建 DLQ

先创建 `mcp-doc-pipeline-prod-sqs-embed-dlq`。

建议配置：

- 类型：`Standard`
- `Visibility timeout`：先用默认或保持简单
- 其他选项：先用默认值

点击 `Create queue`。

### 3.2 创建 ingest queue

再创建 `mcp-doc-pipeline-prod-sqs-ingest`。

建议配置：

- 类型：`Standard`
- `Visibility timeout`：180 秒
- 其他选项：先用默认值

点击 `Create queue`。

### 3.3 创建 embed queue

再创建 `mcp-doc-pipeline-prod-sqs-embed`。

建议配置：

- 类型：`Standard`
- `Visibility timeout`：180 秒
- 绑定死信队列：`mcp-doc-pipeline-prod-sqs-embed-dlq`
- `Maximum receives`：`5`

点击 `Create queue`。

### 3.4 绑定 DLQ

1. 打开 `mcp-doc-pipeline-prod-sqs-embed`。
2. 进入 `Edit`。
3. 找到 `Dead-letter queue`。
4. 选择 `mcp-doc-pipeline-prod-sqs-embed-dlq`。
5. `Maximum receives` 填 `5`。
6. 保存。

### 3.5 队列用途

| 队列 | 类型 | Visibility timeout | DLQ | 用途 |
| --- | --- | --- | --- | --- |
| `mcp-doc-pipeline-prod-sqs-ingest` | Standard | 180 秒 | 不需要 | 只承接 S3 事件 |
| `mcp-doc-pipeline-prod-sqs-embed` | Standard | 180 秒 | 绑定 `embed-dlq` | 承接向量写入任务 |
| `mcp-doc-pipeline-prod-sqs-embed-dlq` | Standard | 默认即可 | 不适用 | 保存失败消息 |

---

## 4. 创建 DynamoDB Table

当前默认保留三张表。

### 4.1 为什么不是压成一张表

这三张表的职责边界不同，建议不要硬压成一张表：

- `object_state` 负责对象主状态、版本推进、删除治理、最新版本校验。
- `manifest_index` 负责 chunk / asset 的版本级反查索引。
- `embedding_projection_state` 负责按 profile 记录写入和查询就绪状态。

如果压成一张通用表，后面会出现三个问题：

1. 版本推进和向量写入的状态会互相污染。
2. 旧版本清理时很难做到只删该删的记录。
3. 多 profile 并存时，查询就绪状态会变得很难维护。

所以当前建议是：**三张表分开建，职责分开写，清理边界也分开走。**

### 4.2 `object_state` 表结构

推荐表名：

`mcp-doc-pipeline-prod-ddb-object-state`

控制台建议：

- `Partition key`: `pk`
- `Sort key`: 不填
- `Table settings`: `On-demand`
- `Encryption`: 保持默认开启
- `Point-in-time recovery`: 建议开启
- `TTL`: 先不要配
- `Global secondary indexes`: 增加 `lookup-record-type-index`
- `lookup-record-type-index` 的 `Partition key`: `record_type`
- `Projection`: 选 `ALL`
- `Streams`: 先不要配，除非你要额外做审计或事件联动

#### Item 类型

同一张表里会有两类 item：

| item 类型 | `pk` 示例 | 作用 |
| --- | --- | --- |
| `object_state` | `tenant#bucket#key` | 对象主状态 |
| `lookup` | `lookup-v2#bucket#key` | 用 `bucket/key` 反查主状态 |

#### 主状态 item 设计

主状态 item 建议至少保留下面这些字段：

| 字段 | 作用 |
| --- | --- |
| `pk` | 主键，格式为 `tenant#bucket#key` |
| `item_type` | 固定为 `object_state` |
| `bucket` | 源 bucket 名称 |
| `key` | 源对象 key |
| `current_version_id` | 当前处理中的版本 |
| `latest_version_id` | 当前最新版本 |
| `latest_sequencer` | S3 事件乱序治理用 |
| `ingest_status` | 入口状态 |
| `extract_status` | 提取状态 |
| `embed_status` | 向量写入状态 |
| `previous_version_id` | 上一版本 ID，用于清理旧版 |
| `previous_manifest_s3_uri` | 上一版本 manifest 的首选删除入口 |
| `latest_manifest_s3_uri` | 当前最新 manifest |
| `deleted_at` | 删除时间，便于治理 |
| `last_error` | 最近一次失败原因 |

#### 常用字段

| 字段 | 作用 |
| --- | --- |
| `latest_version_id` | 当前最新版本 |
| `previous_version_id` | 上一版本 ID，用于清理旧版 |
| `previous_manifest_s3_uri` | 上一版本 manifest 的首选删除入口 |
| `latest_manifest_s3_uri` | 当前最新 manifest |
| `latest_sequencer` | S3 事件乱序治理用 |
| `ingest_status` | 入口状态 |
| `extract_status` | 提取状态 |
| `embed_status` | 向量写入状态 |
| `deleted_at` | 删除时间 |
| `last_error` | 最近一次失败原因 |

#### lookup item 常用字段

| 字段 | 作用 |
| --- | --- |
| `object_pk` | 主状态 item 的主键 |
| `bucket` | 源 bucket 名称 |
| `key` | 源对象 key |
| `latest_version_id` | 最新版本 |
| `latest_sequencer` | 最新事件序号 |
| `latest_manifest_s3_uri` | 最新 manifest |

`lookup` item 的 `pk` 使用独立命名空间，避免和 `tenant#bucket#key` 形式的主状态主键撞车；不要再按旧的 `lookup#bucket#key` 示例去手工造新数据。

回填和重放不要再扫描整张 `object_state` 表；应直接查询 `lookup-record-type-index`，再按 `object_pk` 做去重和排序。

#### 4.2.1 复合主键迁移说明

如果你是从旧版本升级上来，`object_state`、`manifest_index` 和 `embedding_projection_state` 里已经存在的记录，可能还是按未转义的 composite key 写入的。

在启用现在这版对 `tenant_id`、`bucket`、`key` 和 `version_id` 的百分号转义之前，先把这些旧记录重建、重放，或者用迁移脚本重新写入一遍。

如果跳过这一步，后续对包含 `/`、`#` 或 `%` 的对象重新导入或 backfill 时，DynamoDB 会按新主键写出第二份记录，旧记录会变成孤儿数据。

### 4.3 `manifest_index` 表结构

推荐表名：

`mcp-doc-pipeline-prod-ddb-manifest-index`

控制台建议：

- `Partition key`: `pk`
- `Sort key`: `sk`
- `Table settings`: `On-demand`
- `Encryption`: 保持默认开启
- `Point-in-time recovery`: 建议开启
- `TTL`: 先不要配
- `Global secondary indexes`: 先不要配
- `Streams`: 先不要配

#### 常见设计

| 字段 | 作用 |
| --- | --- |
| `pk` | `object_pk#version_id` |
| `sk` | `chunk#...` 或 `asset#...` |
| `bucket` | 源 bucket |
| `key` | 源对象 key |
| `version_id` | 版本号 |
| `is_latest` | 是否最新版本 |
| `page_no` / `slide_no` | 页码或页号 |
| `chunk_id` | chunk 标识 |
| `manifest_s3_uri` | manifest 定位 |
| `is_latest` | 是否最新版本 |
| `content_kind` | 内容类型，例如文本、图片、幻灯片 |
| `parent_chunk_id` | 如果有层级 chunk，可记录父子关系 |
| `neighbor_ids` | 可选的邻接索引 |

#### 主键建议

- `pk` 统一按 `object_pk#version_id` 组织。
- `sk` 统一按 `chunk#...`、`asset#...` 或 `page#...` 组织。

这样做的好处是：

1. 删除旧版本时可以按 `previous_version_id` 精确删除整组记录。
2. 查单个版本时，不需要额外的 GSI。
3. chunk 和 asset 可以共用同一张表，但仍然保持条目层次清晰。

### 4.4 `embedding_projection_state` 表结构

这张表是可选项，但当前仓库在多 profile 场景下推荐保留。

推荐表名：

`mcp-doc-pipeline-prod-ddb-embedding-projection-state`

控制台建议：

- `Partition key`: `pk`
- `Sort key`: `sk`
- `Table settings`: `On-demand`
- `Encryption`: 保持默认开启
- `Point-in-time recovery`: 建议开启
- `TTL`: 先不要配
- `Global secondary indexes`: 先不要配
- `Streams`: 先不要配

#### 设计目的

这张表只负责两件事：

1. 记录某个 `object_pk#version_id` 在某个 `profile_id` 下是否已经写入成功。
2. 记录这个 profile 的向量写入、查询就绪、失败和重试状态。

#### 常见设计

| 字段 | 作用 |
| --- | --- |
| `pk` | `object_pk#version_id` |
| `sk` | `profile_id` |
| `provider` | embedding provider |
| `model` | 具体模型 |
| `write_status` | 写入状态 |
| `query_status` | 查询就绪状态 |
| `vector_bucket_name` | 向量桶 |
| `vector_index_name` | 向量索引 |
| `vector_count` | 已写向量数 |
| `last_error` | 最近错误 |
| `updated_at` | 最近更新时间 |
| `deleted_at` | 旧版本清理时间 |

#### 主键建议

- `pk` 统一按 `object_pk#version_id` 组织。
- `sk` 统一按 `profile_id` 组织。

这样写的好处是：

1. 单个文档版本可以同时对应多个 embedding profile。
2. 每个 profile 的写入进度互不干扰。
3. 旧版本清理时可以直接删整组 profile 状态，不会误删别的版本。

### 4.5 三张表的保留建议

| 方案 | 建议 | 说明 |
| --- | --- | --- |
| 3 张表 | 推荐 | 当前默认，多 profile 时最清晰 |
| 2 张表 | 勉强可行 | 仅单写入 profile 且不需要独立 projection 追踪时可考虑 |
| 1 张表 | 不建议 | 维护复杂，清理边界不清楚 |

### 4.6 创建完成后检查什么

三张表都创建完以后，建议逐项确认：

1. 表名是否和本教程一致。
2. `Partition key` 和 `Sort key` 是否完全按上面的设计。
3. `Billing mode` 是否都是 `On-demand`。
4. `Point-in-time recovery` 是否已打开。
5. 是否没有误加 `GSI`、`TTL` 或 `Streams`。
6. 是否给每张表都留了对应的环境变量名，方便 Lambda 配置。

### 4.7 对应环境变量名

建议你在 Lambda 环境变量里至少保留这些名字：

- `OBJECT_STATE_TABLE_NAME`
- `MANIFEST_INDEX_TABLE_NAME`
- `EMBEDDING_PROJECTION_STATE_TABLE_NAME`

这样后面改表名时，代码不需要跟着改，只要改环境变量即可。

---
