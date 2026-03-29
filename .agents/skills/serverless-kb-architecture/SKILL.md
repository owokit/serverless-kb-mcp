---
description: 适用于本仓库整体 serverless 文档检索系统的架构设计、边界收敛、重构、测试和文档同步，覆盖 ingestion、query、storage、infra 和 skill 之间的协同。
name: serverless-kb-architecture
---


# 仓库整体架构

当任务涉及本仓库的整体代码架构、模块边界、运行时职责、目录落位、跨层重构、测试策略或文档同步时，优先使用这个 skill。

## 适用范围

- `ocr-service/ocr-pipeline/src/serverless_mcp/` 下的所有业务模块
- `ocr-service/infra/` 的资源拓扑和部署命名
- `docs/` 中关于运行时、部署、验证、排障和交付流程的文档
- `ai/skills-src/` 中与本仓库架构相关的其他 skill

## 当前整体架构

```text
source bucket
-> S3 Event Notification
-> SQS ingest queue
-> Ingest Lambda
-> Step Functions Standard
   -> extract workflow lambdas
   -> manifest bucket
   -> SQS embed queue
-> Embed Lambda
-> Embedding provider
-> S3 Vectors
-> SQS DLQ
-> Query Service / MCP Query Lambda
-> metadata store / manifest store / object storage
```

本仓库不是单体应用。摄取链路、查询链路、存储层、部署层和 skill 层都应分别收敛，不能因为某一处入口改动就把职责重新压回一个大 Lambda 或一个大目录。

## 核心边界

### 摄取链路

- 负责 S3 版本事件、入队、OCR 编排、Step Functions 状态推进、manifest 生成和 embedding 投递。
- 对象主身份必须围绕 `version_id` 管理，不要只按路径或文件名建模。
- `object_state` 负责主状态推进，`manifest_index` 负责版本级 chunk 反查，`embedding_projection_state` 负责按 profile 隔离的投影状态。

### 查询链路

- 负责 query-time 的检索、摘要、版本查询和状态查询。
- 远程 MCP 入口只承载查询侧能力，不承载 OCR、Step Functions 或 embedding worker。
- 工具应该对应业务能力，不应该暴露 embedding、vector、worker 或底层协议实现细节。

### 存储层

- `manifest bucket` 用于保存结构化提取结果、切片资产和可回放材料。
- `S3 Vectors` 只承载同一 embedding profile 的向量，不混写不同模型、维度或 provider。
- `DynamoDB` 用于幂等、版本隔离、状态推进、失败补偿和回查，不要把所有职责压成一张通用表。

### 基础设施

- `infra/` 只表达资源拓扑、命名、环境变量和部署入口。
- 任何资源、权限、流程或命名变化，都必须回写到 `docs/` 和相关 skill。
- 默认验证不能依赖真实云账号，优先使用本地仿真、service containers 和固定样本。

## 推荐配套 skill

这个 skill 管的是总纲。具体子域任务应切到对应 skill：

- `versioned-s3-ingest`：S3 versioning、事件、幂等、object_state、版本推进
- `durable-s3-embed-pipeline`：OCR、Step Functions、抽取编排、embed worker
- `vector-manifest-storage`：manifest、projection、S3 Vectors、状态存储
- `remote-mcp-query-gateway`：远程 MCP 查询入口和协议层收敛
- `project-delivery-guardrails`：跨模块改动前的架构梳理和落位
- `architecture-reset-refactor`：需要彻底重排目录和删除旧抽象时

如果一个任务同时涉及多个子域，先用这个 skill 理清总边界，再按子域 skill 分段处理，不要把所有规则混在一个答复里。

## 实施顺序

1. 先判断需求属于摄取、查询、存储、infra 还是文档层。
2. 盘点现有模块职责和依赖方向，找出重复抽象和错误落位。
3. 先给出目标目录和文件边界，再开始改代码。
4. 相关测试、文档、skill 和部署说明一起收敛。
5. 修改 Python 代码后，必须跑仓库要求的全量测试。
6. 修改 skill 后，必须运行 `python ai/scripts/sync-ai.py`，同步到 `.agents/skills/` 和 `.claude/skills/`。

## 不要做的事

- 不要把查询入口、摄取编排和 embedding worker 合并成一个大入口。
- 不要把协议层、业务层和存储层写在同一个模块里。
- 不要为了兼容旧结构，长期保留明显多余的 wrapper、转发层和别名层。
- 不要把本地替代实现当成云服务语义完全等价的实现。
- 不要为了看起来统一，牺牲清晰的职责边界。

## 验证要求

- 先验证结构，再验证实现。
- 修改协议、入口或边界时，补齐生命周期测试和回归测试。
- 修改 infra 或流程时，同步更新文档和相关 skill。
- 如果需要提交 PR，正文前部必须写明 `Closes #123` / `Fixes #123` / 对应跨仓库引用，并按模板完整填写。

## 备注

这个 skill 的目标是让人一眼看清仓库当前整体架构，而不是只记住某一个入口或某一个子系统。
