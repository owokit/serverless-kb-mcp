# 开源库与自研边界说明

本文档说明本仓库哪些能力优先复用成熟开源库，哪些能力必须保留自研逻辑。

## 总体原则

- 通用能力优先复用成熟开源库。
- 业务治理、版本隔离、状态推进和查询融合必须自研。
- 任何会影响 `version_id`、`object_state`、`embedding profile`、`S3 Vectors index` 或查询融合语义的改动，不能直接交给第三方库替代。

## 适合复用开源库的模块

### 1. 文本分词与 token 估算

- 已使用 `tiktoken`。
- 作用：更接近真实 embedding 模型的 token 边界，减少字符比例估算带来的偏差。
- 保留字符启发式回退，确保部署环境没有 `tiktoken` 时仍可运行。

### 2. Markdown 解析

- 已使用 `markdown-it-py`。
- 作用：按标准 Markdown 语法识别标题边界、段落和块级结构，替代手写正则解析。
- 这样可以减少自定义标题切分逻辑，并降低结构识别错误概率。

### 3. Office 文档解析

- 已使用 `python-docx` 与 `python-pptx`。
- 作用：直接解析 `docx` 和 `pptx`，再复用统一的 Markdown 章节切分逻辑。
- 当前 extract layer 不再依赖 `markitdown`，以避免把 `magika`、`onnxruntime` 等重量级依赖拉入 Lambda layer。

### 4. PDF 解析

- 已使用 `pypdf`。
- 作用：提取 PDF 文本并按页生成 chunk。
- 若后续需要更强的版面还原能力，可以再评估更高层的开源抽取框架，但必须先验证输出仍满足本仓库的 chunk 和 asset 语义。

## 必须自研的模块

### 1. `version_id` 与对象主身份

- 文档主身份必须包含 `version_id`。
- 不能只按文件名、路径或最新对象覆盖来建模。

### 2. `ingest` 幂等与乱序治理

- `S3 Event Notification -> SQS ingest queue -> Ingest Lambda` 的入口治理必须保留。
- 这是版本推进、乱序拦截和重复事件消除的核心逻辑。

### 3. `object_state` / `projection_state` / `manifest_index`

- 状态推进、失败补偿、旧版本治理、profile 级投影状态都属于业务治理层。
- 这些表的写入语义不能交给通用库自动推断。

### 4. `embedding profile` 隔离

- 不同 provider、model、dimension 和向量空间的隔离策略必须自研。
- 不能把不同 profile 的向量混写到同一个 `S3 Vectors index`。

### 5. 查询融合与兜底校验

- 多 profile 的召回、融合排序、邻居回溯和 `latest_version_id` 兜底校验必须由业务代码控制。
- 不能直接依赖第三方索引库默认行为。

### 6. OCR 流程编排

- `PaddleOCR submit -> wait -> query -> fetch` 的轮询式异步流程属于业务工作流。
- 开源库可以提供 HTTP、JSON、token 和重试能力，但任务状态机和结果归档必须自研。

## 当前仓库的实际取向

- 文本解析层已经从 `markitdown` 迁移到 `python-docx` 和 `python-pptx`。
- 业务核心链路仍然保持自研。
- 这样做的目标是降低抽取实现复杂度，同时不牺牲 `version_id`、profile 隔离和查询闭环的治理能力。
