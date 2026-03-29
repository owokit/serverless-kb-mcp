---
name: python-bilingual-comments
description: 适用于当前 Python 项目的模块 docstring、类 docstring、函数 docstring、方法 docstring、块注释和行内注释。当需要新增、补齐、重写或审查注释，并且要求采用“英文一行、中文一行、英文在前、中文在后”的双语格式时使用本 skill。
---

# Python 双语注释

为当前仓库编写或审查 Python 注释时，统一采用逐行对应的双语格式：先英文，再中文；两行表达同一语义，不合并到同一行。

## 强制格式

- 每个语义单元必须写成两行：`EN:` 一行，`CN:` 一行。
- 英文在前，中文在后，不要写成 `EN/CN` 同行混排。
- 中文不是英文摘要，必须与英文表达同一约束、前提和结果。
- 标识符、类型名、AWS 资源名和领域术语保持原样，不翻译 `version_id`、`object_state`、`manifest_index`、`Step Functions Standard`、`S3 Vectors`、`Gemini Embedding 2`。
- 注释解释“为什么、约束是什么、输入输出语义是什么”，不要解释显而易见的 Python 语法。
- 公共模块、公共类、公共函数和关键流程入口优先使用 docstring；复杂分支或关键状态推进使用块注释；简单行内注释只在不写就难以理解时添加。
- 如果第三方格式限制无法双行展开，至少保留中文，并在后续可控位置补齐双语说明。

## Docstring 模板

公共函数和方法默认使用 Python 三引号 docstring，参数区采用 `Args`、`Returns`、`Raises`。

```python
def start_ingest(record: S3EventRecord, state_repo: ObjectStateRepository) -> bool:
    """
    EN: Start the ingest workflow for a specific S3 object version.
    CN: 为指定的 S3 对象版本启动导入工作流。

    Args:
        record:
            EN: Parsed S3 event record including bucket, key, version_id, and sequencer.
            CN: 已解析的 S3 事件记录，包含 bucket、key、version_id 和 sequencer。
        state_repo:
            EN: Repository used to enforce idempotency and latest-version progression.
            CN: 用于执行幂等和最新版本推进的状态仓库。

    Returns:
        EN: True when the workflow start request is accepted by the ingress gate.
        CN: 当入口门控接受工作流启动请求时返回 True。

    Raises:
        EN: ValueError if version_id is missing from the normalized record.
        CN: 当规范化记录缺少 version_id 时抛出 ValueError。
    """
```

## 模块与类注释模板

模块和类注释只写职责、边界和关键约束，不重复实现细节。

```python
"""
EN: Coordinate the durable extract workflow from OCR submission to manifest persistence.
CN: 协调从 OCR 提交到 manifest 持久化的 durable 提取工作流。
"""


class EmbedDispatcher:
    """
    EN: Dispatch manifest chunks to Gemini Embedding 2 and persist vectors into S3 Vectors.
    CN: 将 manifest chunk 分发给 Gemini Embedding 2，并把向量写入 S3 Vectors。
    """
```

## 块注释与行内注释模板

复杂流程前使用块注释；不要在语句尾部堆砌长行注释。

```python
# EN: Persist previous_version_id before flipping latest_version_id.
# CN: 在切换 latest_version_id 之前先持久化 previous_version_id。
previous_version_id = current.latest_version_id

# EN: Reject stale events so an older S3 sequencer cannot overwrite the latest state.
# CN: 拒绝过期事件，避免更旧的 S3 sequencer 覆盖最新状态。
if is_stale_event(incoming_sequencer, current.sequencer):
    return False
```

## 当前项目推荐写法

- Ingest 入口注释要明确 `bucket/key/version_id/sequencer` 是最小身份单元。
- 版本治理注释要明确 `previous_version_id`、`latest_version_id` 和幂等拦截的先后关系。
- OCR 轮询注释要明确当前采用 `submit -> wait -> query -> fetch`，不要写成 webhook 或 callback。
- Embed 注释要明确普通 `S3 manifest` 与 `S3 Vectors` 的职责边界，不能互相替代。
- Query 相关注释要明确向量命中后仍需校验 `object_state.latest_version_id`。

## 反例

以下写法禁止使用：

```python
"""EN: Parse event / CN: 解析事件。"""
```

```python
# EN: Save manifest.
```

```python
# CN: 保存 manifest。
```

```python
# EN: Increment retry count.
# CN: 重试次数加一。
retry_count += 1
```

最后一个反例的问题不是双语格式，而是注释只在复述代码，没有提供额外语义。

## 审查清单

- 检查每段注释是否满足 `EN` 一行、`CN` 一行、语义一致。
- 检查是否遗漏 `Returns`、`Raises`、状态推进前提或失败条件。
- 检查是否把领域标识符翻译成自然语言，导致与代码脱节。
- 检查是否把显而易见的赋值、加减、遍历写成无信息量注释。
- 检查是否把仓库既有结论写错，例如把 `S3 manifest` 当成向量存储，或把 `PaddleOCR` 异步接口写成回调模型。
