---
description: 适用于先审视当前结构、在不保留旧项目兼容性的前提下做大规模重构，并在改写 Python 代码时同步应用双语注释与 docstring 规范。
name: architecture-reset-refactor
---

# 架构重置与 Python 双语注释

当任务满足以下任一条件时，使用本 skill：

- 需要先确定目标结构，再开始改代码
- 需要删除旧兼容层、转接层、别名层或一次性胶水代码
- 需要重排目录、迁移模块、合并职责边界或清理历史抽象
- 需要在 Python 文件中补齐、重写或审查模块、类、函数、方法 docstring，以及块注释和行内注释

## 核心原则

- 默认不考虑旧项目、旧代码、旧路径、旧 wrapper、旧 adapter 的兼容性
- 以当前最合理的结构为准，能删就删，能合就合，能移就移
- 不要为了照顾旧调用方式，故意保留坏边界、重复抽象或双轨结构
- 重构时要同步收敛测试、文档、脚本、入口和导出
- Python 注释与 docstring 统一使用双语：英文一行、中文一行、英文在前、中文在后

## 先做什么

1. 先输出目标结构
2. 再列出需要删除、迁移、合并、拆分的文件和模块
3. 同时说明测试、文档、脚本和入口的收敛范围
4. 再开始改代码

## 重构要求

- 先确定当前需求落在哪一层，不要一上来就补函数或塞新目录
- 识别重复抽象、错误归属和临时拼接点，优先消除它们
- 新逻辑放到真正拥有职责的模块，不要放在顺手能改的文件里
- 批量处理相关文件，不要把本该一次收敛的结构拆成多轮小修小补
- 旧测试如果只验证过时结构，就同步重写测试，不替旧行为背书
- 不保留“以后再删”的过渡代码，除非用户明确要求阶段性迁移

## Python 双语注释规则

- 每个语义单元必须写成两行：`EN:` 一行，`CN:` 一行
- 英文在前，中文在后，不要写成同一行混排
- 中文不是英文摘要，必须和英文保持同一约束、前提和结果
- 标识符、类型名、AWS 资源名和领域术语保持原样，不翻译
- 注释解释“为什么、约束是什么、输入输出语义是什么”，不要解释显而易见的 Python 语法
- 公共模块、公共类、公共函数和关键流程入口优先使用 docstring
- 复杂分支或关键状态推进使用块注释，简单行内注释只在不写就难以理解时添加
- 如果第三方格式限制无法双行展开，至少保留中文，并在后续可控位置补齐双语说明

## Python Docstring 模板

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

```python
"""
EN: Coordinate the durable extract workflow from OCR submission to manifest persistence.
CN: 协调从 OCR 提交到 manifest 持久化的 durable 提取工作流。
"""


class EmbedDispatcher:
    """
    EN: Dispatch manifest chunks to the embedding backend and persist vectors.
    CN: 将 manifest 分片派发到 embedding 后端并持久化向量。
    """
```

## 块注释模板

```python
# EN: Persist previous_version_id before flipping latest_version_id.
# CN: 在切换 latest_version_id 之前先持久化 previous_version_id。
previous_version_id = current.latest_version_id

# EN: Reject stale events so an older S3 sequencer cannot overwrite the latest state.
# CN: 拒绝过期事件，避免较旧的 S3 sequencer 覆盖最新状态。
if is_stale_event(incoming_sequencer, current.sequencer):
    return False
```

## 当前项目推荐写法

- Ingest 入口注释要明确 `bucket/key/version_id/sequencer` 是最小身份单元
- 版本治理注释要明确 `previous_version_id`、`latest_version_id` 和幂等拦截的先后关系
- OCR 轮询注释要明确当前采用 `submit -> wait -> query -> fetch`
- Embed 注释要明确 `S3 manifest` 与 `S3 Vectors` 的职责边界
- Query 相关注释要明确向量命中后仍然需要校验 `object_state.latest_version_id`

## 风险控制

- 不要误删当前任务范围之外的有效功能
- 任何大改都要配套仓库要求的全量验证；如果环境不够，要明确说明未验证项
- 如果重构改变了仓库规则、交付流程或 skill 边界，要同步更新 `AGENTS.md` 和相关 skill

## 交付口径

- 在结果说明里明确写出这次重构不保留旧兼容性
- 清楚列出删除、迁移和重命名的关键路径
- 说明新的主入口、模块边界和后续开发应遵循的结构
