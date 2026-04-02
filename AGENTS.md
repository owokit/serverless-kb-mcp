# AGENTS.md instructions for G:\GitProject\serverless-kb-mcp

<INSTRUCTIONS>
任何问题先拉最新的main分支到本地，使用worktree和github issue的skills，使用gh创建问题/提交PR等（简体中文描述），创建worktree后要拉取最新的main分支、git sub modules到本地。问题描述如下：

--- project-doc ---

# AGENTS.md

本仓库把主要项目规则拆分到独立 skill，`AGENTS.md` 只保留总纲、结论和导航。

## 起手必读

- 任何会修改代码、配置、文档、workflow、脚本或 skill 的任务，第一步必须先加载并使用 `github-issue-workflow` 和 `mainline-worktree`。
- 开始前必须先执行 `git fetch origin`，并核对本地基线是否与 `origin/main` 一致；如果不一致，先把任务基线同步到最新的 `origin/main`，再继续后续步骤。
- 在创建任何 worktree 之前，必须先切到本地 `main` 并拉取最新提交，确保本地 `main` 已经与 `origin/main` 对齐后，再按 `mainline-worktree` 的流程派生 worktree。
- 修改仓库文件时优先使用 `apply_patch`、`git apply` 或其他不会经过 PowerShell 默认编码的方式；不要用 `Set-Content`、`Out-File` 或 `>` 重定向直接写入中文文件，除非显式指定 UTF-8 并在提交前复核无乱码。
- 先建 issue，再创建或核对 worktree，然后才开始实现。
- 只有不涉及仓库改动的纯回答任务，才可以跳过这两个 skill。

## 开源安全交付基线

- 本仓库的 PR 验证和 bug 修复，默认只使用 GitHub 官方 hosted runner，不使用 self-hosted runner。
- 不把真实 AWS 账号、真实 AWS endpoint 或任何外部贡献者密钥作为 CI 前置条件；PR 是否通过，必须建立在不依赖 AWS 账号的可重复验证上。
- 需要验证云侧行为时，优先使用固定输入、golden fixtures、service containers 和本地仿真，并且必须能够在 GitHub Actions 官方 runner 内完成。
- Python 工作区以 `ocr-service/pyproject.toml` 和 `ocr-service/uv.lock` 为唯一基线；本地开发、CI、lint、测试和脚本执行默认都要走 `uv sync --locked --project ocr-service`、`uv run --project ocr-service` 或 `uvx`，不要再回退到裸 `pip install`、`python -m pip`、`venv` 或手工环境拼装。
- CI 分三层组织：
  - 第一层是纯逻辑层，覆盖 lint、type check、unit test、schema 校验、manifest 生成、chunk 切分、幂等、版本推进、状态推进、重试决策和错误码映射；这一层不得调用真实 AWS SDK endpoint，也不得调用真实 OCR 或 embedding 服务，输入输出必须由固定样本和 golden fixtures 驱动。
  - 第二层是契约层，要求把 OCR 和 embedding 抽象成 provider 接口，仓库内固化标准输入输出样本；每次 PR 重点验证 provider 适配器、序列化格式、向量入库前后的字段契约，以及 manifest 引用链是否稳定。
  - 第三层是本地编排集成层，仍然只跑在 GitHub Actions 官方 runner 上，通过 service containers 和本地仿真把整条链路串起来；Lambda 用 SAM CLI Local，DynamoDB 用 DynamoDB Local，SQS 用本地队列模拟器，Step Functions 用 Step Functions Local 加 mocked service integrations，S3 用本地文件系统适配器或 S3-compatible 本地对象存储，向量后端用可本地运行的 Qdrant 加 adapter。
- `workflow-sanity.yml` 是最早的 workflow 语法与卫生门禁，采用矩阵拆分 tabs、actionlint 和 workflow 清单一致性，简单项尽量直接写在 workflow 中。
- GitHub Actions 的 Node.js 和 Python 版本必须通过 `.github/config/ci-runtime.json` 统一管理，并且只能通过 `.github/actions/setup-runtime/action.yml` 这个共享入口安装；workflow 不得再直接写死 `actions/setup-node` / `actions/setup-python` 的版本号。
- 由于 S3 Vectors 没有本地版本，本地向量后端只允许作为适配层验证，不得把它当作真实 S3 Vectors 的语义完全等价实现。
- `Step Functions Local` 只作为流程编排和状态路径验证工具，不得被当成与真实 AWS 行为完全等价的最终裁判。
- 默认只保留两类测试工作流：
- `workflow-sanity.yml`、`guardrails.yml`、`logic-ci.yml`、`contract-ci.yml`、`local-integration-ci.yml` 组成默认 PR 门禁链，覆盖第一层、第二层和一部分本地编排集成测试。
- `docs-ci.yml` 作为独立的 PR 质量门禁，直接触发 `pull_request`，负责文档一致性。
- `security-ci.yml` 继续作为额外的 PR 质量门禁，负责安全审计；在当前链路中它仍然位于 `Workflow Sanity` 之后。
- `stale-issues.yml` 是日常卫生 workflow，只做长期无活动 issue / PR 的自动清理，不进入默认 PR 门禁。
- `issue-hierarchy-guard.yml` 是 issue 卫生 workflow，负责校验主 issue 与子 issue 的关闭层级，并在最后一个子 issue 关闭后自动关闭父 issue，不进入默认 PR 门禁。
- `prod-deploy.yml` 和 `destroy.yml` 只允许手动触发，不能作为默认 PR 门禁。
- 任何需要真实云资源、真实账号、真实 key 或真实供应商在线行为的验证，都只能作为可选补充，不能作为默认交付门槛。
- `guardrails.yml` 现在还负责扫描疑似简体中文乱码和私用区字符，发现后必须给出文件路径、行号和修复建议。
- `pr-path-conflict-guard.yml` 负责扫描并行 PR 的删除 / 重命名路径与修改路径漂移，发现后必须在相关 PR 上给出文件路径和迁移建议，但它不进入默认 PR 门禁链。
- 关联 issue 的 PR 必须在正文前部用普通文本写明 `Closes #123`、`Fixes #123` 或对应的跨仓库引用，不能放进反引号、代码块或引用块；issue 合并后的自动关闭结果必须以 GitHub 实际识别和对应 workflow 的层级校验为准，不能因为父 issue 已合并就默认兄弟 issue 也会一起关闭。
- 如果一个 PR 同时声称修复多个 issue，PR 正文必须逐条列出每个 issue 的处理状态；只有被该 PR 实际完全覆盖的 issue 才能进入自动关闭范围。其余仍然开放的兄弟 issue 必须继续保持打开，或先拆成 subissue / 后续 issue 再进入新的父级链路，不能靠一次合并“顺手带过”。
- 在继续向既有分支提交、推送或补丁之前，必须先核查对应 PR 的真实状态；如果该 PR 已经 merged 或 closed，就不能继续沿用同一条 PR 叙事，必须新开分支并创建新的 PR。
- 当 PR 分支已经包含较大的既有改动时，禁止为了补一个小修复而用 `reset`、强制回退、重建分支或其他会丢失提交历史的方式“整理”分支；必须保留原有提交序列，只能在确认 PR 真实 head 后通过追加提交、`cherry-pick` 或等价的非破坏性方式恢复缺失内容。
- 如果需要对已有 PR 分支强制推送，必须先确认当前分支上的全部有效提交都已在本地恢复，并在推送前核对 `gh pr view` 中的 head commit、commit 列表和 diff 规模，避免把大改动误压成单个补丁。

## 总原则

- 本项目是企业级多格式文档语义检索系统，不是前端模板项目。
- 文档主身份必须包含 `version_id`，不得只按文件名或路径建模。
- 业务完整链路始终围绕 `S3 -> OCR -> Embedding -> S3 Vectors`。
- 当前默认入口固定为 `source bucket -> S3 Event Notification -> SQS ingest queue -> Ingest Lambda -> Step Functions Standard`，对外只暴露 `Ingest Lambda` 一个入口。
- 当前默认运行时固定为 `source bucket -> S3 Event Notification -> SQS ingest queue -> Ingest Lambda -> Step Functions Standard -> extract workflow lambdas -> manifest bucket -> SQS embed queue -> Embed Lambda -> SQS DLQ`。
- 普通 `S3 manifest` 用于保存结构化提取结果、切片资产和可回放材料，不替代 `S3 Vectors`。
- 对外文件分发统一走 `CloudFront`，不直接暴露 `S3 URL`。
- `PaddleOCR` 当前公开异步能力按“提交任务 + 轮询状态 + 拉取结果”建模，不按 webhook 或 callback token 建模。
- 多个 embedding 模型可以并存，但必须按 `embedding profile`、`provider/model/dimension` 和向量空间隔离治理，不得混写到同一个 `S3 Vectors index`。
- 不同 embedding 模型或不同版本默认视为向量空间不兼容；模型切换、profile 下线或维度变更都按“新 profile 上线 + 全量重嵌 + 查询灰度切换”治理，不得只改环境变量直接覆盖。
- 旧版本清理由“最接近写入旧版本信息的那一层”负责，优先复用 `previous_version_id` / `previous_manifest_s3_uri`，缺失记录一律按幂等 `no-op` 处理。
- `source bucket` 的历史对象版本仍然只交给 S3 Lifecycle 删除，不把源文件版本清理写进应用层业务代码。
- 仓库正式部署口径以 `GitHub Actions` 为准；`docs/` 中的控制台文档保留为资源说明和排障参考，不作为默认交付入口。
- 公开仓库的 CI / 发布链与分层策略统一记录在 `docs/open-source-ci-strategy.md`；`examples/workflows/workflow_reference_only/*` 只作为 workflow 编排和冒烟测试的参考素材，不进入默认 PR 门禁。
- `issue-hierarchy-guard.yml` 统一记录在 `docs/open-source-ci-strategy.md`，用于 issue 层级卫生，不作为默认 PR 门禁。
- 默认 PR 门禁之外，如需覆盖网络或 AWS 标记测试，可以使用 `external-validation.yml` 这样的手动 workflow，但它不能依赖真实云作为默认门禁。
- `examples/workflows/workflow_reference_only/*` 仅作为 `reference-only` 素材，不进入默认 PR 门禁。
- 为了满足 PR 失败回评，`guardrails.yml`、`logic-ci.yml`、`contract-ci.yml`、`local-integration-ci.yml`、`docs-ci.yml`、`security-ci.yml` 允许最小化的 `issues: write`，但只能用于向当前 PR 写失败评论，不能扩展到其他写权限。
- `ocr-service/tools/ci/` 作为 CI Python helper 层，只保留 `validate_workflows.py` 和 `comment_pr_failure.py`；简单扫描、路径判断、字符串检查等尽量直接写在 workflow 里。
- `infra/pipeline-config.json` 是部署命名默认值的单一来源；由 `infra/` 和运行时共同读取。
- 旧的 boto3 部署兼容层已删除，不再保留重复实现。
- 本仓库维护正式的 `deploy` 与 `destroy` workflow；控制台文档只作为辅助参考，不替代自动化交付。
- `infra/pipeline-config.json` 是部署命名默认值的单一来源；其中 `resource_names` 必须显式列出所有 AWS 资源名，`embedding_profiles` 必须显式列出每个 profile 的 `vector_index_name`，修改该文件后，`deploy` / `destroy` / `sync` 三条入口会自动采用新的默认值。
- 现在默认 embedding profile 是 OpenAI，Gemini 作为备用方案保留，但默认关闭；启用 Gemini 只改 `infra/pipeline-config.json` 里的 `enabled`。
- 规则如果已经沉淀到对应 skill，优先更新 skill，不在本文件重复维护实现细节。
- 如果用户明确要求与现有设计或既有规则冲突，以用户当次明确要求为准；随后必须把冲突点同步更新到相关 skill 或 `AGENTS.md`，包括这条冲突优先规则本身。

## Skill 导航

> Skills 唯一事实源在 `ai/skills-src/`，通过 `ai/scripts/sync-ai.py` 生成到 `.agents/skills/`（Codex）和 `.claude/skills/`（Claude Code）。

- github-issue-workflow
  - [ai/skills-src/organization/organization-github-issue-workflow/SKILL.md](ai/skills-src/organization/organization-github-issue-workflow/SKILL.md)
  - 面向 Codex 的 GitHub issue 优先工作流：先建 issue，再改代码；在技能入口提供简要说明、流程规则、评论规范和 PR 关联要求。
- github-cli
  - [ai/skills-src/organization/organization-github-cli/SKILL.md](ai/skills-src/organization/organization-github-cli/SKILL.md)
  - 面向 `gh` CLI 的通用 GitHub 操作入口，覆盖 issue、PR、分支、workflow/run 读取与评论写入。
- mainline-worktree
  - [ai/skills-src/organization/organization-mainline-worktree/SKILL.md](ai/skills-src/organization/organization-mainline-worktree/SKILL.md)
  - 适用于新建 worktree 前先同步本地 `main` 与 `origin/main`、创建任务分支和语义化命名。
- versioned-s3-ingest
  - [ai/skills-src/versioned-s3-ingest/SKILL.md](ai/skills-src/versioned-s3-ingest/SKILL.md)
  - 适用于 `S3 Versioning`、`S3 Event Notification`、`SQS ingest queue`、对象版本身份、幂等、`object_state` 和入口治理。
- check-pr
  - [ai/skills-src/organization/organization-check-pr/SKILL.md](ai/skills-src/organization/organization-check-pr/SKILL.md)
  - 适用于 GitHub PR 的未解决评论、状态检查、描述完整性和回评修复。
- multimodal-extract-chunking
  - [ai/skills-src/multimodal-extract-chunking/SKILL.md](ai/skills-src/multimodal-extract-chunking/SKILL.md)
  - 适用于 OCR、版面分析、`Step Functions Standard` 轮询式异步抽取、PDF/PPT/图片切片、多模态 chunk 设计。
- vector-manifest-storage
  - [ai/skills-src/vector-manifest-storage/SKILL.md](ai/skills-src/vector-manifest-storage/SKILL.md)
  - 适用于 OpenAI Embedding / Azure OpenAI、`S3 Vectors` metadata、普通 `S3 manifest`、`DynamoDB` 状态与索引。
- project-delivery-guardrails
  - [ai/skills-src/project-delivery-guardrails/SKILL.md](ai/skills-src/project-delivery-guardrails/SKILL.md)
  - 适用于设计或修改代码前先梳理仓库结构、职责边界、文件落位和实施顺序，避免实现发散。
- architecture-reset-refactor
  - [ai/skills-src/architecture-reset-refactor/SKILL.md](ai/skills-src/architecture-reset-refactor/SKILL.md)
  - 适用于先审视架构、再做强重构，并在 Python 文件中同步执行双语注释与 docstring 规范；已合并 `python-bilingual-comments` 的规则，不再单独保留。
- durable-s3-embed-pipeline
  - [ai/skills-src/durable-s3-embed-pipeline/SKILL.md](ai/skills-src/durable-s3-embed-pipeline/SKILL.md)
  - 适用于最小可用的 `S3 Event Notification -> SQS -> Ingest Lambda -> Step Functions Standard -> SQS -> Embed Lambda` 架构设计、资源拆分、职责边界和带序号时序图说明。
- docs-mcp-router
  - [ai/skills-src/organization/organization-docs-mcp-router/SKILL.md](ai/skills-src/organization/organization-docs-mcp-router/SKILL.md)
  - 将 AWS、Azure、Google、GitHub、Cloudflare、OpenAI 和 Anthropic 的文档问题路由到对应的官方 MCP 或第一方文档来源。
- bilingual-programming
  - [ai/skills-src/organization/organization-bilingual-programming/SKILL.md](ai/skills-src/organization/organization-bilingual-programming/SKILL.md)
  - 适用于跨语言代码注释、脚本注释、workflow 注释、UTF-8 编码、`@param`/`@return` 完整性与性能意识；默认要求英文一行、中文一行逐行对应，禁止单行中英混写。

## 当前已定结论

- `S3 Versioning` 必开。
- 当前默认入口固定为 `source bucket -> S3 Event Notification -> SQS ingest queue -> Ingest Lambda -> Step Functions Standard`，对外只暴露 `Ingest Lambda` 一个入口。
- `S3 Event Notification` 当前按原生目标能力使用，默认投递到 `SQS ingest queue`，不直接启动 `Step Functions`。
- 当前默认运行时已经拆分为 `source bucket -> S3 Event Notification -> SQS ingest queue -> Ingest Lambda -> Step Functions Standard -> extract workflow lambdas -> manifest bucket -> SQS embed queue -> Embed Lambda -> SQS DLQ`。
- 当前最小可用资源拆分优先为 `2` 个 `S3 bucket`、`2` 个 `SQS queue`、`1` 个 `DLQ`、`2` 类执行型 `Lambda`、`1` 个启动型 `Lambda`、`1` 个 `Step Functions Standard` 状态机和 `2` 张 `DynamoDB` 表。
- 当前推荐形态是“一个对外入口 Lambda + 内部多职责运行时”：对外入口固定为 `Ingest Lambda`，内部仍拆分为 `ingest lambda`、`extract workflow lambdas` 和 `embed lambda`。
- Ingest 阶段负责承接 `S3` 事件、解析 `bucket/key/version_id/sequencer`、执行幂等与乱序拦截预检查，并启动 `Step Functions Standard`。
- `Step Functions Standard` 负责提交 `PaddleOCR` job、`Wait`、查询 job 状态、拉取 `resultUrl/jsonUrl`、生成 manifest/chunks/assets、写入 manifest bucket、推进 `object_state`/`manifest_index` 并投递 embed job。
- `PaddleOCR` 使用 API。
- `OpenAI Embedding` 是当前默认 embedding 模式，该仓库默认使用 `Azure OpenAI` 兼容 API。
- `DynamoDB` 当前属于必需基础设施，用于幂等、版本隔离、状态推进、失败补偿和 chunk 反查。
- 查询层已具备最小闭环，公网入口改为 `API Gateway REST`，`CloudFront signed URL` 分发层仍按需启用。
- `PPT/PPTX` 默认同时产出 `slide_text_chunk` 和 `slide_image_chunk`。
- 推荐保留一个统一对外入口 `Lambda`，但运行时职责仍应拆分；不要把提取编排、OCR 轮询和 embedding 消费长期合并进同一个 `Lambda` 资源。
- `object_state` 继续承载对象主状态与版本推进上下文，其中必须保留 `previous_version_id` / `previous_manifest_s3_uri` 作为旧版本清理的输入。
- `manifest_index` 继续作为版本级 chunk 反查表，不建议靠 GSI 取代其版本隔离职责。
- `embedding_projection_state` 继续作为按 `profile_id` 隔离的投影状态层；单写入 profile 时可按需省略，多 profile 时不要把它塞回 `object_state`。
- DynamoDB 的二级索引只用于补充查询路径，不用于把 `object_state`、`manifest_index`、`projection_state` 三层职责压成一张通用表。
- 当前部署口径统一为“GitHub Actions 部署”，相关说明集中维护在 `docs/`。

## 维护规则

- Skills 唯一事实源在 `ai/skills-src/`；通过 `ai/scripts/sync-ai.py` 生成 `.agents/skills/`（Codex）和 `.claude/skills/`（Claude Code）；新增、修改、删除 skill 都以 `ai/skills-src/` 为准。修改 skill 后必须运行 `python ai/scripts/sync-ai.py` 并将源与生成物一起提交；禁止直接修改 `.agents/skills/` 或 `.claude/skills/`。
- 如果只是某一类问题的细化规则变化，改对应 skill。
- 如果是全项目方向变化、skill 边界变化、或技能导航变化，再改本文件。
- 如果 `AGENTS.md` 与 skill 冲突，先统一设计，再同步更新对应的 Codex skill 与本文件，避免规则漂移。
- 仓库内新增或更新的文档必须使用简体中文。
- 仓库内新增或更新的代码注释、脚本注释、workflow 注释默认使用双语格式：英文一行、中文一行；若受外部格式限制无法双行展开，至少保留中文，并在可控位置补齐双语说明。具体写法与边界以 `bilingual-programming` skill 为准。
- 如果修改代码导致现有文档不兼容，必须同步更新相关文档。
- 如果资源拓扑、部署流程、环境变量、IAM 权限边界或部署顺序发生变化，必须同步更新 `docs/` 中的对应说明，并检查 `infra/` 是否需要同步修改。
- 如果更新了业务代码、部署流程或环境变量定义，必须先判断 `infra/` 是否需要同步修改，再决定是否需要更新 workflow。
- 当前仓库已经删除 boto3 部署与销毁辅助面；如果 `infra/pipeline-config.json`、资源编排或环境假设发生变化，必须同步更新本文件与相关 skill。
- 简体中文占位句与乱码检查必须复用 `ocr-service/tools/ci/chinese_text_hygiene.py`，本地 pytest 和 `guardrails.yml` 要调用同一份逻辑，不能各写一套漂移的文本卫生规则。
- GitHub Actions workflow 文件名和展示名必须使用语义化命名，禁止再使用 `00/10` 这类数字前缀；对应 YAML 文件应补充英文和中文注释，说明关键步骤或约束。
- 如果用户明确要求与现有设计或既有规则冲突，以用户当次明确要求为准；随后必须把冲突点同步更新到相关 skill 或 `AGENTS.md`，包括这条冲突优先规则本身。
- 如果目标 PR 已经 merged/closed，后续补充必须另起新的 PR，并在新 PR 中重新写明关联关系；不要继续往旧 PR 分支上追加修改。
- 子 issue 只允许有一个主父 issue。若同一个 leaf 问题需要同时出现在多个父类语境里，不要尝试重复挂载；应采用“主父 issue + 共享中间父 issue / 跨引用”的方式：实际执行只归属一个主父 issue，其他父类通过 issue 互链、检查清单或独立 tracking issue 引用主父 issue，避免让同一 leaf 在多棵树里产生重复关闭语义。
- 当用户要求继续收尾某个明确范围内的改动时，先一次性分析并批量修改所有直接相关的文件、链接、标题和说明，不要把明显可推断的后续项拆成一问一答；只有在信息不足且无法合理推断时才询问。
- issue 和 PR 的进度评论要保持简短、连续、可追踪，统一使用 `Checkpoint`、`Change`、`Remaining`、`Blocker`、`Evidence` 这组字段；优先更新同一条进度评论，避免在同一线程刷出大量零碎评论。
- issue 和 PR 在创建、更新或补内容后，都要再回看一遍 GitHub 上实际显示的标题、正文和模板字段；如果发现乱码、错码、字符丢失或明显编码异常，先修复再继续下一步。
- 如果引入新的 embedding provider、embedding profile、vector bucket/index 或查询融合策略，必须同步更新 `AGENTS.md`、相关 skill、`docs/` 手动部署文档和测试。
- 只要修改了代码，必须执行整个仓库的全量测试，不得只跑局部相关测试后直接交付。
- 如果全量测试失败，必须先修复失败项或明确说明阻塞原因、影响范围和未通过项，不能省略。
- 只要修改了其他 Python 代码，必须同步更新相关测试代码；如果改动触及项目规则、交付流程、注释规范或技能边界，还必须同步更新对应 skill 和相关说明文档。
- 如果因为环境、权限或外部依赖无法执行测试，必须在交付说明里明确写出未验证项和原因。
- 当前仓库查询与匿名 MCP 调用只保留 `tenant_id` 约束，不再使用 `security_scope`；新增代码、部署配置和文档不得再把 `security_scope` 作为必填治理字段。
- `merged-branch-cleanup.yml` 是 Branch Lifecycle Cleanup，会在 3 小时周期内同步收敛 PR 分支标签状态，并在可删除分支到期时删除分支。