# 部署配置单一来源

`infra/pipeline-config.json` 是仓库里部署命名和默认值的唯一配置源。它现在被以下位置共同读取：

- `infra/cdk/bin/app.ts`
- `infra/cdk/lib/foundation-stack.ts`
- `infra/cdk/lib/compute-stack.ts`
- `infra/cdk/lib/api-stack.ts`
- `ocr-service/ocr-pipeline/src/serverless_mcp/runtime/config.py`
- `tools/packaging/serverless_mcp/build_lambda_artifacts.py`
- `tools/packaging/serverless_mcp/build_layer_artifacts.py`

## 配置分工

- `name_prefix` 用于统一派生资源名前缀
- `resource_names` 显式列出所有 AWS 资源名
- `embedding_profiles` 显式列出每个 profile 的 `provider`、`model`、`dimension`、`vector_bucket_name`、`vector_index_name` 和开关
- `defaults` 保存运行时默认值，例如 Lambda 超时、OCR 参数和查询阈值
- `lambda_settings` 保存每个 Lambda 函数的内存和超时设置
- OpenAI 兼容 base URL 的主入口是 `OPENAI_API_BASE_URL`，不再接受 `OPEN_BASE_URL` 兼容别名；`AZURE_OPENAI_URL` 不再使用
- 如果兼容端点本身带有自定义路径前缀，例如 OpenRouter 的 `/api/v1`，运行时必须保留该前缀，不要再强行规范化成 `/v1`
- `.env.example` 指向 `infra/pipeline-config.json`

## 现在的消费方式

- CDK 直接读取 `infra/pipeline-config.json` 来合成基础设施
- 运行时通过 `SERVERLESS_MCP_PIPELINE_CONFIG_PATH` 读取同一份配置，默认示例指向 `infra/pipeline-config.json`
- 打包脚本使用同一份配置来生成 Lambda 和 Layer 产物

## 约束

- 任何会影响资源命名、默认值或 embedding profile 的改动，都必须先改 `infra/pipeline-config.json`
- 不要在不同脚本里复制一份新的默认值
- 删除旧 helper 之后，配置文件仍然是唯一事实来源，不要把默认值散落到 workflow 或脚本参数里
- 如果要调整 OpenAI 兼容端点，请只更新 `OPENAI_API_BASE_URL`
