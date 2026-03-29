# 文档中心

[English](README.md) | 简体中文

本目录包含项目的部署说明、运行时指南和边界文档。

## 阅读顺序

1. 先看 GitHub Actions 部署索引，理解整体资源拓扑和执行顺序。
2. 再看 AWS CDK 部署与销毁说明，了解基于 TypeScript 的部署入口。
4. 最后回到服务级实现说明和边界文档，补全运行时上下文。

## 主要文档

| 文档 | 用途 |
| --- | --- |
| [GitHub Actions 部署索引](github-actions-deployment-index.md) | GitHub Actions 部署文档总索引 |
| [AWS 控制台手动部署 - 概览与准备](aws-console-manual-deployment-overview-prep.md) | 部署前准备和端到端流程 |
| [AWS 控制台手动部署 - 存储与状态](aws-console-manual-deployment-storage-state.md) | S3、DynamoDB 以及相关存储层 |
| [GitHub Actions 部署 - Vectors / IAM / Lambda](github-actions-deployment-vectors-iam-lambda.md) | 向量存储、权限和 Lambda 资源 |
| [AWS 控制台手动部署 - Step Functions / 事件 / 触发器](aws-console-manual-deployment-stepfunctions-events-triggers.md) | 编排、事件通知和触发链路 |
| [AWS 控制台手动部署 - 查询 / 治理 / 验证](aws-console-manual-deployment-query-governance-validation.md) | 查询闭环、治理和校验 |
| [部署配置单一事实来源](deployment-config-single-source-of-truth.md) | 默认值与配置归属 |
| [查询契约与运行时前置条件](query-contract-and-runtime-prereqs.md) | 远程查询契约、降级语义和运行时前置条件 |
| [AWS CDK 部署顺序与触发器](aws-cdk-deploy-order-triggers.md) | CDK 部署顺序和触发规则 |
| [AWS CDK 部署与销毁说明](aws-cdk-deploy-destroy-helpers.md) | `cdk deploy` 等 TypeScript CDK 入口 |
| [AWS CDK 销毁说明](aws-cdk-destroy-notes.md) | 销毁流程和操作注意事项 |
| [开源边界说明](open-source-boundaries.md) | 仓库边界与外部使用约束 |
| [工作流对齐说明](workflow-reconcile-notes.md) | 部署与运行时工作流对齐说明 |

## 本地化版本

仓库把本地化入口页放在 `i18n/` 下，根目录 `README.md` 保持英文主入口。

| 语言 | 入口 |
| --- | --- |
| English | `../README.md` |
| 简体中文 | `../i18n/README.zh-CN.md` |

## 约定

1. 当部署流程、环境变量、IAM 边界或 workflow 假设发生变化时，必须同步更新本索引。
2. 如果某条规则已经沉淀到 skill，先更新 skill，再回写到这里。
3. 本地化入口页采用 `README.<locale>.md` 命名，直接放在 `i18n/` 下。

## Python 工具链

1. 在运行 Python 测试或脚本前，先执行 `uv sync --locked --project services`。
2. 本地验证优先使用 `uv run --project services pytest -q`、`uv run --project services ruff check .` 和 `uv run --project services python ...`。
3. 将 `services/pyproject.toml` 和 `services/uv.lock` 视为 Python 依赖的唯一事实来源。
4. 默认工作流保持在 uv 路径上，不要再新增零散的 `pip` 或 `venv` 启动步骤。
