# GitHub Actions 部署索引

本文件是部署文档的第一入口，用于按顺序串起资源拓扑、部署步骤和运行时边界。

## 阅读顺序

1. [AWS 控制台手动部署 - 概览与准备](aws-console-manual-deployment-overview-prep.md)
2. [AWS 控制台手动部署 - 存储与状态](aws-console-manual-deployment-storage-state.md)
3. [GitHub Actions 部署 - Vectors / IAM / Lambda](github-actions-deployment-vectors-iam-lambda.md)
4. [AWS 控制台手动部署 - Step Functions / 事件 / 触发器](aws-console-manual-deployment-stepfunctions-events-triggers.md)
5. [AWS 控制台手动部署 - 查询 / 治理 / 验证](aws-console-manual-deployment-query-governance-validation.md)

## 模块说明

- `概览与准备`：说明部署前置条件、产物准备和整体执行节奏。
- `存储与状态`：说明 S3、SQS、DynamoDB 与状态层归属。
- `Vectors / IAM / Lambda`：说明向量存储、权限边界和 Lambda 资源。
- `Step Functions / 事件 / 触发器`：说明编排、事件通知和触发链路。
- `查询 / 治理 / 验证`：说明查询闭环、治理约束和验证步骤。

## 维护要求

- 如果部署流程、环境变量、资源命名或触发顺序变化，必须先更新对应正文，再更新本索引。
- 如果某条边界规则已经沉淀到 skill，优先更新 skill，再回写到这里。
- 本索引只负责串联真实文档，不再保留数字前缀式的导航噪音。
