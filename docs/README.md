# Documentation Hub

English | [简体中文](../i18n/README.zh-CN.md)

This directory contains the project deployment notes, runtime guidance, and boundary documentation.

## Reading Order

1. Start with the GitHub Actions deployment index to understand the overall resource layout and execution order.
2. Review the AWS CDK deployment and destroy guide to understand the TypeScript-based deployment entry points.
3. Read the query / governance / validation note before probing the remote MCP endpoint.
4. Treat the remote MCP front door as API-Key-Protected by default; do not describe `/mcp` as anonymously open.

## Main Documents

| Document | Purpose |
| --- | --- |
| [GitHub Actions deployment index](github-actions-deployment-index.md) | Main entry point for GitHub Actions deployment documentation |
| [AWS console manual deployment overview and preparation](aws-console-manual-deployment-overview-prep.md) | Deployment prerequisites and end-to-end flow |
| [AWS console manual deployment - storage and state](aws-console-manual-deployment-storage-state.md) | S3, DynamoDB, and related storage layers |
| [GitHub Actions deployment - Vectors / IAM / Lambda](github-actions-deployment-vectors-iam-lambda.md) | Vector storage, permissions, and Lambda resources |
| [AWS console manual deployment - Step Functions / events / triggers](aws-console-manual-deployment-stepfunctions-events-triggers.md) | Orchestration, event notifications, and trigger chains |
| [AWS console manual deployment - query / governance / validation](aws-console-manual-deployment-query-governance-validation.md) | Remote MCP access gate, validation flow, and usage guidance |
| [Deployment config single source of truth](deployment-config-single-source-of-truth.md) | Default values and configuration ownership |
| [Query contract and runtime prerequisites](query-contract-and-runtime-prereqs.md) | Remote query contract, degraded semantics, and runtime prerequisites |
| [AWS CDK deployment order and triggers](aws-cdk-deploy-order-triggers.md) | CDK deployment sequencing and trigger rules |
| [AWS CDK deployment and destroy guide](aws-cdk-deploy-destroy-helpers.md) | TypeScript CDK entry points such as `cdk deploy` |
| [AWS CDK destroy notes](aws-cdk-destroy-notes.md) | Destroy flow and operational cautions |
| [Open-source boundaries](open-source-boundaries.md) | Repository boundaries and external usage constraints |
| [Workflow alignment notes](workflow-reconcile-notes.md) | Alignment notes for deployment and runtime workflows |

## Localized Editions

The repository keeps localized landing pages under `i18n/` so the root README can remain the canonical English entry point.

| Locale | Entry |
| --- | --- |
| English | `../README.md` |
| Simplified Chinese | `../i18n/README.zh-CN.md` |

## Conventions

1. Keep this index aligned with any changes to deployment flow, environment variables, IAM boundaries, or workflow assumptions.
2. If a rule has already been captured in a skill, update the skill first and then reflect the change here.
3. Localized landing pages use a language-suffix naming pattern (`README.<locale>.md`) and live directly under `i18n/`.
4. Any doc that mentions the external MCP front door must state the current `X-API-Key` access gate and the Usage Plan / quota behavior.

## Python Toolchain

1. Use `uv sync --locked --project ocr-service` before running Python tests or scripts.
2. Prefer `uv run --project ocr-service pytest -q`, `uv run --project ocr-service ruff check .`, and `uv run --project ocr-service python ...` for local validation.
3. Treat `ocr-service/pyproject.toml` and `ocr-service/uv.lock` as the single source of truth for Python dependencies.
4. Keep the default workflow on uv and do not add ad hoc `pip` or `venv` bootstrap steps.
