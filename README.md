# serverless-ocr-s3vectors-mcp

Enterprise serverless document ingestion and semantic retrieval on AWS.

English | [简体中文](README/README.zh-CN.md)

This repository is a multi-surface workspace, not a single-purpose app shell.

- The Python workspace is rooted at `services/pyproject.toml` and `services/uv.lock`.
- The core backend service lives under `services/ocr-pipeline/`.
- Infrastructure and deployment code live under `infra/cdk/`.
- Reference-only workflow samples live under `examples/workflows/workflow_reference_only/`.
- The documentation hub lives under `docs/`.

## At a Glance

| Item | Details |
| --- | --- |
| Primary entry point | `Ingest Lambda` |
| Repository identity | `serverless-ocr-s3vectors-mcp` |
| Python workspace root | `services/` |
| Service source root | `services/ocr-pipeline/` |
| Docs hub | `docs/README.md` |
| Validation model | GitHub Actions PR workflows and checks |

## What It Delivers

- Version-aware document ingestion with `version_id` as the primary identity.
- Asynchronous OCR orchestration with deterministic polling and result collection.
- Structured manifests for extraction results, chunk assets, and replayable artifacts.
- Profile-isolated embedding and vector storage for multi-model or multi-dimension setups.
- Query-time guardrails that re-check object state before returning retrieval results.

## Repository Shape

The repository is organized around the runtime flow instead of a single monolithic package.

1. `S3` emits document events.
2. `S3 Event Notification` delivers events to the `SQS ingest queue`.
3. `Ingest Lambda` validates the object version, applies idempotency checks, and starts `Step Functions Standard`.
4. `Step Functions Standard` coordinates OCR submission, polling, result fetch, manifest generation, and embed job dispatch.
5. Extract workers persist structured artifacts to the manifest bucket.
6. `Embed Lambda` consumes embed jobs and writes vectors to the configured vector backend.

This layout keeps the public surface area small while preserving clear operational boundaries inside the workflow.

## Service Boundary

The service package is now physically rooted at `services/ocr-pipeline/src/serverless_mcp/`.

- `services/pyproject.toml` points `serverless-mcp-service` at the `services/ocr-pipeline/` source tree.
- `services/ocr-pipeline/README.md` documents the package-level boundary and the current module layout.
- `services/ocr-pipeline/src/serverless_mcp/__init__.py` is a normal package initializer, not a compatibility shim.

## Documentation

| Document | Purpose |
| --- | --- |
| [Documentation hub](docs/README.md) | Main index for deployment, runtime, and boundary documentation |
| [Service implementation](services/ocr-pipeline/README.md) | Package-level implementation notes |
| [Open-source boundaries](docs/open-source-boundaries.md) | Repository boundaries and external usage constraints |
| [Deployment strategy](docs/open-source-ci-strategy.md) | Open-source CI and delivery model |
| [Simplified Chinese landing page](README/README.zh-CN.md) | Simplified Chinese landing page and local navigation |

## Python Toolchain

- Use `uv sync --locked --project services` to prepare the Python environment.
- Use `uv run --project services pytest -q`, `uv run --project services ruff check .`, and `uv run --project services python ...` for the default local validation loop.
- Treat `services/pyproject.toml` and `services/uv.lock` as the single source of truth for Python dependencies.
- Keep day-to-day development on the uv path; do not reintroduce ad hoc `pip` or `venv` bootstrap steps.

## Localized Editions

| Locale | Entry |
| --- | --- |
| English | `README.md` |
| Simplified Chinese | `README/README.zh-CN.md` |

## Validation

- Pull requests are validated through GitHub Actions workflows and checks.
- Infrastructure and runtime changes should continue to follow the repository's documented guardrails.
- Documentation updates should keep cross-links aligned with the docs hub and language landing pages.
