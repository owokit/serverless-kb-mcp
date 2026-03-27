"""
EN: Tests for the extract Lambda handler covering action dispatch, validation, and component caching.
CN: 同上。
"""

from __future__ import annotations

from dataclasses import dataclass
import json

import pytest

import serverless_mcp.extract.handlers.mark_failed as extract_mark_failed_module
from serverless_mcp.extract.handlers.router import lambda_handler
from serverless_mcp.extract.handlers.support import _WorkflowComponents


def _job_payload() -> dict:
    return {
        "source": {
            "tenant_id": "tenant-a",
            "bucket": "bucket-a",
            "key": "docs/guide.md",
            "version_id": "v1",
            "security_scope": ["team-a"],
        },
        "trace_id": "trace-1",
    }


class _FakeWorkflow:
    # EN: Stand-in for StepFunctionsExtractWorkflow that records method calls.
    # CN: 璁板綍鏂规硶璋冪敤鐨?StepFunctionsExtractWorkflow 鏇胯韩銆?
    def __init__(self) -> None:
        self.prepare_job_call = None
        self.poll_job_id = None
        self.persist_call = None

    def prepare_job(self, *, job):
        self.prepare_job_call = job
        return {"action": "prepare_job", "trace_id": job.trace_id}

    def poll_ocr_job(self, *, job_id: str):
        self.poll_job_id = job_id
        return {"action": "poll_ocr_job", "job_id": job_id}

    def persist_ocr_result(self, *, job, processing_state, json_url):
        self.persist_call = (job, processing_state, json_url)
        return {"action": "persist_ocr_result", "json_url": json_url}

    def mark_failed(self, *, job, failure):
        self.persist_call = (job, failure.error, failure.cause, failure.domain)
        return {
            "action": "mark_failed",
            "error": failure.error,
            "cause": failure.cause,
            "failure_domain": failure.domain,
        }

class _FakeComponents:
    # EN: Stub component registry that returns a fixed workflow for any action.
    # CN: 瀵逛换鎰?action 杩斿洖鍥哄畾 workflow 鐨勭粍浠舵敞鍐屾々銆?
    def __init__(self, workflow: _FakeWorkflow) -> None:
        self.workflow = workflow
        self.actions: list[str] = []

    def workflow_for(self, action: str):
        self.actions.append(action)
        return self.workflow


def test_lambda_handler_prepare_job_validates_and_normalizes_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify that prepare_job action validates the payload and normalizes security_scope to a tuple.
    CN: 楠岃瘉 prepare_job action 鏍￠獙 payload 骞跺皢 security_scope 瑙勮寖鍖栦负鍏冪粍銆?
    """
    workflow = _FakeWorkflow()
    components = _FakeComponents(workflow)
    monkeypatch.setattr("serverless_mcp.extract.handlers.router._get_components", lambda: components)

    result = lambda_handler({"action": "prepare_job", "job": _job_payload()}, None)

    assert result == {"action": "prepare_job", "trace_id": "trace-1"}
    assert workflow.prepare_job_call.source.security_scope == ("team-a",)
    assert components.actions == ["prepare_job"]


def test_lambda_handler_rejects_payload_without_action(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify that a payload missing the action field raises ValueError.
    CN: 同上。
    """
    workflow = _FakeWorkflow()
    components = _FakeComponents(workflow)
    monkeypatch.setattr("serverless_mcp.extract.handlers.router._get_components", lambda: components)

    with pytest.raises(ValueError, match="action is required for extract workflow"):
        lambda_handler({"Records": [{"eventSource": "aws:s3"}]}, None)

    assert components.actions == []


def test_lambda_handler_returns_400_for_empty_direct_invocation() -> None:
    """
    EN: Verify that an empty event dict returns HTTP 400.
    CN: 楠岃瘉绌轰簨浠跺瓧鍏歌繑鍥?HTTP 400銆?
    """
    result = lambda_handler({}, None)

    assert result["statusCode"] == 400


def test_lambda_handler_raises_clear_error_for_invalid_job_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify that a job payload missing version_id raises ValueError with a clear message.
    CN: 楠岃瘉缂哄皯 version_id 鐨?job payload 鎶涘嚭甯︽湁鏄庣‘娑堟伅鐨?ValueError銆?
    """
    workflow = _FakeWorkflow()
    components = _FakeComponents(workflow)
    monkeypatch.setattr("serverless_mcp.extract.handlers.router._get_components", lambda: components)

    with pytest.raises(ValueError, match="Invalid extract workflow payload"):
        lambda_handler(
            {
                "action": "prepare_job",
                "job": {
                    "source": {
                        "tenant_id": "tenant-a",
                        "bucket": "bucket-a",
                        "key": "docs/guide.md",
                    },
                    "trace_id": "trace-1",
                },
            },
            None,
        )


def test_lambda_handler_rejects_empty_json_url_for_persist_ocr_result(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify that persist_ocr_result rejects a blank json_url value.
    CN: 楠岃瘉 persist_ocr_result 鎷掔粷绌虹櫧鐨?json_url 鍊笺€?
    """
    workflow = _FakeWorkflow()
    components = _FakeComponents(workflow)
    monkeypatch.setattr("serverless_mcp.extract.handlers.router._get_components", lambda: components)

    with pytest.raises(ValueError, match="json_url is required for persist_ocr_result"):
        lambda_handler(
            {
                "action": "persist_ocr_result",
                "job": _job_payload(),
                "processing_state": {
                    "pk": "tenant-a#bucket-a#docs/guide.md",
                    "latest_version_id": "v1",
                    "latest_sequencer": "0001",
                    "extract_status": "EXTRACTED",
                    "embed_status": "PENDING",
                },
                "json_url": "   ",
            },
            None,
        )


@dataclass(frozen=True)
class _FakeSettings:
    # EN: Frozen dataclass stand-in for Settings.
    # CN: Settings 鐨勫喕缁?dataclass 鏇胯韩銆?
    object_state_table: str = "object-state"
    manifest_index_table: str = "manifest-index"
    manifest_bucket: str = "manifest-bucket"
    manifest_prefix: str = ""
    embed_queue_url: str | None = None
    paddle_api_base_url: str = "https://example.com"
    paddle_api_token: str | None = "token"
    paddle_ocr_model: str = "PaddleOCR-VL-1.5"
    paddle_poll_interval_seconds: int = 10
    paddle_max_poll_attempts: int = 3
    paddle_http_timeout_seconds: int = 60
    paddle_status_timeout_seconds: int = 10
    paddle_allowed_hosts: tuple[str, ...] = ()


class _ClientRegistry:
    # EN: Stub AWS client registry.
    # CN: 同上。
    s3 = object()
    dynamodb = object()
    sqs = object()


def test_workflow_components_load_only_action_specific_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify that _WorkflowComponents loads only the OCR client for poll_ocr_job, not all dependencies.
    CN: 同上。
    """
    calls: list[str] = []

    class _FakeOcrClient:
        def __init__(self, **kwargs) -> None:
            calls.append("ocr_client")

    monkeypatch.setattr("serverless_mcp.extract.handlers.support._build_ocr_client", lambda **kwargs: calls.append("ocr_client"))
    monkeypatch.setattr("serverless_mcp.extract.handlers.support._build_source_repo", lambda *args, **kwargs: calls.append("source_repo"))
    monkeypatch.setattr("serverless_mcp.extract.handlers.support._build_extraction_service", lambda *args, **kwargs: calls.append("extraction_service"))
    monkeypatch.setattr("serverless_mcp.extract.handlers.support._build_object_state_repo", lambda *args, **kwargs: calls.append("object_state_repo"))
    monkeypatch.setattr("serverless_mcp.extract.handlers.support._build_manifest_repo", lambda *args, **kwargs: calls.append("manifest_repo"))
    monkeypatch.setattr("serverless_mcp.extract.handlers.support._build_embed_dispatcher", lambda *args, **kwargs: calls.append("embed_dispatcher"))
    monkeypatch.setattr("serverless_mcp.extract.handlers.support._build_result_persister", lambda *args, **kwargs: calls.append("result_persister"))
    monkeypatch.setattr("serverless_mcp.extract.handlers.support._build_extract_worker", lambda *args, **kwargs: calls.append("extract_worker"))
    monkeypatch.setattr("serverless_mcp.extract.handlers.support._build_manifest_builder", lambda: calls.append("manifest_builder"))

    components = _WorkflowComponents(settings=_FakeSettings(), clients=_ClientRegistry())

    workflow = components.workflow_for("poll_ocr_job")

    assert workflow is not None
    assert calls == ["ocr_client"]


def test_workflow_components_reuse_cached_ocr_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify that the OCR client is instantiated once and reused across multiple workflow_for calls.
    CN: 同上。
    """
    calls: list[str] = []

    class _FakeOcrClient:
        def __init__(self, **kwargs) -> None:
            calls.append("ocr_client")

    monkeypatch.setattr("serverless_mcp.extract.handlers.support._build_ocr_client", lambda **kwargs: calls.append("ocr_client"))

    components = _WorkflowComponents(
        settings=_FakeSettings(embed_queue_url="https://queue.example.com"),
        clients=_ClientRegistry(),
    )

    components.workflow_for("poll_ocr_job")
    components.workflow_for("submit_ocr_job")

    assert calls == ["ocr_client"]


def test_mark_failed_lambda_handler_uses_structured_error_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    EN: Verify that the dedicated mark_failed Lambda handler forwards error and cause separately.
    CN: 验证专用 mark_failed Lambda 处理器会分别传递 error 和 cause。
    """
    workflow = _FakeWorkflow()
    components = _FakeComponents(workflow)
    monkeypatch.setattr("serverless_mcp.extract.handlers.mark_failed._get_components", lambda: components)

    result = extract_mark_failed_module.lambda_handler(
        {
            "job": _job_payload(),
            "error": "PaddleOCRJobFailed",
            "cause": "timeout",
        },
        None,
    )

    assert result == {
        "action": "mark_failed",
        "error": "PaddleOCRJobFailed",
        "cause": "timeout",
        "failure_domain": "ocr",
    }
    assert workflow.persist_call[1:] == ("PaddleOCRJobFailed", "timeout", "ocr")


def test_mark_failed_lambda_handler_records_domain_metric_and_handler_failure_separately(
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """
    EN: Verify that a workflow exception emits separate domain and handler failure metrics.
    CN: 验证 workflow 异常会分别发出领域 metric 和 handler failure metric。
    """
    class _FailingWorkflow(_FakeWorkflow):
        def mark_failed(self, *, job, failure):
            raise RuntimeError("ddb write failed")

    workflow = _FailingWorkflow()
    components = _FakeComponents(workflow)
    monkeypatch.setattr("serverless_mcp.extract.handlers.mark_failed._get_components", lambda: components)

    with pytest.raises(RuntimeError, match="ddb write failed"):
        extract_mark_failed_module.lambda_handler(
            {
                "job": _job_payload(),
                "error": "PaddleOCRJobFailed",
                "cause": "timeout",
            },
            None,
        )

    metric_names = [
        json.loads(line)["metric_name"]
        for line in capsys.readouterr().out.splitlines()
        if line.startswith("{") and '"stage": "metric"' in line
    ]

    assert metric_names == [
        "extract.handler.invocation",
        "extract.failure.domain_recorded",
        "extract.handler.failure",
    ]


def test_extract_handler_records_domain_metric_and_handler_failure_separately(
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """
    EN: Verify that the main extract handler keeps domain recording separate from technical failure metrics.
    CN: 验证主 extract handler 将领域记录与技术失败 metric 分离。
    """
    class _FailingWorkflow(_FakeWorkflow):
        def mark_failed(self, *, job, failure):
            raise RuntimeError("ddb write failed")

    workflow = _FailingWorkflow()
    components = _FakeComponents(workflow)
    monkeypatch.setattr("serverless_mcp.extract.handlers.router._get_components", lambda: components)

    with pytest.raises(RuntimeError, match="ddb write failed"):
        lambda_handler(
            {
                "action": "mark_failed",
                "job": _job_payload(),
                "error": "PaddleOCRJobFailed",
                "cause": "timeout",
            },
            None,
        )

    metric_names = [
        json.loads(line)["metric_name"]
        for line in capsys.readouterr().out.splitlines()
        if line.startswith("{") and '"stage": "metric"' in line
    ]

    assert metric_names == [
        "extract.handler.invocation",
        "extract.failure.domain_recorded",
        "extract.handler.failure",
    ]
