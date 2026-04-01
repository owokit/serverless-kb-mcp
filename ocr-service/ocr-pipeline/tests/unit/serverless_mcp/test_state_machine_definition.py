"""
EN: Tests for Step Functions state machine definition rendering with Lambda ARN placeholders.
CN: 娴嬭瘯 Step Functions state machine 瀹氫箟娓叉煋鍙?Lambda ARN 鍗犱綅绗︺€?
"""

from __future__ import annotations

import json

import pytest

from serverless_mcp.extract.contracts import validate_extract_state_machine_contract
from serverless_mcp.runtime.state_machine_definition import (
    load_extract_state_machine_definition,
    load_vector_cleanup_state_machine_definition,
)


def test_load_extract_state_machine_definition_renders_lambda_arn() -> None:
    """
    EN: Load extract state machine definition renders lambda arn.
    CN: 楠岃瘉 state machine 瀹氫箟娓叉煋 Lambda ARN銆?
    """
    definition = load_extract_state_machine_definition(
        lambda_arns={
            "prepare_job": "arn:aws:lambda:ap-southeast-1:123:function:extract-prepare",
            "sync_extract": "arn:aws:lambda:ap-southeast-1:123:function:extract-sync",
            "submit_ocr_job": "arn:aws:lambda:ap-southeast-1:123:function:extract-submit",
            "poll_ocr_job": "arn:aws:lambda:ap-southeast-1:123:function:extract-poll",
            "persist_ocr_result": "arn:aws:lambda:ap-southeast-1:123:function:extract-persist",
            "mark_failed": "arn:aws:lambda:ap-southeast-1:123:function:extract-failed",
        },
    )

    parsed = json.loads(definition)

    assert parsed["StartAt"] == "RouteWorkflow"
    assert parsed["States"]["RouteWorkflow"]["Default"] == "PrepareJob"
    assert parsed["States"]["RouteWorkflow"]["Choices"][0]["Next"] == "DeleteVectors"
    assert parsed["States"]["DeleteVectors"]["Resource"] == "arn:aws:states:::aws-sdk:s3vectors:deleteVectors"
    assert parsed["States"]["DeleteVectors"]["Parameters"]["vectorBucketName.$"] == "$.cleanup_target.vector_bucket_name"
    assert parsed["States"]["DeleteVectors"]["Parameters"]["indexName.$"] == "$.cleanup_target.vector_index_name"
    assert parsed["States"]["DeleteVectors"]["Parameters"]["keys.$"] == "$.cleanup_target.keys"
    assert parsed["States"]["PrepareJob"]["Resource"] == "arn:aws:lambda:ap-southeast-1:123:function:extract-prepare"
    assert parsed["States"]["SyncExtract"]["Resource"] == "arn:aws:lambda:ap-southeast-1:123:function:extract-sync"
    assert parsed["States"]["WaitForOcr"]["Type"] == "Wait"
    assert "action" not in parsed["States"]["PersistOcrResult"]["Parameters"]
    assert parsed["States"]["PollOcrJob"]["Parameters"]["poll_attempt.$"] == "$.poll_attempt"
    assert parsed["States"]["PollOcrJob"]["Parameters"]["max_poll_attempts.$"] == "$.max_poll_attempts"
    assert parsed["States"]["PersistOcrResult"]["Parameters"]["markdown_url.$"] == "$.ocr_status.markdown_url"
    assert "max_poll_attempts.$" not in parsed["States"]["PersistOcrResult"]["Parameters"]
    assert parsed["States"]["MarkFailed"]["Parameters"]["failure.$"] == "$.failure"
    assert parsed["States"]["CheckPollBudget"]["Choices"][0]["Next"] == "BuildPollBudgetFailure"
    assert parsed["States"]["MarkFailed"]["Resource"] == "arn:aws:lambda:ap-southeast-1:123:function:extract-failed"
    assert "action" not in parsed["States"]["MarkFailed"]["Parameters"]
    validate_extract_state_machine_contract(parsed)


def test_validate_extract_state_machine_contract_rejects_missing_contract_state() -> None:
    """
    EN: Verify that the contract validator rejects missing states.
    CN: 验证契约校验器会拒绝缺失状态。
    """
    with pytest.raises(ValueError, match="must start at RouteWorkflow"):
        validate_extract_state_machine_contract({"States": {}})


def test_load_vector_cleanup_state_machine_definition_renders_delete_vectors_task() -> None:
    """
    EN: Load vector cleanup state machine definition renders DeleteVectors task.
    CN: 鍔犺浇 vector cleanup state machine 瀹氫箟骞惰兘鍙纭湅鍒?DeleteVectors 浠诲姟銆?
    """
    definition = load_vector_cleanup_state_machine_definition()

    parsed = json.loads(definition)

    assert parsed["StartAt"] == "DeleteVectors"
    assert parsed["States"]["DeleteVectors"]["Resource"] == "arn:aws:states:::aws-sdk:s3vectors:deleteVectors"
    assert parsed["States"]["DeleteVectors"]["Parameters"]["vectorBucketName.$"] == "$.cleanup_target.vector_bucket_name"
    assert parsed["States"]["DeleteVectors"]["Parameters"]["indexName.$"] == "$.cleanup_target.vector_index_name"
    assert parsed["States"]["DeleteVectors"]["Parameters"]["keys.$"] == "$.cleanup_target.keys"
