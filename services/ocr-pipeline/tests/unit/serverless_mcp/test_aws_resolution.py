"""
EN: Tests for Step Functions state machine ARN resolution from canonical ARN references.
CN: 针对规范 ARN 引用的 Step Functions state machine ARN 解析测试。
"""

from __future__ import annotations

import pytest

from serverless_mcp.runtime.aws_resolution import resolve_step_functions_state_machine_arn


def test_resolve_step_functions_state_machine_arn_passthrough() -> None:
    """
    EN: Resolve step functions state machine arn passthrough.
    CN: 楠岃瘉 ARN 鐩存帴閫忎紶銆?
    """
    arn = resolve_step_functions_state_machine_arn(
        state_machine_ref="arn:aws:states:us-east-1:123456789012:stateMachine:extract",
    )

    assert arn == "arn:aws:states:us-east-1:123456789012:stateMachine:extract"


def test_resolve_step_functions_state_machine_arn_rejects_blank_reference() -> None:
    """
    EN: Resolve step functions state machine arn rejects blank reference.
    CN: 同上。
    """
    with pytest.raises(ValueError, match="STEP_FUNCTIONS_STATE_MACHINE_ARN is required"):
        resolve_step_functions_state_machine_arn(state_machine_ref="   ")


def test_resolve_step_functions_state_machine_arn_rejects_name_reference() -> None:
    """
    EN: Resolve step functions state machine arn rejects non-arn references.
    CN: 验证 resolver 会拒绝非 ARN 引用。
    """
    with pytest.raises(ValueError, match="STEP_FUNCTIONS_STATE_MACHINE_ARN must be a full Step Functions ARN"):
        resolve_step_functions_state_machine_arn(state_machine_ref="extract")
