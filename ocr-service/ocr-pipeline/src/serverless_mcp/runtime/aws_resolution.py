"""
EN: Helpers for resolving AWS resource names to ARNs at runtime.
CN: 运行时将 AWS 资源名称解析为 ARN 的辅助函数。
"""
from __future__ import annotations


def resolve_step_functions_state_machine_arn(
    *,
    state_machine_ref: str,
) -> str:
    """
    EN: Return a canonical Step Functions ARN when the supplied reference already uses ARN form.
    CN: 当输入已经是 ARN 形式时，直接返回规范的 Step Functions ARN。
    """
    reference = state_machine_ref.strip()
    if not reference:
        raise ValueError("STEP_FUNCTIONS_STATE_MACHINE_ARN is required")
    # EN: If the reference is already a full ARN, return it directly without reconstruction.
    # CN: 如果引用已经是完整 ARN，则直接返回，无需重建。
    if reference.startswith("arn:"):
        return reference
    raise ValueError("STEP_FUNCTIONS_STATE_MACHINE_ARN must be a full Step Functions ARN")
