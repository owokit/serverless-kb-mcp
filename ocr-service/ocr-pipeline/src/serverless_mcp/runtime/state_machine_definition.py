"""
EN: Load the source-controlled Step Functions ASL definition for the extract workflow.
CN: 加载受源码控制的提取工作流 Step Functions ASL 定义。
"""
from __future__ import annotations

import json
from importlib import resources

from serverless_mcp.extract.contracts import validate_extract_state_machine_contract


def load_extract_state_machine_definition(*, lambda_arns: dict[str, str]) -> str:
    """
    EN: Load and render the extract workflow ASL with the target Lambda ARNs.
CN: 加载并渲染提取工作流 ASL，填入目标 Lambda ARN。

    Args:
        lambda_arns:
        EN: Dict mapping action keys to Lambda ARNs, must include prepare_job, sync_extract,
                submit_ocr_job, poll_ocr_job, persist_ocr_result, and mark_failed.
CN: 映射动作键到 Lambda ARN 的字典，必须包含 prepare_job、sync_extract、submit_ocr_job、poll_ocr_job、persist_ocr_result 和 mark_failed。

    Returns:
        EN: JSON string with all placeholder tokens replaced by actual Lambda ARNs.
CN: 所有占位符都已替换为真实 Lambda ARN 的 JSON 字符串。

    Raises:
        EN: ValueError when a required Lambda ARN key is missing or blank.
CN: 当缺少必需的 Lambda ARN 键，或其值为空时抛出 ValueError。
    """
    required_keys = (
        "prepare_job",
        "sync_extract",
        "submit_ocr_job",
        "poll_ocr_job",
        "persist_ocr_result",
        "mark_failed",
    )
    for key in required_keys:
        value = str(lambda_arns.get(key) or "").strip()
        if not value:
            raise ValueError(f"lambda_arns[{key}] is required")

    template = resources.files("serverless_mcp").joinpath("workflows/extract_state_machine.asl.json")
    rendered = template.read_text(encoding="utf-8")
    for placeholder, key in (
        ("${PREPARE_LAMBDA_ARN}", "prepare_job"),
        ("${SYNC_LAMBDA_ARN}", "sync_extract"),
        ("${SUBMIT_LAMBDA_ARN}", "submit_ocr_job"),
        ("${POLL_LAMBDA_ARN}", "poll_ocr_job"),
        ("${PERSIST_LAMBDA_ARN}", "persist_ocr_result"),
        ("${MARK_FAILED_LAMBDA_ARN}", "mark_failed"),
    ):
        rendered = rendered.replace(placeholder, str(lambda_arns[key]))
    parsed = json.loads(rendered)
    validate_extract_state_machine_contract(parsed)
    return rendered


def load_vector_cleanup_state_machine_definition() -> str:
    """
    EN: Load the dedicated cleanup workflow ASL for previous-version vector deletion.
    CN: 鍔犺浇鐢ㄤ簬鏃х増鏈悜閲忓垹闄ょ殑鐙珛 cleanup workflow ASL銆?
    """
    template = resources.files("serverless_mcp").joinpath("workflows/vector_cleanup_state_machine.asl.json")
    rendered = template.read_text(encoding="utf-8")
    json.loads(rendered)
    return rendered
