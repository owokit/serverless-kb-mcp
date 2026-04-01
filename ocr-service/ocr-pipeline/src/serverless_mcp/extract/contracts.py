"""
EN: Shared contracts for extract state transitions and failure payloads.
CN: extract 状态流转与失败负载的共享契约。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping


@dataclass(frozen=True, slots=True)
class ExtractFailureDetails:
    """
    EN: Normalized failure details passed between Step Functions and the failure handler.
    CN: 在 Step Functions 与失败处理器之间传递的标准化失败信息。
    """

    error: str
    cause: str | None
    message: str
    domain: str


@dataclass(frozen=True, slots=True)
class _StateContract:
    """
    EN: Declarative state-machine contract used to validate the rendered ASL.
    CN: 用于校验渲染后 ASL 的声明式状态机契约。
    """

    name: str
    state_type: str
    parameters: tuple[str, ...] = ()
    result_path: str | None = None
    next_state: str | None = None
    end: bool | None = None
    seconds_path: str | None = None
    default: str | None = None
    nested_parameters: dict[str, tuple[str, ...]] = field(default_factory=dict)


_EXTRACT_STATE_CONTRACTS: tuple[_StateContract, ...] = (
    _StateContract("RouteWorkflow", "Choice", default="PrepareJob"),
    _StateContract(
        "DeleteVectors",
        "Task",
        ("vectorBucketName", "indexName", "keys"),
        end=True,
    ),
    _StateContract("PrepareJob", "Task", ("job", "processing_state"), next_state="RouteByExtension"),
    _StateContract("RouteByExtension", "Choice", default="SyncExtract"),
    _StateContract("SyncExtract", "Task", ("job", "processing_state"), end=True),
    _StateContract("SubmitOcrJob", "Task", ("job",), result_path="$.ocr_submission", next_state="WaitForOcr"),
    _StateContract("WaitForOcr", "Wait", seconds_path="$.poll_interval_seconds", next_state="PollOcrJob"),
    _StateContract(
        "PollOcrJob",
        "Task",
        ("job_id", "poll_attempt", "max_poll_attempts"),
        result_path="$.ocr_status",
        next_state="RouteOcrStatus",
    ),
    _StateContract("RouteOcrStatus", "Choice", default="CheckPollBudget"),
    _StateContract("BuildOcrJobFailure", "Pass", ("job",), next_state="MarkFailed", nested_parameters={"failure": ("error", "cause.$")} ),
    _StateContract("CheckPollBudget", "Choice", default="PromotePollAttempt"),
    _StateContract(
        "PromotePollAttempt",
        "Pass",
        (
            "job",
            "processing_state",
            "document_extension",
            "poll_interval_seconds",
            "max_poll_attempts",
            "poll_attempt",
            "ocr_submission",
            "ocr_status",
        ),
        next_state="WaitForOcr",
    ),
    _StateContract("BuildPollBudgetFailure", "Pass", ("job",), next_state="MarkFailed", nested_parameters={"failure": ("error", "cause.$")} ),
    _StateContract(
        "PersistOcrResult",
        "Task",
        ("job", "processing_state", "json_url", "markdown_url", "poll_attempt"),
        end=True,
    ),
    _StateContract("MarkFailed", "Task", ("job", "failure"), end=True),
)


def build_extract_failure_details(error: str, cause: str | None = None) -> ExtractFailureDetails:
    """
    EN: Normalize Step Functions failure fields into a bounded contract object.
    CN: 将 Step Functions 的失败字段标准化为带边界的契约对象。
    """
    normalized_error = str(error or "").strip()
    if not normalized_error:
        raise ValueError("error is required for extract failure details")
    normalized_cause = str(cause).strip() if cause is not None else None
    if normalized_cause == "":
        normalized_cause = None
    message = _compose_failure_message(normalized_error, normalized_cause)
    return ExtractFailureDetails(
        error=normalized_error,
        cause=normalized_cause,
        message=message,
        domain=_classify_failure_domain(normalized_error, normalized_cause),
    )


def validate_extract_state_machine_contract(definition: Mapping[str, object]) -> None:
    """
    EN: Validate the rendered extract state machine against the checked-in contract.
    CN: 根据仓库内契约校验渲染后的 extract 状态机。
    """
    states = definition.get("States")
    if not isinstance(states, dict):
        raise ValueError("extract state machine definition must contain a States object")
    if definition.get("StartAt") != "RouteWorkflow":
        raise ValueError("extract state machine definition must start at RouteWorkflow")
    for contract in _EXTRACT_STATE_CONTRACTS:
        state = states.get(contract.name)
        if not isinstance(state, dict):
            raise ValueError(f"extract state machine is missing state: {contract.name}")
        if state.get("Type") != contract.state_type:
            raise ValueError(
                f"extract state machine state {contract.name} must have Type={contract.state_type}"
            )
        if contract.parameters:
            _validate_state_parameters(state, contract)
        if contract.nested_parameters:
            _validate_nested_state_parameters(state, contract)
        if contract.result_path is not None and state.get("ResultPath") != contract.result_path:
            raise ValueError(
                f"extract state machine state {contract.name} must have ResultPath={contract.result_path}"
            )
        if contract.next_state is not None and state.get("Next") != contract.next_state:
            raise ValueError(
                f"extract state machine state {contract.name} must have Next={contract.next_state}"
            )
        if contract.end is not None and state.get("End") is not contract.end:
            raise ValueError(f"extract state machine state {contract.name} must have End={contract.end}")
        if contract.seconds_path is not None and state.get("SecondsPath") != contract.seconds_path:
            raise ValueError(
                f"extract state machine state {contract.name} must have SecondsPath={contract.seconds_path}"
            )
        if contract.default is not None and state.get("Default") != contract.default:
            raise ValueError(
                f"extract state machine state {contract.name} must have Default={contract.default}"
            )


def _validate_state_parameters(state: Mapping[str, object], contract: _StateContract) -> None:
    """
    EN: Validate the parameter keys for one rendered state.
    CN: 校验单个渲染状态的参数键。
    """
    params = state.get("Parameters")
    if not isinstance(params, dict):
        raise ValueError(f"extract state machine state {contract.name} must contain Parameters")
    expected = {f"{name}.$" for name in contract.parameters} | set(contract.nested_parameters.keys())
    actual = set(params.keys())
    if actual != expected:
        raise ValueError(
            f"extract state machine state {contract.name} must define Parameters keys {sorted(expected)}"
        )


def _validate_nested_state_parameters(state: Mapping[str, object], contract: _StateContract) -> None:
    """
    EN: Validate nested parameter contracts such as the Step Functions failure object.
    CN: 校验嵌套参数契约，例如 Step Functions 的 failure 对象。
    """
    params = state.get("Parameters")
    if not isinstance(params, dict):
        raise ValueError(f"extract state machine state {contract.name} must contain Parameters")
    for key, nested_expected in contract.nested_parameters.items():
        nested = params.get(key)
        if not isinstance(nested, dict):
            raise ValueError(
                f"extract state machine state {contract.name} must define nested Parameters for {key}"
            )
        actual = set(nested.keys())
        expected = set(nested_expected)
        if actual != expected:
            raise ValueError(
                f"extract state machine state {contract.name} must define nested Parameters keys {sorted(expected)} for {key}"
            )


def _compose_failure_message(error: str, cause: str | None) -> str:
    """
    EN: Combine structured Step Functions error fields into a bounded diagnostic string.
    CN: 将结构化 Step Functions 错误字段组合成有长度上限的诊断字符串。
    """
    if cause:
        return f"{error}: {cause}"[:1000]
    return error[:1000]


def _classify_failure_domain(error: str, cause: str | None) -> str:
    """
    EN: Categorize extract failures into coarse operational domains for metrics and alerts.
    CN: 将 extract 失败粗分到运维指标与告警可用的领域。
    """
    error_lower = error.lower()
    cause_lower = cause.lower() if cause else ""
    if "ocr" in error_lower or "ocr" in cause_lower:
        return "ocr"
    if "persist" in error_lower or "persist" in cause_lower or "manifest" in error_lower or "manifest" in cause_lower:
        return "persist"
    if "poll" in error_lower or "poll" in cause_lower:
        return "poll"
    if "markfailed" in error_lower or "mark_failed" in error_lower:
        return "failure"
    return "workflow"
