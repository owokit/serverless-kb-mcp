"""
EN: Shared Lambda context fixtures for boundary-driven handler tests.
CN: 面向边界驱动 handler 测试的共享 Lambda context fixtures。
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class FakeLambdaContext:
    aws_request_id: str = "req-0001"
    invoked_function_arn: str = "arn:aws:lambda:us-east-1:123456789012:function:handler"
    remaining_ms: int = 2500

    def get_remaining_time_in_millis(self) -> int:
        return self.remaining_ms


def make_lambda_context(**overrides: object) -> FakeLambdaContext:
    data = {
        "aws_request_id": "req-0001",
        "invoked_function_arn": "arn:aws:lambda:us-east-1:123456789012:function:handler",
        "remaining_ms": 2500,
    }
    data.update(overrides)
    return FakeLambdaContext(**data)
