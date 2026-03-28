"""
EN: Tests for DynamoDB batch write retry and exponential backoff logic.
CN: DynamoDB batch write 重试与指数退避逻辑测试。
"""

from __future__ import annotations

import pytest

from serverless_mcp.storage.batch import flush_batch_write


class _FakeDynamoClient:
    # EN: In-memory DynamoDB client stub that returns canned batch_write_item responses.
    # CN: 内存中的 DynamoDB 客户端替身，返回预置的 batch_write_item 响应。
    def __init__(self, responses: list[dict[str, object]]) -> None:
        self.responses = responses
        self.calls = 0

    def batch_write_item(self, *, RequestItems):
        response = self.responses[min(self.calls, len(self.responses) - 1)]
        self.calls += 1
        return response


def test_flush_batch_write_uses_exponential_backoff_until_success() -> None:
    """
    EN: Verify that flush_batch_write retries UnprocessedItems with exponential backoff until drained.
    CN: 验证 flush_batch_write 会对 UnprocessedItems 进行指数退避重试直到清空。
    """
    sleeper_calls: list[float] = []
    client = _FakeDynamoClient(
        [
            {"UnprocessedItems": {"manifest-index": [{"PutRequest": {"Item": {"pk": {"S": "a"}}}}]}},
            {"UnprocessedItems": {}},
        ]
    )

    flush_batch_write(
        client,
        {"manifest-index": [{"PutRequest": {"Item": {"pk": {"S": "a"}}}}]},
        jitter=lambda _low, high: high,
        sleeper=lambda seconds: sleeper_calls.append(seconds),
    )

    assert client.calls == 2
    assert sleeper_calls == [0.05]


def test_flush_batch_write_raises_after_reaching_retry_limit() -> None:
    """
    EN: Verify that flush_batch_write raises RuntimeError when max_attempts is exhausted.
    CN: 验证 flush_batch_write 在 max_attempts 耗尽后抛出 RuntimeError。
    """
    client = _FakeDynamoClient(
        [
            {"UnprocessedItems": {"manifest-index": [{"PutRequest": {"Item": {"pk": {"S": "a"}}}}]}},
            {"UnprocessedItems": {"manifest-index": [{"PutRequest": {"Item": {"pk": {"S": "a"}}}}]}},
        ]
    )

    with pytest.raises(RuntimeError, match="did not drain after 2 attempts"):
        flush_batch_write(
            client,
            {"manifest-index": [{"PutRequest": {"Item": {"pk": {"S": "a"}}}}]},
            max_attempts=2,
            jitter=lambda _low, high: high,
            sleeper=lambda _seconds: None,
        )
