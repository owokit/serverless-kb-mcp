"""
EN: Tests for EmbeddingProjectionStateRepository batch loading behavior.
CN: 鍚屼笂銆?
"""

from serverless_mcp.storage.projection.repository import EmbeddingProjectionStateRepository


class _CapturingDynamoDbClient:
    # EN: Captures batch_get_item requests for assertions.
    # CN: 鍚屼笂銆?
    def __init__(self) -> None:
        self.batch_calls = []

    def batch_get_item(self, **kwargs):
        self.batch_calls.append(kwargs)
        return {"Responses": {}, "UnprocessedKeys": {}}


def test_projection_repository_dedupes_duplicate_batch_keys_before_request() -> None:
    """
    EN: Batch projection loading should dedupe duplicate keys before calling DynamoDB.
    CN: projection 批量读取在调用 DynamoDB 之前应先去重。
    """
    dynamodb = _CapturingDynamoDbClient()
    repo = EmbeddingProjectionStateRepository(table_name="projection-state", dynamodb_client=dynamodb)

    result = repo.get_states_batch(
        keys=[
            ("tenant-a#bucket-a#docs%2Fguide.pdf", "v2", "gemini-default"),
            ("tenant-a#bucket-a#docs%2Fguide.pdf", "v2", "gemini-default"),
            ("tenant-a#bucket-a#docs%2Fguide.pdf", "v2", "gemini-default"),
        ]
    )

    assert result == {
        ("tenant-a#bucket-a#docs%2Fguide.pdf", "v2", "gemini-default"): None,
    }
    assert len(dynamodb.batch_calls) == 1
    request_keys = dynamodb.batch_calls[0]["RequestItems"]["projection-state"]["Keys"]
    assert request_keys == [
        {
            "pk": {"S": "tenant-a#bucket-a#docs%2Fguide.pdf#v2"},
            "sk": {"S": "gemini-default"},
        }
    ]
