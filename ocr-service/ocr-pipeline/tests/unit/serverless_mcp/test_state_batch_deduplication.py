"""
EN: Tests for batch deduplication in state repositories.
CN: 鍚屼笂銆?
"""

from serverless_mcp.storage.state.execution_state_repository import ExecutionStateRepository
from serverless_mcp.storage.state.object_state_repository import ObjectStateRepository


class _CompositeCapturingDynamoDbClient:
    # EN: Captures batch_get_item requests and exposes a composite table schema.
    # CN: 鍚屼笂銆?
    def __init__(self) -> None:
        self.batch_calls = []

    def describe_table(self, **_kwargs):
        return {
            "Table": {
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "record_type", "KeyType": "RANGE"},
                ]
            }
        }

    def batch_get_item(self, **kwargs):
        self.batch_calls.append(kwargs)
        return {"Responses": {}, "UnprocessedKeys": {}}


class _TestObjectStateRepository(ObjectStateRepository):
    def __init__(self, *, dynamodb_client) -> None:
        self._table_name = "object-state"
        self._ddb = dynamodb_client
        self._table_sort_key_name = "record_type"

    def _build_state_key(self, object_pk: str) -> dict[str, dict[str, str]]:
        return {
            "pk": {"S": object_pk},
            "record_type": {"S": "STATE"},
        }


def test_object_state_repository_dedupes_duplicate_batch_keys_before_request() -> None:
    """
    EN: ObjectStateRepository should dedupe duplicate primary keys before batch_get_item.
    CN: ObjectStateRepository 鍦ㄨ皟鐢?batch_get_item 涔嬪墠搴旇鍏堝幓閲嶅涓婚敭銆?
    """
    dynamodb = _CompositeCapturingDynamoDbClient()
    repo = _TestObjectStateRepository(dynamodb_client=dynamodb)

    result = repo.get_states_batch(
        object_pks=[
            "tenant-a#bucket-a#docs%2Fguide.pdf",
            "tenant-a#bucket-a#docs%2Fguide.pdf",
            "tenant-a#bucket-a#docs%2Fguide.pdf",
        ]
    )

    assert result == {
        "tenant-a#bucket-a#docs%2Fguide.pdf": None,
    }
    assert len(dynamodb.batch_calls) == 1
    request_keys = dynamodb.batch_calls[0]["RequestItems"]["object-state"]["Keys"]
    assert request_keys == [
        {
            "pk": {"S": "tenant-a#bucket-a#docs%2Fguide.pdf"},
            "record_type": {"S": "STATE"},
        }
    ]


def test_execution_state_repository_dedupes_duplicate_batch_keys_before_request() -> None:
    """
    EN: ExecutionStateRepository should dedupe duplicate primary keys before batch_get_item.
    CN: ExecutionStateRepository 鍦ㄨ皟鐢?batch_get_item 涔嬪墠搴旇鍏堝幓閲嶅涓婚敭銆?
    """
    dynamodb = _CompositeCapturingDynamoDbClient()
    repo = ExecutionStateRepository(table_name="execution-state", dynamodb_client=dynamodb)

    result = repo.get_states_batch(
        object_pks=[
            "tenant-a#bucket-a#docs%2Fguide.pdf",
            "tenant-a#bucket-a#docs%2Fguide.pdf",
            "tenant-a#bucket-a#docs%2Fguide.pdf",
        ]
    )

    assert result == {
        "tenant-a#bucket-a#docs%2Fguide.pdf": None,
    }
    assert len(dynamodb.batch_calls) == 1
    request_keys = dynamodb.batch_calls[0]["RequestItems"]["execution-state"]["Keys"]
    assert request_keys == [
        {
            "pk": {"S": "tenant-a#bucket-a#docs%2Fguide.pdf"},
        }
    ]
