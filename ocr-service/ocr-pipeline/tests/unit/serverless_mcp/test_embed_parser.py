"""
EN: Tests for parsing embedding job messages from SQS events.
CN: 娴嬭瘯浠?SQS 浜嬩欢瑙ｆ瀽 embedding job 娑堟伅銆?
"""

from serverless_mcp.core.parsers import parse_embedding_event


def test_parse_embedding_event_preserves_previous_version_context() -> None:
    """
    EN: Verify that the parser preserves previous_version_id, previous_manifest_s3_uri, and security_scope from the SQS body.
    CN: 同上。
    """
    jobs = parse_embedding_event(
        {
            "Records": [
                {
                    "body": """
                    {
                      "source": {
                        "tenant_id": "tenant-a",
                        "bucket": "bucket-a",
                        "key": "docs/guide.pdf",
                        "version_id": "v2",
                        "security_scope": ["team-a"]
                      },
                      "profile_id": "gemini-default",
                      "trace_id": "trace-1",
                      "manifest_s3_uri": "s3://manifest-bucket/manifests/v2.json",
                      "previous_version_id": "v1",
                      "previous_manifest_s3_uri": "s3://manifest-bucket/manifests/v1.json",
                      "requests": []
                    }
                    """,
                }
            ]
        }
    )

    assert len(jobs) == 1
    assert jobs[0].profile_id == "gemini-default"
    assert jobs[0].previous_version_id == "v1"
    assert jobs[0].previous_manifest_s3_uri == "s3://manifest-bucket/manifests/v1.json"
    assert jobs[0].source.security_scope == ("team-a",)

