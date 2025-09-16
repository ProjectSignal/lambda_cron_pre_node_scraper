#!/usr/bin/env python3
"""
Local testing script for the Lambda pre-node scraper processor.
Aligns with the API-first architecture by routing all persistence through the REST client.
"""

import json
import os
import sys
from unittest.mock import Mock

# Ensure local imports work when running as a script
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lambda_handler import lambda_handler
from clients import get_clients


def setup_test_environment():
    """Set required environment variables for local testing."""
    os.environ.setdefault("BASE_API_URL", "http://127.0.0.1:5000")
    os.environ.setdefault("INSIGHTS_API_KEY", "local-testing-key")
    os.environ.setdefault("API_TIMEOUT_SECONDS", "15")
    os.environ.setdefault("API_MAX_RETRIES", "2")

    # Provider configuration (use safe defaults unless real credentials supplied)
    os.environ.setdefault("RAPIDAPI_KEY", "test-rapidapi-key")
    os.environ.setdefault("RAPIDAPI_HOST", "test-api-host.rapidapi.com")
    os.environ.setdefault("RAPIDAPI_URL", "/")
    os.environ.setdefault("API_PROVIDER", "rapidapi")
    os.environ.setdefault("PROVIDER_FALLBACK_CHAIN", "rapidapi")

    os.environ.setdefault("REQUEST_TIMEOUT", "15")
    os.environ.setdefault("RETRY_DELAY", "3")
    os.environ.setdefault("MAX_RETRIES", "2")
    os.environ.setdefault("PROCESSING_TIMEOUT", "120")

    os.environ.setdefault("MIN_POPULATED_FIELDS_THRESHOLD", "3")
    os.environ.setdefault("QUALITY_SCORE_THRESHOLD", "70")

    os.environ.setdefault("AWS_LAMBDA_FUNCTION_NAME", "test-pre-node-scraper")

    print("✓ Environment variables configured for API-first testing")


def create_mock_sqs_event(node_ids):
    records = []
    for index, node_id in enumerate(node_ids):
        record = {
            "messageId": f"test-message-{index}",
            "receiptHandle": f"test-receipt-{index}",
            "body": json.dumps({"nodeId": node_id}),
            "eventSource": "aws:sqs",
            "awsRegion": "us-east-1",
        }
        records.append(record)
    return {"Records": records}


def create_mock_direct_event(node_ids=None):
    if node_ids:
        if len(node_ids) == 1:
            return {"nodeId": node_ids[0]}
        return {"nodeIds": node_ids}
    return {"nodeId": "test-node-id"}


def create_mock_context():
    context = Mock()
    context.function_name = "test-pre-node-scraper"
    context.function_version = "$LATEST"
    context.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:test-pre-node-scraper"
    context.memory_limit_in_mb = 512
    context.get_remaining_time_in_millis = lambda: 30000
    context.aws_request_id = "test-request-id"
    return context


def get_test_node_ids(limit: int = 3):
    env_node_ids = os.getenv("TEST_NODE_IDS")
    if env_node_ids:
        try:
            node_ids = json.loads(env_node_ids)
            print(f"✓ Using test node IDs from environment: {node_ids}")
            return node_ids
        except json.JSONDecodeError:
            print("⚠️  Invalid JSON in TEST_NODE_IDS environment variable")

    try:
        nodes = get_clients().nodes.scrape_candidates(limit=limit)
        node_ids = [node.get("_id") or node.get("nodeId") for node in nodes if node.get("_id") or node.get("nodeId")]
        usernames = [node.get("linkedinUsername") for node in nodes]
        if node_ids:
            print(f"✓ Retrieved {len(node_ids)} scrape candidates via API")
            if usernames:
                print(f"   Usernames: {usernames}")
            return node_ids
    except Exception as exc:
        print(f"⚠️  Unable to load scrape candidates via API: {exc}")

    print("⚠️  Provide TEST_NODE_IDS environment variable to run local tests")
    return []


def test_handler_with_valid_nodes():
    print("\n" + "=" * 50)
    print("Testing Lambda handler with valid node IDs (SQS mode)")
    print("=" * 50)

    test_node_ids = get_test_node_ids()
    if not test_node_ids:
        print("❌ No valid test node IDs available")
        return

    event = create_mock_sqs_event(test_node_ids)
    context = create_mock_context()

    try:
        response = lambda_handler(event, context)
        print("\n📊 Handler Response:")
        print(json.dumps(response, indent=2))
    except Exception as exc:  # pragma: no cover - local convenience wrapper
        print(f"❌ Handler raised exception: {exc}")
        import traceback
        traceback.print_exc()


def test_handler_direct_invocation():
    print("\n" + "=" * 50)
    print("Testing Lambda handler with direct invocation")
    print("=" * 50)

    test_node_ids = get_test_node_ids()
    if not test_node_ids:
        print("❌ No valid test node IDs available for direct invocation test")
        return

    event = create_mock_direct_event([test_node_ids[0]])
    context = create_mock_context()

    try:
        response = lambda_handler(event, context)
        print("\n📊 Direct Invocation Response:")
        print(json.dumps(response, indent=2))
    except Exception as exc:  # pragma: no cover - local convenience wrapper
        print(f"❌ Handler raised exception: {exc}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    setup_test_environment()
    test_handler_with_valid_nodes()
    test_handler_direct_invocation()
