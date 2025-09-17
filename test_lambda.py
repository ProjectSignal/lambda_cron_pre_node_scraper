#!/usr/bin/env python3

import os
import json
import sys
from lambda_handler import lambda_handler

# Load environment variables from both local and parent .env files
import sys
sys.path.append('..')
from dotenv import load_dotenv

# Load from local .env first (higher priority)
load_dotenv('.env')
# Load from parent .env as fallback
load_dotenv('../.env')

print(f"🔑 Environment Check:")
print(f"   - RAPIDAPI_KEY: {'✅ Available' if os.getenv('RAPIDAPI_KEY') else '❌ Missing'}")
print(f"   - SCRAPFLY_API_KEY: {'✅ Available' if os.getenv('SCRAPFLY_API_KEY') else '❌ Missing'}")
print(f"   - PROXYCURL_API_KEY: {'✅ Available' if os.getenv('PROXYCURL_API_KEY') else '❌ Missing'}")
print(f"   - BASE_API_URL: {'✅ ' + os.getenv('BASE_API_URL', 'Not Set') if os.getenv('BASE_API_URL') else '❌ Missing'}")
print(f"   - INSIGHTS_API_KEY: {'✅ Available' if os.getenv('INSIGHTS_API_KEY') else '❌ Missing'}")
print("-" * 50)

# Mock AWS Lambda context
class MockContext:
    def __init__(self):
        self.function_name = "pre_node_scraper_test"
        self.function_version = "$LATEST"
        self.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:pre_node_scraper_test"
        self.memory_limit_in_mb = 512
        self.remaining_time_in_millis = lambda: 300000

def test_lambda():
    """Test the Lambda function with the provided nodeId and userId"""

    # Load test event
    with open('test_event.json', 'r') as f:
        event = json.load(f)

    print(f"🚀 Testing Lambda with event: {event}")
    print("-" * 50)

    # Create mock context
    context = MockContext()

    try:
        # Call the Lambda handler
        result = lambda_handler(event, context)

        print(f"✅ Lambda execution completed!")
        print(f"Status Code: {result['statusCode']}")

        # Parse and display the response
        response_body = result.get('body', {})
        if isinstance(response_body, str):
            response_body = json.loads(response_body)

        if result['statusCode'] == 200:
            print(f"🎉 SUCCESS!")
            print(f"📊 Pre-Node Processing Complete:")
            print(f"   - Processed: {response_body.get('processed', 0)}")
            print(f"   - Succeeded: {response_body.get('succeeded', 0)}")
            print(f"   - Failed: {response_body.get('failed', 0)}")
            print(f"   - Profiles Scraped: {response_body.get('profiles_scraped', 0)}")

            if 'results' in response_body:
                print(f"   - ✅ Node processing results available")
                for result_item in response_body['results']:
                    node_id = result_item.get('nodeId', 'Unknown')
                    success = result_item.get('success', False)
                    newly_scraped = result_item.get('newlyScraped', False)
                    already_processed = result_item.get('alreadyProcessed', False)

                    status_icon = "✅" if success else "❌"
                    status_text = []
                    if newly_scraped:
                        status_text.append("newly scraped")
                    if already_processed:
                        status_text.append("already processed")

                    print(f"     {status_icon} Node {node_id}: {', '.join(status_text) if status_text else 'processed'}")

                    if result_item.get('error'):
                        print(f"       Error: {result_item['error']}")

            print()
            print("📝 Note: Node data is now processed via event-based system")

        else:
            print(f"❌ FAILED!")
            print(f"Error: {response_body.get('error', 'Unknown error')}")

    except Exception as e:
        print(f"❌ EXCEPTION: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_lambda()