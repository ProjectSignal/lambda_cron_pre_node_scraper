# Lambda Pre-Node Scraper Deployment Guide

## Prerequisites
- [x] AWS CLI configured with appropriate permissions
- [x] Docker installed for container builds
- [x] BASE_API_URL and INSIGHTS_API_KEY configured
- [x] RapidAPI credentials available

> ℹ️  MongoDB connectivity is no longer required; persistence now flows through Brace REST APIs.

## Deployment Checklist

### 1. Environment Variables
Set the following environment variables in AWS Lambda:

```bash
# Required
BASE_API_URL=https://api.brace.so
INSIGHTS_API_KEY=your-api-key
RAPIDAPI_KEY=your-rapidapi-key
RAPIDAPI_HOST=your-rapidapi-host.rapidapi.com

# Optional Configuration
PROVIDER_FALLBACK_CHAIN=rapidapi,scrapfly,proxycurl
API_PROVIDER=rapidapi
REQUEST_TIMEOUT=30
RETRY_DELAY=5
MAX_RETRIES=2
PROCESSING_TIMEOUT=300
MIN_POPULATED_FIELDS_THRESHOLD=4
QUALITY_SCORE_THRESHOLD=75
```

### 2. IAM Policy
Attach this policy to the Lambda execution role:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "sqs:ReceiveMessage",
                "sqs:DeleteMessage",
                "sqs:GetQueueAttributes"
            ],
            "Resource": "arn:aws:sqs:*:*:pre-node-scraping-queue"
        }
    ]
}
```

### 3. Container Build & Deploy

```bash
# Build the container
docker build -t pre-node-scraper .

# Tag for ECR
docker tag pre-node-scraper:latest YOUR_ACCOUNT.dkr.ecr.REGION.amazonaws.com/pre-node-scraper:latest

# Push to ECR
aws ecr get-login-password --region REGION | docker login --username AWS --password-stdin YOUR_ACCOUNT.dkr.ecr.REGION.amazonaws.com
docker push YOUR_ACCOUNT.dkr.ecr.REGION.amazonaws.com/pre-node-scraper:latest
```

### 4. Lambda Configuration
- **Runtime**: Container image
- **Image URI**: YOUR_ACCOUNT.dkr.ecr.REGION.amazonaws.com/pre-node-scraper:latest
- **Handler**: lambda_handler.lambda_handler (set automatically for containers)
- **Memory**: 512 MB - 1024 MB (adjust based on processing requirements)
- **Timeout**: 5 minutes (300 seconds)
- **Concurrent executions**: 3-5 (adjust based on RapidAPI rate limits)

### 5. SQS Trigger Setup (Optional)
- **Event source mapping**: SQS queue for node processing
- **Batch size**: 1-5 (start with 1 for testing)
- **Maximum batching window**: 10 seconds
- **Partial batch failure reporting**: Enabled
- **Dead letter queue**: Configured for failed processing

### 6. Alternative Trigger Options

#### Option A: EventBridge Scheduled Rule
For batch processing at regular intervals:
- **Rule type**: Schedule expression
- **Schedule**: `rate(1 hour)` or `cron(0 */2 * * ? *)`
- **Target**: Lambda function
- **Input**: `{"batch": true, "limit": 50}`

#### Option B: Direct Invocation
For on-demand processing via API:
- **Input format**: 
  - Single node: `{"nodeId": "node_id"}`
  - Multiple nodes: `{"nodeIds": ["id1", "id2"]}`
  - Batch mode: `{"batch": true, "limit": 100}`

### 7. VPC Configuration (if Brace API is private)
- **VPC**: Same VPC as Brace API
- **Subnets**: Private subnets with NAT gateway access
- **Security groups**: Allow outbound 27017 to Brace API and 443 for RapidAPI

## Testing

### Local Testing
```bash
# Set environment variables
export MONGODB_URI="brace-api://localhost:27017"
export RAPIDAPI_KEY="your-test-api-key"
export RAPIDAPI_HOST="your-rapidapi-host.rapidapi.com"
export TEST_NODE_IDS='["node_id_1", "node_id_2"]'

# Run tests
python test_local.py
```

### Container Testing
```bash
# Run in container
docker run -it --rm \
  -e MONGODB_URI="brace-api://host.docker.internal:27017" \
  -e RAPIDAPI_KEY="your-api-key" \
  -e RAPIDAPI_HOST="your-rapidapi-host.rapidapi.com" \
  -e TEST_NODE_IDS='["node_id_1"]' \
  pre-node-scraper python test_local.py
```

### Lambda Testing
```bash
# Test with SQS event
aws lambda invoke \
  --function-name pre-node-scraper \
  --payload '{"Records":[{"body":"{\"nodeId\":\"test_node_id\"}"}]}' \
  response.json

# Test with direct invocation
aws lambda invoke \
  --function-name pre-node-scraper \
  --payload '{"nodeId":"test_node_id"}' \
  response.json

# Test batch processing
aws lambda invoke \
  --function-name pre-node-scraper \
  --payload '{"batch":true,"limit":10}' \
  response.json
```

## Monitoring

### CloudWatch Metrics to Monitor
- **Duration**: Processing time per batch
- **Errors**: Failed invocations
- **Throttles**: Concurrent execution limits hit
- **RapidAPI Rate Limits**: Monitor API call failures

### CloudWatch Logs
- Search for "ERROR" and "Failed" patterns
- Monitor API scraping success rates
- Track duplicate node processing statistics
- Watch for inaccessible profile deletions

### Custom Metrics to Create
- Profiles successfully scraped per hour
- API errors by type
- Database update failures
- Processing time per profile

## Troubleshooting

### Common Issues

1. **RapidAPI Rate Limits**: 
   - Reduce MAX_CONCURRENT_WORKERS
   - Increase RETRY_DELAY
   - Monitor API response headers for rate limit info

2. **Brace API Connection Timeouts**: 
   - Verify network connectivity
   - Check VPC/security group settings
   - Increase connection timeout in config

3. **Profile Data Transformation Errors**:
   - Check API response format changes
   - Verify transform_data logic
   - Review data validation errors

4. **Memory Issues**: 
   - Increase Lambda memory allocation
   - Monitor concurrent processing
   - Check for memory leaks in long-running processes

5. **SQS Message Processing Failures**:
   - Verify message format
   - Check dead letter queue for failed messages
   - Review batch failure handling

### Debug Commands
```bash
# Check container locally
docker run -it --rm pre-node-scraper sh

# Test imports
python -c "from lambda_handler import lambda_handler; print('OK')"

# Test REST client bootstrap
python -c "from clients import get_clients; clients=get_clients(); print('API client ready')"

# Test provider connection
python -c "from external_apis import api_manager; print(api_manager.get_available_providers())"

# Test data transformation
python -c "from data_transformer import DataTransformer; t=DataTransformer(); print('Transformer OK')"
```

## Performance Tuning

### Batch Size Optimization
- Start with batch size 1 for SQS processing
- For direct invocation, limit to 10-20 nodes per call
- Monitor processing time and adjust accordingly

### Memory Allocation
- 512 MB: Basic processing (1-5 nodes)
- 1024 MB: Standard batch processing (10-50 nodes)
- 1536 MB: Large batch processing or complex profiles

### API Rate Limiting
- RapidAPI typically allows 100-1000 requests per minute
- Set MAX_CONCURRENT_WORKERS to stay within limits
- Use exponential backoff for retries

### Concurrency Settings
- Reserved concurrency: 3-5 for controlled processing
- Monitor RapidAPI usage to avoid rate limits
- Consider downstream Brace API impact

## Security

### Secrets Management
- Use AWS Secrets Manager for sensitive credentials
- Store RapidAPI keys in environment variables
- Rotate API keys regularly

### Network Security
- Use private subnets for Lambda if Brace API is private
- Restrict security group access to required ports only
- Enable CloudTrail logging for API calls

### Data Privacy
- Profile data is handled according to LinkedIn's terms
- Implement proper error handling to avoid data leaks
- Log processing statistics without exposing personal data

## Scaling Considerations

### Horizontal Scaling
- Multiple Lambda functions can process different node batches
- Use SQS for distributed processing
- Consider partitioning by LinkedIn username or creation date

### Vertical Scaling
- Increase Lambda memory for faster processing
- Optimize Brace API queries for better performance
- Use connection pooling for database efficiency

## Cost Optimization

### Lambda Costs
- Monitor invocation duration and frequency
- Use reserved concurrency to control costs
- Consider switching to provisioned concurrency for consistent loads

### API Costs
- Monitor RapidAPI usage and billing
- Implement caching for frequently accessed profiles
- Use deduplication to avoid unnecessary API calls

### Data Transfer Costs
- Minimize data transfer between regions
- Use compression for large data transfers
- Monitor Brace API data transfer costs