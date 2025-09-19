# Lambda Pre Node Scraper

Pre Node Scraper Lambda function for processing nodes before main scraping.

## Response Shape

Direct invocations now return a structured body so downstream Step Functions can read the outcome without parsing JSON strings:

```json
{
  "statusCode": 200,
  "body": {
    "processed": 1,
    "succeeded": 1,
    "failed": 0,
    "profiles_scraped": 1,
    "success": true,
    "nodeId": "...",
    "userId": "...",
    "alreadyProcessed": false,
    "newlyScraped": true
  }
}
```

## Test CI/CD Pipeline - Wed Sep 17 16:24:31 IST 2025
