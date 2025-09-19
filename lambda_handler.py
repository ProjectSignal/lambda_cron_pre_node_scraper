import json
from typing import Any, Dict, List, Optional

from config import config
from utils import setup_logging
from processor import PreNodeProcessor, ProcessingOutcome


logger = setup_logging()
_processor: Optional[PreNodeProcessor] = None


def _get_processor() -> PreNodeProcessor:
    global _processor
    if _processor is None:
        logger.info("Bootstrapping pre-node processor")
        config.validate()
        _processor = PreNodeProcessor()
    return _processor


def _parse_sqs_message(record: Dict[str, Any]) -> str:
    body = record.get("body", "{}")
    try:
        message_body = json.loads(body)
    except json.JSONDecodeError as exc:
        logger.error("Invalid SQS message body: %s", exc)
        raise

    node_id = message_body.get("nodeId")
    if not node_id:
        raise ValueError("nodeId not found in message body")
    return node_id


def _parse_direct_invocation(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    payload: Dict[str, Any] = {}

    if "body" in event:
        body = event.get("body")
        if isinstance(body, str):
            try:
                payload = json.loads(body or "{}")
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid request body: {exc}") from exc
        elif isinstance(body, dict):
            payload = body
    if not payload:
        payload = event

    if not payload:
        raise ValueError("Direct invocation must supply nodeId or nodeIds")

    def _normalize_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
        node_id_val = entry.get("nodeId")
        if not node_id_val:
            raise ValueError("nodeId is required for each entry")
        normalized: Dict[str, Any] = {"nodeId": node_id_val}
        if entry.get("userId"):
            normalized["userId"] = entry["userId"]
        return normalized

    if "nodes" in payload and isinstance(payload["nodes"], list):
        return [_normalize_entry(item) for item in payload["nodes"]]

    if "nodeIds" in payload and isinstance(payload["nodeIds"], (list, tuple)):
        user_id = payload.get("userId")
        return [{"nodeId": node_id, **({"userId": user_id} if user_id else {})} for node_id in payload["nodeIds"]]

    if "nodeId" in payload:
        return [_normalize_entry(payload)]

    raise ValueError("Direct invocation must specify nodeId or nodeIds")


def _outcome_to_result(node_id: str, outcome: ProcessingOutcome, *, user_id: Optional[str] = None) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "nodeId": node_id,
        "success": outcome.success,
        "alreadyProcessed": outcome.already_processed,
        "newlyScraped": outcome.newly_scraped,
    }
    if user_id:
        result["userId"] = user_id
    if outcome.error:
        result["error"] = outcome.error
    return result


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    processor = _get_processor()

    records = event.get("Records")
    if records:
        logger.info("Received %s SQS records", len(records))
        batch_item_failures: List[Dict[str, str]] = []
        success_count = 0
        scraped_count = 0

        for record in records:
            message_id = record.get("messageId")
            try:
                node_id = _parse_sqs_message(record)
                outcome = processor.process_node(node_id)
                if outcome.success:
                    success_count += 1
                    if outcome.newly_scraped and not outcome.already_processed:
                        scraped_count += 1
                else:
                    logger.error("Processing failed for node %s: %s", node_id, outcome.error)
                    batch_item_failures.append({
                        "itemIdentifier": message_id or node_id or "unknown",
                    })
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error("Error processing SQS record %s: %s", message_id, exc)
                batch_item_failures.append({
                    "itemIdentifier": message_id or "unknown",
                })

        response: Dict[str, Any] = {
            "statusCode": 200,
            "body": {
                "processed": len(records),
                "succeeded": success_count,
                "failed": len(batch_item_failures),
                "profiles_scraped": scraped_count,
            },
        }
        if batch_item_failures:
            response["batchItemFailures"] = batch_item_failures
        logger.info(
            "SQS batch complete: processed=%s succeeded=%s failed=%s scraped=%s",
            len(records),
            success_count,
            len(batch_item_failures),
            scraped_count,
        )
        return response

    try:
        jobs = _parse_direct_invocation(event)
    except ValueError:
        logger.info("Empty event received; nothing to process")
        return {
            "statusCode": 200,
            "body": {
                "processed": 0,
                "succeeded": 0,
                "failed": 0,
                "message": "No nodes to process",
            },
        }

    logger.info("Direct invocation for %s nodes", len(jobs))
    results: List[Dict[str, Any]] = []
    success_count = 0
    scraped_count = 0

    for job in jobs:
        node_id = job["nodeId"]
        user_id = job.get("userId")
        outcome = processor.process_node(node_id)
        result_entry = _outcome_to_result(node_id, outcome, user_id=user_id)
        results.append(result_entry)
        if outcome.success:
            success_count += 1
            if outcome.newly_scraped and not outcome.already_processed:
                scraped_count += 1

    response_body = {
        "processed": len(jobs),
        "succeeded": success_count,
        "failed": len(jobs) - success_count,
        "profiles_scraped": scraped_count,
        "results": results,
        "success": success_count == len(jobs),
    }
    if len(results) == 1:
        response_body.update(results[0])
    logger.info(
        "Direct processing complete: processed=%s succeeded=%s failed=%s scraped=%s",
        len(jobs),
        success_count,
        len(jobs) - success_count,
        scraped_count,
    )
    return {"statusCode": 200, "body": response_body}
