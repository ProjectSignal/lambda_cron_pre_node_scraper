import datetime
import time
from dataclasses import dataclass
from typing import Dict, Any, Optional

from clients import ServiceClients, get_clients
from external_apis import ProfileAPIManager, api_manager
from data_transformer import DataTransformer, validate_provider_data
from utils import get_logger
from config import config
from errors import (
    ErrorTaxonomy,
    error_handler,
    create_api_error,
    create_data_quality_error,
    create_database_error,
)


logger = get_logger(__name__)


@dataclass
class ProcessingOutcome:
    """Represents the result of processing a single node."""

    success: bool
    newly_scraped: bool = False
    already_processed: bool = False
    error: Optional[str] = None


class PreNodeProcessor:
    """Core orchestration for LinkedIn pre-node scraping."""

    def __init__(self, *, clients: Optional[ServiceClients] = None, api_manager: ProfileAPIManager = api_manager):
        self.clients = clients or get_clients()
        self.node_repo = self.clients.nodes
        self.api_manager = api_manager
        self.transformer = DataTransformer()
        self.logger = get_logger(__name__)

        available_providers = self.api_manager.get_available_providers()
        self.logger.info("Initialized processor with providers: %s", available_providers)
        self.logger.info("Fallback chain: %s", config.PROVIDER_FALLBACK_CHAIN)

    def process_node(self, node_id: str) -> ProcessingOutcome:
        """Process a single node and persist the resulting profile data."""
        linkedin_username: Optional[str] = None
        try:
            node = self.node_repo.fetch(node_id)
            if not node:
                error = ErrorTaxonomy.create_error("DB_003", f"Node {node_id} not found", node_id=node_id)
                error_handler.handle_error(error)
                return ProcessingOutcome(success=False, error=error.to_log_message())

            linkedin_username = node.get("linkedinUsername")
            if not linkedin_username:
                error = ErrorTaxonomy.create_error(
                    "BL_001",
                    f"Missing linkedinUsername for node {node_id}",
                    node_id=node_id,
                )
                error_handler.handle_error(error)
                self.node_repo.mark_error(node_id, error.to_log_message())
                return ProcessingOutcome(success=False, error=error.to_log_message())

            if node.get("apiScraped") and node.get("scrapped"):
                error = ErrorTaxonomy.create_error(
                    "BL_002",
                    f"Node {node_id} ({linkedin_username}) already processed",
                    node_id=node_id,
                    linkedin_username=linkedin_username,
                )
                error_handler.handle_error(error, "info")
                return ProcessingOutcome(success=True, already_processed=True)

            self.logger.info("Processing node %s (%s)", node_id, linkedin_username)

            try:
                self.node_repo.touch_last_attempted(node_id)
            except Exception as exc:  # pragma: no cover - logging side effect only
                self.logger.error("Failed to update lastAttemptedAt for %s: %s", node_id, exc)

            outcome = self._process_profile_with_retry(node_id, linkedin_username)
            if not outcome.success:
                self.logger.error(
                    "Failed to process node %s (%s): %s",
                    node_id,
                    linkedin_username,
                    outcome.error,
                )
                self.node_repo.mark_error(node_id, outcome.error)
            else:
                self.logger.info("Successfully processed node %s (%s)", node_id, linkedin_username)
            return outcome

        except Exception as exc:  # pragma: no cover - defensive logging
            error = error_handler.handle_exception(
                exc,
                {"node_id": node_id, "linkedin_username": linkedin_username},
            )
            self.node_repo.mark_error(node_id, error.to_log_message())
            return ProcessingOutcome(success=False, error=error.to_log_message())

    def _process_profile_with_retry(self, node_id: str, linkedin_username: str) -> ProcessingOutcome:
        max_retries = config.MAX_RETRIES
        retry_delay = config.RETRY_DELAY

        for attempt in range(max_retries):
            try:
                api_result = self.api_manager.fetch_with_fallback(linkedin_username)
                if api_result["success"] and api_result["data"]:
                    profile_data = api_result["data"]
                    provider_used = api_result["provider"]
                    self.logger.info(
                        "Fetched data for %s (%s) via %s on attempt %s",
                        linkedin_username,
                        node_id,
                        provider_used,
                        attempt + 1,
                    )

                    if (
                        isinstance(profile_data, dict)
                        and profile_data.get("success") is False
                        and "can't be accessed" in profile_data.get("message", "").lower()
                    ):
                        error = ErrorTaxonomy.create_error(
                            "API_004",
                            f"Profile cannot be accessed (Attempt {attempt + 1}/{max_retries}): {profile_data.get('message')}",
                            provider_used,
                            node_id,
                            linkedin_username,
                        )
                        error_handler.handle_error(error)

                        if self._delete_inaccessible_node(node_id, linkedin_username):
                            return ProcessingOutcome(success=True, newly_scraped=False)
                        failure = create_database_error("Failed to delete inaccessible profile", node_id)
                        error_handler.handle_error(failure)
                        return ProcessingOutcome(success=False, error=failure.to_log_message())

                    validation_result = validate_provider_data(profile_data, provider_used)
                    if not validation_result["valid"]:
                        error = create_data_quality_error(
                            f"Data validation failed: {validation_result['quality_report']}",
                            provider_used,
                            node_id,
                            linkedin_username,
                            validation_result["quality_score"],
                        )
                        error_handler.handle_error(error)

                        if validation_result["quality_score"] < config.QUALITY_SCORE_THRESHOLD:
                            threshold_error = ErrorTaxonomy.create_error(
                                "DQ_003",
                                (
                                    f"Quality score {validation_result['quality_score']} below threshold "
                                    f"{config.QUALITY_SCORE_THRESHOLD}"
                                ),
                                provider_used,
                                node_id,
                                linkedin_username,
                                {
                                    "quality_score": validation_result["quality_score"],
                                    "threshold": config.QUALITY_SCORE_THRESHOLD,
                                },
                            )
                            error_handler.handle_error(threshold_error)

                            if attempt < max_retries - 1:
                                time.sleep(retry_delay)
                                retry_delay *= 1.5
                                continue
                            return ProcessingOutcome(success=False, error=threshold_error.to_log_message())

                    transformed_data = self.transformer.transform_data(profile_data, provider_used)
                    if transformed_data:
                        quality_score = transformed_data.get("quality_score", 0)
                        self.logger.info(
                            "Transformed data for %s (%s) via %s (Quality Score: %s)",
                            linkedin_username,
                            node_id,
                            provider_used,
                            quality_score,
                        )
                        updated = self._update_node_with_data(node_id, linkedin_username, transformed_data)
                        if updated:
                            return ProcessingOutcome(success=True, newly_scraped=True)
                        failure = create_database_error("Failed to update node in database", node_id)
                        error_handler.handle_error(failure)
                        return ProcessingOutcome(success=False, error=failure.to_log_message())

                    transform_error = ErrorTaxonomy.create_error(
                        "TRANS_001",
                        f"Data transformation failed for provider {provider_used}",
                        provider_used,
                        node_id,
                        linkedin_username,
                    )
                    error_handler.handle_error(transform_error)
                    return ProcessingOutcome(success=False, error=transform_error.to_log_message())

                error_msg = api_result.get("error", "Unknown error")
                error = create_api_error(
                    f"All providers failed on attempt {attempt + 1}/{max_retries}: {error_msg}",
                    None,
                    node_id,
                    linkedin_username,
                )
                error_handler.handle_error(error)

                if attempt < max_retries - 1:
                    self.logger.info("Retrying in %s seconds...", retry_delay)
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    final_error = ErrorTaxonomy.create_error(
                        "API_001",
                        f"All providers failed after all retries: {error_msg}",
                        None,
                        node_id,
                        linkedin_username,
                    )
                    error_handler.handle_error(final_error)
                    return ProcessingOutcome(success=False, error=final_error.to_log_message())

            except Exception as exc:  # pragma: no cover - defensive logging
                error = error_handler.handle_exception(
                    exc,
                    {
                        "provider": None,
                        "node_id": node_id,
                        "linkedin_username": linkedin_username,
                        "attempt": attempt + 1,
                    },
                )
                if attempt < max_retries - 1:
                    self.logger.info("Retrying in %s seconds...", retry_delay)
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    return ProcessingOutcome(success=False, error=error.to_log_message())

        return ProcessingOutcome(success=False, error="Max retries reached")

    def _delete_inaccessible_node(self, node_id: str, linkedin_username: str) -> bool:
        try:
            result = self.node_repo.delete(node_id)
            if result:
                self.logger.info("Deleted profile %s (%s) as it was inaccessible", node_id, linkedin_username)
                return True
            self.logger.warning(
                "Attempted to delete inaccessible profile %s (%s), but it was not found",
                node_id,
                linkedin_username,
            )
            return True
        except Exception as exc:  # pragma: no cover - defensive logging
            self.logger.error("Failed to delete inaccessible profile %s: %s", node_id, exc)
            return False

    def _update_node_with_data(self, node_id: str, linkedin_username: str, transformed_data: Dict[str, Any]) -> bool:
        try:
            update_payload = {
                **transformed_data,
                "scrapped": True,
                "apiScraped": True,
                "lastAttemptedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "descriptionGenerated": False,
            }

            primary_success = self.node_repo.update_node(node_id, update_payload)
            if primary_success:
                self.logger.info("Updated primary profile %s (%s)", node_id, linkedin_username)
                duplicates_updated = self.node_repo.update_duplicates(linkedin_username, node_id, update_payload)
                if duplicates_updated > 0:
                    self.logger.info("Updated %s duplicate entries for username '%s'", duplicates_updated, linkedin_username)
                else:
                    self.logger.debug("No additional unscraped duplicates found for username '%s'", linkedin_username)
                return True

            self.logger.error("Failed to update primary profile %s (%s) via API", node_id, linkedin_username)
            return False

        except Exception as exc:  # pragma: no cover - defensive logging
            self.logger.error("Error updating node %s with data: %s", node_id, exc)
            return False

    def get_provider_status(self) -> Dict[str, Any]:
        try:
            provider_tests = self.api_manager.test_all_providers()
            available = self.api_manager.get_available_providers()
            fallback_status = config.get_fallback_chain_status()
            return {
                "available_providers": available,
                "provider_tests": provider_tests,
                "fallback_chain": config.PROVIDER_FALLBACK_CHAIN,
                "fallback_status": fallback_status,
                "quality_threshold": config.QUALITY_SCORE_THRESHOLD,
                "min_fields_threshold": config.MIN_POPULATED_FIELDS_THRESHOLD,
            }
        except Exception as exc:  # pragma: no cover - surface error information
            error = error_handler.handle_exception(exc, {"context": "get_provider_status"})
            return {"error": error.to_log_message()}

    def get_error_summary(self) -> Dict[str, Any]:
        try:
            error_summary = error_handler.get_error_summary()
            recent_errors = error_handler.get_recent_errors(limit=10)
            return {
                "error_statistics": error_summary,
                "recent_errors": [error.to_dict() for error in recent_errors],
                "processor_health": {
                    "total_errors": error_summary.get("total", 0),
                    "critical_errors": error_summary.get("by_severity", {}).get("critical", 0),
                    "retryable_errors": error_summary.get("retryable", 0),
                    "provider_errors": error_summary.get("by_provider", {}),
                },
            }
        except Exception as exc:  # pragma: no cover - surface error information
            logger.error("Error getting error summary: %s", exc)
            return {"error": str(exc)}

    def close(self) -> None:
        """Compatibility placeholder for cleanup hooks."""
        # No persistent connections to close; API session re-used via clients module.
        return
