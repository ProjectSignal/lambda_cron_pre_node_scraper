import os
from typing import Optional, List, Dict, Any

from dotenv import load_dotenv

# Load environment variables from .env file for local testing
load_dotenv()


class Config:
    """Configuration loader for the pre-node scraper Lambda."""

    def __init__(self):
        # REST API configuration (replaces MongoDB direct access)
        self.BASE_API_URL = self._get_env("BASE_API_URL", required=True).rstrip("/")
        self.API_KEY = self._get_env("INSIGHTS_API_KEY", required=True)
        self.API_TIMEOUT_SECONDS = int(self._get_env("API_TIMEOUT_SECONDS", default="30"))
        self.API_MAX_RETRIES = int(self._get_env("API_MAX_RETRIES", default="3"))

        # Provider configuration and fallback chain
        self.RAPIDAPI_KEY = self._get_env("RAPIDAPI_KEY")
        self.RAPIDAPI_HOST = self._get_env("RAPIDAPI_HOST")
        self.RAPIDAPI_URL = self._get_env("RAPIDAPI_URL", default="/")

        self.SCRAPFLY_API_KEY = self._get_env("SCRAPFLY_API_KEY")
        self.SCRAPFLY_BASE_URL = self._get_env("SCRAPFLY_BASE_URL", default="https://api.scrapfly.io/scrape")

        self.PROXYCURL_API_KEY = self._get_env("PROXYCURL_API_KEY")
        self.PROXYCURL_BASE_URL = self._get_env("PROXYCURL_BASE_URL", default="https://nubela.co/proxycurl/api/v2/linkedin")

        self.API_PROVIDER = self._get_env("API_PROVIDER", default="rapidapi")
        self.PROVIDER_FALLBACK_CHAIN = self._parse_fallback_chain(
            self._get_env("PROVIDER_FALLBACK_CHAIN", default="rapidapi,scrapfly,proxycurl")
        )

        # Processing behaviour
        self.REQUEST_TIMEOUT = int(self._get_env("REQUEST_TIMEOUT", default="30"))
        self.RETRY_DELAY = int(self._get_env("RETRY_DELAY", default="5"))
        self.MAX_RETRIES = int(self._get_env("MAX_RETRIES", default="2"))
        self.SLEEP_BETWEEN_REQUESTS = float(self._get_env("SLEEP_BETWEEN_REQUESTS", default="1.0"))
        self.PROCESSING_TIMEOUT = int(self._get_env("PROCESSING_TIMEOUT", default="300"))

        # Data validation tuning
        self.MIN_POPULATED_FIELDS_THRESHOLD = int(self._get_env("MIN_POPULATED_FIELDS_THRESHOLD", default="4"))
        self.REQUIRED_FIELDS_FOR_VALIDATION = self._parse_required_fields(
            self._get_env(
                "REQUIRED_FIELDS_FOR_VALIDATION",
                default="linkedinHeadline,about,workExperience,education,skills,currentLocation",
            )
        )
        self.QUALITY_SCORE_THRESHOLD = int(self._get_env("QUALITY_SCORE_THRESHOLD", default="75"))
        self.MINIMUM_HEADLINE_WORDS = int(self._get_env("MINIMUM_HEADLINE_WORDS", default="3"))
        self.MINIMUM_ABOUT_LENGTH = int(self._get_env("MINIMUM_ABOUT_LENGTH", default="50"))
        self.MINIMUM_SKILLS_COUNT = int(self._get_env("MINIMUM_SKILLS_COUNT", default="3"))
        self.REQUIRE_WORK_OR_EDUCATION = self._get_env("REQUIRE_WORK_OR_EDUCATION", default="true").lower() == "true"

        # Metadata
        self.PLATFORM = "linkedin"

    def _get_env(self, key: str, default: Optional[str] = None, required: bool = False) -> str:
        value = os.getenv(key, default)
        if required and not value:
            raise ValueError(f"Required environment variable {key} is not set")
        return value

    def _parse_fallback_chain(self, chain_str: str) -> List[str]:
        return [provider.strip() for provider in chain_str.split(",") if provider.strip()]

    def _parse_required_fields(self, fields_str: str) -> List[str]:
        return [field.strip() for field in fields_str.split(",") if field.strip()]

    def get_configured_providers(self) -> List[str]:
        providers = []
        if self.RAPIDAPI_KEY and self.RAPIDAPI_HOST:
            providers.append("rapidapi")
        if self.SCRAPFLY_API_KEY:
            providers.append("scrapfly")
        if self.PROXYCURL_API_KEY:
            providers.append("proxycurl")
        return providers

    def get_fallback_chain_status(self) -> Dict[str, bool]:
        configured = self.get_configured_providers()
        return {provider: provider in configured for provider in self.PROVIDER_FALLBACK_CHAIN}

    def get_processing_config(self) -> Dict[str, Any]:
        return {
            "retry_delay": self.RETRY_DELAY,
            "max_retries": self.MAX_RETRIES,
            "timeout": self.PROCESSING_TIMEOUT,
            "sleep_between_requests": self.SLEEP_BETWEEN_REQUESTS,
        }

    def get_validation_config(self) -> Dict[str, Any]:
        return {
            "min_populated_fields": self.MIN_POPULATED_FIELDS_THRESHOLD,
            "required_fields": self.REQUIRED_FIELDS_FOR_VALIDATION,
            "quality_score_threshold": self.QUALITY_SCORE_THRESHOLD,
            "minimum_headline_words": self.MINIMUM_HEADLINE_WORDS,
            "minimum_about_length": self.MINIMUM_ABOUT_LENGTH,
            "minimum_skills_count": self.MINIMUM_SKILLS_COUNT,
            "require_work_or_education": self.REQUIRE_WORK_OR_EDUCATION,
        }

    def get_provider_config(self, provider: str) -> Dict[str, Any]:
        if provider == "rapidapi":
            return {
                "api_key": self.RAPIDAPI_KEY,
                "api_host": self.RAPIDAPI_HOST,
                "api_url": self.RAPIDAPI_URL,
                "timeout": self.REQUEST_TIMEOUT,
            }
        if provider == "scrapfly":
            return {
                "api_key": self.SCRAPFLY_API_KEY,
                "base_url": self.SCRAPFLY_BASE_URL,
                "timeout": self.REQUEST_TIMEOUT,
            }
        if provider == "proxycurl":
            return {
                "api_key": self.PROXYCURL_API_KEY,
                "base_url": self.PROXYCURL_BASE_URL,
                "timeout": self.REQUEST_TIMEOUT,
            }
        raise ValueError(f"Unknown provider: {provider}")

    def validate(self) -> None:
        required = ["BASE_API_URL", "API_KEY"]
        missing = [var for var in required if not getattr(self, var)]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

        configured_providers = self.get_configured_providers()
        if not configured_providers:
            raise ValueError("No API providers are configured. Configure at least one provider credential.")

        invalid_providers = [p for p in self.PROVIDER_FALLBACK_CHAIN if p not in ["rapidapi", "scrapfly", "proxycurl"]]
        if invalid_providers:
            raise ValueError(f"Invalid providers in fallback chain: {invalid_providers}")

        unconfigured = [p for p in self.PROVIDER_FALLBACK_CHAIN if p not in configured_providers]
        if unconfigured:
            print(f"Warning: Providers in fallback chain are not configured: {unconfigured}")

        if self.REQUEST_TIMEOUT <= 0:
            raise ValueError("REQUEST_TIMEOUT must be greater than 0")
        if self.RETRY_DELAY < 0:
            raise ValueError("RETRY_DELAY must be greater than or equal to 0")
        if self.MAX_RETRIES < 0:
            raise ValueError("MAX_RETRIES must be greater than or equal to 0")
        if not (0 <= self.QUALITY_SCORE_THRESHOLD <= 100):
            raise ValueError("QUALITY_SCORE_THRESHOLD must be between 0 and 100")
        if self.MINIMUM_HEADLINE_WORDS < 1:
            raise ValueError("MINIMUM_HEADLINE_WORDS must be at least 1")
        if self.MINIMUM_ABOUT_LENGTH < 0:
            raise ValueError("MINIMUM_ABOUT_LENGTH must be non-negative")
        if self.MINIMUM_SKILLS_COUNT < 0:
            raise ValueError("MINIMUM_SKILLS_COUNT must be non-negative")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "api": {
                "base_url": self.BASE_API_URL,
                "timeout": self.API_TIMEOUT_SECONDS,
                "max_retries": self.API_MAX_RETRIES,
            },
            "processing": self.get_processing_config(),
            "validation": self.get_validation_config(),
            "providers": {
                "configured": self.get_configured_providers(),
                "fallback_chain": self.PROVIDER_FALLBACK_CHAIN,
                "fallback_status": self.get_fallback_chain_status(),
            },
            "metadata": {
                "platform": self.PLATFORM,
            },
        }


config = Config()
