"""HTTP client and repository abstractions for lambda_pre_node_scraper."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import config
from utils import get_logger


logger = get_logger(__name__)


class ApiClient:
    """Lightweight REST client with retry-aware session."""

    def __init__(self, base_url: str, api_key: str, timeout: int, max_retries: int):
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._timeout = timeout
        self._session: Session = Session()

        retry = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=(408, 429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST", "PUT", "PATCH", "DELETE"),
        )
        adapter = HTTPAdapter(max_retries=retry)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)

    def _headers(self) -> Dict[str, str]:
        return {
            "X-API-Key": self._api_key,
            "Content-Type": "application/json",
        }

    def _url(self, route: str) -> str:
        route = route.lstrip("/")
        if not route.startswith("api/"):
            route = f"api/{route}"
        return f"{self._base_url}/{route}"

    def request(self, method: str, route: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = self._url(route)
        logger.debug("API %s %s", method.upper(), url)
        response = self._session.request(
            method=method.upper(),
            url=url,
            headers=self._headers(),
            data=json.dumps(payload or {}),
            timeout=self._timeout,
        )
        if response.status_code >= 400:
            logger.error("API %s %s failed: %s %s", method.upper(), url, response.status_code, response.text)
            raise RuntimeError(f"API request failed with status {response.status_code}: {response.text}")
        if not response.text:
            return {}
        return response.json()

    def get(self, route: str, *, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = self._url(route)
        logger.debug("API GET %s", url)
        response = self._session.get(
            url,
            headers=self._headers(),
            params=params,
            timeout=self._timeout,
        )
        if response.status_code >= 400:
            logger.error("API GET %s failed: %s %s", url, response.status_code, response.text)
            raise RuntimeError(f"API GET failed with status {response.status_code}: {response.text}")
        if not response.text:
            return {}
        return response.json()


def _utc_iso(dt: Optional[datetime] = None) -> str:
    value = dt or datetime.now(timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


@dataclass
class NodeRepository:
    """REST-backed node persistence layer."""

    api_client: ApiClient

    def fetch(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a node by identifier."""
        # API Route: nodes.getById, Input: {nodeId}, Output: {data: {...}}
        response = self.api_client.get(f"nodes/{node_id}")
        return response.get("data", response)

    def touch_last_attempted(self, node_id: str) -> bool:
        payload = {
            "nodeId": node_id,
            "lastAttemptedAt": _utc_iso(),
        }
        # API Route: nodes.updateLastAttempted, Input: payload, Output: {success: bool}
        response = self.api_client.request("PATCH", f"nodes/{node_id}", payload)
        return bool(response.get("success", True))

    def update_node(self, node_id: str, data: Dict[str, Any]) -> bool:
        payload = {"nodeId": node_id, "data": data}
        # API Route: nodes.updateProfile, Input: payload, Output: {success: bool}
        response = self.api_client.request("PATCH", f"nodes/{node_id}", payload)
        return bool(response.get("success", True))

    def update_duplicates(self, linkedin_username: str, exclude_node_id: str, data: Dict[str, Any]) -> int:
        payload = {
            "linkedinUsername": linkedin_username,
            "excludeNodeId": exclude_node_id,
            "data": data,
        }
        # API Route: nodes.updateDuplicates, Input: payload, Output: {modifiedCount: int}
        response = self.api_client.request("POST", "nodes/update-duplicates", payload)
        return int(response.get("modifiedCount", 0))

    def delete(self, node_id: str) -> bool:
        # API Route: nodes.delete, Input: {nodeId}, Output: {success: bool}
        response = self.api_client.request("DELETE", f"nodes/{node_id}")
        return bool(response.get("success", True))

    def mark_error(self, node_id: str, error_message: Optional[str] = None) -> bool:
        payload = {
            "nodeId": node_id,
            "errorMessage": error_message,
        }
        # API Route: nodes.markError, Input: payload, Output: {success: bool}
        response = self.api_client.request("POST", "nodes/mark-error", payload)
        return bool(response.get("success", True))

    def scraping_statistics(self) -> Dict[str, Any]:
        # API Route: nodes.scrapeStats, Input: {}, Output: {stats: {...}}
        response = self.api_client.get("nodes/scrape-stats")
        return response.get("stats", response)

    def recent_attempts(self, *, hours: int = 1, limit: int = 100) -> List[Dict[str, Any]]:
        params = {"hours": hours, "limit": limit}
        # API Route: nodes.recentAttempts, Input: params, Output: {nodes: [...]}
        response = self.api_client.get("nodes/recent-attempts", params=params)
        return response.get("nodes", [])

    def scrape_candidates(self, *, limit: int = 5) -> List[Dict[str, Any]]:
        params = {"limit": limit}
        # API Route: nodes.scrapeCandidates, Input: params, Output: {nodes: [...]}
        response = self.api_client.get("nodes/scrape-candidates", params=params)
        return response.get("nodes", [])


class ServiceClients:
    """Aggregate reusable service clients for the Lambda runtime."""

    def __init__(self):
        self.api = ApiClient(
            base_url=config.BASE_API_URL,
            api_key=config.API_KEY,
            timeout=config.API_TIMEOUT_SECONDS,
            max_retries=config.API_MAX_RETRIES,
        )
        self.nodes = NodeRepository(self.api)


_clients: Optional[ServiceClients] = None


def get_clients() -> ServiceClients:
    global _clients
    if _clients is None:
        _clients = ServiceClients()
    return _clients
