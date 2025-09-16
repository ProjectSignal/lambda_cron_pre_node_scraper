import http.client
import json
import re
from typing import Optional, Dict, Any, Protocol
from abc import ABC, abstractmethod
from urllib.parse import quote
import time

from config import config
from utils import get_logger


class ProfileDataFetcher(ABC):
    """Abstract base class for profile data fetchers to enable easy provider swapping"""
    
    @abstractmethod
    def fetch(self, linkedin_username: str) -> Optional[Dict[str, Any]]:
        """Fetch profile data from external API"""
        pass
    
    @abstractmethod
    def get_provider_name(self) -> str:
        """Return the name of this provider"""
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Test API connection and authentication"""
        pass


class RapidAPIProfileFetcher(ProfileDataFetcher):
    """Fetcher for RapidAPI LinkedIn profile data with enhanced error handling"""
    
    def __init__(self, api_key: Optional[str] = None, api_host: Optional[str] = None, 
                 timeout: Optional[int] = None, retry_delay: Optional[float] = None):
        self.api_key = api_key or config.RAPIDAPI_KEY
        self.api_host = api_host or config.RAPIDAPI_HOST
        self.timeout = timeout or config.REQUEST_TIMEOUT
        self.retry_delay = retry_delay or config.RETRY_DELAY
        self.logger = get_logger(__name__)
        
        if not self.api_key or self.api_key == "YOUR_RAPIDAPI_KEY_HERE":
            self.logger.warning("RapidAPI key not configured - this fetcher will not be functional")
    
    def fetch(self, linkedin_username: str) -> Optional[Dict[str, Any]]:
        """Fetch profile data from RapidAPI with enhanced error handling"""
        if not self.api_key or self.api_key == "YOUR_RAPIDAPI_KEY_HERE":
            self.logger.error(f"API key not configured for {linkedin_username}. Set RAPIDAPI_KEY env var.")
            return None
        
        if not self.api_host or self.api_host == "YOUR_RAPIDAPI_HOST_HERE":
            self.logger.error(f"API host not configured for {linkedin_username}. Set RAPIDAPI_HOST env var.")
            return None
        
        # Enhanced connection with timeout and retry handling
        conn = http.client.HTTPSConnection(self.api_host, timeout=self.timeout)
        headers = {
            'x-rapidapi-key': self.api_key,
            'x-rapidapi-host': self.api_host,
            'User-Agent': 'LinkedInNodeProcessor/1.0',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate'
        }
        
        try:
            # Handle potential double encoding workaround
            corrected_username = self._correct_username_encoding(linkedin_username)
            encoded_username = quote(corrected_username)
            
            self.logger.debug(f"RapidAPI: Attempting to fetch data for {encoded_username} (timeout: {self.timeout}s)")
            
            # Make API request with enhanced error handling
            conn.request("GET", f"/?username={encoded_username}", headers=headers)
            res = conn.getresponse()
            data = res.read()
            
            # Enhanced response handling
            if res.status == 200:
                try:
                    response_text = data.decode("utf-8")
                    if not response_text.strip():
                        self.logger.warning(f"RapidAPI: Empty response for {linkedin_username}")
                        return None
                    
                    profile_data = json.loads(response_text)
                    
                    # Validate that we got actual profile data, not an error response
                    if isinstance(profile_data, dict):
                        if profile_data.get('success') is False:
                            # API returned structured error
                            error_msg = profile_data.get('message', 'Unknown API error')
                            self.logger.warning(f"RapidAPI: API error for {linkedin_username}: {error_msg}")
                            return profile_data  # Return the error data for processing
                        elif not profile_data.get('username') and not profile_data.get('headline'):
                            # Empty or invalid profile data
                            self.logger.warning(f"RapidAPI: Invalid/empty profile data for {linkedin_username}")
                            return None
                    
                    self.logger.debug(f"RapidAPI: Successfully fetched data for {linkedin_username}")
                    return profile_data
                    
                except json.JSONDecodeError as e:
                    self.logger.error(f"RapidAPI: JSON decode error for {linkedin_username}: {e}")
                    self.logger.debug(f"RapidAPI: Raw response preview: {data[:200] if data else 'No data'}")
                    return None
            elif res.status == 429:
                # Rate limiting - extract retry info if available
                retry_after = res.getheader('Retry-After', 'unknown')
                self.logger.warning(f"RapidAPI: Rate limited for {linkedin_username} (retry after: {retry_after})")
                return None
            elif res.status == 401:
                self.logger.error(f"RapidAPI: Authentication failed for {linkedin_username} - check API key")
                return None
            elif res.status == 403:
                self.logger.error(f"RapidAPI: Access forbidden for {linkedin_username} - check API permissions")
                return None
            elif res.status == 404:
                self.logger.info(f"RapidAPI: Profile not found for {linkedin_username}")
                return None
            elif 500 <= res.status < 600:
                self.logger.warning(f"RapidAPI: Server error {res.status} for {linkedin_username} - may retry")
                return None
            else:
                error_msg = data.decode('utf-8')[:200] if data else "No response data"
                self.logger.debug(f"RapidAPI: Fetch failed for {linkedin_username} with status {res.status}: {error_msg}")
                return None
                
        except http.client.HTTPException as e:
            self.logger.warning(f"RapidAPI: HTTP connection error for {linkedin_username}: {e}")
            return None
        except TimeoutError as e:
            self.logger.warning(f"RapidAPI: Timeout error for {linkedin_username}: {e}")
            return None
        except ConnectionError as e:
            self.logger.warning(f"RapidAPI: Connection error for {linkedin_username}: {e}")
            return None
        except Exception as e:
            self.logger.debug(f"RapidAPI: Exception during fetch for {linkedin_username}: {e}")
            return None
        finally:
            try:
                conn.close()
            except:
                pass
    
    def _correct_username_encoding(self, username: str) -> str:
        """Handle potential double encoding issues"""
        try:
            corrected_username = username.encode('latin-1').decode('utf-8')
            if corrected_username != username:
                self.logger.debug(f"Corrected username encoding for '{username}' to '{corrected_username}'")
                return corrected_username
        except (UnicodeEncodeError, UnicodeDecodeError):
            self.logger.debug(f"Username '{username}' did not require encoding correction")
        
        return username
    
    def test_connection(self) -> bool:
        """Test RapidAPI connection"""
        if not self.api_key or not self.api_host:
            self.logger.error("RapidAPI credentials not configured for connection test")
            return False
        
        conn = http.client.HTTPSConnection(self.api_host)
        headers = {
            'x-rapidapi-key': self.api_key,
            'x-rapidapi-host': self.api_host
        }
        
        try:
            # Test with a simple request (using a test username)
            conn.request("GET", "/?username=test", headers=headers)
            res = conn.getresponse()
            
            # Any response (even error) indicates connection is working
            # 200 or 400 series errors are both acceptable for connection test
            if 200 <= res.status < 500:
                self.logger.info("RapidAPI connection test successful")
                return True
            else:
                self.logger.error(f"RapidAPI connection test failed with status: {res.status}")
                return False
                
        except Exception as e:
            self.logger.error(f"RapidAPI connection test failed with exception: {e}")
            return False
        finally:
            try:
                conn.close()
            except:
                pass
    
    def get_provider_name(self) -> str:
        return "rapidapi"


class ScrapflyProfileFetcher(ProfileDataFetcher):
    """Future Scrapfly API support for LinkedIn profile scraping"""
    
    def __init__(self, api_key: Optional[str] = None, timeout: Optional[int] = None):
        self.api_key = api_key or config.SCRAPFLY_API_KEY
        self.timeout = timeout or config.REQUEST_TIMEOUT
        self.logger = get_logger(__name__)
        
        if not self.api_key:
            self.logger.warning("Scrapfly API key not configured - this fetcher will not be functional")
    
    def fetch(self, linkedin_username: str) -> Optional[Dict[str, Any]]:
        """Future implementation for Scrapfly API"""
        self.logger.info(f"Scrapfly: Not yet implemented for {linkedin_username}")
        return None
    
    def test_connection(self) -> bool:
        """Future implementation for Scrapfly connection test"""
        self.logger.info("Scrapfly: Connection test not yet implemented")
        return False
    
    def get_provider_name(self) -> str:
        return "scrapfly"


class ProxycurlProfileFetcher(ProfileDataFetcher):
    """Future Proxycurl API support for LinkedIn profile scraping"""
    
    def __init__(self, api_key: Optional[str] = None, timeout: Optional[int] = None):
        self.api_key = api_key or config.PROXYCURL_API_KEY
        self.timeout = timeout or config.REQUEST_TIMEOUT
        self.logger = get_logger(__name__)
        
        if not self.api_key:
            self.logger.warning("Proxycurl API key not configured - this fetcher will not be functional")
    
    def fetch(self, linkedin_username: str) -> Optional[Dict[str, Any]]:
        """Future implementation for Proxycurl API"""
        self.logger.info(f"Proxycurl: Not yet implemented for {linkedin_username}")
        return None
    
    def test_connection(self) -> bool:
        """Future implementation for Proxycurl connection test"""
        self.logger.info("Proxycurl: Connection test not yet implemented")
        return False
    
    def get_provider_name(self) -> str:
        return "proxycurl"


class ProfileAPIManager:
    """
    Manager class for handling multiple external API providers with fallback chain logic.
    Provides a centralized way to add new providers and manage fallback behavior.
    """
    
    def __init__(self):
        self.providers = {}
        self.fallback_chain = config.PROVIDER_FALLBACK_CHAIN
        self.logger = get_logger(__name__)
        
        # Initialize configured providers
        self._initialize_providers()
    
    def _initialize_providers(self):
        """Initialize all available providers based on configuration."""
        
        # Initialize RapidAPI if key is available
        if config.RAPIDAPI_KEY and config.RAPIDAPI_KEY != "YOUR_RAPIDAPI_KEY_HERE":
            try:
                self.providers["rapidapi"] = RapidAPIProfileFetcher()
                self.logger.info("Initialized RapidAPI profile fetcher")
            except Exception as e:
                self.logger.error(f"Failed to initialize RapidAPI fetcher: {e}")
        else:
            self.logger.info("RapidAPI key not configured, skipping RapidAPI initialization")
        
        # Initialize Scrapfly if key is available (future)
        if hasattr(config, 'SCRAPFLY_API_KEY') and config.SCRAPFLY_API_KEY:
            try:
                self.providers["scrapfly"] = ScrapflyProfileFetcher()
                self.logger.info("Initialized Scrapfly profile fetcher")
            except Exception as e:
                self.logger.error(f"Failed to initialize Scrapfly fetcher: {e}")
        
        # Initialize Proxycurl if key is available (future)
        if hasattr(config, 'PROXYCURL_API_KEY') and config.PROXYCURL_API_KEY:
            try:
                self.providers["proxycurl"] = ProxycurlProfileFetcher()
                self.logger.info("Initialized Proxycurl profile fetcher")
            except Exception as e:
                self.logger.error(f"Failed to initialize Proxycurl fetcher: {e}")
    
    def add_provider(self, name: str, fetcher: ProfileDataFetcher):
        """Add a new external API provider."""
        self.providers[name] = fetcher
        self.logger.info(f"Added provider: {name}")
    
    def get_provider(self, name: str) -> Optional[ProfileDataFetcher]:
        """Get a specific provider by name."""
        return self.providers.get(name)
    
    def fetch_with_fallback(self, linkedin_username: str) -> Dict[str, Any]:
        """
        Fetch profile data using fallback chain.
        Returns structured result with success status and provider used.
        """
        for provider_name in self.fallback_chain:
            provider = self.providers.get(provider_name)
            if not provider:
                self.logger.debug(f"Provider {provider_name} not available, skipping")
                continue
            
            self.logger.info(f"Trying provider: {provider_name} for {linkedin_username}")
            
            try:
                result = provider.fetch(linkedin_username)
                if result:
                    return {
                        "success": True,
                        "data": result,
                        "provider": provider_name,
                        "error": None
                    }
                else:
                    self.logger.debug(f"Provider {provider_name} returned no data for {linkedin_username}")
            except Exception as e:
                self.logger.warning(f"Provider {provider_name} failed with error: {e}")
            
            # Add delay between provider attempts
            if config.RETRY_DELAY > 0:
                time.sleep(config.RETRY_DELAY)
        
        return {
            "success": False,
            "data": None,
            "provider": None,
            "error": "All providers failed or no providers available"
        }
    
    def get_available_providers(self) -> list:
        """Get list of available provider names."""
        return list(self.providers.keys())
    
    def test_all_providers(self) -> Dict[str, bool]:
        """Test connection to all configured providers."""
        results = {}
        for name, provider in self.providers.items():
            try:
                results[name] = provider.test_connection()
            except Exception as e:
                self.logger.error(f"Error testing provider {name}: {e}")
                results[name] = False
        return results


# Global API manager instance for reuse
api_manager = ProfileAPIManager()


# Backward compatibility wrapper
class ExternalAPIs:
    """Backward compatibility wrapper for the old ExternalAPIs interface"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.api_manager = api_manager
    
    def fetch_profile_data(self, linkedin_username: str) -> Optional[Dict[str, Any]]:
        """
        Fetch profile data using the new manager with fallback logic
        Returns profile data on success, None on failure
        """
        result = self.api_manager.fetch_with_fallback(linkedin_username)
        
        if result["success"]:
            self.logger.info(f"Successfully fetched data for {linkedin_username} via {result['provider']}")
            return result["data"]
        else:
            self.logger.error(f"Failed to fetch data for {linkedin_username}: {result['error']}")
            return None
    
    def test_connection(self) -> bool:
        """Test connection to available providers"""
        results = self.api_manager.test_all_providers()
        return any(results.values())
    
    def get_rate_limit_info(self) -> Dict[str, Any]:
        """Get rate limit information from API providers"""
        return {
            "available_providers": self.api_manager.get_available_providers(),
            "fallback_chain": config.PROVIDER_FALLBACK_CHAIN,
            "timeout": config.REQUEST_TIMEOUT,
            "retry_delay": config.RETRY_DELAY
        }
    
    def close(self):
        """Clean up any connections or resources"""
        # Currently no persistent connections to clean up
        pass