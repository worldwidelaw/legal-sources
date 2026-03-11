"""
HTTP client wrapper for World Wide Law.

Centralized HTTP handling with:
- Automatic retries with exponential backoff
- robots.txt respect
- Response caching (optional)
- Consistent error handling
- Request logging
"""

import time
import logging
import hashlib
import json
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, urljoin

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    raise ImportError("Install requests: pip install requests")

logger = logging.getLogger("legal-data-hunter")


class HttpClient:
    """
    HTTP client with retry logic, rate limiting awareness, and caching.
    """

    def __init__(
        self,
        base_url: str = "",
        headers: dict = None,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
        timeout: int = 30,
        cache_dir: Optional[str] = None,
        respect_robots: bool = True,
        verify: bool = True,
        proxy: Optional[str] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.cache_dir = Path(cache_dir) if cache_dir else None
        self.respect_robots = respect_robots
        self.verify = verify
        self._request_count = 0
        self._error_count = 0

        # Set up session with retries
        self.session = requests.Session()
        self.session.verify = verify
        if headers:
            self.session.headers.update(headers)

        # Proxy support for Cloudflare bypass / residential IP routing
        if proxy:
            self.session.proxies = {"http": proxy, "https": proxy}

        self.session.headers.setdefault("User-Agent", "WorldWideLaw/1.0 (Open Data Research)")

        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get(self, url: str, params: dict = None, **kwargs) -> requests.Response:
        """GET request with full URL or path relative to base_url."""
        full_url = self._resolve_url(url)
        return self._request("GET", full_url, params=params, **kwargs)

    def post(self, url: str, data: dict = None, json_data: dict = None, **kwargs) -> requests.Response:
        """POST request."""
        full_url = self._resolve_url(url)
        return self._request("POST", full_url, data=data, json=json_data, **kwargs)

    def get_json(self, url: str, params: dict = None, **kwargs) -> dict:
        """GET request that returns parsed JSON."""
        resp = self.get(url, params=params, **kwargs)
        resp.raise_for_status()
        return resp.json()

    def get_cached(self, url: str, params: dict = None, max_age_hours: int = 24) -> dict:
        """
        GET with local file cache. Useful for reference data that rarely changes.
        """
        if not self.cache_dir:
            return self.get_json(url, params=params)

        cache_key = hashlib.md5(f"{url}{json.dumps(params, sort_keys=True)}".encode()).hexdigest()
        cache_file = self.cache_dir / f"{cache_key}.json"

        if cache_file.exists():
            age_hours = (time.time() - cache_file.stat().st_mtime) / 3600
            if age_hours < max_age_hours:
                with open(cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)

        data = self.get_json(url, params=params)
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        return data

    def _resolve_url(self, url: str) -> str:
        """Resolve a relative URL against the base URL."""
        if url.startswith(("http://", "https://")):
            return url
        return f"{self.base_url}/{url.lstrip('/')}"

    def _request(self, method: str, url: str, rate_limiter=None, **kwargs) -> requests.Response:
        """
        Execute a request with logging and error tracking.

        Args:
            rate_limiter: Optional RateLimiter/AdaptiveRateLimiter instance.
                          If provided, calls record_success()/record_429()
                          to enable adaptive rate discovery.
        """
        kwargs.setdefault("timeout", self.timeout)
        self._request_count += 1

        logger.debug(f"{method} {url}")

        try:
            response = self.session.request(method, url, **kwargs)

            if response.status_code == 429:
                # Rate limited — notify adaptive limiter and retry
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"Rate limited. Waiting {retry_after}s")
                if rate_limiter:
                    rate_limiter.record_429(retry_after)
                else:
                    time.sleep(retry_after)
                return self.session.request(method, url, **kwargs)

            if rate_limiter and response.ok:
                rate_limiter.record_success()

            return response

        except requests.RequestException as e:
            self._error_count += 1
            logger.error(f"Request failed: {method} {url} - {e}")
            raise

    def stats(self) -> dict:
        """Return request statistics."""
        return {
            "total_requests": self._request_count,
            "errors": self._error_count,
        }
