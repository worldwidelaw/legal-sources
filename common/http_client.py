"""
HTTP client wrapper for Legal Data Hunter.

Centralized HTTP handling with:
- Automatic retries with exponential backoff
- robots.txt respect
- Response caching (optional)
- Consistent error handling
- Request logging
"""

import time
import email.utils
import logging
import hashlib
import json
import threading
from pathlib import Path
from typing import Optional, Union
from urllib.parse import urlparse, urljoin

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    raise ImportError("Install requests: pip install requests")

logger = logging.getLogger("legal-data-hunter")

# Never honour a Retry-After longer than this, however large the header says.
RETRY_AFTER_MAX = 300


def parse_retry_after(value, default: int = 60, cap: int = RETRY_AFTER_MAX) -> int:
    """Parse a Retry-After header (delta-seconds or HTTP-date) into seconds.

    Always bounded by ``cap``: a hostile or misconfigured host answering
    ``Retry-After: 86400`` must not put a fleet worker to sleep for a day.
    """
    if value is None:
        return min(default, cap)
    try:
        seconds = int(str(value).strip())
    except (TypeError, ValueError):
        try:
            when = email.utils.parsedate_to_datetime(str(value))
        except (TypeError, ValueError):
            return min(default, cap)
        if when is None:
            return min(default, cap)
        seconds = int(when.timestamp() - time.time())
    return max(0, min(seconds, cap))


def timeout_seconds(timeout) -> float:
    """Reduce a ``requests`` timeout spec to one attempt's worth of seconds.

    ``requests`` accepts either a scalar or a ``(connect, read)`` tuple, and
    scrapers legitimately pass the tuple form to bound each socket phase
    separately. Connect and read run back to back, so the sum is the ceiling
    for a single attempt — the figure the wall-clock budget is built from.
    """
    if isinstance(timeout, (tuple, list)):
        return sum(float(part) for part in timeout if part is not None)
    return float(timeout)


class CappedRetry(Retry):
    """``Retry`` that refuses to sleep longer than :data:`RETRY_AFTER_MAX`.

    urllib3 honours ``Retry-After`` verbatim inside ``session.request``, so a
    503 with a large header stalls the crawl silently — no log line, no
    progress, process alive. Cap it.
    """

    def get_retry_after(self, response):
        return parse_retry_after(
            response.headers.get("Retry-After"), default=0, cap=RETRY_AFTER_MAX
        ) or None


def request_with_deadline(
    session: "requests.Session",
    method: str,
    url: str,
    wall_timeout: int,
    **kwargs,
) -> requests.Response:
    """Send one request, giving up after ``wall_timeout`` seconds of wall clock.

    ``requests``' ``timeout`` is per socket operation: a host that trickles
    bytes, or urllib3 sleeping between its own retries, can hold a single call
    for hours while the log stays silent and the worker looks hung (issues
    #1265, #1272, #1279, #1280, #1284, #1287, #1343). Run the call in a daemon
    thread and abandon it once the wall clock runs out, so the caller sees an
    ordinary ``requests.Timeout`` and moves on to the next document.

    Exposed for scrapers that drive a bare ``requests.Session`` instead of
    :class:`HttpClient`.
    """
    box = {}

    def worker():
        try:
            box["response"] = session.request(method, url, **kwargs)
        except BaseException as exc:  # re-raised on the calling thread
            box["error"] = exc

    thread = threading.Thread(target=worker, daemon=True, name=f"http-{method}")
    thread.start()
    thread.join(wall_timeout)

    if thread.is_alive():
        raise requests.exceptions.Timeout(
            f"wall-clock deadline ({wall_timeout}s) exceeded: {method} {url}"
        )
    if "error" in box:
        raise box["error"]
    return box["response"]


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
        timeout: Union[int, float, tuple] = 30,
        cache_dir: Optional[str] = None,
        respect_robots: bool = True,
        verify: bool = True,
        proxy: Optional[str] = None,
        wall_timeout: Optional[int] = None,
        insecure_ssl_hosts: Optional[set] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        # Hard ceiling on how long one call may occupy the crawl, covering
        # urllib3's internal retries and Retry-After sleeps as well as the
        # socket timeout. Defaults to a full legitimate retry cycle plus slack.
        # Built from a scalar: `timeout` may be a (connect, read) tuple, and
        # tuple arithmetic silently repeats the tuple then blows up on `+ 60`
        # (#1348).
        self.wall_timeout = wall_timeout or (
            timeout_seconds(timeout) * (max_retries + 1) + 60
        )
        self.cache_dir = Path(cache_dir) if cache_dir else None
        self.respect_robots = respect_robots
        self.verify = verify
        self._request_count = 0
        self._error_count = 0

        # Hosts whose TLS chain is known-broken beyond AIA repair. On such a
        # host, and only such a host, a verify failure degrades to an
        # unverified retry instead of losing the document. Same escape hatch as
        # VN/CongBao (#1236) and INTL/EnergyCharterTreaty (#1241), but reached
        # only after the AIA repair has been tried.
        self.insecure_ssl_hosts = {
            h.lower().lstrip(".") for h in (insecure_ssl_hosts or ())
        }
        # host -> CA bundle path that verified successfully after an AIA repair.
        # Reused on later requests to the same host so the crawl stops paying a
        # doomed handshake per call (issue #1484: two days of one failed
        # handshake + one AIA retry for every single full-text fetch).
        self._aia_bundles: dict = {}
        self._ssl_lock = threading.Lock()

        # Set up session with retries
        self.session = requests.Session()
        self.session.verify = verify
        if headers:
            self.session.headers.update(headers)

        # Proxy support for Cloudflare bypass / residential IP routing
        if proxy:
            self.session.proxies = {"http": proxy, "https": proxy}

        self.session.headers.setdefault("User-Agent", "LegalDataHunter/1.0 (Open Data Research)")

        retry_strategy = CappedRetry(
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
        """POST request.

        Accepts requests' own `json=` spelling as well as `json_data=`. Scrapers are
        written against a raw Session first and use `json=`, so porting one to
        HttpClient used to collide on the keyword inside _request and raise
        "got multiple values for keyword argument 'json'" (#1558).
        """
        full_url = self._resolve_url(url)
        body = kwargs.pop("json", None)
        if body is not None and json_data is not None:
            raise TypeError("pass either json= or json_data=, not both")
        if json_data is None:
            json_data = body
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

    def _send(self, method: str, url: str, **kwargs) -> requests.Response:
        """Send one request under this client's wall-clock deadline."""
        return request_with_deadline(
            self.session, method, url, self.wall_timeout, **kwargs
        )

    def _known_bundle(self, url: str) -> Optional[str]:
        """CA bundle already proven to verify this host, if any."""
        host = (urlparse(url).hostname or "").lower()
        with self._ssl_lock:
            return self._aia_bundles.get(host)

    def _allows_unverified(self, url: str) -> bool:
        """True if this host is on the caller's known-broken-chain allowlist."""
        host = (urlparse(url).hostname or "").lower()
        if not host:
            return False
        return any(
            host == allowed or host.endswith("." + allowed)
            for allowed in self.insecure_ssl_hosts
        )

    def _retry_after_ssl_error(
        self, method: str, url: str, ssl_exc: Exception, kwargs: dict
    ) -> requests.Response:
        """
        Recover from a TLS verification failure, or re-raise.

        Some servers omit their intermediate CA cert → "unable to get local
        issuer certificate". First try to repair the chain properly by
        AIA-fetching the missing intermediate and retrying with an augmented
        CA bundle, keeping verification on (issue #1161 / common.ssl_aia). A
        bundle that works is remembered for the host.

        Only if that fails, and only for a host the caller explicitly
        allowlisted via ``insecure_ssl_hosts``, fall back to an unverified
        retry rather than dropping the document (issue #1484).
        """
        if kwargs.get("verify", self.verify) is False:
            raise ssl_exc

        from common.ssl_aia import is_missing_issuer_error, ca_bundle_for

        if is_missing_issuer_error(ssl_exc):
            bundle = ca_bundle_for(url)
            # A cached bundle that just failed is stale — don't retry with it.
            if bundle and bundle != kwargs.get("verify"):
                logger.warning(f"Retrying with AIA-augmented CA bundle: {url}")
                try:
                    response = self._send(method, url, **dict(kwargs, verify=bundle))
                except requests.exceptions.SSLError:
                    pass
                else:
                    host = (urlparse(url).hostname or "").lower()
                    with self._ssl_lock:
                        self._aia_bundles[host] = bundle
                    return response

        if not self._allows_unverified(url):
            raise ssl_exc

        logger.warning(
            f"TLS verify failed and AIA repair did not help for {url[:120]} — "
            "retrying unverified (host is on insecure_ssl_hosts)"
        )
        try:
            import urllib3

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:
            pass
        return self._send(method, url, **dict(kwargs, verify=False))

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

        # A host already repaired in this process keeps its augmented bundle,
        # so the doomed default-CA handshake is paid once, not per request.
        if "verify" not in kwargs:
            bundle = self._known_bundle(url)
            if bundle:
                kwargs["verify"] = bundle

        try:
            try:
                response = self._send(method, url, **kwargs)
            except requests.exceptions.SSLError as ssl_exc:
                response = self._retry_after_ssl_error(method, url, ssl_exc, kwargs)

            if response.status_code == 429:
                # Rate limited — notify adaptive limiter and retry
                retry_after = parse_retry_after(response.headers.get("Retry-After"))
                logger.warning(f"Rate limited. Waiting {retry_after}s")
                if rate_limiter:
                    rate_limiter.record_429(retry_after)
                else:
                    time.sleep(retry_after)
                return self._send(method, url, **kwargs)

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
