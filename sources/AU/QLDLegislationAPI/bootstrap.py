#!/usr/bin/env python3
"""
AU/QLDLegislationAPI -- Queensland Legislation (Official REST API) Fetcher

Fetches Queensland Acts and subordinate legislation from the official
authenticated REST API at api.legislation.qld.gov.au.

Auth strategy (runtime token exchange -- NEVER a stored JWT):
  - Read durable account credentials from the environment:
        QLD_LEGISLATION_USERNAME / QLD_LEGISLATION_PASSWORD
  - POST /v1/auth/token { username, password } -> access + refresh tokens
  - Send  Authorization: Bearer <access token>  on every data call
  - On expiry / 401: POST /v1/auth/refresh { refresh_token } for a new pair
  - If the refresh is rejected/expired: fall back to a full username/password
    re-login. Tolerates rotating and non-rotating refresh responses.

Discovery + full text:
  - GET /v1/documents?page=N&limit=50  (paginated list)
  - GET /v1/renditions/xml/{id}         (QuILLS DTD XML full text)

Usage:
  python bootstrap.py test               # Auth + one document (needs creds)
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap --full   # Full initial pull
  python bootstrap.py bootstrap-fast     # Concurrent full pull
  python bootstrap.py update             # Recently published documents
"""

import os
import sys
import json
import time
import logging
import re
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List

import urllib.request
import urllib.error
import urllib.parse

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AU.QLDLegislationAPI")

BASE_URL = "https://api.legislation.qld.gov.au"
PORTAL_URL = "https://www.legislation.qld.gov.au"

TOKEN_ENDPOINT = f"{BASE_URL}/v1/auth/token"
REFRESH_ENDPOINT = f"{BASE_URL}/v1/auth/refresh"
DOCUMENTS_ENDPOINT = f"{BASE_URL}/v1/documents"
XML_RENDITION_ENDPOINT = f"{BASE_URL}/v1/renditions/xml/{{doc_id}}"

# Durable secret names (account credentials, NOT tokens)
ENV_USERNAME = "QLD_LEGISLATION_USERNAME"
ENV_PASSWORD = "QLD_LEGISLATION_PASSWORD"

PAGE_LIMIT = 50  # API max records per page

# The /v1/documents endpoint requires a `print_type`. "reprint" is the
# consolidated in-force reprint series (currently ~18,769 acts + subordinate
# legislation), which matches the /inforce/current/ rendition this scraper
# pulls. Without it the API rejects every list request with HTTP 422
# ("Missing required parameter 'print_type'").
DEFAULT_PRINT_TYPE = "reprint"

USER_AGENT = "LegalDataHunter/1.0 (legal research; open data)"


class AuthError(RuntimeError):
    """Raised when authentication cannot be established or refreshed."""


class MissingCredentials(AuthError):
    """Raised when the durable account credentials are not configured."""


# ── Helpers to defensively parse token responses ─────────────────────

_ACCESS_KEYS = ("access_token", "accessToken", "token", "access", "id_token")
_REFRESH_KEYS = ("refresh_token", "refreshToken", "refresh")
_EXPIRES_IN_KEYS = ("expires_in", "expiresIn", "expiry_in", "ttl")
_EXPIRES_AT_KEYS = ("expires_at", "expiresAt", "expiry", "expires")


def _dig(payload: Dict[str, Any], keys) -> Optional[str]:
    """Return the first present, truthy value for any of `keys`.

    Looks at the top level and one common nesting level ("data"/"result").
    """
    if not isinstance(payload, dict):
        return None
    for k in keys:
        v = payload.get(k)
        if v:
            return v
    for wrapper in ("data", "result", "tokens"):
        inner = payload.get(wrapper)
        if isinstance(inner, dict):
            for k in keys:
                v = inner.get(k)
                if v:
                    return v
    return None


def _parse_expiry(payload: Dict[str, Any]) -> datetime:
    """Best-effort expiry as an absolute UTC datetime.

    Falls back to a conservative 5-minute lifetime when the response does
    not disclose an expiry, so we always re-auth well before the documented
    six-hour ceiling.
    """
    now = datetime.now(timezone.utc)
    secs = _dig(payload, _EXPIRES_IN_KEYS)
    if secs:
        try:
            return now + timedelta(seconds=int(float(secs)))
        except (TypeError, ValueError):
            pass
    at = _dig(payload, _EXPIRES_AT_KEYS)
    if at:
        # Accept epoch seconds or ISO 8601
        try:
            return datetime.fromtimestamp(int(float(at)), tz=timezone.utc)
        except (TypeError, ValueError):
            try:
                iso = str(at).replace("Z", "+00:00")
                dt = datetime.fromisoformat(iso)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                pass
    return now + timedelta(minutes=5)


class QLDApiClient:
    """Thin authenticated JSON client with runtime token exchange.

    The account credentials (username/password) are the only durable
    secret; access/refresh tokens are minted per run and never persisted.
    """

    def __init__(self, username: Optional[str] = None, password: Optional[str] = None,
                 timeout: int = 60):
        self.username = username if username is not None else os.environ.get(ENV_USERNAME)
        self.password = password if password is not None else os.environ.get(ENV_PASSWORD)
        self.timeout = timeout
        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None
        self._expires_at: datetime = datetime.now(timezone.utc)

    # -- low-level request ------------------------------------------------

    def _request(self, url: str, method: str = "GET",
                 body: Optional[dict] = None, authed: bool = True,
                 accept: str = "application/json") -> tuple:
        """Perform an HTTP request. Returns (status, headers, bytes).

        Raises urllib.error.HTTPError for non-2xx so callers can branch on
        401. Auth request bodies and tokens are never logged.
        """
        headers = {"User-Agent": USER_AGENT, "Accept": accept}
        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if authed:
            headers["Authorization"] = f"Bearer {self._valid_access_token()}"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            return resp.status, dict(resp.headers), resp.read()

    # -- auth -------------------------------------------------------------

    def _login(self) -> None:
        if not self.username or not self.password:
            raise MissingCredentials(
                f"Set {ENV_USERNAME} and {ENV_PASSWORD} to authenticate to the "
                "Queensland Legislation API. Register at "
                "https://api.legislation.qld.gov.au/api/signup"
            )
        try:
            _, _, raw = self._request(
                TOKEN_ENDPOINT, method="POST",
                body={"username": self.username, "password": self.password},
                authed=False,
            )
        except urllib.error.HTTPError as e:
            raise AuthError(f"Token endpoint returned HTTP {e.code}") from None
        self._apply_token_response(raw, context="login")

    def _refresh(self) -> None:
        if not self._refresh_token:
            self._login()
            return
        try:
            _, _, raw = self._request(
                REFRESH_ENDPOINT, method="POST",
                body={"refresh_token": self._refresh_token},
                authed=False,
            )
        except urllib.error.HTTPError as e:
            # Refresh expired/rejected -> full re-login
            logger.info("Refresh rejected (HTTP %s); re-logging in.", e.code)
            self._login()
            return
        self._apply_token_response(raw, context="refresh")

    def _apply_token_response(self, raw: bytes, context: str) -> None:
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            raise AuthError(f"Unparseable {context} response") from None
        access = _dig(payload, _ACCESS_KEYS)
        if not access:
            raise AuthError(f"No access token in {context} response")
        self._access_token = access
        # Tolerate rotating (new refresh returned) and non-rotating
        # (refresh omitted) responses: keep the old refresh if none given.
        new_refresh = _dig(payload, _REFRESH_KEYS)
        if new_refresh:
            self._refresh_token = new_refresh
        self._expires_at = _parse_expiry(payload)
        logger.debug("Auth ok via %s; access valid ~%ss",
                     context, int((self._expires_at - datetime.now(timezone.utc)).total_seconds()))

    def _valid_access_token(self) -> str:
        """Return a usable access token, minting/refreshing as needed."""
        now = datetime.now(timezone.utc)
        if self._access_token and now < (self._expires_at - timedelta(seconds=30)):
            return self._access_token
        if self._access_token or self._refresh_token:
            self._refresh()
        else:
            self._login()
        return self._access_token

    def ensure_auth(self) -> None:
        """Eagerly establish a session (used by `test`)."""
        self._valid_access_token()

    # -- authed GET with 401 recovery ------------------------------------

    def get_json(self, url: str, params: Optional[dict] = None) -> Dict[str, Any]:
        if params:
            url = f"{url}?{urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})}"
        for attempt in range(2):
            try:
                _, _, raw = self._request(url, method="GET", authed=True,
                                          accept="application/json")
                return json.loads(raw.decode("utf-8"))
            except urllib.error.HTTPError as e:
                if e.code == 401 and attempt == 0:
                    self._access_token = None  # force re-auth
                    self._refresh()
                    continue
                raise
        raise AuthError("Unreachable")

    def get_bytes(self, url: str, accept: str = "application/xml") -> Optional[bytes]:
        for attempt in range(2):
            try:
                _, _, raw = self._request(url, method="GET", authed=True, accept=accept)
                return raw
            except urllib.error.HTTPError as e:
                if e.code == 401 and attempt == 0:
                    self._access_token = None
                    self._refresh()
                    continue
                if e.code in (404, 410):
                    return None
                # A handful of individual documents error server-side (400/500)
                # when their rendition is requested. Skip them rather than abort
                # a full-corpus run of ~18k documents.
                if e.code == 400 or e.code >= 500:
                    logger.warning("Rendition %s failed HTTP %s; skipping doc.",
                                   url, e.code)
                    return None
                raise
        return None


# ── QuILLS XML text extraction (shared shape with AU/QLD-Legislation) ──

def extract_text_from_xml(xml_bytes: bytes) -> tuple:
    """Extract (title, text, date, doc_type) from QuILLS DTD XML."""
    xml_str = xml_bytes.decode("utf-8", errors="replace")

    title = ""
    m = re.search(r'<(?:act|sl|regulation)\s[^>]*title="([^"]+)"', xml_str[:3000])
    if m:
        title = m.group(1).strip()
    if not title:
        m = re.search(r'<heading[^>]*>\s*<txt[^>]*>(.*?)</txt>', xml_str[:5000], re.DOTALL)
        if m:
            title = re.sub(r'<[^>]+>', '', m.group(1)).strip()

    date = None
    m = re.search(r'assent\.date="(\d{4}-\d{2}-\d{2})', xml_str[:3000])
    if not m:
        m = re.search(r'publication\.date="(\d{4}-\d{2}-\d{2})', xml_str[:3000])
    if m:
        date = m.group(1)

    doc_type = "act"
    rm = re.match(r'.*?<(act|sl|regulation)\s', xml_str[:2000], re.DOTALL)
    if rm:
        tag = rm.group(1)
        if tag == "sl":
            doc_type = "subordinate_legislation"
        elif tag == "regulation":
            doc_type = "regulation"

    text = re.sub(r'<!--.*?-->', ' ', xml_str, flags=re.DOTALL)
    text = re.sub(r'<\?.*?\?>', ' ', text)
    text = re.sub(r'<!DOCTYPE[^>]+>', ' ', text)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()

    return title, text, date, doc_type


class QLDLegislationAPIScraper(BaseScraper):
    """Scraper for AU/QLDLegislationAPI (official authenticated REST API)."""

    def __init__(self, client: Optional[QLDApiClient] = None):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = client or QLDApiClient()

    # -- discovery --------------------------------------------------------

    def _iter_document_stubs(self, max_pages: Optional[int] = None,
                             extra_params: Optional[dict] = None
                             ) -> Generator[Dict[str, Any], None, None]:
        """Yield document stub dicts from the paginated /v1/documents list."""
        page = 1
        while True:
            params = {"page": page, "limit": PAGE_LIMIT,
                      "print_type": DEFAULT_PRINT_TYPE}
            if extra_params:
                params.update(extra_params)
            payload = self.client.get_json(DOCUMENTS_ENDPOINT, params=params)
            docs = payload.get("documents") or []
            if not docs:
                break
            for d in docs:
                yield d
            meta = payload.get("_meta") or {}
            total_pages = meta.get("total_pages")
            if total_pages and page >= total_pages:
                break
            if max_pages and page >= max_pages:
                break
            page += 1
            time.sleep(1)

    def _rendition_url(self, stub: Dict[str, Any]) -> tuple:
        """Resolve a full-text rendition URL for a document stub.

        Returns (url, kind) where kind is "xml" or "html". Documents come in two
        forms: newer QuILLS ones expose an `_links.xml` rendition, older reprints
        expose only `_links.html`. Both rendition endpoints require BOTH
        `print_type` and a `point_in_time` matching an actual valid version —
        without them they 400/404/500 ("Document not found ... point-in-time
        '<today>'"). Each stub already carries the correct href (with print_type
        + point_in_time) under `_links`, so prefer that; fall back to
        constructing an XML URL from the stub's own print_type and valid-date.
        """
        links = stub.get("_links") or {}
        for kind in ("xml", "html"):
            for link in links.get(kind) or []:
                href = link.get("href")
                if href:
                    return urllib.parse.urljoin(BASE_URL, href), kind
        doc_id = stub.get("id")
        if not doc_id:
            return None, None
        print_type = stub.get("print_type") or DEFAULT_PRINT_TYPE
        pit = stub.get("first_valid_date") or stub.get("end_valid_date")
        params = {"print_type": print_type}
        if pit:
            params["point_in_time"] = pit
        url = XML_RENDITION_ENDPOINT.format(doc_id=urllib.parse.quote(doc_id, safe=""))
        return f"{url}?{urllib.parse.urlencode(params)}", "xml"

    def _fetch_full_text(self, stub: Dict[str, Any]) -> tuple:
        url, kind = self._rendition_url(stub)
        if not url:
            return None, None
        # Both the xml and html rendition endpoints only produce
        # application/xml (the html rendition returns an XML-wrapped HTML
        # fragment); Accept: text/html is rejected with HTTP 406.
        return self.client.get_bytes(url, accept="application/xml"), kind

    def _build_raw(self, stub: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        doc_id = stub.get("id")
        if not doc_id:
            return None
        body, kind = self._fetch_full_text(stub)
        if not body or len(body) < 300:
            return None
        title, text, xml_date, doc_type = extract_text_from_xml(body)
        if not text or len(text) < 200:
            return None
        date = (stub.get("publication_date") or stub.get("first_valid_date")
                or xml_date or None)
        return {
            "doc_id": doc_id,
            "title": stub.get("title") or title or doc_id,
            "text": text,
            "date": _norm_date(date),
            "doc_type": stub.get("type") or doc_type,
            "no": stub.get("no"),
            "year": stub.get("year"),
            "repealed": stub.get("repealed"),
            "url": f"{PORTAL_URL}/view/whole/html/inforce/current/{doc_id}",
        }

    # -- BaseScraper contract --------------------------------------------

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        for stub in self._iter_document_stubs():
            raw = self._build_raw(stub)
            if raw:
                yield raw
            time.sleep(1)

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        # Filter by publication_date >= since where the API supports it.
        since_str = since.strftime("%Y-%m-%d")
        for stub in self._iter_document_stubs(
            extra_params={"publication_date": since_str}
        ):
            raw = self._build_raw(stub)
            if raw:
                yield raw
            time.sleep(1)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw["doc_id"],
            "_source": "AU/QLDLegislationAPI",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", raw["doc_id"]),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "doc_id": raw["doc_id"],
            "doc_type": raw.get("doc_type", "act"),
            "no": raw.get("no"),
            "year": raw.get("year"),
        }


def _norm_date(value: Optional[str]) -> Optional[str]:
    """Coerce a date-ish string to ISO YYYY-MM-DD, else None."""
    if not value:
        return None
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})', str(value))
    if m:
        return m.group(0)
    m = re.search(r'(\d{2})/(\d{2})/(\d{4})', str(value))
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return None


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="AU/QLDLegislationAPI data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = QLDLegislationAPIScraper()

    if args.command == "test":
        try:
            scraper.client.ensure_auth()
        except MissingCredentials as e:
            logger.error(str(e))
            sys.exit(2)
        except AuthError as e:
            logger.error("Auth failed: %s", e)
            sys.exit(1)
        logger.info("Auth OK. Fetching one document...")
        stub = next(scraper._iter_document_stubs(max_pages=1), None)
        if not stub:
            logger.error("No documents returned.")
            sys.exit(1)
        raw = scraper._build_raw(stub)
        if raw and raw.get("text"):
            logger.info("OK — '%s' (%d chars)", raw["title"], len(raw["text"]))
        else:
            logger.error("Could not extract full text for %s", stub.get("id"))
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info("Bootstrap complete: %s", json.dumps(stats, indent=2))

    elif args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info("Bootstrap-fast complete: %s", json.dumps(stats, indent=2))

    elif args.command == "update":
        stats = scraper.update()
        logger.info("Update complete: %s", json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
