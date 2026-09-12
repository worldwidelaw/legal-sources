#!/usr/bin/env python3
"""
UA/ConstitutionalCourt -- Constitutional Court of Ukraine Data Fetcher

Fetches decisions of the Constitutional Court of Ukraine from the
Verkhovna Rada Open Data Portal (data.rada.gov.ua).

Strategy:
  - List: GET /laws/main/o79.json returns all CCU decisions (org=79)
  - Paginated: GET /laws/main/o79/page{N}.json for page-by-page access
  - Full text: GET /laws/show/{nreg}.txt returns plain text

API Documentation:
  - Base URL: https://data.rada.gov.ua
  - List endpoint: /laws/main/o79.json (~2,578 Constitutional Court decisions)
  - Text endpoint: /laws/show/{nreg}.txt (individual document text)
  - View URL: https://zakon.rada.gov.ua/laws/show/{nreg}

Data Coverage:
  - Constitutional Court decisions (1997-present)
  - Rulings (ухвали), Decisions (рішення), Separate opinions (окремі думки)
  - Full text available in plain text format

Rate Limits:
  - 60 requests/minute, 100,000 requests/day
  - Recommended: 1 second delays between requests

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap-fast      # Full pull, threaded (used by the VPS runner)
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update              # Incremental update (recent docs)
  python bootstrap.py test-api            # Quick API connectivity test
"""

import sys
import json
import logging
import threading
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UA.ConstitutionalCourt")

# API configuration
BASE_URL = "https://data.rada.gov.ua"
USER_AGENT = "OpenData"
ORG_ID = 79  # Constitutional Court organization ID

# Transient HTTP statuses worth retrying. 429 is the documented throttle
# response; the 5xx set matters because `bootstrap_fast` runs normalize() —
# and therefore the per-document text fetch — on several threads at once,
# which pushes data.rada.gov.ua past its 60 req/min budget.
RETRYABLE_STATUSES = {408, 425, 429, 500, 502, 503, 504, 509, 520, 521, 522, 524}
MAX_ATTEMPTS = 5


class ConstitutionalCourtScraper(BaseScraper):
    """
    Scraper for UA/ConstitutionalCourt -- Constitutional Court of Ukraine.
    Country: UA
    URL: https://ccu.gov.ua

    Data types: case_law
    Auth: none (anonymous access via OpenData User-Agent)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        # `bootstrap_fast` calls normalize() from a ThreadPoolExecutor, and
        # normalize() is what downloads the document text. requests.Session is
        # not thread-safe, so give every worker thread its own session.
        self._local = threading.local()
        self._rate_lock = threading.Lock()
        self.last_request_time = 0.0

        # Cache for list data
        self._list_cache = None

        # Diagnostics: why individual documents failed, surfaced at run end.
        self._failure_reasons: Dict[str, int] = {}
        self._failure_lock = threading.Lock()

    @property
    def session(self) -> requests.Session:
        """Per-thread requests.Session (see __init__ for why)."""
        sess = getattr(self._local, "session", None)
        if sess is None:
            sess = requests.Session()
            sess.headers.update({
                "User-Agent": USER_AGENT,
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "uk,en;q=0.9",
            })
            self._local.session = sess
        return sess

    def _record_failure(self, reason: str):
        with self._failure_lock:
            self._failure_reasons[reason] = self._failure_reasons.get(reason, 0) + 1

    def _rate_limit(self, delay: float = 1.0):
        """Enforce a global minimum gap between requests across all threads."""
        with self._rate_lock:
            elapsed = time.time() - self.last_request_time
            if elapsed < delay:
                time.sleep(delay - elapsed)
            self.last_request_time = time.time()

    def _get(self, url: str, timeout: int, delay: float) -> Optional[requests.Response]:
        """
        GET with retry/backoff on transient failures.

        Returns the response for 2xx and 404, or None once retries are
        exhausted. Never raises — callers decide what a None means.
        """
        for attempt in range(1, MAX_ATTEMPTS + 1):
            self._rate_limit(delay)
            try:
                resp = self.session.get(url, timeout=timeout)
            except Exception as e:
                reason = type(e).__name__
                if attempt == MAX_ATTEMPTS:
                    logger.warning(f"{url}: giving up after {attempt} attempts ({reason})")
                    self._record_failure(reason)
                    return None
                time.sleep(min(2 ** attempt, 30))
                continue

            if resp.status_code in RETRYABLE_STATUSES:
                if attempt == MAX_ATTEMPTS:
                    logger.warning(f"{url}: giving up after {attempt} attempts (HTTP {resp.status_code})")
                    self._record_failure(f"http_{resp.status_code}")
                    return None
                retry_after = resp.headers.get("Retry-After")
                try:
                    backoff = float(retry_after) if retry_after else min(2 ** attempt, 30)
                except ValueError:
                    backoff = min(2 ** attempt, 30)
                time.sleep(backoff)
                continue

            if resp.status_code == 404 or resp.ok:
                return resp

            logger.warning(f"{url}: HTTP {resp.status_code}")
            self._record_failure(f"http_{resp.status_code}")
            return None

        return None

    def _fetch_json(self, url: str, timeout: int = 120) -> Optional[Any]:
        """Fetch JSON from URL with error handling."""
        resp = self._get(url, timeout=timeout, delay=0.5)
        if resp is None or resp.status_code == 404:
            return None
        try:
            return resp.json()
        except ValueError as e:
            logger.warning(f"JSON decode error for {url}: {e}")
            self._record_failure("json_decode")
            return None

    def _fetch_text(self, url: str, timeout: int = 60) -> Optional[str]:
        """
        Fetch plain text content from URL.

        Returns the body, "" if the document does not exist (404), or None if
        the fetch failed transiently even after retries.
        """
        resp = self._get(url, timeout=timeout, delay=1.0)
        if resp is None:
            return None
        if resp.status_code == 404:
            self._record_failure("http_404")
            return ""
        # data.rada.gov.ua serves UTF-8 but does not always say so.
        if not resp.encoding or resp.encoding.lower() == "iso-8859-1":
            resp.encoding = "utf-8"
        return resp.text

    def _get_decisions_list(self) -> List[Dict[str, Any]]:
        """
        Get all Constitutional Court decisions from the API.

        Fetches all pages (API returns 1000 records per page).
        Returns list of decision metadata dicts.
        """
        if self._list_cache is not None:
            return self._list_cache

        all_decisions = []
        page = 1

        while True:
            # Page 1 is the default endpoint, page 2+ use /pageN.json suffix
            if page == 1:
                url = f"{BASE_URL}/laws/main/o{ORG_ID}.json"
            else:
                url = f"{BASE_URL}/laws/main/o{ORG_ID}/page{page}.json"

            logger.info(f"Fetching page {page} from {url}...")

            data = self._fetch_json(url)
            if data is None:
                if page == 1:
                    logger.error("Failed to fetch decisions list")
                    return []
                # No more pages
                break

            # API returns {"cnt": N, "from": X, "list": [...]}
            decisions = data.get("list", [])
            total = data.get("cnt", 0)

            if not decisions:
                break

            all_decisions.extend(decisions)
            logger.info(f"Page {page}: got {len(decisions)} decisions (total so far: {len(all_decisions)}/{total})")

            # Check if we have all records
            if len(all_decisions) >= total:
                break

            page += 1

        self._list_cache = all_decisions
        logger.info(f"Loaded all {len(all_decisions)} Constitutional Court decisions")
        return all_decisions

    def _get_document_text(self, nreg: str) -> Optional[str]:
        """
        Fetch full text for a document using its registration number.

        Returns the text, "" when the document genuinely has none (404), or
        None when it could not be fetched — the caller drops those rather than
        overwriting a good stored record with an empty one.
        """
        url = f"{BASE_URL}/laws/show/{nreg}.txt"

        text = self._fetch_text(url)
        if text is None:
            return None
        text = text.strip()
        if not text:
            logger.warning(f"No text content for nreg={nreg}")
            self._record_failure("empty_body")
        return text

    def _get_decision_type(self, typ: int) -> str:
        """Map document type code to human-readable name."""
        type_map = {
            22: "decision",          # Рішення
            30: "ruling",            # Ухвала
            153: "separate_opinion", # Окрема думка
        }
        return type_map.get(typ, "other")

    def _get_decision_type_uk(self, typ: int) -> str:
        """Map document type code to Ukrainian name."""
        type_map = {
            22: "Рішення",
            30: "Ухвала",
            153: "Окрема думка",
        }
        return type_map.get(typ, "Інше")

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from Constitutional Court of Ukraine.

        Fetches list JSON, then yields each decision with metadata.
        Full text is fetched during normalize().
        """
        logger.info("Starting full Constitutional Court decisions fetch...")

        decisions = self._get_decisions_list()

        fetched = 0
        errors = 0

        for decision in decisions:
            # The API has been seen to return a bare string in place of a
            # record; that used to abort the whole run with
            # "'str' object has no attribute 'get'".
            if not isinstance(decision, dict):
                logger.warning(f"Skipping non-dict list entry: {str(decision)[:80]}")
                errors += 1
                continue

            nreg = decision.get("nreg", "")

            if not nreg:
                errors += 1
                continue

            fetched += 1
            yield decision

            if fetched % 100 == 0:
                logger.info(f"Yielded {fetched}/{len(decisions)} decisions")

        logger.info(f"Fetch complete: {fetched} decisions, {errors} errors")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents updated since the given date.

        Uses orgdat field (decision date) in YYYYMMDD format.
        """
        since_int = int(since.strftime("%Y%m%d"))
        logger.info(f"Checking for updates since {since_int}...")

        decisions = self._get_decisions_list()

        count = 0
        for decision in decisions:
            orgdat = decision.get("orgdat", 0)

            # Check if decision was made since the given date
            if orgdat >= since_int:
                count += 1
                yield decision

        logger.info(f"Found {count} decisions since {since_int}")

    def normalize(self, raw: dict) -> Optional[dict]:
        """
        Transform raw decision data into standard schema.

        CRITICAL: Downloads and includes FULL TEXT from text endpoint.
        Returns None when the full text could not be retrieved, so a transient
        outage never replaces a stored decision with an empty one.
        """
        if not isinstance(raw, dict):
            logger.warning(f"normalize got {type(raw).__name__}, skipping")
            self._record_failure("non_dict_raw")
            return None

        nreg = raw.get("nreg", "")
        dokid = raw.get("dokid", 0)

        # Create unique document ID
        doc_id = nreg if nreg else f"UA-CCU-{dokid}"

        # Get dates - orgdat is in YYYYMMDD format as integer
        orgdat = raw.get("orgdat", 0)

        # Convert to ISO date
        date_str = ""
        if orgdat:
            try:
                orgdat_str = str(orgdat)
                date_str = f"{orgdat_str[:4]}-{orgdat_str[4:6]}-{orgdat_str[6:8]}"
            except:
                pass

        # Build URL
        url = f"https://zakon.rada.gov.ua/laws/show/{nreg}"

        # Get full text from text endpoint
        full_text = ""
        if nreg:
            fetched_text = self._get_document_text(nreg)
            if fetched_text is None:
                logger.warning(f"Dropping {doc_id}: full text unavailable")
                return None
            full_text = fetched_text

        if not full_text:
            logger.warning(f"No full text for {doc_id}")
            return None

        # Get title
        title = raw.get("nazva", "")

        # Get document type
        typ = raw.get("typ", 0)
        decision_type = self._get_decision_type(typ)
        decision_type_uk = self._get_decision_type_uk(typ)

        # Get official decision number (e.g., "1-р/2021")
        decision_number = raw.get("orgnum", "") or raw.get("n_vlas", "")

        # Get status
        status = raw.get("status", 0)
        status_names = {
            0: "draft",
            1: "adopted",
            2: "in_force",
            3: "expired",
            4: "cancelled",
            5: "published",
        }
        status_name = status_names.get(status, "unknown")

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "UA/ConstitutionalCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_str,
            "url": url,
            # Case law specific fields
            "court": "Constitutional Court of Ukraine",
            "court_uk": "Конституційний Суд України",
            "decision_number": decision_number,
            "decision_type": decision_type,
            "decision_type_uk": decision_type_uk,
            # Source-specific fields
            "nreg": nreg,
            "dokid": dokid,
            "orgid": raw.get("orgid", 0),
            "typ": typ,
            "types": raw.get("types", ""),
            "status": status,
            "status_name": status_name,
            "minjust": raw.get("minjust", ""),
            "publics": raw.get("publics", ""),
            "language": "uk",
        }

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing Constitutional Court of Ukraine API...")

        # Test list endpoint
        print("\n1. Testing decisions list endpoint...")
        url = f"{BASE_URL}/laws/main/o{ORG_ID}.json"
        try:
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            total = data.get("cnt", 0)
            decisions = data.get("list", [])
            print(f"   Total decisions: {total}")
            print(f"   Returned in first page: {len(decisions)}")
            if decisions:
                sample = decisions[0]
                print(f"   Sample: nreg={sample.get('nreg')}, typ={sample.get('typ')}")
                print(f"   Title: {sample.get('nazva', '')[:70]}...")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        # Test text endpoint
        print("\n2. Testing text endpoint...")
        if decisions:
            nreg = decisions[0].get("nreg")
            url = f"{BASE_URL}/laws/show/{nreg}.txt"
            try:
                resp = self.session.get(url, timeout=60)
                resp.raise_for_status()
                text = resp.text.strip()
                print(f"   Document nreg={nreg}")
                print(f"   Text length: {len(text)} characters")
                print(f"   Preview: {text[:200]}...")
            except Exception as e:
                print(f"   ERROR: {e}")

        # Count by type
        print("\n3. Document types distribution...")
        type_counts = {}
        for d in decisions[:500]:  # Sample first 500
            typ = d.get("typ", 0)
            type_counts[typ] = type_counts.get(typ, 0) + 1
        for typ, count in sorted(type_counts.items(), key=lambda x: -x[1]):
            type_name = self._get_decision_type_uk(typ)
            print(f"   typ={typ} ({type_name}): {count}")

        print("\nAPI test complete!")


def main():
    scraper = ConstitutionalCourtScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test-api] "
            "[--sample] [--sample-size N] [--full] [--workers N] [--batch-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])
    workers = None
    if "--workers" in sys.argv:
        workers = int(sys.argv[sys.argv.index("--workers") + 1])
    batch_size = 100
    if "--batch-size" in sys.argv:
        batch_size = int(sys.argv[sys.argv.index("--batch-size") + 1])

    def _report_failures():
        if scraper._failure_reasons:
            print("\nFailure reasons:")
            for reason, count in sorted(
                scraper._failure_reasons.items(), key=lambda kv: -kv[1]
            ):
                print(f"  {reason}: {count}")

    if command == "test-api":
        scraper.test_api()

    elif command in ("bootstrap", "bootstrap-fast"):
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        elif command == "bootstrap-fast":
            stats = scraper.bootstrap_fast(max_workers=workers, batch_size=batch_size)
            print(
                f"\nBootstrap-fast complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['errors']} errors"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        _report_failures()
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
