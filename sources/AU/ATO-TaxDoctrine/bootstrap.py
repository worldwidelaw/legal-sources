#!/usr/bin/env python3
"""
AU/ATO-TaxDoctrine -- Australian Tax Office Rulings & Determinations

Fetches ATO tax doctrine from AustLII (Australian Legal Information Institute).

Strategy:
  - Enumerate ATO ruling databases on AustLII (ATOTR, ATOTD, ATOCR, etc.)
  - For each database, iterate years and sequential document numbers
  - Fetch HTML pages and extract full text content
  - For updates, use AustLII RSS feeds per database

Data:
  - Taxation Rulings, Determinations, Class/Product/GST Rulings, etc.
  - Full text in HTML, stripped to plain text
  - Documents from 1951 to present
  - Language: English

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Check RSS for new rulings
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AU.ATO-TaxDoctrine")

BASE_URL = "https://www.austlii.edu.au"
USER_AGENT = "LegalDataHunter/1.0 (open-data research; https://github.com/worldwidelaw/legal-sources)"

# ATO ruling databases on AustLII with their year ranges
DATABASES = [
    {"code": "ATOTR", "name": "Taxation Rulings", "start": 1992, "prefix": "TR"},
    {"code": "ATOTD", "name": "Taxation Determinations", "start": 1991, "prefix": "TD"},
    {"code": "ATOCR", "name": "Class Rulings", "start": 2001, "prefix": "CR"},
    {"code": "ATOPR", "name": "Product Rulings", "start": 2004, "prefix": "PR"},
    {"code": "ATOGSTR", "name": "GST Rulings", "start": 1999, "prefix": "GSTR"},
    {"code": "ATOGSTD", "name": "GST Determinations", "start": 2000, "prefix": "GSTD"},
    {"code": "ATOMTR", "name": "Miscellaneous Tax Rulings", "start": 1993, "prefix": "MTR"},
    {"code": "ATOSGR", "name": "Superannuation Guarantee Rulings", "start": 1993, "prefix": "SGR"},
    {"code": "ATOSMSFR", "name": "SMSF Rulings", "start": 2008, "prefix": "SMSFR"},
    {"code": "ATOITR", "name": "Old Series Tax Rulings", "start": 1951, "prefix": "IT"},
]

# Max sequential documents to try per year before moving on
MAX_DOCS_PER_YEAR = 300

# AustLII numbering has gaps (withdrawn/reserved numbers), so tolerate a run of
# misses before declaring a year exhausted. Five was too tight and silently
# truncated dense years.
MISS_TOLERANCE = 12

CHECKPOINT_PATH = Path(__file__).parent / "data" / "checkpoint.json"


class SourceBlockedError(RuntimeError):
    """Raised when AustLII refuses this vantage (Cloudflare WAF).

    This must NOT be swallowed: a swallowed 403 looks identical to "document
    does not exist", which turns a total block into a silent 0-record run and
    lets the pipeline fall back to ingesting the committed samples.
    """


def _fetch_url(url: str, timeout: int = 30, retries: int = 3) -> Optional[str]:
    """Fetch URL content.

    Returns None only when the document genuinely does not exist (404/410).
    Raises SourceBlockedError on 403 (Cloudflare block) so the run fails loud.
    """
    req = Request(url, headers={"User-Agent": USER_AGENT})
    last_err: Optional[Exception] = None
    for attempt in range(retries):
        try:
            resp = urlopen(req, timeout=timeout)
            if resp.geturl() != url and "/error" in resp.geturl():
                return None
            return resp.read().decode("utf-8", errors="replace")
        except HTTPError as e:
            if e.code in (404, 410):
                return None
            if e.code == 403:
                raise SourceBlockedError(
                    f"HTTP 403 from AustLII for {url} — Cloudflare is blocking this "
                    f"vantage (WAF rule, not a missing document). AustLII serves ATO "
                    f"rulings only to unblocked (typically AU residential) clients; "
                    f"re-run from a residential/AU proxy."
                ) from e
            if e.code in (429, 500, 502, 503, 504):
                last_err = e
                time.sleep(min(60, 2 ** attempt * 5))
                continue
            raise
        except URLError as e:
            last_err = e
            time.sleep(min(30, 2 ** attempt * 2))
    logger.warning(f"Giving up on {url} after {retries} attempts: {last_err}")
    return None


def _strip_html(html_text: str) -> str:
    """Strip HTML tags and decode entities from text."""
    # Remove script/style blocks
    text = re.sub(r"<script[^>]*>.*?</script>", "", html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
    # Remove HTML tags
    text = re.sub(r"<[^>]+>", " ", text)
    # Decode HTML entities
    text = html.unescape(text)
    # Normalize whitespace
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _extract_content(page_html: str) -> Dict[str, str]:
    """Extract title, text, and date from an AustLII ruling page."""
    result = {"title": "", "text": "", "date": ""}

    # Extract title from <title> tag
    title_match = re.search(r"<title>([^<]+)</title>", page_html)
    if title_match:
        raw_title = html.unescape(title_match.group(1)).strip()
        # Remove the AustLII citation suffix like " [2024] ATOTR 1"
        result["title"] = re.sub(r"\s*\[\d{4}\]\s+ATO\w+\s+\d+\s*$", "", raw_title).strip()

    # Extract main content - AustLII uses <div id="main-content"> or article body
    # Try multiple content selectors
    content = ""
    for pattern in [
        r'<div[^>]*id="main-content"[^>]*>(.*?)</div>\s*<(?:footer|div[^>]*id="footer")',
        r'<article[^>]*>(.*?)</article>',
        r'<!-- end header -->(.*?)<!-- start footer -->',
        r'<div[^>]*class="[^"]*document-content[^"]*"[^>]*>(.*?)</div>',
    ]:
        match = re.search(pattern, page_html, re.DOTALL | re.IGNORECASE)
        if match:
            content = match.group(1)
            break

    if not content:
        # Fallback: extract everything between </header> and <footer>
        match = re.search(r"</header>(.*?)<footer", page_html, re.DOTALL | re.IGNORECASE)
        if match:
            content = match.group(1)
        else:
            # Last resort: get body content
            match = re.search(r"<body[^>]*>(.*?)</body>", page_html, re.DOTALL | re.IGNORECASE)
            if match:
                content = match.group(1)

    if content:
        # Remove navigation, sidebar, search elements
        content = re.sub(r'<nav[^>]*>.*?</nav>', '', content, flags=re.DOTALL | re.IGNORECASE)
        content = re.sub(r'<aside[^>]*>.*?</aside>', '', content, flags=re.DOTALL | re.IGNORECASE)
        content = re.sub(r'<form[^>]*>.*?</form>', '', content, flags=re.DOTALL | re.IGNORECASE)
        content = re.sub(r'<div[^>]*id="page-search"[^>]*>.*?</div>', '', content, flags=re.DOTALL | re.IGNORECASE)
        result["text"] = _strip_html(content)

    # Try to extract date from the content
    # AustLII pages often have "Date of effect:" or ruling date in content
    date_patterns = [
        r'(?:Date of [Ee]ffect|Issue [Dd]ate|Date of [Rr]uling)[:\s]*(\d{1,2}\s+\w+\s+\d{4})',
        r'(?:Gazetted|Published)[:\s]*(\d{1,2}\s+\w+\s+\d{4})',
    ]
    for pat in date_patterns:
        m = re.search(pat, page_html)
        if m:
            try:
                dt = _parse_date(m.group(1))
                if dt:
                    result["date"] = dt
                    break
            except Exception:
                pass

    return result


def _parse_date(date_str: str) -> Optional[str]:
    """Parse various date formats to ISO 8601."""
    for fmt in ["%d %B %Y", "%d %b %Y", "%Y-%m-%d", "%d/%m/%Y"]:
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


class AustraliaATOTaxDoctrineScraper(BaseScraper):
    """
    Scraper for AU/ATO-TaxDoctrine.
    Country: AU
    URL: https://www.austlii.edu.au/cgi-bin/viewdb/au/other/rulings/ato/

    Data types: doctrine
    Auth: none (Open Data via AustLII)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _build_doc_url(self, db_code: str, year: int, num: int) -> str:
        """Build AustLII document URL."""
        return f"{BASE_URL}/cgi-bin/viewdoc/au/other/rulings/ato/{db_code}/{year}/{num}.html"

    def _fetch_document(self, db_code: str, db_name: str, db_prefix: str,
                        year: int, num: int) -> Optional[dict]:
        """Fetch and parse a single ATO ruling document."""
        url = self._build_doc_url(db_code, year, num)
        self.rate_limiter.wait()

        page = _fetch_url(url)
        if page is None:
            return None

        # Check for redirect to error page (302 becomes a different page)
        if len(page) < 500 or "Page not found" in page or "Error" in page[:200]:
            return None

        extracted = _extract_content(page)
        if not extracted["text"] or len(extracted["text"]) < 100:
            return None

        ruling_id = f"{db_prefix} {year}/{num}"

        return {
            "ruling_id": ruling_id,
            "title": extracted["title"] or ruling_id,
            "text": extracted["text"],
            "date": extracted["date"] or f"{year}-01-01",
            "url": url,
            "database": db_code,
            "database_name": db_name,
            "year": year,
            "number": num,
        }

    # ── checkpoint/resume ────────────────────────────────────────────
    # A full sweep is ~10 databases x up to 75 years at 1 req/s, which does not
    # fit in a single fleet slot. Persist the (database, year) units already
    # finished so a relaunch advances instead of re-walking from 1951.

    def _load_checkpoint(self) -> set:
        try:
            with open(CHECKPOINT_PATH) as fh:
                return {tuple(unit) for unit in json.load(fh).get("done", [])}
        except (FileNotFoundError, ValueError):
            return set()

    def _save_checkpoint(self, done: set) -> None:
        CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = CHECKPOINT_PATH.with_suffix(".tmp")
        with open(tmp, "w") as fh:
            json.dump({"done": sorted(list(unit) for unit in done)}, fh)
        tmp.replace(CHECKPOINT_PATH)

    def _preflight(self) -> None:
        """Confirm AustLII actually serves this vantage before a long sweep.

        Without this, a site-wide block reads as "every document is missing"
        and the run exits 0 having written nothing.
        """
        probe = self._build_doc_url("ATOTR", 1992, 2)
        page = _fetch_url(probe)  # raises SourceBlockedError on 403
        if page is None or len(page) < 500:
            raise SourceBlockedError(
                f"Preflight failed: {probe} returned no usable content. This "
                f"document is known to exist (see sample/record_0000.json), so "
                f"AustLII is unreachable or blocking this vantage."
            )
        logger.info(f"Preflight OK: AustLII reachable ({len(page)} bytes)")

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all ATO rulings across all databases and years."""
        current_year = datetime.now().year

        self._preflight()

        done = self._load_checkpoint()
        if done:
            logger.info(f"Resuming: {len(done)} (database, year) units already done")

        for db in DATABASES:
            db_code = db["code"]
            db_name = db["name"]
            db_prefix = db["prefix"]
            start_year = db["start"]

            logger.info(f"Scanning {db_name} ({db_code}) from {start_year} to {current_year}")

            for year in range(start_year, current_year + 1):
                if (db_code, year) in done:
                    continue

                consecutive_misses = 0
                for num in range(1, MAX_DOCS_PER_YEAR + 1):
                    doc = self._fetch_document(db_code, db_name, db_prefix, year, num)
                    if doc:
                        consecutive_misses = 0
                        yield doc
                    else:
                        consecutive_misses += 1
                        if consecutive_misses >= MISS_TOLERANCE:
                            break

                done.add((db_code, year))
                self._save_checkpoint(done)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent rulings from AustLII RSS feeds."""
        for db in DATABASES:
            db_code = db["code"]
            db_name = db["name"]
            db_prefix = db["prefix"]
            feed_url = f"{BASE_URL}/cgi-bin/feed/au/other/rulings/ato/{db_code}/"

            logger.info(f"Checking RSS feed for {db_name}")
            self.rate_limiter.wait()

            feed_content = _fetch_url(feed_url)
            if not feed_content:
                continue

            # Parse RSS items
            items = re.findall(r"<item>(.*?)</item>", feed_content, re.DOTALL)
            for item in items:
                link_match = re.search(r"<link>(.*?)</link>", item)
                if not link_match:
                    continue

                link = link_match.group(1).strip()
                # Extract year and number from URL
                url_match = re.search(r"/(\d{4})/(\d+)\.html", link)
                if not url_match:
                    continue

                year = int(url_match.group(1))
                num = int(url_match.group(2))

                doc = self._fetch_document(db_code, db_name, db_prefix, year, num)
                if doc:
                    yield doc

    def normalize(self, raw: dict) -> dict:
        """Transform raw ATO ruling into standard schema."""
        return {
            "_id": raw["ruling_id"],
            "_source": "AU/ATO-TaxDoctrine",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "ruling_id": raw["ruling_id"],
            "title": raw["title"],
            "text": raw["text"],
            "date": raw["date"],
            "url": raw["url"],
            "database": raw["database"],
            "database_name": raw.get("database_name", ""),
        }


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="AU/ATO-TaxDoctrine bootstrap")
    parser.add_argument(
        "command",
        # "bootstrap-fast" is what the fleet wrapper invokes; without it argparse
        # errored out and the wrapper silently fell back to ingesting sample/.
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--sample-size", type=int, default=15, help="Sample size")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = AustraliaATOTaxDoctrineScraper()

    if args.command == "test":
        logger.info("Testing connectivity to AustLII...")
        url = scraper._build_doc_url("ATOTR", 2024, 1)
        try:
            page = _fetch_url(url)
        except SourceBlockedError as e:
            logger.error(f"BLOCKED: {e}")
            sys.exit(1)
        if page and len(page) > 1000:
            logger.info(f"SUCCESS: AustLII accessible, page size={len(page)} bytes")
        else:
            logger.error("FAILED: Could not fetch test document")
            sys.exit(1)

    elif args.command in ("bootstrap", "bootstrap-fast"):
        result = scraper.bootstrap(sample_mode=args.sample, sample_size=args.sample_size)
        logger.info(f"Bootstrap result: {json.dumps(result, indent=2, default=str)}")
        # BaseScraper.bootstrap() catches fetch_all exceptions and records them in
        # stats["error_message"] rather than propagating, so a total block would
        # otherwise exit 0 and let the fleet ingest sample/ as a "success".
        if result.get("error_message"):
            logger.error(f"FAILED: {result['error_message']}")
            sys.exit(1)
        if not args.sample and result.get("records_fetched", 0) == 0:
            logger.error("FAILED: 0 records fetched — refusing to report success")
            sys.exit(1)

    elif args.command == "update":
        result = scraper.update()
        logger.info(f"Update result: {json.dumps(result, indent=2, default=str)}")


if __name__ == "__main__":
    main()
