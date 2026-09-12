#!/usr/bin/env python3
"""
FJ/RBF-SupervisionPolicies -- Reserve Bank of Fiji Supervision Policy Statements

Fetches prudential supervision policies from the Reserve Bank of Fiji:
  - PSPS  (Prudential Supervision Policy Statements)
  - BSPS  (Banking Supervision Policy Statements)
  - ISPS  (Insurance Supervision Policy Statements)
  - CMSPS (Capital Markets Supervision Policy Statements)
  - SSPS  (Superannuation Supervision Policy Statements)
  - RFEDMC SPS (Restricted Foreign Exchange Dealers and Money Changers)
  - Payment Service Provider policy statements
  - FNPF  (Fiji National Provident Fund)

Strategy:
  1. Fetch page content via WordPress REST API (page ID 20542)
  2. Parse all PDF links from the rendered HTML content
  3. Download each PDF and extract text via common/pdf_extract

Usage:
  python bootstrap.py bootstrap          # Full pull (~60 PDFs)
  python bootstrap.py bootstrap --sample # Fetch ~12 sample records
  python bootstrap.py update             # (same as bootstrap -- static docs)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
import html as html_mod
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Tuple
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FJ.RBF-SupervisionPolicies")

USER_AGENT = (
    "LegalDataHunter/1.0 (open-data research; "
    "https://github.com/worldwidelaw/legal-sources)"
)
BASE_URL = "https://www.rbf.gov.fj"
WP_API_PAGE = f"{BASE_URL}/wp-json/wp/v2/pages/20542"
REQUEST_DELAY = 1.5

PDF_LINK_RE = re.compile(
    r'<a\s[^>]*href=["\']([^"\']+\.pdf)["\'][^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
TAG_RE = re.compile(r"<[^>]+>")

# Category detection from URL/filename patterns
CATEGORY_PATTERNS = [
    (re.compile(r"PSPS", re.IGNORECASE), "PSPS"),
    (re.compile(r"BSPS|Banking.Supervision", re.IGNORECASE), "BSPS"),
    (re.compile(r"ISPS|Insurance.Supervision", re.IGNORECASE), "ISPS"),
    (re.compile(r"CMSPS|Capital.Market", re.IGNORECASE), "CMSPS"),
    (re.compile(r"SSPS|Superannuation", re.IGNORECASE), "SSPS"),
    (re.compile(r"RFEDMC|Foreign.Exchange.Dealer", re.IGNORECASE), "RFEDMC"),
    (re.compile(r"PSPSPS|Payment.Service", re.IGNORECASE), "PSP"),
    (re.compile(r"FNPF|Provident.Fund", re.IGNORECASE), "FNPF"),
    (re.compile(r"Insurance.Broker", re.IGNORECASE), "ISPS"),
]


def _clean_text(text: str) -> str:
    """Strip HTML tags and decode entities."""
    text = TAG_RE.sub(" ", text)
    text = html_mod.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _detect_category(url: str, title: str) -> str:
    """Detect policy category from URL or title."""
    combined = f"{url} {title}"
    for pattern, cat in CATEGORY_PATTERNS:
        if pattern.search(combined):
            return cat
    return "other"


def _http_get(url: str, timeout: int = 30, accept: str = "*/*") -> Optional[bytes]:
    req = Request(url, headers={"User-Agent": USER_AGENT, "Accept": accept})
    try:
        resp = urlopen(req, timeout=timeout)
        return resp.read()
    except (HTTPError, URLError) as e:
        logger.warning(f"HTTP error for {url}: {e}")
        return None


def _download_pdf(url: str, timeout: int = 60) -> Optional[bytes]:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        resp = urlopen(req, timeout=timeout)
        data = resp.read()
        if data and b"%PDF" in data[:20]:
            return data
    except (HTTPError, URLError) as e:
        logger.debug(f"PDF download failed for {url}: {e}")
    return None


def _fetch_pdf_links_from_api() -> List[Tuple[str, str, str]]:
    """Fetch page via WP REST API and extract PDF links with categories.

    Returns list of (pdf_url, title, category) tuples.
    """
    logger.info(f"Fetching page content from WP API: {WP_API_PAGE}")
    data = _http_get(WP_API_PAGE, accept="application/json")
    if not data:
        logger.error("Failed to fetch WP API page")
        return []

    try:
        page_data = json.loads(data.decode("utf-8", errors="replace"))
    except json.JSONDecodeError:
        logger.error("Failed to parse WP API JSON response")
        return []

    # Content is in rendered HTML
    html_content = page_data.get("content", {}).get("rendered", "")
    if not html_content:
        logger.error("No rendered content in page response")
        return []

    results = []
    seen = set()

    for match in PDF_LINK_RE.finditer(html_content):
        raw_url = match.group(1).strip()
        raw_title = _clean_text(match.group(2))

        pdf_url = urljoin(BASE_URL, raw_url)
        if pdf_url in seen:
            continue
        seen.add(pdf_url)

        # Derive title from link text or filename
        if not raw_title or len(raw_title) < 3:
            fname = pdf_url.rsplit("/", 1)[-1]
            raw_title = fname.replace(".pdf", "").replace("-", " ").replace("_", " ")

        category = _detect_category(pdf_url, raw_title)
        results.append((pdf_url, raw_title, category))

    logger.info(f"Found {len(results)} PDF links across categories")
    return results


class RBFSupervisionPoliciesScraper(BaseScraper):
    """
    Scraper for FJ/RBF-SupervisionPolicies.
    Country: FJ
    URL: https://www.rbf.gov.fj

    Data types: doctrine
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _fetch_docs(self, max_records: int = 999999) -> Generator[dict, None, None]:
        links = _fetch_pdf_links_from_api()
        if not links:
            logger.error("No PDF links found")
            return

        count = 0
        for pdf_url, title, category in links:
            if count >= max_records:
                return

            time.sleep(REQUEST_DELAY)
            pdf_bytes = _download_pdf(pdf_url)
            if not pdf_bytes:
                logger.warning(f"  PDF download failed: {title[:60]} ({pdf_url})")
                continue

            # Derive a stable doc_id from the filename
            fname = pdf_url.rsplit("/", 1)[-1]
            doc_id = re.sub(r"\.pdf$", "", fname, flags=re.IGNORECASE)
            doc_id = re.sub(r"[^a-zA-Z0-9_-]", "_", doc_id)[:120]

            text = extract_pdf_markdown(
                source="FJ/RBF-SupervisionPolicies",
                source_id=f"{category}-{doc_id}",
                pdf_bytes=pdf_bytes,
                table="doctrine",
            ) or ""

            if not text or len(text) < 100:
                logger.warning(f"  Insufficient text for {title[:60]}: {len(text)} chars")
                continue

            yield {
                "doc_id": doc_id,
                "category": category,
                "title": title,
                "text": text,
                "url": pdf_url,
            }
            count += 1
            logger.info(f"  [{count}] {title[:60]} ({len(text)} chars)")

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._fetch_docs()

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        yield from self._fetch_docs()

    def normalize(self, raw: dict) -> dict:
        category = raw.get("category", "other")
        doc_id = raw.get("doc_id", "unknown")

        return {
            "_id": f"{category}-{doc_id}",
            "_source": "FJ/RBF-SupervisionPolicies",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw["text"],
            "date": None,
            "url": raw.get("url", ""),
            "doc_id": doc_id,
            "category": category,
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = RBFSupervisionPoliciesScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        data = _http_get(WP_API_PAGE, accept="application/json")
        if data and b".pdf" in data:
            print("OK: WP API page reachable and contains PDF links")
        else:
            print("FAIL: Cannot reach WP API or no PDF links found")
            sys.exit(1)

    elif command in ("bootstrap", "update"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        limit = 12 if sample else 999999

        if sample:
            logger.info("=== SAMPLE MODE: fetching ~12 records ===")

        for raw in scraper._fetch_docs(max_records=limit):
            record = scraper.normalize(raw)
            out_file = sample_dir / f"{record['_id']}.json"
            out_file.write_text(json.dumps(record, indent=2, ensure_ascii=False))
            count += 1
            logger.info(f"Saved [{count}]: {record['title'][:70]}")

        logger.info(f"Done. Total records: {count}")
        if count == 0:
            logger.error("No records fetched — check connectivity")
            sys.exit(1)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
