#!/usr/bin/env python3
"""
BN/AGCLaws -- Brunei Attorney General's Chambers Laws

Fetches legislation from the Brunei AGC "Laws of Brunei" (Texts of Acts) index.

Strategy:
  - Parse the index page table: chapter number, title, B.L.R.O. revision and the
    PDF links in each row (principal Act plus subsidiary rules/orders).
  - If the live page carries no PDF links (the AGC stripped the link column from
    the live page in mid-2026 — the PDFs themselves are still served), fall back
    to the newest Internet Archive capture that still has them, and overlay the
    current titles from the live page.
  - Download each PDF and extract text with the shared extractor.

Data: ~220 chapters / ~310 documents of consolidated legislation
License: Open access (government legislation portal)
Rate limit: 0.5 req/sec.

Usage:
  python bootstrap.py bootstrap            # Sample pull (15 records)
  python bootstrap.py bootstrap --full     # Full pull to data/records.jsonl
  python bootstrap.py bootstrap-fast       # Full pull, concurrent extraction
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import threading
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List
from urllib.parse import unquote

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BN.AGCLaws")

BASE_URL = "https://www.agc.gov.bn"
INDEX_PATH = "/AGC%20Site%20Pages/Laws%20of%20Brunei.aspx"
INDEX_URL = BASE_URL + INDEX_PATH

CDX_URL = "http://web.archive.org/cdx/search/cdx"
WAYBACK_REPLAY = "https://web.archive.org/web/{ts}id_/" + INDEX_URL
# A usable capture has the full link column; the table itself is ~220 rows.
MIN_ARCHIVE_LINKS = 50
MAX_ARCHIVE_CAPTURES = 12

BLRO_RE = re.compile(r"B\.?\s*L\.?\s*R\.?\s*O", re.I)

# The AGC's leaf certificate expired on 2026-08-11 and has not been renewed
# (issue #1585). That is an expiry, not a missing intermediate, so the AIA
# repair cannot fix it — without this allowlist every index page and every one
# of the ~312 PDFs fails CERTIFICATE_VERIFY_FAILED and the corpus falls back to
# the 15 bundled samples. Scoped to this host only; drop it once AGC renews.
INSECURE_SSL_HOSTS = {"agc.gov.bn"}

# A full run that loses more than this fraction of its PDFs is a site/transport
# regression, not ordinary attrition — fail loud rather than silently shipping a
# truncated corpus that looks like a successful crawl.
MAX_PDF_FAILURE_RATE = 0.5
MIN_ATTEMPTS_BEFORE_FAILING = 25


def _extract_blro_year(blro: str) -> Optional[str]:
    """Extract the most recent year from a B.L.R.O. string like 'B.L.R.O 3/2013'."""
    # Match 4-digit years first
    years = re.findall(r'(\d{4})', blro)
    if years:
        return max(years)
    # Match 2-digit years (e.g., '1/84' -> 1984)
    short_years = re.findall(r'/(\d{2})\b', blro)
    if short_years:
        full_years = [f"19{y}" if int(y) > 25 else f"20{y}" for y in short_years]
        return max(full_years)
    return None


def _clean(text: str) -> str:
    """Collapse whitespace and drop the zero-width joiners the CMS sprinkles in."""
    return re.sub(r'\s+', ' ', text.replace('​', '').replace('﻿', '')).strip()


def _slug(pdf_path: str) -> str:
    """Stable id fragment derived from the PDF filename."""
    stem = unquote(pdf_path).rsplit('/', 1)[-1]
    stem = re.sub(r'\.pdf$', '', stem, flags=re.I)
    return re.sub(r'[^A-Za-z0-9]+', '-', stem).strip('-')[:60]


def _compose_titles(items: List[dict]) -> None:
    """Fold each item's base title and instrument suffix into a final title."""
    for item in items:
        base = item.pop("base_title", "") or f"Chapter {item['chapter']}"
        suffix = item.pop("suffix", "")
        item["title"] = f"{base} — {suffix}" if suffix else base


class BNAGCLawsScraper(BaseScraper):
    """
    Scraper for BN/AGCLaws -- Brunei Attorney General's Chambers.
    Country: BN
    URL: https://www.agc.gov.bn

    Data types: legislation
    Auth: none
    """

    def __init__(self, source_dir=None):
        super().__init__(source_dir or str(Path(__file__).parent))

        self.client = HttpClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=(15, 90),
            max_retries=3,
            insecure_ssl_hosts=INSECURE_SSL_HOSTS,
        )
        self.session = self.client.session
        self._index_cache: Optional[List[dict]] = None
        self._pdf_attempts = 0
        self._pdf_failures = 0
        self._pdf_lock = threading.Lock()
        # Full runs rely on the extractor's skip-if-already-in-Neon check; sample
        # and connectivity runs must re-extract so they always produce records.
        self.force_extract = False

    def _get_with_retry(self, url: str, max_retries: int = 3, timeout: int = 90) -> Optional[requests.Response]:
        """GET with retry logic."""
        for attempt in range(max_retries):
            try:
                resp = self.client.get(url, timeout=(15, timeout))
                if resp.status_code == 200:
                    return resp
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt+1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(3 * (attempt + 1))
        return None

    # ── Index parsing ─────────────────────────────────────────────────

    def _parse_rows(self, html: str) -> List[dict]:
        """
        Parse the chapter table out of an index page (live or archived).

        Each table row is one chapter and may link several PDFs: the principal
        Act plus its subsidiary rules/orders. Every PDF becomes its own record.
        """
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, 'html.parser')
        items: List[dict] = []
        seen_urls = set()

        for row in soup.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) < 2:
                continue

            chapter = _clean(cells[0].get_text())
            if not re.match(r'^\d+$', chapter):
                continue

            title = _clean(cells[1].get_text())
            # Drop the dangling bracket the CMS leaves on "Foo [Act]" entries
            title = re.sub(r'\s*\[\s*$', '', title)
            if not title:
                title = f"Chapter {chapter}"

            row_blro = _clean(cells[2].get_text()) if len(cells) > 2 else ""

            links = []
            for a in row.find_all('a', href=re.compile(r'\.pdf$', re.I)):
                href = a.get('href', '')
                if not href:
                    continue
                url = BASE_URL + href if href.startswith('/') else href
                if 'agc.gov.bn' not in url or url in seen_urls:
                    continue
                seen_urls.add(url)
                links.append((url, href, _clean(a.get_text())))

            if not links:
                continue

            # The principal Act is the first ACT_PDF link when present; that keeps
            # the pre-existing BN-AGCLaws-Ch{n} ids stable.
            primary = next((i for i, l in enumerate(links) if '/LAWS/ACT_PDF/' in l[1].upper()), 0)
            links.insert(0, links.pop(primary))

            for i, (url, href, label) in enumerate(links):
                blro = label if BLRO_RE.search(label) else row_blro
                if i == 0:
                    doc_id = f"BN-AGCLaws-Ch{chapter}"
                    suffix = ""
                else:
                    doc_id = f"BN-AGCLaws-Ch{chapter}-{_slug(href)}"
                    # Prefer a descriptive anchor ("Extradition Order, 2006") over
                    # a bare B.L.R.O. reference; fall back to the filename.
                    suffix = label if label and not BLRO_RE.search(label) else _slug(href)

                items.append({
                    "_id": doc_id,
                    "chapter": chapter,
                    "base_title": title,
                    "suffix": suffix,
                    "blro": blro,
                    "pdf_url": url,
                    "pdf_path": unquote(href),
                })

        return items

    def _live_titles(self, html: str) -> dict:
        """Chapter number -> current title, from the live index page."""
        from bs4 import BeautifulSoup

        titles = {}
        soup = BeautifulSoup(html, 'html.parser')
        for row in soup.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) < 2:
                continue
            chapter = _clean(cells[0].get_text())
            if not re.match(r'^\d+$', chapter) or chapter in titles:
                continue
            title = re.sub(r'\s*\[\s*$', '', _clean(cells[1].get_text()))
            if title:
                titles[chapter] = title
        return titles

    def _archive_captures(self) -> List[str]:
        """Timestamps of archived index captures, newest first."""
        try:
            resp = self.client.get(
                CDX_URL,
                params={
                    "url": "agc.gov.bn/AGC Site Pages/Laws of Brunei.aspx",
                    "output": "json",
                    "fl": "timestamp,statuscode",
                    "filter": "statuscode:200",
                    "limit": -MAX_ARCHIVE_CAPTURES,
                },
                timeout=(15, 90),
            )
            resp.raise_for_status()
            rows = resp.json()
        except Exception as e:
            logger.error(f"Wayback CDX query failed: {e}")
            return []

        return [r[0] for r in rows[1:]][::-1]  # newest first

    def _parse_index(self) -> List[dict]:
        """
        Build the document list, preferring the live page and falling back to
        the Internet Archive when the live page has lost its PDF links.
        """
        if self._index_cache is not None:
            return self._index_cache

        live_html = ""
        resp = self._get_with_retry(INDEX_URL)
        if resp:
            live_html = resp.text
            items = self._parse_rows(live_html)
            if items:
                _compose_titles(items)
                logger.info(f"Parsed {len(items)} documents from the live index")
                self._index_cache = items
                return items
            logger.warning(
                "Live index has no PDF links (AGC stripped the link column) — "
                "falling back to the Internet Archive"
            )
        else:
            logger.warning("Live index unreachable — falling back to the Internet Archive")

        titles = self._live_titles(live_html) if live_html else {}

        for ts in self._archive_captures():
            # Wayback throttles replay requests with a 503; space them out so a
            # burst of throttled captures does not look like "no usable capture".
            arch = self._get_with_retry(WAYBACK_REPLAY.format(ts=ts), timeout=120)
            if arch is None:
                logger.warning(f"Wayback capture {ts} unavailable")
                time.sleep(2)
                continue

            items = self._parse_rows(arch.text)
            if len(items) < MIN_ARCHIVE_LINKS:
                logger.info(f"Wayback capture {ts}: only {len(items)} documents — trying older")
                continue

            # The live page still carries the current chapter titles even though
            # the links are gone; prefer them over the archived ones, which run
            # every instrument in the row together.
            for item in items:
                current = titles.get(item["chapter"])
                if current:
                    item["base_title"] = current
            _compose_titles(items)

            logger.info(f"Parsed {len(items)} documents from Wayback capture {ts}")
            self._index_cache = items
            return items

        logger.error("Could not obtain a chapter index from the live site or the archive")
        self._index_cache = []
        return []

    # ── Fetching ──────────────────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield raw chapter metadata; normalize() downloads the PDF text."""
        items = self._parse_index()
        if not items:
            raise RuntimeError(
                "Parsed 0 documents from the AGC index (live page and Wayback both empty)"
            )
        for item in items:
            yield item

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch chapters revised since a date (based on B.L.R.O. year)."""
        since_year = str(since.year)
        for item in self._parse_index():
            blro_year = _extract_blro_year(item.get("blro", ""))
            if blro_year and blro_year >= since_year:
                yield item

    def _fetch_pdf(self, url: str) -> Optional[bytes]:
        """
        Download one chapter PDF through this scraper's client.

        pdf_extract's own downloader has no insecure-host escape hatch, so with
        the AGC certificate expired it drops every document (#1585). Fetching the
        bytes here and handing them to the extractor keeps the corpus flowing.
        """
        try:
            resp = self.client.get(url, timeout=(15, 90))
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"PDF fetch failed {url}: {e}")
            return None
        if not resp.content:
            logger.warning(f"PDF fetch returned an empty body: {url}")
            return None
        return resp.content

    def _record_pdf_result(self, ok: bool) -> None:
        """Track download attrition and abort a run that is losing most of it."""
        with self._pdf_lock:
            self._pdf_attempts += 1
            if not ok:
                self._pdf_failures += 1
            attempts, failures = self._pdf_attempts, self._pdf_failures

        if (
            attempts >= MIN_ATTEMPTS_BEFORE_FAILING
            and failures / attempts > MAX_PDF_FAILURE_RATE
        ):
            raise RuntimeError(
                f"{failures} of {attempts} AGC PDF downloads failed "
                f"(>{MAX_PDF_FAILURE_RATE:.0%}) — refusing to ship a truncated "
                "corpus; the site or its TLS chain has changed again"
            )

    def normalize(self, raw: dict) -> Optional[dict]:
        """Download the chapter PDF and build the normalized record."""
        pdf_bytes = self._fetch_pdf(raw["pdf_url"])
        self._record_pdf_result(pdf_bytes is not None)
        if pdf_bytes is None:
            return None

        text = extract_pdf_markdown(
            source="BN/AGCLaws",
            source_id=raw["_id"],
            pdf_bytes=pdf_bytes,
            table="legislation",
            force=self.force_extract,
        )
        if not text or not text.strip():
            logger.debug(f"No extractable text: {raw['_id']} {raw['pdf_url']}")
            return None

        blro_year = _extract_blro_year(raw.get("blro", ""))
        date = f"{blro_year}-01-01" if blro_year else None

        return {
            "_id": raw["_id"],
            "_source": "BN/AGCLaws",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": _clean(raw.get("title", ""))[:500] or f"Chapter {raw['chapter']}",
            "text": text,
            "date": date,
            "url": raw["pdf_url"],
            "chapter": raw["chapter"],
            "blro": raw.get("blro", ""),
            "pdf_path": raw.get("pdf_path", ""),
        }

    def test_api(self) -> bool:
        """Test connectivity and PDF extraction."""
        logger.info("Testing Brunei AGC Laws...")

        items = self._parse_index()
        if not items:
            logger.error("Failed to parse index")
            return False
        chapters = {i["chapter"] for i in items}
        logger.info(f"Index OK: {len(items)} documents across {len(chapters)} chapters")

        for item in items[:5]:
            time.sleep(1)
            record = self.normalize(item)
            if record:
                logger.info(
                    f"PDF extraction OK: {len(record['text'])} chars from {record['_id']}"
                )
                logger.info("All tests passed")
                return True

        logger.error("No PDFs with extractable text found")
        return False


# -- CLI entry point ---------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="BN/AGCLaws -- Brunei AGC Laws")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test-api", "test"],
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (default)")
    parser.add_argument("--full", action="store_true", help="Fetch the whole corpus")
    parser.add_argument("--count", type=int, default=15, help="Sample size")
    parser.add_argument("--since", help="update: only chapters revised since YYYY-MM-DD")
    args = parser.parse_args()

    scraper = BNAGCLawsScraper()

    if args.command in ("test-api", "test"):
        scraper.force_extract = True
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"Bootstrap-fast complete: {json.dumps(stats, indent=2, default=str)}")

    elif args.command == "bootstrap":
        sample_mode = args.sample or not args.full
        scraper.force_extract = sample_mode
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=args.count)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2, default=str)}")

    elif args.command == "update":
        since = (
            datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if args.since
            else datetime(datetime.now(timezone.utc).year - 1, 1, 1, tzinfo=timezone.utc)
        )
        saved = 0
        for raw in scraper.fetch_updates(since):
            record = scraper.normalize(raw)
            if record:
                scraper.storage.write(scraper._dedup_key(record), record)
                saved += 1
        scraper.storage.flush()
        logger.info(f"Update complete: {saved} records written")


if __name__ == "__main__":
    main()
