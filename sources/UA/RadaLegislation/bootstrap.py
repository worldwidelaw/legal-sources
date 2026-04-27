#!/usr/bin/env python3
"""
UA/RadaLegislation -- Ukraine Legislation Data Fetcher

Fetches Ukrainian legislation from the Verkhovna Rada (Ukrainian Parliament)
Open Data Portal using bulk JSON datasets and TSV bulk downloads.

Strategy:
  - Primary legislation cards: GET /ogd/zak/perv/cards.json (~2,918 acts)
  - Full laws TSV bulk file:   GET /ogd/zak/laws/data/csv/doc.txt (~287K acts)
  - Full text per document:    GET /ogd/zak/perv/text/d{dokid}.htm (HTML)

API Documentation:
  - Base URL: https://data.rada.gov.ua
  - Cards JSON: /ogd/zak/perv/cards.json (primary legislation cards)
  - Laws TSV:   /ogd/zak/laws/data/csv/doc.txt (all laws metadata, tab-separated)
  - Texts JSON: /ogd/zak/perv/texts.json (text file references)
  - HTML text:  /ogd/zak/perv/text/d{dokid}.htm (individual document text)

Data Coverage:
  - Primary legislation acts (2,918+) via cards.json
  - All laws (287K+) via bulk TSV download
  - Full consolidated text available in HTML format

Rate Limits:
  - Anonymous access (no authentication)
  - Recommended: 1-2 second delays between requests

Usage:
  python bootstrap.py bootstrap              # Full initial pull (all categories)
  python bootstrap.py bootstrap --sample     # Fetch sample records for validation
  python bootstrap.py bootstrap --perv-only  # Primary legislation only (fast)
  python bootstrap.py bootstrap --metadata-only  # All 287K records without full text
  python bootstrap.py update                 # Incremental update (recent docs)
  python bootstrap.py test-api               # Quick API connectivity test

Note:
  --metadata-only skips per-record HTML downloads, allowing fast bulk ingest of
  the full 287K corpus. Records will have empty "text" fields but include all
  metadata (title, date, nreg, status, etc.). Useful for initial bulk load when
  full-text backfill is done separately.
"""

import csv
import io
import sys
import json
import logging
import time
import re
import html
from pathlib import Path
from datetime import datetime, timezone, timedelta
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
logger = logging.getLogger("legal-data-hunter.UA.RadaLegislation")

# API configuration
BASE_URL = "https://data.rada.gov.ua"
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"


class RadaLegislationScraper(BaseScraper):
    """
    Scraper for UA/RadaLegislation -- Ukrainian Legislation.
    Country: UA
    URL: https://zakon.rada.gov.ua

    Data types: legislation
    Auth: none (anonymous access to bulk JSON datasets)
    """

    def __init__(self, metadata_only: bool = False):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/html, */*",
            "Accept-Language": "uk,en;q=0.9",
        })
        self.last_request_time = 0

        # Cache for bulk JSON data
        self._cards_cache = None
        self._texts_cache = None

        # When True, skip per-record HTML text downloads in normalize()
        # This allows ingesting the full 287K corpus with metadata only
        self.metadata_only = metadata_only

    def _rate_limit(self, delay: float = 1.0):
        """Enforce rate limiting with configurable delay."""
        current_time = time.time()
        elapsed = current_time - self.last_request_time

        if elapsed < delay:
            time.sleep(delay - elapsed)

        self.last_request_time = time.time()

    def _fetch_json(self, url: str, timeout: int = 120) -> Optional[Any]:
        """Fetch JSON from URL with error handling."""
        try:
            self._rate_limit(0.5)
            resp = self.session.get(url, timeout=timeout)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.JSONDecodeError as e:
            logger.warning(f"JSON decode error for {url}: {e}")
            return None
        except Exception as e:
            logger.warning(f"Request failed for {url}: {e}")
            return None

    def _fetch_html(self, url: str, timeout: int = 60) -> str:
        """Fetch HTML content from URL."""
        try:
            self._rate_limit(1.0)
            resp = self.session.get(url, timeout=timeout)
            if resp.status_code == 404:
                return ""
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Request failed for {url}: {e}")
            return ""

    def _get_cards(self) -> List[Dict[str, Any]]:
        """
        Get all document cards (metadata) from bulk JSON.

        Returns list of document metadata dicts.
        """
        if self._cards_cache is not None:
            return self._cards_cache

        url = f"{BASE_URL}/ogd/zak/perv/cards.json"
        logger.info(f"Fetching cards from {url}...")

        data = self._fetch_json(url)
        if data is None:
            logger.error("Failed to fetch cards.json")
            return []

        self._cards_cache = data
        logger.info(f"Loaded {len(data)} document cards")
        return data

    def _get_texts_index(self) -> Dict[int, Dict[str, Any]]:
        """
        Get text file references indexed by dokid.

        Returns dict mapping dokid -> text file info.
        """
        if self._texts_cache is not None:
            return self._texts_cache

        url = f"{BASE_URL}/ogd/zak/perv/texts.json"
        logger.info(f"Fetching texts index from {url}...")

        data = self._fetch_json(url)
        if data is None:
            logger.error("Failed to fetch texts.json")
            return {}

        # Index by dokid for quick lookup
        self._texts_cache = {item["dokid"]: item for item in data}
        logger.info(f"Indexed {len(self._texts_cache)} text file references")
        return self._texts_cache

    def _extract_text_from_html(self, html_content: str) -> str:
        """
        Extract clean text from HTML document.

        Handles two content formats:
        1. data.rada.gov.ua HTML files (simple structure)
        2. zakon.rada.gov.ua print pages (complex structure with article div)

        Removes HTML tags and cleans up whitespace.
        """
        if not html_content:
            return ""

        # Try to extract only the article content (for zakon.rada.gov.ua print pages)
        # The main content is in <div id="article"> or <div class="rvts0">
        article_match = re.search(r'<div id="article"[^>]*>(.*?)</div>\s*</div>\s*</body>', html_content, flags=re.DOTALL | re.IGNORECASE)
        if article_match:
            html_content = article_match.group(1)
        else:
            # Try to extract rvts0 div (main content container)
            rvts_match = re.search(r'<div class="rvts0"[^>]*>(.*)', html_content, flags=re.DOTALL | re.IGNORECASE)
            if rvts_match:
                html_content = rvts_match.group(1)

        # Remove print panel and navigation elements
        html_content = re.sub(r'<div id="prnpanel"[^>]*>.*?</div>', '', html_content, flags=re.DOTALL | re.IGNORECASE)

        # Remove style and script tags
        text = re.sub(r"<style[^>]*>.*?</style>", "", html_content, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<link[^>]*>", "", text, flags=re.IGNORECASE)

        # Remove HTML tags but preserve some structure
        text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"</p>", "\n\n", text, flags=re.IGNORECASE)
        text = re.sub(r"</tr>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"</td>", " | ", text, flags=re.IGNORECASE)
        text = re.sub(r"</div>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"<[^>]+>", " ", text)

        # Decode HTML entities
        text = html.unescape(text)

        # Normalize whitespace
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
        text = re.sub(r"^\s*\|\s*", "", text, flags=re.MULTILINE)  # Clean table cell markers
        text = re.sub(r"\s*\|\s*$", "", text, flags=re.MULTILINE)
        text = text.strip()

        return text

    def _get_document_text(self, dokid: int) -> str:
        """
        Fetch and extract full text for a document.

        Returns cleaned plain text content.
        """
        url = f"{BASE_URL}/ogd/zak/perv/text/d{dokid}.htm"

        html_content = self._fetch_html(url)
        if not html_content:
            logger.warning(f"No HTML content for dokid {dokid}")
            return ""

        text = self._extract_text_from_html(html_content)
        return text

    def _get_laws_tsv(self) -> List[Dict[str, Any]]:
        """
        Download the bulk laws TSV file (290K+ records).

        Returns list of dicts parsed from the tab-separated file.
        The TSV has NO header row; columns are:
          0: dokid    - document ID
          1: nreg     - registration number (unique identifier)
          2: nazva    - title
          3: perv     - primary/international flag (often 0)
          4: orgid    - organ ID(s), pipe-separated
          5: organs   - organ info with dates (colon-separated)
          6: empty    - usually empty
          7: status   - document status code
          8: orgdat   - adoption date (YYYYMMDD)
        """
        url = f"{BASE_URL}/ogd/zak/laws/data/csv/doc.txt"
        logger.info(f"Downloading laws TSV from {url}...")

        try:
            resp = self.session.get(url, timeout=300)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to download laws TSV: {e}")
            return []

        # Parse TSV — the file uses tab as delimiter
        # File is encoded in windows-1251
        content = resp.content
        for encoding in ("windows-1251", "cp1251", "utf-8", "latin-1"):
            try:
                text = content.decode(encoding)
                break
            except (UnicodeDecodeError, LookupError):
                continue
        else:
            logger.error("Could not decode TSV file with any known encoding")
            return []

        # Define column names explicitly (TSV has NO header row)
        column_names = ["dokid", "nreg", "nazva", "perv", "orgid", "organs", "extra", "status", "orgdat"]

        reader = csv.reader(io.StringIO(text), delimiter="\t")
        rows = []
        for line in reader:
            if len(line) < 9:
                continue  # Skip malformed rows
            row = dict(zip(column_names, line))
            rows.append(row)

        logger.info(f"Parsed {len(rows)} rows from laws TSV")
        return rows

    def fetch_all(self, perv_only: bool = False) -> Generator[dict, None, None]:
        """
        Yield all documents from Ukraine legislation databases.

        Two data sources:
          1. ``/ogd/zak/perv/cards.json`` — ~2,918 primary legislation acts
             (has dokid for HTML full-text download)
          2. ``/ogd/zak/laws/data/csv/doc.txt`` — ~287K all laws (TSV bulk)
             (metadata only; full text fetched from zakon.rada.gov.ua)

        Args:
            perv_only: If True, only fetch primary legislation (fast mode).
        """
        logger.info("Starting full Ukraine Rada legislation fetch...")

        # --- Phase 1: Primary legislation (perv) with HTML full-text ---
        cards = self._get_cards()
        texts_index = self._get_texts_index()

        perv_nregs = set()
        fetched = 0
        errors = 0

        for card in cards:
            dokid = card.get("dokid")
            nreg = card.get("nreg", "")

            if not dokid:
                errors += 1
                continue

            perv_nregs.add(nreg)

            text_info = texts_index.get(dokid, {})
            card["_text_file"] = text_info.get("file", "")
            card["_text_size"] = text_info.get("size", 0)
            card["_dataset"] = "perv"

            fetched += 1
            yield card

            if fetched % 500 == 0:
                logger.info(f"[perv] Yielded {fetched}/{len(cards)} document cards")

        logger.info(f"[perv] Complete: {fetched} documents, {errors} errors")

        if perv_only:
            return

        # --- Phase 2: All laws from bulk TSV (skip those already in perv) ---
        laws_rows = self._get_laws_tsv()
        if not laws_rows:
            logger.warning("No laws TSV rows — skipping phase 2")
            return

        laws_fetched = 0
        laws_skipped = 0

        for row in laws_rows:
            nreg = row.get("nreg", "").strip()
            if not nreg:
                continue

            # Skip if already yielded from perv
            if nreg in perv_nregs:
                laws_skipped += 1
                continue

            # Convert TSV row to card-like dict for normalize()
            card = {
                "nreg": nreg,
                "nazva": row.get("nazva", "").strip(),
                "_dataset": "laws",
            }

            # Parse dokid from TSV (available for linking but not for text files)
            dokid_str = row.get("dokid", "").strip()
            if dokid_str:
                try:
                    card["dokid"] = int(dokid_str)
                except ValueError:
                    card["dokid"] = 0
            else:
                card["dokid"] = 0

            # Parse orgdat — YYYYMMDD format
            orgdat_str = row.get("orgdat", "").strip()
            if orgdat_str:
                try:
                    card["orgdat"] = int(orgdat_str)
                except ValueError:
                    card["orgdat"] = 0

            # Parse status
            status_str = row.get("status", "").strip()
            if status_str:
                try:
                    card["status"] = int(status_str)
                except ValueError:
                    card["status"] = 0

            # Parse orgid (may contain pipe-separated values)
            orgid_str = row.get("orgid", "").strip()
            if orgid_str:
                # Take first orgid if multiple
                first_orgid = orgid_str.split("|")[0]
                try:
                    card["orgid"] = int(first_orgid)
                except ValueError:
                    card["orgid"] = 0

            # Parse perv flag
            perv_str = row.get("perv", "").strip()
            if perv_str:
                try:
                    card["perv"] = int(perv_str)
                except ValueError:
                    card["perv"] = 0

            laws_fetched += 1
            yield card

            if laws_fetched % 10000 == 0:
                logger.info(f"[laws] Yielded {laws_fetched} additional law records")

        logger.info(
            f"[laws] Complete: {laws_fetched} new records "
            f"({laws_skipped} already in perv, skipped)"
        )

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents updated since the given date.

        Uses orgdat field (date of adoption) in YYYYMMDD format.
        """
        since_int = int(since.strftime("%Y%m%d"))
        logger.info(f"Checking for updates since {since_int}...")

        cards = self._get_cards()
        texts_index = self._get_texts_index()

        count = 0
        for card in cards:
            orgdat = card.get("orgdat", 0)

            # Check if document was adopted since the given date
            if orgdat >= since_int:
                dokid = card.get("dokid")
                text_info = texts_index.get(dokid, {})
                card["_text_file"] = text_info.get("file", "")
                card["_text_size"] = text_info.get("size", 0)

                count += 1
                yield card

        logger.info(f"Found {count} documents since {since_int}")

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw card data into standard schema.

        CRITICAL: Downloads and includes FULL TEXT from HTML file.
        """
        dokid = raw.get("dokid", 0)
        nreg = raw.get("nreg", "")

        # Create unique document ID
        doc_id = nreg if nreg else f"UA-{dokid}"

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

        # Get full text from HTML file
        full_text = ""
        dataset = raw.get("_dataset", "perv")

        # Skip full-text download in metadata-only mode (for bulk TSV ingest)
        if not self.metadata_only:
            if dataset == "perv" and dokid:
                # Primary legislation: fetch from data.rada.gov.ua HTML files
                # These files only exist for perv records, not for laws TSV
                full_text = self._get_document_text(dokid)
            elif nreg:
                # All other records (including laws TSV): fetch from zakon.rada.gov.ua
                # This works for any nreg regardless of dokid value
                zakon_url = f"https://zakon.rada.gov.ua/laws/show/{nreg}/print"
                html_content = self._fetch_html(zakon_url, timeout=30)
                if html_content:
                    full_text = self._extract_text_from_html(html_content)

            if not full_text:
                logger.debug(f"No full text for {doc_id}")

        # Get title
        title = raw.get("nazva", "")

        # Determine document type based on 'perv' field
        # perv=1: domestic legislation, perv=2: international treaties
        perv = raw.get("perv", 0)
        perv_names = {1: "domestic", 2: "international_treaty"}
        perv_name = perv_names.get(perv, "legislation")

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
            "_source": "UA/RadaLegislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_str,
            "url": url,
            # Source-specific fields
            "nreg": nreg,
            "dokid": dokid,
            "n_vlas": raw.get("n_vlas", ""),  # Official number (e.g., 4769-IX)
            "orgnum": raw.get("orgnum", ""),
            "orgid": raw.get("orgid", 0),
            "perv": perv,
            "perv_type": perv_name,
            "typ": raw.get("typ", 0),
            "types": raw.get("types", 0),
            "status": status,
            "status_name": status_name,
            "npix": raw.get("npix", 0),  # Session/convocation number
            "publics": raw.get("publics", ""),  # Publication references
            "tags": raw.get("tags", ""),
            "klasy": raw.get("klasy", ""),  # Classification
            "history": raw.get("history", ""),
            "language": "uk",
        }

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing Ukraine Rada Legislation API...")

        # Test cards endpoint
        print("\n1. Testing cards.json endpoint...")
        url = f"{BASE_URL}/ogd/zak/perv/cards.json"
        try:
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            cards = resp.json()
            print(f"   Found {len(cards)} document cards")
            if cards:
                sample = cards[0]
                print(f"   Sample card: dokid={sample.get('dokid')}, nreg={sample.get('nreg')}")
                print(f"   Title: {sample.get('nazva', '')[:60]}...")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        # Test texts endpoint
        print("\n2. Testing texts.json endpoint...")
        url = f"{BASE_URL}/ogd/zak/perv/texts.json"
        try:
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            texts = resp.json()
            print(f"   Found {len(texts)} text file references")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        # Test HTML text file
        print("\n3. Testing HTML text file...")
        if cards:
            dokid = cards[0].get("dokid")
            url = f"{BASE_URL}/ogd/zak/perv/text/d{dokid}.htm"
            try:
                resp = self.session.get(url, timeout=60)
                resp.raise_for_status()
                text = self._extract_text_from_html(resp.text)
                print(f"   Document dokid={dokid}")
                print(f"   Text length: {len(text)} characters")
                print(f"   Preview: {text[:200]}...")
            except Exception as e:
                print(f"   ERROR: {e}")

        print("\nAPI test complete!")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="UA/RadaLegislation Ukrainian Legislation Fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (few records)")
    parser.add_argument("--sample-size", type=int, default=15, help="Number of sample records")
    parser.add_argument("--perv-only", action="store_true",
                        help="Only fetch primary legislation (~2.9K records, fast)")
    parser.add_argument("--metadata-only", action="store_true",
                        help="Skip per-record HTML text downloads (ingest metadata only, ~287K records)")
    parser.add_argument("--workers", type=int, default=None, help="Concurrent threads (bootstrap-fast)")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size (bootstrap-fast)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    # Create scraper with metadata-only flag if specified
    scraper = RadaLegislationScraper(metadata_only=args.metadata_only)

    if args.command == "test-api":
        scraper.test_api()

    elif args.command == "bootstrap":
        if args.sample:
            stats = scraper.run_sample(n=args.sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            # Monkey-patch fetch_all to pass perv_only flag
            if args.perv_only:
                orig_fetch = scraper.fetch_all
                scraper.fetch_all = lambda: orig_fetch(perv_only=True)
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif args.command == "bootstrap-fast":
        if args.perv_only:
            orig_fetch = scraper.fetch_all
            scraper.fetch_all = lambda: orig_fetch(perv_only=True)
        print(f"Running fast bootstrap (workers={args.workers or 'auto'}, batch_size={args.batch_size})...")
        stats = scraper.bootstrap_fast(
            max_workers=args.workers,
            batch_size=args.batch_size,
        )
        print(f"\nFast bootstrap complete: {stats['records_new']} new, "
              f"{stats['records_updated']} updated, "
              f"{stats['errors']} errors")
        print(json.dumps(stats, indent=2))

    elif args.command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
