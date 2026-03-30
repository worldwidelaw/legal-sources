#!/usr/bin/env python3
"""
SG/eLitigation -- Singapore eLitigation Court Judgments

Fetches court judgments from the official Singapore eLitigation portal.
Full text extracted from HTML judgment pages.

Strategy:
  - Paginate through /gd/Home/Index listing pages (SUPCT filter)
  - Extract judgment URLs, titles, citations, dates from listing cards
  - Fetch full text from /gd/s/{CITATION_PATH} pages
  - Text is in <div id="divJudgement"><content>...</content></div>
  - Rate limited to 0.5 req/s (2s between requests)

API:
  - Base: https://www.elitigation.sg
  - Listing: /gd/Home/Index?Filter=SUPCT&YearOfDecision={year}&CurrentPage={page}&...
  - Judgment: /gd/s/{YEAR}_{COURT}_{NUMBER}
  - No auth required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch ~15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html as htmlmod
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SG.eLitigation")

BASE_URL = "https://www.elitigation.sg"

# Court code to human-readable name mapping
COURT_NAMES = {
    "SGCA": "Court of Appeal",
    "SGHC": "High Court (General Division)",
    "SGHCI": "Singapore International Commercial Court",
    "SGHCF": "High Court (Family Division)",
    "SGHCR": "High Court (Registrar)",
    "SGDC": "District Court",
    "SGMC": "Magistrate's Court",
    "SGFC": "Family Court",
}

# Years to iterate through (2000-2026)
YEARS = list(range(2026, 1999, -1))


def clean_html_text(html_str: str) -> str:
    """Strip HTML tags and clean text from judgment content."""
    if not html_str:
        return ""
    # Remove style and script blocks
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_str, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
    # Convert block elements to newlines
    text = re.sub(r'<(?:p|div|br|h[1-6]|li|tr|blockquote|table|tbody|hr)[^>]*/?>', '\n', text, flags=re.IGNORECASE)
    # Remove remaining tags
    text = re.sub(r'<[^>]+>', ' ', text)
    # Decode HTML entities
    text = htmlmod.unescape(text)
    # Normalize whitespace within lines
    lines = text.split('\n')
    lines = [' '.join(line.split()) for line in lines]
    lines = [line for line in lines if line.strip()]
    return '\n'.join(lines).strip()


class SGeLitigationScraper(BaseScraper):
    """Scraper for SG/eLitigation -- Singapore court judgments."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/worldwidelaw/legal-sources)",
                "Accept": "text/html,application/xhtml+xml,*/*",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=60,
        )

    def _get_listing_page(self, year: str, page: int) -> Optional[str]:
        """Fetch a listing page for a given year and page number."""
        self.rate_limiter.wait()
        params = (
            f"Filter=SUPCT&YearOfDecision={year}"
            f"&SortBy=DateOfDecision&CurrentPage={page}"
            f"&SortAscending=False&PageSize=0&SearchMode=True"
        )
        try:
            resp = self.client.get(f"/gd/Home/Index?{params}")
            if not resp or resp.status_code != 200:
                return None
            return resp.text
        except Exception as e:
            logger.warning(f"Error fetching listing year={year} page={page}: {e}")
            return None

    def _parse_listing_page(self, html: str) -> List[Dict]:
        """Extract judgment metadata from a listing page."""
        entries = []

        # Extract total count
        total_match = re.search(r'Total Judgment\(s\) Found\s*:\s*(\d+)', html)
        if total_match:
            total = int(total_match.group(1))
            if total == 0:
                return entries

        # Split HTML into card blocks (each card is a <div class="card col-12">)
        cards = re.split(r'<div\s+class="card\s+col-12"', html)

        for card in cards[1:]:  # skip the part before the first card
            # Extract judgment URL and citation path
            link_match = re.search(r"href='(/gd/s/([^']+))'", card)
            if not link_match:
                continue
            url_path = link_match.group(1)
            citation_path = link_match.group(2)

            # Extract title from the gd-heardertext link
            title_match = re.search(
                r'class="h5 gd-heardertext">\s*(.*?)\s*</a>',
                card, re.DOTALL
            )
            title = htmlmod.unescape(title_match.group(1).strip()) if title_match else ""
            title = ' '.join(title.split())

            # Extract citation like [2025] SGHC 267
            cite_match = re.search(
                r'class="gd-addinfo-text">\s*(\[[^\]]+\])\s*\|',
                card
            )
            citation = htmlmod.unescape(cite_match.group(1).strip()) if cite_match else ""

            # Extract decision date
            date_match = re.search(r'DecisionDate:(?:&\#34;|")(\d{4}-\d{2}-\d{2})', card)
            date_str = date_match.group(1) if date_match else ""

            entries.append({
                "url_path": url_path,
                "citation_path": citation_path,
                "title": title,
                "citation": citation,
                "date": date_str,
            })

        return entries

    def _get_max_page(self, html: str) -> int:
        """Extract the maximum page number from pagination."""
        # Look for the "Last" page link
        last_match = re.search(r'CurrentPage=(\d+)[^"]*"[^>]*>Last</a>', html)
        if last_match:
            return int(last_match.group(1))
        # Fallback: find highest page number in pagination
        pages = re.findall(r'CurrentPage=(\d+)', html)
        if pages:
            return max(int(p) for p in pages)
        return 1

    def _fetch_judgment_text(self, citation_path: str) -> Optional[str]:
        """Fetch full text from a judgment page."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(f"/gd/s/{citation_path}")
            if not resp or resp.status_code != 200:
                logger.warning(f"Failed to fetch judgment {citation_path}: HTTP {resp.status_code if resp else 'None'}")
                return None

            html = resp.text

            # Extract content from <div id="divJudgement"><content>...</content></div>
            match = re.search(
                r'<div[^>]*id="divJudgement"[^>]*>\s*<content>(.*?)</content>',
                html, re.DOTALL
            )
            if match:
                return clean_html_text(match.group(1))

            # Fallback: try just divJudgement
            match = re.search(
                r'<div[^>]*id="divJudgement"[^>]*>(.*?)</div>\s*(?:</div>|<script)',
                html, re.DOTALL
            )
            if match:
                return clean_html_text(match.group(1))

            logger.warning(f"Could not extract text from {citation_path}")
            return None

        except Exception as e:
            logger.warning(f"Error fetching judgment {citation_path}: {e}")
            return None

    def _extract_court(self, citation_path: str) -> str:
        """Extract court name from citation path like 2026_SGHC_64."""
        parts = citation_path.split('_')
        if len(parts) >= 2:
            court_code = parts[1]
            return COURT_NAMES.get(court_code, court_code)
        return "Unknown"

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all judgments from eLitigation, year by year."""
        total_yielded = 0

        for year in YEARS:
            logger.info(f"Fetching year {year}...")

            # Get first page to determine total pages
            html = self._get_listing_page(str(year), 1)
            if not html:
                logger.info(f"No results for year {year}")
                continue

            entries = self._parse_listing_page(html)
            max_page = self._get_max_page(html)
            logger.info(f"Year {year}: {len(entries)} entries on page 1, {max_page} pages total")

            # Yield entries from page 1
            for entry in entries:
                text = self._fetch_judgment_text(entry["citation_path"])
                if text:
                    entry["text"] = text
                    total_yielded += 1
                    yield entry
                else:
                    logger.warning(f"Skipping {entry['citation_path']}: no text extracted")

            # Fetch remaining pages
            for page in range(2, max_page + 1):
                html = self._get_listing_page(str(year), page)
                if not html:
                    break

                entries = self._parse_listing_page(html)
                if not entries:
                    break

                for entry in entries:
                    text = self._fetch_judgment_text(entry["citation_path"])
                    if text:
                        entry["text"] = text
                        total_yielded += 1
                        yield entry

                if page % 10 == 0:
                    logger.info(f"Year {year}, page {page}/{max_page}, total yielded: {total_yielded}")

        logger.info(f"Fetch complete. Total judgments yielded: {total_yielded}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch judgments since a given date by iterating recent years."""
        since_year = since.year
        for year in range(datetime.now().year, since_year - 1, -1):
            html = self._get_listing_page(str(year), 1)
            if not html:
                continue

            entries = self._parse_listing_page(html)
            max_page = self._get_max_page(html)

            for page_entries in [entries]:
                for entry in page_entries:
                    if entry.get("date"):
                        try:
                            entry_date = datetime.strptime(entry["date"], "%Y-%m-%d")
                            if entry_date.replace(tzinfo=timezone.utc) < since:
                                return
                        except ValueError:
                            pass
                    text = self._fetch_judgment_text(entry["citation_path"])
                    if text:
                        entry["text"] = text
                        yield entry

            for page in range(2, max_page + 1):
                html = self._get_listing_page(str(year), page)
                if not html:
                    break
                page_entries = self._parse_listing_page(html)
                if not page_entries:
                    break
                for entry in page_entries:
                    if entry.get("date"):
                        try:
                            entry_date = datetime.strptime(entry["date"], "%Y-%m-%d")
                            if entry_date.replace(tzinfo=timezone.utc) < since:
                                return
                        except ValueError:
                            pass
                    text = self._fetch_judgment_text(entry["citation_path"])
                    if text:
                        entry["text"] = text
                        yield entry

    def normalize(self, raw: dict) -> dict:
        """Transform raw listing+text data into standard schema."""
        citation_path = raw.get("citation_path", "")
        citation = raw.get("citation", "")
        title = raw.get("title", "")
        text = raw.get("text", "")
        date_str = raw.get("date", "")

        if not text or len(text) < 50:
            return None

        # Extract court from citation path
        court = self._extract_court(citation_path)

        # Build full URL
        full_url = f"{BASE_URL}/gd/s/{citation_path}"

        return {
            "_id": citation or citation_path,
            "_source": "SG/eLitigation",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str if date_str else None,
            "url": full_url,
            "citation": citation,
            "court": court,
            "country": "SG",
        }


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="SG/eLitigation scraper")
    parser.add_argument("command", choices=["bootstrap", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch only sample records")
    parser.add_argument("--sample-size", type=int, default=15,
                        help="Number of sample records")
    args = parser.parse_args()

    scraper = SGeLitigationScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        html = scraper._get_listing_page("2025", 1)
        if html:
            entries = scraper._parse_listing_page(html)
            logger.info(f"Test OK: found {len(entries)} entries on page 1 of 2025")
            if entries:
                text = scraper._fetch_judgment_text(entries[0]["citation_path"])
                if text:
                    logger.info(f"Text extraction OK: {len(text)} chars from {entries[0]['citation_path']}")
                else:
                    logger.error("Text extraction FAILED")
        else:
            logger.error("Connectivity test FAILED")
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
