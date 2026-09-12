#!/usr/bin/env python3
"""
BB/ERT-Decisions -- Barbados Employment Rights Tribunal Decisions

Fetches the full text of decisions issued by the Barbados Employment
Rights Tribunal, published by the Ministry of Labour.

Strategy:
  The site uses WordPress Download Manager. Each year has a
  download-category page listing PDFs. We:
    1. Scrape each year's category page to extract download entries
       (title, slug, wpdmdl ID, date).
    2. Download each PDF via the ?wpdmdl=ID URL.
    3. Extract text with pdfminer.
  Scanned-image PDFs (mostly 2022+) are skipped automatically.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap-fast     # Same as bootstrap (no parallelism needed)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import re
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Dict

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BB.ERT-Decisions")

BASE_URL = "https://labour.gov.bb"
CATEGORY_URL = BASE_URL + "/download-category/{year}-decisions/"
YEARS = list(range(2015, 2027))

MIN_TEXT_CHARS = 200

# Regex to extract wpdmdl entries from category pages
WPDMDL_RE = re.compile(
    r'data-downloadurl="([^"]*\?wpdmdl=(\d+)[^"]*)"'
)
# Title from h3.media-heading
TITLE_RE = re.compile(
    r'<h3\s+class="media-heading[^"]*"[^>]*>\s*<a[^>]*>([^<]+)</a>\s*</h3>'
)
# Date patterns in slug or title
DATE_PATTERNS = [
    re.compile(r'(\w+)-(\d{1,2})(?:st|nd|rd|th)?-(\d{4})'),  # month-day-year in slug
    re.compile(r'(\d{4})-(\d{2})-(\d{2})'),  # ISO date
]

MONTH_MAP = {
    'january': '01', 'february': '02', 'march': '03', 'april': '04',
    'may': '05', 'june': '06', 'july': '07', 'august': '08',
    'september': '09', 'october': '10', 'november': '11', 'december': '12',
}


def extract_pdf_text(content: bytes) -> str:
    """Extract text from PDF bytes using pdfminer."""
    try:
        from pdfminer.high_level import extract_text
        import io
        text = extract_text(io.BytesIO(content))
        return text or ""
    except Exception as e:
        logger.warning(f"PDF extraction failed: {e}")
        return ""


def clean_text(text: str) -> str:
    """Collapse excessive whitespace, strip control chars."""
    text = text.replace("\x00", " ").replace("\x0c", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_date_from_slug(slug: str) -> Optional[str]:
    """Extract date from download slug like 'vicky-chandler-v-btmi-june-17-2020'."""
    slug_lower = slug.lower()
    # Try month-day-year pattern
    m = DATE_PATTERNS[0].search(slug_lower)
    if m:
        month_str, day, year = m.group(1), m.group(2), m.group(3)
        month_num = MONTH_MAP.get(month_str)
        if month_num:
            try:
                datetime(int(year), int(month_num), int(day))
                return f"{year}-{month_num}-{int(day):02d}"
            except ValueError:
                pass
    # Try ISO date
    m = DATE_PATTERNS[1].search(slug)
    if m:
        y, mo, d = m.group(1), m.group(2), m.group(3)
        try:
            datetime(int(y), int(mo), int(d))
            return f"{y}-{mo}-{d}"
        except ValueError:
            pass
    return None


class ERTDecisionsScraper(BaseScraper):
    """
    Scraper for BB/ERT-Decisions — Barbados Employment Rights Tribunal.
    Country: BB
    URL: https://labour.gov.bb/employment-rights-tribunal-2/

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) LegalDataHunter/1.0 "
                          "(research; https://github.com/ZachLaik/LegalDataHunter)",
        })

    def _discover(self) -> List[Dict]:
        """
        Scrape all year category pages to build list of download entries.
        Returns list of dicts: {title, slug, wpdmdl, url, date, year}.
        """
        entries = []
        seen_ids = set()

        for year in YEARS:
            url = CATEGORY_URL.format(year=year)
            try:
                r = self.session.get(url, timeout=30)
                if r.status_code != 200:
                    logger.debug(f"No page for {year}: HTTP {r.status_code}")
                    continue
            except Exception as e:
                logger.warning(f"Failed to fetch {year} page: {e}")
                continue

            html = r.text

            # Extract titles
            titles = TITLE_RE.findall(html)

            # Extract download URLs with wpdmdl IDs
            downloads = WPDMDL_RE.findall(html)

            # Match titles to downloads (they appear in same order)
            for i, (full_url, wpdmdl) in enumerate(downloads):
                if wpdmdl in seen_ids:
                    continue
                seen_ids.add(wpdmdl)

                title = titles[i].strip() if i < len(titles) else f"ERT Decision {wpdmdl}"

                # Extract slug from URL
                slug_match = re.search(r'/download/([^/]+)/', full_url)
                slug = slug_match.group(1) if slug_match else str(wpdmdl)

                date = parse_date_from_slug(slug)
                if not date:
                    # Fall back to year only
                    date = f"{year}-01-01"

                entries.append({
                    "title": title,
                    "slug": slug,
                    "wpdmdl": wpdmdl,
                    "url": f"{BASE_URL}/download/{slug}/?wpdmdl={wpdmdl}",
                    "date": date,
                    "year": year,
                })

            time.sleep(1.0)

        entries.sort(key=lambda d: d["date"], reverse=True)
        logger.info(f"Discovered {len(entries)} ERT decision PDFs across {len(YEARS)} years")
        return entries

    def _download_and_extract(self, entry: Dict) -> Optional[dict]:
        """Download PDF and extract text. Returns None if scanned/empty."""
        try:
            r = self.session.get(entry["url"], timeout=60)
            if r.status_code != 200:
                logger.debug(f"HTTP {r.status_code} for {entry['url']}")
                return None
            ct = r.headers.get("Content-Type", "").lower()
            if "pdf" not in ct and not r.content[:4] == b"%PDF":
                logger.debug(f"Not a PDF response for {entry['title']}")
                return None
        except Exception as e:
            logger.warning(f"Download failed for {entry['title']}: {e}")
            return None

        text = clean_text(extract_pdf_text(r.content))
        if len(text) < MIN_TEXT_CHARS:
            logger.debug(f"Insufficient text ({len(text)} chars): {entry['title']} — likely scanned")
            return None

        return {
            "title": entry["title"],
            "text": text,
            "date": entry["date"],
            "url": entry["url"],
            "slug": entry["slug"],
            "wpdmdl": entry["wpdmdl"],
            "year": entry["year"],
            "pdf_size": len(r.content),
        }

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"BB-ERT-{raw['wpdmdl']}",
            "_source": "BB/ERT-Decisions",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "jurisdiction": "BB",
            "court": "Employment Rights Tribunal",
            "year": raw.get("year"),
            "pdf_size": raw.get("pdf_size", 0),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        entries = self._discover()
        yielded = 0
        skipped = 0
        for entry in entries:
            result = self._download_and_extract(entry)
            if result:
                yield result
                yielded += 1
                if yielded % 5 == 0:
                    logger.info(f"Extracted {yielded} decisions ({skipped} scanned/skipped)...")
            else:
                skipped += 1
            time.sleep(1.5)
        logger.info(f"fetch_all complete: {yielded} decisions with full text, {skipped} skipped")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        entries = self._discover()
        yielded = 0
        for entry in entries:
            if entry["date"] and entry["date"] >= since:
                result = self._download_and_extract(entry)
                if result:
                    yield result
                    yielded += 1
                time.sleep(1.5)
        logger.info(f"fetch_updates complete: {yielded} decisions since {since}")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="BB/ERT-Decisions — Barbados Employment Rights Tribunal"
    )
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test", "update"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--sample-size", type=int, default=15, help="Sample size")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--since", type=str, help="Fetch updates since date (ISO format)")

    args = parser.parse_args()

    scraper = ERTDecisionsScraper()

    if args.command == "test":
        logger.info("Testing ERT connectivity...")
        entries = scraper._discover()
        if not entries:
            logger.error("No decision PDFs discovered")
            sys.exit(1)
        logger.info(f"Found {len(entries)} entries, testing first...")
        result = scraper._download_and_extract(entries[0])
        if not result:
            # Try a few more — newest might be scanned
            for entry in entries[1:5]:
                result = scraper._download_and_extract(entry)
                if result:
                    break
                time.sleep(1)
        if result:
            logger.info(f"Title: {result['title']}")
            logger.info(f"Date: {result['date']}")
            logger.info(f"Text: {len(result['text'])} chars")
            logger.info(f"Preview: {result['text'][:200]}")
            logger.info("Connectivity test passed!")
        else:
            logger.error("Failed to extract text from any candidate")
            sys.exit(1)

    elif args.command in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        if args.since:
            stats = scraper.update(since=args.since)
        else:
            stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
