#!/usr/bin/env python3
"""
MY/SC -- Securities Commission Malaysia Enforcement Actions

Fetches enforcement actions from sc.com.my across four categories:
  - Criminal Prosecution (1993-present)
  - Administrative Actions (2002-present)
  - Civil Actions & Regulatory Settlements (2005-present)
  - Cases Compounded (1996-present)

Each category has year-specific pages with HTML tables containing
structured case data (offender, offence, facts, outcome, date).
Full text is inline HTML — no PDF downloads needed.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MY.SC")

BASE_URL = "https://www.sc.com.my"
DELAY = 2.0

# Category definitions: (slug_pattern, start_year, end_year)
CATEGORIES = [
    {
        "name": "Criminal Prosecution",
        "slug": "criminal-prosecution/updates-on-criminal-prosecution-in-{year}",
        "start": 1993,
        "end": 2026,
    },
    {
        "name": "Administrative Actions",
        "slug": "administrative-actions/administrative-actions-in-{year}",
        "start": 2002,
        "end": 2026,
    },
    {
        "name": "Cases Compounded",
        "slug": "cases-compounded/cases-compounded-in-{year}",
        "start": 1996,
        "end": 2024,
    },
    {
        "name": "Civil Actions",
        "slug": "civil-actions-and-regulatory-settlements/civil-action-in-{year}",
        "start": 2005,
        "end": 2025,
    },
    {
        "name": "Regulatory Settlements",
        "slug": "civil-actions-and-regulatory-settlements/regulatory-settlements-in-{year}",
        "start": 2023,
        "end": 2026,
    },
]


def _clean_text(html_fragment: str) -> str:
    """Strip HTML tags and normalize whitespace."""
    text = re.sub(r"<br\s*/?>", "\n", html_fragment, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&ldquo;|&rdquo;", '"', text)
    text = re.sub(r"&lsquo;|&rsquo;", "'", text)
    text = re.sub(r"&mdash;", "—", text)
    text = re.sub(r"&ndash;", "–", text)
    text = re.sub(r"&#\d+;", "", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def _parse_tables(html: str) -> List[List[List[str]]]:
    """Parse all HTML tables into lists of rows of cells (cleaned text)."""
    tables = []
    for table_match in re.finditer(r"<table[^>]*>(.*?)</table>", html, re.DOTALL):
        table_html = table_match.group(1)
        rows = []
        for row_match in re.finditer(r"<tr[^>]*>(.*?)</tr>", table_html, re.DOTALL):
            row_html = row_match.group(1)
            cells = []
            for cell_match in re.finditer(r"<t[hd][^>]*>(.*?)</t[hd]>", row_html, re.DOTALL):
                cells.append(_clean_text(cell_match.group(1)))
            if cells:
                rows.append(cells)
        if rows:
            tables.append(rows)
    return tables


class SCMalaysiaScraper(BaseScraper):
    """Scraper for Securities Commission Malaysia enforcement actions."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.http = HttpClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            },
        )

    def _fetch_year_page(self, category: Dict, year: int) -> Optional[str]:
        """Fetch a year page for a category. Returns HTML or None."""
        slug = category["slug"].format(year=year)
        url = f"{BASE_URL}/regulation/enforcement/actions/{slug}"
        time.sleep(DELAY)
        resp = self.http.get(url, allow_redirects=False)
        if resp.status_code == 200:
            return resp.text
        if resp.status_code in (301, 302):
            return None  # Year doesn't exist
        logger.warning("Failed to fetch %s: %s", url, resp.status_code)
        return None

    def _extract_cases_from_page(
        self, html: str, category_name: str, year: int
    ) -> List[Dict[str, Any]]:
        """Extract cases from a year page's HTML tables."""
        tables = _parse_tables(html)
        cases = []

        for table_idx, table in enumerate(tables):
            if len(table) < 2:
                continue

            # First row is header
            headers = [h.lower().strip() for h in table[0]]

            for row_idx, row in enumerate(table[1:], 1):
                if len(row) < 3:
                    continue

                # Skip rows that are just numbering or empty
                first_cell = row[0].strip()
                if not first_cell or first_cell.lower() in ("no", "no.", ""):
                    continue

                case = {
                    "category": category_name,
                    "year": year,
                    "table_index": table_idx,
                    "row_index": row_idx,
                }

                # Map cells to fields based on header keywords
                for i, cell in enumerate(row):
                    if i >= len(headers):
                        break
                    header = headers[i]
                    if any(k in header for k in ("nature", "offence", "misconduct", "breach")):
                        case["nature_of_offence"] = cell
                    elif any(k in header for k in ("offender", "parties", "person", "name")):
                        case["offender"] = cell
                    elif any(k in header for k in ("fact", "description", "brief")):
                        case["facts"] = cell
                    elif any(k in header for k in ("outcome", "action taken", "action", "result")):
                        case["outcome"] = cell
                    elif any(k in header for k in ("date", "when")):
                        case["date_text"] = cell

                # Build full text from all available content
                text_parts = []
                if case.get("nature_of_offence"):
                    text_parts.append(f"Nature of offence: {case['nature_of_offence']}")
                if case.get("offender"):
                    text_parts.append(f"Offender(s): {case['offender']}")
                if case.get("facts"):
                    text_parts.append(f"Facts: {case['facts']}")
                if case.get("outcome"):
                    text_parts.append(f"Outcome: {case['outcome']}")
                if case.get("date_text"):
                    text_parts.append(f"Date: {case['date_text']}")

                case["text"] = "\n\n".join(text_parts)

                # Only include cases with meaningful content
                if len(case["text"]) > 50:
                    cases.append(case)

        return cases

    def _build_url(self, category: Dict, year: int) -> str:
        """Build the URL for a category/year page."""
        slug = category["slug"].format(year=year)
        return f"{BASE_URL}/regulation/enforcement/actions/{slug}"

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all enforcement action records across all categories and years."""
        for cat in CATEGORIES:
            for year in range(cat["end"], cat["start"] - 1, -1):
                url = self._build_url(cat, year)
                logger.info("Fetching %s %d: %s", cat["name"], year, url)
                html = self._fetch_year_page(cat, year)
                if not html:
                    continue

                cases = self._extract_cases_from_page(html, cat["name"], year)
                logger.info("  Found %d cases for %s %d", len(cases), cat["name"], year)

                for case in cases:
                    case["url"] = url
                    yield case

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Yield records from recent years only."""
        since_year = since.year
        for cat in CATEGORIES:
            for year in range(cat["end"], max(since_year - 1, cat["start"]) - 1, -1):
                html = self._fetch_year_page(cat, year)
                if not html:
                    continue
                cases = self._extract_cases_from_page(html, cat["name"], year)
                for case in cases:
                    case["url"] = self._build_url(cat, year)
                    yield case

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize a raw enforcement case into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 50:
            return None

        category = raw.get("category", "")
        year = raw.get("year", "")
        offender = raw.get("offender", "Unknown")
        nature = raw.get("nature_of_offence", "")

        # Build title
        offender_short = offender.split("\n")[0][:100].strip()
        title = f"{offender_short} — {category} ({year})"

        # Build unique ID using content hash
        content_hash = hashlib.md5(
            f"{category}|{year}|{raw.get('table_index')}|{raw.get('row_index')}".encode()
        ).hexdigest()[:12]
        doc_id = f"MY-SC-{year}-{content_hash}"

        return {
            "_id": doc_id,
            "_source": "MY/SC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date_text", ""),
            "category": category,
            "year": year,
            "offender": offender,
            "nature_of_offence": nature,
            "facts": raw.get("facts", ""),
            "outcome": raw.get("outcome", ""),
            "url": raw["url"],
        }


# ── CLI entry point ──────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="MY/SC bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Run full bootstrap")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--sample-size", type=int, default=15, help="Sample size")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    scraper = SCMalaysiaScraper()

    if args.command == "test":
        for cat in CATEGORIES:
            url = scraper._build_url(cat, cat["end"])
            resp = scraper.http.get(url, allow_redirects=False)
            status = "OK" if resp.status_code == 200 else f"HTTP {resp.status_code}"
            print(f"{cat['name']}: {status}")
            time.sleep(1)
        return

    if args.command == "bootstrap":
        sample = args.sample and not args.full
        stats = scraper.bootstrap(sample_mode=sample, sample_size=args.sample_size)
        print(json.dumps(stats, indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
