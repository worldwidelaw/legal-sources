#!/usr/bin/env python3
"""
INTL/WIPOLex -- WIPO Lex Global IP Law Database

Fetches IP legislation from 200+ jurisdictions via WIPO Lex.

Strategy:
  - Scrape members page for all country/org codes
  - For each jurisdiction, fetch results page to get legislation detail IDs
  - For each detail ID, fetch the page and extract inline full text (HTML)
  - If no inline text, document is PDF-only (skip for now)
  - ~50,000+ laws/regulations across all jurisdictions

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.WIPOLex")

BASE_URL = "https://www.wipo.int"
MEMBERS_URL = f"{BASE_URL}/wipolex/en/legislation/members"
RESULTS_URL = f"{BASE_URL}/wipolex/en/legislation/results"
DETAIL_URL = f"{BASE_URL}/wipolex/en/legislation/details"

# Sample jurisdictions for --sample mode (small, diverse set)
SAMPLE_JURISDICTIONS = ["LI", "MT", "IS", "CY", "LU"]


def strip_html(html_text: str) -> str:
    """Strip HTML tags and clean whitespace from text."""
    if not html_text:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</h\d>', '\n\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</tr>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</td>', '\t', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = unescape(text)
    # Collapse multiple blank lines
    text = re.sub(r'\n{3,}', '\n\n', text)
    # Collapse multiple spaces/tabs
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r' *\n *', '\n', text)
    return text.strip()


class WIPOLexScraper(BaseScraper):
    """
    Scraper for INTL/WIPOLex -- WIPO Lex Global IP Law Database.
    Country: INTL
    URL: https://www.wipo.int/wipolex/en/

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })

    def _get_jurisdictions(self, sample_mode: bool = False) -> list[str]:
        """Get list of jurisdiction codes from the members page."""
        if sample_mode:
            return SAMPLE_JURISDICTIONS

        r = self.session.get(MEMBERS_URL, timeout=30)
        r.raise_for_status()

        # Extract from window.membersPageData JSON embedded in the page
        import json as _json
        match = re.search(r'members:\s*(\[.*?\])\s*[,}]', r.text, re.DOTALL)
        if match:
            members = _json.loads(match.group(1))
            codes = [m['code'] for m in members if m.get('code')]
            logger.info(f"Found {len(codes)} jurisdictions from membersPageData")
            return codes

        logger.warning("Could not parse membersPageData, using fallback")
        return SAMPLE_JURISDICTIONS

    def _get_legislation_ids(self, country_code: str) -> list[str]:
        """Get all legislation detail IDs for a given jurisdiction."""
        params = {"countryOrgs": country_code, "last": "true"}
        try:
            r = self.session.get(RESULTS_URL, params=params, timeout=30)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch results for {country_code}: {e}")
            return []

        ids = list(set(re.findall(
            r'/wipolex/en/legislation/details/(\d+)',
            r.text,
        )))
        logger.info(f"  {country_code}: {len(ids)} legislation documents")
        return ids

    def _parse_detail_page(self, doc_id: str) -> Optional[dict]:
        """Fetch and parse a legislation detail page."""
        url = f"{DETAIL_URL}/{doc_id}"
        try:
            r = self.session.get(url, timeout=60)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch detail {doc_id}: {e}")
            return None

        html = r.text

        # Extract title from the heading
        title_match = re.search(
            r'class="laws-heading[^"]*"[^>]*>(.*?)</div>',
            html,
            re.DOTALL,
        )
        title = ""
        if title_match:
            title = strip_html(title_match.group(1))
            # Remove "Back" navigation text
            title = re.sub(r'\bBack\b', '', title).strip()
            # Remove "Latest Version in WIPO Lex"
            title = re.sub(r'Latest Version in WIPO Lex', '', title).strip()

        # Extract metadata from between heading and textFrame
        metadata = {}
        heading_pos = html.find('laws-heading')
        textframe_pos = html.find('textFrame')
        if heading_pos > -1 and textframe_pos > -1:
            meta_section = html[heading_pos:textframe_pos]

            # Year of Version
            year_match = re.search(r'Year of Version.*?(\d{4})', meta_section, re.DOTALL)
            if year_match:
                metadata['year_of_version'] = year_match.group(1)

            # Dates (multiple types)
            for date_type in ['Enacted', 'Issued', 'Adopted', 'Entry into force', 'Amended']:
                date_match = re.search(
                    rf'{date_type}:?\s*</[^>]+>\s*<[^>]+>([^<]+)',
                    meta_section,
                    re.DOTALL,
                )
                if date_match:
                    metadata[date_type.lower().replace(' ', '_')] = date_match.group(1).strip()

            # Type of Text
            type_match = re.search(r'Type of Text.*?</[^>]+>\s*<[^>]+>([^<]+)', meta_section, re.DOTALL)
            if type_match:
                metadata['type_of_text'] = type_match.group(1).strip()

            # Subject Matter
            subj_match = re.search(r'Subject Matter.*?</[^>]+>\s*<[^>]+>([^<]+)', meta_section, re.DOTALL)
            if subj_match:
                metadata['subject_matter'] = subj_match.group(1).strip()

            # WIPO Lex No.
            lex_no_match = re.search(r'WIPO Lex No\.\s*</[^>]+>\s*<[^>]+>([^<]+)', meta_section, re.DOTALL)
            if lex_no_match:
                metadata['wipolex_no'] = lex_no_match.group(1).strip()

        # Extract full text (inline HTML content)
        text = ""
        text_match = re.search(
            r'class="htmlView content needTranslation">(.*?)(?:</div>\s*</div>\s*</div>\s*<div class="textFrame"|</div>\s*</div>\s*</div>\s*</div>\s*</div>\s*<div class="machine)',
            html,
            re.DOTALL,
        )
        if text_match:
            text = strip_html(text_match.group(1))

        # If no text found via the specific class, try a broader search
        if not text:
            # Try to find any large block of content after textFrame
            all_texts = re.findall(
                r'class="htmlView content needTranslation">(.*?)</div>\s*(?:</div>){2,}',
                html,
                re.DOTALL,
            )
            if all_texts:
                # Take the longest text block
                best = max(all_texts, key=len)
                text = strip_html(best)

        # Determine the best date
        date = None
        for key in ['enacted', 'issued', 'adopted', 'entry_into_force', 'amended']:
            if key in metadata:
                date = self._parse_date(metadata[key])
                if date:
                    break
        if not date and 'year_of_version' in metadata:
            date = f"{metadata['year_of_version']}-01-01"

        return {
            "id": doc_id,
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "metadata": metadata,
        }

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse various date formats to ISO 8601."""
        if not date_str:
            return None
        date_str = date_str.strip()

        # Try "Month DD, YYYY" format
        for fmt in ["%B %d, %Y", "%b %d, %Y", "%B %Y", "%Y-%m-%d", "%d/%m/%Y"]:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        # Try extracting just a year
        year_match = re.search(r'(\d{4})', date_str)
        if year_match:
            return f"{year_match.group(1)}-01-01"

        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legislation documents from all jurisdictions."""
        jurisdictions = self._get_jurisdictions(sample_mode=False)
        total_docs = 0

        for i, country in enumerate(jurisdictions):
            logger.info(f"Processing jurisdiction {i+1}/{len(jurisdictions)}: {country}")
            doc_ids = self._get_legislation_ids(country)
            time.sleep(1)

            for doc_id in doc_ids:
                record = self._parse_detail_page(doc_id)
                if record:
                    record["jurisdiction"] = country
                    yield record
                    total_docs += 1
                time.sleep(2)  # Respect rate limits

            logger.info(f"  Completed {country}, total so far: {total_docs}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch updates - for WIPO Lex we re-fetch all as there's no date filter."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw document into standard schema."""
        if not raw:
            return None

        text = raw.get("text", "")
        if not text or len(text) < 50:
            # Skip documents without substantial text (PDF-only)
            return None

        title = raw.get("title", "")
        if not title:
            return None

        metadata = raw.get("metadata", {})
        doc_id = raw["id"]

        return {
            "_id": f"INTL/WIPOLex/{doc_id}",
            "_source": "INTL/WIPOLex",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "wipolex_id": doc_id,
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "jurisdiction": raw.get("jurisdiction", ""),
            "type_of_text": metadata.get("type_of_text", ""),
            "subject_matter": metadata.get("subject_matter", ""),
            "wipolex_no": metadata.get("wipolex_no", ""),
            "year_of_version": metadata.get("year_of_version", ""),
        }

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            r = self.session.get(MEMBERS_URL, timeout=15)
            r.raise_for_status()
            has_members = 'membersPageData' in r.text
            logger.info(f"Connectivity test: members page OK, found profiles: {has_members}")

            # Test a single detail page
            r2 = self.session.get(f"{DETAIL_URL}/6585", timeout=15)
            r2.raise_for_status()
            has_text = 'htmlView content needTranslation' in r2.text
            logger.info(f"Detail page test: OK, has inline text: {has_text}")

            return has_members and has_text
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            return False


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="INTL/WIPOLex bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    args = parser.parse_args()

    scraper = WIPOLexScraper()

    if args.command == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        if args.sample:
            # Override fetch_all to use sample jurisdictions
            original_fetch = scraper.fetch_all

            def sample_fetch():
                jurisdictions = SAMPLE_JURISDICTIONS
                for country in jurisdictions:
                    logger.info(f"[SAMPLE] Processing: {country}")
                    doc_ids = scraper._get_legislation_ids(country)
                    time.sleep(1)
                    for doc_id in doc_ids:
                        record = scraper._parse_detail_page(doc_id)
                        if record:
                            record["jurisdiction"] = country
                            yield record
                        time.sleep(2)

            scraper.fetch_all = sample_fetch
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
        else:
            stats = scraper.bootstrap()
        print(json.dumps(stats, indent=2))
    elif args.command == "update":
        stats = scraper.update()
        print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
