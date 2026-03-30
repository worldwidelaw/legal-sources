#!/usr/bin/env python3
"""
INTL/WIPOLex-Judgments -- WIPO Lex IP Judicial Decisions

Fetches leading IP judicial decisions from 46 jurisdictions via WIPO Lex.

Strategy:
  - Iterate over known jurisdictions, fetch results page per country
  - Extract judgment detail IDs from results HTML
  - For each judgment, parse detail page for metadata (wu-field elements)
  - Fetch full text from /wipolex/en/text/{id} pages (HTML versions preferred)
  - Skip PDF-only documents
  - ~2,300 judgments across 46 jurisdictions

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
logger = logging.getLogger("legal-data-hunter.INTL.WIPOLex-Judgments")

BASE_URL = "https://www.wipo.int"
RESULTS_URL = f"{BASE_URL}/wipolex/en/judgments/results"
DETAIL_URL = f"{BASE_URL}/wipolex/en/judgments/details"
TEXT_URL = f"{BASE_URL}/wipolex/en/text"

# All 46 jurisdictions with judgments
ALL_JURISDICTIONS = [
    "OAPI", "AL", "CAN", "AU", "BZ", "BJ", "BR", "BF", "CM", "CL",
    "CN", "CG", "CR", "CZ", "CI", "EG", "EPO", "EU", "GA", "DE",
    "HK", "IN", "JM", "JP", "LV", "LT", "MX", "NZ", "NE", "NG",
    "PA", "PY", "PE", "PH", "PL", "PT", "KR", "SN", "SG", "ES",
    "TG", "TT", "UA", "GB", "TZ", "US",
]

# Small set for --sample mode
SAMPLE_JURISDICTIONS = ["LV", "NZ", "PT"]


def strip_html(html_text: str) -> str:
    """Strip HTML tags and clean whitespace from text."""
    if not html_text:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</h\d>', '\n\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</tr>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</td>', '\t', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = unescape(text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r' *\n *', '\n', text)
    return text.strip()


class WIPOLexJudgmentsScraper(BaseScraper):
    """
    Scraper for INTL/WIPOLex-Judgments -- WIPO Lex IP Judicial Decisions.
    Country: INTL
    URL: https://www.wipo.int/wipolex/en/main/judgments

    Data types: case_law
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

    def _get_judgment_ids(self, country_code: str) -> list[str]:
        """Get all judgment detail IDs for a given jurisdiction."""
        params = {"countryOrg": country_code}
        try:
            r = self.session.get(RESULTS_URL, params=params, timeout=30)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch results for {country_code}: {e}")
            return []

        ids = list(set(re.findall(
            r'/wipolex/en/judgments/details/(\d+)',
            r.text,
        )))
        logger.info(f"  {country_code}: {len(ids)} judgments")
        return ids

    def _parse_wu_fields(self, html: str) -> dict:
        """Extract metadata from wu-field web components."""
        metadata = {}
        fields = re.findall(r'<wu-field[^>]*>(.*?)</wu-field>', html, re.DOTALL)

        current_label = None
        for field_html in fields:
            # Clean and split into parts
            clean = re.sub(r'<[^>]+>', '|', field_html)
            parts = [p.strip() for p in clean.split('|') if p.strip()]

            if not parts:
                continue

            label = parts[0]

            if label == "Date of Judgment" and len(parts) > 1:
                metadata["date_of_judgment"] = parts[1]
            elif label == "Issuing Authority" and len(parts) > 1:
                metadata["issuing_authority"] = parts[1]
            elif label == "Level of the Issuing Authority" and len(parts) > 1:
                metadata["authority_level"] = parts[1]
            elif label == "Type of Procedure" and len(parts) > 1:
                metadata["procedure_type"] = parts[1]
            elif label == "Subject Matter" and len(parts) > 1:
                metadata["subject_matter"] = parts[1]
            elif label == "Plaintiff/Appellant" and len(parts) > 1:
                metadata["plaintiff"] = unescape(parts[1])
            elif label == "Defendant/Respondent" and len(parts) > 1:
                metadata["defendant"] = unescape(parts[1])
            elif label in ("Keywords", "Judgment/Decision", "Summary"):
                current_label = label
            elif current_label == "Keywords":
                # Keywords are spread across multiple parts
                keywords = [unescape(p.rstrip(',')) for p in parts if p.rstrip(',')]
                metadata["keywords"] = keywords
                current_label = None

        return metadata

    def _find_html_text_ids(self, html: str) -> list[str]:
        """Find text IDs that have HTML versions (not PDF-only)."""
        # Look for text links labeled "HTML" within judgment/summary sections
        html_text_ids = []

        # Pattern: text link followed by or containing "HTML" label
        # In wu-field sections for Judgment/Decision and Summary
        sections = re.split(r'<wu-field', html)
        for section in sections:
            if 'HTML' in section:
                text_ids = re.findall(r'/wipolex/en/text/(\d+)', section)
                html_text_ids.extend(text_ids)

        return list(dict.fromkeys(html_text_ids))  # deduplicate preserving order

    def _fetch_text(self, text_id: str) -> str:
        """Fetch full text from a text page."""
        url = f"{TEXT_URL}/{text_id}"
        try:
            r = self.session.get(url, timeout=60)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch text {text_id}: {e}")
            return ""

        # Try to extract inline HTML content
        matches = re.findall(
            r'class="htmlView[^"]*"[^>]*>(.*?)(?:</div>\s*){2,}',
            r.text,
            re.DOTALL,
        )
        if matches:
            best = max(matches, key=len)
            return strip_html(best)

        # Broader search for any content blocks
        matches = re.findall(
            r'class="htmlView[^"]*">(.*?)</div>',
            r.text,
            re.DOTALL,
        )
        if matches:
            best = max(matches, key=len)
            text = strip_html(best)
            if len(text) > 50:
                return text

        return ""

    def _parse_detail_page(self, judgment_id: str) -> Optional[dict]:
        """Fetch and parse a judgment detail page."""
        url = f"{DETAIL_URL}/{judgment_id}"
        try:
            r = self.session.get(url, timeout=60)
            if r.status_code == 404:
                return None
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch detail {judgment_id}: {e}")
            return None

        html = r.text

        # Extract title from h1
        title = ""
        h1_match = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
        if h1_match:
            title = strip_html(h1_match.group(1))
            # Clean up country suffix
            title = re.sub(r',\s*$', '', title).strip()

        # Extract metadata from wu-field elements
        metadata = self._parse_wu_fields(html)

        # Find HTML text IDs
        html_text_ids = self._find_html_text_ids(html)

        # Fetch full text from HTML text pages
        text = ""
        for text_id in html_text_ids:
            time.sleep(1.5)
            text = self._fetch_text(text_id)
            if text and len(text) > 50:
                break

        # Parse date
        date = self._parse_date(metadata.get("date_of_judgment", ""))

        return {
            "id": judgment_id,
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

        for fmt in ["%B %d, %Y", "%b %d, %Y", "%B %Y", "%Y-%m-%d", "%d/%m/%Y", "%d %B %Y"]:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        year_match = re.search(r'(\d{4})', date_str)
        if year_match:
            return f"{year_match.group(1)}-01-01"

        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all judgment documents from all jurisdictions."""
        jurisdictions = ALL_JURISDICTIONS
        total_docs = 0

        for i, country in enumerate(jurisdictions):
            logger.info(f"Processing jurisdiction {i+1}/{len(jurisdictions)}: {country}")
            judgment_ids = self._get_judgment_ids(country)
            time.sleep(1)

            for jid in judgment_ids:
                record = self._parse_detail_page(jid)
                if record:
                    record["jurisdiction"] = country
                    yield record
                    total_docs += 1
                time.sleep(2)

            logger.info(f"  Completed {country}, total so far: {total_docs}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch updates - re-fetch all as there's no date filter."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw document into standard schema."""
        if not raw:
            return None

        text = raw.get("text", "")
        if not text or len(text) < 50:
            return None

        title = raw.get("title", "")
        if not title:
            return None

        metadata = raw.get("metadata", {})
        doc_id = raw["id"]

        return {
            "_id": f"INTL/WIPOLex-Judgments/{doc_id}",
            "_source": "INTL/WIPOLex-Judgments",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "judgment_id": doc_id,
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "jurisdiction": raw.get("jurisdiction", ""),
            "issuing_authority": metadata.get("issuing_authority", ""),
            "authority_level": metadata.get("authority_level", ""),
            "procedure_type": metadata.get("procedure_type", ""),
            "subject_matter": metadata.get("subject_matter", ""),
            "plaintiff": metadata.get("plaintiff", ""),
            "defendant": metadata.get("defendant", ""),
        }

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            r = self.session.get(f"{RESULTS_URL}?countryOrg=LV", timeout=15)
            r.raise_for_status()
            ids = re.findall(r'/wipolex/en/judgments/details/(\d+)', r.text)
            logger.info(f"Connectivity test: results page OK, found {len(ids)} judgments for LV")

            if ids:
                r2 = self.session.get(f"{DETAIL_URL}/{ids[0]}", timeout=15)
                r2.raise_for_status()
                has_fields = 'wu-field' in r2.text
                logger.info(f"Detail page test: OK, has wu-field: {has_fields}")
                return has_fields

            return len(ids) > 0
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            return False


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="INTL/WIPOLex-Judgments bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    args = parser.parse_args()

    scraper = WIPOLexJudgmentsScraper()

    if args.command == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        if args.sample:
            original_fetch = scraper.fetch_all

            def sample_fetch():
                for country in SAMPLE_JURISDICTIONS:
                    logger.info(f"[SAMPLE] Processing: {country}")
                    jids = scraper._get_judgment_ids(country)
                    time.sleep(1)
                    for jid in jids:
                        record = scraper._parse_detail_page(jid)
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
