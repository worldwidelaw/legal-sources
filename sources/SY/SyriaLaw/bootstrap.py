#!/usr/bin/env python3
"""
SY/SyriaLaw -- English Syrian Legislation & Case Law

Fetches legislation and case law from syria.law via the WordPress REST API.
Legislation pages contain multiple laws as HTML tables, and the Case Law page
contains ~280 Court of Cassation judgments.

Source: https://www.syria.law/
Rate limit: 1 req/sec

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import html as html_mod
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SY.SyriaLaw")

BASE_URL = "https://www.syria.law"
WP_API = f"{BASE_URL}/index.php/wp-json/wp/v2"

# Legislation page IDs (exclude non-law pages like Contact Us, home, main listing)
LEGISLATION_PAGE_IDS = [
    175,   # Court System
    180,   # Civil Code
    184,   # Commercial Law
    192,   # Company Law
    198,   # Investment Laws
    205,   # Property Law
    212,   # Intellectual Property Law
    224,   # Finance
    234,   # Insurance Law
    239,   # Employment Law
    246,   # Public Procurement Law
    251,   # Taxation Law
    267,   # Family Law
    272,   # Criminal Law
    279,   # Nationality Law
    1676,  # Transport Law
    127,   # Legislation Under Review
]

CASE_LAW_PAGE_ID = 1733


class SyriaLawScraper(BaseScraper):
    """
    Scraper for SY/SyriaLaw -- English Syrian Legislation & Case Law.
    Country: SY
    URL: https://www.syria.law/

    Data types: legislation, case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json",
            },
            timeout=60,
        )

    def _clean_html(self, text: str) -> str:
        """Strip HTML tags and clean whitespace."""
        if not text:
            return ""
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<(?:p|div|br|h[1-6]|li|tr)[^>]*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<(?:td|th)[^>]*>', ' ', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', '', text)
        text = html_mod.unescape(text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n[ \t]+', '\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _extract_table_field(self, table_html: str, field_name: str) -> str:
        """Extract a field value from a table row by field name."""
        pattern = r'<td[^>]*>\s*' + re.escape(field_name) + r'\s*</td>\s*<td[^>]*>(.*?)</td>'
        match = re.search(pattern, table_html, re.DOTALL | re.IGNORECASE)
        if match:
            return self._clean_html(match.group(1))
        return ""

    def _extract_h4(self, table_html: str) -> str:
        """Extract h4 title from a table."""
        match = re.search(r'<h4[^>]*>(.*?)</h4>', table_html, re.DOTALL | re.IGNORECASE)
        if match:
            return self._clean_html(match.group(1))
        return ""

    def _extract_bold(self, table_html: str) -> str:
        """Extract first bold/strong text from a table."""
        match = re.search(r'<strong[^>]*>(.*?)</strong>', table_html, re.DOTALL | re.IGNORECASE)
        if match:
            return self._clean_html(match.group(1))
        return ""

    def _parse_legislation_page(self, page_data: dict) -> List[dict]:
        """Parse a legislation page into individual law records."""
        records = []
        content = page_data.get("content", {}).get("rendered", "")
        category = page_data.get("title", {}).get("rendered", "")
        page_slug = page_data.get("slug", "")
        page_link = page_data.get("link", "")

        # Split into tables
        tables = re.findall(r'<table[^>]*>(.*?)</table>', content, re.DOTALL)

        for i, table_html in enumerate(tables):
            title = self._extract_h4(table_html)
            if not title:
                title = self._extract_bold(table_html)
            if not title:
                continue

            reference = self._extract_table_field(table_html, "REFERENCE")
            if not reference:
                reference = self._extract_bold(table_html)

            date_text = self._extract_table_field(table_html, "DATE OF PROMULGATION")
            provisions = self._extract_table_field(table_html, "RELEVANT PROVISIONS")
            related = self._extract_table_field(table_html, "RELATED LEGISLATION")
            historical = self._extract_table_field(table_html, "HISTORICAL CONTEXT")

            # Build full text
            text_parts = []
            if title:
                text_parts.append(f"Law: {title}")
            if reference:
                text_parts.append(f"Reference: {reference}")
            if date_text:
                text_parts.append(f"Date of Promulgation: {date_text}")
            if provisions:
                text_parts.append(f"\nRelevant Provisions:\n{provisions}")
            if related:
                text_parts.append(f"\nRelated Legislation:\n{related}")
            if historical:
                text_parts.append(f"\nHistorical Context:\n{historical}")

            full_text = "\n".join(text_parts)
            if len(full_text.strip()) < 30:
                continue

            # Parse date
            date_iso = self._parse_date(date_text)

            # Extract table ID for URL anchor
            table_id_match = re.search(r'id="([^"]+)"', f'<table{table_html}')

            records.append({
                "title": title,
                "text": full_text,
                "date": date_iso,
                "category": category,
                "reference": reference,
                "url": page_link,
                "_type": "legislation",
            })

        return records

    def _parse_case_law_page(self, page_data: dict) -> List[dict]:
        """Parse the case law page into individual judgment records."""
        records = []
        content = page_data.get("content", {}).get("rendered", "")
        page_link = page_data.get("link", "")

        tables = re.findall(r'<table[^>]*>(.*?)</table>', content, re.DOTALL)
        current_category = ""

        for table_html in tables:
            h4 = self._extract_h4(table_html)
            bold = self._extract_bold(table_html)

            # Tables with an h4 define the category AND first judgment
            if h4:
                current_category = h4

            # The judgment number is in the <strong> tag
            judgment_ref = bold if bold else ""

            # Extract the full text of the table
            full_text = self._clean_html(table_html)
            if len(full_text.strip()) < 30:
                continue

            title = judgment_ref if judgment_ref else h4
            if current_category and title and current_category not in title:
                title = f"{current_category} - {title}"

            # Parse date from judgment reference (e.g., "Court Judgment No. 29/1983")
            date_iso = None
            year_match = re.search(r'/(\d{4})', judgment_ref)
            if year_match:
                date_iso = f"{year_match.group(1)}-01-01"

            records.append({
                "title": title,
                "text": full_text,
                "date": date_iso,
                "category": current_category,
                "reference": judgment_ref,
                "url": page_link,
                "_type": "case_law",
            })

        return records

    def _parse_date(self, date_text: str) -> Optional[str]:
        """Try to extract an ISO date from free-text date description."""
        if not date_text:
            return None
        # Look for patterns like "January 3, 2016" or "November 15, 1961"
        match = re.search(r'(\w+ \d{1,2},?\s+\d{4})', date_text)
        if match:
            try:
                dt = datetime.strptime(match.group(1).replace(',', ''), "%B %d %Y")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass
        # Look for just a year
        year_match = re.search(r'\b(19\d{2}|20\d{2})\b', date_text)
        if year_match:
            return f"{year_match.group(1)}-01-01"
        return None

    def normalize(self, raw: dict) -> dict:
        """Transform a raw record into the standard schema."""
        return {
            "_id": "SY-SyriaLaw-" + re.sub(r'[^a-zA-Z0-9]+', '-', raw.get("title", "unknown"))[:80],
            "_source": "SY/SyriaLaw",
            "_type": raw.get("_type", "legislation"),
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", BASE_URL),
            "category": raw.get("category", ""),
            "reference": raw.get("reference", ""),
        }

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch updates since a given date (not supported)."""
        logger.info("fetch_updates not supported; use fetch_all")
        return
        yield

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all legislation and case law from syria.law."""
        seen_ids = set()

        # Fetch legislation pages
        logger.info("Fetching legislation pages...")
        for page_id in LEGISLATION_PAGE_IDS:
            self.rate_limiter.wait()
            try:
                resp = self.client.get(f"{WP_API}/pages/{page_id}", timeout=30)
                page_data = resp.json()
                title = page_data.get("title", {}).get("rendered", "")
                records = self._parse_legislation_page(page_data)
                logger.info(f"  Page '{title}': {len(records)} laws")
                for raw in records:
                    record = self.normalize(raw)
                    if record["_id"] not in seen_ids:
                        seen_ids.add(record["_id"])
                        yield record
            except Exception as e:
                logger.warning(f"Failed to fetch page {page_id}: {e}")
                time.sleep(2)

        # Fetch case law page
        logger.info("Fetching case law page...")
        self.rate_limiter.wait()
        try:
            resp = self.client.get(f"{WP_API}/pages/{CASE_LAW_PAGE_ID}", timeout=60)
            page_data = resp.json()
            records = self._parse_case_law_page(page_data)
            logger.info(f"  Case law: {len(records)} judgments")
            for raw in records:
                record = self.normalize(raw)
                if record["_id"] not in seen_ids:
                    seen_ids.add(record["_id"])
                    yield record
        except Exception as e:
            logger.warning(f"Failed to fetch case law page: {e}")

        # Fetch doctrine posts
        logger.info("Fetching doctrine posts...")
        self.rate_limiter.wait()
        try:
            resp = self.client.get(f"{WP_API}/posts?per_page=100", timeout=30)
            posts = resp.json()
            logger.info(f"  Posts: {len(posts)}")
            for post in posts:
                title = post.get("title", {}).get("rendered", "")
                content_html = post.get("content", {}).get("rendered", "")
                text = self._clean_html(content_html)
                if len(text) < 50:
                    continue

                date_str = post.get("date", "")
                date_iso = date_str[:10] if date_str else None

                raw = {
                    "title": title,
                    "text": text,
                    "date": date_iso,
                    "category": "Doctrine",
                    "reference": "",
                    "url": post.get("link", BASE_URL),
                    "_type": "doctrine",
                }
                record = self.normalize(raw)
                if record["_id"] not in seen_ids:
                    seen_ids.add(record["_id"])
                    yield record
        except Exception as e:
            logger.warning(f"Failed to fetch posts: {e}")

        logger.info(f"Completed: {len(seen_ids)} total records")

    def fetch_sample(self, n: int = 15) -> List[dict]:
        """Fetch a sample of records for testing."""
        logger.info(f"Fetching {n} sample records...")
        samples = []

        # Get a few legislation pages
        for page_id in LEGISLATION_PAGE_IDS[:4]:
            if len(samples) >= n:
                break
            self.rate_limiter.wait()
            try:
                resp = self.client.get(f"{WP_API}/pages/{page_id}", timeout=30)
                page_data = resp.json()
                records = self._parse_legislation_page(page_data)
                for raw in records:
                    if len(samples) >= n - 5:
                        break
                    record = self.normalize(raw)
                    samples.append(record)
            except Exception as e:
                logger.warning(f"Failed to fetch page {page_id}: {e}")

        # Get some case law
        self.rate_limiter.wait()
        try:
            resp = self.client.get(f"{WP_API}/pages/{CASE_LAW_PAGE_ID}", timeout=60)
            page_data = resp.json()
            records = self._parse_case_law_page(page_data)
            for raw in records[:5]:
                if len(samples) >= n:
                    break
                record = self.normalize(raw)
                samples.append(record)
        except Exception as e:
            logger.warning(f"Failed to fetch case law: {e}")

        logger.info(f"Collected {len(samples)} samples")
        return samples

    def test_api(self):
        """Test API connectivity."""
        logger.info("Testing syria.law WordPress API...")
        resp = self.client.get(f"{WP_API}/pages?per_page=100", timeout=30)
        pages = resp.json()
        logger.info(f"Pages: {len(pages)}")
        for p in pages[:5]:
            content_len = len(p.get("content", {}).get("rendered", ""))
            logger.info(f"  - {p['title']['rendered']}: {content_len} chars")

        self.rate_limiter.wait()
        resp = self.client.get(f"{WP_API}/posts?per_page=100", timeout=30)
        posts = resp.json()
        logger.info(f"Posts: {len(posts)}")

    @staticmethod
    def cli():
        import argparse

        parser = argparse.ArgumentParser(description="SY/SyriaLaw bootstrap")
        parser.add_argument("command", choices=["bootstrap", "test-api"])
        parser.add_argument("--sample", action="store_true")
        parser.add_argument("--full", action="store_true")
        args = parser.parse_args()

        scraper = SyriaLawScraper()

        if args.command == "test-api":
            scraper.test_api()
            return

        if args.command == "bootstrap":
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)

            if args.sample:
                records = scraper.fetch_sample(15)
            else:
                records = list(scraper.fetch_all())

            for i, record in enumerate(records):
                out_path = sample_dir / f"{i:04d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            logger.info(f"Wrote {len(records)} records to {sample_dir}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    SyriaLawScraper.cli()
