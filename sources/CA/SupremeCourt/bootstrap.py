#!/usr/bin/env python3
"""
CA/SupremeCourt -- Supreme Court of Canada Decisions Fetcher

Fetches case law decisions from the Supreme Court of Canada decisions portal.

Strategy:
  - Bootstrap: Iterate through years (1970-current), extracting case item IDs
    from each year's navigation page, then fetch full judgment HTML for each case
  - Update: Use the RSS feed to get recently added/updated cases
  - Sample: Fetches 12+ recent English decisions for validation

Source: https://decisions.scc-csc.ca
RSS Feed: https://decisions.scc-csc.ca/scc-csc/scc-csc/en/rss.do

Data notes:
  - ~15,500+ decisions (1970-present with full text, older cases have less detail)
  - Bilingual: English (en) and French (fr)
  - Full HTML content with judges, citations, subject areas
  - PDF versions also available

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records for validation
  python bootstrap.py update             # Incremental update via RSS
  python bootstrap.py test-api           # Quick API connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List
from xml.etree import ElementTree as ET

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CA.SupremeCourt")

# API base URL
BASE_URL = "https://decisions.scc-csc.ca"
RSS_URL = f"{BASE_URL}/scc-csc/scc-csc/en/rss.do"

# Year range for bootstrap (the site has cases from 1970 onwards in the judgments collection)
START_YEAR = 1970


class CASupremeCourtScraper(BaseScraper):
    """
    Scraper for CA/SupremeCourt -- Supreme Court of Canada Decisions.
    Country: CA
    URL: https://decisions.scc-csc.ca

    Data types: case_law
    Auth: none (Open Access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "text/html, application/xhtml+xml",
            },
            timeout=60,
        )

    # -- API helpers --------------------------------------------------------

    def _get_rss_feed(self) -> Optional[ET.Element]:
        """Fetch the RSS feed for recent decisions."""
        self.rate_limiter.wait()

        try:
            resp = self.client.get(RSS_URL)
            resp.raise_for_status()
            return ET.fromstring(resp.content)
        except Exception as e:
            logger.error(f"Error fetching RSS feed: {e}")
            return None

    def _get_year_cases(self, year: int) -> List[str]:
        """Get all case item IDs for a given year, handling pagination."""
        all_ids = set()
        page = 1
        pattern = r'/scc-csc/scc-csc/en/item/(\d+)/index\.do'

        while True:
            url = f"/scc-csc/scc-csc/en/{year}/nav_date.do?page={page}&iframe=true"
            self.rate_limiter.wait()

            try:
                resp = self.client.get(url)
                resp.raise_for_status()
                ids = set(re.findall(pattern, resp.text))
                new_ids = ids - all_ids

                if not new_ids:
                    break

                all_ids.update(new_ids)
                page += 1

            except Exception as e:
                # 404 on last page is expected (no more pages)
                if '404' not in str(e):
                    logger.error(f"Error fetching year {year} page {page}: {e}")
                break

        logger.info(f"Year {year}: found {len(all_ids)} cases across {page - 1} page(s)")
        return list(all_ids)

    def _get_case_content(self, item_id: str) -> Optional[dict]:
        """Fetch full case content for a given item ID."""
        url = f"/scc-csc/scc-csc/en/item/{item_id}/index.do?iframe=true"
        self.rate_limiter.wait()

        try:
            resp = self.client.get(url)
            resp.raise_for_status()
            content = resp.text

            return self._parse_case_html(item_id, content)

        except Exception as e:
            logger.error(f"Error fetching case {item_id}: {e}")
            return None

    def _parse_case_html(self, item_id: str, html_content: str) -> dict:
        """Parse case HTML and extract metadata and full text."""
        result = {
            "item_id": item_id,
            "html_raw": html_content,
        }

        # Extract title from <h3 class="title">
        title_match = re.search(r'<h3[^>]*class="title"[^>]*>([^<]+)</h3>', html_content)
        if title_match:
            result["title"] = html.unescape(title_match.group(1).strip())

        # Extract metadata from table rows
        # Pattern: <td class="label">Field</td><td class="metadata">Value (no closing </td> tag)
        # The SCC HTML doesn't properly close metadata TDs, they end at </tr>
        metadata_pattern = r'<td[^>]*class="label"[^>]*>([^<]+)</td>\s*<td[^>]*class="metadata"[^>]*>(.*?)</tr>'
        for match in re.finditer(metadata_pattern, html_content, re.DOTALL):
            label = match.group(1).strip().lower()
            value = self._clean_html(match.group(2))

            if "date" in label and "date" not in result:
                result["decision_date"] = value.strip()
            elif "neutral citation" in label:
                result["neutral_citation"] = value.strip()
            elif "case number" in label or "docket" in label:
                result["case_number"] = value.strip()
            elif "judges" in label:
                result["judges"] = value.strip()
            elif "on appeal from" in label:
                result["appealed_from"] = value.strip()
            elif "subjects" in label or "subject" in label:
                result["subjects"] = value.strip()

        # Extract full judgment text from document-content div
        doc_content_match = re.search(
            r'<div id="document-content"[^>]*>(.*?)</div>\s*</div>\s*</div>\s*</body>',
            html_content, re.DOTALL
        )
        if not doc_content_match:
            # Try alternative pattern
            doc_content_match = re.search(
                r'<div class="documentcontent"[^>]*>(.*?)</div>\s*</div>\s*</div>',
                html_content, re.DOTALL
            )

        if doc_content_match:
            raw_text = doc_content_match.group(1)
            result["text"] = self._extract_text_from_judgment(raw_text)
        else:
            # Fallback: extract all text from body
            body_match = re.search(r'<body[^>]*>(.*?)</body>', html_content, re.DOTALL)
            if body_match:
                result["text"] = self._extract_text_from_judgment(body_match.group(1))
            else:
                result["text"] = ""

        return result

    def _extract_text_from_judgment(self, html_content: str) -> str:
        """Extract clean text from judgment HTML content."""
        # Remove script and style elements
        text = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)

        # Remove hidden elements
        text = re.sub(r'<[^>]+style="[^"]*display:\s*none[^"]*"[^>]*>.*?</[^>]+>', '', text, flags=re.DOTALL | re.IGNORECASE)

        # Convert paragraphs and divs to newlines
        text = re.sub(r'</p>', '\n\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)

        # Remove all remaining HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)

        # Decode HTML entities
        text = html.unescape(text)

        # Clean up whitespace
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n[ \t]+', '\n', text)
        text = re.sub(r'[ \t]+\n', '\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)

        return text.strip()

    def _clean_html(self, html_content: str) -> str:
        """Remove HTML tags and clean text."""
        text = re.sub(r'<br\s*/?>', ' ', html_content, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', '', text)
        text = html.unescape(text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    # -- Public API ---------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all Supreme Court of Canada decisions (full bootstrap)."""
        current_year = datetime.now().year

        logger.info(f"Starting full bootstrap from {START_YEAR} to {current_year}")

        for year in range(current_year, START_YEAR - 1, -1):  # Most recent first
            item_ids = self._get_year_cases(year)

            for item_id in item_ids:
                case = self._get_case_content(item_id)
                if case and case.get("text"):
                    case["year"] = year
                    yield case
                else:
                    logger.warning(f"No text found for case {item_id}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch decisions updated since a given date using RSS feed."""
        logger.info(f"Fetching updates since {since.isoformat()}")

        feed = self._get_rss_feed()
        if feed is None:
            return

        since_date = since.date()

        for item in feed.findall(".//item"):
            # Extract decision date from custom namespace element
            date_elem = item.find("{http://lexum.com/decision/}date")
            if date_elem is not None and date_elem.text:
                try:
                    item_date = datetime.strptime(date_elem.text, "%Y-%m-%d").date()
                    if item_date < since_date:
                        continue
                except ValueError:
                    pass

            # Extract item ID from link
            link = item.findtext("link", "")
            id_match = re.search(r'/item/(\d+)/', link)
            if not id_match:
                continue

            item_id = id_match.group(1)
            case = self._get_case_content(item_id)
            if case and case.get("text"):
                yield case

    def fetch_sample(self) -> Generator[dict, None, None]:
        """Fetch sample records for validation (12+ recent decisions)."""
        logger.info("Fetching sample records...")

        # Get recent cases from RSS feed and current year
        current_year = datetime.now().year
        item_ids = self._get_year_cases(current_year)

        # If not enough in current year, add from previous year
        if len(item_ids) < 15:
            item_ids.extend(self._get_year_cases(current_year - 1))

        count = 0
        target = 12

        for item_id in item_ids[:20]:  # Try first 20 to get 12 good ones
            case = self._get_case_content(item_id)

            if case and case.get("text") and len(case["text"]) > 500:
                yield case
                count += 1

                if count >= target:
                    return

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record to the standard schema."""
        item_id = raw.get("item_id", "")

        # Parse decision date
        date_str = raw.get("decision_date", "")
        if date_str:
            # Clean the date string
            date_str = date_str.strip()
            # Should be in YYYY-MM-DD format already
            if not re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
                # Try to parse other formats
                date_str = ""

        # Build URL
        url = f"{BASE_URL}/scc-csc/scc-csc/en/item/{item_id}/index.do"

        return {
            "_id": f"scc_{item_id}",
            "_source": "CA/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date_str,
            "url": url,
            "item_id": item_id,
            "neutral_citation": raw.get("neutral_citation", ""),
            "case_number": raw.get("case_number", ""),
            "judges": raw.get("judges", ""),
            "appealed_from": raw.get("appealed_from", ""),
            "subjects": raw.get("subjects", ""),
            "year": raw.get("year", ""),
        }

    def test_api(self) -> bool:
        """Test API connectivity."""
        logger.info("Testing Supreme Court of Canada decisions API...")

        try:
            # Test RSS feed
            feed = self._get_rss_feed()
            if feed is None:
                logger.error("Failed to fetch RSS feed")
                return False

            items = feed.findall(".//item")
            logger.info(f"RSS feed: {len(items)} recent items")

            # Test year navigation
            current_year = datetime.now().year
            cases = self._get_year_cases(current_year)
            logger.info(f"Year {current_year}: {len(cases)} cases found")

            # Test fetching one case
            if cases:
                case = self._get_case_content(cases[0])
                if case:
                    text_len = len(case.get("text", ""))
                    logger.info(f"Case fetch successful: {text_len} chars of text")
                    logger.info(f"Title: {case.get('title', 'N/A')[:80]}")
                    return True

            return True
        except Exception as e:
            logger.error(f"API test failed: {e}")
            return False


def main():
    """CLI entry point."""
    scraper = CASupremeCourtScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py <command> [options]")
        print("Commands: bootstrap, bootstrap-fast, update, test-api")
        print("Options: --sample")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    elif command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(f"\nBootstrap complete: {json.dumps(stats, indent=2)}")

    elif command == "bootstrap-fast":
        workers = 3
        batch_size = 100
        for i, arg in enumerate(sys.argv):
            if arg == "--workers" and i + 1 < len(sys.argv):
                workers = int(sys.argv[i + 1])
            if arg == "--batch-size" and i + 1 < len(sys.argv):
                batch_size = int(sys.argv[i + 1])
        stats = scraper.bootstrap_fast(max_workers=workers, batch_size=batch_size)
        print(f"\nBootstrap-fast complete: {json.dumps(stats, indent=2)}")

    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {json.dumps(stats, indent=2)}")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
