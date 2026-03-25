#!/usr/bin/env python3
"""
BH/LLOCLegislation -- Bahrain LLOC Legislation

Fetches legislation from the Bahrain Legislation and Legal Opinion
Commission (LLOC) portal.

Strategy:
  - POST search to /legislation/search with anti-forgery token
  - Parse HTML response to extract document IDs, titles, dates
  - Fetch full text via /Legislation/HTM/{id}
  - Strip HTML tags from full text content

Data: ~23,000+ records (laws, decrees, royal orders, regulations)
License: Open access (government legislation portal)
Rate limit: 0.5 req/sec (server is slow, be generous).

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List
from html import unescape

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BH.LLOCLegislation")

BASE_URL = "https://www.lloc.gov.bh"

# Arabic month names to numbers
ARABIC_MONTHS = {
    'يناير': '01', 'فبراير': '02', 'مارس': '03', 'أبريل': '04',
    'ابريل': '04', 'مايو': '05', 'يونيو': '06', 'يوليو': '07',
    'أغسطس': '08', 'اغسطس': '08', 'سبتمبر': '09', 'أكتوبر': '10',
    'اكتوبر': '10', 'نوفمبر': '11', 'ديسمبر': '12',
}


def strip_html(html_text: str) -> str:
    """Strip HTML tags and clean up text."""
    if not html_text:
        return ""
    # Remove style blocks
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.S | re.I)
    # Remove script blocks
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.S | re.I)
    # Replace br with newlines
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.I)
    # Replace p/div with double newlines
    text = re.sub(r'</?(?:p|div)[^>]*>', '\n\n', text, flags=re.I)
    # Remove remaining tags
    text = re.sub(r'<[^>]+>', '', text)
    # Decode entities
    text = unescape(text)
    text = text.replace('&nbsp;', ' ')
    # Normalize whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' +\n', '\n', text)
    return text.strip()


def parse_arabic_date(date_str: str) -> Optional[str]:
    """Parse Arabic date like '5-مارس-2026' to ISO format."""
    if not date_str:
        return None
    date_str = date_str.strip()
    m = re.match(r'(\d{1,2})-([^-]+)-(\d{4})', date_str)
    if not m:
        return None
    day = m.group(1).zfill(2)
    month_name = m.group(2).strip()
    year = m.group(3)
    month = ARABIC_MONTHS.get(month_name)
    if not month:
        return None
    return f"{year}-{month}-{day}"


class BHLLOCLegislationScraper(BaseScraper):
    """
    Scraper for BH/LLOCLegislation -- Bahrain LLOC.
    Country: BH
    URL: https://www.lloc.gov.bh

    Data types: legislation
    Auth: none (anti-forgery token auto-acquired)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html, */*; q=0.01",
        })
        self._token = None

    def _get_with_retry(self, url: str, max_retries: int = 3) -> Optional[requests.Response]:
        """GET with retry logic for flaky server."""
        for attempt in range(max_retries):
            try:
                resp = self.session.get(url, timeout=90)
                if resp.status_code == 200:
                    return resp
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt+1}/{max_retries} failed for {url}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5 * (attempt + 1))
        return None

    def _init_token(self):
        """Get anti-forgery token from search page."""
        resp = self._get_with_retry(f"{BASE_URL}/Legislation/Search")
        if not resp:
            raise RuntimeError("Failed to load search page")
        match = re.search(r'id="token"[^>]*value="([^"]+)"', resp.text)
        if not match:
            raise RuntimeError("Anti-forgery token not found")
        self._token = match.group(1)
        logger.info("Anti-forgery token acquired")

    def _search_page(self, page_num: int, page_size: int = 100) -> Optional[str]:
        """Execute a search and return result HTML."""
        if not self._token:
            self._init_token()

        legsearch = json.dumps({
            "IsSearchTitle": True, "LegNum": None, "YearFrom": None,
            "YearTo": None, "TypeID": "any", "SourceID": "any",
            "CategoryID": "any", "KeywordAll": None, "KeywordAny": None,
            "KeywordPhrase": None, "KeywordNot": None,
            "IsSearchTreaty": False, "IsSearchWomen": False,
            "IsSearchEnglish": False, "SortBy": "date"
        })
        body = '{PostParam: ' + legsearch + ', PageNum: ' + str(page_num) + ', PageSize: ' + str(page_size) + '}'

        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "X-Requested-With": "XMLHttpRequest",
            "RequestVerificationToken": self._token,
            "Origin": BASE_URL,
            "Referer": f"{BASE_URL}/Legislation/Search",
        }

        for attempt in range(3):
            try:
                resp = self.session.post(
                    f"{BASE_URL}/legislation/search",
                    headers=headers,
                    data=body.encode("utf-8"),
                    timeout=90,
                )
                if resp.status_code == 200 and len(resp.text) > 500:
                    return resp.text
                if resp.status_code == 499:
                    logger.info("No more results")
                    return None
                logger.warning(f"Search page {page_num} HTTP {resp.status_code}")
            except requests.RequestException as e:
                logger.warning(f"Search attempt {attempt+1} failed: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
                    # Re-acquire token
                    self._token = None
                    self._init_token()

        return None

    def _parse_search_results(self, html: str) -> List[dict]:
        """Parse search result HTML to extract document metadata."""
        results = []

        # Split HTML by PDF links to get one block per document
        parts = re.split(r'href="/PDF/', html)

        for part in parts[1:]:
            doc_id_match = re.match(r'([^"]+)\.pdf"', part)
            if not doc_id_match:
                continue
            doc_id = doc_id_match.group(1)

            # Get date from hvalue spans
            date_match = re.search(
                r'التاريخ\s*:\s*</span>\s*<span[^>]*class="hvalue"[^>]*>([^<]+)',
                part
            )
            date_str = date_match.group(1).strip() if date_match else ""

            # Get gazette number
            gazette_match = re.search(
                r'الجريدة الرسمية\s*:\s*</span>\s*<span[^>]*class="hvalue"[^>]*>([^<]+)',
                part
            )
            gazette = gazette_match.group(1).strip() if gazette_match else ""

            results.append({
                "doc_id": doc_id,
                "title": "",  # Will be extracted from full text
                "date_raw": date_str,
                "gazette_number": gazette,
            })

        return results

    def _fetch_full_text(self, doc_id: str) -> Optional[str]:
        """Fetch full text of a document via HTM endpoint."""
        url = f"{BASE_URL}/Legislation/HTM/{doc_id}"
        resp = self._get_with_retry(url)
        if not resp:
            return None
        text = strip_html(resp.text)
        return text if len(text) > 50 else None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legislation with full text."""
        self._init_token()
        total = 0
        page = 1
        page_size = 100
        consecutive_empty = 0

        while True:
            time.sleep(2)
            html = self._search_page(page, page_size)
            if not html:
                consecutive_empty += 1
                if consecutive_empty >= 2:
                    break
                page += 1
                continue

            consecutive_empty = 0
            items = self._parse_search_results(html)
            if not items:
                break

            for item in items:
                time.sleep(2)
                text = self._fetch_full_text(item["doc_id"])
                if text:
                    item["text"] = text
                    total += 1
                    yield item

                    if total % 50 == 0:
                        logger.info(f"Progress: {total} documents (page {page})")

            page += 1

        logger.info(f"Scan complete: {total} documents")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent legislation (sorted by date desc)."""
        since_str = since.strftime("%Y-%m-%d")
        logger.info(f"Fetching updates since {since_str}")
        self._init_token()

        for page in range(1, 20):
            time.sleep(2)
            html = self._search_page(page, 100)
            if not html:
                break

            items = self._parse_search_results(html)
            if not items:
                break

            for item in items:
                date_iso = parse_arabic_date(item.get("date_raw", ""))
                if date_iso and date_iso < since_str:
                    return

                time.sleep(2)
                text = self._fetch_full_text(item["doc_id"])
                if text:
                    item["text"] = text
                    yield item

    def fetch_sample(self, count: int = 15) -> Generator[dict, None, None]:
        """Fetch sample legislation documents."""
        self._init_token()
        found = 0

        html = self._search_page(1, count + 5)
        if not html:
            logger.error("Failed to fetch search results")
            return

        items = self._parse_search_results(html)
        logger.info(f"Search returned {len(items)} items")

        for item in items:
            if found >= count:
                break

            time.sleep(2)
            text = self._fetch_full_text(item["doc_id"])
            if not text:
                logger.debug(f"No text for {item['doc_id']}")
                continue

            item["text"] = text
            found += 1
            logger.info(
                f"Sample {found}/{count}: {item['doc_id']} "
                f"({len(text)} chars) {item.get('title', '')[:60]}"
            )
            yield item

        logger.info(f"Sample complete: {found} records")

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw legislation record to standard schema."""
        doc_id = raw["doc_id"]
        text = raw["text"]

        # Extract title from first non-empty line of the full text
        title = raw.get("title", "")
        if not title:
            lines = [l.strip() for l in text.split('\n') if l.strip()]
            title = lines[0] if lines else doc_id
        title = re.sub(r'\s+', ' ', title).strip()[:500]

        date_iso = parse_arabic_date(raw.get("date_raw", ""))

        return {
            "_id": f"BH-LLOC-{doc_id}",
            "_source": "BH/LLOCLegislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": date_iso,
            "url": f"{BASE_URL}/Legislation/HTM/{doc_id}",
            "doc_id": doc_id,
            "gazette_number": raw.get("gazette_number", ""),
        }

    def test_api(self) -> bool:
        """Test API connectivity."""
        logger.info("Testing Bahrain LLOC portal...")

        # Test search page
        try:
            self._init_token()
        except RuntimeError as e:
            logger.error(f"Token acquisition failed: {e}")
            return False
        logger.info("Token acquisition OK")

        # Test search
        html = self._search_page(1, 5)
        if not html:
            logger.error("Search failed")
            return False
        items = self._parse_search_results(html)
        logger.info(f"Search OK: {len(items)} items")

        if not items:
            logger.error("No items parsed from search results")
            return False

        # Test full text
        time.sleep(2)
        text = self._fetch_full_text(items[0]["doc_id"])
        if not text:
            logger.error(f"Full text fetch failed for {items[0]['doc_id']}")
            return False
        logger.info(f"Full text OK: {len(text)} chars for {items[0]['doc_id']}")

        logger.info("All tests passed")
        return True


# -- CLI entry point ---------------------------------------------------------

if __name__ == "__main__":
    scraper = BHLLOCLegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample] [--count N]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        count = 15
        for i, arg in enumerate(sys.argv):
            if arg == "--count" and i + 1 < len(sys.argv):
                count = int(sys.argv[i + 1])

        if sample_mode:
            gen = scraper.fetch_sample(count=count)
        else:
            gen = scraper.fetch_all()

        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in gen:
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1
            logger.info(f"Saved: {out_path.name}")

        logger.info(f"Bootstrap complete: {saved} records saved to {sample_dir}")

    elif command == "update":
        since_str = None
        for i, arg in enumerate(sys.argv):
            if arg == "--since" and i + 1 < len(sys.argv):
                since_str = sys.argv[i + 1]

        if not since_str:
            print("Usage: python bootstrap.py update --since YYYY-MM-DD")
            sys.exit(1)

        since = datetime.strptime(since_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in scraper.fetch_updates(since):
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1

        logger.info(f"Update complete: {saved} records saved")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
