#!/usr/bin/env python3
"""
MN/LegalInfo -- Mongolia Unified Legal Information System Fetcher

Fetches Mongolian legislation from legalinfo.mn via internal AJAX endpoints.

Strategy:
  - List laws via POST /mn/ajaxList/ with category IDs
  - Extract lawId values from returned HTML
  - Fetch full text via GET /mn/detail?lawId=ID
  - Parse HTML to extract clean text from div.law_content

Categories:
  26: Constitution, 27: Laws, 28: Parliament Resolutions,
  29: International Treaties, 30: Presidential Decrees,
  31: Constitutional Court Decisions, 32: Supreme Court Resolutions,
  33: Government Resolutions, 34: Minister Orders, 35: Agency Director Orders

Data:
  - 13K+ active legal documents
  - Coverage: Constitution through ministerial orders
  - Language: Mongolian (some English translations available)
  - License: Public (open government data)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import math
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MN.legalinfo")

BASE_URL = "https://legalinfo.mn"

# Category IDs and their types
CATEGORIES = {
    26: {"name": "Constitution", "type": "legislation"},
    27: {"name": "Laws", "type": "legislation"},
    28: {"name": "Parliament Resolutions", "type": "legislation"},
    29: {"name": "International Treaties", "type": "legislation"},
    30: {"name": "Presidential Decrees", "type": "legislation"},
    31: {"name": "Constitutional Court Decisions", "type": "case_law"},
    32: {"name": "Supreme Court Resolutions", "type": "case_law"},
    33: {"name": "Government Resolutions", "type": "legislation"},
    34: {"name": "Minister Orders", "type": "legislation"},
    35: {"name": "Agency Director Orders", "type": "legislation"},
}

# For sample mode, use a subset
SAMPLE_CATEGORIES = [27, 31, 33]


class LegalInfoScraper(BaseScraper):
    """
    Scraper for MN/LegalInfo -- Mongolia Unified Legal Information System.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "mn,en;q=0.5",
            },
            timeout=60,
        )

    def _list_laws_page(self, category_id: int, page: int) -> List[Dict[str, str]]:
        """
        Fetch one page of law listings for a category.

        Returns list of dicts with 'law_id' and 'title'.
        """
        self.rate_limiter.wait()
        try:
            resp = self.client.post(
                "/mn/ajaxList/",
                data={
                    "filtercategorytypeid": str(category_id),
                    "page": str(page),
                    "sort": "title",
                    "sortType": "asc",
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            if resp.status_code != 200:
                logger.warning(f"List page {page} for cat {category_id}: HTTP {resp.status_code}")
                return []

            data = resp.json()
            html_content = data.get("Html", "")

            if not html_content or html_content.strip() == "":
                return []

            # Extract law IDs and titles from the HTML
            results = []
            # Pattern: <a href="...lawId=NNNN...">TITLE</a>
            for m in re.finditer(
                r'lawId=(\d+)[^"]*"[^>]*>\s*([^<]+)',
                html_content
            ):
                law_id = m.group(1)
                title = html.unescape(m.group(2).strip())
                if law_id and title:
                    results.append({"law_id": law_id, "title": title})

            return results

        except Exception as e:
            logger.warning(f"Error listing page {page} for cat {category_id}: {e}")
            return []

    def _list_all_laws(self, category_id: int) -> List[Dict[str, str]]:
        """List all law IDs for a category by paginating."""
        all_laws = []
        page = 1
        seen_ids = set()

        while True:
            laws = self._list_laws_page(category_id, page)
            if not laws:
                break

            new_count = 0
            for law in laws:
                if law["law_id"] not in seen_ids:
                    seen_ids.add(law["law_id"])
                    all_laws.append(law)
                    new_count += 1

            if new_count == 0:
                break

            logger.info(f"Category {category_id} page {page}: {new_count} new laws (total: {len(all_laws)})")
            page += 1

        return all_laws

    def _fetch_detail(self, law_id: str) -> Optional[str]:
        """Fetch the full HTML detail page for a law."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(f"/mn/detail?lawId={law_id}")

            if resp.status_code != 200:
                logger.warning(f"Detail for law {law_id}: HTTP {resp.status_code}")
                return None

            return resp.content.decode("utf-8", errors="replace")

        except Exception as e:
            logger.warning(f"Error fetching detail for law {law_id}: {e}")
            return None

    def _extract_text(self, html_content: str) -> str:
        """
        Extract clean text from a law detail page.

        The actual law text lives in <div> elements whose class contains
        'responsive_mobile'. These are nested inside a 'law_content' container.
        """
        # Primary: collect all responsive_mobile blocks (class may contain other classes)
        blocks = re.findall(
            r'<div[^>]*responsive_mobile[^>]*>(.*?)</div>',
            html_content,
            re.DOTALL,
        )

        if not blocks:
            # Fallback: grab everything inside law_content
            match = re.search(
                r'<div[^>]*law_content[^>]*>(.*?)(?:<div[^>]*nom-more-content|<footer|</body)',
                html_content,
                re.DOTALL | re.IGNORECASE,
            )
            if match:
                return self._strip_html(match.group(1))
            return ""

        return self._strip_html("\n".join(blocks))

    def _strip_html(self, text: str) -> str:
        """Remove HTML tags and clean text."""
        # Remove HTML comments
        text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)
        # Remove style/script tags
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
        # Block-level tags to newlines
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</(?:p|div|li|tr|h[1-6]|blockquote|article|section)>', '\n', text, flags=re.IGNORECASE)
        # Remove all remaining tags
        text = re.sub(r'<[^>]+>', '', text)
        # Decode entities
        text = html.unescape(text)
        # Clean whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)
        lines = [line.strip() for line in text.split('\n')]
        text = '\n'.join(lines)
        return text.strip()

    def _extract_metadata(self, html_content: str, law_id: str, full_text: str = "") -> Dict[str, Any]:
        """Extract metadata from the detail page and extracted text."""
        metadata = {"law_id": law_id}

        # Title from <title> tag
        title_match = re.search(r'<title>([^<]+)</title>', html_content)
        if title_match:
            metadata["page_title"] = html.unescape(title_match.group(1).strip())

        # Extract date from the full text (first 1000 chars)
        # Mongolian date format: YYYY оны MM дугаар сарын DD-ны өдөр
        search_text = full_text[:1000] if full_text else ""
        date_match = re.search(
            r'(\d{4})\s*оны\s*(\d{1,2})\s*(?:дугаар\s+)?(?:дүгээр\s+)?сарын\s*(\d{1,2})',
            search_text,
        )
        if date_match:
            try:
                y, m, d = int(date_match.group(1)), int(date_match.group(2)), int(date_match.group(3))
                if 1900 <= y <= 2100 and 1 <= m <= 12 and 1 <= d <= 31:
                    metadata["date"] = f"{y:04d}-{m:02d}-{d:02d}"
            except (ValueError, IndexError):
                pass

        # Extract act/law number if present in text
        num_match = re.search(r'(?:дугаар|№)\s*[:\s]*(\d+)', search_text)
        if num_match:
            metadata["act_number"] = num_match.group(1)

        return metadata

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legal documents from all categories."""
        for cat_id, cat_info in CATEGORIES.items():
            logger.info(f"Listing category {cat_id}: {cat_info['name']}...")
            laws = self._list_all_laws(cat_id)
            logger.info(f"Category {cat_id}: found {len(laws)} laws")

            for i, law_entry in enumerate(laws):
                law_id = law_entry["law_id"]
                list_title = law_entry["title"]

                detail_html = self._fetch_detail(law_id)
                if not detail_html:
                    continue

                full_text = self._extract_text(detail_html)
                if not full_text or len(full_text) < 50:
                    logger.warning(f"No text for law {law_id} ({list_title[:50]})")
                    continue

                metadata = self._extract_metadata(detail_html, law_id, full_text)

                yield {
                    "law_id": law_id,
                    "category_id": cat_id,
                    "category_name": cat_info["name"],
                    "data_type": cat_info["type"],
                    "title": list_title,
                    "full_text": full_text,
                    "metadata": metadata,
                }

                if (i + 1) % 50 == 0:
                    logger.info(f"Category {cat_id}: processed {i + 1}/{len(laws)}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent documents. Re-fetches all since site has no date filter."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw data into standard schema."""
        law_id = raw.get("law_id", "")
        cat_id = raw.get("category_id", 0)
        metadata = raw.get("metadata", {})

        doc_id = f"MN-LI-{law_id}"
        title = raw.get("title", metadata.get("page_title", f"Law {law_id}"))
        date_str = metadata.get("date", None)
        data_type = raw.get("data_type", "legislation")

        return {
            "_id": doc_id,
            "_source": "MN/LegalInfo",
            "_type": data_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("full_text", ""),
            "date": date_str,
            "url": f"{BASE_URL}/mn/detail?lawId={law_id}",
            "category": CATEGORIES.get(cat_id, {}).get("name", ""),
            "category_id": cat_id,
            "act_number": metadata.get("act_number", ""),
            "jurisdiction": "MN",
            "language": "mn",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Mongolia Legal Info System endpoints...")

        # Test 1: List laws in category 27 (Laws)
        print("\n1. Testing law listing (category 27: Laws)...")
        laws = self._list_laws_page(27, 1)
        if laws:
            print(f"   Found {len(laws)} laws on page 1")
            for law in laws[:3]:
                print(f"   - {law['law_id']}: {law['title'][:60]}")
        else:
            print("   FAILED: No laws returned")
            return

        # Test 2: Fetch a detail page
        if laws:
            test_id = laws[0]["law_id"]
            print(f"\n2. Fetching detail for law {test_id}...")
            detail = self._fetch_detail(test_id)
            if detail:
                print(f"   HTML length: {len(detail)} chars")
                text = self._extract_text(detail)
                print(f"   Text length: {len(text)} chars")
                if text:
                    print(f"   Sample: {text[:300]}...")
                else:
                    print("   WARNING: No text extracted!")
            else:
                print("   FAILED: Could not fetch detail page")

        # Test 3: Constitutional Court (category 31)
        print("\n3. Testing Constitutional Court (category 31)...")
        cc_laws = self._list_laws_page(31, 1)
        if cc_laws:
            print(f"   Found {len(cc_laws)} decisions on page 1")
            test_id = cc_laws[0]["law_id"]
            detail = self._fetch_detail(test_id)
            if detail:
                text = self._extract_text(detail)
                print(f"   Decision text: {len(text)} chars")
        else:
            print("   No Constitutional Court decisions found")

        print("\nTest complete!")


def main():
    scraper = LegalInfoScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, {stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
