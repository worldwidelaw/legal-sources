#!/usr/bin/env python3
"""
TM/Legislation -- Turkmenistan Parliament (Mejlis) Legislation

Fetches laws, codes, and constitutional laws from mejlis.gov.tm,
the official website of the Turkmenistan Parliament.

Strategy:
  1. Paginate through /laws, /codes, /constitutional-law list pages
  2. Extract law IDs from list pages
  3. Fetch individual law pages at /single-law/{id}?lang=tm
  4. Extract full text from HTML content

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap --full     # Full bootstrap
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TM.Legislation")

BASE_URL = "https://mejlis.gov.tm"
USER_AGENT = "LegalDataHunter/1.0 (legal research; open data collection)"

# Legislation categories with their list endpoints
CATEGORIES = {
    "laws": {"list_path": "/laws", "detail_path": "/single-law", "pages": 42},
    "codes": {"list_path": "/codes", "detail_path": "/single-code", "pages": 1},
}


class MejlisScraper(BaseScraper):
    """Scraper for Turkmenistan legislation from mejlis.gov.tm."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "tk,ru;q=0.9,en;q=0.8",
        })

    def _fetch_page(self, url: str, params: dict = None) -> Optional[str]:
        """Fetch a page with retry logic."""
        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params, timeout=30)
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp.text
            except requests.exceptions.RequestException as e:
                if attempt < 2:
                    logger.warning(f"Request error (attempt {attempt+1}): {e}")
                    time.sleep(2 ** (attempt + 1))
                    continue
                logger.error(f"Failed to fetch {url}: {e}")
                return None
        return None

    def _extract_ids_from_list(self, html: str, detail_path: str) -> List[Dict[str, Any]]:
        """Extract law IDs and metadata from a list page."""
        soup = BeautifulSoup(html, "html.parser")
        items = []

        # Look for links matching the detail path pattern
        pattern = re.compile(rf'{detail_path}/(\d+)')
        seen_ids = set()

        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                law_id = int(match.group(1))
                if law_id in seen_ids:
                    continue
                seen_ids.add(law_id)

                # Try to extract date from nearby text
                parent = link.find_parent(["tr", "div", "li"])
                date_text = None
                if parent:
                    date_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', parent.get_text())
                    if date_match:
                        date_text = date_match.group(1)

                items.append({
                    "id": law_id,
                    "date_raw": date_text,
                    "link_text": link.get_text(strip=True)[:200],
                })

        return items

    def _extract_law_text(self, html: str) -> Dict[str, Any]:
        """Extract full text and metadata from an individual law page."""
        soup = BeautifulSoup(html, "html.parser")

        # Extract title from h4 (first one that isn't the site name)
        title = ""
        for h4 in soup.find_all("h4"):
            h4_text = h4.get_text(strip=True)
            if h4_text and "MEJLISI" not in h4_text.upper():
                title = h4_text
                break

        # Find the main content: div.text_content or div.text_block
        content = soup.select_one("div.text_content")
        if not content or len(content.get_text(strip=True)) < 100:
            content = soup.select_one("div.text_block")
        if not content or len(content.get_text(strip=True)) < 100:
            content = soup.select_one("div.right_side")
        if not content or len(content.get_text(strip=True)) < 100:
            # Fallback: largest div
            for div in soup.find_all("div"):
                if len(div.get_text(strip=True)) > 500:
                    content = div
                    break

        if not content:
            return {"title": title, "text": "", "date": None}

        # Remove navigation elements within content
        for tag in content.find_all(["script", "style", "nav"]):
            tag.decompose()

        # Extract full text from content
        full_text = content.get_text(separator="\n", strip=True)

        # Clean up the text
        full_text = re.sub(r'\n{3,}', '\n\n', full_text)
        full_text = re.sub(r'[ \t]+', ' ', full_text)

        # Extract date from content
        date = None
        date_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', full_text[:500])
        if date_match:
            try:
                d = datetime.strptime(date_match.group(1), "%d.%m.%Y")
                date = d.strftime("%Y-%m-%d")
            except ValueError:
                pass

        return {
            "title": title,
            "text": full_text,
            "date": date,
        }

    def _fetch_law(self, law_id: int, category: str, detail_path: str) -> Optional[Dict[str, Any]]:
        """Fetch a single law's full text."""
        url = f"{BASE_URL}{detail_path}/{law_id}"
        params = {"lang": "tm"}

        html = self._fetch_page(url, params)
        if not html:
            logger.warning(f"  Could not fetch {detail_path}/{law_id}")
            return None

        extracted = self._extract_law_text(html)
        if not extracted["text"] or len(extracted["text"]) < 100:
            # Try Russian version as fallback
            params = {"lang": "ru"}
            html = self._fetch_page(url, params)
            if html:
                extracted = self._extract_law_text(html)

        if not extracted["text"] or len(extracted["text"]) < 100:
            logger.warning(f"  Insufficient text for {detail_path}/{law_id}: {len(extracted.get('text', ''))} chars")
            return None

        return {
            "law_id": law_id,
            "category": category,
            "title": extracted["title"],
            "text": extracted["text"],
            "date": extracted["date"],
            "url": f"{BASE_URL}{detail_path}/{law_id}?lang=tm",
        }

    def _get_all_ids(self, category: str, max_pages: int = None) -> List[Dict[str, Any]]:
        """Get all law IDs from list pages for a category."""
        cat_info = CATEGORIES[category]
        list_path = cat_info["list_path"]
        detail_path = cat_info["detail_path"]
        total_pages = max_pages or cat_info["pages"]

        all_items = []
        for page in range(1, total_pages + 1):
            url = f"{BASE_URL}{list_path}/{page}"
            params = {"lang": "tm"}
            logger.info(f"  Listing {category} page {page}/{total_pages}")

            html = self._fetch_page(url, params)
            if not html:
                break

            items = self._extract_ids_from_list(html, detail_path)
            if not items:
                # Try without page number for first page
                if page == 1:
                    url = f"{BASE_URL}{list_path}"
                    html = self._fetch_page(url, params)
                    if html:
                        items = self._extract_ids_from_list(html, detail_path)
                if not items:
                    break

            all_items.extend(items)
            time.sleep(1.0)

        return all_items

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all legislation from mejlis.gov.tm."""
        delay = self.config.get("fetch", {}).get("delay", 2.0)

        for category, cat_info in CATEGORIES.items():
            logger.info(f"Fetching category: {category}")
            items = self._get_all_ids(category)
            logger.info(f"  Found {len(items)} items in {category}")

            detail_path = cat_info["detail_path"]
            for item in items:
                time.sleep(delay)
                raw = self._fetch_law(item["id"], category, detail_path)
                if raw:
                    # Merge date from list if not found in content
                    if not raw["date"] and item.get("date_raw"):
                        try:
                            d = datetime.strptime(item["date_raw"], "%d.%m.%Y")
                            raw["date"] = d.strftime("%Y-%m-%d")
                        except ValueError:
                            pass
                    yield raw

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent legislation."""
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        if not since:
            from datetime import timedelta
            since = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")

        delay = self.config.get("fetch", {}).get("delay", 2.0)
        since_date = datetime.strptime(since, "%Y-%m-%d")

        # Only check first few pages of laws (most recent)
        for category in ["laws", "constitutional-law"]:
            cat_info = CATEGORIES[category]
            items = self._get_all_ids(category, max_pages=3)
            detail_path = cat_info["detail_path"]

            for item in items:
                if item.get("date_raw"):
                    try:
                        d = datetime.strptime(item["date_raw"], "%d.%m.%Y")
                        if d < since_date:
                            continue
                    except ValueError:
                        pass

                time.sleep(delay)
                raw = self._fetch_law(item["id"], category, detail_path)
                if raw:
                    if not raw["date"] and item.get("date_raw"):
                        try:
                            d = datetime.strptime(item["date_raw"], "%d.%m.%Y")
                            raw["date"] = d.strftime("%Y-%m-%d")
                        except ValueError:
                            pass
                    yield raw

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw law record into standard schema."""
        law_id = raw.get("law_id", 0)
        category = raw.get("category", "laws")
        doc_id = f"TM-{category.upper()}-{law_id}"

        return {
            "_id": doc_id,
            "_source": "TM/Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "category": raw.get("category", ""),
            "jurisdiction": "TM",
        }

    def test_connection(self) -> bool:
        """Test connectivity to mejlis.gov.tm."""
        try:
            url = f"{BASE_URL}/laws/1"
            html = self._fetch_page(url, {"lang": "tm"})
            if not html:
                logger.error("Could not reach mejlis.gov.tm/laws")
                return False

            items = self._extract_ids_from_list(html, "/single-law")
            logger.info(f"Connection OK: found {len(items)} law entries on page 1")

            if items:
                # Try to fetch one law
                first = items[0]
                law_data = self._fetch_law(first["id"], "laws", "/single-law")
                if law_data:
                    logger.info(f"  Sample: {law_data['title'][:60]} ({len(law_data['text'])} chars)")
                    return True
                else:
                    logger.warning("  Could not extract text from sample law")
                    return False
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def fetch_sample(self, count: int = 15) -> Generator[Dict[str, Any], None, None]:
        """Fetch sample records across categories."""
        delay = self.config.get("fetch", {}).get("delay", 2.0)
        total = 0
        per_cat = max(3, count // len(CATEGORIES))

        for category, cat_info in CATEGORIES.items():
            if total >= count:
                break

            logger.info(f"Sampling from {category}...")
            items = self._get_all_ids(category, max_pages=2)
            detail_path = cat_info["detail_path"]
            cat_count = 0

            for item in items:
                if total >= count or cat_count >= per_cat:
                    break
                time.sleep(delay)
                raw = self._fetch_law(item["id"], category, detail_path)
                if raw:
                    # Merge date from list
                    if not raw["date"] and item.get("date_raw"):
                        try:
                            d = datetime.strptime(item["date_raw"], "%d.%m.%Y")
                            raw["date"] = d.strftime("%Y-%m-%d")
                        except ValueError:
                            pass
                    total += 1
                    cat_count += 1
                    yield raw

        logger.info(f"Total sampled: {total}")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="TM/Legislation -- Turkmenistan Parliament Legislation"
    )
    subparsers = parser.add_subparsers(dest="command")

    boot_parser = subparsers.add_parser("bootstrap", help="Bootstrap data")
    boot_parser.add_argument("--sample", action="store_true")
    boot_parser.add_argument("--full", action="store_true")
    boot_parser.add_argument("--count", type=int, default=15)

    upd_parser = subparsers.add_parser("update", help="Incremental update")
    upd_parser.add_argument("--since", required=True)

    subparsers.add_parser("test", help="Test connectivity")

    args = parser.parse_args()
    scraper = MejlisScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)

    elif args.command == "bootstrap":
        if args.sample:
            sample_dir = scraper.source_dir / "sample"
            sample_dir.mkdir(parents=True, exist_ok=True)
            records = []
            for raw in scraper.fetch_sample(count=args.count):
                record = scraper.normalize(raw)
                records.append(record)
                fname = f"{record['_id']}.json"
                with open(sample_dir / fname, "w") as f:
                    json.dump(record, f, indent=2, ensure_ascii=False, default=str)
                logger.info(
                    f"[{len(records)}/{args.count}] {record['title'][:50]} — "
                    f"{len(record.get('text', ''))} chars"
                )
            logger.info(f"\n=== SAMPLE SUMMARY ===")
            logger.info(f"Records: {len(records)}")
            if records:
                avg_text = sum(len(r.get("text", "")) for r in records) / len(records)
                logger.info(f"Avg text length: {avg_text:.0f} chars")
                with_text = sum(1 for r in records if len(r.get("text", "")) > 100)
                logger.info(f"With substantial text: {with_text}/{len(records)}")
        elif args.full:
            stats = scraper.bootstrap(sample_mode=False)
            logger.info(f"Bootstrap: {json.dumps(stats, indent=2, default=str)}")

    elif args.command == "update":
        sample_dir = scraper.source_dir / "data"
        sample_dir.mkdir(parents=True, exist_ok=True)
        count = 0
        for raw in scraper.fetch_updates(since=args.since):
            record = scraper.normalize(raw)
            count += 1
            fname = f"{record['_id']}.json"
            with open(sample_dir / fname, "w") as f:
                json.dump(record, f, indent=2, ensure_ascii=False, default=str)
            logger.info(f"[{count}] {record['title'][:50]}")
        logger.info(f"Updated {count} records since {args.since}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
