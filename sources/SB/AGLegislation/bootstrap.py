#!/usr/bin/env python3
"""
SB/AGLegislation -- Solomon Islands Attorney General Legislation Portal

Fetches legislation from the AG Chambers' WordPress site via the REST API.
The site uses the Download Manager plugin (dlm_download post type) with
categories for Acts Currently in Force, Constitution, Repealed Acts, etc.

Strategy:
  1. Query WP REST API for downloads in legislation categories
  2. Download each PDF via the download link (serves PDF directly)
  3. Extract full text via common.pdf_extract

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

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SB.AGLegislation")

BASE_URL = "https://attorneygenerals.gov.sb"
API_BASE = f"{BASE_URL}/wp-json/wp/v2"
USER_AGENT = "LegalDataHunter/1.0 (legal research; open data collection)"

# Key legislation categories (category_id: name)
LEGISLATION_CATEGORIES = {
    27: "Acts Currently in Force",
    245: "Constitution",
    30: "Repealed Acts",
    137: "Revised Laws 1996",
    151: "Provincial Ordinances",
    246: "Applicable Legislations",
}


class AGLegislationScraper(BaseScraper):
    """Scraper for Solomon Islands legislation via WordPress REST API."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        })
        self._category_names = {}

    def _load_category_names(self):
        """Cache category id -> name mapping."""
        if self._category_names:
            return
        for cat_id, cat_name in LEGISLATION_CATEGORIES.items():
            self._category_names[cat_id] = cat_name

    def _fetch_downloads(self, category_id: int, page: int = 1,
                         per_page: int = 20) -> List[Dict]:
        url = f"{API_BASE}/dlm_download"
        params = {
            "dlm_download_category": category_id,
            "per_page": per_page,
            "page": page,
            "_fields": "id,date,title,link,slug",
        }
        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params, timeout=30)
                if resp.status_code == 400:
                    return []
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.RequestException as e:
                if attempt < 2:
                    logger.warning(f"API error (attempt {attempt+1}): {e}")
                    time.sleep(2 ** (attempt + 1))
                    continue
                raise
        return []

    def _get_total_pages(self, category_id: int, per_page: int = 20) -> int:
        url = f"{API_BASE}/dlm_download"
        params = {
            "dlm_download_category": category_id,
            "per_page": per_page,
        }
        try:
            resp = self.session.head(url, params=params, timeout=30)
            return int(resp.headers.get("X-WP-TotalPages", 1))
        except Exception:
            return 1

    def _download_pdf(self, download_link: str) -> Optional[bytes]:
        """Download the PDF from the download link (serves PDF directly)."""
        clean_url = download_link.split("?")[0]
        if not clean_url.endswith("/"):
            clean_url += "/"
        for attempt in range(3):
            try:
                resp = self.session.get(
                    clean_url, timeout=60, allow_redirects=True,
                    headers={"Accept": "application/pdf,*/*"},
                )
                resp.raise_for_status()
                if len(resp.content) > 500 and resp.content[:5] == b"%PDF-":
                    return resp.content
                logger.warning(
                    f"Not a PDF ({len(resp.content)} bytes): {clean_url}"
                )
                return None
            except Exception as e:
                if attempt < 2:
                    logger.warning(f"Download error: {e}")
                    time.sleep(2)
                    continue
                logger.error(f"Failed to download {clean_url}: {e}")
                return None
        return None

    def _process_download(self, item: Dict, category_id: int) -> Optional[Dict]:
        """Process a single download entry: fetch PDF + extract text."""
        title = item.get("title", {}).get("rendered", "")
        link = item.get("link", "")
        wp_id = item.get("id", 0)
        date_str = item.get("date", "")

        if not title or not link:
            return None

        logger.info(f"  Downloading: {title[:60]}...")
        pdf_data = self._download_pdf(link)
        if not pdf_data:
            logger.warning(f"  No PDF data for: {title[:60]}")
            return None

        text = extract_pdf_markdown(
            "SB/AGLegislation", str(wp_id), pdf_bytes=pdf_data,
        )
        if not text or len(text) < 100:
            logger.warning(f"  Insufficient text ({len(text or '')} chars): {title[:60]}")
            return None

        self._load_category_names()
        category_name = self._category_names.get(category_id, "Unknown")

        return {
            "wp_id": wp_id,
            "title": title,
            "slug": item.get("slug", ""),
            "date": date_str[:10] if date_str else None,
            "link": link,
            "category_id": category_id,
            "category": category_name,
            "text": text,
            "pdf_size": len(pdf_data),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        delay = self.config.get("fetch", {}).get("delay", 2.0)
        for cat_id, cat_name in LEGISLATION_CATEGORIES.items():
            logger.info(f"Fetching category: {cat_name} (id={cat_id})")
            total_pages = self._get_total_pages(cat_id)
            for page in range(1, total_pages + 1):
                items = self._fetch_downloads(cat_id, page=page)
                if not items:
                    break
                for item in items:
                    time.sleep(delay)
                    raw = self._process_download(item, cat_id)
                    if raw:
                        yield raw

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        if not since:
            from datetime import timedelta
            since = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
        delay = self.config.get("fetch", {}).get("delay", 2.0)
        for cat_id, cat_name in LEGISLATION_CATEGORIES.items():
            logger.info(f"Updates for {cat_name} since {since}")
            page = 1
            while True:
                items = self._fetch_downloads(cat_id, page=page)
                if not items:
                    break
                for item in items:
                    item_date = item.get("date", "")[:10]
                    if item_date and item_date < since:
                        continue
                    time.sleep(delay)
                    raw = self._process_download(item, cat_id)
                    if raw:
                        yield raw
                page += 1

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        wp_id = raw.get("wp_id", 0)
        doc_id = f"SB-LEG-{wp_id}"
        return {
            "_id": doc_id,
            "_source": "SB/AGLegislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("link", ""),
            "category": raw.get("category", ""),
            "jurisdiction": "SB",
        }

    def test_connection(self) -> bool:
        try:
            url = f"{API_BASE}/dlm_download"
            resp = self.session.get(
                url, params={"dlm_download_category": 27, "per_page": 1},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            total = resp.headers.get("X-WP-Total", "?")
            logger.info(f"Connection OK: {total} Acts Currently in Force")
            if data:
                logger.info(f"  First item: {data[0]['title']['rendered'][:60]}")
            return len(data) > 0
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def fetch_sample(self, count: int = 15) -> Generator[Dict[str, Any], None, None]:
        """Fetch samples spread across categories."""
        delay = self.config.get("fetch", {}).get("delay", 2.0)
        total = 0
        cats = [27, 245, 30, 137, 151]
        per_cat = max(2, count // len(cats))

        for cat_id in cats:
            if total >= count:
                break
            cat_name = LEGISLATION_CATEGORIES.get(cat_id, str(cat_id))
            logger.info(f"Sampling from {cat_name}...")
            items = self._fetch_downloads(cat_id, page=1, per_page=per_cat + 2)
            cat_count = 0
            for item in items:
                if total >= count or cat_count >= per_cat:
                    break
                time.sleep(delay)
                raw = self._process_download(item, cat_id)
                if raw:
                    total += 1
                    cat_count += 1
                    yield raw
        logger.info(f"Total sampled: {total}")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="SB/AGLegislation -- Solomon Islands legislation"
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
    scraper = AGLegislationScraper()

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
                cats = set(r.get("category", "") for r in records)
                logger.info(f"Categories: {', '.join(sorted(cats))}")
        elif args.full:
            stats = scraper.bootstrap(sample_mode=False)
            logger.info(f"Bootstrap: {json.dumps(stats, indent=2, default=str)}")
        else:
            stats = scraper.bootstrap(sample_mode=True, sample_size=args.count)
            logger.info(f"Bootstrap: {json.dumps(stats, indent=2, default=str)}")

    elif args.command == "update":
        count = 0
        data_dir = scraper.source_dir / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        with open(data_dir / "updates.jsonl", "w") as f:
            for raw in scraper.fetch_updates(since=args.since):
                record = scraper.normalize(raw)
                f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
                count += 1
        logger.info(f"Fetched {count} updates since {args.since}")

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
