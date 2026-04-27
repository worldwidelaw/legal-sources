#!/usr/bin/env python3
"""
NO/Skatteetaten-Uttalelser -- Norwegian Tax Directorate Statements & Advance Rulings

Fetches tax doctrine from the Norwegian Tax Administration (Skatteetaten):
  - Bindende forhåndsuttalelser (BFU): binding advance rulings (~951)
  - Uttalelser fra Skattedirektoratet: directorate statements (~24)
  - Prinsipputtalelser: principle statements (~989)
  - Domskommentarer: case commentaries (~96)

Data access: Two-phase crawl:
  1. Listing pages embed complete catalog as inline JSON: var allthedata = [...]
  2. Each document page has full text in <div class="article-wrapper narrow-container">

No API available. Episerver CMS. Norwegian language only.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental (newest first)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NO.Skatteetaten-Uttalelser")

BASE_URL = "https://www.skatteetaten.no"
DELAY = 1.0

# Listing pages with their category names
LISTING_PAGES = {
    "/rettskilder/type/uttalelser/bfu/": "Binding advance ruling (BFU)",
    "/rettskilder/type/uttalelser/uttalelser/": "Directorate statement",
    "/rettskilder/type/uttalelser/prinsipputtalelser/": "Principle statement",
    "/rettskilder/type/uttalelser/domskommentarer/": "Case commentary",
}


def strip_html(raw_html: str) -> str:
    """Strip HTML tags and decode entities to plain text."""
    text = re.sub(r'<(style|script)[^>]*>.*?</\1>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<(br|p|div|h[1-6]|li|tr)[^>]*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class SkatteetatenUttalelser(BaseScraper):
    SOURCE_ID = "NO/Skatteetaten-Uttalelser"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
                "Accept": "text/html,application/xhtml+xml",
            },
        )

    def get_catalog(self, listing_path: str) -> List[Dict[str, Any]]:
        """Fetch a listing page and extract the inline JSON catalog."""
        resp = self.http.get(f"{BASE_URL}{listing_path}")
        time.sleep(DELAY)
        if resp is None or resp.status_code != 200:
            logger.warning("Failed to fetch listing: %s", listing_path)
            return []

        html = resp.text
        match = re.search(r'var\s+allthedata\s*=\s*(\[.*?\]);', html, re.DOTALL)
        if not match:
            logger.warning("No allthedata found in %s", listing_path)
            return []

        try:
            items = json.loads(match.group(1))
            return items
        except json.JSONDecodeError as e:
            logger.warning("Failed to parse allthedata from %s: %s", listing_path, e)
            return []

    def _extract_div_content(self, html: str, class_pattern: str) -> str:
        """Extract full content of a div by class, handling nested divs."""
        marker = html.find(class_pattern)
        if marker < 0:
            return ""
        div_start = html.rfind("<div", 0, marker)
        if div_start < 0:
            return ""
        depth = 0
        i = div_start
        while i < len(html):
            if html[i:i + 4] == "<div":
                depth += 1
            elif html[i:i + 6] == "</div>":
                depth -= 1
                if depth == 0:
                    return html[div_start:i + 6]
            i += 1
        return ""

    def fetch_document_text(self, doc_url: str) -> str:
        """Fetch a document page and extract the full text."""
        full_url = f"{BASE_URL}{doc_url}" if doc_url.startswith("/") else doc_url
        resp = self.http.get(full_url)
        time.sleep(DELAY)
        if resp is None or resp.status_code != 200:
            return ""

        html = resp.text

        # Extract article-wrapper div with nested div tracking
        content = self._extract_div_content(html, 'class="article-wrapper')
        if not content:
            # Fallback: main tag
            match = re.search(r'<main[^>]*>(.*?)</main>', html, re.DOTALL | re.IGNORECASE)
            if match:
                content = match.group(1)

        if not content:
            return ""

        return strip_html(content)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a catalog item into the standard schema."""
        props = raw.get("properties", {})
        product_id = raw.get("productId", "")

        # Date: prefer metadataDate, then startPublish
        date_str = props.get("metadataDate") or props.get("startPublish", "")
        date_iso = None
        if date_str:
            try:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                date_iso = dt.strftime("%Y-%m-%d")
            except Exception:
                date_iso = date_str[:10] if len(date_str) >= 10 else None

        serial = props.get("wholeSerialNumber", "")

        return {
            "_id": str(product_id),
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("_text", ""),
            "date": date_iso,
            "url": f"{BASE_URL}{raw.get('url', '')}",
            "language": "no",
            "category": raw.get("_category", ""),
            "serial_number": serial,
            "year": props.get("appliesForYear", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all doctrine documents."""
        total_yielded = 0
        sample_limit = 15 if sample else None

        for listing_path, category in LISTING_PAGES.items():
            if sample_limit and total_yielded >= sample_limit:
                break

            logger.info("Fetching catalog: %s...", category)
            catalog = self.get_catalog(listing_path)
            logger.info("  Found %d entries", len(catalog))

            # Sort by date descending for freshest first
            catalog.sort(
                key=lambda x: x.get("properties", {}).get("metadataDate") or "",
                reverse=True,
            )

            for item in catalog:
                if sample_limit and total_yielded >= sample_limit:
                    break

                doc_url = item.get("url", "")
                if not doc_url:
                    continue

                text = self.fetch_document_text(doc_url)
                if not text:
                    logger.warning("Empty text for %s: %s", item.get("productId"), item.get("title", "")[:60])
                    continue

                item["_text"] = text
                item["_category"] = category
                yield item
                total_yielded += 1

                if total_yielded % 50 == 0:
                    logger.info("  Progress: %d documents fetched", total_yielded)

            logger.info("  Done with %s. Total so far: %d", category, total_yielded)

        logger.info("Fetch complete. Total documents: %d", total_yielded)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch documents published since a given date."""
        for listing_path, category in LISTING_PAGES.items():
            logger.info("Checking updates for %s since %s...", category, since)
            catalog = self.get_catalog(listing_path)

            for item in catalog:
                props = item.get("properties", {})
                pub_date = props.get("startPublish", "")[:10]
                if pub_date and pub_date >= since:
                    doc_url = item.get("url", "")
                    if not doc_url:
                        continue
                    text = self.fetch_document_text(doc_url)
                    if text:
                        item["_text"] = text
                        item["_category"] = category
                        yield item

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            catalog = self.get_catalog("/rettskilder/type/uttalelser/bfu/")
            logger.info("Test passed: %d BFU entries in catalog", len(catalog))
            return len(catalog) > 0
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


# === CLI entry point ===

def main():
    import argparse

    parser = argparse.ArgumentParser(description="NO/Skatteetaten-Uttalelser bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 10-15 sample records")
    parser.add_argument("--since", type=str, help="Date for incremental update (YYYY-MM-DD)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = SkatteetatenUttalelser()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    if args.command == "bootstrap":
        if args.sample:
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)

            count = 0
            for raw in scraper.fetch_all(sample=True):
                record = scraper.normalize(raw)
                safe_name = re.sub(r'[^\w\-.]', '_', str(record['_id']))
                out_file = sample_dir / f"{safe_name}.json"
                out_file.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
                count += 1
                text_len = len(record.get("text", ""))
                logger.info(
                    "  [%d] %s | %s | text=%d chars",
                    count, record["date"], record["title"][:60], text_len
                )

            logger.info("Bootstrap complete: %d records saved to sample/", count)
            sys.exit(0 if count >= 10 else 1)
        else:
            stats = scraper.bootstrap()
            logger.info("Bootstrap complete: %s", stats)
            sys.exit(0)

    if args.command == "update":
        since = args.since or "2026-01-01"
        count = 0
        for record in scraper.fetch_updates(since):
            count += 1
            logger.info("  [%d] %s: %s", count, record["date"], record["title"][:60])
        logger.info("Update complete: %d new records since %s", count, since)


if __name__ == "__main__":
    main()
