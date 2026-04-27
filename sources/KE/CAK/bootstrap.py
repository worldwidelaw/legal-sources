#!/usr/bin/env python3
"""
KE/CAK -- Competition Authority of Kenya Determinations

Fetches full-text CAK determinations (merger decisions, abuse of dominance,
consumer protection, exemptions) from cak.go.ke.

Strategy:
  - Scrape paginated listing pages (?page=0 through last page)
  - Extract PDF download URLs from each listing page
  - Download each PDF and extract text via common/pdf_extract
  - ~100+ determinations spanning 2014-2026

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.parse import urljoin, unquote

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KE.CAK")

BASE_URL = "https://www.cak.go.ke"
LISTING_URL = f"{BASE_URL}/information-center/CAK-latest-determinations"
DELAY = 2.0


class CAKScraper(BaseScraper):
    """Scraper for KE/CAK -- Competition Authority of Kenya determinations."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    def _get_page(self, url: str) -> str:
        """Fetch a page with rate limiting."""
        time.sleep(DELAY)
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text

    def _extract_determinations(self, html: str) -> List[Dict[str, str]]:
        """Extract determination entries from a listing page.

        Returns list of dicts with keys: title, date, pdf_url
        """
        soup = BeautifulSoup(html, "html.parser")
        results = []

        # Find all links to PDF files under /sites/default/files/determinations/
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "/sites/default/files/determinations/" not in href:
                continue

            pdf_url = urljoin(BASE_URL, href)
            title = link.get_text(strip=True)
            if not title:
                # Try parent element
                parent = link.parent
                if parent:
                    title = parent.get_text(strip=True)

            # Extract date from nearby elements or from the URL path
            date_str = ""
            # URL pattern: /sites/default/files/determinations/YYYY-MM/...
            m = re.search(r'/determinations/(\d{4})-(\d{2})/', href)
            if m:
                date_str = f"{m.group(1)}-{m.group(2)}"

            # Try to find a date near the link
            row = link.find_parent("div") or link.find_parent("tr") or link.find_parent("li")
            if row:
                date_match = re.search(r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{4})', row.get_text())
                if date_match:
                    date_str = date_match.group(1)
                else:
                    date_match2 = re.search(r'(\d{4}-\d{2}-\d{2})', row.get_text())
                    if date_match2:
                        date_str = date_match2.group(1)

            if not title:
                # Derive title from filename
                filename = unquote(href.split("/")[-1])
                title = filename.replace(".pdf", "").replace("%20", " ").replace("_", " ")

            results.append({
                "title": title.strip(),
                "date_raw": date_str.strip(),
                "pdf_url": pdf_url,
            })

        return results

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse various date formats to ISO 8601."""
        date_str = date_str.strip()
        if not date_str:
            return None

        for fmt in (
            "%d %B %Y", "%d %b %Y", "%B %d, %Y",
            "%Y-%m-%d", "%Y-%m", "%d/%m/%Y",
        ):
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        # Handle YYYY-MM format
        if re.match(r'^\d{4}-\d{2}$', date_str):
            return f"{date_str}-01"

        return None

    def _make_id(self, pdf_url: str) -> str:
        """Create a stable ID from the PDF URL."""
        # Extract meaningful part from URL
        filename = unquote(pdf_url.split("/")[-1])
        # Clean up the filename to make a readable ID
        clean = re.sub(r'\.pdf$', '', filename, flags=re.IGNORECASE)
        clean = re.sub(r'[^\w\s-]', '', clean)
        clean = re.sub(r'\s+', '-', clean.strip())
        clean = clean[:120]  # Limit length
        return f"KE-CAK-{clean}"

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all CAK determinations with full text from PDFs."""
        seen_urls = set()
        page = 0
        max_pages = 20  # Safety limit

        while page < max_pages:
            url = f"{LISTING_URL}?page={page}" if page > 0 else LISTING_URL
            logger.info("Fetching listing page %d: %s", page + 1, url)

            try:
                html = self._get_page(url)
            except requests.RequestException as e:
                logger.warning("Failed to fetch page %d: %s", page + 1, e)
                break

            entries = self._extract_determinations(html)
            if not entries:
                logger.info("No determinations on page %d, stopping", page + 1)
                break

            new_count = 0
            for entry in entries:
                if entry["pdf_url"] in seen_urls:
                    continue
                seen_urls.add(entry["pdf_url"])
                new_count += 1

                # Download and extract PDF text
                try:
                    time.sleep(DELAY)
                    resp = self.session.get(entry["pdf_url"], timeout=60)
                    resp.raise_for_status()
                    pdf_bytes = resp.content
                except requests.RequestException as e:
                    logger.warning("Failed to download PDF %s: %s", entry["pdf_url"], e)
                    continue

                text = extract_pdf_markdown(
                    source="KE/CAK",
                    source_id=self._make_id(entry["pdf_url"]),
                    pdf_bytes=pdf_bytes,
                    table="case_law",
                ) or ""

                if not text or len(text) < 100:
                    logger.warning(
                        "Insufficient text from %s (%d chars)",
                        entry["pdf_url"], len(text) if text else 0
                    )
                    continue

                yield {
                    "_pdf_url": entry["pdf_url"],
                    "_title": entry["title"],
                    "_date_raw": entry["date_raw"],
                    "_text": text,
                }

            logger.info("Page %d: %d new determinations (total: %d)", page + 1, new_count, len(seen_urls))

            # Check if there's a next page
            soup = BeautifulSoup(html, "html.parser")
            next_link = soup.find("a", title="Go to next page") or soup.find("li", class_="pager__item--next")
            if not next_link:
                logger.info("No next page link found, stopping")
                break

            page += 1

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent determinations (first 2 pages)."""
        seen_urls = set()
        for page in range(2):
            url = f"{LISTING_URL}?page={page}" if page > 0 else LISTING_URL
            try:
                html = self._get_page(url)
            except requests.RequestException:
                break

            for entry in self._extract_determinations(html):
                if entry["pdf_url"] in seen_urls:
                    continue
                seen_urls.add(entry["pdf_url"])

                try:
                    time.sleep(DELAY)
                    resp = self.session.get(entry["pdf_url"], timeout=60)
                    resp.raise_for_status()
                    pdf_bytes = resp.content
                except requests.RequestException:
                    continue

                text = extract_pdf_markdown(
                    source="KE/CAK",
                    source_id=self._make_id(entry["pdf_url"]),
                    pdf_bytes=pdf_bytes,
                    table="case_law",
                ) or ""

                if text and len(text) >= 100:
                    yield {
                        "_pdf_url": entry["pdf_url"],
                        "_title": entry["title"],
                        "_date_raw": entry["date_raw"],
                        "_text": text,
                    }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw CAK determination into standard schema."""
        _id = self._make_id(raw["_pdf_url"])
        date_iso = self._parse_date(raw.get("_date_raw", ""))

        return {
            "_id": _id,
            "_source": "KE/CAK",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["_title"],
            "text": raw["_text"],
            "date": date_iso,
            "url": raw["_pdf_url"],
            "authority": "Competition Authority of Kenya (CAK)",
            "language": "en",
        }

    def test_connection(self) -> bool:
        """Test that we can access CAK determinations listing."""
        try:
            html = self._get_page(LISTING_URL)
            entries = self._extract_determinations(html)
            logger.info("Connection test: %d determinations on page 1", len(entries))
            return len(entries) > 0
        except Exception as e:
            logger.error("Connection test failed: %s", e)
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="KE/CAK bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CAKScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)

    if args.command == "update":
        count = 0
        for record in scraper.fetch_updates():
            norm = scraper.normalize(record)
            count += 1
            if count % 10 == 0:
                logger.info("Saved %d records", count)
        logger.info("Update complete: %d records", count)
        return

    # bootstrap
    sample_dir = Path(__file__).resolve().parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.sample:
        count = 0
        target = 15
        for raw in scraper.fetch_all():
            record = scraper.normalize(raw)
            if record["text"] and len(record["text"]) > 100:
                fname = re.sub(r'[^\w-]', '_', record["_id"])[:80] + ".json"
                with open(sample_dir / fname, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
                logger.info(
                    "Sample %d/%d: %s (%d chars)",
                    count, target, record["_id"], len(record["text"])
                )
            if count >= target:
                break

        logger.info("Sample complete: %d records saved to %s", count, sample_dir)
        if count == 0:
            sys.exit(1)
    else:
        stats = scraper.bootstrap()
        logger.info("Bootstrap complete: %s", stats)


if __name__ == "__main__":
    main()
