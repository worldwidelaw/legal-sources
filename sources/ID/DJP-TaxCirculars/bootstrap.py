#!/usr/bin/env python3
"""
ID/DJP-TaxCirculars -- Indonesia Directorate General of Taxes Regulations & Circulars

Fetches tax regulations, circulars, and ministerial decrees from pajak.go.id
with full text extracted from HTML detail pages.

Strategy:
  - Paginate through /id/peraturan?page=N (5 items per page, ~1,252 pages)
  - Extract regulation links, titles, and document numbers from listing
  - Fetch each detail page and extract full text from <article> tag
  - Extract metadata: date, categories, status from field divs

Data:
  - ~6,260 regulations (income tax, VAT, stamp duty, admin sanctions, etc.)
  - Full text available inline as HTML (no PDFs needed)
  - No authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent regulations
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

import requests

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: BeautifulSoup4 is required. Install with: pip install beautifulsoup4")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ID.DJP-TaxCirculars")

BASE_URL = "https://www.pajak.go.id"
LISTING_URL = f"{BASE_URL}/id/peraturan"
ITEMS_PER_PAGE = 5
MAX_PAGES = 1252  # ~6,260 regulations


class DJPTaxCircularsScraper(BaseScraper):
    """
    Scraper for ID/DJP-TaxCirculars -- Indonesia tax regulations from pajak.go.id.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; https://github.com/worldwidelaw/legal-sources)",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "id,en;q=0.5",
        })

    def _get_listing_page(self, page: int) -> List[Dict[str, str]]:
        """Fetch a listing page and extract regulation entries."""
        url = f"{LISTING_URL}?page={page}"
        resp = self.session.get(url, timeout=60)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        entries = []

        for row in soup.find_all("div", class_="views-row"):
            entry = {}

            # Document number and link
            nomor_div = row.find("div", class_="views-field-field-nomor-dokumen")
            if nomor_div:
                link_tag = nomor_div.find("a", href=True)
                if link_tag:
                    entry["href"] = link_tag["href"]
                    entry["doc_number"] = link_tag.get_text(strip=True)

            # Title
            title_div = row.find("div", class_="views-field-title")
            if title_div:
                entry["title"] = title_div.get_text(strip=True)

            if entry.get("href"):
                entries.append(entry)

        return entries

    def _get_detail_page(self, path: str) -> Optional[Dict[str, Any]]:
        """Fetch a detail page and extract full text + metadata."""
        url = urljoin(BASE_URL, path)
        resp = self.session.get(url, timeout=60)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        detail = {"url": url}

        # Title from <h1>
        h1 = soup.find("h1")
        if h1:
            detail["title"] = h1.get_text(strip=True)

        # Date from field
        date_div = soup.find("div", class_="field--name-field-tanggal-peraturan")
        if date_div:
            time_tag = date_div.find("time", attrs={"datetime": True})
            if time_tag:
                detail["date"] = time_tag["datetime"]
            else:
                detail["date_raw"] = date_div.get_text(strip=True)

        # Categories
        cat_div = soup.find("div", class_="field--name-field-kategori-peraturan")
        if cat_div:
            items = cat_div.find_all("div", class_="field__item")
            detail["categories"] = [item.get_text(strip=True) for item in items]

        # Status
        status_div = soup.find("div", class_="field--name-field-status-peraturan")
        if status_div:
            item = status_div.find("div", class_="field__item")
            if item:
                detail["status"] = item.get_text(strip=True)

        # Document type
        jenis_div = soup.find("div", class_="field--name-field-jenis-dokumen")
        if jenis_div:
            item = jenis_div.find("div", class_="field__item")
            if item:
                detail["doc_type"] = item.get_text(strip=True)

        # Full text from <article>
        article = soup.find("article")
        if article:
            for tag in article.find_all(["script", "style", "nav"]):
                tag.decompose()
            text = article.get_text(separator="\n", strip=True)
            # Clean up excessive whitespace
            text = re.sub(r"\n{3,}", "\n\n", text)
            detail["text"] = text

        return detail if detail.get("text") else None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all regulations from pajak.go.id listing pages."""
        for page in range(MAX_PAGES):
            try:
                entries = self._get_listing_page(page)
            except Exception as e:
                logger.warning(f"Error fetching listing page {page}: {e}")
                if page > 5:
                    continue
                else:
                    raise

            if not entries:
                logger.info(f"No entries on page {page}, stopping pagination")
                break

            for entry in entries:
                time.sleep(1.0)  # Rate limit
                try:
                    detail = self._get_detail_page(entry["href"])
                    if detail:
                        # Merge listing metadata with detail
                        detail["doc_number"] = entry.get("doc_number", "")
                        if not detail.get("title"):
                            detail["title"] = entry.get("title", "")
                        detail["listing_title"] = entry.get("title", "")
                        yield detail
                    else:
                        logger.warning(f"No text found for {entry['href']}")
                except Exception as e:
                    logger.warning(f"Error fetching detail {entry['href']}: {e}")
                    continue

            if page % 50 == 0:
                logger.info(f"Processed page {page}/{MAX_PAGES}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch regulations published after the given date.
        Scans listing pages (newest first) until we hit older entries."""
        for page in range(MAX_PAGES):
            entries = self._get_listing_page(page)
            if not entries:
                break

            all_old = True
            for entry in entries:
                time.sleep(1.0)
                try:
                    detail = self._get_detail_page(entry["href"])
                    if detail:
                        detail["doc_number"] = entry.get("doc_number", "")
                        if not detail.get("title"):
                            detail["title"] = entry.get("title", "")

                        # Check date
                        doc_date = detail.get("date", "")
                        if doc_date:
                            try:
                                dt = datetime.fromisoformat(doc_date.replace("Z", "+00:00"))
                                if dt < since:
                                    continue
                                all_old = False
                            except ValueError:
                                all_old = False

                        yield detail
                except Exception as e:
                    logger.warning(f"Error fetching {entry['href']}: {e}")
                    continue

            if all_old:
                logger.info(f"All entries on page {page} are older than {since}, stopping")
                break

    def normalize(self, raw: dict) -> dict:
        """Transform raw regulation data into standard schema."""
        # Build document ID from slug or doc number
        url = raw.get("url", "")
        slug = url.rstrip("/").split("/")[-1] if url else ""
        doc_number = raw.get("doc_number", "")
        doc_id = doc_number if doc_number else slug

        # Parse date
        date_str = raw.get("date", "")
        if date_str:
            try:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                date_iso = dt.strftime("%Y-%m-%d")
            except ValueError:
                date_iso = date_str
        else:
            date_raw = raw.get("date_raw", "")
            date_iso = date_raw if date_raw else None

        # Clean text - remove the metadata header that gets repeated
        text = raw.get("text", "")

        categories = raw.get("categories", [])
        cat_str = "; ".join(categories) if categories else None

        return {
            "_id": f"ID/DJP-TaxCirculars/{doc_id}",
            "_source": "ID/DJP-TaxCirculars",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": text,
            "date": date_iso,
            "url": url,
            "doc_number": doc_number,
            "doc_type": raw.get("doc_type"),
            "categories": cat_str,
            "regulation_status": raw.get("status"),
            "language": "id",
        }


# ── CLI entrypoint ────────────────────────────────────────────────
if __name__ == "__main__":
    scraper = DJPTaxCircularsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        print("Testing connectivity to pajak.go.id...")
        try:
            entries = scraper._get_listing_page(0)
            print(f"OK: Found {len(entries)} entries on page 0")
            if entries:
                detail = scraper._get_detail_page(entries[0]["href"])
                if detail and detail.get("text"):
                    print(f"OK: Detail page has {len(detail['text'])} chars of text")
                    print(f"  Title: {detail.get('title', '')[:100]}")
                    print(f"  Date: {detail.get('date', 'N/A')}")
                else:
                    print("WARN: Could not extract text from detail page")
        except Exception as e:
            print(f"FAIL: {e}")
            sys.exit(1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(json.dumps(stats, indent=2, default=str))

    elif command == "update":
        stats = scraper.update()
        print(json.dumps(stats, indent=2, default=str))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
