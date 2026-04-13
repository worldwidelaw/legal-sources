#!/usr/bin/env python3
"""
NP/LawsOfNepal -- Nepal Law Commission: Laws of Nepal

Fetches Nepal's prevailing legislation from the Nepal Law Commission website.
Laws are organized into 17 volumes by subject matter, each containing multiple
acts published as PDFs.

Strategy:
  - Crawl 17 volume category pages to discover all acts
  - Each category page is a paginated table with: title, date, PDF link, content URL
  - Download PDFs from government CDN (giwmscdntwo.gov.np)
  - Extract full text using PyPDF2
  - ~340 acts covering constitutional, criminal, civil, commercial law, etc.

Source: https://lawcommission.gov.np/ (Government of Nepal, open data)
Rate limit: 1 req/sec (respectful to government server)

Usage:
  python bootstrap.py bootstrap            # Full pull (all acts)
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import io
import json
import logging
import re
import time
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NP.LawsOfNepal")

BASE_URL = "https://lawcommission.gov.np"

# 17 volumes of acts by subject matter
VOLUME_CATEGORIES = {
    1762: "Constitutional bodies and governance",
    1763: "Courts and justice administration",
    1764: "Parliament, political parties, elections",
    1765: "Internal administration",
    1766: "Security administration",
    1767: "Revenue and financial administration",
    1768: "Currency, banking, insurance, securities",
    1769: "Industry, commerce, supply",
    1783: "Tourism, labor, transportation",
    1784: "Communications, science, technology",
    1785: "Planning, development, construction",
    1786: "Food, agriculture, cooperatives, land",
    1787: "Forests, environment, water resources",
    1788: "Foreign affairs, education, sports",
    1789: "Health",
    1790: "Women, children, social welfare, culture",
    1791: "Local development",
}

# Navigation/sidebar content IDs to exclude
NAV_CONTENT_IDS = {"11069", "13529", "13541", "13546", "13554", "13555", "13556"}

# Sample content IDs known to have substantial content
SAMPLE_IDS = [
    "13544",  # Customs Act, 2082
    "13423",  # Good Governance Act, 2064
    "13511",  # Monitoring and Evaluation Act, 2080
    "13475",  # Oath Related Act, 2079
    "13430",  # Impeachment Act, 2059
    "13428",  # Immunity Act
    "13429",  # CIAA Act
    "12210",  # Recent act
    "12208",  # Recent act
    "12171",  # Recent act
    "13371",  # Act from page 3
    "13477",  # Act
    "13478",  # Act
    "13485",  # Act
    "13486",  # Act
]


class LawsOfNepalScraper(BaseScraper):
    """
    Scraper for NP/LawsOfNepal -- Laws of Nepal.
    Country: NP
    URL: https://lawcommission.gov.np/

    Data types: legislation
    Auth: none (government open data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=60,
        )

    # -- Category crawling ---------------------------------------------------

    def _parse_category_page(self, html: str) -> list[dict]:
        """Parse a category page table to extract act entries."""
        entries = []
        # Find table rows
        table_match = re.search(r'<table.*?</table>', html, re.DOTALL)
        if not table_match:
            return entries

        rows = re.findall(r'<tr.*?</tr>', table_match.group(0), re.DOTALL)
        for row in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            links = re.findall(r'href="([^"]+)"', row)
            if len(cells) < 3:
                continue

            # Extract title (2nd cell, clean HTML)
            title = re.sub(r'<[^>]+>', '', cells[1]).strip()
            if not title:
                continue

            # Extract date (3rd cell)
            date_str = re.sub(r'<[^>]+>', '', cells[2]).strip() if len(cells) > 2 else ""

            # Extract PDF URL and content URL from links
            pdf_url = None
            content_url = None
            content_id = None

            for link in links:
                link = link.strip()
                if 'pdf' in link.lower() or 'giwmscdntwo.gov.np' in link:
                    pdf_url = link
                elif '/content/' in link:
                    content_url = link.strip()
                    cid_match = re.search(r'/content/(\d+)/', link)
                    if cid_match:
                        content_id = cid_match.group(1)

            if content_id and content_id in NAV_CONTENT_IDS:
                continue

            if not content_id:
                continue

            entries.append({
                "content_id": content_id,
                "title": title,
                "date_str": date_str,
                "pdf_url": pdf_url,
                "content_url": f"{BASE_URL}{content_url.strip()}" if content_url else None,
            })

        return entries

    def _crawl_category(self, cat_id: int) -> list[dict]:
        """Crawl all pages of a category to get all act entries."""
        all_entries = []
        seen_ids = set()

        for page in range(1, 100):
            self.rate_limiter.wait()
            url = f"{BASE_URL}/category/{cat_id}/?page={page}"
            try:
                resp = self.client.get(url, timeout=30)
                if resp is None or resp.status_code != 200:
                    break
                entries = self._parse_category_page(resp.text)
                new_entries = [e for e in entries if e["content_id"] not in seen_ids]
                if not new_entries:
                    break
                for e in new_entries:
                    seen_ids.add(e["content_id"])
                    all_entries.append(e)
            except Exception as e:
                logger.warning(f"Error crawling category {cat_id} page {page}: {e}")
                break

        return all_entries

    def _discover_all_acts(self) -> list[dict]:
        """Discover all acts across all 17 volume categories."""
        all_acts = []
        seen_ids = set()

        for cat_id, cat_name in VOLUME_CATEGORIES.items():
            logger.info(f"Crawling volume {cat_id}: {cat_name}")
            entries = self._crawl_category(cat_id)
            new_count = 0
            for entry in entries:
                if entry["content_id"] not in seen_ids:
                    seen_ids.add(entry["content_id"])
                    entry["category"] = cat_name
                    entry["category_id"] = cat_id
                    all_acts.append(entry)
                    new_count += 1
            logger.info(f"  Found {new_count} new acts (total: {len(all_acts)})")

        logger.info(f"Discovery complete: {len(all_acts)} unique acts")
        return all_acts

    # -- PDF downloading and text extraction ---------------------------------

    def _fetch_pdf_from_content_page(self, content_id: str) -> Optional[str]:
        """Fetch PDF URL from a content page if not already known."""
        self.rate_limiter.wait()
        url = f"{BASE_URL}/content/{content_id}/"
        try:
            resp = self.client.get(url, timeout=30)
            if resp is None or resp.status_code != 200:
                return None
            match = re.search(r"pdf\s*=\s*'([^']+)'", resp.text)
            if match:
                return match.group(1)
        except Exception as e:
            logger.debug(f"Error fetching content page {content_id}: {e}")
        return None

    def _download_and_extract_pdf(self, pdf_url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="NP/LawsOfNepal",
            source_id="",
            pdf_url=pdf_url,
            table="legislation",
        ) or ""

    def _fetch_act(self, entry: dict) -> Optional[dict]:
        """Fetch full text for a single act entry."""
        content_id = entry["content_id"]
        pdf_url = entry.get("pdf_url")

        # If no PDF URL from category listing, try content page
        if not pdf_url:
            pdf_url = self._fetch_pdf_from_content_page(content_id)

        if not pdf_url:
            logger.debug(f"No PDF URL for content {content_id}: {entry.get('title', 'N/A')}")
            return None

        text = self._download_and_extract_pdf(pdf_url)
        if not text or len(text) < 50:
            logger.debug(f"Content {content_id}: text too short ({len(text) if text else 0} chars)")
            return None

        entry["text"] = text
        entry["pdf_url"] = pdf_url
        return entry

    # -- Core scraper methods ------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all acts from all 17 volume categories."""
        acts = self._discover_all_acts()
        found = 0
        for entry in acts:
            result = self._fetch_act(entry)
            if result:
                found += 1
                if found % 20 == 0:
                    logger.info(f"Progress: {found} acts fetched with full text")
                yield result

        logger.info(f"Fetch complete: {found} acts with full text out of {len(acts)} discovered")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch all acts — no incremental update mechanism available."""
        logger.info("No incremental update available; fetching all acts")
        yield from self.fetch_all()

    def fetch_sample(self, count: int = 15) -> Generator[dict, None, None]:
        """Fetch sample acts for validation."""
        found = 0

        for content_id in SAMPLE_IDS:
            if found >= count:
                break

            # First get the PDF URL from the content page
            pdf_url = self._fetch_pdf_from_content_page(content_id)
            if not pdf_url:
                logger.debug(f"Sample {content_id}: no PDF URL found")
                continue

            # Get the title from the content page
            self.rate_limiter.wait()
            try:
                resp = self.client.get(f"{BASE_URL}/content/{content_id}/", timeout=30)
                title = "Unknown"
                if resp and resp.status_code == 200:
                    match = re.search(r'<meta[^>]*property="og:title"[^>]*content="([^"]+)"', resp.text)
                    if match:
                        title = match.group(1).replace(" | Nepal Law Commission", "").strip()
            except Exception:
                title = "Unknown"

            text = self._download_and_extract_pdf(pdf_url)
            if not text or len(text) < 50:
                logger.debug(f"Sample {content_id}: text too short")
                continue

            entry = {
                "content_id": content_id,
                "title": title,
                "text": text,
                "pdf_url": pdf_url,
                "content_url": f"{BASE_URL}/content/{content_id}/",
                "category": "sample",
                "date_str": "",
            }

            found += 1
            logger.info(f"Sample {found}/{count}: {content_id} - {title[:60]} ({len(text)} chars)")
            yield entry

        logger.info(f"Sample complete: {found} records")

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw act record to standard schema."""
        content_id = raw.get("content_id", "0")
        title = raw.get("title", "Unknown Act")
        text = raw.get("text", "")

        # Build the URL
        content_url = raw.get("content_url", f"{BASE_URL}/content/{content_id}/")

        return {
            "_id": f"NP-LawsOfNepal-{content_id}",
            "_source": "NP/LawsOfNepal",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": None,  # Dates are in Bikram Sambat calendar, not easily convertible
            "url": content_url,
            "content_id": content_id,
            "category": raw.get("category", ""),
            "pdf_url": raw.get("pdf_url", ""),
            "language": "ne",
        }

    def test_api(self) -> bool:
        """Test connectivity to Nepal Law Commission website."""
        logger.info("Testing Nepal Law Commission website access...")

        # Test main page
        resp = self.client.get(f"{BASE_URL}/", timeout=15)
        if not resp or resp.status_code != 200:
            logger.error(f"Main page failed: {resp.status_code if resp else 'No response'}")
            return False
        logger.info("Main page: OK")

        # Test a category page
        self.rate_limiter.wait()
        resp = self.client.get(f"{BASE_URL}/category/1762/", timeout=15)
        if not resp or resp.status_code != 200:
            logger.error(f"Category page failed: {resp.status_code if resp else 'No response'}")
            return False
        entries = self._parse_category_page(resp.text)
        logger.info(f"Category 1762: {len(entries)} entries found")

        # Test PDF download
        if entries and entries[0].get("pdf_url"):
            self.rate_limiter.wait()
            pdf_url = entries[0]["pdf_url"]
            text = self._download_and_extract_pdf(pdf_url)
            if text and len(text) > 50:
                logger.info(f"PDF text extraction: OK ({len(text)} chars)")
            else:
                logger.error("PDF text extraction failed")
                return False

        logger.info("All tests passed!")
        return True


# ── CLI ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    scraper = LawsOfNepalScraper()

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
        logger.info("No incremental update; running full bootstrap")
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in scraper.fetch_all():
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1

        logger.info(f"Update complete: {saved} records saved")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
