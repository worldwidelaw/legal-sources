#!/usr/bin/env python3
"""
GI/GibraltarLaws -- Laws of Gibraltar (gibraltarlaws.gov.gi)

Fetches all consolidated legislation from the official Gibraltar Laws website.
The site provides a single HTML listing page with all ~8,300+ legislation items,
each downloadable as PDF.

Strategy:
  - Fetch /legislations listing page (all items in one HTML page)
  - Parse table rows to extract: URL, date, number, title, topic, type
  - Download PDF via {detail_url}/download
  - Extract text from PDF using pdfplumber/pypdf

Listing HTML structure:
  <div class="tr" data-href="https://.../{slug}-{id}">
    <div class="td"><h6>DATE</h6><p>17 Apr 2026</p></div>
    <div class="td"><h6>LAST UPDATE</h6><p>17 Apr 2026</p></div>
    <div class="td"><h6>NUMBER</h6><p>2026/078</p></div>
    <div class="td "><h6>TITLE</h6><p>Inquiry Rules 2026</p></div>
    <div class="td"><h6>TOPIC</h6><p>INQUIRIES</p></div>
    <div class="td"><h6>TYPE</h6><p class="light">Subsidiary Legislation</p></div>
  </div>

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Re-fetch all
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GI.GibraltarLaws")

BASE_URL = "https://www.gibraltarlaws.gov.gi"

# Regex to extract each table row: data-href and all <p> contents
ROW_RE = re.compile(
    r'<div\s+class="tr"\s+data-href="([^"]+)">', re.DOTALL
)

# Extract <p> tag content (may have class attribute)
P_RE = re.compile(r'<p(?:\s+class="[^"]*")?>(.*?)</p>', re.DOTALL)

MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def parse_date(date_str: str) -> Optional[str]:
    """Parse 'DD Mon YYYY' to ISO date."""
    date_str = date_str.strip()
    match = re.match(r'(\d{1,2})\s+(\w{3,})\s+(\d{4})', date_str)
    if match:
        day, month_name, year = match.groups()
        month = MONTH_MAP.get(month_name[:3].lower())
        if month:
            return f"{year}-{month:02d}-{int(day):02d}"
    return None


def _slug_id_from_url(url: str) -> str:
    """Extract slug-id from URL like .../legislations/inquiry-rules-2026-8424"""
    parts = url.rstrip("/").split("/")
    return parts[-1] if parts else url


class GibraltarLawsScraper(BaseScraper):
    """Scraper for GI/GibraltarLaws -- Laws of Gibraltar."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,*/*",
            },
            timeout=180,
        )

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        return extract_pdf_markdown(
            source="GI/GibraltarLaws",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="legislation",
        ) or ""

    def _parse_listing(self) -> List[Dict[str, Any]]:
        """Fetch and parse the legislation listing page."""
        logger.info("Fetching legislation listing (this may take a moment — 11MB page)...")
        try:
            resp = self.client.get("/legislations")
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch listing page: {e}")
            return []

        html = resp.text
        logger.info(f"Listing page fetched: {len(html)} bytes")

        results = []

        # Split HTML by data-href rows
        row_positions = [m.start() for m in ROW_RE.finditer(html)]
        # Add end of HTML as sentinel
        row_positions.append(len(html))

        for i in range(len(row_positions) - 1):
            chunk = html[row_positions[i]:row_positions[i + 1]]

            # Extract URL
            url_match = ROW_RE.search(chunk)
            if not url_match:
                continue
            detail_url = url_match.group(1).strip()

            # Extract all <p> contents in order
            p_contents = [p.strip() for p in P_RE.findall(chunk)]
            # Expected order: date, last_update, number, title, topic, type
            if len(p_contents) < 4:
                continue

            entry = {
                "detail_url": detail_url,
                "slug_id": _slug_id_from_url(detail_url),
                "date_raw": p_contents[0] if len(p_contents) > 0 else "",
                "date": parse_date(p_contents[0]) if len(p_contents) > 0 else None,
                "last_update": p_contents[1] if len(p_contents) > 1 else "",
                "number": p_contents[2] if len(p_contents) > 2 else "",
                "title": p_contents[3] if len(p_contents) > 3 else "",
                "topic": p_contents[4] if len(p_contents) > 4 else "",
                "leg_type": p_contents[5] if len(p_contents) > 5 else "",
            }
            results.append(entry)

        logger.info(f"Parsed {len(results)} legislation items from listing")
        return results

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        slug_id = raw.get("slug_id", "")
        detail_url = raw.get("detail_url", "")

        return {
            "_id": f"GI/GibraltarLaws/{slug_id}",
            "_source": "GI/GibraltarLaws",
            "_type": "legislation",
            "_fetched_at": now,
            "title": raw.get("title", "Unknown"),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": detail_url,
            "slug_id": slug_id,
            "number": raw.get("number", ""),
            "topic": raw.get("topic", ""),
            "leg_type": raw.get("leg_type", ""),
            "last_update": raw.get("last_update", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        entries = self._parse_listing()
        if not entries:
            logger.error("No entries found in listing page")
            return

        count = 0
        errors = 0

        for i, entry in enumerate(entries):
            title = entry.get("title", "?")
            slug_id = entry["slug_id"]
            download_url = f"{entry['detail_url']}/download"

            logger.info(f"  [{i + 1}/{len(entries)}] Downloading: {title[:60]}")

            try:
                self.rate_limiter.wait()
                resp = self.client.get(download_url)
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"  Failed to download PDF for {slug_id}: {e}")
                errors += 1
                continue

            if not resp.content or resp.content[:5] != b"%PDF-":
                logger.warning(f"  Not a PDF: {slug_id}")
                errors += 1
                continue

            text = self._extract_pdf_text(resp.content)
            if not text or len(text.strip()) < 50:
                logger.warning(f"  Insufficient text from {slug_id}: {len(text) if text else 0} chars")
                errors += 1
                continue

            entry["text"] = text
            yield entry
            count += 1

        logger.info(f"Fetched {count} legislation items ({errors} errors)")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()


# ── CLI ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = GibraltarLawsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing listing page...")
        entries = scraper._parse_listing()
        if not entries:
            logger.error("FAILED — no entries found")
            sys.exit(1)
        logger.info(f"OK — {len(entries)} items in listing")

        logger.info("Testing PDF download...")
        first = entries[0]
        download_url = f"{first['detail_url']}/download"
        import requests
        resp = requests.get(download_url, timeout=60,
                            headers={"User-Agent": "LegalDataHunter/1.0"})
        if resp.status_code == 200 and resp.content[:5] == b"%PDF-":
            text = scraper._extract_pdf_text(resp.content)
            logger.info(f"OK — PDF download works, {len(text)} chars extracted")
        else:
            logger.error(f"FAILED — status {resp.status_code}")
            sys.exit(1)

    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
