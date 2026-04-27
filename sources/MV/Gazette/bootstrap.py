#!/usr/bin/env python3
"""
MV/Gazette -- Maldives Government Gazette

Fetches laws, regulations, decisions, principles, tax rulings, and
other legal instruments from the official Maldives Government Gazette
(gazette.gov.mv). Full text extracted from PDFs hosted on Google
Cloud Storage.

Strategy:
  1. Scrape paginated listing pages per gazette type
  2. Extract gazette ID, title, date, and PDF URL from each entry
  3. Download PDF from Google Cloud Storage and extract text

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
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from html.parser import HTMLParser

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MV.Gazette")

BASE_URL = "https://www.gazette.gov.mv"
PDF_BASE = "https://storage.googleapis.com/gazette.gov.mv/docs/gazette"

# Gazette document types and their internal names
GAZETTE_TYPES = {
    "gaanoonu": "law",
    "gavaaidhu": "regulation",
    "garaaru": "decision",
    "usoolu": "principle",
    "other": "other",
    "tax-ruling": "tax_ruling",
}

# Dhivehi month names → month numbers
DHIVEHI_MONTHS = {
    "ޖަނަވަރީ": 1,
    "ފެބުރުވަރީ": 2,
    "މާރިޗު": 3,
    "އޭޕްރިލް": 4,
    "މެއި": 5,
    "ޖޫން": 6,
    "ޖުލައި": 7,
    "އޮގަސްޓު": 8,
    "ސެޕްޓެންބަރު": 9,
    "އޮކްޓޫބަރު": 10,
    "ނޮވެންބަރު": 11,
    "ޑިސެންބަރު": 12,
}


def _parse_dhivehi_date(date_str: str) -> Optional[str]:
    """Parse a Dhivehi date string like '14 މާރިޗު 2026' to ISO 8601."""
    date_str = date_str.strip()
    parts = date_str.split()
    if len(parts) != 3:
        return None
    try:
        day = int(parts[0])
        month = DHIVEHI_MONTHS.get(parts[1])
        year = int(parts[2])
        if month is None:
            return None
        return f"{year:04d}-{month:02d}-{day:02d}"
    except (ValueError, IndexError):
        return None


def _extract_entries_from_html(html: str) -> List[Dict[str, str]]:
    """Extract gazette entries from a listing page HTML.

    Each entry has: gazette_id, title, date_raw, pdf_id
    """
    entries = []
    pattern = (
        r'<a[^>]*class="gazette-title"[^>]*href="[^"]*gazette/(\d+)"'
        r'[^>]*title="([^"]*)"[^>]*>.*?'
        r'ތާރީޚު:\s*([^<]+).*?'
        r'storage\.googleapis\.com/gazette\.gov\.mv/docs/gazette/(\d+)\.pdf'
    )
    for match in re.finditer(pattern, html, re.DOTALL):
        gazette_id, title, date_raw, pdf_id = match.groups()
        entries.append({
            "gazette_id": gazette_id,
            "title": re.sub(r"\s+", " ", title).strip(),
            "date_raw": date_raw.strip(),
            "pdf_id": pdf_id,
        })
    return entries


def _get_max_page(html: str) -> int:
    """Extract the maximum page number from pagination links."""
    pages = re.findall(r'page=(\d+)', html)
    if not pages:
        return 1
    return max(int(p) for p in pages)


class MVGazetteScraper(BaseScraper):
    """Scraper for MV/Gazette -- Maldives Government Gazette."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,*/*",
            },
            timeout=120,
        )

    def _fetch_page(self, path: str) -> str:
        """Fetch an HTML page and return its content."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(path)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch page {path}: {e}")
            return ""

    def _download_pdf_bytes(self, pdf_url: str) -> Optional[bytes]:
        """Download a PDF and return raw bytes."""
        try:
            self.rate_limiter.wait()
            import urllib.request
            req = urllib.request.Request(
                pdf_url,
                headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            )
            with urllib.request.urlopen(req, timeout=120) as resp:
                content = resp.read()
            if content and content[:5] == b"%PDF-":
                return content
            if content and len(content) > 100:
                return content
            logger.warning(f"Empty or invalid PDF response: {pdf_url}")
            return None
        except Exception as e:
            logger.warning(f"Failed to download PDF {pdf_url}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all gazette entries with metadata and PDF URL."""
        for gazette_type, type_label in GAZETTE_TYPES.items():
            logger.info(f"Fetching gazette type: {gazette_type} ({type_label})")

            # Get first page to determine max page
            first_page_path = f"/gazette?type={gazette_type}"
            html = self._fetch_page(first_page_path)
            if not html:
                logger.warning(f"No response for type {gazette_type}")
                continue

            max_page = _get_max_page(html)
            logger.info(f"  {gazette_type}: {max_page} pages")

            # Process first page
            entries = _extract_entries_from_html(html)
            for entry in entries:
                entry["gazette_type"] = gazette_type
                entry["type_label"] = type_label
                yield entry

            # Process remaining pages
            for page in range(2, max_page + 1):
                page_path = f"/gazette?type={gazette_type}&page={page}"
                html = self._fetch_page(page_path)
                if not html:
                    continue
                entries = _extract_entries_from_html(html)
                for entry in entries:
                    entry["gazette_type"] = gazette_type
                    entry["type_label"] = type_label
                    yield entry

                if page % 50 == 0:
                    logger.info(f"  {gazette_type}: processed page {page}/{max_page}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield entries published since the given date."""
        # Re-fetch recent pages until we pass the since date
        for gazette_type, type_label in GAZETTE_TYPES.items():
            first_page_path = f"/gazette?type={gazette_type}"
            html = self._fetch_page(first_page_path)
            if not html:
                continue
            max_page = _get_max_page(html)

            for page in range(1, max_page + 1):
                if page > 1:
                    html = self._fetch_page(f"/gazette?type={gazette_type}&page={page}")
                    if not html:
                        continue

                entries = _extract_entries_from_html(html)
                found_old = False
                for entry in entries:
                    iso_date = _parse_dhivehi_date(entry["date_raw"])
                    if iso_date:
                        entry_date = datetime.fromisoformat(iso_date)
                        if entry_date.replace(tzinfo=timezone.utc) < since:
                            found_old = True
                            continue
                    entry["gazette_type"] = gazette_type
                    entry["type_label"] = type_label
                    yield entry

                if found_old:
                    break

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a raw gazette entry into a normalized record."""
        gazette_id = raw.get("gazette_id", "")
        pdf_id = raw.get("pdf_id", "")
        title = raw.get("title", "")
        date_raw = raw.get("date_raw", "")
        gazette_type = raw.get("gazette_type", "")
        type_label = raw.get("type_label", "")

        if not gazette_id or not pdf_id:
            return None

        # Parse date
        iso_date = _parse_dhivehi_date(date_raw) or ""

        # Build PDF URL
        pdf_url = f"{PDF_BASE}/{pdf_id}.pdf"

        # Extract text from PDF
        text = extract_pdf_markdown(
            source="MV/Gazette",
            source_id=f"MV-GAZ-{gazette_id}",
            pdf_url=pdf_url,
            table="legislation",
        )

        if not text or len(text.strip()) < 50:
            logger.warning(f"Insufficient text for gazette/{gazette_id} (pdf/{pdf_id})")
            return None

        return {
            "_id": f"MV-GAZ-{gazette_id}",
            "_source": "MV/Gazette",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "gazette_id": gazette_id,
            "title": title,
            "text": text,
            "date": iso_date,
            "date_raw": date_raw,
            "gazette_type": gazette_type,
            "type_label": type_label,
            "pdf_url": pdf_url,
            "url": f"{BASE_URL}/gazette/{gazette_id}",
        }


# ── CLI entry point ──────────────────────────────────────────────

def main():
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        sys.exit(1)

    scraper = MVGazetteScraper()

    if args[0] == "test":
        print("Testing connectivity to gazette.gov.mv ...")
        html = scraper._fetch_page("/gazette?type=gaanoonu")
        entries = _extract_entries_from_html(html)
        print(f"Found {len(entries)} entries on laws page 1")
        if entries:
            e = entries[0]
            print(f"  First entry: gazette/{e['gazette_id']} pdf/{e['pdf_id']}")
            print(f"  Title: {e['title'][:80]}")
            print(f"  Date: {e['date_raw']} -> {_parse_dhivehi_date(e['date_raw'])}")
        print("Connectivity OK")

    elif args[0] == "bootstrap":
        sample = "--sample" in args
        stats = scraper.bootstrap(sample_mode=sample, sample_size=15)
        print(json.dumps(stats, indent=2, default=str))

    elif args[0] == "update":
        last_run = scraper.status.get("last_run")
        since = datetime.fromisoformat(last_run) if last_run else datetime(2020, 1, 1, tzinfo=timezone.utc)
        stats = scraper.bootstrap(sample_mode=False)
        print(json.dumps(stats, indent=2, default=str))

    else:
        print(f"Unknown command: {args[0]}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
