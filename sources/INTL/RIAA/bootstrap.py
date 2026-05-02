#!/usr/bin/env python3
"""
INTL/RIAA -- UN Reports of International Arbitral Awards

Fetches international arbitral awards from the UN RIAA collection.

Strategy:
  - Scrape each volume page (vol_1.shtml .. vol_34.shtml) for case entries
  - Parse case title, parties, date from HTML table rows
  - Download each case PDF from legal.un.org/riaa/cases/
  - Extract full text via common/pdf_extract

Data Coverage:
  - 34 volumes, covering awards from 18th century to present
  - State-to-state and state-to-international organization disputes
  - English and French

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.RIAA")

BASE_URL = "https://legal.un.org/riaa/"
MAX_VOLUME = 34
MAX_PDF_BYTES = 100 * 1024 * 1024  # 100MB limit per PDF

# Arabic to Roman numeral mapping for volumes
ROMAN = {
    1: "I", 2: "II", 3: "III", 4: "IV", 5: "V",
    6: "VI", 7: "VII", 8: "VIII", 9: "IX", 10: "X",
    11: "XI", 12: "XII", 13: "XIII", 14: "XIV", 15: "XV",
    16: "XVI", 17: "XVII", 18: "XVIII", 19: "XIX", 20: "XX",
    21: "XXI", 22: "XXII", 23: "XXIII", 24: "XXIV", 25: "XXV",
    26: "XXVI", 27: "XXVII", 28: "XXVIII", 29: "XXIX", 30: "XXX",
    31: "XXXI", 32: "XXXII", 33: "XXXIII", 34: "XXXIV",
}


class RIAAScraper(BaseScraper):
    """Scraper for UN Reports of International Arbitral Awards."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
            "Accept": "text/html,application/xhtml+xml,application/pdf",
            "Accept-Language": "en",
        })

    def _indirect_to_direct_url(self, href: str) -> str:
        """Convert docs/?path=../riaa/cases/... URL to direct PDF URL."""
        # Extract the relative path from the docs wrapper
        m = re.search(r'path=\.\./(riaa/cases/[^&]+)', href)
        if m:
            return f"https://legal.un.org/{m.group(1)}"
        # Already a direct URL
        if "riaa/cases/" in href:
            if href.startswith("http"):
                return href.replace("http://", "https://")
            return f"https://legal.un.org{href}"
        return href

    def _parse_volume_page(self, vol_num: int) -> list[dict]:
        """Scrape a volume page for all case entries."""
        url = f"{BASE_URL}vol_{vol_num}.shtml"
        roman = ROMAN[vol_num]
        entries = []

        try:
            time.sleep(1)
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            resp.encoding = "utf-8"
        except Exception as e:
            logger.error(f"Failed to fetch volume {vol_num}: {e}")
            return []

        html = resp.text

        # Find the table content area
        # Each case is in a <tr> with: <td>NUMBER</td> <td>TITLE</td> <td><a href="PDF">...</a></td>
        # We'll parse row by row using regex

        # Extract all table rows that contain PDF links to cases
        row_pattern = re.compile(
            r'<tr[^>]*>\s*'
            r'<td[^>]*>(.*?)</td>\s*'
            r'<td[^>]*>(.*?)</td>\s*'
            r'<td[^>]*>(.*?)</td>\s*'
            r'</tr>',
            re.DOTALL | re.IGNORECASE,
        )

        seen_urls = set()
        for row_match in row_pattern.finditer(html):
            col1 = row_match.group(1).strip()
            col2 = row_match.group(2).strip()
            col3 = row_match.group(3).strip()

            # Find PDF link in col3 (or sometimes col2)
            pdf_link = None
            for col in [col3, col2]:
                link_m = re.search(r'href="([^"]*?riaa/cases/[^"]+\.pdf[^"]*)"', col)
                if link_m:
                    pdf_link = unescape(link_m.group(1))
                    break

            if not pdf_link:
                continue

            direct_url = self._indirect_to_direct_url(pdf_link)

            # Skip forewords, indexes, complete volumes, table of contents
            pdf_filename = direct_url.split("/")[-1].lower()
            if any(skip in pdf_filename for skip in ["foreword", "index", "table_of_content", "toc"]):
                continue

            # Skip rows where case number or title is foreword/avant-propos/complete PDF
            combined = re.sub(r'<[^>]+>', '', col1 + " " + col2).lower()
            if any(skip in combined for skip in ["complete pdf", "pdf complet", "foreword", "avant-propos"]):
                continue

            # Skip duplicates
            if direct_url in seen_urls:
                continue
            seen_urls.add(direct_url)

            # Parse case number from col1
            case_num = re.sub(r'<[^>]+>', '', col1).strip().rstrip(".")

            # Parse title and metadata from col2
            title_raw = re.sub(r'<[^>]+>', ' ', col2)
            title_raw = unescape(title_raw)
            title_raw = re.sub(r'\s+', ' ', title_raw).strip()

            if not title_raw or len(title_raw) < 3:
                title_raw = f"RIAA Volume {roman}, Case {case_num}" if case_num else f"RIAA Volume {roman}"

            # Try to extract date from title text
            date_str = self._extract_date(title_raw)

            # Build a stable ID from the PDF path
            # e.g., vol_XXVII/35-125.pdf -> riaa-vol-xxvii-35-125
            path_part = direct_url.split("riaa/cases/")[-1] if "riaa/cases/" in direct_url else pdf_filename
            path_part = path_part.replace(".pdf", "").replace("/", "-").lower()
            case_id = f"riaa-{path_part}"

            entries.append({
                "case_id": case_id,
                "volume": vol_num,
                "volume_roman": roman,
                "case_number": case_num,
                "title": title_raw,
                "date": date_str,
                "pdf_url": direct_url,
            })

        logger.info(f"Volume {roman} ({vol_num}): {len(entries)} cases")
        return entries

    def _extract_date(self, text: str) -> Optional[str]:
        """Try to extract a date from case title text."""
        # Common patterns: "October 13, 1922" or "13 octobre 1922" or "4 septembre 1920"
        months_en = {
            "january": 1, "february": 2, "march": 3, "april": 4,
            "may": 5, "june": 6, "july": 7, "august": 8,
            "september": 9, "october": 10, "november": 11, "december": 12,
        }
        months_fr = {
            "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4,
            "mai": 5, "juin": 6, "juillet": 7, "août": 8, "aout": 8,
            "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12,
        }
        all_months = {**months_en, **months_fr}

        # Pattern: "Month DD, YYYY" or "DD month YYYY"
        for m_name, m_num in all_months.items():
            # English: October 13, 1922
            pat = re.search(
                rf'{m_name}\s+(\d{{1,2}}),?\s+(\d{{4}})',
                text, re.IGNORECASE,
            )
            if pat:
                return f"{pat.group(2)}-{m_num:02d}-{int(pat.group(1)):02d}"

            # French: 4 septembre 1920
            pat = re.search(
                rf'(\d{{1,2}})\s+{m_name}\s+(\d{{4}})',
                text, re.IGNORECASE,
            )
            if pat:
                return f"{pat.group(2)}-{m_num:02d}-{int(pat.group(1)):02d}"

        # Fallback: just year
        year_m = re.search(r'\b(1[789]\d{2}|20[012]\d)\b', text)
        if year_m:
            return f"{year_m.group(1)}-01-01"

        return None

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download a PDF with rate limiting."""
        try:
            time.sleep(1.5)
            resp = self.session.get(url, timeout=120)
            resp.raise_for_status()
            content = resp.content
            if len(content) > MAX_PDF_BYTES:
                logger.warning(f"  PDF too large ({len(content)} bytes), skipping")
                return None
            if len(content) < 500:
                logger.warning(f"  PDF too small ({len(content)} bytes), likely error")
                return None
            # Verify it's actually a PDF
            if not content[:5].startswith(b'%PDF'):
                logger.warning(f"  Not a PDF (starts with {content[:20]}), skipping")
                return None
            return content
        except Exception as e:
            logger.error(f"  PDF download failed: {e}")
            return None

    def _extract_text(self, pdf_bytes: bytes, source_id: str) -> Optional[str]:
        """Extract text from PDF bytes."""
        text = extract_pdf_markdown(
            source="INTL/RIAA",
            source_id=source_id,
            pdf_bytes=pdf_bytes,
            table="case_law",
        )
        if text and len(text.strip()) >= 100:
            return text
        return None

    def _get_all_entries(self) -> list[dict]:
        """Get all case entries from all volumes."""
        all_entries = []
        for vol in range(1, MAX_VOLUME + 1):
            entries = self._parse_volume_page(vol)
            all_entries.extend(entries)
        logger.info(f"Total entries across all volumes: {len(all_entries)}")
        return all_entries

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all awards with full text from PDFs."""
        entries = self._get_all_entries()
        logger.info(f"Total entries to process: {len(entries)}")

        for i, entry in enumerate(entries):
            try:
                logger.info(
                    f"[{i+1}/{len(entries)}] Downloading {entry['case_id']} from vol {entry['volume_roman']}..."
                )
                pdf_bytes = self._download_pdf(entry["pdf_url"])
                if not pdf_bytes:
                    continue

                text = self._extract_text(pdf_bytes, entry["case_id"])
                if not text:
                    logger.warning(f"  Insufficient text for {entry['case_id']}, skipping")
                    continue

                entry["_extracted_text"] = text
                yield entry

            except Exception as e:
                logger.error(f"  Error processing {entry['case_id']}: {e}")
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield awards from recent volumes."""
        # RIAA updates are rare (new volumes every few years)
        # Fetch last 3 volumes for updates
        for vol in range(max(1, MAX_VOLUME - 2), MAX_VOLUME + 1):
            entries = self._parse_volume_page(vol)
            for entry in entries:
                pdf_bytes = self._download_pdf(entry["pdf_url"])
                if not pdf_bytes:
                    continue
                text = self._extract_text(pdf_bytes, entry["case_id"])
                if not text:
                    continue
                entry["_extracted_text"] = text
                yield entry

    def normalize(self, raw: dict) -> dict:
        """Transform raw award data into standard schema."""
        return {
            "_id": raw.get("case_id", ""),
            "_source": "INTL/RIAA",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("_extracted_text", ""),
            "date": raw.get("date"),
            "url": raw.get("pdf_url", ""),
            "volume": raw.get("volume"),
            "volume_roman": raw.get("volume_roman"),
            "case_number": raw.get("case_number"),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = RIAAScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        entries = scraper._get_all_entries()
        for e in entries:
            print(f"  Vol {e['volume_roman']:>6}  {e['case_number']:>6}  {e['date'] or '????-??-??'}  {e['title'][:70]}")
        print(f"\nTotal: {len(entries)} awards across {MAX_VOLUME} volumes")
        sys.exit(0)

    if command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample, sample_size=15)
        print(json.dumps(result, indent=2, default=str))
    elif command == "update":
        result = scraper.update()
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
