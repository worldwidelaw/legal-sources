#!/usr/bin/env python3
"""
INTL/IMFAT -- IMF Administrative Tribunal Judgments

Fetches judgments from the IMF Administrative Tribunal.

Strategy:
  - Scrape listing page for PDF links (pattern: j{YYYY}-{N}.pdf)
  - Download each PDF and extract full text via common/pdf_extract
  - Parse titles/dates from listing page HTML

Data Coverage:
  - 61 judgments from 2006 to present
  - Employment disputes between IMF and staff
  - English

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

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.IMFAT")

LISTING_URL = "https://www.imf.org/en/about/imfat-index/imfat-judgements"
PDF_BASE = "https://www.imf.org/-/media/files/imfat/"
MAX_PDF_BYTES = 50 * 1024 * 1024

# Month name to number mapping
MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


class IMFATScraper(BaseScraper):
    """Scraper for IMF Administrative Tribunal judgments."""

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

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date like 'Oct 27, 2025' to ISO format."""
        m = re.match(r'(\w{3})\w*\.?\s+(\d{1,2}),?\s+(\d{4})', date_str.strip())
        if m:
            month = MONTHS.get(m.group(1).lower()[:3])
            if month:
                return f"{m.group(3)}-{month:02d}-{int(m.group(2)):02d}"
        return None

    def _get_judgment_entries(self) -> list[dict]:
        """Scrape the listing page for all judgment entries."""
        resp = self.session.get(LISTING_URL, timeout=30)
        resp.raise_for_status()
        html = resp.text

        entries = []
        seen = set()

        # Find all PDF links matching the IMFAT judgment pattern
        # Pattern: href="..../imfat/jYYYY-N.pdf"
        pdf_pattern = re.compile(
            r'href="([^"]*?/imfat/j(\d{4})-(\d+)\.pdf)"',
            re.IGNORECASE,
        )

        for match in pdf_pattern.finditer(html):
            pdf_url = match.group(1)
            year = int(match.group(2))
            num = int(match.group(3))
            key = f"{year}-{num}"

            if key in seen:
                continue
            seen.add(key)

            # Skip summary PDFs
            if "-summary" in pdf_url.lower():
                continue

            # Ensure full URL
            if pdf_url.startswith("/"):
                pdf_url = f"https://www.imf.org{pdf_url}"

            # Try to extract title and date from surrounding context
            # Look for text near this link
            pos = match.start()
            context = html[max(0, pos - 500):pos + 200]

            # Try to find the case title (text before the PDF link)
            title = f"IMFAT Judgment {year}-{num}"
            title_match = re.search(
                r'(?:^|>)\s*([A-Z""][^<]{5,80}?v\.\s*IMF[^<]*?)(?:\s*<|\s*\()',
                context,
            )
            if title_match:
                title = title_match.group(1).strip()
                # Clean up HTML entities
                title = title.replace("&quot;", '"').replace("&#8220;", "\u201c").replace("&#8221;", "\u201d")

            # Try to find the date
            date_str = None
            date_match = re.search(
                r'(\w{3,9}\.?\s+\d{1,2},?\s+\d{4})',
                context,
            )
            if date_match:
                date_str = self._parse_date(date_match.group(1))

            entries.append({
                "judgment_id": f"IMFAT/{year}/{num}",
                "year": year,
                "number": num,
                "pdf_url": pdf_url,
                "title": title,
                "date": date_str or f"{year}-01-01",
            })

        # Sort by year desc, number desc
        entries.sort(key=lambda e: (e["year"], e["number"]), reverse=True)
        logger.info(f"Found {len(entries)} IMFAT judgments on listing page")
        return entries

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download a PDF with rate limiting."""
        try:
            time.sleep(1.5)
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            if len(resp.content) > MAX_PDF_BYTES:
                logger.warning(f"  PDF too large ({len(resp.content)} bytes), skipping")
                return None
            if len(resp.content) < 500:
                logger.warning(f"  PDF too small ({len(resp.content)} bytes), likely error")
                return None
            return resp.content
        except Exception as e:
            logger.error(f"  PDF download failed: {e}")
            return None

    def _extract_text(self, pdf_bytes: bytes, source_id: str) -> Optional[str]:
        """Extract text from PDF bytes."""
        text = extract_pdf_markdown(
            source="INTL/IMFAT",
            source_id=source_id,
            pdf_bytes=pdf_bytes,
            table="case_law",
        )
        if text and len(text.strip()) >= 100:
            return text

        import io
        try:
            import pdfplumber
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pages = [p.extract_text() or "" for p in pdf.pages]
                text = "\n\n".join(p for p in pages if p)
                if text and len(text.strip()) >= 100:
                    return text
        except Exception:
            pass
        try:
            from pypdf import PdfReader
            reader = PdfReader(io.BytesIO(pdf_bytes))
            pages = [p.extract_text() or "" for p in reader.pages]
            text = "\n\n".join(p for p in pages if p)
            if text and len(text.strip()) >= 100:
                return text
        except Exception:
            pass
        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all judgments with full text from PDFs."""
        entries = self._get_judgment_entries()
        logger.info(f"Total entries to process: {len(entries)}")

        for i, entry in enumerate(entries):
            try:
                logger.info(
                    f"[{i+1}/{len(entries)}] Downloading {entry['judgment_id']} ..."
                )
                pdf_bytes = self._download_pdf(entry["pdf_url"])
                if not pdf_bytes:
                    continue

                text = self._extract_text(pdf_bytes, entry["judgment_id"])
                if not text:
                    logger.warning(f"  Insufficient text for {entry['judgment_id']}, skipping")
                    continue

                entry["_extracted_text"] = text
                yield entry

            except Exception as e:
                logger.error(f"  Error processing {entry['judgment_id']}: {e}")
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield judgments from recent years."""
        since_year = since.year
        entries = [e for e in self._get_judgment_entries() if e["year"] >= since_year]

        for entry in entries:
            pdf_bytes = self._download_pdf(entry["pdf_url"])
            if not pdf_bytes:
                continue
            text = self._extract_text(pdf_bytes, entry["judgment_id"])
            if not text:
                continue
            entry["_extracted_text"] = text
            yield entry

    def normalize(self, raw: dict) -> dict:
        """Transform raw judgment data into standard schema."""
        judgment_id = raw.get("judgment_id", "")
        uid_slug = judgment_id.lower().replace("/", "-").replace(" ", "-")

        return {
            "_id": f"imfat-{uid_slug}",
            "_source": "INTL/IMFAT",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("_extracted_text", ""),
            "date": raw.get("date"),
            "url": raw.get("pdf_url", ""),
            "judgment_number": judgment_id,
            "year": raw.get("year"),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = IMFATScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        entries = scraper._get_judgment_entries()
        for e in entries:
            print(f"  {e['judgment_id']}  {e['date']}  {e['title'][:60]}")
        print(f"\nTotal: {len(entries)} judgments")
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
