#!/usr/bin/env python3
"""
INTL/OECDTribunal -- OECD Administrative Tribunal Judgments

Fetches judgments from the OECD Administrative Tribunal.

Strategy:
  - Enumerate PDF URLs using predictable patterns:
    TAOECD_judgement_{N}.pdf  or  TAOECD_judgment_{N}.pdf
  - Also check combined documents (e.g., TAOECD_judgement_86_89.pdf)
  - Download each PDF and extract full text via common/pdf_extract

Data Coverage:
  - ~92+ judgments from 1980s to present
  - Employment disputes between OECD and staff
  - English and French

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
"""

import sys
import json
import logging
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
logger = logging.getLogger("legal-data-hunter.INTL.OECDTribunal")

BASE_PDF_URL = "https://www.oecd.org/content/dam/oecd/en/about/administrative-tribunal"
MAX_PDF_BYTES = 50 * 1024 * 1024
MAX_JUDGMENT_NUMBER = 130  # scan up to this number

# Combined documents where multiple judgments are in a single PDF
COMBINED_DOCS = ["43_46", "86_89"]


class OECDTribunalScraper(BaseScraper):
    """Scraper for OECD Administrative Tribunal judgments."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
            "Accept": "application/pdf",
        })

    def _try_pdf_url(self, number_str: str) -> Optional[str]:
        """Try both spelling variants and return the working URL or None."""
        for spelling in ["judgement", "judgment"]:
            url = f"{BASE_PDF_URL}/TAOECD_{spelling}_{number_str}.pdf"
            try:
                resp = self.session.head(url, timeout=15, allow_redirects=True)
                if resp.status_code == 200:
                    return url
            except Exception:
                pass
        return None

    def _enumerate_judgments(self) -> list[dict]:
        """Enumerate all available judgment PDFs."""
        entries = []

        # Individual judgments
        for i in range(1, MAX_JUDGMENT_NUMBER + 1):
            url = self._try_pdf_url(str(i))
            if url:
                entries.append({
                    "number": str(i),
                    "title": f"OECD Administrative Tribunal Judgment No. {i}",
                    "pdf_url": url,
                })
                logger.info(f"  Found judgment #{i}")
            time.sleep(0.3)

        # Combined documents
        for combo in COMBINED_DOCS:
            url = self._try_pdf_url(combo)
            if url:
                entries.append({
                    "number": combo,
                    "title": f"OECD Administrative Tribunal Judgments No. {combo.replace('_', '-')}",
                    "pdf_url": url,
                })
                logger.info(f"  Found combined judgment #{combo}")
            time.sleep(0.3)

        logger.info(f"Enumerated {len(entries)} available judgments")
        return entries

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download a PDF."""
        try:
            time.sleep(1)
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
            source="INTL/OECDTribunal",
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
        entries = self._enumerate_judgments()
        logger.info(f"Total entries to process: {len(entries)}")

        for i, entry in enumerate(entries):
            try:
                logger.info(
                    f"[{i+1}/{len(entries)}] Downloading judgment #{entry['number']} ..."
                )
                pdf_bytes = self._download_pdf(entry["pdf_url"])
                if not pdf_bytes:
                    continue

                text = self._extract_text(pdf_bytes, entry["number"])
                if not text:
                    logger.warning(f"  Insufficient text for #{entry['number']}, skipping")
                    continue

                entry["_extracted_text"] = text
                yield entry

            except Exception as e:
                logger.error(f"  Error processing judgment #{entry['number']}: {e}")
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Re-fetch all (no dates available in URL pattern)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw judgment data into standard schema."""
        number = raw.get("number", "")
        title = raw.get("title", f"OECD AT Judgment {number}")

        uid_slug = number.lower().replace("/", "-").replace("_", "-")

        return {
            "_id": f"oecd-at-{uid_slug}",
            "_source": "INTL/OECDTribunal",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("_extracted_text", ""),
            "date": None,  # Dates not available from URL pattern
            "url": raw.get("pdf_url", ""),
            "judgment_number": number,
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = OECDTribunalScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        entries = scraper._enumerate_judgments()
        print(f"Total available judgments: {len(entries)}")
        if entries:
            print(f"First: #{entries[0]['number']}")
            print(f"Last:  #{entries[-1]['number']}")
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
