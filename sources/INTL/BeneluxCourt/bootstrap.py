#!/usr/bin/env python3
"""
INTL/BeneluxCourt -- Benelux Court of Justice Judgments

Fetches judgments from the Benelux Court of Justice (Cour de Justice Benelux).

Strategy:
  - Scrape listing page at courbeneluxhof.int/fr/arrets-conclusions/
  - Each table row has: case number, name, subject code, decision date, link
  - Visit each case page to find PDF download links
  - Download PDF and extract full text via common/pdf_extract

Data Coverage:
  - ~370 judgments from 1974 to present
  - Preliminary rulings, BOIP IP appeals, civil service disputes
  - French and Dutch

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
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.BeneluxCourt")

LISTING_URL = "https://www.courbeneluxhof.int/fr/arrets-conclusions/"
MAX_PDF_BYTES = 50 * 1024 * 1024


class BeneluxCourtScraper(BaseScraper):
    """Scraper for Benelux Court of Justice judgments."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
            "Accept": "text/html,application/xhtml+xml,application/pdf",
            "Accept-Language": "fr,nl,en",
        })

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date like '21-06-2024' to ISO format '2024-06-21'."""
        m = re.match(r'(\d{2})-(\d{2})-(\d{4})', date_str.strip())
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        return None

    def _get_case_entries(self) -> list[dict]:
        """Scrape the listing page for all case entries."""
        resp = self.session.get(LISTING_URL, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")

        entries = []
        seen = set()

        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 4:
                continue

            case_number = cells[0].get_text(strip=True)
            case_name = cells[1].get_text(strip=True)
            subject_code = cells[2].get_text(strip=True)
            date_raw = cells[3].get_text(strip=True)

            link = cells[1].find("a", href=True)
            if not link:
                continue

            case_url = link["href"]
            if not case_url.startswith("http"):
                case_url = f"https://www.courbeneluxhof.int{case_url}"

            if case_url in seen:
                continue
            seen.add(case_url)

            date_iso = self._parse_date(date_raw) if date_raw else None

            entries.append({
                "case_number": case_number,
                "case_name": case_name,
                "subject_code": subject_code,
                "date": date_iso,
                "case_url": case_url,
            })

        logger.info(f"Found {len(entries)} cases on listing page")
        return entries

    def _get_pdf_urls(self, case_url: str) -> list[str]:
        """Visit a case page and extract PDF download URLs."""
        try:
            time.sleep(1.5)
            resp = self.session.get(case_url, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.content, "html.parser")

            pdf_urls = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if ".pdf" in href.lower():
                    if not href.startswith("http"):
                        href = f"https://www.courbeneluxhof.int{href}"
                    pdf_urls.append(href)

            return pdf_urls
        except Exception as e:
            logger.error(f"  Failed to fetch case page {case_url}: {e}")
            return []

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
            source="INTL/BeneluxCourt",
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
        entries = self._get_case_entries()
        logger.info(f"Total entries to process: {len(entries)}")

        for i, entry in enumerate(entries):
            try:
                logger.info(
                    f"[{i+1}/{len(entries)}] Processing {entry['case_number']} - "
                    f"{entry['case_name'][:50]}..."
                )
                pdf_urls = self._get_pdf_urls(entry["case_url"])
                if not pdf_urls:
                    logger.warning(f"  No PDFs found for {entry['case_number']}")
                    continue

                # Pick the first PDF (usually the French judgment)
                pdf_url = pdf_urls[0]
                pdf_bytes = self._download_pdf(pdf_url)
                if not pdf_bytes:
                    continue

                text = self._extract_text(pdf_bytes, entry["case_number"])
                if not text:
                    logger.warning(f"  Insufficient text for {entry['case_number']}, skipping")
                    continue

                entry["_extracted_text"] = text
                entry["pdf_url"] = pdf_url
                yield entry

            except Exception as e:
                logger.error(f"  Error processing {entry['case_number']}: {e}")
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield judgments from recent years."""
        since_year = since.year
        entries = self._get_case_entries()
        recent = []
        for e in entries:
            if e.get("date"):
                try:
                    yr = int(e["date"][:4])
                    if yr >= since_year:
                        recent.append(e)
                except (ValueError, IndexError):
                    pass
        logger.info(f"Found {len(recent)} entries since {since_year}")

        for entry in recent:
            pdf_urls = self._get_pdf_urls(entry["case_url"])
            if not pdf_urls:
                continue
            pdf_bytes = self._download_pdf(pdf_urls[0])
            if not pdf_bytes:
                continue
            text = self._extract_text(pdf_bytes, entry["case_number"])
            if not text:
                continue
            entry["_extracted_text"] = text
            entry["pdf_url"] = pdf_urls[0]
            yield entry

    def normalize(self, raw: dict) -> dict:
        """Transform raw judgment data into standard schema."""
        case_num = raw.get("case_number", "unknown")
        uid_slug = re.sub(r'[^a-z0-9]+', '-', case_num.lower()).strip('-')

        return {
            "_id": f"benelux-court-{uid_slug}",
            "_source": "INTL/BeneluxCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("case_name", ""),
            "text": raw.get("_extracted_text", ""),
            "date": raw.get("date"),
            "url": raw.get("case_url", ""),
            "case_number": case_num,
            "subject_code": raw.get("subject_code", ""),
            "pdf_url": raw.get("pdf_url", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = BeneluxCourtScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        entries = scraper._get_case_entries()
        for e in entries[:10]:
            print(f"  {e['case_number']}  {e['date']}  {e['case_name'][:60]}")
        print(f"\nTotal: {len(entries)} cases")
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
