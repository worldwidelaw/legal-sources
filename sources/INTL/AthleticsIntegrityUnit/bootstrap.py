#!/usr/bin/env python3
"""
INTL/AthleticsIntegrityUnit -- Athletics Integrity Unit (AIU) First Instance Decisions

Fetches first-instance disciplinary decisions from the Athletics Integrity Unit
(AIU), the independent body established by World Athletics that investigates and
prosecutes doping and non-doping (integrity / manipulation) violations in the
sport of athletics. The independent Disciplinary Tribunal hears first-instance
cases under the World Athletics Anti-Doping Rules and Integrity Code of Conduct.

Strategy:
  - The "First Instance Decisions" page is a single server-rendered HTML table
    (Date, Respondent, NAT, Violation, Outcome, Status). The Outcome cell of each
    row carries one or more <a> links to the born-digital, full-text decision PDFs
    hosted openly on www.athleticsintegrity.org/downloads/pdfs/disciplinary-process.
  - We parse every decision PDF link, recover the row metadata (date, respondent,
    nationality, violation, outcome), download each PDF, and extract full text via
    common/pdf_extract (pdfplumber/pypdf fallback).

The decisions are openly published (no login, no WAF) and reachable from any IP.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py test               # Print parsed listing entries
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
logger = logging.getLogger("legal-data-hunter.INTL.AthleticsIntegrityUnit")

LISTING_URL = "https://www.athleticsintegrity.org/disciplinary-process/first-instance-decisions"
MAX_PDF_BYTES = 50 * 1024 * 1024

# DD/MM/YYYY in the listing table.
DATE_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")


class AthleticsIntegrityUnitScraper(BaseScraper):
    """Scraper for AIU First Instance Decisions."""

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
        """Parse 'DD/MM/YYYY' to ISO 'YYYY-MM-DD'."""
        try:
            return datetime.strptime(date_str.strip(), "%d/%m/%Y").date().isoformat()
        except (ValueError, AttributeError):
            return None

    def _slug_from_pdf(self, pdf_url: str) -> str:
        stem = pdf_url.rsplit("/", 1)[-1]
        stem = re.sub(r"\.pdf$", "", stem, flags=re.IGNORECASE)
        slug = re.sub(r"[^a-z0-9]+", "-", stem.lower()).strip("-")
        return slug or "decision"

    def _get_entries(self) -> list[dict]:
        """Parse the First Instance Decisions table into structured entries."""
        resp = self.session.get(LISTING_URL, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")

        anchors = [
            a for a in soup.find_all("a", href=True)
            if ".pdf" in a["href"].lower()
            and "disciplinary-process" in a["href"].lower()
        ]

        entries = []
        seen = set()
        for a in anchors:
            pdf_url = a["href"].strip().replace("http://", "https://")
            if not pdf_url.lower().startswith("https://"):
                continue
            if pdf_url in seen:
                continue

            # Walk up to the enclosing <tr> and read its cells.
            date_iso = respondent = nat = violation = outcome = None
            tr = a.find_parent("tr")
            if tr is not None:
                cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
                if cells:
                    if DATE_RE.match(cells[0]):
                        date_iso = self._parse_date(cells[0])
                    if len(cells) > 1:
                        respondent = cells[1]
                    if len(cells) > 2:
                        nat = cells[2]
                    if len(cells) > 3:
                        violation = cells[3]
                    if len(cells) > 4:
                        outcome = cells[4]

            respondent = (respondent or "").strip()
            link_text = a.get_text(" ", strip=True)
            if respondent:
                title = f"AIU Decision — {respondent}"
                if violation:
                    title += f" ({violation})"
            else:
                title = link_text or f"AIU Decision {self._slug_from_pdf(pdf_url)}"

            seen.add(pdf_url)
            entries.append({
                "id_slug": self._slug_from_pdf(pdf_url),
                "date": date_iso,
                "respondent": respondent,
                "nationality": nat,
                "violation": violation,
                "outcome": outcome,
                "title": title,
                "pdf_url": pdf_url,
            })

        logger.info(f"Parsed {len(entries)} decision entries from listing page")
        return entries

    def _download_pdf(self, url: str) -> Optional[bytes]:
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
        text = extract_pdf_markdown(
            source="INTL/AthleticsIntegrityUnit",
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
        entries = self._get_entries()
        logger.info(f"Total entries to process: {len(entries)}")
        for i, entry in enumerate(entries):
            try:
                logger.info(
                    f"[{i+1}/{len(entries)}] {entry['id_slug']} - "
                    f"{entry['title'][:60]}..."
                )
                pdf_bytes = self._download_pdf(entry["pdf_url"])
                if not pdf_bytes:
                    continue
                text = self._extract_text(pdf_bytes, entry["id_slug"])
                if not text:
                    logger.warning(f"  Insufficient text for {entry['id_slug']}, skipping")
                    continue
                entry["_extracted_text"] = text
                yield entry
            except Exception as e:
                logger.error(f"  Error processing {entry['id_slug']}: {e}")
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        since_iso = since.date().isoformat()
        for entry in self.fetch_all():
            if not entry.get("date") or entry["date"] >= since_iso:
                yield entry

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"aiu-{raw.get('id_slug', 'decision')}",
            "_source": "INTL/AthleticsIntegrityUnit",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("_extracted_text", ""),
            "date": raw.get("date"),
            "url": LISTING_URL,
            "respondent": raw.get("respondent", ""),
            "nationality": raw.get("nationality", ""),
            "violation": raw.get("violation", ""),
            "outcome": raw.get("outcome", ""),
            "pdf_url": raw.get("pdf_url", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = AthleticsIntegrityUnitScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        entries = scraper._get_entries()
        for e in entries:
            print(f"  {e['date']}  {e['id_slug'][:40]:40}  {(e['respondent'] or '')[:30]}")
        print(f"\nTotal: {len(entries)} entries")
        sys.exit(0)

    if command in ("bootstrap", "bootstrap-fast"):
        result = scraper.bootstrap(sample_mode=sample, sample_size=10)
        print(json.dumps(result, indent=2, default=str))
    elif command == "update":
        result = scraper.update()
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
