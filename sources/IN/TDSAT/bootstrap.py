#!/usr/bin/env python3
"""
IN/TDSAT -- Telecom Disputes Settlement and Appellate Tribunal (India)

Fetches final judgments from tdsat.gov.in.

Strategy:
  - POST date-wise search to judgment.php to get listing of all judgments
  - Parse HTML table for case number, parties, bench, date, and PDF URL
  - Download PDF and extract text via pdfplumber
  - Normalize into standard schema

Data:
  - ~1,500 final judgments from 2001–present
  - Telecom, broadcasting, AERA (airports), and cyber disputes
  - Full text in PDF (text-based, not scanned)
  - No authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch from last 180 days
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import html as html_mod
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional

import requests

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

try:
    from pypdf import PdfReader
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IN.TDSAT")

BASE_URL = "https://tdsat.gov.in"
JUDGMENT_URL = f"{BASE_URL}/Delhi/services/judgment.php"


class TDSATScraper:
    """Scraper for IN/TDSAT -- Telecom Disputes Settlement and Appellate Tribunal."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research)",
            "Accept": "text/html, */*",
        })

    def _search_judgments(self, from_date: str, to_date: str) -> list:
        """Search for judgments by date range. Dates in DD/MM/YYYY format."""
        # Establish session
        self.session.get(JUDGMENT_URL, timeout=30)

        data = {
            "from_date1": from_date,
            "to_date1": to_date,
            "frm3": "",
            "submit11": "Go",
        }
        try:
            resp = self.session.post(JUDGMENT_URL, data=data, timeout=120)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error("Failed to search judgments (%s to %s): %s", from_date, to_date, e)
            return []

        html = resp.text
        if "No Record Found" in html:
            logger.info("No records found for %s to %s", from_date, to_date)
            return []

        entries = []
        rows = re.findall(r"<tr>(.*?)</tr>", html, re.S)
        for row in rows:
            tds = re.findall(r"<td[^>]*>(.*?)</td>", row, re.S)
            if len(tds) < 5:
                continue

            serial = re.sub(r"<[^>]+>", "", tds[0]).strip()
            if not serial.isdigit():
                continue

            case_no = re.sub(r"<[^>]+>", " ", tds[1]).strip()
            case_no = re.sub(r"\s+", " ", case_no)

            bench = re.sub(r"<[^>]+>", " ", tds[2]).strip()
            bench = re.sub(r"\s+", " ", bench)

            # Party details: petitioner VS respondent
            party_html = tds[3]
            parties_raw = re.sub(r"<[^>]+>", " ", party_html).strip()
            parties_raw = html_mod.unescape(parties_raw)
            parties_raw = re.sub(r"\s+", " ", parties_raw)

            date_str = re.sub(r"<[^>]+>", "", tds[4]).strip()

            # PDF link
            pdf_match = re.search(r'href="([^"]*\.pdf)"', row, re.I)
            if not pdf_match:
                pdf_match = re.search(r"href='([^']*\.pdf)'", row, re.I)
            pdf_path = pdf_match.group(1) if pdf_match else ""

            entries.append({
                "serial": serial,
                "case_no": case_no,
                "bench": bench,
                "parties": parties_raw,
                "date_str": date_str,
                "pdf_path": pdf_path,
            })

        logger.info("Found %d judgments for %s to %s", len(entries), from_date, to_date)
        return entries

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse DD-MM-YYYY to ISO 8601."""
        if not date_str:
            return None
        for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _extract_pdf_text(self, pdf_content: bytes) -> Optional[str]:
        """Extract text from PDF bytes."""
        if HAS_PDFPLUMBER:
            try:
                pdf = pdfplumber.open(io.BytesIO(pdf_content))
                pages = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages.append(text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass
                pdf.close()
                full_text = "\n\n".join(pages)
                if len(full_text) > 100:
                    return full_text
            except Exception as e:
                logger.warning("pdfplumber extraction failed: %s", e)

        if HAS_PYPDF:
            try:
                reader = PdfReader(io.BytesIO(pdf_content))
                pages = []
                for page in reader.pages:
                    text = page.extract_text()
                    if text:
                        pages.append(text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass
                full_text = "\n\n".join(pages)
                if len(full_text) > 100:
                    return full_text
            except Exception as e:
                logger.warning("pypdf extraction failed: %s", e)

        return None

    def _download_and_extract(self, pdf_path: str) -> Optional[str]:
        """Download a PDF and extract its text."""
        if not pdf_path:
            return None

        url = pdf_path if pdf_path.startswith("http") else f"{BASE_URL}{pdf_path}"
        try:
            resp = self.session.get(url, timeout=120)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning("Failed to download PDF %s: %s", url, e)
            return None

        if len(resp.content) < 500:
            logger.warning("PDF too small (%d bytes): %s", len(resp.content), url)
            return None

        return self._extract_pdf_text(resp.content)

    def fetch_all(self) -> Generator:
        """Yield all TDSAT judgment entries by searching full date range."""
        entries = self._search_judgments("01/01/2000", "31/12/2030")
        for entry in entries:
            yield entry

    def fetch_updates(self, since: datetime) -> Generator:
        """Yield judgments from a recent date range."""
        from_str = since.strftime("%d/%m/%Y")
        to_str = datetime.now().strftime("%d/%m/%Y")
        entries = self._search_judgments(from_str, to_str)
        for entry in entries:
            yield entry

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw entry into standard schema."""
        case_no = raw.get("case_no", "").strip()
        pdf_path = raw.get("pdf_path", "")

        # Build unique ID from case number
        doc_id = re.sub(r"[^A-Za-z0-9]+", "-", case_no).strip("-")
        if not doc_id:
            doc_id = f"TDSAT-{raw.get('serial', 'unknown')}"

        # Extract text from PDF
        text = self._download_and_extract(pdf_path)
        if not text:
            logger.warning("No text extracted for %s", doc_id)
            return None

        # Parse date
        date = self._parse_date(raw.get("date_str", ""))

        # Build title from parties
        parties = raw.get("parties", "")
        title = f"{case_no}: {parties}" if parties else case_no

        pdf_url = pdf_path if pdf_path.startswith("http") else f"{BASE_URL}{pdf_path}"

        return {
            "_id": doc_id,
            "_source": "IN/TDSAT",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date or "",
            "url": pdf_url,
            "case_no": case_no,
            "bench": raw.get("bench", ""),
            "parties": parties,
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import argparse

    if not HAS_PDFPLUMBER and not HAS_PYPDF:
        logger.error("No PDF library available. Install pdfplumber or pypdf.")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="IN/TDSAT bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Full initial pull")
    boot.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")

    upd = sub.add_parser("update", help="Fetch recent judgments")
    upd.add_argument("--days", type=int, default=180, help="Look back N days (default 180)")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = TDSATScraper()

    if args.command == "test":
        logger.info("Testing TDSAT connectivity...")
        entries = scraper._search_judgments("01/01/2025", "31/07/2025")
        logger.info("Found %d judgments in 2025 (Jan-Jul)", len(entries))
        if entries:
            s = entries[0]
            logger.info("Sample: %s | %s | %s", s["case_no"], s["date_str"], s["pdf_path"])
        logger.info("PDF library: %s", "pdfplumber" if HAS_PDFPLUMBER else ("pypdf" if HAS_PYPDF else "NONE"))
        logger.info("Test PASSED")

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        skipped = 0
        limit = 15 if args.sample else 999999

        for raw in scraper.fetch_all():
            rec = scraper.normalize(raw)
            if rec:
                count += 1
                out_path = sample_dir / f"{rec['_id']}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(rec, f, ensure_ascii=False, indent=2)
                logger.info("[%d] Saved %s (%d chars text)", count, rec["_id"],
                            len(rec.get("text", "")))
                if count >= limit:
                    break
            else:
                skipped += 1
            time.sleep(1.5)

        logger.info("Bootstrap complete: %d records saved, %d skipped", count, skipped)

    elif args.command == "update":
        since = datetime.now(timezone.utc) - timedelta(days=args.days)
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0

        for raw in scraper.fetch_updates(since):
            rec = scraper.normalize(raw)
            if rec:
                count += 1
                out_path = sample_dir / f"{rec['_id']}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(rec, f, ensure_ascii=False, indent=2)
                logger.info("[%d] Saved %s", count, rec["_id"])
            time.sleep(1.5)

        logger.info("Update complete: %d records saved", count)

    else:
        parser.print_help()
