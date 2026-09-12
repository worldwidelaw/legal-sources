#!/usr/bin/env python3
"""
IN/APTEL -- Appellate Tribunal for Electricity

Fetches judgments and orders from the APTEL website.

Strategy:
  - Scrape HTML table from /en/old-judgement-data?field_judge_year_value=YYYY
  - Extract case metadata (serial, case number, cause title, bench, date)
  - Download PDFs and extract full text using pdfplumber
  - Normalize into standard schema

Data:
  - ~3,000 judgments/orders from 2008-present
  - All judgments are PDFs with selectable text
  - No authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch orders from last 90 days
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional
from html import unescape

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IN.APTEL")

BASE_URL = "https://aptel.gov.in"
YEARS = list(range(2008, 2027))


class APTELScraper(BaseScraper):
    """
    Scraper for IN/APTEL -- Appellate Tribunal for Electricity.
    Country: IN
    URL: https://aptel.gov.in/
    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        self.session.verify = False  # aptel.gov.in has SSL cert issues

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse Indian date format (D.MM.YYYY or DD.MM.YYYY) to ISO 8601."""
        if not date_str:
            return None
        date_str = unescape(date_str).strip()
        # Extract just the first date (before "Uploaded On" or separators)
        match = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_str)
        if not match:
            return None
        try:
            day, month, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
            dt = datetime(year, month, day)
            return dt.strftime("%Y-%m-%d")
        except (ValueError, OverflowError):
            return None

    def _scrape_year(self, year: int) -> list:
        """Scrape all judgment entries for a given year."""
        url = f"{BASE_URL}/en/old-judgement-data"
        params = {"field_judge_year_value": str(year)}

        try:
            resp = self.session.get(url, params=params, timeout=60)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error("Failed to fetch year %d: %s", year, e)
            return []

        html = resp.text
        entries = []

        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.S)
        for row in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.S)
            if len(cells) < 5:
                continue

            # Column 0: serial number
            serial_text = re.sub(r'<[^>]+>', '', cells[0]).strip()
            if not re.match(r'^\d{1,5}$', serial_text):
                continue
            serial = int(serial_text)

            # Column 1: case number + PDF link
            case_cell = cells[1]
            pdf_links = re.findall(r'href="([^"]*\.pdf[^"]*)"', case_cell, re.I)
            if not pdf_links:
                # Try relative links too
                pdf_links = re.findall(r'href="(/sites/default/files/[^"]+)"', case_cell, re.I)
            pdf_url = None
            if pdf_links:
                pdf_url = pdf_links[0]
                if pdf_url.startswith('/'):
                    pdf_url = BASE_URL + pdf_url
                elif not pdf_url.startswith('http'):
                    pdf_url = BASE_URL + '/' + pdf_url

            case_no = re.sub(r'<[^>]+>', '', case_cell).strip()
            case_no = re.sub(r'\s+', ' ', case_no)

            # Column 2: cause title (parties)
            cause_title = re.sub(r'<[^>]+>', '', cells[2]).strip()
            cause_title = unescape(cause_title)
            cause_title = re.sub(r'\s+', ' ', cause_title)

            # Column 3: bench
            bench = re.sub(r'<[^>]+>', '', cells[3]).strip()
            bench = unescape(bench)
            bench = re.sub(r'\s+', ' ', bench)

            # Column 4: date
            date_text = re.sub(r'<[^>]+>', '', cells[4]).strip()
            decision_date = self._parse_date(date_text)

            entries.append({
                "serial": serial,
                "case_no": case_no,
                "cause_title": cause_title,
                "bench": bench,
                "decision_date": decision_date,
                "date_raw": date_text,
                "pdf_url": pdf_url,
                "year": year,
            })

        logger.info("Year %d: %d entries found", year, len(entries))
        return entries

    def _download_pdf_text(self, pdf_url: str, source_id: str) -> Optional[str]:
        """Download a PDF and extract text."""
        if not pdf_url:
            return None

        try:
            text = extract_pdf_markdown(
                source="IN/APTEL",
                source_id=source_id,
                pdf_url=pdf_url,
                table="case_law",
            )
            if text and len(text.strip()) > 100:
                return text.strip()
        except Exception as e:
            logger.debug("extract_pdf_markdown failed for %s: %s", source_id, e)

        # Fallback: direct pdfplumber extraction
        try:
            import pdfplumber
            resp = self.session.get(pdf_url, timeout=120)
            resp.raise_for_status()
            pdf_bytes = resp.content

            text_parts = []
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass

            full_text = "\n\n".join(text_parts)
            if len(full_text.strip()) > 100:
                return full_text.strip()
        except Exception as e:
            logger.warning("PDF extraction failed for %s: %s", pdf_url, e)

        return None

    def fetch_all(self) -> Generator:
        """Yield all judgment records across all years."""
        for year in YEARS:
            entries = self._scrape_year(year)
            for entry in entries:
                yield entry
            if entries:
                time.sleep(1)

    def fetch_updates(self, since: datetime) -> Generator:
        """Yield judgment records from recent years."""
        current_year = datetime.now().year
        years_to_check = [current_year - 1, current_year]
        if current_year + 1 <= 2026:
            years_to_check.append(current_year + 1)

        for year in years_to_check:
            entries = self._scrape_year(year)
            for entry in entries:
                if entry.get("decision_date"):
                    try:
                        entry_date = datetime.strptime(entry["decision_date"], "%Y-%m-%d")
                        if entry_date.replace(tzinfo=timezone.utc) >= since:
                            yield entry
                    except ValueError:
                        yield entry
                else:
                    yield entry
            time.sleep(1)

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw entry into standard schema."""
        serial = raw.get("serial")
        case_no = raw.get("case_no", "")
        pdf_url = raw.get("pdf_url")

        doc_id = f"APTEL-{serial}" if serial else f"APTEL-{hash(case_no) % 100000}"

        # Download and extract PDF text
        text = self._download_pdf_text(pdf_url, doc_id)
        if not text:
            logger.warning("No text extracted for %s (%s)", doc_id, case_no[:60])
            return None

        cause_title = raw.get("cause_title", "")
        # Clean HTML entities from case_no
        case_no = unescape(case_no).replace('\xa0', ' ').strip()
        title = f"{case_no}"
        if cause_title:
            # Build a readable title
            parties = unescape(cause_title).replace('\xa0', ' ')
            parties = parties.replace("VERSUS", " v. ").replace("Versus", " v. ")
            parties = re.sub(r'\s+', ' ', parties).strip()
            if len(parties) > 120:
                parties = parties[:117] + "..."
            title = f"{case_no} — {parties}"

        url = pdf_url or f"{BASE_URL}/en/old-judgement-data"

        return {
            "_id": doc_id,
            "_source": "IN/APTEL",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("decision_date"),
            "url": url,
            "case_no": case_no,
            "cause_title": cause_title,
            "bench": raw.get("bench", ""),
            "year": raw.get("year"),
        }


# ----- CLI -----
if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import argparse
    import warnings
    warnings.filterwarnings("ignore", message="Unverified HTTPS request")

    parser = argparse.ArgumentParser(description="IN/APTEL bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Full initial pull")
    boot.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")

    upd = sub.add_parser("update", help="Fetch recent orders")
    upd.add_argument("--days", type=int, default=90, help="Look back N days (default 90)")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = APTELScraper()

    if args.command == "test":
        logger.info("Testing APTEL connectivity...")
        entries = scraper._scrape_year(2024)
        logger.info("Year 2024: %d entries found", len(entries))
        if entries:
            sample = entries[0]
            logger.info("Sample entry: serial=%s, case=%s, date=%s",
                        sample["serial"], sample["case_no"][:60], sample["decision_date"])
            if sample.get("pdf_url"):
                logger.info("PDF URL: %s", sample["pdf_url"][:100])
        logger.info("Test PASSED")

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
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

        logger.info("Bootstrap complete: %d records saved to %s", count, sample_dir)

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

        logger.info("Update complete: %d records saved", count)

    else:
        parser.print_help()
