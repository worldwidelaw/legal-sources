#!/usr/bin/env python3
"""
IN/CAT -- Central Administrative Tribunal

Fetches final orders/judgments from the CAT CIS portal (cis.cgat.gov.in).

Strategy:
  - Query fiorder_detail.php with date ranges per bench
  - Parse HTML table for case number, parties, date, and PDF link
  - Download PDFs and extract full text using pdfplumber
  - Normalize into standard schema

Data:
  - 19 benches across India
  - ~800,000+ cases disposed since 1985
  - Final orders available as PDFs
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
logger = logging.getLogger("legal-data-hunter.IN.CAT")

BASE_URL = "https://cis.cgat.gov.in/catlive"

# 19 CAT benches: (numeric code, display name)
BENCHES = [
    (100, "Delhi"),
    (120, "Ahmedabad"),
    (330, "Allahabad"),
    (103, "Bangalore"),
    (60, "Chandigarh"),
    (310, "Chennai"),
    (260, "Cuttack"),
    (180, "Ernakulam"),
    (40, "Guwahati"),
    (21, "Hyderabad"),
    (200, "Jabalpur"),
    (291, "Jaipur"),
    (117, "Jammu"),
    (111, "Jodhpur"),
    (350, "Kolkata"),
    (332, "Lucknow"),
    (210, "Mumbai"),
    (116, "Patna"),
    (119, "Srinagar"),
]

# Start from 2015 — earlier records may not have PDFs online
START_YEAR = 2015


class CATScraper(BaseScraper):
    """
    Scraper for IN/CAT -- Central Administrative Tribunal.
    Country: IN
    URL: https://cis.cgat.gov.in/catlive/
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
            "Referer": f"{BASE_URL}/final_order.php",
        })
        self.session.verify = False

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse dd/mm/yyyy date to ISO 8601."""
        if not date_str:
            return None
        date_str = unescape(date_str).strip()
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y"):
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _fetch_final_orders(self, bench_code: int, from_date: str, to_date: str) -> list:
        """
        Fetch final orders for a bench within a date range.
        Dates in dd/mm/yyyy format.
        Returns list of raw entries.
        """
        url = f"{BASE_URL}/fiorder_detail.php"
        params = {
            "benchCode3": str(bench_code),
            "from_date": from_date,
            "to_date": to_date,
            "id": "partynamewise",
        }

        try:
            resp = self.session.get(url, params=params, timeout=60)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error("Failed to fetch orders for bench %d (%s - %s): %s",
                         bench_code, from_date, to_date, e)
            return []

        html = resp.text
        entries = []

        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.S)
        for row in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.S)
            if len(cells) < 4:
                continue

            # Cell 0: serial number
            serial_text = re.sub(r'<[^>]+>', '', cells[0]).strip()
            if not re.match(r'^\d{1,6}$', serial_text):
                continue
            serial = int(serial_text)

            # Cell 1: case number (e.g., O.A./110/2024)
            case_no = re.sub(r'<[^>]+>', '', cells[1]).strip()
            case_no = re.sub(r'\s+', ' ', case_no)

            # Cell 2: party details (HTML with red/blue colors)
            party_html = cells[2]
            # Extract petitioner and respondent
            party_text = re.sub(r'<[^>]+>', ' ', party_html)
            party_text = unescape(party_text).strip()
            party_text = re.sub(r'\s+', ' ', party_text)

            # Try to split on VS
            parts = re.split(r'\bVS\b', party_text, maxsplit=1)
            petitioner = parts[0].strip() if len(parts) > 0 else ""
            respondent = parts[1].strip() if len(parts) > 1 else ""

            # Cell 3: date
            date_text = re.sub(r'<[^>]+>', '', cells[3]).strip()
            decision_date = self._parse_date(date_text)

            # PDF link (in cell 4 if present, or elsewhere in row)
            pdf_link = None
            pdf_match = re.search(r'href="([^"]*pdf/judge\.php\?file=[^"]+)"', row, re.I)
            if pdf_match:
                pdf_link = pdf_match.group(1)
                if pdf_link.startswith('./'):
                    pdf_link = f"{BASE_URL}/{pdf_link[2:]}"
                elif not pdf_link.startswith('http'):
                    pdf_link = f"{BASE_URL}/{pdf_link}"

            entries.append({
                "serial": serial,
                "case_no": case_no,
                "petitioner": petitioner,
                "respondent": respondent,
                "party_text": party_text,
                "decision_date": decision_date,
                "date_raw": date_text,
                "pdf_url": pdf_link,
                "bench_code": bench_code,
            })

        logger.info("Bench %d (%s - %s): %d entries", bench_code, from_date, to_date, len(entries))
        return entries

    def _download_pdf_text(self, pdf_url: str, source_id: str) -> Optional[str]:
        """Download a PDF and extract text."""
        if not pdf_url:
            return None

        try:
            text = extract_pdf_markdown(
                source="IN/CAT",
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
            if len(resp.content) == 0:
                logger.warning("Empty PDF for %s", source_id)
                return None
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
            logger.warning("PDF extraction failed for %s: %s", source_id, e)

        return None

    def _month_ranges(self, start_year: int, end_year: int):
        """Generate (from_date, to_date) pairs as dd/mm/yyyy for each month."""
        import calendar
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                if year == end_year and month > datetime.now().month:
                    break
                last_day = calendar.monthrange(year, month)[1]
                from_date = f"01/{month:02d}/{year}"
                to_date = f"{last_day:02d}/{month:02d}/{year}"
                yield from_date, to_date

    def fetch_all(self) -> Generator:
        """Yield all final order records across all benches and years."""
        end_year = datetime.now().year
        for bench_code, bench_name in BENCHES:
            logger.info("Processing bench: %s (code %d)", bench_name, bench_code)
            for from_date, to_date in self._month_ranges(START_YEAR, end_year):
                entries = self._fetch_final_orders(bench_code, from_date, to_date)
                for entry in entries:
                    entry["bench_name"] = bench_name
                    yield entry
                time.sleep(1.5)

    def fetch_updates(self, since: datetime) -> Generator:
        """Yield final order records from recent months."""
        now = datetime.now()
        for bench_code, bench_name in BENCHES:
            # Go back from since date
            check_date = since
            while check_date <= now:
                from_date = f"01/{check_date.month:02d}/{check_date.year}"
                import calendar
                last_day = calendar.monthrange(check_date.year, check_date.month)[1]
                to_date = f"{last_day:02d}/{check_date.month:02d}/{check_date.year}"

                entries = self._fetch_final_orders(bench_code, from_date, to_date)
                for entry in entries:
                    entry["bench_name"] = bench_name
                    if entry.get("decision_date"):
                        try:
                            entry_dt = datetime.strptime(entry["decision_date"], "%Y-%m-%d")
                            if entry_dt.replace(tzinfo=timezone.utc) >= since:
                                yield entry
                        except ValueError:
                            yield entry
                    else:
                        yield entry

                # Move to next month
                if check_date.month == 12:
                    check_date = check_date.replace(year=check_date.year + 1, month=1)
                else:
                    check_date = check_date.replace(month=check_date.month + 1)
                time.sleep(1.5)

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw entry into standard schema."""
        case_no = raw.get("case_no", "")
        bench_name = raw.get("bench_name", "")
        bench_code = raw.get("bench_code", 0)
        decision_date = raw.get("decision_date", "")

        # Build unique ID from bench + case number
        safe_case = re.sub(r'[^a-zA-Z0-9]', '-', case_no).strip('-')
        doc_id = f"CAT-{bench_code}-{safe_case}"

        # Download and extract PDF text
        pdf_url = raw.get("pdf_url")
        text = self._download_pdf_text(pdf_url, doc_id)
        if not text:
            logger.warning("No text extracted for %s (%s)", doc_id, case_no)
            return None

        petitioner = raw.get("petitioner", "")
        respondent = raw.get("respondent", "")

        # Build title
        title = case_no
        if petitioner and respondent:
            pet_short = petitioner[:60] + "..." if len(petitioner) > 60 else petitioner
            res_short = respondent[:60] + "..." if len(respondent) > 60 else respondent
            title = f"{case_no} — {pet_short} v. {res_short}"
        elif petitioner:
            title = f"{case_no} — {petitioner[:80]}"

        url = pdf_url or f"{BASE_URL}/final_order.php"

        return {
            "_id": doc_id,
            "_source": "IN/CAT",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": decision_date,
            "url": url,
            "case_no": case_no,
            "petitioner": petitioner,
            "respondent": respondent,
            "bench": bench_name,
            "bench_code": bench_code,
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

    parser = argparse.ArgumentParser(description="IN/CAT bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Full initial pull")
    boot.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")

    upd = sub.add_parser("update", help="Fetch recent orders")
    upd.add_argument("--days", type=int, default=90, help="Look back N days (default 90)")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = CATScraper()

    if args.command == "test":
        logger.info("Testing CAT CIS connectivity...")
        entries = scraper._fetch_final_orders(100, "01/04/2026", "30/04/2026")
        logger.info("Delhi April 2026: %d entries found", len(entries))
        if entries:
            sample = entries[0]
            logger.info("Sample: serial=%s, case=%s, date=%s",
                        sample["serial"], sample["case_no"], sample["decision_date"])
            if sample.get("pdf_url"):
                logger.info("PDF URL: %s", sample["pdf_url"][:120])
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
