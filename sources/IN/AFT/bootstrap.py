#!/usr/bin/env python3
"""
IN/AFT -- Armed Forces Tribunal

Fetches judgments from the Armed Forces Tribunal (AFT) of India.

Strategy:
  - Regional benches: JSON API at aftpb.org/aft/views/fetch_judgements.php
    Returns all records for a bench in a single JSON response.
  - Principal bench: Paginated HTML scraping at aftpb.org/aft/
    Parameters: year, case_type, page, record_per_page
  - Full text: Download PDFs from aftdelhi.nic.in, extract via pdfplumber

Data:
  - Military service disputes: discharge, pension, court-martial appeals
  - Principal bench (New Delhi) + 6 regional benches
  - ~56K judgments from 2009-present
  - No authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
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
from urllib.parse import quote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IN.AFT")

AFTPB_BASE = "http://aftpb.org/aft"
PDF_BASE = "https://aftdelhi.nic.in"

REGIONAL_BENCHES = ["Chennai", "Chandigarh", "Kochi", "Mumbai", "Srinagar", "Jabalpur"]

PB_CASE_TYPES = ["OA", "TA", "AT", "CA", "RA", "MA", "WP(C)", "MA (Ex)"]
PB_YEARS = list(range(2009, datetime.now().year + 1))


class AFTScraper(BaseScraper):
    """
    Scraper for IN/AFT -- Armed Forces Tribunal.
    Country: IN
    URL: http://aftpb.org/aft/
    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; legal research)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })

    # ---- Regional Bench (JSON API) ----

    def _fetch_regional_bench(self, bench: str) -> list:
        """Fetch all judgments for a regional bench via JSON API."""
        url = f"{AFTPB_BASE}/views/fetch_judgements.php?bench={quote(bench)}"
        try:
            resp = self.session.get(url, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            logger.info("Regional bench %s: %d records", bench, len(data))
            return data
        except Exception as e:
            logger.error("Failed to fetch regional bench %s: %s", bench, e)
            return []

    def _normalize_rb_entry(self, entry: dict) -> dict:
        """Convert a regional bench JSON entry to a raw record dict."""
        bench = entry.get("bench", "").strip()
        case_no = entry.get("case_no", "").strip()
        year = entry.get("year", "")
        month = entry.get("mon", "")

        # Build PDF URL
        path = entry.get("path", "")
        pdf_url = f"{PDF_BASE}/{path}" if path else None

        return {
            "source_type": "regional",
            "bench": bench,
            "case_no": case_no,
            "year": year,
            "month": month,
            "judge1": entry.get("judge1", "").strip(),
            "judge2": entry.get("judge2", "").strip(),
            "case_title": entry.get("case_title", "").strip(),
            "pdf_url": pdf_url,
            "rb_id": entry.get("id"),
        }

    # ---- Principal Bench (HTML scraping) ----

    def _fetch_pb_page(self, page: int, year: str = "", case_type: str = "",
                       record_per_page: int = 100) -> list:
        """Fetch one page of principal bench judgments."""
        params = {
            "page": page,
            "record_per_page": record_per_page,
        }
        if year:
            params["year"] = year
        if case_type:
            params["case_type"] = case_type

        try:
            resp = self.session.get(f"{AFTPB_BASE}/", params=params, timeout=60)
            resp.raise_for_status()
            return self._parse_pb_html(resp.text)
        except Exception as e:
            logger.error("Failed to fetch PB page %d: %s", page, e)
            return []

    def _parse_pb_html(self, html: str) -> list:
        """Parse the principal bench HTML table into raw records."""
        entries = []
        rows = re.findall(r'<tr>\s*<td[^>]*>(\d+)</td>(.*?)</tr>', html, re.S)

        for serial_str, row_html in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.S)
            if len(cells) < 15:
                continue

            # Extract details link and judgment PDF link
            detail_match = re.search(
                r'href="(http://aftpb\.org/aft/views/judgement-details\.php\?id=(\d+))"',
                cells[0], re.I
            )
            detail_id = detail_match.group(2) if detail_match else None

            pdf_match = re.search(
                r'href="(https://aftdelhi\.nic\.in/[^"]+\.pdf)"',
                cells[1], re.I
            )
            pdf_url = pdf_match.group(1) if pdf_match else None

            def strip_html(s):
                return re.sub(r'<[^>]+>', '', s).strip()

            regno = strip_html(cells[0]) if not detail_match else ""
            if detail_match:
                # RegNo is in cells[1] sometimes, but let's extract from context
                regno_match = re.search(r'>([A-Z()]+\s+\d+/\d+)<', cells[1] + cells[0], re.I)
                if not regno_match:
                    # Try the third cell (index 2 in original)
                    pass

            # The table structure from the HTML:
            # cells[0] = Details button (with link)
            # cells[1] = RegNo
            # cells[2] = Judgement PDF link
            # cells[3] = Case Type
            # cells[4] = File number
            # cells[5] = Year
            # cells[6] = Subject
            # cells[7] = Petitioner
            # cells[8] = Respondent
            # cells[9] = Associated
            # cells[10] = Department
            # cells[11] = Petitioner Advocate
            # cells[12] = Court No
            # cells[13] = GNO
            # cells[14] = DOD (date of disposal)
            # cells[15] = MOD (mode of disposal)

            regno = strip_html(cells[1])
            case_type = strip_html(cells[3])
            file_no = strip_html(cells[4])
            year = strip_html(cells[5])
            subject = strip_html(cells[6])
            petitioner = strip_html(cells[7])
            respondent = strip_html(cells[8])
            associated = strip_html(cells[9])
            department = strip_html(cells[10])
            p_advocate = strip_html(cells[11])
            court_no = strip_html(cells[12])
            gno = strip_html(cells[13])
            dod = strip_html(cells[14])
            mod = strip_html(cells[15]) if len(cells) > 15 else ""

            entries.append({
                "source_type": "principal",
                "bench": "Principal Bench, New Delhi",
                "detail_id": detail_id,
                "case_no": regno,
                "case_type": case_type,
                "file_no": file_no,
                "year": year,
                "subject": subject,
                "petitioner": petitioner,
                "respondent": respondent,
                "associated": associated,
                "department": department,
                "p_advocate": p_advocate,
                "court_no": court_no,
                "gno": gno,
                "date_of_disposal": dod,
                "mode_of_disposal": mod,
                "pdf_url": pdf_url,
            })

        return entries

    def _get_pb_total_pages(self, record_per_page: int = 100) -> int:
        """Get total number of pages for principal bench."""
        try:
            resp = self.session.get(f"{AFTPB_BASE}/",
                                   params={"record_per_page": record_per_page, "page": 1},
                                   timeout=60)
            resp.raise_for_status()
            # Find the last page link
            match = re.search(r'page=(\d+)" class="page-link">\d+</a></li>\s*<li class="page-item"><a href="[^"]*" class="page-link">Next', resp.text)
            if match:
                return int(match.group(1))
            # Alternative: find max page number
            pages = re.findall(r'page=(\d+)', resp.text)
            if pages:
                return max(int(p) for p in pages)
        except Exception as e:
            logger.error("Failed to get PB page count: %s", e)
        return 1

    # ---- PDF extraction ----

    def _download_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download a PDF and extract text via pdfplumber."""
        if not pdf_url:
            return None

        try:
            import pdfplumber
            resp = self.session.get(pdf_url, timeout=120)
            resp.raise_for_status()

            if resp.content[:4] != b'%PDF':
                logger.warning("Non-PDF response from %s", pdf_url)
                return None

            text_parts = []
            with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
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
        except requests.exceptions.ConnectionError:
            logger.warning("Connection failed for PDF (aftdelhi.nic.in may be unreachable): %s", pdf_url)
        except requests.exceptions.Timeout:
            logger.warning("PDF download timed out: %s", pdf_url)
        except Exception as e:
            logger.warning("PDF extraction failed for %s: %s", pdf_url, e)

        return None

    # ---- Date parsing ----

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse dd-mm-yyyy or dd/mm/yyyy to ISO 8601."""
        if not date_str:
            return None
        date_str = date_str.strip()
        for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%d.%m.%Y"):
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    # ---- Main fetch methods ----

    def fetch_all(self) -> Generator:
        """Yield all judgment entries from regional and principal benches."""
        # Regional benches first (JSON API — fast and complete)
        for bench in REGIONAL_BENCHES:
            entries = self._fetch_regional_bench(bench)
            for entry in entries:
                yield self._normalize_rb_entry(entry)
            time.sleep(1.5)

        # Principal bench (paginated HTML)
        total_pages = self._get_pb_total_pages(record_per_page=100)
        logger.info("Principal bench: %d pages to fetch", total_pages)

        for page in range(1, total_pages + 1):
            entries = self._fetch_pb_page(page, record_per_page=100)
            if entries:
                logger.info("PB page %d/%d: %d records", page, total_pages, len(entries))
            for entry in entries:
                yield entry
            time.sleep(1.5)

    def fetch_updates(self, since: datetime) -> Generator:
        """Yield recent records (regional benches + recent PB pages)."""
        # Regional benches — re-fetch all (API returns everything)
        for bench in REGIONAL_BENCHES:
            entries = self._fetch_regional_bench(bench)
            for entry in entries:
                raw = self._normalize_rb_entry(entry)
                yield raw
            time.sleep(1.5)

        # Principal bench — fetch recent year
        current_year = str(datetime.now().year)
        page = 1
        while True:
            entries = self._fetch_pb_page(page, year=current_year, record_per_page=100)
            if not entries:
                break
            for entry in entries:
                yield entry
            page += 1
            time.sleep(1.5)

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw entry into standard schema."""
        source_type = raw.get("source_type", "")
        case_no = raw.get("case_no", "")
        bench = raw.get("bench", "")

        # Build unique ID
        if source_type == "regional":
            rb_id = raw.get("rb_id", "")
            bench_code = bench[:3].upper() if bench else "RB"
            doc_id = f"AFT-{bench_code}-{rb_id}" if rb_id else f"AFT-{bench_code}-{case_no.replace(' ', '').replace('/', '-')}"
        else:
            detail_id = raw.get("detail_id", "")
            if detail_id:
                doc_id = f"AFT-PB-{detail_id}"
            else:
                safe_case = case_no.replace(" ", "").replace("/", "-")
                doc_id = f"AFT-PB-{safe_case}" if safe_case else f"AFT-PB-{raw.get('file_no', 'unknown')}"

        # Download and extract PDF text
        pdf_url = raw.get("pdf_url")
        text = self._download_pdf_text(pdf_url)
        if not text:
            logger.warning("No text extracted for %s (pdf=%s)", doc_id, pdf_url or "none")
            return None

        # Build title
        if source_type == "principal":
            petitioner = raw.get("petitioner", "")
            respondent = raw.get("respondent", "")
            if petitioner and respondent:
                title = f"{case_no} — {petitioner[:60]} v. {respondent[:60]}"
            else:
                title = case_no or doc_id
        else:
            case_title = raw.get("case_title", "")
            if case_title:
                title = f"{case_no} — {case_title[:80]}"
            else:
                title = case_no or doc_id

        # Date
        if source_type == "principal":
            date = self._parse_date(raw.get("date_of_disposal", ""))
        else:
            # Regional bench entries have year and month but no exact date
            year = raw.get("year", "")
            month = raw.get("month", "")
            if year and month:
                try:
                    date = f"{year}-{int(month):02d}-01"
                except (ValueError, TypeError):
                    date = f"{year}-01-01" if year else None
            elif year:
                date = f"{year}-01-01"
            else:
                date = None

        # URL
        url = pdf_url or f"{AFTPB_BASE}/"

        result = {
            "_id": doc_id,
            "_source": "IN/AFT",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "case_no": case_no,
            "bench": bench,
        }

        # Add principal bench specific fields
        if source_type == "principal":
            result.update({
                "case_type": raw.get("case_type", ""),
                "subject": raw.get("subject", ""),
                "petitioner": raw.get("petitioner", ""),
                "respondent": raw.get("respondent", ""),
                "department": raw.get("department", ""),
                "mode_of_disposal": raw.get("mode_of_disposal", ""),
                "p_advocate": raw.get("p_advocate", ""),
            })
        else:
            result.update({
                "judge1": raw.get("judge1", ""),
                "judge2": raw.get("judge2", ""),
            })

        return result


# ----- CLI -----
if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import argparse

    parser = argparse.ArgumentParser(description="IN/AFT bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Full initial pull")
    boot.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")

    upd = sub.add_parser("update", help="Fetch recent records")
    upd.add_argument("--days", type=int, default=90, help="Look back N days (default 90)")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = AFTScraper()

    if args.command == "test":
        logger.info("Testing AFT connectivity...")
        # Test regional bench API
        data = scraper._fetch_regional_bench("Srinagar")
        logger.info("Srinagar bench: %d records", len(data))
        if data:
            sample = scraper._normalize_rb_entry(data[0])
            logger.info("Sample: case=%s, bench=%s, pdf=%s",
                        sample["case_no"], sample["bench"],
                        "yes" if sample["pdf_url"] else "no")

        # Test principal bench HTML
        entries = scraper._fetch_pb_page(1, record_per_page=10)
        logger.info("Principal bench page 1: %d records", len(entries))
        if entries:
            logger.info("PB sample: case=%s, petitioner=%s",
                        entries[0].get("case_no"), entries[0].get("petitioner", ""))

        # Test PDF download
        if data and data[0].get("path"):
            pdf_url = f"{PDF_BASE}/{data[0]['path']}"
            logger.info("Testing PDF download from: %s", pdf_url)
            text = scraper._download_pdf_text(pdf_url)
            if text:
                logger.info("PDF text extracted: %d chars", len(text))
            else:
                logger.warning("PDF download failed (aftdelhi.nic.in may be unreachable from this IP)")

        logger.info("Test complete")

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        limit = 15 if args.sample else 999999
        skipped = 0

        for raw in scraper.fetch_all():
            rec = scraper.normalize(raw)
            if rec:
                count += 1
                safe_id = rec["_id"].replace("/", "-").replace(" ", "_")
                out_path = sample_dir / f"{safe_id}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(rec, f, ensure_ascii=False, indent=2)
                logger.info("[%d] %s — %d chars", count, rec["_id"], len(rec["text"]))
                if count >= limit:
                    break
            else:
                skipped += 1
                if args.sample and skipped > 50:
                    logger.warning("Too many PDF failures — aftdelhi.nic.in may be unreachable")
                    break
            time.sleep(1.5)

        logger.info("Done: %d records saved, %d skipped (no text)", count, skipped)

    elif args.command == "update":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        since = datetime.now(timezone.utc) - timedelta(days=args.days)
        count = 0

        for raw in scraper.fetch_updates(since):
            rec = scraper.normalize(raw)
            if rec:
                count += 1
                safe_id = rec["_id"].replace("/", "-").replace(" ", "_")
                out_path = sample_dir / f"{safe_id}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(rec, f, ensure_ascii=False, indent=2)
                logger.info("[%d] %s", count, rec["_id"])
            time.sleep(1.5)

        logger.info("Update done: %d records", count)
