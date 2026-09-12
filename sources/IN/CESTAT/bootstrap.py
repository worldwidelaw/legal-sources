#!/usr/bin/env python3
"""
IN/CESTAT -- Customs, Excise & Service Tax Appellate Tribunal

Fetches final orders from cestat.gov.in via their AJAX search API.

Strategy:
  - GET the final-order-status page to obtain CSRF token + session cookies
  - POST to /ajax/order-status-web with bench and date range params
  - Parse JSON response (DataTables format) for case metadata + PDF links
  - Download PDFs and extract full text using pdfplumber
  - Normalize into standard schema

Data:
  - 9 benches: Ahmedabad, Allahabad, Bangalore, Chandigarh, Chennai,
    Delhi, Hyderabad, Kolkata, Mumbai
  - Covers Customs, Excise, Service Tax, Antidumping, Central Sales Tax appeals
  - Final orders available as PDFs (digitally generated, text-extractable)
  - No authentication required (CAPTCHA is auto-filled / hidden)

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

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IN.CESTAT")

BASE_URL = "https://cestat.gov.in"
AJAX_URL = f"{BASE_URL}/ajax/order-status-web"
STATUS_URL = f"{BASE_URL}/final-order-status"

# Bench codes (from page select options) → display name
BENCHES = [
    (124438, "Ahmedabad"),
    (109120, "Allahabad"),
    (129525, "Bangalore"),
    (104044, "Chandigarh"),
    (133568, "Chennai"),
    (107079, "Delhi"),
    (136507, "Hyderabad"),
    (119315, "Kolkata"),
    (127482, "Mumbai"),
]

# Start from 2015 — earlier records may not have PDFs
START_YEAR = 2015


class CESTATScraper(BaseScraper):
    """
    Scraper for IN/CESTAT -- Customs, Excise & Service Tax Appellate Tribunal.
    Country: IN
    URL: https://cestat.gov.in/
    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research)",
            "Accept": "application/json, text/html, */*",
            "X-Requested-With": "XMLHttpRequest",
        })
        self._csrf_token = None

    def _get_csrf_token(self):
        """Fetch the final-order-status page to get a fresh CSRF token."""
        try:
            resp = self.session.get(STATUS_URL, timeout=30)
            resp.raise_for_status()
            match = re.search(
                r'name=["\']csrf_token["\'][^>]*value=["\']([^"\']+)["\']',
                resp.text,
            )
            if match:
                self._csrf_token = match.group(1)
                logger.info("Got CSRF token: %s...", self._csrf_token[:8])
            else:
                logger.warning("CSRF token not found in page")
        except requests.RequestException as e:
            logger.error("Failed to fetch CSRF token: %s", e)

    def _search_orders(self, bench_code: int, from_date: str, to_date: str) -> list:
        """
        Search final orders for a bench within a date range.
        Dates in dd-mm-yyyy format.
        Returns list of raw entries parsed from JSON response.
        """
        if not self._csrf_token:
            self._get_csrf_token()

        data = {
            "csrf_token": self._csrf_token or "",
            "order_type": "final",
            "bench": str(bench_code),
            "from": from_date,
            "to": to_date,
            "captcha_code": "111111",
            "tab": "4",
        }

        try:
            resp = self.session.post(AJAX_URL, data=data, timeout=60)
            resp.raise_for_status()
            result = resp.json()
        except (requests.RequestException, json.JSONDecodeError) as e:
            logger.error("AJAX request failed for bench %d (%s - %s): %s",
                         bench_code, from_date, to_date, e)
            return []

        # Update CSRF token if returned
        if "csrf_token" in result:
            self._csrf_token = result["csrf_token"]

        # Check for errors
        if isinstance(result.get("data"), dict) and "errors" in result["data"]:
            logger.error("API errors: %s", result["data"]["errors"])
            return []

        records = result.get("data", [])
        if not isinstance(records, list):
            return []

        entries = []
        for row in records:
            if not isinstance(row, list) or len(row) < 5:
                continue

            # row = [serial, case_no, parties_html, date, pdf_link_html]
            case_no = row[1].strip() if row[1] else ""

            # Parse parties from HTML
            parties_html = row[2] if row[2] else ""
            parties_text = re.sub(r'<[^>]+>', ' ', parties_html).strip()
            parties_text = re.sub(r'\s+', ' ', parties_text)
            parts = re.split(r'\bvs\b', parties_text, maxsplit=1, flags=re.I)
            petitioner = parts[0].strip() if len(parts) > 0 else ""
            respondent = parts[1].strip() if len(parts) > 1 else ""

            # Parse date (dd/mm/yyyy)
            date_str = row[3].strip() if row[3] else ""
            decision_date = self._parse_date(date_str)

            # Extract PDF URL
            pdf_url = None
            pdf_match = re.search(r'href="([^"]+)"', row[4] if row[4] else "")
            if pdf_match:
                pdf_path = pdf_match.group(1)
                if pdf_path.startswith('./'):
                    pdf_url = f"{BASE_URL}/{pdf_path[2:]}"
                elif pdf_path.startswith('/'):
                    pdf_url = f"{BASE_URL}{pdf_path}"
                elif not pdf_path.startswith('http'):
                    pdf_url = f"{BASE_URL}/{pdf_path}"
                else:
                    pdf_url = pdf_path

            entries.append({
                "case_no": case_no,
                "order_id": self._order_id(pdf_url),
                "petitioner": petitioner,
                "respondent": respondent,
                "decision_date": decision_date,
                "date_raw": date_str,
                "pdf_url": pdf_url,
                "bench_code": bench_code,
            })

        logger.info("Bench %d (%s - %s): %d entries", bench_code, from_date, to_date, len(entries))
        return entries

    @staticmethod
    def _order_id(pdf_url: Optional[str]) -> str:
        """Pull the stable order identifier out of a weborders PDF link.

        Links look like ``/weborders/file/chandigarh/328752``. That trailing
        number is CESTAT's own per-order identifier: unique per document and
        stable across crawls, unlike the case number (which is blank on some
        listings and shared by every order issued in the same appeal — one
        number was seen on 45 separate orders, see #1437).
        """
        if not pdf_url:
            return ""
        match = re.search(r'/weborders/file/([^/]+)/(\d+)', pdf_url)
        if match:
            return f"{match.group(1)}-{match.group(2)}"
        # Unknown link shape — fall back to the last non-empty path segment.
        tail = [seg for seg in pdf_url.split("?")[0].split("/") if seg]
        return tail[-1] if tail else ""

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse dd/mm/yyyy date to ISO 8601."""
        if not date_str:
            return None
        date_str = date_str.strip()
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y"):
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _download_pdf_text(self, pdf_url: str, source_id: str) -> Optional[str]:
        """Download a PDF and extract text."""
        if not pdf_url:
            return None

        try:
            text = extract_pdf_markdown(
                source="IN/CESTAT",
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
        except Exception as e:
            logger.warning("PDF extraction failed for %s: %s", source_id, e)

        return None

    def _month_ranges(self, start_year: int, end_year: int):
        """Generate (from_date, to_date) pairs as dd-mm-yyyy for each month."""
        import calendar
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                if year == end_year and month > datetime.now().month:
                    break
                last_day = calendar.monthrange(year, month)[1]
                from_date = f"01-{month:02d}-{year}"
                to_date = f"{last_day:02d}-{month:02d}-{year}"
                yield from_date, to_date

    def _checkpoint_path(self) -> Path:
        return Path(__file__).parent / "data" / "cestat_checkpoint.json"

    def _load_checkpoint(self) -> set:
        """Return the set of (bench, month) windows already crawled."""
        path = self._checkpoint_path()
        if not path.exists():
            return set()
        try:
            done = json.loads(path.read_text())
            logger.info("Checkpoint: %d bench-months already crawled", len(done))
            return set(done)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Could not read checkpoint (%s) — starting fresh", e)
            return set()

    def _save_checkpoint(self, done: set) -> None:
        path = self._checkpoint_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            path.write_text(json.dumps(sorted(done)))
        except OSError as e:
            logger.warning("Could not write checkpoint: %s", e)

    def fetch_all(self) -> Generator:
        """Yield all final order records across all benches and years.

        The full crawl is ~9 benches × ~11 years of months and runs past the
        fleet's 100-hour cap (#1437), so completed bench-months are recorded in
        data/cestat_checkpoint.json and skipped with no network call on a
        restart. Successive slots then advance monotonically instead of
        re-walking 2015 every time; the loader dedups on _id.
        """
        end_year = datetime.now().year
        done = self._load_checkpoint()
        for bench_code, bench_name in BENCHES:
            logger.info("Processing bench: %s (code %d)", bench_name, bench_code)
            for from_date, to_date in self._month_ranges(START_YEAR, end_year):
                window = f"{bench_code}:{from_date}"
                if window in done:
                    continue
                entries = self._search_orders(bench_code, from_date, to_date)
                for entry in entries:
                    entry["bench_name"] = bench_name
                    yield entry
                done.add(window)
                self._save_checkpoint(done)
                time.sleep(1.5)

    def fetch_updates(self, since: datetime) -> Generator:
        """Yield final order records from recent months."""
        now = datetime.now()
        for bench_code, bench_name in BENCHES:
            check_date = since
            while check_date <= now:
                import calendar
                from_date = f"01-{check_date.month:02d}-{check_date.year}"
                last_day = calendar.monthrange(check_date.year, check_date.month)[1]
                to_date = f"{last_day:02d}-{check_date.month:02d}-{check_date.year}"

                entries = self._search_orders(bench_code, from_date, to_date)
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

        # Identify the document by CESTAT's own order id, not the case number.
        # A case number is blank on some listings and is shared by every order
        # issued in the same appeal, so bench+case collapsed 219,500 records
        # onto 58,744 ids and made re-runs insert duplicates instead of
        # colliding (#1437). The weborders link is unique per order.
        order_id = raw.get("order_id") or self._order_id(raw.get("pdf_url"))
        if order_id:
            doc_id = f"CESTAT-{re.sub(r'[^a-zA-Z0-9]', '-', order_id).strip('-')}"
        else:
            # No usable link — fall back to bench+case+date, which is at least
            # deterministic across crawls even if it is not always unique.
            safe_case = re.sub(r'[^a-zA-Z0-9]', '-', case_no).strip('-')
            doc_id = f"CESTAT-{bench_code}-{safe_case}-{decision_date or 'nodate'}"

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
            title = f"{case_no} — {pet_short} vs. {res_short}"
        elif petitioner:
            title = f"{case_no} — {petitioner[:80]}"

        # Build source URL
        source_url = pdf_url or f"{BASE_URL}/final-order-status"

        return {
            "_id": doc_id,
            "_source": "IN/CESTAT",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": decision_date,
            "url": source_url,
            "case_no": case_no,
            "case_number": case_no or None,
            "order_id": order_id or None,
            "petitioner": petitioner,
            "respondent": respondent,
            "bench": bench_name,
            "bench_code": bench_code,
            "case_type": self._detect_case_type(case_no),
        }

    def _detect_case_type(self, case_no: str) -> str:
        """Detect case type from case number prefix."""
        case_no_upper = case_no.upper()
        if case_no_upper.startswith("C/"):
            return "customs"
        elif case_no_upper.startswith("E/"):
            return "excise"
        elif case_no_upper.startswith("ST/"):
            return "service_tax"
        elif "ANTIDUMPING" in case_no_upper or case_no_upper.startswith("AD/"):
            return "antidumping"
        elif case_no_upper.startswith("CST/"):
            return "central_sales_tax"
        return "unknown"


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="IN/CESTAT bootstrap")
    sub = parser.add_subparsers(dest="command")

    for name in ("bootstrap", "bootstrap-fast"):
        boot = sub.add_parser(name, help="Full initial pull")
        boot.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
        boot.add_argument("--full", action="store_true", help="Full corpus (the default)")

    upd = sub.add_parser("update", help="Fetch recent orders")
    upd.add_argument("--days", type=int, default=90, help="Look back N days (default 90)")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = CESTATScraper()

    if args.command == "test":
        logger.info("Testing CESTAT connectivity...")
        scraper._get_csrf_token()
        entries = scraper._search_orders(107079, "01-04-2026", "30-04-2026")
        logger.info("Delhi April 2026: %d entries found", len(entries))
        if entries:
            sample = entries[0]
            logger.info("Sample: case=%s, date=%s, pdf=%s",
                        sample["case_no"], sample["decision_date"],
                        (sample.get("pdf_url") or "")[:100])
        logger.info("Test PASSED")

    elif args.command in ("bootstrap", "bootstrap-fast"):
        if args.sample:
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)
            count = 0

            for raw in scraper.fetch_all():
                rec = scraper.normalize(raw)
                if rec:
                    count += 1
                    out_path = sample_dir / f"{rec['_id']}.json"
                    with open(out_path, "w", encoding="utf-8") as f:
                        json.dump(rec, f, ensure_ascii=False, indent=2)
                    logger.info("[%d] Saved %s (%d chars text)", count, rec["_id"],
                                len(rec.get("text", "")))
                    if count >= 15:
                        break

            logger.info("Sample complete: %d records saved to %s", count, sample_dir)
        else:
            # Route the full corpus through BaseScraper so it streams to
            # data/records.jsonl instead of writing 200k+ files into sample/.
            stats = scraper.bootstrap_fast()
            logger.info("Bootstrap complete: %s", stats)

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
