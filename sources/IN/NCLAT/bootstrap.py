#!/usr/bin/env python3
"""
IN/NCLAT -- National Company Law Appellate Tribunal

Fetches judgments and orders from nclat.nic.in via their display-board endpoints.

Strategy:
  - GET the judgment page to obtain CSRF token + session cookies
  - POST to /display-board/judgement_details with date range and case_type=All
  - Parse HTML response for filing numbers, case metadata, and order download forms
  - POST to /display-board/view_order to download PDFs
  - Extract full text using pdfplumber
  - Normalize into standard schema

Data:
  - 2 benches: New Delhi, Chennai
  - Covers Company Appeals, Competition Appeals, Insolvency Appeals,
    Contempt, Review, Restoration, Transfer cases
  - Judgments available as digital PDFs (text-extractable)
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

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IN.NCLAT")

BASE_URL = "https://nclat.nic.in/display-board"
JUDGE_PAGE = f"{BASE_URL}/judge"
JUDGEMENT_URL = f"{BASE_URL}/judgement_details"
VIEW_ORDER_URL = f"{BASE_URL}/view_order"

LOCATIONS = ["delhi", "chennai"]

# Start from 2018 — NCLAT became fully operational mid-2016, digital records from ~2018
START_YEAR = 2018


class NCLATScraper:
    """
    Scraper for IN/NCLAT -- National Company Law Appellate Tribunal.
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research)",
            "Accept": "text/html, application/json, */*",
            "X-Requested-With": "XMLHttpRequest",
        })
        self._token = None

    def _get_token(self):
        """Fetch the judgment page to get a fresh CSRF token."""
        try:
            resp = self.session.get(JUDGE_PAGE, timeout=30)
            resp.raise_for_status()
            match = re.search(
                r'name=["\']_token["\'][^>]*value=["\']([^"\']+)["\']',
                resp.text,
            )
            if match:
                self._token = match.group(1)
                logger.info("Got CSRF token: %s...", self._token[:8])
            else:
                logger.warning("CSRF token not found")
        except requests.RequestException as e:
            logger.error("Failed to fetch CSRF token: %s", e)

    def _search_judgments(self, location: str, from_date: str, to_date: str) -> list:
        """
        Search judgments for a location within a date range.
        Dates in dd-mm-yyyy format.
        Returns list of raw entries parsed from HTML response.
        """
        if not self._token:
            self._get_token()

        data = {
            "_token": self._token or "",
            "search_by": "order_date_wise",
            "location": location,
            "case_type": "All",
            "category": "",
            "court": "",
            "case_year": "",
            "from_date": from_date,
            "to_date": to_date,
            "case_number": "",
            "text_name": "",
            "exact_search_word": "",
            "diary_no": "",
            "select_judge": "",
        }

        try:
            resp = self.session.post(JUDGEMENT_URL, data=data, timeout=60)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error("Request failed for %s (%s - %s): %s",
                         location, from_date, to_date, e)
            return []

        html = resp.text

        # Check for JSON error response
        if html.startswith("{"):
            try:
                j = json.loads(html)
                if j.get("status") is False:
                    logger.error("API error: %s", j.get("errors", j.get("message")))
                    return []
            except json.JSONDecodeError:
                pass

        # Parse cases and their order forms from HTML
        entries = []
        # Extract case table rows
        case_pattern = (
            r'<tr>\s*<th>(\d+)</th>\s*<th>(\d+)</th>\s*'
            r'<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*'
            r'<td>(.*?)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>'
        )
        cases = re.findall(case_pattern, html, re.S)

        # Extract order forms — maps filing_no → list of (order_date, order_type)
        order_forms = re.findall(
            r'filing_no["\s]*value="(\d+)".*?order_date["\s]*value="([^"]+)".*?order_type["\s]*value="([^"]+)"',
            html, re.S
        )

        # Build filing_no → orders mapping
        orders_by_filing = {}
        for filing_no, order_date, order_type in order_forms:
            if filing_no not in orders_by_filing:
                orders_by_filing[filing_no] = []
            orders_by_filing[filing_no].append((order_date, order_type))

        for case in cases:
            serial, filing_no, case_no, case_type, parties_html, bench, status = case
            parties_text = re.sub(r'<[^>]+>', ' ', parties_html).strip()
            parties_text = re.sub(r'\s+', ' ', parties_text)

            # Split parties on VS
            parts = re.split(r'\bVS\b', parties_text, maxsplit=1, flags=re.I)
            petitioner = parts[0].strip() if len(parts) > 0 else ""
            respondent = parts[1].strip() if len(parts) > 1 else ""

            # Get orders for this filing
            orders = orders_by_filing.get(filing_no, [])

            # Only include cases with judgment orders (type J or JC)
            for order_date, order_type in orders:
                if order_type in ("J", "JC"):
                    entries.append({
                        "filing_no": filing_no,
                        "case_no": case_no.strip(),
                        "case_type": case_type.strip(),
                        "petitioner": petitioner,
                        "respondent": respondent,
                        "bench": bench.strip(),
                        "status": status.strip(),
                        "order_date": order_date,
                        "order_type": order_type,
                        "location": location,
                    })

        # Also parse entries that might not match the strict table pattern
        # but have order forms (some cases span multiple sections)
        if not cases and order_forms:
            for filing_no, order_date, order_type in order_forms:
                if order_type in ("J", "JC"):
                    entries.append({
                        "filing_no": filing_no,
                        "case_no": "",
                        "case_type": "",
                        "petitioner": "",
                        "respondent": "",
                        "bench": location.title(),
                        "status": "",
                        "order_date": order_date,
                        "order_type": order_type,
                        "location": location,
                    })

        logger.info("Location %s (%s - %s): %d judgment entries",
                    location, from_date, to_date, len(entries))
        return entries

    def _download_pdf_text(self, filing_no: str, order_date: str,
                           order_type: str, location: str, source_id: str) -> Optional[str]:
        """Download order PDF and extract text."""
        if not self._token:
            self._get_token()

        data = {
            "search_type": "view_order",
            "_token": self._token or "",
            "bench_name": location,
            "filing_no": filing_no,
            "order_date": order_date,
            "order_type": order_type,
        }

        try:
            text = extract_pdf_markdown(
                source="IN/NCLAT",
                source_id=source_id,
                pdf_url=VIEW_ORDER_URL,
                table="case_law",
            )
            if text and len(text.strip()) > 100:
                return text.strip()
        except Exception:
            pass

        # Direct download and pdfplumber extraction
        try:
            import pdfplumber
            resp = self.session.post(VIEW_ORDER_URL, data=data, timeout=120)
            resp.raise_for_status()

            content_type = resp.headers.get("Content-Type", "")
            if "pdf" not in content_type and len(resp.content) < 1000:
                logger.warning("Non-PDF response for %s: %s", source_id, content_type)
                return None

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

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date to ISO 8601."""
        if not date_str:
            return None
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%m-%d-%Y"):
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
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

    def fetch_all(self) -> Generator:
        """Yield all judgment records across all locations and years."""
        end_year = datetime.now().year
        for location in LOCATIONS:
            logger.info("Processing location: %s", location)
            for from_date, to_date in self._month_ranges(START_YEAR, end_year):
                entries = self._search_judgments(location, from_date, to_date)
                for entry in entries:
                    yield entry
                time.sleep(1.5)

    def fetch_updates(self, since: datetime) -> Generator:
        """Yield judgment records from recent months."""
        now = datetime.now()
        for location in LOCATIONS:
            check_date = since
            while check_date <= now:
                import calendar
                from_date = f"01-{check_date.month:02d}-{check_date.year}"
                last_day = calendar.monthrange(check_date.year, check_date.month)[1]
                to_date = f"{last_day:02d}-{check_date.month:02d}-{check_date.year}"

                entries = self._search_judgments(location, from_date, to_date)
                for entry in entries:
                    yield entry

                if check_date.month == 12:
                    check_date = check_date.replace(year=check_date.year + 1, month=1)
                else:
                    check_date = check_date.replace(month=check_date.month + 1)
                time.sleep(1.5)

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw entry into standard schema."""
        filing_no = raw.get("filing_no", "")
        case_no = raw.get("case_no", "")
        order_date = raw.get("order_date", "")
        order_type = raw.get("order_type", "J")
        location = raw.get("location", "delhi")

        # Build unique ID
        date_part = order_date.replace("-", "")
        doc_id = f"NCLAT-{filing_no}-{date_part}-{order_type}"

        # Download and extract PDF text
        text = self._download_pdf_text(filing_no, order_date, order_type, location, doc_id)
        if not text:
            logger.warning("No text extracted for %s (%s)", doc_id, case_no)
            return None

        petitioner = raw.get("petitioner", "")
        respondent = raw.get("respondent", "")
        case_type = raw.get("case_type", "")

        # Build title
        title = case_no or filing_no
        if petitioner and respondent:
            pet_short = petitioner[:60] + "..." if len(petitioner) > 60 else petitioner
            res_short = respondent[:60] + "..." if len(respondent) > 60 else respondent
            title = f"{case_no} — {pet_short} vs. {res_short}"
        elif petitioner:
            title = f"{case_no} — {petitioner[:80]}"

        decision_date = self._parse_date(order_date)

        return {
            "_id": doc_id,
            "_source": "IN/NCLAT",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": decision_date,
            "url": f"https://nclat.nic.in/display-board/judge",
            "filing_no": filing_no,
            "case_no": case_no,
            "case_type": case_type,
            "petitioner": petitioner,
            "respondent": respondent,
            "bench": raw.get("bench", ""),
            "location": location,
            "order_type": "judgment" if order_type == "J" else "judgment_corrected",
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import argparse

    parser = argparse.ArgumentParser(description="IN/NCLAT bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Full initial pull")
    boot.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")

    upd = sub.add_parser("update", help="Fetch recent orders")
    upd.add_argument("--days", type=int, default=90, help="Look back N days (default 90)")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = NCLATScraper()

    if args.command == "test":
        logger.info("Testing NCLAT connectivity...")
        scraper._get_token()
        entries = scraper._search_judgments("delhi", "01-04-2026", "05-04-2026")
        logger.info("Delhi 1-5 April 2026: %d judgment entries", len(entries))
        if entries:
            sample = entries[0]
            logger.info("Sample: filing=%s, case=%s, date=%s",
                        sample["filing_no"], sample["case_no"], sample["order_date"])
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
