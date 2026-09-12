#!/usr/bin/env python3
"""
IN/SAT -- Securities Appellate Tribunal

Fetches orders from the SAT portal (satweb.sat.gov.in).

Strategy:
  - GET /orders page to obtain security_token (CSRF)
  - POST to get-orders-by-date AJAX endpoint with date ranges per appeal type
  - Parse HTML table response for case metadata and order links
  - Download PDFs from view-order/{hash}/{id} URLs
  - Extract full text using pdfplumber
  - Normalize into standard schema

Data:
  - Appeals against SEBI, IRDAI, PFRDA orders
  - Single bench at Mumbai, all-India jurisdiction
  - Records from 1970-present (online orders from ~2003+)
  - ~500+ orders per year for SEBI alone
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

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IN.SAT")

BASE_URL = "https://satweb.sat.gov.in"

# Appeal types: (value, label)
APPEAL_TYPES = [
    ("1", "SEBI"),
    ("2", "IRDAI"),
    ("3", "PFRDA"),
]

# Start from 2003 — earlier records unlikely to have PDFs online
START_YEAR = 2003


class SATScraper(BaseScraper):
    """
    Scraper for IN/SAT -- Securities Appellate Tribunal.
    Country: IN
    URL: https://satweb.sat.gov.in/orders
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
        self._security_token = None

    def _get_security_token(self) -> str:
        """Fetch the orders page and extract the CSRF security token."""
        resp = self.session.get(f"{BASE_URL}/orders", timeout=30)
        resp.raise_for_status()
        match = re.search(r'security_token[^>]*value="([^"]+)"', resp.text)
        if not match:
            raise RuntimeError("Could not extract security_token from orders page")
        self._security_token = match.group(1)
        return self._security_token

    def _ensure_token(self) -> str:
        """Return current token or fetch a new one."""
        if not self._security_token:
            return self._get_security_token()
        return self._security_token

    def _fetch_orders_by_date(self, apl_type: str, start_date: str, end_date: str) -> list:
        """
        Fetch orders via AJAX endpoint for a date range and appeal type.
        Dates in dd-mm-yyyy format.
        Returns list of raw entries parsed from the HTML table.
        """
        token = self._ensure_token()

        data = {
            "apl_type": apl_type,
            "startDate": start_date,
            "endDate": end_date,
            "security_token": token,
        }

        try:
            self.session.headers["X-Requested-With"] = "XMLHttpRequest"
            self.session.headers["Referer"] = f"{BASE_URL}/orders"
            resp = self.session.post(
                f"{BASE_URL}/get-orders-by-date",
                data=data,
                timeout=60,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error("Failed to fetch orders (type=%s, %s - %s): %s",
                         apl_type, start_date, end_date, e)
            return []

        try:
            j = resp.json()
        except ValueError:
            logger.error("Non-JSON response for type=%s, %s - %s", apl_type, start_date, end_date)
            return []

        if "token" in j:
            self._security_token = j["token"]

        content = j.get("content", "")
        if not content:
            return []

        return self._parse_order_table(content, apl_type)

    def _parse_order_table(self, html: str, apl_type: str) -> list:
        """Parse the HTML table from the AJAX response into structured entries."""
        entries = []
        rows = re.findall(r'<tr>(.*?)</tr>', html, re.S)

        for row in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.S)
            if len(cells) < 7:
                continue

            serial_text = re.sub(r'<[^>]+>', '', cells[0]).strip()
            if not re.match(r'^\d{1,6}$', serial_text):
                continue

            al_no = re.sub(r'<[^>]+>', '', cells[1]).strip()
            appeal_no = re.sub(r'<[^>]+>', '', cells[2]).strip()

            # Parties: "Appellant <span class="vs">vs</span> Respondent"
            parties_html = cells[3]
            parties_text = re.sub(r'<[^>]+>', ' ', parties_html).strip()
            parties_text = re.sub(r'\s+', ' ', parties_text)
            parts = re.split(r'\bvs\b', parties_text, maxsplit=1, flags=re.I)
            appellant = parts[0].strip() if len(parts) > 0 else ""
            respondent = parts[1].strip() if len(parts) > 1 else ""

            court = re.sub(r'<[^>]+>', '', cells[4]).strip()
            date_text = re.sub(r'<[^>]+>', '', cells[5]).strip()
            decision_date = self._parse_date(date_text)

            # PDF link: <a href="https://satweb.sat.gov.in/view-order/{hash}/{id}" ...>
            pdf_match = re.search(
                r'href="(https://satweb\.sat\.gov\.in/view-order/[a-f0-9]+/\d+)"',
                cells[6], re.I
            )
            pdf_url = pdf_match.group(1) if pdf_match else None

            entries.append({
                "serial": int(serial_text),
                "al_no": al_no,
                "appeal_no": appeal_no,
                "appellant": appellant,
                "respondent": respondent,
                "parties_text": parties_text,
                "court": court,
                "decision_date": decision_date,
                "date_raw": date_text,
                "pdf_url": pdf_url,
                "apl_type": apl_type,
            })

        return entries

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse dd/mm/yyyy or dd.mm.yyyy date to ISO 8601."""
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

    def _download_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download a PDF from SAT and extract text via pdfplumber."""
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
        except Exception as e:
            logger.warning("PDF extraction failed for %s: %s", pdf_url, e)

        return None

    def _generate_date_ranges(self, start_year: int, end_year: int):
        """Generate monthly date ranges for querying."""
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                if year == end_year and month > datetime.now().month:
                    break
                start = f"01-{month:02d}-{year}"
                if month == 12:
                    end = f"31-12-{year}"
                else:
                    # Last day of current month
                    import calendar
                    last_day = calendar.monthrange(year, month)[1]
                    end = f"{last_day:02d}-{month:02d}-{year}"
                yield start, end

    def fetch_all(self) -> Generator:
        """Yield all order entries across all appeal types and years."""
        current_year = datetime.now().year
        for apl_val, apl_label in APPEAL_TYPES:
            logger.info("Fetching %s appeals (%s-%s)...", apl_label, START_YEAR, current_year)
            for start_date, end_date in self._generate_date_ranges(START_YEAR, current_year):
                entries = self._fetch_orders_by_date(apl_val, start_date, end_date)
                if entries:
                    logger.info("%s %s: %d orders", apl_label, start_date, len(entries))
                for entry in entries:
                    yield entry
                time.sleep(1.5)

    def fetch_updates(self, since: datetime) -> Generator:
        """Yield orders from the last N days."""
        since_date = since if isinstance(since, datetime) else datetime.now(timezone.utc) - timedelta(days=90)
        start_str = since_date.strftime("%d-%m-%Y")
        end_str = datetime.now().strftime("%d-%m-%Y")

        for apl_val, apl_label in APPEAL_TYPES:
            entries = self._fetch_orders_by_date(apl_val, start_str, end_str)
            logger.info("Update %s: %d orders since %s", apl_label, len(entries), start_str)
            for entry in entries:
                yield entry
            time.sleep(1.5)

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw entry into standard schema."""
        appeal_no = raw.get("appeal_no", "")
        al_no = raw.get("al_no", "")
        pdf_url = raw.get("pdf_url")

        # Build unique ID from appeal number
        doc_id = appeal_no.replace(" ", "").replace("/", "-") if appeal_no else f"SAT-AL-{al_no.replace('/', '-')}"
        if not doc_id or doc_id == "-":
            doc_id = f"SAT-{raw.get('serial', 0)}"

        # Download and extract PDF text
        text = self._download_pdf_text(pdf_url)
        if not text:
            logger.warning("No text extracted for %s", doc_id)
            return None

        # Build title
        appellant = raw.get("appellant", "")
        respondent = raw.get("respondent", "")
        if appellant and respondent:
            parties_short = f"{appellant[:60]} v. {respondent[:60]}"
            title = f"{appeal_no} — {parties_short}"
        else:
            title = appeal_no or doc_id

        # Map appeal type code to label
        apl_labels = {"1": "SEBI", "2": "IRDAI", "3": "PFRDA"}
        appeal_type = apl_labels.get(raw.get("apl_type", ""), "")

        url = pdf_url or f"{BASE_URL}/orders"

        return {
            "_id": doc_id,
            "_source": "IN/SAT",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("decision_date"),
            "url": url,
            "appeal_no": appeal_no,
            "al_no": al_no,
            "appellant": appellant,
            "respondent": respondent,
            "court": raw.get("court", ""),
            "appeal_type": appeal_type,
        }


# ----- CLI -----
if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import argparse

    parser = argparse.ArgumentParser(description="IN/SAT bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Full initial pull")
    boot.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")

    upd = sub.add_parser("update", help="Fetch recent orders")
    upd.add_argument("--days", type=int, default=90, help="Look back N days (default 90)")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = SATScraper()

    if args.command == "test":
        logger.info("Testing SAT connectivity...")
        token = scraper._get_security_token()
        logger.info("Security token obtained: %s...", token[:20])
        entries = scraper._fetch_orders_by_date("1", "01-01-2024", "31-01-2024")
        logger.info("SEBI Jan 2024: %d orders found", len(entries))
        if entries:
            sample = entries[0]
            logger.info("Sample: appeal=%s, date=%s, pdf=%s",
                        sample["appeal_no"], sample["decision_date"],
                        "yes" if sample["pdf_url"] else "no")
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
                logger.info("[%d] %s — %d chars", count, rec["_id"], len(rec["text"]))
                if count >= limit:
                    break
            time.sleep(1.5)

        logger.info("Bootstrap complete: %d records saved to sample/", count)

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
                logger.info("[%d] %s", count, rec["_id"])
            time.sleep(1.5)

        logger.info("Update complete: %d records", count)

    else:
        parser.print_help()
