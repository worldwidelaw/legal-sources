#!/usr/bin/env python3
"""
IN/DRAT -- Debt Recovery Tribunals & Appellate Tribunals

Fetches daily orders from India's DRTs and DRATs via the public
drt.gov.in API (no authentication required).

Strategy:
  - GET /drtapi/getDrtDratScheamName to list all 44 tribunals
  - GET /drtapi/getDrtDratCaseTyepName to list case types per tribunal
  - POST /drtapi/getDrtDailyOrderReportCaseNo to get orders per case
  - Download order PDFs from cis.drt.gov.in/drtlive/order/secure-pdf-serve.php
  - Extract text using pdfplumber
  - Normalize into standard schema

Data:
  - 39 DRTs + 5 DRATs across India
  - Banking debt recovery (RDDBFI Act, SARFAESI Act, IBC)
  - Case types: Original Application, Securitization Application, etc.
  - Orders are PDFs — some text-based, some scanned images
  - No authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent orders
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
from urllib.parse import parse_qs, urlparse

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IN.DRAT")

API_BASE = "https://drt.gov.in/drtapi"
PDF_BASE = "https://cis.drt.gov.in/drtlive/order/secure-pdf-serve.php"

# Years to scan for cases
START_YEAR = 2020
END_YEAR = 2026

# Max consecutive empty case numbers before stopping for a given type/year
MAX_EMPTY_STREAK = 10

# Case types most likely to have orders (Original Application is the main one)
PRIORITY_CASE_TYPES = ["1", "7", "4"]  # OA, SA, Appeal


class DRATScraper(BaseScraper):
    """
    Scraper for IN/DRAT -- Debt Recovery Tribunals & Appellate Tribunals.
    Country: IN
    URL: https://drt.gov.in/
    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; legal research)",
        })
        self._tribunals = None
        self._case_types_cache = {}
        # Checkpoint/resume: the full scan (44 tribunals × 3 case types ×
        # 7 years × case-no walk, each a 1s-throttled API call + per-order PDF
        # download) cannot finish inside the 100h fleet window (exit 124). We
        # persist a per-unit "done" set keyed by (tribunal_id, case_type, year)
        # plus the last case_no reached in the in-progress unit, so successive
        # fleet slots skip completed units with NO network calls and resume the
        # current unit where it left off — the run advances monotonically to
        # completion instead of restarting from scratch each slot (#1103).
        self._checkpoint_path = self.source_dir / "data" / "drat_checkpoint.json"
        self._completed_units, self._unit_progress = self._load_checkpoint()

    @staticmethod
    def _unit_key(tid: str, ct_id: str, year: int) -> str:
        return f"{tid}|{ct_id}|{year}"

    def _load_checkpoint(self):
        """Load (completed unit keys, in-progress {unit_key: last_case_no})."""
        try:
            with open(self._checkpoint_path) as f:
                data = json.load(f)
            completed = set(data.get("completed_units", []))
            progress = data.get("unit_progress", {})
            if completed or progress:
                logger.info(
                    "Resuming from checkpoint: %d units done, %d in progress",
                    len(completed), len(progress),
                )
            return completed, progress
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return set(), {}

    def _save_checkpoint(self) -> None:
        """Persist completed units + in-progress case-no positions."""
        try:
            self._checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._checkpoint_path.with_suffix(".json.tmp")
            with open(tmp, "w") as f:
                json.dump({
                    "completed_units": sorted(self._completed_units),
                    "unit_progress": self._unit_progress,
                }, f)
            tmp.replace(self._checkpoint_path)
        except OSError as e:
            logger.warning("Could not write checkpoint: %s", e)

    def _api_post(self, endpoint: str, data: dict, retries: int = 2) -> Optional[dict]:
        """POST to drtapi with multipart form data."""
        url = f"{API_BASE}/{endpoint}"
        for attempt in range(retries + 1):
            try:
                resp = self.session.post(url, data=data, timeout=30)
                if resp.status_code == 200:
                    text = resp.text.strip()
                    if not text:
                        return None
                    return resp.json()
                elif resp.status_code == 400:
                    return resp.json()
                else:
                    logger.warning("API %s returned %d (attempt %d)", endpoint, resp.status_code, attempt + 1)
            except requests.RequestException as e:
                logger.warning("API %s error (attempt %d): %s", endpoint, attempt + 1, e)
            if attempt < retries:
                time.sleep(2 * (attempt + 1))
        return None

    def _get_tribunals(self) -> list:
        """Get list of all DRT/DRAT tribunals."""
        if self._tribunals is not None:
            return self._tribunals
        data = self._api_post("getDrtDratScheamName", {})
        if not isinstance(data, list):
            logger.error("Failed to get tribunal list")
            return []
        self._tribunals = data
        logger.info("Found %d tribunals", len(data))
        return data

    def _get_case_types(self, tribunal_id: str) -> list:
        """Get case types available at a tribunal."""
        if tribunal_id in self._case_types_cache:
            return self._case_types_cache[tribunal_id]
        data = self._api_post("getDrtDratCaseTyepName", {"schemeNameDrtId": tribunal_id})
        if not isinstance(data, list):
            self._case_types_cache[tribunal_id] = []
            return []
        self._case_types_cache[tribunal_id] = data
        return data

    def _get_daily_orders(self, tribunal_id: str, case_type: str, case_no: int, year: int) -> list:
        """Fetch daily orders for a specific case."""
        data = self._api_post("getDrtDailyOrderReportCaseNo", {
            "schemeNameDrtId": tribunal_id,
            "caseType": case_type,
            "caseNo": str(case_no),
            "caseYear": str(year),
        })
        if isinstance(data, list):
            return data
        return []

    def _extract_pdf_file_param(self, pdf_viewer_url: str) -> Optional[str]:
        """Extract the base64 file parameter from a PDF viewer URL."""
        parsed = urlparse(pdf_viewer_url)
        params = parse_qs(parsed.query)
        files = params.get("file", [])
        return files[0] if files else None

    def _download_and_extract_text(self, pdf_viewer_url: str) -> Optional[str]:
        """Download a PDF and extract text using pdfplumber."""
        file_param = self._extract_pdf_file_param(pdf_viewer_url)
        if not file_param:
            return None

        try:
            resp = self.session.get(
                f"{PDF_BASE}?file={file_param}",
                timeout=30,
            )
            if resp.status_code != 200:
                return None

            # Verify it's actually a PDF
            if not resp.content[:5].startswith(b"%PDF"):
                return None

            import pdfplumber
            with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                texts = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        texts.append(text.strip())
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass
                return "\n\n".join(texts) if texts else None

        except Exception as e:
            logger.debug("PDF extraction failed for %s: %s", file_param[:30], e)
            return None

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse DD/MM/YYYY to ISO 8601."""
        if not date_str:
            return None
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all orders across all tribunals."""
        tribunals = self._get_tribunals()
        if not tribunals:
            return

        # Prioritize tribunals known to have text-based PDFs
        text_rich_ids = {"30", "25", "31", "32", "33", "34", "35", "22", "11"}
        tribunals_sorted = sorted(
            tribunals,
            key=lambda t: (0 if t["schemeNameDrtId"] in text_rich_ids else 1),
        )

        for tribunal in tribunals_sorted:
            tid = tribunal["schemeNameDrtId"]
            tname = tribunal["SchemaName"]
            logger.info("Processing tribunal: %s (ID=%s)", tname, tid)

            case_types = self._get_case_types(tid)
            if not case_types:
                logger.info("  No case types for %s, skipping", tname)
                continue
            time.sleep(0.5)

            for ct in case_types:
                ct_id = ct["caseType"]
                ct_name = ct["caseTypeName"]

                # Only process priority case types in full bootstrap
                if ct_id not in PRIORITY_CASE_TYPES:
                    continue

                for year in range(END_YEAR, START_YEAR - 1, -1):
                    unit = self._unit_key(tid, ct_id, year)

                    # Skip fully-completed units with NO network calls (resume).
                    if unit in self._completed_units:
                        continue

                    empty_streak = 0
                    # Resume the in-progress unit at its last position (re-fetch
                    # only the last case_no; the loader dedups on item_no).
                    case_no = self._unit_progress.get(unit, 1)

                    while empty_streak < MAX_EMPTY_STREAK:
                        orders = self._get_daily_orders(tid, ct_id, case_no, year)
                        time.sleep(1)

                        if not orders:
                            empty_streak += 1
                            case_no += 1
                            continue

                        empty_streak = 0

                        for order in orders:
                            yield {
                                "tribunal_id": tid,
                                "tribunal_name": tname,
                                "case_type_id": ct_id,
                                "case_type_name": ct_name,
                                "case_year": year,
                                "case_no": case_no,
                                **order,
                            }

                        case_no += 1
                        # Persist progress every 25 case numbers to bound disk
                        # writes; a crash mid-unit re-fetches only from here.
                        if case_no % 25 == 0:
                            self._unit_progress[unit] = case_no
                            self._save_checkpoint()

                    # Unit exhausted (hit MAX_EMPTY_STREAK): mark done so future
                    # slots skip it entirely.
                    self._completed_units.add(unit)
                    self._unit_progress.pop(unit, None)
                    self._save_checkpoint()

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent orders — scan current year across all tribunals."""
        current_year = datetime.now().year
        tribunals = self._get_tribunals()
        if not tribunals:
            return

        for tribunal in tribunals:
            tid = tribunal["schemeNameDrtId"]
            tname = tribunal["SchemaName"]
            case_types = self._get_case_types(tid)
            time.sleep(0.5)

            for ct in case_types:
                ct_id = ct["caseType"]
                ct_name = ct["caseTypeName"]
                if ct_id not in PRIORITY_CASE_TYPES:
                    continue

                empty_streak = 0
                case_no = 1
                while empty_streak < MAX_EMPTY_STREAK:
                    orders = self._get_daily_orders(tid, ct_id, case_no, current_year)
                    time.sleep(1)

                    if not orders:
                        empty_streak += 1
                        case_no += 1
                        continue

                    empty_streak = 0
                    for order in orders:
                        order_date = self._parse_date(order.get("dateoffiling", ""))
                        if order_date and order_date >= since.strftime("%Y-%m-%d"):
                            yield {
                                "tribunal_id": tid,
                                "tribunal_name": tname,
                                "case_type_id": ct_id,
                                "case_type_name": ct_name,
                                "case_year": current_year,
                                "case_no": case_no,
                                **order,
                            }
                    case_no += 1

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw order data into standard schema."""
        item_no = raw.get("item_no", "")
        if not item_no:
            return None

        tribunal_id = raw.get("tribunal_id", "")
        tribunal_name = raw.get("tribunal_name", "")
        case_no_str = raw.get("applicantno", "")
        applicant = raw.get("applicantName", "")
        respondent = raw.get("respondentName", "")
        pronounced_by = raw.get("pronouncedBy", "")
        date_str = raw.get("dateoffiling", "")
        pdf_url = raw.get("dailyOrderPdf", "")
        diary_no = raw.get("diaryno", "")

        date_iso = self._parse_date(date_str)

        title = f"{case_no_str} — {applicant} vs. {respondent}"

        # Download and extract PDF text
        text = None
        if pdf_url:
            text = self._download_and_extract_text(pdf_url)

        if not text:
            text = f"[Order PDF — text not extractable] {case_no_str} dated {date_str}"

        unique_id = f"DRAT-{tribunal_id}-{item_no}"

        return {
            "_id": unique_id,
            "_source": "IN/DRAT",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_iso,
            "url": pdf_url,
            "item_no": item_no,
            "tribunal_id": tribunal_id,
            "tribunal_name": tribunal_name,
            "case_number": case_no_str,
            "diary_number": diary_no,
            "case_type": raw.get("case_type_name", ""),
            "applicant": applicant,
            "respondent": respondent,
            "pronounced_by": pronounced_by,
        }


# ── CLI entry point ──────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="IN/DRAT data source bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Sample mode — fetch only 15 records")
    parser.add_argument("--full", action="store_true",
                        help="Full bootstrap (all tribunals)")
    args = parser.parse_args()

    scraper = DRATScraper()

    if args.command == "test":
        logger.info("Testing API connectivity...")
        tribunals = scraper._get_tribunals()
        if tribunals:
            logger.info("OK — %d tribunals found", len(tribunals))
            for t in tribunals[:5]:
                logger.info("  %s (ID=%s)", t["SchemaName"], t["schemeNameDrtId"])
            # Test one order fetch
            orders = scraper._get_daily_orders("1", "1", 1, 2024)
            logger.info("Test order fetch: %d orders for DRT-1/OA/1/2024", len(orders))
        else:
            logger.error("FAIL — could not reach API")
            sys.exit(1)
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info("Bootstrap complete: %s", json.dumps(stats, indent=2))
        return

    if args.command == "update":
        since = datetime.now(timezone.utc) - timedelta(days=90)
        stats = scraper.update(since=since)
        logger.info("Update complete: %s", json.dumps(stats, indent=2))
        return


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
