#!/usr/bin/env python3
"""
PK/IHC -- Islamabad High Court Judgments

Fetches judgments from the Islamabad High Court MIS portal.

Strategy:
  - ASMX web service endpoints return JSON with full case metadata
  - srchDecision1 for keyword/year search (up to 2000 results per query)
  - srchDecision with date parameter for date-based iteration
  - PDF judgments at /attachments/judgements/{id}/{num}/{filename}.pdf
  - Text extraction using PyMuPDF (fitz)

Endpoints:
  - Search: https://mis.ihc.gov.pk/ihc.asmx/srchDecision
  - Keyword: https://mis.ihc.gov.pk/ihc.asmx/srchDecision1
  - Years: https://mis.ihc.gov.pk/ihc.asmx/FillYear
  - PDF: https://mis.ihc.gov.pk/attachments/judgements/{casecode}/{num}/{filename}.pdf

Data:
  - Thousands of judgments (2010-present)
  - Rich metadata: parties, bench, judge, subject, headnotes, citations
  - Full text in English
  - License: Public court records

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List, Set

import requests
try:
    import fitz  # PyMuPDF — optional; falls back to shared extractor if absent (issue #816)
except ImportError:
    fitz = None

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PK.IHC")

BASE_URL = "https://mis.ihc.gov.pk"
SEARCH_URL = BASE_URL + "/ihc.asmx/srchDecision"
KEYWORD_URL = BASE_URL + "/ihc.asmx/srchDecision1"
YEARS_URL = BASE_URL + "/ihc.asmx/FillYear"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": BASE_URL + "/frmSrchOrdr",
    "X-Requested-With": "XMLHttpRequest",
}

# Common legal keywords for comprehensive sampling
SAMPLE_KEYWORDS = [
    "constitution", "petition", "criminal", "appeal", "habeas",
    "fundamental", "taxation", "contract", "property", "family",
]


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from a PDF.

    Prefers PyMuPDF (fitz) when installed; otherwise falls back to the shared
    pdfplumber/pypdf extractor in common.pdf_extract, so the scraper still runs
    on hosts without PyMuPDF (issue #816).
    """
    if fitz is not None:
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            parts = []
            for page in doc:
                text = page.get_text()
                if text:
                    parts.append(text.strip())
            doc.close()
            joined = "\n\n".join(parts)
            if joined.strip():
                return joined
        except Exception as e:
            logger.warning(f"PyMuPDF extraction failed: {e}")
    # Fallback: shared extractor (pdfplumber/pypdf — always in requirements.txt)
    try:
        from common.pdf_extract import extract_pdf_markdown
        text = extract_pdf_markdown(
            source="PK/IHC", source_id="_pdf", pdf_bytes=pdf_bytes, force=True
        )
        return text or ""
    except Exception as e:
        logger.warning(f"pdf_extract fallback failed: {e}")
        return ""


def _parse_date(date_str: str) -> Optional[str]:
    """Parse IHC date format (DD-MMM-YYYY) to ISO 8601."""
    if not date_str or date_str == "-":
        return None
    try:
        dt = datetime.strptime(date_str.strip(), "%d-%b-%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def _parse_json_date(json_date: str) -> Optional[str]:
    """Parse .NET JSON date (/Date(timestamp)/) to ISO 8601."""
    if not json_date:
        return None
    m = re.search(r"/Date\((\d+)\)/", str(json_date))
    if m:
        ts = int(m.group(1)) / 1000
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    return None


class PKIHCScraper(BaseScraper):
    """
    Scraper for PK/IHC -- Islamabad High Court Judgments.
    Country: PK
    URL: https://mis.ihc.gov.pk
    Data types: case_law
    Auth: none (public court records)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._seen_ids: Set[int] = set()
        # Checkpoint of fully-processed date strings, so a torn-down /
        # re-launched run resumes instead of re-fetching the whole corpus
        # from scratch (issue #961). The day-by-day full-corpus scan is the
        # slow part; skipping completed days makes a re-run pick up where the
        # previous one stopped.
        self._checkpoint_path = self.source_dir / "data" / "ihc_checkpoint.json"
        self._completed_dates: Set[str] = self._load_checkpoint()

    def _load_checkpoint(self) -> Set[str]:
        """Load the set of date strings already fully processed."""
        try:
            with open(self._checkpoint_path) as f:
                data = json.load(f)
            dates = set(data.get("completed_dates", []))
            if dates:
                logger.info(f"Resuming from checkpoint: {len(dates)} dates already done")
            return dates
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return set()

    def _save_checkpoint(self) -> None:
        """Persist the set of fully-processed date strings."""
        try:
            self._checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._checkpoint_path, "w") as f:
                json.dump({"completed_dates": sorted(self._completed_dates)}, f)
        except OSError as e:
            logger.warning(f"Could not write checkpoint: {e}")

    def _search_by_keyword(self, keyword: str, year: str) -> List[Dict]:
        """Search judgments by keyword and year."""
        self.rate_limiter.wait()
        try:
            self.session.headers["Content-Type"] = "application/json; charset=utf-8"
            resp = self.session.post(
                KEYWORD_URL,
                json={
                    "PKYWRD": keyword,
                    "PCSEKWYR": year,
                    "PAFR": "9999",
                    "PISSWS": "0",
                },
                timeout=60,
            )
            resp.raise_for_status()
            d = resp.json().get("d", "")
            if not d or d == '"empty"':
                return []
            return json.loads(d) if isinstance(d, str) else d
        except Exception as e:
            logger.error(f"Search failed for keyword={keyword}, year={year}: {e}")
            return []

    def _search_by_date(self, date_str: str) -> List[Dict]:
        """Search judgments by decision date (format: DD-MMM-YYYY)."""
        self.rate_limiter.wait()
        try:
            self.session.headers["Content-Type"] = "application/json; charset=utf-8"
            resp = self.session.post(
                SEARCH_URL,
                json={
                    "PCASENO": "0",
                    "PJUG": "0",
                    "PADV": "0",
                    "PYEAR": "0",
                    "pPrty": "",
                    "PDDATE": date_str,
                    "PLANDMARK": "0",
                    "PAFR": "9999",
                },
                timeout=60,
            )
            resp.raise_for_status()
            d = resp.json().get("d", "")
            if not d or d == '"empty"':
                return []
            results = json.loads(d) if isinstance(d, str) else d
            return results if isinstance(results, list) else []
        except Exception as e:
            logger.error(f"Date search failed for {date_str}: {e}")
            return []

    def _download_pdf_text(self, attachment_path: str) -> Optional[str]:
        """Download a PDF judgment and extract text."""
        if not attachment_path or attachment_path == "-":
            return None
        try:
            url = f"{BASE_URL}{attachment_path}"
            self.rate_limiter.wait()
            # Remove Content-Type for GET requests
            ct = self.session.headers.pop("Content-Type", None)
            resp = self.session.get(url, timeout=120)
            if ct:
                self.session.headers["Content-Type"] = ct

            if resp.status_code != 200:
                return None
            if resp.content[:5] != b"%PDF-":
                return None

            text = extract_pdf_text(resp.content)
            if text and len(text.strip()) > 100:
                return text.strip()
            return None

        except Exception as e:
            logger.debug(f"PDF download failed: {e}")
            return None

    def _process_record(self, rec: Dict) -> Optional[Dict]:
        """Process a single search result, deduplicating by O_ID."""
        o_id = rec.get("O_ID")
        if not o_id or o_id in self._seen_ids:
            return None
        self._seen_ids.add(o_id)

        attachment = rec.get("ATTACHMENTS", "")
        text = self._download_pdf_text(attachment)
        if not text:
            return None

        rec["text"] = text
        return rec

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all IHC judgments by iterating through dates."""
        # Get available years
        try:
            self.rate_limiter.wait()
            self.session.headers["Content-Type"] = "application/json; charset=utf-8"
            resp = self.session.post(YEARS_URL, json={}, timeout=30)
            years_data = resp.json().get("d", [])
            if isinstance(years_data, str):
                years_data = json.loads(years_data)
            years = sorted([int(y["Code"]) for y in years_data], reverse=True)
        except Exception:
            years = list(range(2026, 2009, -1))

        logger.info(f"Iterating through years: {years[0]}-{years[-1]}")

        for year in years:
            # Iterate by month to stay under 2000 limit
            for month in range(1, 13):
                # Get days in month
                if month == 12:
                    next_month = datetime(year + 1, 1, 1)
                else:
                    next_month = datetime(year, month + 1, 1)
                days_in_month = (next_month - datetime(year, month, 1)).days

                for day in range(1, days_in_month + 1):
                    dt = datetime(year, month, day)
                    if dt > datetime.now():
                        break
                    date_str = dt.strftime("%d-%b-%Y").upper()

                    # Resume support: skip dates fully processed by a prior run
                    if date_str in self._completed_dates:
                        continue

                    results = self._search_by_date(date_str)
                    if results:
                        logger.info(f"  {date_str}: {len(results)} judgments")
                        for rec in results:
                            processed = self._process_record(rec)
                            if processed:
                                yield processed

                    # Mark the date done only after all its records are yielded,
                    # checkpointing every ~25 dates to bound disk writes. A crash
                    # mid-date simply re-fetches that date next run (the loader
                    # dedups on case_id with ON CONFLICT DO NOTHING).
                    self._completed_dates.add(date_str)
                    if len(self._completed_dates) % 25 == 0:
                        self._save_checkpoint()

            # Flush checkpoint at each year boundary too.
            self._save_checkpoint()

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent judgments by date."""
        current = since
        now = datetime.now()
        while current <= now:
            date_str = current.strftime("%d-%b-%Y").upper()
            results = self._search_by_date(date_str)
            for rec in results:
                processed = self._process_record(rec)
                if processed:
                    yield processed
            current += timedelta(days=1)

    def normalize(self, raw: dict) -> dict:
        """Transform raw IHC record into standard schema."""
        title = raw.get("TITLE", "").strip()
        if not title:
            title = raw.get("PARTIES", "Unknown Case")

        date_str = _parse_date(raw.get("DDATE", ""))
        if not date_str:
            date_str = _parse_json_date(raw.get("O_DDATE", ""))

        case_number = raw.get("CASENO", "")
        case_id = f"IHC-{raw.get('O_ID', '')}"

        headnote = raw.get("HEADNOTE", "")
        if headnote == "-":
            headnote = ""
        ihc_headnote = raw.get("O_IHC_HEADNOTE", "")
        if ihc_headnote == "-":
            ihc_headnote = ""

        attachment = raw.get("ATTACHMENTS", "")
        url = f"{BASE_URL}{attachment}" if attachment and attachment != "-" else ""

        return {
            "_id": case_id,
            "_source": "PK/IHC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "case_id": case_id,
            "title": title,
            "text": raw.get("text", ""),
            "date": date_str,
            "url": url,
            "case_number": case_number,
            "parties": raw.get("PARTIES", ""),
            "bench": raw.get("BENCHNAME", ""),
            "author_judge": raw.get("AUTHOR_JUDGES", ""),
            "subject": raw.get("O_SUBJECT", ""),
            "headnote": headnote,
            "ihc_headnote": ihc_headnote,
            "under_section": raw.get("O_UNDERSECTION", ""),
            "remarks": raw.get("O_REMARKS", ""),
            "citation": raw.get("O_CITATION", ""),
            "court": "Islamabad High Court",
            "country": "PK",
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import argparse

    parser = argparse.ArgumentParser(description="PK/IHC scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (10+ records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap (all records)")
    args = parser.parse_args()

    scraper = PKIHCScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        results = scraper._search_by_keyword("constitution", "2024")
        logger.info(f"Test OK: {len(results)} results for 'constitution' 2024")
        if results:
            text = scraper._download_pdf_text(results[0].get("ATTACHMENTS", ""))
            logger.info(f"PDF test: {len(text) if text else 0} chars")
        sys.exit(0)

    if args.command == "bootstrap":
        if args.sample:
            # For sample mode, use keyword search (faster than date iteration)
            logger.info("Sample mode: using keyword search")
            sample_records = []
            for keyword in SAMPLE_KEYWORDS:
                if len(sample_records) >= 12:
                    break
                results = scraper._search_by_keyword(keyword, "2024")
                for rec in results[:3]:
                    if len(sample_records) >= 12:
                        break
                    processed = scraper._process_record(rec)
                    if processed:
                        normalized = scraper.normalize(processed)
                        sample_records.append(normalized)
                        logger.info(
                            f"  Sample {len(sample_records)}: {normalized['title'][:60]} "
                            f"({len(normalized['text'])} chars)"
                        )

            # Save samples
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)
            for i, rec in enumerate(sample_records):
                with open(sample_dir / f"record_{i:04d}.json", "w") as f:
                    json.dump(rec, f, indent=2, ensure_ascii=False)
            with open(sample_dir / "all_samples.json", "w") as f:
                json.dump(sample_records, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {len(sample_records)} samples to {sample_dir}")

            # Also run bootstrap for status tracking
            result = scraper.bootstrap(sample_mode=True, sample_size=12)
            logger.info(f"Bootstrap result: {result}")
        else:
            result = scraper.bootstrap(sample_mode=False)
            logger.info(f"Bootstrap result: {result}")

    elif args.command == "update":
        result = scraper.update()
        logger.info(f"Update result: {result}")
