#!/usr/bin/env python3
"""
BH/MIA-Gazette -- Bahrain Official Gazette

Fetches Official Gazette issues from the Bahrain LLOC portal.

Strategy:
  - Download PDFs from https://www.lloc.gov.bh/OG/{issue_number}.pdf
  - Extract text with PyMuPDF (fitz)
  - Parse date from header line pattern "YYYY  Month Day  DayOfWeek- NNNN  العدد"
  - Issues 2917–3884+ available (sequential numbering)

Data: ~968 gazette issues (laws, decrees, royal orders, announcements)
License: Bahrain Government Open Data (commercial use OK with attribution)
Rate limit: 0.5 req/sec

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import io
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

try:
    import fitz  # PyMuPDF
except ImportError:
    print("ERROR: PyMuPDF not installed. Run: pip3 install PyMuPDF")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BH.MIA-Gazette")

BASE_URL = "https://www.lloc.gov.bh"
FIRST_ISSUE = 2917

ARABIC_MONTHS = {
    'يناير': '01', 'فبراير': '02', 'مارس': '03', 'أبريل': '04',
    'ابريل': '04', 'مايو': '05', 'يونيو': '06', 'يوليو': '07',
    'أغسطس': '08', 'اغسطس': '08', 'سبتمبر': '09', 'أكتوبر': '10',
    'اكتوبر': '10', 'نوفمبر': '11', 'ديسمبر': '12',
}


# Map Arabic-Indic (U+0660–U+0669) and Eastern-Arabic/Persian (U+06F0–U+06F9)
# digits to ASCII. Python's \d matches these, so a captured year like "٢٠١٣"
# would otherwise leak through into the ISO date string and break Postgres date
# parsing on ingest (issue #866).
_ARABIC_DIGITS = str.maketrans(
    "٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹",
    "01234567890123456789",
)


def _ascii_digits(s: str) -> str:
    """Convert any Arabic-Indic / Eastern-Arabic digits in s to ASCII."""
    return s.translate(_ARABIC_DIGITS)


def extract_date_from_text(text: str) -> Optional[str]:
    """Extract publication date from gazette header text.

    Typical pattern: '2026  مايو21  الخميس- 3884  العدد'
    """
    for month_ar, month_num in ARABIC_MONTHS.items():
        pattern = rf'(\d{{4}})\s+{re.escape(month_ar)}\s*(\d{{1,2}})'
        m = re.search(pattern, text)
        if m:
            year = _ascii_digits(m.group(1))
            day = _ascii_digits(m.group(2)).zfill(2)
            return f"{year}-{month_num}-{day}"
    return None


class BHMIAGazetteScraper(BaseScraper):
    """
    Scraper for BH/MIA-Gazette -- Bahrain Official Gazette.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
        })

    def _find_latest_issue(self) -> int:
        """Find the latest available gazette issue number."""
        lo, hi = 3880, 4100
        latest = lo
        for n in range(hi, lo - 1, -1):
            try:
                resp = self.session.head(
                    f"{BASE_URL}/OG/{n}.pdf", timeout=15, allow_redirects=True
                )
                if resp.status_code == 200:
                    return n
            except requests.RequestException:
                continue
        # Fallback: scan forward from known recent
        for n in range(3884, 4100):
            try:
                resp = self.session.head(
                    f"{BASE_URL}/OG/{n}.pdf", timeout=15, allow_redirects=True
                )
                if resp.status_code == 200:
                    latest = n
                else:
                    break
            except requests.RequestException:
                break
        return latest

    def _download_pdf(self, issue_num: int) -> Optional[bytes]:
        """Download a gazette PDF, with retries."""
        url = f"{BASE_URL}/OG/{issue_num}.pdf"
        for attempt in range(3):
            try:
                self.rate_limiter.wait()
                resp = self.session.get(url, timeout=120)
                if resp.status_code == 200 and len(resp.content) > 1000:
                    return resp.content
                if resp.status_code == 404:
                    return None
                logger.warning(f"Issue {issue_num}: HTTP {resp.status_code}")
            except requests.RequestException as e:
                logger.warning(f"Issue {issue_num} attempt {attempt+1}: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
        return None

    def _extract_text(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF bytes using PyMuPDF."""
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        pages = []
        for page in doc:
            text = page.get_text()
            if text and text.strip():
                pages.append(text.strip())
        doc.close()
        return "\n\n".join(pages)

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all gazette issues from oldest to newest."""
        latest = self._find_latest_issue()
        logger.info(f"Fetching gazette issues {FIRST_ISSUE}–{latest}")
        for issue_num in range(FIRST_ISSUE, latest + 1):
            pdf_bytes = self._download_pdf(issue_num)
            if pdf_bytes is None:
                logger.debug(f"Issue {issue_num}: not available, skipping")
                continue
            text = self._extract_text(pdf_bytes)
            if not text or len(text) < 50:
                logger.warning(f"Issue {issue_num}: insufficient text ({len(text)} chars)")
                continue
            yield {
                "issue_number": issue_num,
                "pdf_size": len(pdf_bytes),
                "text": text,
            }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield recently published gazette issues."""
        latest = self._find_latest_issue()
        for issue_num in range(max(FIRST_ISSUE, latest - 20), latest + 1):
            pdf_bytes = self._download_pdf(issue_num)
            if pdf_bytes is None:
                continue
            text = self._extract_text(pdf_bytes)
            if not text or len(text) < 50:
                continue
            yield {
                "issue_number": issue_num,
                "pdf_size": len(pdf_bytes),
                "text": text,
            }

    def normalize(self, raw: dict) -> dict:
        """Normalize a gazette issue into standard schema."""
        issue_num = raw["issue_number"]
        text = raw["text"]
        pub_date = extract_date_from_text(text)

        return {
            "_id": f"BH-OG-{issue_num}",
            "_source": "BH/MIA-Gazette",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "issue_number": issue_num,
            "title": f"Official Gazette of the Kingdom of Bahrain - Issue {issue_num}",
            "text": text,
            "date": pub_date,
            "url": f"{BASE_URL}/OG/{issue_num}.pdf",
            "pdf_size_bytes": raw.get("pdf_size"),
            "language": "ar",
        }


# ── CLI ─────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="BH/MIA-Gazette bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch 15 sample records only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = BHMIAGazetteScraper()

    if args.command == "test-api":
        logger.info("Testing API connectivity...")
        resp = scraper.session.head(f"{BASE_URL}/OG/3884.pdf", timeout=15)
        logger.info(f"HEAD /OG/3884.pdf → {resp.status_code}")
        if resp.status_code == 200:
            logger.info("API OK")
        else:
            logger.error("API FAILED")
            sys.exit(1)
        return

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    count = 0
    limit = 15 if args.sample else 999999

    if args.sample:
        # For sample mode, grab recent issues (latest 20 to ensure we get 15)
        latest = scraper._find_latest_issue()
        logger.info(f"Sample mode: fetching from issues {latest-19}–{latest}")
        gen_range = range(latest, latest - 20, -1)
        for issue_num in gen_range:
            if count >= limit:
                break
            pdf_bytes = scraper._download_pdf(issue_num)
            if pdf_bytes is None:
                continue
            text = scraper._extract_text(pdf_bytes)
            if not text or len(text) < 50:
                logger.warning(f"Issue {issue_num}: insufficient text, skipping")
                continue
            raw = {"issue_number": issue_num, "pdf_size": len(pdf_bytes), "text": text}
            record = scraper.normalize(raw)
            out_path = sample_dir / f"{record['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info(
                f"[{count}/{limit}] Issue {issue_num}: {len(text)} chars, "
                f"date={record.get('date')}"
            )
    else:
        for raw in scraper.fetch_all():
            if count >= limit:
                break
            record = scraper.normalize(raw)
            out_path = sample_dir / f"{record['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            if count % 50 == 0:
                logger.info(f"Progress: {count} issues processed")

    logger.info(f"Done. {count} gazette issues saved to {sample_dir}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
