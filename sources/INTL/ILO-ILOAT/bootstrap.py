#!/usr/bin/env python3
"""
INTL/ILO-ILOAT -- ILO Administrative Tribunal (TRIBLEX)

Fetches judgments from the TRIBLEX case-law database. Each judgment is a PDF
accessed via a predictable URL pattern. Metadata (organization, date, judges,
keywords, decision summary) is parsed from HTML detail pages.

~5,200 judgments from 1946 to present, covering employment disputes at 60+
international organizations (WHO, UNESCO, CERN, EPO, etc.).

Usage:
    python bootstrap.py bootstrap --sample   # Fetch 15 sample records
    python bootstrap.py bootstrap            # Full fetch all judgments
    python bootstrap.py test                 # Quick connectivity test
"""

import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
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
logger = logging.getLogger("legal-data-hunter.INTL.ILO-ILOAT")

# TRIBLEX moved from www.ilo.org to wwwex.ilo.org. www.ilo.org still answers, but
# its redirect inserts a trailing slash before the query string
# (".detail/?p_lang=en&...") which the Oracle PL/SQL gateway rejects with a 404 —
# that redirect is why an entire ID walk returned zero records (#1306).
BASE_URL = "https://wwwex.ilo.org/dyn/triblex/triblexmain"
DETAIL_URL = f"{BASE_URL}.detail?p_lang=en&p_judgment_no={{}}"
PDF_URL = f"{BASE_URL}.fullText?p_lang=en&p_judgment_no={{}}"
# Highest live judgment was ~5,325 as of 2026-08; walk a little past it so newly
# published judgments are picked up without a code change. Missing IDs in the
# range are simply skipped (the detail page 200s with no Organization).
MAX_JUDGMENT_NO = 5400
RATE_LIMIT = 1  # seconds between requests

# If the site moves again, fail loudly after this many consecutive misses with no
# successes rather than burning a 10h fleet slot walking every ID for nothing.
MAX_CONSECUTIVE_FAILURES = 150


def _parse_triblex_date(date_str: str) -> Optional[str]:
    """Parse TRIBLEX date format (DD.MM.YYYY) to ISO 8601."""
    if not date_str:
        return None
    m = re.match(r'(\d{2})\.(\d{2})\.(\d{4})', date_str.strip())
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return None


def _extract_text_from_pdf(content: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="INTL/ILO-ILOAT",
        source_id="",
        pdf_bytes=content,
        table="case_law",
    ) or ""

class ILOILOATScraper(BaseScraper):
    """Scraper for INTL/ILO-ILOAT -- ILO Administrative Tribunal."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml,*/*",
        })

    def _fetch_metadata(self, judgment_no: int) -> Optional[dict]:
        """Fetch metadata from the detail page for a judgment."""
        url = DETAIL_URL.format(judgment_no)
        try:
            resp = self.session.get(url, timeout=30, allow_redirects=True)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Detail page error for #{judgment_no}: {e}")
            return None

        text = resp.text
        meta = {"judgment_no": judgment_no}

        # Parse structured metadata from <li><em>Label:</em> Value</li> pattern
        m = re.search(r'<em>Organization:</em>\s*(.+?)</li>', text)
        if m:
            from html import unescape
            meta["organization"] = unescape(re.sub(r'<[^>]+>', '', m.group(1))).strip()

        m = re.search(r'<em>Date:</em>\s*([\d.]+)', text)
        if m:
            meta["date_raw"] = m.group(1).strip()

        m = re.search(r'<em>Judges?:</em>\s*(.+?)</li>', text)
        if m:
            meta["judges"] = re.sub(r'<[^>]+>', '', m.group(1)).strip()

        m = re.search(r'<em>Original:</em>\s*(\w+)', text)
        if m:
            meta["original_language"] = m.group(1).strip()

        # Parse text sections using BeautifulSoup
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(text, 'html.parser')
        page_text = soup.get_text()

        dec_m = re.search(r'Decision\s*\n(.+?)(?:Summary|$)', page_text, re.DOTALL)
        if dec_m:
            meta["decision"] = dec_m.group(1).strip()

        sum_m = re.search(r'Summary\s*\n(.+?)(?:Judgment keywords|$)', page_text, re.DOTALL)
        if sum_m:
            meta["summary"] = sum_m.group(1).strip()

        kw_m = re.search(r'Keywords\s*\n(.+?)(?:Consideration|$)', page_text, re.DOTALL)
        if kw_m:
            meta["keywords"] = kw_m.group(1).strip()

        return meta

    def _fetch_pdf_text(self, judgment_no: int) -> str:
        """Download and extract text from a judgment PDF."""
        url = PDF_URL.format(judgment_no)
        try:
            resp = self.session.get(url, timeout=120, allow_redirects=True)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"PDF download error for #{judgment_no}: {e}")
            return ""

        if resp.content[:4] != b'%PDF':
            content_preview = resp.content[:100].decode('utf-8', errors='replace')
            logger.warning(f"Not a PDF for #{judgment_no}: {content_preview}")
            return ""

        return _extract_text_from_pdf(resp.content)

    def _fetch_judgment(self, judgment_no: int) -> Optional[dict]:
        """Fetch a single judgment: metadata + PDF full text."""
        meta = self._fetch_metadata(judgment_no)
        if meta is None:
            return None

        time.sleep(RATE_LIMIT)

        text = self._fetch_pdf_text(judgment_no)
        if not text:
            logger.warning(f"No text extracted for judgment #{judgment_no}")
            return None

        meta["text"] = text
        return meta

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all TRIBLEX judgments from 1 to MAX_JUDGMENT_NO."""
        yielded = 0
        consecutive_failures = 0

        for no in range(1, MAX_JUDGMENT_NO + 1):
            try:
                raw = self._fetch_judgment(no)
                if raw:
                    yield raw
                    yielded += 1
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
                    logger.info(f"Skipping judgment #{no} (no data)")
            except Exception as e:
                consecutive_failures += 1
                logger.error(f"Error processing judgment #{no}: {e}")

            # A long miss streak with nothing at all extracted means the site
            # moved again (#1306). Abort loudly instead of spending ~10h of a
            # fleet slot walking every remaining ID for nothing. Gaps later in
            # the range are normal, so only bail while yielded is still zero.
            if yielded == 0 and consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                raise RuntimeError(
                    f"Aborting: {consecutive_failures} consecutive judgments returned no "
                    f"data and nothing has been extracted. The TRIBLEX URL template is "
                    f"probably stale again — re-verify {DETAIL_URL.format(4000)}"
                )

            time.sleep(RATE_LIMIT)

            if no % 100 == 0:
                logger.info(f"Progress: {no}/{MAX_JUDGMENT_NO} ({yielded} extracted)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent judgments. Iterate from MAX down until we hit older dates."""
        logger.info("Use bootstrap for full refresh. Incremental: iterate from latest judgment.")
        return
        yield

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw TRIBLEX judgment into standardized schema."""
        text = raw.get("text", "")
        if not text or len(text) < 100:
            return None

        judgment_no = raw.get("judgment_no", 0)
        org = raw.get("organization", "")
        date_raw = raw.get("date_raw", "")
        date_iso = _parse_triblex_date(date_raw)

        summary = raw.get("summary", "")
        title = f"ILOAT Judgment No. {judgment_no}"
        if org:
            title += f" — {org}"

        return {
            "_id": f"INTL-ILOAT-{judgment_no}",
            "_source": "INTL/ILO-ILOAT",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_iso,
            "url": DETAIL_URL.format(judgment_no),
            "judgment_no": str(judgment_no),
            "organization": org,
            "judges": raw.get("judges", ""),
            "original_language": raw.get("original_language", ""),
            "decision": raw.get("decision", ""),
            "summary": summary,
            "keywords": raw.get("keywords", ""),
        }


if __name__ == "__main__":
    scraper = ILOILOATScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|test] [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    sample = "--sample" in sys.argv

    if cmd == "test":
        print("Testing TRIBLEX connectivity...")
        try:
            meta = scraper._fetch_metadata(4900)
            if meta:
                print(f"OK: Judgment #4900 — {meta.get('organization', '?')}, {meta.get('date_raw', '?')}")
            else:
                print("FAIL: Could not fetch metadata")
                sys.exit(1)
        except Exception as e:
            print(f"FAIL: {e}")
            sys.exit(1)

    elif cmd in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(sample_mode="--sample" in sys.argv, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
