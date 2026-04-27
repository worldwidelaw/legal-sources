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

BASE_URL = "https://www.ilo.org/dyn/triblex/triblexmain"
DETAIL_URL = f"{BASE_URL}.detail?p_lang=en&p_judgment_no={{}}"
PDF_URL = f"{BASE_URL}.fullText?p_lang=en&p_judgment_no={{}}"
MAX_JUDGMENT_NO = 5203
RATE_LIMIT = 2  # seconds between requests


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
        for no in range(1, MAX_JUDGMENT_NO + 1):
            try:
                raw = self._fetch_judgment(no)
                if raw:
                    yield raw
                else:
                    logger.info(f"Skipping judgment #{no} (no data)")
            except Exception as e:
                logger.error(f"Error processing judgment #{no}: {e}")

            time.sleep(RATE_LIMIT)

            if no % 100 == 0:
                logger.info(f"Progress: {no}/{MAX_JUDGMENT_NO}")

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
            "url": f"https://www.ilo.org/dyn/triblex/triblexmain.detail?p_lang=en&p_judgment_no={judgment_no}",
            "judgment_no": judgment_no,
            "organization": org,
            "judges": raw.get("judges", ""),
            "original_language": raw.get("original_language", ""),
            "decision": raw.get("decision", ""),
            "summary": summary,
            "keywords": raw.get("keywords", ""),
        }


if __name__ == "__main__":
    if not PDF_AVAILABLE:
        print("ERROR: pypdf required for PDF text extraction", file=sys.stderr)
        sys.exit(1)

    scraper = ILOILOATScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
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

    elif cmd == "bootstrap":
        stats = scraper.bootstrap(sample_mode="--sample" in sys.argv, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
