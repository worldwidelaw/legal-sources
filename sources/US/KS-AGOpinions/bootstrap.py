#!/usr/bin/env python3
"""
US/KS-AGOpinions -- Kansas Attorney General Opinions

Fetches the full text of formal legal opinions issued by the Kansas
Attorney General. Each opinion answers a legal question posed by a
public official and constitutes an authoritative (advisory) state legal
interpretation (doctrine).

The Kansas AG's own site (ag.ks.gov) bot-blocks datacenter/automated
access (HTTP 403), but the complete opinion corpus is mirrored as an
open, static, full-text archive by Washburn University School of Law
Library at ksag.washburnlaw.edu — one directory per year, each holding
the opinions as text PDFs (digitally produced, real text layer, no OCR
needed). Coverage runs 1974-present.

Strategy:
  1. Walk year directories /opinions/{YEAR}/ for YEAR in 1974..current.
     Each year index lists its opinions as {YEAR}-NNN.pdf links.
  2. Download each PDF and extract its text via the shared
     common.pdf_extract.extract_pdf_markdown helper (OOM-hardened).
  3. Normalize into the standard doctrine schema.

Usage:
  python bootstrap.py bootstrap            # Full pull (all years)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.KS-AGOpinions")

BASE_URL = "https://ksag.washburnlaw.edu"
FIRST_YEAR = 1974
# Walk through the current year; out-of-range years simply 404 / list nothing.
CURRENT_YEAR = datetime.now(timezone.utc).year

# Opinion files are named {YEAR}-NNN.pdf (optionally with a trailing
# letter for supplements/corrections, e.g. 1980-123a.pdf).
PDF_RE = re.compile(r'href="((\d{4})-(\d+[a-z]?)\.pdf)"', re.IGNORECASE)

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}


def clean_text(text: str) -> str:
    """Normalize whitespace; strip pdfplumber (cid:N) artefacts."""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\(cid:\d+\)", " ", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_opinion_date(text: str, year: int) -> str | None:
    """Extract the issued date from the opinion's first lines.

    Kansas opinions open with a line like 'January 10, 2020'. Fall back
    to year-only (Jan 1) if no full date is recoverable.
    """
    head = text[:600]
    m = re.search(
        r"\b(January|February|March|April|May|June|July|August|"
        r"September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})",
        head, re.IGNORECASE,
    )
    if m:
        mon = MONTHS[m.group(1).lower()]
        day = int(m.group(2))
        yr = int(m.group(3))
        if 1 <= day <= 31 and FIRST_YEAR <= yr <= CURRENT_YEAR + 1:
            return f"{yr:04d}-{mon:02d}-{day:02d}"
    return f"{year:04d}-01-01"


def opinion_number(year: int, num: str) -> str:
    """Human opinion number, e.g. '2020-1' from filename 2020-001."""
    n = num.lstrip("0") or "0"
    return f"{year}-{n}"


class KSAGOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=60,
        )
        self.delay = 1.0

    def _get(self, url: str, retries: int = 4) -> str:
        """Fetch a URL (HTML) with rate limiting and retry/backoff."""
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp.text
                if resp.status_code == 404:
                    return ""
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return ""

    def _year_index_url(self, year: int) -> str:
        return f"{BASE_URL}/opinions/{year}/"

    def parse_year_index(self, html: str, year: int) -> list:
        """Return ordered, de-duplicated list of (filename, num) for a year."""
        if not html:
            return []
        out = []
        seen = set()
        for m in PDF_RE.finditer(html):
            fname, fyear, num = m.group(1), m.group(2), m.group(3)
            if int(fyear) != year:
                continue
            key = fname.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append((fname, num))
        # Sort by numeric portion for stable ordering. Strip ALL non-digits
        # (num may carry an upper- or lower-case letter suffix, e.g. 029A) so
        # int() never sees a stray letter — issue #1145 crashed on int('029A').
        out.sort(key=lambda t: (int(re.sub(r"\D", "", t[1]) or 0), t[1]))
        return out

    def _build_record(self, year: int, fname: str, num: str) -> dict | None:
        url = f"{BASE_URL}/opinions/{year}/{fname}"
        try:
            raw = extract_pdf_markdown(url, "US/KS-AGOpinions", pdf_url=url,
                                       table="legislation")
        except Exception as e:
            logger.warning(f"PDF extract error {fname}: {e}")
            return None
        text = clean_text(raw or "")
        if not text or len(text) < 200:
            logger.warning(f"No usable text for {fname} ({len(text)} chars)")
            return None
        number = opinion_number(year, num)
        date_iso = parse_opinion_date(text, year)
        return self.normalize({
            "number": number,
            "fname": fname,
            "text": text,
            "date": date_iso,
            "url": url,
        })

    def test_api(self) -> bool:
        """Test connectivity, year-index parse, and PDF text extraction."""
        logger.info("Testing Kansas AG opinions archive...")
        try:
            html = self._get(self._year_index_url(2020))
            ops = self.parse_year_index(html, 2020)
            if not ops:
                logger.error("  Year index parse returned no opinions")
                return False
            logger.info(f"  Year index parse OK ({len(ops)} opinions for 2020)")

            rec = self._build_record(2020, ops[0][0], ops[0][1])
            if rec and rec["text"] and len(rec["text"]) > 200:
                logger.info(f"  PDF text extraction OK ({len(rec['text'])} chars)")
            else:
                logger.error("  PDF text extraction failed or too short")
                return False

            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard doctrine schema."""
        number = raw["number"]
        return {
            "_id": f"US/KS-AGOpinions/{raw['fname'].replace('.pdf', '')}",
            "_source": "US/KS-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "title": f"Kansas Attorney General Opinion No. {number}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
        }

    def _iter_docs(self, sample: bool = False) -> Generator[dict, None, None]:
        """Iterate opinion records by walking year directories newest-first."""
        emitted = 0
        for year in range(CURRENT_YEAR, FIRST_YEAR - 1, -1):
            html = self._get(self._year_index_url(year))
            ops = self.parse_year_index(html, year)
            if not ops:
                continue
            logger.info(f"Year {year}: {len(ops)} opinions")
            for fname, num in ops:
                rec = self._build_record(year, fname, num)
                if rec:
                    yield rec
                    emitted += 1
                    if sample and emitted >= 12:
                        return

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._iter_docs(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_docs(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch opinions issued on/after `since` (ISO date)."""
        for record in self.fetch_all():
            if not since or (record.get("date") and record["date"] >= since):
                yield record


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/KS-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = KSAGOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    gen = scraper.fetch_sample() if args.sample else scraper.fetch_all()

    count = 0
    for record in gen:
        safe_id = record["_id"].replace("/", "_")
        out_path = sample_dir / f"{safe_id}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")

    logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
