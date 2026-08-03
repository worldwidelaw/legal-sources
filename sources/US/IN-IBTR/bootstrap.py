#!/usr/bin/env python3
"""
US/IN-IBTR -- Indiana Board of Tax Review (Final Determinations)

Fetches the FULL TEXT of the final determinations of the Indiana Board of Tax
Review (IBTR) — the state administrative body that adjudicates appeals of
Indiana property-tax assessments (taxpayer / county assessor v. the other,
reviewing the county Property Tax Assessment Board of Appeals). Each final
determination resolves a specific assessment controversy, so the corpus is
case_law.

Source / access
---------------
Official site ``https://www.in.gov/ibtr/decisions/``. The index page links one
"<month>-<year>-decisions" page per month (~186 pages, 2002-present); each
monthly page links that month's final-determination PDFs under ``/ibtr/files/``.
The PDFs are born-digital text-layer documents. No JS, no CAPTCHA, no auth;
in.gov is reachable from ordinary clients.

Strategy:
  1. GET the index, collect every ``/ibtr/decisions/<month>-<year>-decisions``
     month-page link.
  2. GET each month page, collect every ``/ibtr/files/*.pdf`` link (skipping
     non-decision artefacts like "*Petition-Listing.pdf").
  3. Download each PDF, extract its text layer via common.pdf_extract, parse the
     petitioner/parties and date.

Usage:
  python3 bootstrap.py bootstrap            # Full pull (all determinations)
  python3 bootstrap.py bootstrap --sample   # Sample (newest months first)
  python3 bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python3 bootstrap.py test-api             # Connectivity / extraction test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import subprocess
import time
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common import pdf_extract

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.IN-IBTR")

SOURCE_ID = "US/IN-IBTR"
BASE_URL = "https://www.in.gov"
INDEX_PAGE = "/ibtr/decisions/"

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}
MONTH_ALT = "|".join(MONTHS)
# Any month/decision sub-page under /ibtr/decisions/. The slug format varies by
# era: "february-2026-decisions", "2008-april-decisions",
# "2002-january-february-decisions", "december-2008" (no suffix),
# "february-2025-decisions2" (deduped suffix). We accept any /ibtr/decisions/<slug>
# whose slug carries a 4-digit year and derive (year, month) from the slug.
DECISION_LINK_RE = re.compile(
    r'href=["\']([^"\']*?/ibtr/decisions/[^"\']+)["\']', re.I)
YEAR_RE = re.compile(r"(?:19|20)\d{2}")
# "Issued" date in the month-page table, e.g. 2/11/2026
DATE_MDY_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")
PDF_HREF_RE = re.compile(r'href=["\']([^"\']*?/ibtr/files/[^"\']+\.pdf)["\']', re.I)
TEXT_DATE_RE = re.compile(rf"\b({MONTH_ALT})\s+(\d{{1,2}}),\s+(\d{{4}})", re.I)
ORDINAL_DATE_RE = re.compile(
    rf"\b(\d{{1,2}})(?:st|nd|rd|th)?\s+day\s+of\s+({MONTH_ALT}),?\s+(\d{{4}})", re.I)
# Petition / parcel number embedded in the filename, e.g. 29-020-22-1-5-00005-24
PETITION_RE = re.compile(r"(\d{2}-\d{3}-\d{2}-\d-\d-\d{4,5}-\d{2})")
# Short cause-number variant some counties use, e.g. ...Gamso-00112-25
SHORT_PETITION_RE = re.compile(r"[-_](\d{4,5}-\d{2})(?:[-_]|$)")
# Filenames that are not individual determinations.
SKIP_NAME_RE = re.compile(
    r"(petition-list|cause-list|hearing-calendar|listing|index)", re.I)


class INIBTRScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120 Safari/537.36"
        )

    # ── HTTP via curl ─────────────────────────────────────────────────
    def _curl_bytes(self, url: str) -> Optional[bytes]:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "90", "-A", self._ua,
                     "-H", "Accept: */*", url],
                    capture_output=True, timeout=120,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    def _curl_text(self, url: str) -> Optional[str]:
        out = self._curl_bytes(url)
        return out.decode("utf-8", "replace") if out else None

    # ── Enumeration ───────────────────────────────────────────────────
    @staticmethod
    def _slug_year_month(slug: str) -> tuple[Optional[int], Optional[int]]:
        ym = YEAR_RE.search(slug)
        year = int(ym.group(0)) if ym else None
        month = None
        low = slug.lower()
        for name, num in MONTHS.items():
            if name in low:
                month = num
                break
        return year, month

    @staticmethod
    def _mdy_iso(m: re.Match) -> Optional[str]:
        mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mo <= 12 and 1 <= d <= 31 and 1990 <= y <= 2100:
            return f"{y:04d}-{mo:02d}-{d:02d}"
        return None

    def _month_pages(self) -> list[tuple[str, int, int]]:
        """Return [(month_page_url, year, month_num)], newest first."""
        html = self._curl_text(BASE_URL + INDEX_PAGE)
        if not html:
            return []
        seen: set[str] = set()
        out: list[tuple[str, int, int]] = []
        for href in DECISION_LINK_RE.findall(html):
            url = urllib.parse.urljoin(BASE_URL + INDEX_PAGE, href)
            path = urllib.parse.urlsplit(url).path.rstrip("/")
            if path == "/ibtr/decisions":
                continue
            slug = path.rsplit("/", 1)[-1]
            year, month = self._slug_year_month(slug)
            if not year:
                continue
            if url in seen:
                continue
            seen.add(url)
            out.append((url, year, month or 1))
        out.sort(key=lambda t: (t[1], t[2]), reverse=True)
        return out

    def _month_pdfs(self, month_url: str) -> list[tuple[str, Optional[str]]]:
        """Return [(pdf_url, issued_date_iso_or_None)] for one month page.

        The month pages are HTML tables (Petition # | Issued | Petitioner link |
        Issues); the authoritative determination date is the "Issued" M/D/YYYY in
        the cell immediately before the petitioner link, so we associate each PDF
        href with the last date string that occurs before it in the document.
        """
        html = self._curl_text(month_url)
        if not html:
            return []
        dates = [(m.start(), self._mdy_iso(m)) for m in DATE_MDY_RE.finditer(html)]
        dates = [(p, d) for p, d in dates if d]
        seen: set[str] = set()
        out: list[tuple[str, Optional[str]]] = []
        for m in PDF_HREF_RE.finditer(html):
            url = urllib.parse.urljoin(month_url, m.group(1))
            name = urllib.parse.unquote(url.rsplit("/", 1)[-1])
            if SKIP_NAME_RE.search(name):
                continue
            if url in seen:
                continue
            seen.add(url)
            issued = None
            for pos, d in dates:
                if pos < m.start():
                    issued = d
                else:
                    break
            out.append((url, issued))
        return out

    def discover_documents(self, sample: bool = False) -> Generator[tuple, None, None]:
        for month_url, year, month in self._month_pages():
            for pdf_url, issued in self._month_pdfs(month_url):
                yield (pdf_url, year, month, issued)

    # ── Parsing ───────────────────────────────────────────────────────
    @staticmethod
    def _slug(pdf_url: str) -> str:
        stem = urllib.parse.unquote(pdf_url.rsplit("/", 1)[-1])
        stem = re.sub(r"\.pdf$", "", stem, flags=re.I)
        stem = re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-")
        return stem[:180]

    @staticmethod
    def _petition(slug: str) -> Optional[str]:
        m = PETITION_RE.search(slug)
        return m.group(1) if m else None

    @classmethod
    def _party_from_slug(cls, slug: str) -> str:
        # Drop everything from the first petition-number group onward.
        m = PETITION_RE.search(slug)
        head = slug[:m.start()] if m else slug
        head = head.rstrip("-_ ")
        head = head.replace("_", " ").replace("-", " ")
        head = re.sub(r"\s+", " ", head).strip()
        return head

    @classmethod
    def _parse_date(cls, text: str, year: int, month: int) -> Optional[str]:
        m = ORDINAL_DATE_RE.search(text) or None
        if m:
            day = int(m.group(1)); mon = MONTHS[m.group(2).lower()]; yr = int(m.group(3))
            if 1995 <= yr <= 2100 and 1 <= day <= 31:
                return f"{yr:04d}-{mon:02d}-{day:02d}"
        m = TEXT_DATE_RE.search(text)
        if m:
            mon = MONTHS[m.group(1).lower()]; day = int(m.group(2)); yr = int(m.group(3))
            if 1995 <= yr <= 2100 and 1 <= day <= 31:
                return f"{yr:04d}-{mon:02d}-{day:02d}"
        # Fallback: first of the listing month.
        return f"{year:04d}-{month:02d}-01"

    # ── Build / normalize ─────────────────────────────────────────────
    def _build_raw(self, pdf_url: str, year: int, month: int,
                   issued: Optional[str] = None) -> Optional[dict]:
        pdf_bytes = self._curl_bytes(pdf_url)
        if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
            logger.warning(f"PDF download failed or not a PDF: {pdf_url}")
            return None
        slug = self._slug(pdf_url)
        text = pdf_extract.extract_pdf_markdown(
            SOURCE_ID, slug, pdf_bytes=pdf_bytes,
            table="case_law", force=True,
        )
        if not text or len(text.strip()) < 200:
            logger.warning(f"No usable text (scanned?) for {pdf_url} "
                           f"({len(text) if text else 0} chars)")
            return None
        # The month-page "Issued" date is authoritative; fall back to a date
        # parsed from the PDF body, then to the first of the listing month.
        date = issued or self._parse_date(text, year, month)
        return {
            "slug": slug,
            "petition": self._petition(slug),
            "party": self._party_from_slug(slug),
            "text": text.strip(),
            "url": pdf_url,
            "date": date,
        }

    def normalize(self, raw: dict) -> dict:
        party = (raw.get("party") or "").strip()
        if party:
            title = f"{party} — Indiana Board of Tax Review Final Determination"
        else:
            title = "Indiana Board of Tax Review Final Determination"
        petition = raw.get("petition")
        if petition:
            title += f" (Petition {petition})"
        return {
            "_id": f"{SOURCE_ID}/{raw['slug']}",
            "_source": "US/IN-IBTR",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "docket_number": petition,
            "court": "Indiana Board of Tax Review",
            "petitioner": party or None,
            "title": title[:300],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-IN",
        }

    # ── Iteration ─────────────────────────────────────────────────────
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for pdf_url, year, month, issued in self.discover_documents(sample=sample):
            raw = self._build_raw(pdf_url, year, month, issued)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw

    def test_api(self) -> bool:
        logger.info("Testing Indiana Board of Tax Review index + PDF extraction...")
        try:
            pages = self._month_pages()
            if not pages:
                logger.error("  No month pages discovered")
                return False
            logger.info(f"  Discovered {len(pages)} month pages")
            emitted = 0
            for pdf_url, year, month, issued in self.discover_documents(sample=True):
                raw = self._build_raw(pdf_url, year, month, issued)
                if raw:
                    logger.info(f"  Full text OK: {raw['party'][:40]} "
                                f"[{raw.get('petition')}] ({len(raw['text'])} chars)")
                    emitted += 1
                    if emitted >= 3:
                        logger.info("API test PASSED")
                        return True
            logger.error("  No usable full-text documents extracted")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/IN-IBTR bootstrap")
    parser.add_argument("command",
                        choices=["bootstrap", "bootstrap-fast", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = INIBTRScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
