#!/usr/bin/env python3
"""
US/KY-TaxAppeals -- Kentucky Board of Tax Appeals, Final Orders

Fetches the full text of the Final Orders issued by the Kentucky Board of Tax
Appeals (BTA), the Commonwealth's independent quasi-judicial tribunal that
hears taxpayer appeals from Department of Revenue assessments/refund denials
(state tax = "Revenue" cases) and from local property-tax valuation and
classification determinations (Property-Valuation-Administrator appeals). The
BTA now sits inside the Public Protection Cabinet's Office of Claims and
Appeals (OCA); each Final Order finally adjudicates a specific contested case
= case_law, and they are public Kentucky state-government works (pd-us,
government-edicts doctrine).

Access (no JavaScript needed, no CAPTCHA, no auth):
  The OCA case search at https://kycc.ky.gov/claims/search.aspx is a classic
  ASP.NET WebForms page. Selecting a Tax Year and pressing Search posts back
  and re-renders the results table server-side (all rows on one page, no
  pagination). Each result row exposes a "View Final Order" hyperlink whose
  href is a direct, born-digital PDF of the Board's order:
      /claims/FinalOrder/{APPEAL-NO}-{DOCID}.pdf
  e.g. FinalOrder/24-R-010-43207.pdf  (24-R-010 = 2024 Revenue appeal #010).
  A case with more than one order exposes more than one link. The row also
  carries: Tax Year, County, Appeal Type (Revenue / Property / ...), Status,
  Tax Payer Name, K# (appeal number) and Date Filed.

  The searchable corpus spans the tax years offered in the TxYear dropdown
  (2021-2026 as of 2026-07); ~140 orders per recent year.

Strategy:
  1. GET search.aspx once; capture the ASP.NET hidden fields
     (__VIEWSTATE, __VIEWSTATEGENERATOR, __EVENTVALIDATION).
  2. For each tax year, POST the Search with TxYear set; parse the results
     table rows, pairing every FinalOrder PDF link with its row metadata.
  3. Download each PDF and extract text via the shared OOM-hardened
     common.pdf_extract helper (born-digital, no OCR). date = Date Filed.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import html as _htmllib
import time
import urllib.parse
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
logger = logging.getLogger("legal-data-hunter.US.KY-TaxAppeals")

BASE = "https://kycc.ky.gov/claims/"
SEARCH_URL = "https://kycc.ky.gov/claims/search.aspx"
YEARS = ["2026", "2025", "2024", "2023", "2022", "2021"]

MIN_TEXT_CHARS = 100

HIDDEN_RE = re.compile(
    r'id="(__VIEWSTATE|__VIEWSTATEGENERATOR|__EVENTVALIDATION)"[^>]*value="([^"]*)"'
)
ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
CELL_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.S | re.I)
FINALORDER_RE = re.compile(r'href="([^"]*FinalOrder/[^"]+\.pdf)"', re.I)
DATE_MDY_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _cell_text(cell_html: str) -> str:
    return _htmllib.unescape(
        re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", cell_html))
    ).strip()


def _iso_from_mdy(s: str) -> str | None:
    m = DATE_MDY_RE.search(s or "")
    if not m:
        return None
    mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if 1980 <= y <= 2100 and 1 <= mo <= 12 and 1 <= d <= 31:
        return f"{y}-{mo:02d}-{d:02d}"
    return None


class KYTaxAppealsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=90,
        )
        self.delay = 1.0
        self._hidden: dict | None = None

    # ---- fetch helpers -------------------------------------------------

    def _get(self, url: str, retries: int = 4) -> str:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp.text
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return ""

    def _post(self, url: str, data: dict, retries: int = 4) -> str:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.post(url, data=data)
                if resp.status_code == 200:
                    return resp.text
                logger.warning(f"HTTP {resp.status_code} (POST) for {url}")
            except Exception as e:
                logger.warning(f"Error POSTing {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return ""

    def _get_bytes(self, url: str, retries: int = 3) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.content:
                    ctype = resp.headers.get("Content-Type", "").lower()
                    if "pdf" in ctype or resp.content[:5] == b"%PDF-":
                        return resp.content
                    logger.warning(f"Non-PDF response for {url} ({ctype})")
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching PDF {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- discovery -----------------------------------------------------

    def _load_hidden(self) -> dict:
        if self._hidden is not None:
            return self._hidden
        html = self._get(SEARCH_URL)
        hidden = {k: _htmllib.unescape(v) for k, v in HIDDEN_RE.findall(html)}
        self._hidden = hidden
        return hidden

    def _search_year(self, year: str) -> str:
        hidden = self._load_hidden()
        if not hidden:
            return ""
        data = {
            "__VIEWSTATE": hidden.get("__VIEWSTATE", ""),
            "__VIEWSTATEGENERATOR": hidden.get("__VIEWSTATEGENERATOR", ""),
            "__EVENTVALIDATION": hidden.get("__EVENTVALIDATION", ""),
            "ctl00$MainContent$TxYear": year,
            "ctl00$MainContent$btnSrch": "Search",
        }
        return self._post(SEARCH_URL, data)

    def _parse_rows(self, html: str) -> Generator[dict, None, None]:
        for row in ROW_RE.findall(html or ""):
            if "FinalOrder" not in row:
                continue
            cells = CELL_RE.findall(row)
            if len(cells) < 9:
                continue
            links = FINALORDER_RE.findall(cells[0])
            if not links:
                continue
            tax_year = _cell_text(cells[1]) or None
            county = _cell_text(cells[2]) or None
            appeal_type = _cell_text(cells[3]) or None
            status = _cell_text(cells[4]) or None
            taxpayer = _cell_text(cells[5]) or None
            appeal_no = _cell_text(cells[7]) or None
            date_filed = _iso_from_mdy(_cell_text(cells[8]))
            for href in links:
                href = _htmllib.unescape(href).strip()
                pdf_url = urllib.parse.urljoin(BASE, href)
                fname = pdf_url.rsplit("/", 1)[-1]
                slug = re.sub(r"[^A-Za-z0-9._-]+", "-", fname.rsplit(".", 1)[0]).strip("-")[:80]
                if not slug:
                    continue
                yield {
                    "slug": slug,
                    "url": pdf_url,
                    "appeal_no": appeal_no,
                    "tax_year": tax_year,
                    "appeal_type": appeal_type,
                    "county": county,
                    "status": status,
                    "taxpayer": taxpayer,
                    "date": date_filed,
                }

    def discover_documents(self) -> Generator[dict, None, None]:
        seen: set[str] = set()
        total = 0
        for year in YEARS:
            html = self._search_year(year)
            if not html:
                logger.warning(f"No results returned for tax year {year}")
                continue
            year_count = 0
            for doc in self._parse_rows(html):
                if doc["slug"] in seen:
                    continue
                seen.add(doc["slug"])
                total += 1
                year_count += 1
                yield doc
            logger.info(f"Tax year {year}: {year_count} final orders")
        logger.info(f"Discovered {total} Kentucky Board of Tax Appeals final orders")

    # ---- build ---------------------------------------------------------

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._get_bytes(doc["url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/KY-TaxAppeals",
            doc["slug"],
            pdf_bytes=pdf_bytes,
            table="case_law",
            force=True,
        )
        text = clean_text(text or "")
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text ({len(text)} chars), likely "
                           f"scanned: {doc['slug']}")
            return None
        doc = dict(doc)
        doc["text"] = text
        return doc

    # ---- public interface ----------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing Kentucky Board of Tax Appeals final orders...")
        try:
            hidden = self._load_hidden()
            if not hidden.get("__VIEWSTATE"):
                logger.error("  Could not read ASP.NET hidden fields")
                return False
            html = self._search_year("2024")
            docs = list(self._parse_rows(html))
            if not docs:
                logger.error("  No final orders discovered for 2024")
                return False
            logger.info(f"  Discovered {len(docs)} final orders for tax year 2024")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) >= MIN_TEXT_CHARS:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw['slug']}")
            else:
                logger.error("  Text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        taxpayer = (raw.get("taxpayer") or "").strip()
        appeal_no = (raw.get("appeal_no") or "").strip()
        parts = ["Kentucky Board of Tax Appeals — Final Order"]
        if taxpayer:
            parts.append(taxpayer)
        if appeal_no:
            parts.append(f"({appeal_no})")
        title = " ".join(parts)[:300]
        return {
            "_id": f"US/KY-TaxAppeals/{raw['slug']}",
            "_source": "US/KY-TaxAppeals",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "appeal_no": appeal_no or None,
            "tax_year": raw.get("tax_year"),
            "tax_type": raw.get("appeal_type"),
            "county": raw.get("county"),
            "status": raw.get("status"),
            "taxpayer": taxpayer or None,
            "issuer": "Kentucky Board of Tax Appeals (Office of Claims and Appeals)",
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-KY",
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        examined = 0
        for doc in self.discover_documents():
            examined += 1
            raw = self._build_raw(doc)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return
            if sample and examined >= 40:
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


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/KY-TaxAppeals bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = KYTaxAppealsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
