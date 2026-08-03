#!/usr/bin/env python3
"""
US/AR-TaxDecisions -- Arkansas DFA, Act 896 Administrative Decisions & Legal Opinions

Fetches the full text of the two families of tax adjudicative / interpretive
records the Arkansas Department of Finance & Administration (DFA) is required
to publish online under Act 896 of 2015:

  * Administrative Decisions (case_law) -- written decisions of the DFA Office
    of Hearings & Appeals resolving a taxpayer's protest of an assessment,
    refund denial, or other determination. Each is signed by an Administrative
    Law Judge and captioned "STATE OF ARKANSAS / DEPARTMENT OF FINANCE &
    ADMINISTRATION / OFFICE OF HEARINGS & APPEALS / ADMINISTRATIVE DECISION".
  * Legal Opinions (doctrine) -- Revenue Legal Counsel opinions interpreting
    Arkansas tax law (the Department's official written position).

Both are public, taxpayer-identifier-redacted, born-digital PDFs served from
the Tyler Technologies "Act 896" portal at app.ar.tylertech.com/dfa/act896.

Access (no JavaScript, no CAPTCHA, no auth):
  Each family has a server-rendered search endpoint:
    .../index.php/search/decision   (case_law)
    .../index.php/search/opinion    (doctrine)
  A GET search (?query=<term>&search=Search) renders a <table> whose rows
  carry, per document: Docket Number, Release Date (MM/DD/YYYY), and one or
  more PDF download links (.../download/<hash>.pdf). Results paginate with
  ?page=N. An empty query returns nothing and common stop-words ("the", "a",
  "of") are filtered, so the crawler unions a set of tax-domain query terms
  ("tax", "taxpayer", "assessment", ...) and dedups by the PDF's hashed URL
  to enumerate the full corpus (~1,600 decisions + ~600 opinions).

Strategy:
  1. For each family and each broad query term, page through the search
     results until a page yields no rows; parse (docket, release_date, pdf
     urls) from each row. Dedup globally by download-hash.
  2. Download each PDF and extract its text via the shared, OOM-hardened
     common.pdf_extract helper (born-digital -> pypdf/pdfplumber, no OCR
     needed). Multi-part documents ("... Part1 / Part2") are separate rows
     and become separate records keyed by their own hash.
  3. Normalize: decisions -> case_law, opinions -> doctrine. date comes from
     the row's Release Date cell.

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
logger = logging.getLogger("legal-data-hunter.US.AR-TaxDecisions")

BASE = "https://app.ar.tylertech.com/dfa/act896/index.php"

# Two document families served by the Act 896 portal. kind -> (path, _type,
# human label).
FAMILIES = [
    ("decision", "case_law", "Administrative Decision"),
    ("opinion", "doctrine", "Legal Opinion"),
]

# Empty queries return nothing and stop-words are filtered, so we union a
# handful of terms that appear in essentially every Arkansas tax record and
# dedup by the PDF hash. Order is deliberate: the broadest first.
QUERIES = ["tax", "taxpayer", "assessment", "protest", "refund", "sales", "income"]

MAX_PAGES = 400          # safety ceiling per (family, query)
MIN_TEXT_CHARS = 200

ROW_RE = re.compile(
    r'<td style="width: 540px">(.*?)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>',
    re.S)
DL_RE = re.compile(
    r'href="(https://app\.ar\.tylertech\.com/[^"]*/download/[^"]+\.pdf)"', re.I)
HASH_RE = re.compile(r"/download/([0-9a-f]+)\.pdf", re.I)
TAG_RE = re.compile(r"<[^>]+>")
SLASH_DATE_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _strip_tags(html: str) -> str:
    return re.sub(r"\s+", " ", _htmllib.unescape(TAG_RE.sub(" ", html))).strip()


def _iso_date(mmddyyyy: str) -> str | None:
    m = SLASH_DATE_RE.search(mmddyyyy or "")
    if not m:
        return None
    mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if 1980 <= y <= 2100 and 1 <= mo <= 12 and 1 <= d <= 31:
        return f"{y}-{mo:02d}-{d:02d}"
    return None


class ARTaxDecisionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=90,
        )
        self.delay = 1.0

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

    def _parse_rows(self, html: str, kind: str, type_: str, label: str):
        """Yield row descriptors from one results page."""
        for docket_html, date_html, cell_html in ROW_RE.findall(html):
            docket = _strip_tags(docket_html)
            date = _iso_date(_strip_tags(date_html))
            urls = DL_RE.findall(cell_html)
            if not urls:
                continue
            for url in urls:
                hm = HASH_RE.search(url)
                if not hm:
                    continue
                yield {
                    "hash": hm.group(1),
                    "url": url,
                    "docket": docket or None,
                    "kind": kind,
                    "type": type_,
                    "label": label,
                    "date": date,
                }

    def discover_documents(self) -> Generator[dict, None, None]:
        """Yield unique document descriptors across both families."""
        seen: set[str] = set()
        for kind, type_, label in FAMILIES:
            fam_count = 0
            for query in QUERIES:
                page = 1
                while page <= MAX_PAGES:
                    url = (f"{BASE}/search/{kind}"
                           f"?query={query}&search=Search&page={page}")
                    html = self._get(url)
                    rows = list(self._parse_rows(html, kind, type_, label)) if html else []
                    if not rows:
                        break
                    new_on_page = 0
                    for row in rows:
                        if row["hash"] in seen:
                            continue
                        seen.add(row["hash"])
                        new_on_page += 1
                        fam_count += 1
                        yield row
                    page += 1
                logger.info(f"  {kind}: query='{query}' done "
                            f"(cumulative unique {fam_count})")
            logger.info(f"{kind}: discovered {fam_count} unique documents")

    # ---- build ---------------------------------------------------------

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._get_bytes(doc["url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/AR-TaxDecisions",
            doc["hash"],
            pdf_bytes=pdf_bytes,
            table=doc["type"],
            force=True,
        )
        text = clean_text(text or "")
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text ({len(text)} chars), likely "
                           f"scanned: {doc.get('docket') or doc['hash']}")
            return None
        doc = dict(doc)
        doc["text"] = text
        return doc

    # ---- public interface ----------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing Arkansas DFA Act 896 portal...")
        try:
            docs = []
            for d in self.discover_documents():
                docs.append(d)
                if len(docs) >= 3:
                    break
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)}+ documents (partial crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) >= MIN_TEXT_CHARS:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('docket') or raw['hash']}")
            else:
                logger.error("  Text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        docket = (raw.get("docket") or "").strip()
        label = raw.get("label", "Administrative Decision")
        if docket:
            title = f"Arkansas DFA {label} No. {docket}"
        else:
            title = f"Arkansas DFA {label} ({raw['hash'][:8]})"
        title = title[:300]
        return {
            "_id": f"US/AR-TaxDecisions/{raw['hash']}",
            "_source": "US/AR-TaxDecisions",
            "_type": raw["type"],
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "docket_number": docket or None,
            "document_kind": label,
            "issuer": ("Arkansas DFA Office of Hearings & Appeals"
                       if raw["kind"] == "decision"
                       else "Arkansas DFA Revenue Legal Counsel"),
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-AR",
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

    parser = argparse.ArgumentParser(description="US/AR-TaxDecisions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = ARTaxDecisionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
