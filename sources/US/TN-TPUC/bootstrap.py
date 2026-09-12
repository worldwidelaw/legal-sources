#!/usr/bin/env python3
"""
US/TN-TPUC -- Tennessee Public Utility Commission — Orders

Fetches the full text of Orders issued by the Tennessee Public Utility
Commission (TPUC, formerly the Tennessee Regulatory Authority / TRA) — the
state agency that adjudicates public-utility dockets: rate cases, certificates
of convenience and necessity (CCN), service-territory amendments, tariff and
franchise matters, consumer complaints, and related contested cases. Each
Commission Order disposes of (or governs the procedure of) a specific docketed
case = case_law. Public domain (edict of a US state government body,
17 U.S.C. §105 rationale).

Strategy (TPUC Electronic Docket File Room):

  TPUC publishes its electronic docket file room as static HTML at
  ``tpucdockets.tn.gov``. Each docket has a page
  ``/dockets/{DDDDDDD}.htm`` (7-digit docket number = 2-digit filing year +
  5-digit sequence, e.g. ``2300051`` = docket 23-00051). The page lists every
  filing in a table: (Date Filed + link to the filing PDF), (Description),
  (Company Filing). Filing PDFs live at
  ``/archive/filings/{YYYY}/{DDDDDDD}{suffix}.pdf`` (born-digital).

  A "Commission Order" is a filing row whose Company-Filing column is the
  agency itself ("Tennessee Public Utility Commission" / "Tennessee Regulatory
  Authority") and whose Description contains the word "Order" — this cleanly
  excludes party-filed documents (motions, proposed orders, witness lists).

  Docket pages are enumerated per filing-year prefix with a
  consecutive-miss gap tolerance (a missing docket returns HTTP 403). Newest
  years are scanned first so sample mode fills quickly from born-digital PDFs.

  Full text is retrieved by downloading the filing PDF and extracting text
  with PyMuPDF/fitz. Born-digital orders (the e-filing era, roughly 2000+)
  carry a clean text layer; the small residue of older scanned-image orders
  have no OCR text layer and are dropped (body < MIN_BODY_CHARS).

Usage:
  python bootstrap.py bootstrap            # Full pull (all Commission Orders)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import html
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from common.base_scraper import BaseScraper  # noqa: E402

logger = logging.getLogger("legal-data-hunter")

BASE = "https://tpucdockets.tn.gov"

# 2-digit filing-year prefixes, newest first (sample mode fills from born-digital).
YEAR_PREFIXES = [f"{y % 100:02d}" for y in range(2026, 1995, -1)]

# Docket-sequence enumeration knobs.
MAX_SEQ = 1600           # per-year upper bound on the 5-digit sequence
CONSEC_MISS_LIMIT = 60   # stop a year after this many consecutive missing dockets

# Company-Filing values that mark a filing as a Commission order (case_law).
AGENCY_RE = re.compile(r"(public utility commission|regulatory authority)", re.I)

# Minimum extracted body length to count a PDF as text-bearing.
MIN_BODY_CHARS = 500

CHECKPOINT = Path(__file__).parent / "data" / "tpuc_checkpoint.json"


class TNTPUCScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            }
        )
        self.delay = 0.4
        self._sample = False  # when True, do not persist the enumeration checkpoint

    # ---- low-level ----------------------------------------------------------

    def _get(self, url: str, binary: bool = False, retries: int = 3):
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                r = self.session.get(url, timeout=90)
                if r.status_code == 200:
                    # Docket HTML is UTF-8 but served without a charset header
                    # (requests would default to ISO-8859-1 → mojibake).
                    return r.content if binary else r.content.decode("utf-8", "replace")
                if r.status_code in (403, 404):
                    return None  # missing docket / filing
                logger.warning(f"HTTP {r.status_code} for {url}")
            except Exception as e:
                logger.warning(f"GET error {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- docket parsing -----------------------------------------------------

    @staticmethod
    def _clean(html_cell: str) -> str:
        text = html.unescape(re.sub(r"<[^>]+>", " ", html_cell))
        return re.sub(r"\s+", " ", text).strip()

    def _parse_docket(self, docket: str, html: str) -> list[dict]:
        """Return the Commission-order filing rows in a docket page."""
        caption = ""
        m = re.search(r"IN\s+RE:\s*(.*?)(?:</td>|<tr|Date\s+Filed)", html, re.S | re.I)
        if m:
            caption = self._clean(m.group(1))[:400]

        orders = []
        for row in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S | re.I):
            cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.S | re.I)
            if len(cells) < 3:
                continue
            pdfs = re.findall(r'archive/filings/[^"\'>]+\.pdf', cells[0], re.I)
            if not pdfs:
                continue
            date = self._clean(cells[0])
            desc = self._clean(cells[1])
            filer = self._clean(cells[2])
            if not re.search(r"\border\b", desc, re.I):
                continue
            if not AGENCY_RE.search(filer):
                continue
            orders.append(
                {
                    "docket": docket,
                    "date": date,
                    "desc": desc,
                    "filer": filer,
                    "caption": caption,
                    "pdf_url": "https://" + pdfs[0].lstrip("/")
                    if pdfs[0].startswith("//")
                    else BASE + "/" + pdfs[0].lstrip("/"),
                }
            )
        return orders

    def _iter_dockets(self, done_prefixes: set) -> Generator[dict, None, None]:
        for prefix in YEAR_PREFIXES:
            if prefix in done_prefixes:
                continue
            misses = 0
            for seq in range(1, MAX_SEQ + 1):
                docket = f"{prefix}{seq:05d}"
                html = self._get(f"{BASE}/dockets/{docket}.htm")
                if html is None:
                    misses += 1
                    if misses >= CONSEC_MISS_LIMIT:
                        break
                    continue
                misses = 0
                for order in self._parse_docket(docket, html):
                    yield order
            self._mark_prefix_done(prefix)

    # ---- checkpoint ---------------------------------------------------------

    def _load_checkpoint(self) -> set:
        try:
            return set(json.loads(CHECKPOINT.read_text()).get("done_prefixes", []))
        except Exception:
            return set()

    def _mark_prefix_done(self, prefix: str):
        if self._sample:
            return
        done = self._load_checkpoint()
        done.add(prefix)
        try:
            CHECKPOINT.parent.mkdir(parents=True, exist_ok=True)
            CHECKPOINT.write_text(json.dumps({"done_prefixes": sorted(done)}))
        except Exception as e:
            logger.warning(f"checkpoint write failed: {e}")

    # ---- normalize ----------------------------------------------------------

    @staticmethod
    def _iso(mdY: str) -> Optional[str]:
        for fmt in ("%m/%d/%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(mdY.strip(), fmt).date().isoformat()
            except Exception:
                continue
        return None

    @staticmethod
    def _fmt_docket(docket: str) -> str:
        return f"{docket[:2]}-{docket[2:]}" if len(docket) == 7 else docket

    def normalize(self, raw: dict) -> Optional[dict]:
        if fitz is None:
            logger.error("PyMuPDF (fitz) not installed — cannot extract text")
            return None

        pdf = self._get(raw["pdf_url"], binary=True)
        if not pdf or pdf[:4] != b"%PDF":
            return None
        try:
            doc = fitz.open(stream=pdf, filetype="pdf")
            full = "".join(p.get_text() for p in doc)
            pages = doc.page_count
            doc.close()
        except Exception as e:
            logger.warning(f"PDF parse failed for {raw['pdf_url']}: {e}")
            return None

        body = re.sub(r"[ \t]+", " ", full)
        body = re.sub(r"\n{3,}", "\n\n", body).strip()
        if len(body) < MIN_BODY_CHARS:
            return None  # scanned image, no OCR text layer

        fname = raw["pdf_url"].rsplit("/", 1)[-1]
        doc_id = re.sub(r"\.pdf$", "", fname, flags=re.I)
        case_no = self._fmt_docket(raw["docket"])
        desc = raw["desc"]
        caption = raw.get("caption") or ""
        title = f"{desc} — Docket {case_no}"
        if caption:
            title = f"{desc} — Docket {case_no}: {caption}"
        title = title[:500]

        return {
            "_id": f"TN-TPUC-{doc_id}",
            "_source": "US/TN-TPUC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": body,
            "date": self._iso(raw["date"]),
            "url": f"{BASE}/dockets/{raw['docket']}.htm",
            "pdf_url": raw["pdf_url"],
            "case_number": case_no,
            "docket_caption": caption or None,
            "document_type": desc,
            "pages": pages,
            "jurisdiction": "US-TN",
            "court": "Tennessee Public Utility Commission",
        }

    # ---- iteration ----------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._iter_dockets(self._load_checkpoint())

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        # No cheap server-side "modified since"; the daily-updates index is the
        # newest activity. Re-scan the newest two filing years; loader dedups _id.
        for prefix in YEAR_PREFIXES[:2]:
            misses = 0
            for seq in range(1, MAX_SEQ + 1):
                docket = f"{prefix}{seq:05d}"
                html = self._get(f"{BASE}/dockets/{docket}.htm")
                if html is None:
                    misses += 1
                    if misses >= CONSEC_MISS_LIMIT:
                        break
                    continue
                misses = 0
                for order in self._parse_docket(docket, html):
                    yield order

    # ---- connectivity test --------------------------------------------------

    def test_api(self) -> bool:
        try:
            html = self._get(f"{BASE}/dockets/2300051.htm")
            if not html:
                logger.error("test-api: docket page unreachable")
                return False
            orders = self._parse_docket("2300051", html)
            logger.info(f"test-api: docket 23-00051 -> {len(orders)} Commission orders")
            if orders:
                rec = self.normalize(orders[0])
                logger.info(
                    "test-api normalize: "
                    + (f"text {len(rec['text'])} chars" if rec else "image-only (skipped)")
                )
            return bool(orders)
        except Exception as e:
            logger.error(f"test-api FAILED: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/TN-TPUC bootstrap")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "test-api"], help="Command"
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    scraper = TNTPUCScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        return

    scraper._sample = args.sample
    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
