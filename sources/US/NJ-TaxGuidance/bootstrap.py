#!/usr/bin/env python3
"""
US/NJ-TaxGuidance -- New Jersey Division of Taxation, interpretive guidance

Fetches the full text of the New Jersey Division of Taxation's official
interpretive tax guidance = doctrine:

  * Technical Bulletins (TBs) -- the Division's published interpretation of the
    law, regulations, and policy on a given tax topic (~89 documents).
  * Letter Rulings (LRs) -- written determinations issued on behalf of the
    Director applying the law/regulations/policy to a specific taxpayer's facts
    (~23 documents). An LR binds the Division only as to its recipient but is
    published as guidance for like-situated taxpayers.

Both are public, born-digital PDFs on nj.gov. This is guidance/doctrine, NOT
adjudication: NJ Tax Court case_law is covered by US/NJ-Courts, and NJ statutes
by US/NJ-Legislation.

Access (no JavaScript needed, no CAPTCHA, no auth):
  Two server-rendered index pages under www.nj.gov/treasury/taxation/ list the
  documents in a uniform 5-column table:
      /tech-pubs.shtml            (Technical Bulletins)
      /letterrulings-pubs.shtml   (Letter Rulings)
  Each <tr> row: [0] doc number + <a href> to the PDF, [1] title, [2] tax type,
  [3] "YYYY Mon" issue date, [4] (blank). PDFs live under
  /treasury/taxation/pdf/pubs/tb/*.pdf and .../letter_rulings/*.pdf.

Strategy:
  1. Fetch both index pages; parse each row that carries a .pdf link.
  2. For each doc, resolve the PDF URL, capture doc number / title / tax type /
     issue date from the row.
  3. Download the PDF and extract text via the shared OOM-hardened
     common.pdf_extract helper (born-digital, no OCR). date = the row's
     "YYYY Mon" -> YYYY-MM-01.

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
logger = logging.getLogger("legal-data-hunter.US.NJ-TaxGuidance")

BASE = "https://www.nj.gov/treasury/taxation/"
INDEXES = [
    ("https://www.nj.gov/treasury/taxation/tech-pubs.shtml", "Technical Bulletin"),
    ("https://www.nj.gov/treasury/taxation/letterrulings-pubs.shtml", "Letter Ruling"),
]

MIN_TEXT_CHARS = 200

ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
CELL_RE = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", re.S | re.I)
PDF_HREF_RE = re.compile(r'href="([^"]+\.pdf)"', re.I)
MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}
YEAR_MON_RE = re.compile(r"(\d{4})\s+([A-Za-z]{3,4})")


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


def _iso_from_year_mon(s: str) -> str | None:
    m = YEAR_MON_RE.search(s or "")
    if not m:
        return None
    y = int(m.group(1))
    mo = MONTHS.get(m.group(2).lower()[:4]) or MONTHS.get(m.group(2).lower()[:3])
    if mo and 1980 <= y <= 2100:
        return f"{y}-{mo:02d}-01"
    return None


class NJTaxGuidanceScraper(BaseScraper):

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

    def discover_documents(self) -> Generator[dict, None, None]:
        seen: set[str] = set()
        total = 0
        for index_url, kind in INDEXES:
            html = self._get(index_url)
            if not html:
                logger.warning(f"Failed to fetch index {index_url}")
                continue
            kind_count = 0
            for row in ROW_RE.findall(html):
                cells = CELL_RE.findall(row)
                if len(cells) < 2:
                    continue
                hm = PDF_HREF_RE.search(cells[0]) or PDF_HREF_RE.search(row)
                if not hm:
                    continue
                href = _htmllib.unescape(hm.group(1)).strip()
                pdf_url = urllib.parse.urljoin(index_url, href)
                # Only keep the actual guidance PDFs (skip the boilerplate
                # "dcc.pdf" disclaimer and other non-pub links).
                if "/pubs/" not in pdf_url:
                    continue
                fname = pdf_url.rsplit("/", 1)[-1]
                slug = re.sub(r"[^A-Za-z0-9._-]+", "-",
                              fname.rsplit(".", 1)[0]).strip("-")[:80]
                if not slug or slug in seen:
                    continue
                seen.add(slug)
                doc_no = _cell_text(cells[0]) or None
                title = _cell_text(cells[1]) if len(cells) > 1 else None
                tax_type = _cell_text(cells[2]) if len(cells) > 2 else None
                date_iso = _iso_from_year_mon(_cell_text(cells[3])) if len(cells) > 3 else None
                total += 1
                kind_count += 1
                yield {
                    "slug": slug,
                    "url": pdf_url,
                    "kind": kind,
                    "doc_no": doc_no,
                    "title": title or doc_no or slug,
                    "tax_type": tax_type,
                    "date": date_iso,
                }
            logger.info(f"{kind}: {kind_count} documents")
        logger.info(f"Discovered {total} NJ Division of Taxation guidance documents")

    # ---- build ---------------------------------------------------------

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._get_bytes(doc["url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/NJ-TaxGuidance",
            doc["slug"],
            pdf_bytes=pdf_bytes,
            table="doctrine",
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
        logger.info("Testing NJ Division of Taxation guidance...")
        try:
            docs = []
            for d in self.discover_documents():
                docs.append(d)
                if len(docs) >= 5:
                    break
            if not docs:
                logger.error("  No guidance documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)}+ documents (partial crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) >= MIN_TEXT_CHARS:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw['title']}")
            else:
                logger.error("  Text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        doc_no = (raw.get("doc_no") or "").strip()
        base_title = (raw.get("title") or "").strip()
        kind = raw.get("kind") or "Guidance"
        parts = [f"NJ Division of Taxation {kind}"]
        if doc_no and doc_no not in base_title:
            parts.append(doc_no)
        if base_title and base_title != doc_no:
            parts.append(f"— {base_title}")
        title = " ".join(parts)[:300]
        return {
            "_id": f"US/NJ-TaxGuidance/{raw['slug']}",
            "_source": "US/NJ-TaxGuidance",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_no": doc_no or None,
            "kind": kind,
            "tax_type": raw.get("tax_type"),
            "issuer": "New Jersey Division of Taxation",
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-NJ",
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

    parser = argparse.ArgumentParser(description="US/NJ-TaxGuidance bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NJTaxGuidanceScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
