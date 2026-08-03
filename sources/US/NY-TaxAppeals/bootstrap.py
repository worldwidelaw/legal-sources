#!/usr/bin/env python3
"""
US/NY-TaxAppeals -- New York State Division of Tax Appeals

Fetches the full text of the two adjudicative bodies of the New York
State Division of Tax Appeals (DTA):

  * Tax Appeals Tribunal **decisions** (*.dec.pdf) — the Tribunal is the
    final administrative review body for NY State tax disputes.
  * Administrative Law Judge **determinations** (*.det.pdf) — first-level
    adjudication of taxpayer petitions.
  * Tribunal/ALJ **orders** (*.ord.pdf) — procedural orders.

Each document resolves a specific tax controversy between a taxpayer and
the NY Department of Taxation and Finance, so the corpus is case_law.

The decisions/determinations are published openly by the Division at
dta.ny.gov as born-digital, text-layer PDFs. Three server-rendered HTML
index pages list every document as a direct PDF link — no JavaScript, no
pagination beyond the index pages, no CAPTCHA:

  * /decisions/                    current Tribunal decisions
  * /determinations/               current ALJ determinations
  * /pdf/archive/archive_index.htm full historical archive (~23,600 links,
                                   1986-present, decisions + determinations)

Strategy:
  1. GET each index page and collect every "*.dec.pdf / *.det.pdf /
     *.ord.pdf" link (absolute-ized against dta.ny.gov).
  2. Parse the DTA docket number and document kind from the filename
     (e.g. "831624.dec.pdf" -> DTA No. 831624, decision).
  3. Download each PDF and extract its text layer via common.pdf_extract.
  4. Derive the petitioner name and decision date from the document text.
  5. Normalize into the standard case_law schema.

Usage:
  python bootstrap.py bootstrap            # Full pull (all documents)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
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
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common import pdf_extract

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NY-TaxAppeals")

BASE_URL = "https://dta.ny.gov"
INDEX_PAGES = [
    "/decisions/",
    "/determinations/",
    "/decisions/orders.htm",
    "/determinations/orders.htm",
    "/pdf/archive/archive_index.htm",
]

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}
MONTH_ALT = "|".join(MONTHS)
TEXT_DATE_RE = re.compile(rf"\b({MONTH_ALT})\s+(\d{{1,2}}),\s+(\d{{4}})", re.I)
# Document filename: leading docket digits, then a kind marker.
#   831624.dec.pdf      824971and824972dec.pdf   805768.dec.wpd.pdf
#   850080.det.pdf      830004.dec.final.pdf     830290.dec1.pdf
DOCKET_RE = re.compile(r"(\d{4,})")
PDF_HREF_RE = re.compile(r'href="([^"]+\.pdf)"', re.I)
# Kind detection on the filename (after the docket number).
KIND_RE = re.compile(r"(det|dec|ord)", re.I)


class NYTaxAppealsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120 Safari/537.36"
        )

    def _curl_text(self, url: str) -> str | None:
        out = self._curl_bytes(url)
        return out.decode("utf-8", "replace") if out else None

    def _curl_bytes(self, url: str) -> bytes | None:
        """Fetch raw bytes via the curl CLI with a browser UA."""
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

    @staticmethod
    def _kind(pdf_url: str) -> str:
        """Classify a document as decision / determination / order from the
        path and filename."""
        low = pdf_url.lower()
        name = low.rsplit("/", 1)[-1]
        # Strip the leading docket digits so the kind marker is unambiguous.
        tail = re.sub(r"^\d+", "", name)
        if "/determinations/" in low or tail.startswith("det") or ".det" in name:
            return "determination"
        if tail.startswith("ord") or ".ord" in name or "/orders" in low:
            return "order"
        return "decision"

    @staticmethod
    def _docket(pdf_url: str) -> str | None:
        name = urllib.parse.unquote(pdf_url.rsplit("/", 1)[-1])
        m = DOCKET_RE.search(name)
        return m.group(1) if m else None

    @staticmethod
    def _slug(pdf_url: str) -> str:
        stem = urllib.parse.unquote(pdf_url.rsplit("/", 1)[-1])
        stem = re.sub(r"\.pdf$", "", stem, flags=re.I)
        stem = re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-")
        return stem[:180]

    # Right-column boilerplate that interleaves into the caption when the
    # two-column colon layout is flattened (longest phrases first).
    _CAPTION_NOISE = [
        "DETERMINATION DISMISSING THE PETITION",
        "DETERMINATION DISMISSING PETITION",
        "DECISION DISMISSING THE PETITION",
        "DECISION DISMISSING PETITION",
        "ORDER DISMISSING THE PETITION",
        "ORDER DISMISSING PETITION",
        "DISMISSING THE PETITION",
        "DISMISSING PETITION",
        "DETERMINATION",
        "DECISION",
        "ORDER",
    ]

    @classmethod
    def _parse_petitioner(cls, text: str) -> str | None:
        """Pull the petitioner name out of the 'In the Matter of the Petition
        of <NAME>' caption. The PDFs use a two-column colon layout, so the
        caption is reconstructed from the head of the document and the
        right-column boilerplate (DECISION / DETERMINATION DISMISSING
        PETITION / ...) that interleaves into it is stripped out."""
        head = text[:1500]
        # Drop the column-separator colons and collapse whitespace.
        flat = re.sub(r"\s*:\s*", " ", head)
        flat = re.sub(r"\s+", " ", flat)
        m = re.search(
            r"Petition\s+of\s+(.+?)\s+"
            r"(?:DTA\s+NO|for\s+Redetermination|for\s+Refund|for\s+Revision)",
            flat, re.I,
        )
        if not m:
            return None
        name = m.group(1)
        for phrase in cls._CAPTION_NOISE:
            name = re.sub(re.escape(phrase), " ", name, flags=re.I)
        name = re.sub(r"\s+", " ", name).strip(" ,.")
        # Guard against runaway / empty captures.
        if not name or len(name) > 200:
            return None
        return name

    @staticmethod
    def _parse_date(text: str) -> str | None:
        """Derive the decision/determination date. Prefer the spelled-out
        date following 'DATED:' near the end; fall back to the last
        spelled-out date in the document."""
        tail = text[-2500:]
        dated = re.search(r"DATED:.*", tail, re.I | re.S)
        candidates = []
        if dated:
            candidates = TEXT_DATE_RE.findall(dated.group(0))
        if not candidates:
            candidates = TEXT_DATE_RE.findall(text)
        if not candidates:
            return None
        mo_name, day, yr = candidates[-1]
        mo = MONTHS[mo_name.lower()]
        day, yr = int(day), int(yr)
        if 1980 <= yr <= 2035 and 1 <= day <= 31:
            return f"{yr:04d}-{mo:02d}-{day:02d}"
        return None

    def discover_documents(self, sample: bool = False) -> list:
        """Return ordered (slug, docket, kind, pdf_url) tuples for every
        DTA document PDF across the index pages, newest docket first.

        In sample mode only the small current decisions/determinations
        pages are read (born-digital, guaranteed text-layer)."""
        pages = INDEX_PAGES[:2] if sample else INDEX_PAGES
        urls = []
        seen = set()
        for page in pages:
            html = self._curl_text(BASE_URL + page)
            if not html:
                logger.warning(f"Failed to fetch {page}")
                continue
            for href in PDF_HREF_RE.findall(html):
                full = urllib.parse.urljoin(BASE_URL + page, href)
                full = full.split("#", 1)[0]
                if not full.lower().endswith(".pdf"):
                    continue
                if full in seen:
                    continue
                seen.add(full)
                urls.append(full)
            logger.info(f"  {page}: cumulative {len(urls)} unique PDFs")
        out = []
        for u in urls:
            docket = self._docket(u)
            out.append((self._slug(u), docket, self._kind(u), u))
        # Newest docket first (docket numbers increase over time).
        out.sort(key=lambda t: (t[1] or "0").zfill(12), reverse=True)
        logger.info(f"Discovered {len(out)} DTA document PDFs")
        return out

    def _build_raw(self, slug: str, docket: str | None, kind: str,
                   pdf_url: str) -> dict | None:
        pdf_bytes = self._curl_bytes(pdf_url)
        if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
            logger.warning(f"PDF download failed or not a PDF: {pdf_url}")
            return None
        text = pdf_extract.extract_pdf_markdown(
            pdf_url, "US/NY-TaxAppeals", pdf_bytes=pdf_bytes,
            table="case_law", force=True,
        )
        if not text or len(text.strip()) < 150:
            logger.warning(f"No usable text for {pdf_url} "
                           f"({len(text) if text else 0} chars)")
            return None
        return {
            "slug": slug,
            "docket": docket,
            "kind": kind,
            "petitioner": self._parse_petitioner(text),
            "text": text.strip(),
            "url": pdf_url,
            "date": self._parse_date(text),
        }

    def test_api(self) -> bool:
        logger.info("Testing NY Division of Tax Appeals index + PDF extraction...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} documents")
            raw = self._build_raw(*docs[0])
            if raw and raw["text"] and len(raw["text"]) > 150:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars)")
            else:
                logger.error("  PDF text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard case_law schema."""
        kind = raw.get("kind") or "decision"
        body = {
            "decision": "Decision",
            "determination": "Determination",
            "order": "Order",
        }.get(kind, "Decision")
        forum = ("Tax Appeals Tribunal" if kind == "decision"
                 else "Division of Tax Appeals")
        docket = raw.get("docket")
        petitioner = (raw.get("petitioner") or "").strip()
        if petitioner:
            title = f"Matter of {petitioner} — NY {forum} {body}"
        else:
            title = f"NY {forum} {body}"
        if docket:
            title += f" (DTA No. {docket})"
        title = title[:300]
        return {
            "_id": f"US/NY-TaxAppeals/{raw['slug']}",
            "_source": "US/NY-TaxAppeals",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "docket_number": docket,
            "document_kind": kind,
            "court": f"New York State {forum}",
            "petitioner": petitioner or None,
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for slug, docket, kind, pdf_url in self.discover_documents(sample=sample):
            raw = self._build_raw(slug, docket, kind, pdf_url)
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


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/NY-TaxAppeals bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NYTaxAppealsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    gen = scraper.fetch_sample() if args.sample else scraper.fetch_all()

    count = 0
    for raw in gen:
        record = scraper.normalize(raw)
        safe_id = record["_id"].replace("/", "_")
        out_path = sample_dir / f"{safe_id}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")

    logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
