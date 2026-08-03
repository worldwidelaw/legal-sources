#!/usr/bin/env python3
"""
US/IL-LetterRulings -- Illinois Department of Revenue
(Private Letter Rulings + General Information Letters)

Fetches the full text of the Illinois Department of Revenue's published
interpretive guidance — IDOR's official position on how Illinois tax law
applies to a taxpayer's facts. Two document families are collected, both
`doctrine`:

  * Private Letter Rulings (PLR) -- binding determinations issued to the
    requesting taxpayer on the specific facts presented.
  * General Information Letters (GIL) -- non-binding general statements of
    the Department's interpretation, issued in response to inquiries.

Both are official state-government interpretive guidance, not adjudications
of a contested case (those go to the Illinois Independent Tax Tribunal,
US/IL-TaxTribunal), so the corpus is `doctrine`.

Access (no JavaScript, no CAPTCHA, no auth):
  IDOR's legal-rulings library lives on the Illinois tax archive site as a
  set of server-rendered, year-partitioned index pages:

      https://taxarchive.illinois.gov/research/legal/letter-rulings/
        {income-tax|sales-tax}/{YEAR}.html

  Each year page links the individual rulings as born-digital PDFs at a
  deterministic AEM "dam" path:

      /content/dam/soi/en/web/taxarchive/research/legal/letter-rulings/
        {tax-type}/{YEAR}/{DOCID}.pdf

  where DOCID is e.g. IT24-0001-PLR (income tax) or ST24-0007-GIL (sales
  tax). The PDFs carry a clean text layer; the first line repeats the
  doc number, a date (M/D/YYYY) and the subject. Years run 2010-present.

Strategy:
  1. For each tax type and year, fetch the index page and regex the dam
     PDF hrefs.
  2. Download each PDF and extract its text layer via common.pdf_extract.
  3. Normalize into the standard doctrine schema (date parsed from the
     document head, falling back to the partition year).

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
import subprocess
import time
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
logger = logging.getLogger("legal-data-hunter.US.IL-LetterRulings")

BASE_URL = "https://taxarchive.illinois.gov"
INDEX_PATH = "/research/legal/letter-rulings/{tax_type}/{year}.html"
FIRST_YEAR = 2010
LAST_YEAR = datetime.now(timezone.utc).year

TAX_TYPES = {
    "income-tax": "Income Tax",
    "sales-tax": "Sales Tax",
}

# dam PDF hrefs, e.g.
# /content/dam/.../letter-rulings/income-tax/2024/IT24-0001-PLR.pdf
PDF_HREF_RE = re.compile(
    r'href="(/content/dam/[^"]*?/letter-rulings/[^"]*?/(\d{4})/'
    r'([A-Za-z0-9._-]+?)\.pdf)"',
    re.I,
)

# Ruling-type suffix on the filename (PLR / GIL).
TYPE_SUFFIX_RE = re.compile(r"-(PLR|GIL)\b", re.I)

# A "M/D/YYYY" date near the top of the document body.
DATE_MDY_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")


class ILLetterRulingsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120 Safari/537.36"
        )

    def _curl(self, url: str) -> str | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "90", "-A", self._ua,
                     "-H", "Accept: text/html,*/*", url],
                    capture_output=True, timeout=120,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout.decode("utf-8", "replace")
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    def _curl_bytes(self, url: str) -> bytes | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "120", "-A", self._ua, url],
                    capture_output=True, timeout=150,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout
            except Exception as e:
                logger.warning(f"curl bytes failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    @staticmethod
    def _to_iso(mo: int, d: int, y: int) -> str | None:
        if 1 <= mo <= 12 and 1 <= d <= 31 and 1990 <= y <= 2100:
            return f"{y:04d}-{mo:02d}-{d:02d}"
        return None

    def discover_documents(self, sample: bool = False) -> Generator[dict, None, None]:
        """Yield ruling metadata rows, walking each tax type/year index page."""
        seen: set[str] = set()
        total = 0
        for tax_slug, tax_name in TAX_TYPES.items():
            for year in range(LAST_YEAR, FIRST_YEAR - 1, -1):
                url = BASE_URL + INDEX_PATH.format(tax_type=tax_slug, year=year)
                html = self._curl(url)
                if html is None:
                    logger.warning(f"[{tax_name} {year}] index fetch failed")
                    continue
                rows = PDF_HREF_RE.findall(html)
                new_on_page = 0
                for href, _yr, docid in rows:
                    pdf_url = BASE_URL + href
                    if pdf_url in seen:
                        continue
                    seen.add(pdf_url)
                    sfx = TYPE_SUFFIX_RE.search(docid)
                    rtype = (sfx.group(1).upper() if sfx else None)
                    total += 1
                    new_on_page += 1
                    yield {
                        "pdf_url": pdf_url,
                        "docid": docid,
                        "slug": re.sub(r"[^A-Za-z0-9._-]+", "-", docid).strip("-")[:80],
                        "tax_type": tax_name,
                        "ruling_type": rtype,
                        "year": str(year),
                    }
                    if sample and total >= 30:
                        return
                if new_on_page:
                    logger.info(f"[{tax_name} {year}] {new_on_page} rulings "
                                f"(running total {total})")

    def _doc_date(self, text: str, year: str | None) -> str | None:
        # Prefer a real M/D/YYYY date from the document head.
        for m in DATE_MDY_RE.finditer(text[:1500]):
            iso = self._to_iso(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            if iso:
                return iso
        if year and year.isdigit():
            return f"{int(year):04d}-01-01"
        return None

    def _subject(self, text: str) -> str | None:
        # The first non-empty content line after the doc number is the
        # subject caption (often ALL CAPS), e.g. "APPORTIONMENT-SALES FACTOR".
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            # skip a leading "IT 24-0001-PLR 8/22/2024" header line
            if re.match(r"^[A-Z]{2}\s*\d", line) and "/" in line[:40]:
                continue
            if len(line) >= 6:
                return line[:200]
        return None

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._curl_bytes(doc["pdf_url"])
        if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
            logger.warning(f"PDF download failed or not a PDF: {doc['pdf_url']}")
            return None
        text = pdf_extract.extract_pdf_markdown(
            "US/IL-LetterRulings", doc["slug"], pdf_bytes=pdf_bytes,
            table="doctrine", force=True,
        )
        if not text or len(text.strip()) < 150:
            logger.warning(f"No usable text for {doc['pdf_url']} "
                           f"({len(text) if text else 0} chars)")
            return None
        doc = dict(doc)
        doc["text"] = text.strip()
        doc["date"] = self._doc_date(doc["text"], doc.get("year"))
        doc["subject"] = self._subject(doc["text"])
        return doc

    def test_api(self) -> bool:
        logger.info("Testing IDOR letter-rulings index + PDF extraction...")
        try:
            docs = []
            for d in self.discover_documents(sample=True):
                docs.append(d)
                if len(docs) >= 5:
                    break
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)}+ rulings (sample crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 150:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('docid')}: {raw.get('subject')}")
            else:
                logger.error("  Text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard doctrine schema."""
        rtype = raw.get("ruling_type") or "Letter Ruling"
        rtype_full = {
            "PLR": "Private Letter Ruling",
            "GIL": "General Information Letter",
        }.get(rtype, rtype)
        docid = raw.get("docid") or raw["slug"]
        subject = (raw.get("subject") or "").strip()
        title = f"IL {raw.get('tax_type', '')} {rtype_full} {docid}".strip()
        title = re.sub(r"\s+", " ", title)
        if subject:
            title = f"{title}: {subject}"
        title = title[:300]
        return {
            "_id": f"US/IL-LetterRulings/{raw['slug']}",
            "_source": "US/IL-LetterRulings",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "document_number": docid,
            "ruling_type": rtype_full,
            "tax_type": raw.get("tax_type"),
            "issuer": "Illinois Department of Revenue",
            "title": title,
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-IL",
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        examined = 0
        for doc in self.discover_documents(sample=sample):
            examined += 1
            raw = self._build_raw(doc)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return
            if sample and examined >= 30:
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

    parser = argparse.ArgumentParser(description="US/IL-LetterRulings bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = ILLetterRulingsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
