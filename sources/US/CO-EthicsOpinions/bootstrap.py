#!/usr/bin/env python3
"""
US/CO-EthicsOpinions -- Colorado Independent Ethics Commission --
Advisory Opinions, Letter Rulings and Position Statements.

Fetches the full text of the formal ethics guidance issued by the Colorado
Independent Ethics Commission (IEC) under the authority of Article XXIX of the
Colorado Constitution ("Amendment 41") and Title 24, Article 18.5, C.R.S. The
IEC issues three published document classes, all of which are the Commission's
written interpretation of the constitutional and statutory standards of conduct
= doctrine:

  - Advisory Opinion (AO)  -- addresses a specific ethics inquiry by a public
    officer, member of the general assembly, local government official or
    government employee.
  - Letter Ruling (LR)     -- addresses a specific ethics inquiry by a person
    or entity who is NOT a covered public official.
  - Position Statement (PS) -- a general statement of policy interpreting a
    provision of Article XXIX or other standards of conduct, offering broad
    guidance to officials and the public.

Access (no JavaScript, no CAPTCHA, no auth):
  The IEC site is Drupal. The opinions are enumerated on per-year listing pages

      https://iec.colorado.gov/opinions/iec-opinions-{YEAR}   (2008-present)

  and each opinion is a born-digital PDF (clean text layer, no OCR) at

      https://iec.colorado.gov/sites/iec/files/documents/{CODE} FR.pdf

  where {CODE} is e.g. "AO 23-01", "LR 14-02", "PS 23-01". The listing anchor's
  link text carries a descriptive caption ("Advisory Opinion 23-01: Acceptance
  of Gifts"); the title attribute / filename carries the document code.

Strategy:
  Walk the per-year listing pages newest-first, collect every AO/LR/PS anchor,
  dedup by document code (a few opinions are cross-linked on more than one year
  page), download each PDF and extract full text via the shared
  common.pdf_extract._extract backend chain. The issue date is parsed from the
  first "Month DD, YYYY" in the opinion body; it falls back to Jan 1 of the year
  encoded in the document code. All records are doctrine.

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples (newest first)
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity + extraction test
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
from urllib.parse import unquote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import _extract as _pdf_extract_bytes

try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover
    BeautifulSoup = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CO-EthicsOpinions")

BASE_URL = "https://iec.colorado.gov"
YEAR_URL = "https://iec.colorado.gov/opinions/iec-opinions-{year}"

# Listing pages exist from 2008 to the current year.
FIRST_YEAR = 2008
LAST_YEAR = 2026

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)

# Document PDFs live under /sites/iec/files/... and are named "{AO|LR|PS} NN-NN ...".
DOC_HREF_RE = re.compile(r"/sites/iec/files/[^\"']*\.pdf", re.I)
# Extract the document code (type + NN-NN) from a filename or caption.
CODE_RE = re.compile(r"\b(AO|LR|PS)\s*[- ]?\s*(\d{2})-(\d{1,3})\b", re.I)

TYPE_NAMES = {
    "AO": "Advisory Opinion",
    "LR": "Letter Ruling",
    "PS": "Position Statement",
}

MONTHS = (
    "January February March April May June July August September "
    "October November December"
).split()
DATE_RE = re.compile(
    r"\b(" + "|".join(MONTHS) + r")\s+(\d{1,2}),?\s+((?:19|20)\d{2})\b"
)


def _year_from_yy(yy: int) -> int:
    """A 2-digit year in a Colorado IEC document code is always 20YY."""
    return 2000 + yy


def _iso_from_body(text: str) -> str | None:
    """Parse the first 'Month DD, YYYY' date in the opinion body to ISO 8601."""
    m = DATE_RE.search(text or "")
    if not m:
        return None
    mo = MONTHS.index(m.group(1)) + 1
    d = int(m.group(2))
    y = int(m.group(3))
    if 1 <= d <= 31:
        return f"{y:04d}-{mo:02d}-{d:02d}"
    return None


def _clean_caption(text: str) -> str:
    """Collapse whitespace / decode &nbsp; in a listing caption."""
    text = text.replace("\xa0", " ").replace("&nbsp;", " ")
    return re.sub(r"\s+", " ", text).strip()


class COEthicsOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})

    # ---------------------------------------------------------------- http
    def _get(self, url: str):
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                return self.session.get(url, timeout=60, allow_redirects=True)
            except Exception as e:
                logger.warning(f"GET failed for {url} (attempt {attempt + 1}): {e}")
                time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    def _parse_year(self, year: int) -> list[dict]:
        """Parse one per-year listing page into row dicts (may be empty)."""
        r = self._get(YEAR_URL.format(year=year))
        if r is None or r.status_code != 200:
            logger.warning(f"{year}: listing fetch failed "
                           f"(status={getattr(r, 'status_code', None)})")
            return []
        if BeautifulSoup is None:
            logger.error("BeautifulSoup unavailable — cannot parse listing")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        rows: list[dict] = []
        for a in soup.find_all("a", href=DOC_HREF_RE):
            href = a.get("href", "")
            caption = _clean_caption(a.get_text(" ", strip=True))
            # The document code may live in the filename or the caption.
            m = CODE_RE.search(unquote(href)) or CODE_RE.search(caption)
            if not m:
                continue
            typ = m.group(1).upper()
            yy = int(m.group(2))
            seq = int(m.group(3))
            code = f"{typ} {yy:02d}-{seq:02d}"
            if href.startswith("/"):
                href = BASE_URL + href
            rows.append({
                "code": code,
                "type_prefix": typ,
                "opinion_year": _year_from_yy(yy),
                "seq": seq,
                "url": href,
                "caption": caption or None,
            })
        return rows

    def _collect_index(self) -> list[dict]:
        """Walk all year pages newest-first, dedup by document code."""
        by_code: dict[str, dict] = {}
        for year in range(LAST_YEAR, FIRST_YEAR - 1, -1):
            rows = self._parse_year(year)
            for row in rows:
                # First (newest listing) wins; prefer a caption if a later
                # duplicate carries one and the earlier did not.
                existing = by_code.get(row["code"])
                if existing is None:
                    by_code[row["code"]] = row
                elif not existing.get("caption") and row.get("caption"):
                    existing["caption"] = row["caption"]
            if rows:
                logger.info(f"{year}: {len(rows)} opinion links")
        # Sort newest-first: by opinion_year desc, then type, then seq desc.
        ordered = sorted(
            by_code.values(),
            key=lambda r: (r["opinion_year"], r["seq"], r["type_prefix"]),
            reverse=True,
        )
        logger.info(f"Index collected: {len(ordered)} distinct opinions")
        return ordered

    def _fetch_one(self, row: dict) -> dict | None:
        """Download the opinion PDF and attach extracted full text."""
        r = self._get(row["url"])
        if r is None or r.status_code != 200 or not r.content:
            return None
        if not r.content[:5].startswith(b"%PDF"):
            logger.warning(f"  {row['code']}: not a PDF — skipped")
            return None
        text = (_pdf_extract_bytes(r.content) or "").strip()
        if len(text) < 200:
            logger.warning(f"  {row['code']}: thin text ({len(text)} chars) — skipped")
            return None
        out = dict(row)
        out["text"] = text
        out["date"] = _iso_from_body(text) or f"{row['opinion_year']:04d}-01-01"
        return out

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for row in self._collect_index():
            rec = self._fetch_one(row)
            if rec:
                yield rec
                emitted += 1
                logger.info(f"  {rec['code']} OK ({len(rec['text'])} chars, "
                            f"date={rec['date']})")
                if sample and emitted >= 12:
                    return

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Colorado IEC ethics opinions...")
        idx = self._collect_index()
        if len(idx) < 50:
            logger.error(f"API test FAILED: index too small ({len(idx)})")
            return False
        ok = 0
        for row in idx[:4]:
            rec = self._fetch_one(row)
            if rec and len(rec["text"]) > 200:
                logger.info(f"  {rec['code']} OK ({len(rec['text'])} chars, "
                            f"date={rec['date']})")
                ok += 1
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: could not extract full text")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        typ = raw["type_prefix"]
        type_name = TYPE_NAMES.get(typ, "Opinion")
        yy = raw["opinion_year"] % 100
        number = f"{yy:02d}-{raw['seq']:02d}"
        caption = raw.get("caption") or f"{type_name} {number}"
        return {
            "_id": f"US/CO-EthicsOpinions/{typ}-{raw['opinion_year']}-{raw['seq']:02d}",
            "_source": "US/CO-EthicsOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": f"{type_name} {number}",
            "document_type": type_name,
            "issuer": "Colorado Independent Ethics Commission",
            "title": caption,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-CO",
        }

    # ------------------------------------------------------------- fetch
    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            date = raw.get("date")
            if not since or (date and date >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/CO-EthicsOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = COEthicsOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
