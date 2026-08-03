#!/usr/bin/env python3
"""
US/LA-ExecutiveOrders -- Louisiana Governor Executive Orders

Fetches the full text of the Executive Orders of the Governor of Louisiana.
Under the Governor's constitutional executive authority (La. Const. art. IV)
and statute, the Governor issues numbered Executive Orders that establish
binding state policy, create and direct executive agencies, allocate
private-activity bond volume, declare states of emergency, and otherwise
regulate the conduct of the executive branch. Each Executive Order is a
binding regulatory/administrative instrument = legislation (which includes
regulations). These are official Louisiana state-government works in the
public domain (government edicts).

The corpus is published by the State Library of Louisiana as part of its
CONTENTdm "Louisiana Public Documents Digital Archive" (alias
``p267101coll4``) on the OCLC-hosted CONTENTdm instance at
``https://cdm16313.contentdm.oclc.org/`` -- the same open dmwebservices JSON
API used by the sibling US/LA-AGOpinions source. The archive holds ~44,700
heterogeneous public documents, so the Executive Orders are isolated with a
server-side title search (``title^Executive Order^all^and`` -> ~2,406 items,
each an EO of a Louisiana governor, e.g. "BJ 15-25", "KB 04-12", "MJF 98-1"):

  - Enumerate:  dmQuery/p267101coll4/title^Executive Order^all^and/
                dmrecord!title!date/date/{max}/{start}/1/0/0/0/json
                -> pager.total + records[].dmrecord (each an item pointer)
  - Per item:   dmGetItemInfo/p267101coll4/{dmrecord}/json
                -> Dublin-Core fields:
                     title   = "Executive Order"
                     date    = Date of order (ISO YYYY-MM-DD)
                     descri  = Caption ("BJ 15-25; Bond Allocation ...; October 22, 2015")
                     creato  = "Louisiana Office of the Governor"
                     subjec  = Subject headings
                     find    = PDF file name

The ``transc`` field is empty for this collection, so the full order text is
extracted from the item's PDF, downloaded from the CONTENTdm download endpoint
(/digital/api/collection/{alias}/id/{pointer}/download). PDF extraction uses
the shared, OOM-hardened ``common.pdf_extract`` helper (pdfplumber -> pypdf ->
OCR fallback). The archived EO PDFs carry an embedded text layer; the rare
pure image scans (0 chars) are skipped.

Usage:
  python bootstrap.py bootstrap            # Full pull (~2,406 orders)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
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
from urllib.parse import quote
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.LA-ExecutiveOrders")

CDM_HOST = "https://cdm16313.contentdm.oclc.org"
DMWS = CDM_HOST + "/digital/bl/dmwebservices/index.php?q="
ALIAS = "p267101coll4"
# Server-side title filter isolates the Governor's Executive Orders from the
# ~44,700-item "Louisiana Public Documents Digital Archive".
SEARCH = "title^Executive Order^all^and"
ITEM_URL = CDM_HOST + "/digital/collection/" + ALIAS + "/id/{rec}"
DOWNLOAD_URL = CDM_HOST + "/digital/api/collection/" + ALIAS + "/id/{rec}/download"

WS_RE = re.compile(r"[ \t\r\f\v]+")
NL_RE = re.compile(r"\n{3,}")
ISO_DATE_RE = re.compile(r"(\d{4})-(\d{1,2})-(\d{1,2})")
US_DATE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")
_MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july",
     "august", "september", "october", "november", "december"], start=1)}
SPELLED_DATE_RE = re.compile(
    r"\b(" + "|".join(_MONTHS) + r")\s+(\d{1,2}),?\s+(\d{4})\b", re.I)
# EO designation, e.g. "BJ 15-25", "KB 04-12", "MJF 98-1", "JML 2016-9".
EO_NUM_RE = re.compile(r"\b([A-Z]{1,4})\s*(\d{2,4}[-–]\d{1,4})\b")
MIN_TEXT_CHARS = 200

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")


class LAExecutiveOrdersScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.4

    # ---------------------------------------------------------------- http
    def _get_json(self, path: str):
        url = DMWS + path
        for attempt in range(5):
            time.sleep(self.delay)
            try:
                req = Request(url, headers={"User-Agent": UA,
                                            "Accept": "application/json,*/*"})
                with urlopen(req, timeout=60) as resp:
                    data = resp.read()
                return json.loads(data.decode("utf-8", "replace"))
            except Exception as e:
                logger.warning(f"API failed ({path[:60]}) attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return None

    def _get_pdf(self, rec: str) -> bytes | None:
        url = DOWNLOAD_URL.format(rec=rec)
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                req = Request(url, headers={"User-Agent": UA,
                                            "Accept": "application/pdf,*/*"})
                with urlopen(req, timeout=90) as resp:
                    data = resp.read()
                if data[:5] == b"%PDF-":
                    return data
                logger.warning(f"Non-PDF payload for rec {rec} "
                               f"({data[:16]!r})")
                return None
            except Exception as e:
                logger.warning(f"PDF fetch failed rec {rec} attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- helpers
    @classmethod
    def _clean_text(cls, text: str) -> str:
        if not text:
            return ""
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        lines = [WS_RE.sub(" ", ln).strip() for ln in text.split("\n")]
        text = "\n".join(lines)
        text = NL_RE.sub("\n\n", text)
        return text.strip()

    @staticmethod
    def _iso_date(raw: str | None) -> str | None:
        if not raw or not isinstance(raw, str):
            return None
        m = ISO_DATE_RE.search(raw)
        if m:
            yy, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
        else:
            m = US_DATE_RE.search(raw)
            if not m:
                return None
            mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mm <= 12 and 1 <= dd <= 31 and 1900 <= yy <= 2100:
            return f"{yy:04d}-{mm:02d}-{dd:02d}"
        return None

    @classmethod
    def _spelled_date(cls, raw: str | None) -> str | None:
        """Parse a spelled-out date like 'October 22, 2015'."""
        if not raw or not isinstance(raw, str):
            return None
        m = SPELLED_DATE_RE.search(raw)
        if not m:
            return None
        mm = _MONTHS[m.group(1).lower()]
        dd, yy = int(m.group(2)), int(m.group(3))
        if 1 <= dd <= 31 and 1900 <= yy <= 2100:
            return f"{yy:04d}-{mm:02d}-{dd:02d}"
        return None

    @staticmethod
    def _field(val):
        """CONTENTdm returns {} for empty fields; coerce to str-or-None."""
        if val is None or isinstance(val, (dict, list)):
            return None
        s = str(val).strip()
        return s or None

    # --------------------------------------------------------- discovery
    def discover_pointers(self, sample: bool = False) -> Generator[str, None, None]:
        """Yield pointers for the Governor's Executive Orders.

        The title search returns records whose title contains "Executive
        Order"; the subject headings ("Executive orders--Louisiana") confirm
        each is a gubernatorial EO.
        """
        start = 0
        page = 1000 if not sample else 60
        seen: set[str] = set()
        search = quote(SEARCH, safe="^")
        while True:
            path = (f"dmQuery/{ALIAS}/{search}/dmrecord!title!date/date/"
                    f"{page}/{start}/1/0/0/0/json")
            data = self._get_json(path)
            if not data or "records" not in data:
                break
            recs = data.get("records") or []
            if not recs:
                break
            for r in recs:
                rec = str(r.get("dmrecord") or r.get("pointer") or "").strip()
                if not rec or rec in seen:
                    continue
                if r.get("parentobject") not in (None, -1, "-1"):
                    continue
                title = str(r.get("title") or "").lower()
                if "executive order" not in title:
                    continue
                seen.add(rec)
                yield rec
                if sample and len(seen) >= 30:
                    logger.info(f"Discovered {len(seen)} EO pointers (sample)")
                    return
            total = int(data.get("pager", {}).get("total", 0) or 0)
            start += len(recs)
            if start >= total or len(recs) < page:
                break
        logger.info(f"Discovered {len(seen)} Louisiana Executive Order pointers")

    # ------------------------------------------------------- build record
    def _build_raw(self, rec: str) -> dict | None:
        info = self._get_json(f"dmGetItemInfo/{ALIAS}/{rec}/json")
        if not info or not isinstance(info, dict):
            return None
        pdf = self._get_pdf(rec)
        if not pdf:
            return None
        text = self._clean_text(
            extract_pdf_markdown("US/LA-ExecutiveOrders", rec,
                                 pdf_bytes=pdf, table="legislation", force=True) or "")
        if not text or len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text for rec {rec} "
                           f"({len(text)} chars), likely scanned image")
            return None

        descri = self._field(info.get("descri"))
        subjec = self._field(info.get("subjec"))
        issuer = self._field(info.get("creato")) or "Louisiana Office of the Governor"
        date = (self._iso_date(self._field(info.get("date")))
                or self._spelled_date(descri)
                or self._spelled_date(text[:800]))

        # EO designation (e.g. "BJ 15-25") lives at the head of the caption or
        # in the order body ("EXECUTIVE ORDER BJ 15-25").
        eo_num = None
        for hay in (descri or "", text[:400]):
            m = EO_NUM_RE.search(hay)
            if m:
                eo_num = f"{m.group(1)} {m.group(2).replace('–', '-')}"
                break

        # Caption text after the EO number (drop the leading designation).
        caption = descri
        if eo_num and descri:
            trimmed = re.sub(r"^\s*[A-Z]{1,4}\s*\d{2,4}[-–]\d{1,4}\s*[;:,-]?\s*",
                             "", descri).strip()
            caption = trimmed or descri

        if eo_num:
            title = f"Louisiana Executive Order {eo_num}"
        else:
            title = "Louisiana Executive Order"
        if date:
            title = f"{title} ({date})"

        return {
            "rec": rec,
            "eo_number": eo_num,
            "caption": caption,
            "subject": subjec,
            "issuer": issuer,
            "title": title[:300],
            "text": text,
            "date": date,
            "url": ITEM_URL.format(rec=rec),
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Louisiana Executive Orders CONTENTdm API...")
        try:
            recs = list(self.discover_pointers(sample=True))
            if not recs:
                logger.error("  No pointers discovered")
                return False
            logger.info(f"  Discovered {len(recs)} pointers (sample)")
            raw = None
            for rec in recs:
                raw = self._build_raw(rec)
                if raw:
                    break
            if raw and raw["text"] and len(raw["text"]) > MIN_TEXT_CHARS:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('eo_number')} [{raw.get('date')}]")
            else:
                logger.error("  Text extraction failed")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"US/LA-ExecutiveOrders/{raw['rec']}",
            "_source": "US/LA-ExecutiveOrders",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["rec"],
            "eo_number": raw.get("eo_number"),
            "caption": raw.get("caption"),
            "subject": raw.get("subject"),
            "issuer": raw.get("issuer"),
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-LA",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for rec in self.discover_pointers(sample=sample):
            raw = self._build_raw(rec)
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

    parser = argparse.ArgumentParser(description="US/LA-ExecutiveOrders bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = LAExecutiveOrdersScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
