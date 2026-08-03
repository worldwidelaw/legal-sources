#!/usr/bin/env python3
"""
US/LA-AdministrativeCode -- Louisiana Administrative Code (LAC)

Fetches the full text of the Louisiana Administrative Code -- the official
codification of the rules and regulations adopted by Louisiana state agencies
under the Administrative Procedure Act (La. R.S. 49:950 et seq.). The LAC is
organized into ~50 numbered Titles (e.g. Title 37 Insurance, Title 61 Revenue
and Taxation, Title 76 Wildlife and Fisheries), each further split into Parts.
Agency regulations are legally binding secondary legislation = legislation
(cf. the sibling codified-regulation sources). These are official Louisiana
state-government works in the public domain (edicts of government, 17 U.S.C.
s 105 rationale).

The corpus is published by the State Library of Louisiana as part of its
CONTENTdm "Louisiana Public Documents Digital Archive" (alias
``p267101coll4``) on the OCLC-hosted CONTENTdm instance at
``https://cdm16313.contentdm.oclc.org/``. CONTENTdm exposes a public,
un-authenticated JSON web-services API (``dmwebservices``). The archive holds
~44,700 heterogeneous public documents, so the LAC volumes are isolated with a
server-side title search (``title^Louisiana Administrative Code^all^and`` ->
736 items across all Titles/Parts and dated editions):

  - Enumerate:  dmQuery/p267101coll4/title^Louisiana Administrative Code^all^and/
                dmrecord!title!date/title/{max}/{start}/1/0/0/0/json
                -> pager.total + records[].dmrecord (each an item pointer)
  - Per item:   dmGetItemInfo/p267101coll4/{dmrecord}/json
                -> Dublin-Core fields: title, date (YYYY-MM / YYYY), descri,
                   subjec, find (PDF file name)
  - Full text:  /digital/api/collection/p267101coll4/id/{pointer}/download
                -> born-digital PDF (embedded text layer); extracted via the
                   shared, OOM-hardened common.pdf_extract helper.

Each LAC volume PDF is large (a whole Title/Part, hundreds of pages ->
millions of chars of regulatory text), but they are streamed and processed one
at a time.

Usage:
  python bootstrap.py bootstrap            # Full pull (~736 volumes)
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
logger = logging.getLogger("legal-data-hunter.US.LA-AdministrativeCode")

CDM_HOST = "https://cdm16313.contentdm.oclc.org"
DMWS = CDM_HOST + "/digital/bl/dmwebservices/index.php?q="
ALIAS = "p267101coll4"
# Server-side title filter isolates the LAC volumes from the ~44,700-item
# "Louisiana Public Documents Digital Archive".
SEARCH = "title^Louisiana Administrative Code^all^and"
TITLE_PREFIX = "louisiana administrative code"
ITEM_URL = CDM_HOST + "/digital/collection/" + ALIAS + "/id/{rec}"
DOWNLOAD_URL = CDM_HOST + "/digital/api/collection/" + ALIAS + "/id/{rec}/download"

WS_RE = re.compile(r"[ \t\r\f\v]+")
NL_RE = re.compile(r"\n{3,}")
YMD_RE = re.compile(r"^(\d{4})-(\d{1,2})-(\d{1,2})")
YM_RE = re.compile(r"^(\d{4})-(\d{1,2})$")
Y_RE = re.compile(r"^(\d{4})$")
TITLE_NO_RE = re.compile(r"Title\s+([0-9IVXLC]+)", re.I)
MIN_TEXT_CHARS = 200

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")


class LAAdministrativeCodeScraper(BaseScraper):

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
                with urlopen(req, timeout=180) as resp:
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
        """LAC dates arrive as YYYY-MM-DD, YYYY-MM or YYYY. Normalise to a
        full ISO date (first of the month/year for partial values)."""
        if not raw or not isinstance(raw, str):
            return None
        raw = raw.strip()
        m = YMD_RE.match(raw)
        if m:
            yy, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
        else:
            m = YM_RE.match(raw)
            if m:
                yy, mm, dd = int(m.group(1)), int(m.group(2)), 1
            else:
                m = Y_RE.match(raw)
                if not m:
                    return None
                yy, mm, dd = int(m.group(1)), 1, 1
        if 1 <= mm <= 12 and 1 <= dd <= 31 and 1900 <= yy <= 2100:
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
    def discover_pointers(self, sample: bool = False):
        """Yield (pointer, cdm_title) tuples for the LAC volumes only."""
        start = 0
        page = 1500 if not sample else 60
        seen: set[str] = set()
        search = quote(SEARCH, safe="^")
        while True:
            path = (f"dmQuery/{ALIAS}/{search}/dmrecord!title!date/title/"
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
                title = str(r.get("title") or "").strip()
                if not title.lower().startswith(TITLE_PREFIX):
                    continue
                seen.add(rec)
                yield rec, title
                if sample and len(seen) >= 30:
                    logger.info(f"Discovered {len(seen)} LAC pointers (sample)")
                    return
            total = int(data.get("pager", {}).get("total", 0) or 0)
            start += len(recs)
            if start >= total or len(recs) < page:
                break
        logger.info(f"Discovered {len(seen)} Louisiana Administrative Code pointers")

    # ------------------------------------------------------- build record
    def _build_raw(self, rec: str, cdm_title: str) -> dict | None:
        info = self._get_json(f"dmGetItemInfo/{ALIAS}/{rec}/json")
        if not info or not isinstance(info, dict):
            return None
        pdf = self._get_pdf(rec)
        if not pdf:
            return None
        text = self._clean_text(
            extract_pdf_markdown("US/LA-AdministrativeCode", rec,
                                 pdf_bytes=pdf, table="legislation",
                                 force=True) or "")
        if not text or len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text for rec {rec} "
                           f"({len(text)} chars), likely scanned image")
            return None

        info_title = self._field(info.get("title")) or cdm_title
        descri = self._field(info.get("descri"))
        subjec = self._field(info.get("subjec"))
        date = self._iso_date(self._field(info.get("date")))

        m = TITLE_NO_RE.search(info_title)
        title_no = m.group(1) if m else None

        title = info_title.strip()
        if date:
            title = f"{title} ({date[:7]})"

        return {
            "rec": rec,
            "lac_title_number": title_no,
            "edition": descri,
            "subject": subjec,
            "title": title[:300],
            "text": text,
            "date": date,
            "url": ITEM_URL.format(rec=rec),
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Louisiana Administrative Code CONTENTdm API...")
        try:
            recs = list(self.discover_pointers(sample=True))
            if not recs:
                logger.error("  No pointers discovered")
                return False
            logger.info(f"  Discovered {len(recs)} pointers (sample)")
            raw = None
            for rec, ct in recs:
                raw = self._build_raw(rec, ct)
                if raw:
                    break
            if raw and raw["text"] and len(raw["text"]) > MIN_TEXT_CHARS:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('title')} [{raw.get('date')}]")
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
            "_id": f"US/LA-AdministrativeCode/{raw['rec']}",
            "_source": "US/LA-AdministrativeCode",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["rec"],
            "lac_title_number": raw.get("lac_title_number"),
            "edition": raw.get("edition"),
            "subject": raw.get("subject"),
            "issuer": "Louisiana Office of the State Register",
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-LA",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for rec, ct in self.discover_pointers(sample=sample):
            raw = self._build_raw(rec, ct)
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

    parser = argparse.ArgumentParser(description="US/LA-AdministrativeCode bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = LAAdministrativeCodeScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
