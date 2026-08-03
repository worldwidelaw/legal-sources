#!/usr/bin/env python3
"""
US/OR-DAPublicRecords -- Oregon District Attorney Public Records Orders

Fetches the full text of the public-records appeal orders issued by Oregon
District Attorneys. Under the Oregon Public Records Law (ORS 192.415) a person
whose request for records held by a LOCAL public body is denied may petition
the District Attorney of the county in which the body is located; the DA then
adjudicates the petition and issues a written order granting or denying
disclosure (the county-level counterpart of the Attorney General's orders for
state agencies, cf. US/OR-AGPublicRecords). Each order resolves a specific
contested public-records appeal = case_law. These are official Oregon
state/local-government works in the public domain (government edicts).

The corpus is published by the Oregon State Library as a CONTENTdm digital
collection (alias ``p17027coll4``, "District Attorney Public Records Orders")
on the OCLC-hosted CONTENTdm instance at
``https://cdm17027.contentdm.oclc.org/``. CONTENTdm exposes a public,
un-authenticated JSON web-services API (``dmwebservices``):

  - Enumerate:  dmQuery/p17027coll4/0/dmrecord!date/dmrecord/{max}/{start}/0/0/0/0/json
                -> pager.total + records[].dmrecord (each an item pointer)
  - Per item:   dmGetItemInfo/p17027coll4/{dmrecord}/json
                -> Dublin-Core fields:
                     title   = Case Name
                     subjec  = Petitioner
                     descri  = Respondent (public body)
                     date    = Date of Order (ISO YYYY-MM-DD)
                     type    = Primary Exemptions at Issue
                     format  = Records Requested
                     source  = Result
                     county  = County
                     find    = PDF file name

Unlike the sibling CONTENTdm collections, the ``transc`` (Transcript) field is
EMPTY for this collection, so the full order text must be extracted from the
item's PDF, downloaded from the CONTENTdm download endpoint
(/digital/api/collection/{alias}/id/{pointer}/download). PDF extraction uses
the shared, OOM-hardened ``common.pdf_extract`` helper (pdfplumber -> pypdf ->
OCR fallback).

Usage:
  python bootstrap.py bootstrap            # Full pull (~89 orders)
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
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.OR-DAPublicRecords")

CDM_HOST = "https://cdm17027.contentdm.oclc.org"
DMWS = CDM_HOST + "/digital/bl/dmwebservices/index.php?q="
ALIAS = "p17027coll4"
ITEM_URL = CDM_HOST + "/digital/collection/" + ALIAS + "/id/{rec}"
DOWNLOAD_URL = CDM_HOST + "/digital/api/collection/" + ALIAS + "/id/{rec}/download"

WS_RE = re.compile(r"[ \t\r\f\v]+")
NL_RE = re.compile(r"\n{3,}")
ISO_DATE_RE = re.compile(r"(\d{4})-(\d{1,2})-(\d{1,2})")
US_DATE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")
MIN_TEXT_CHARS = 200

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")


class ORDAPublicRecordsScraper(BaseScraper):

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

    @staticmethod
    def _field(val):
        """CONTENTdm returns {} for empty fields; coerce to str-or-None."""
        if val is None or isinstance(val, (dict, list)):
            return None
        s = str(val).strip()
        return s or None

    # --------------------------------------------------------- discovery
    def discover_pointers(self, sample: bool = False) -> Generator[str, None, None]:
        start = 0
        page = 1000 if not sample else 40
        seen: set[str] = set()
        while True:
            path = (f"dmQuery/{ALIAS}/0/dmrecord!date/dmrecord/"
                    f"{page}/{start}/0/0/0/0/json")
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
                seen.add(rec)
                yield rec
            total = int(data.get("pager", {}).get("total", 0) or 0)
            start += len(recs)
            if sample or start >= total or len(recs) < page:
                break
        logger.info(f"Discovered {len(seen)} DA public-records order pointers")

    # ------------------------------------------------------- build record
    def _build_raw(self, rec: str) -> dict | None:
        info = self._get_json(f"dmGetItemInfo/{ALIAS}/{rec}/json")
        if not info or not isinstance(info, dict):
            return None
        pdf = self._get_pdf(rec)
        if not pdf:
            return None
        text = self._clean_text(
            extract_pdf_markdown("US/OR-DAPublicRecords", rec,
                                 pdf_bytes=pdf, table="case_law", force=True) or "")
        if not text or len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text for rec {rec} "
                           f"({len(text)} chars), likely scanned")
            return None
        case_name = self._field(info.get("title"))
        petitioner = self._field(info.get("subjec"))
        respondent = self._field(info.get("descri"))
        county = self._field(info.get("county"))
        if case_name:
            title = f"Oregon DA Public Records Order — {case_name}"
        elif petitioner and respondent:
            title = f"Oregon DA Public Records Order — {petitioner} v. {respondent}"
        else:
            title = f"Oregon DA Public Records Order {rec}"
        if county:
            title = f"{title} ({county} County)"
        return {
            "rec": rec,
            "case_name": case_name,
            "petitioner": petitioner,
            "respondent": respondent,
            "county": county,
            "exemptions": self._field(info.get("type")),
            "records_requested": self._field(info.get("format")),
            "result": self._field(info.get("source")),
            "title": title[:300],
            "text": text,
            "date": self._iso_date(self._field(info.get("date"))),
            "url": ITEM_URL.format(rec=rec),
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Oregon DA Public Records Orders CONTENTdm API...")
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
                            f"{raw.get('case_name')} [{raw.get('date')}]")
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
            "_id": f"US/OR-DAPublicRecords/{raw['rec']}",
            "_source": "US/OR-DAPublicRecords",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["rec"],
            "case_name": raw.get("case_name"),
            "petitioner": raw.get("petitioner"),
            "respondent": raw.get("respondent"),
            "county": raw.get("county"),
            "exemptions": raw.get("exemptions"),
            "records_requested": raw.get("records_requested"),
            "result": raw.get("result"),
            "issuer": "Oregon District Attorney",
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-OR",
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

    parser = argparse.ArgumentParser(description="US/OR-DAPublicRecords bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = ORDAPublicRecordsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
