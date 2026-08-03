#!/usr/bin/env python3
"""
US/LA-Register -- Louisiana Register

Fetches the full text of the Louisiana Register -- the official monthly
publication of the Louisiana Office of the State Register (La. R.S. 49:954.1),
the state's counterpart of the Federal Register. Each monthly issue publishes
the state's rulemaking record: gubernatorial Executive Orders, Emergency
Rules, adopted Rules, Notices of Intent (proposed rules), Potpourri, committee
reports and Attorney General opinion summaries. Adopted rules published here
are later codified into the Louisiana Administrative Code (US/LA-AdministrativeCode).
The rulemaking record is legally operative secondary legislation = legislation.
These are official Louisiana state-government works in the public domain
(edicts of government, 17 U.S.C. s 105 rationale).

The corpus is published by the State Library of Louisiana as part of its
CONTENTdm "Louisiana Public Documents Digital Archive" (alias
``p267101coll4``) on the OCLC-hosted CONTENTdm instance at
``https://cdm16313.contentdm.oclc.org/`` -- the same archive as the sibling
sources US/LA-AGOpinions, US/LA-ExecutiveOrders and US/LA-AdministrativeCode.
CONTENTdm exposes a public, un-authenticated JSON web-services API
(``dmwebservices``). A server-side title search isolates the Register issues
(``title^Louisiana Register^all^and`` -> 640 monthly issues, 1975-present):

  - Enumerate:  dmQuery/p267101coll4/title^Louisiana Register^all^and/
                dmrecord!title!date/date/{max}/{start}/1/0/0/0/json
  - Per item:   dmGetItemInfo/p267101coll4/{dmrecord}/json
                -> Dublin-Core: title, date, descri (issue month), subjec
  - Full text:  /digital/api/collection/p267101coll4/id/{pointer}/download
                -> PDF; born-digital text layer for the modern issues (the
                   minority of pre-digital scanned issues yield 0 chars and are
                   skipped by the <200-char guard). Extracted via the shared,
                   OOM-hardened common.pdf_extract helper.

Usage:
  python bootstrap.py bootstrap            # Full pull (~640 issues)
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
logger = logging.getLogger("legal-data-hunter.US.LA-Register")

CDM_HOST = "https://cdm16313.contentdm.oclc.org"
DMWS = CDM_HOST + "/digital/bl/dmwebservices/index.php?q="
ALIAS = "p267101coll4"
SEARCH = "title^Louisiana Register^all^and"
EXACT_TITLE = "louisiana register"
ITEM_URL = CDM_HOST + "/digital/collection/" + ALIAS + "/id/{rec}"
DOWNLOAD_URL = CDM_HOST + "/digital/api/collection/" + ALIAS + "/id/{rec}/download"

WS_RE = re.compile(r"[ \t\r\f\v]+")
NL_RE = re.compile(r"\n{3,}")
YMD_RE = re.compile(r"^(\d{4})-(\d{1,2})-(\d{1,2})")
YM_RE = re.compile(r"^(\d{4})-(\d{1,2})$")
Y_RE = re.compile(r"^(\d{4})$")
MIN_TEXT_CHARS = 200

MONTHS = ["", "January", "February", "March", "April", "May", "June",
          "July", "August", "September", "October", "November", "December"]

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")


class LARegisterScraper(BaseScraper):

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
                logger.warning(f"Non-PDF payload for rec {rec} ({data[:16]!r})")
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
    def _parse_date(raw: str | None):
        """Return (iso_date, year, month) from YYYY-MM-DD / YYYY-MM / YYYY."""
        if not raw or not isinstance(raw, str):
            return None, None, None
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
                    return None, None, None
                yy, mm, dd = int(m.group(1)), 1, 1
        if not (1 <= mm <= 12 and 1 <= dd <= 31 and 1900 <= yy <= 2100):
            return None, None, None
        return f"{yy:04d}-{mm:02d}-{dd:02d}", yy, mm

    @staticmethod
    def _field(val):
        if val is None or isinstance(val, (dict, list)):
            return None
        s = str(val).strip()
        return s or None

    # --------------------------------------------------------- discovery
    def _query_records(self, page: int, start: int):
        search = quote(SEARCH, safe="^")
        path = (f"dmQuery/{ALIAS}/{search}/dmrecord!title!date/date/"
                f"{page}/{start}/1/0/0/0/json")
        return self._get_json(path)

    def discover_pointers(self, sample: bool = False):
        """Yield (pointer, raw_date) for the Louisiana Register issues only.

        Sample mode fetches the whole list in one query and yields the most
        recent issues first (born-digital text layer), so samples do not stall
        on the pre-digital scanned issues from the 1970s-90s.
        """
        if sample:
            data = self._query_records(1000, 0)
            recs = (data or {}).get("records") or []
            items = []
            for r in recs:
                rec = str(r.get("dmrecord") or r.get("pointer") or "").strip()
                if not rec or r.get("parentobject") not in (None, -1, "-1"):
                    continue
                if str(r.get("title") or "").strip().lower() != EXACT_TITLE:
                    continue
                items.append((rec, self._field(r.get("date"))))
            items.sort(key=lambda t: t[1] or "", reverse=True)
            for rec, rd in items[:30]:
                yield rec, rd
            logger.info(f"Discovered {len(items)} Register pointers (sample, recent-first)")
            return

        start = 0
        page = 1000
        seen: set[str] = set()
        while True:
            data = self._query_records(page, start)
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
                title = str(r.get("title") or "").strip().lower()
                if title != EXACT_TITLE:
                    continue
                seen.add(rec)
                yield rec, self._field(r.get("date"))
            total = int(data.get("pager", {}).get("total", 0) or 0)
            start += len(recs)
            if start >= total or len(recs) < page:
                break
        logger.info(f"Discovered {len(seen)} Louisiana Register pointers")

    # ------------------------------------------------------- build record
    def _build_raw(self, rec: str, raw_date: str | None) -> dict | None:
        info = self._get_json(f"dmGetItemInfo/{ALIAS}/{rec}/json")
        if not info or not isinstance(info, dict):
            return None
        pdf = self._get_pdf(rec)
        if not pdf:
            return None
        text = self._clean_text(
            extract_pdf_markdown("US/LA-Register", rec, pdf_bytes=pdf,
                                 table="legislation", force=True) or "")
        if not text or len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text for rec {rec} "
                           f"({len(text)} chars), likely pre-digital scan")
            return None

        descri = self._field(info.get("descri"))
        subjec = self._field(info.get("subjec"))
        date, yy, mm = self._parse_date(self._field(info.get("date")) or raw_date)

        if yy and mm and mm >= 1:
            label = f"{MONTHS[mm]} {yy}" if 1 <= mm <= 12 else str(yy)
        elif descri:
            label = descri
        elif date:
            label = date
        else:
            label = rec
        title = f"Louisiana Register — {label}"

        return {
            "rec": rec,
            "issue": descri or label,
            "subject": subjec,
            "title": title[:300],
            "text": text,
            "date": date,
            "url": ITEM_URL.format(rec=rec),
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Louisiana Register CONTENTdm API...")
        try:
            recs = list(self.discover_pointers(sample=True))
            if not recs:
                logger.error("  No pointers discovered")
                return False
            logger.info(f"  Discovered {len(recs)} pointers (sample)")
            raw = None
            for rec, rd in recs:
                raw = self._build_raw(rec, rd)
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
            "_id": f"US/LA-Register/{raw['rec']}",
            "_source": "US/LA-Register",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["rec"],
            "issue": raw.get("issue"),
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
        for rec, rd in self.discover_pointers(sample=sample):
            raw = self._build_raw(rec, rd)
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

    parser = argparse.ArgumentParser(description="US/LA-Register bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = LARegisterScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
