#!/usr/bin/env python3
"""
US/LA-AGOpinions -- Louisiana Attorney General Opinions

Fetches the full text of the formal advisory opinions of the Louisiana
Attorney General. Under La. R.S. 49:251-253 the Attorney General renders
written legal opinions, on request, to state and local public officials and
bodies construing the constitution, statutes and regulations of Louisiana.
Each opinion is an authoritative statement of the state's legal position on a
question of law = doctrine (official advisory legal opinion of the state, cf.
the sibling US state AG-opinion sources US/OR-AGOpinions, US/CA-AGOpinions,
US/NC-AGOpinions, ...). These are official Louisiana state-government works in
the public domain (government edicts, 17 U.S.C. s 105 rationale).

The corpus is published by the State Library of Louisiana as part of its
CONTENTdm "Louisiana Public Documents Digital Archive" (alias
``p267101coll4``) on the OCLC-hosted CONTENTdm instance at
``https://cdm16313.contentdm.oclc.org/``. CONTENTdm exposes a public,
un-authenticated JSON web-services API (``dmwebservices``). The archive holds
~44,700 heterogeneous public documents, so the AG opinions are isolated with a
server-side title search (``title^Attorney General^all^and`` -> ~1,790 items,
both individual opinions and the weekly opinion summaries):

  - Enumerate:  dmQuery/p267101coll4/title^Attorney General^all^and/
                dmrecord!date/date/{max}/{start}/1/0/0/0/json
                -> pager.total + records[].dmrecord (each an item pointer)
  - Per item:   dmGetItemInfo/p267101coll4/{dmrecord}/json
                -> Dublin-Core fields:
                     title   = "Attorney General's Opinions" / "... [Summary]"
                     date    = Date of opinion (ISO YYYY-MM-DD)
                     descri  = Caption (e.g. "January 24, 2024; Opinion 23-0132")
                     subjec  = Subject headings
                     find    = PDF file name

The ``transc`` field is empty for this collection, so the full opinion text is
extracted from the item's PDF, downloaded from the CONTENTdm download endpoint
(/digital/api/collection/{alias}/id/{pointer}/download). PDF extraction uses
the shared, OOM-hardened ``common.pdf_extract`` helper (pdfplumber -> pypdf ->
OCR fallback). The archived opinion PDFs carry an embedded text layer (the
State Library's own OCR); the minority of pure image scans (0 chars) are
skipped.

Usage:
  python bootstrap.py bootstrap            # Full pull (~1,790 opinions)
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
logger = logging.getLogger("legal-data-hunter.US.LA-AGOpinions")

CDM_HOST = "https://cdm16313.contentdm.oclc.org"
DMWS = CDM_HOST + "/digital/bl/dmwebservices/index.php?q="
ALIAS = "p267101coll4"
# Server-side title filter isolates the AG opinions from the ~44,700-item
# "Louisiana Public Documents Digital Archive".
SEARCH = "title^Attorney General^all^and"
ITEM_URL = CDM_HOST + "/digital/collection/" + ALIAS + "/id/{rec}"
DOWNLOAD_URL = CDM_HOST + "/digital/api/collection/" + ALIAS + "/id/{rec}/download"

WS_RE = re.compile(r"[ \t\r\f\v]+")
NL_RE = re.compile(r"\n{3,}")
ISO_DATE_RE = re.compile(r"(\d{4})-(\d{1,2})-(\d{1,2})")
US_DATE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")
OP_NUM_RE = re.compile(r"Opinion\s+([0-9]{2,4}[-–][0-9]{3,4}[A-Z]?)", re.I)
MIN_TEXT_CHARS = 200

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")


class LAAGOpinionsScraper(BaseScraper):

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
        """Yield pointers for the individual AG opinions only.

        The title search "Attorney General" also returns the weekly opinion
        summaries ("... [Summary]") and the Attorney General's monthly press
        columns; both are excluded by keeping only records whose title is
        exactly "Attorney General's Opinions" (1,163 of the 1,793 hits).
        """
        start = 0
        page = 1000 if not sample else 80
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
                title = str(r.get("title") or "").strip().lower()
                if title != "attorney general's opinions":
                    continue
                seen.add(rec)
                yield rec
                if sample and len(seen) >= 30:
                    logger.info(f"Discovered {len(seen)} AG opinion pointers (sample)")
                    return
            total = int(data.get("pager", {}).get("total", 0) or 0)
            start += len(recs)
            if start >= total or len(recs) < page:
                break
        logger.info(f"Discovered {len(seen)} Louisiana AG opinion pointers")

    # ------------------------------------------------------- build record
    def _build_raw(self, rec: str) -> dict | None:
        info = self._get_json(f"dmGetItemInfo/{ALIAS}/{rec}/json")
        if not info or not isinstance(info, dict):
            return None
        pdf = self._get_pdf(rec)
        if not pdf:
            return None
        text = self._clean_text(
            extract_pdf_markdown("US/LA-AGOpinions", rec,
                                 pdf_bytes=pdf, table="doctrine", force=True) or "")
        if not text or len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text for rec {rec} "
                           f"({len(text)} chars), likely scanned image")
            return None

        descri = self._field(info.get("descri"))
        subjec = self._field(info.get("subjec"))
        date = self._iso_date(self._field(info.get("date")))

        # Opinion number lives in the caption ("...; Opinion 23-0132") or,
        # failing that, in the opinion body text ("OPINION 23-0132").
        op_num = None
        for hay in (descri or "", text[:1200]):
            m = OP_NUM_RE.search(hay)
            if m:
                op_num = m.group(1).replace("–", "-")
                break

        if op_num:
            title = f"Louisiana Attorney General Opinion {op_num}"
        else:
            title = "Louisiana Attorney General Opinion"
        if date:
            title = f"{title} ({date})"

        return {
            "rec": rec,
            "opinion_number": op_num,
            "caption": descri,
            "subject": subjec,
            "title": title[:300],
            "text": text,
            "date": date,
            "url": ITEM_URL.format(rec=rec),
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Louisiana AG Opinions CONTENTdm API...")
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
                            f"{raw.get('opinion_number')} [{raw.get('date')}]")
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
            "_id": f"US/LA-AGOpinions/{raw['rec']}",
            "_source": "US/LA-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["rec"],
            "opinion_number": raw.get("opinion_number"),
            "caption": raw.get("caption"),
            "subject": raw.get("subject"),
            "issuer": "Louisiana Attorney General",
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

    parser = argparse.ArgumentParser(description="US/LA-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = LAAGOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
