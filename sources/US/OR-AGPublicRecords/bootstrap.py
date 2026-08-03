#!/usr/bin/env python3
"""
US/OR-AGPublicRecords -- Oregon Attorney General Public Records Orders

Fetches the full text of every Public Records Order issued by the Oregon
Attorney General. Under Oregon's Public Records Law (ORS 192.311–192.478),
a person denied access to a public record held by a state agency may
petition the Attorney General, who then issues a written ORDER either
directing the agency to disclose the record or upholding the denial. Each
order resolves a specific petition/dispute = case_law. These are official
Oregon state-government works in the public domain (government edicts).

The corpus is published by the Oregon State Library / DOJ as a CONTENTdm
digital collection (alias ``p17027coll2``, "Oregon Attorney General Public
Records Orders") on the OCLC-hosted CONTENTdm instance at
``https://cdm17027.contentdm.oclc.org/``. CONTENTdm exposes a public,
un-authenticated JSON web-services API (``dmwebservices``):

  - Enumerate:  dmQuery/p17027coll2/0/dmrecord!date/dmrecord/{max}/{start}/0/0/0/0/json
                -> pager.total + records[].dmrecord (each an item pointer)
  - Per item:   dmGetItemInfo/p17027coll2/{dmrecord}/json
                -> Dublin-Core fields, including:
                     title   = Petitioner + order date (caption)
                     subjec  = Public body / agency involved
                     date    = Order date (ISO YYYY-MM-DD)
                     transc  = full-text Transcript (the complete order text)

The ``transc`` field carries the FULL order text, so no PDF download /
extraction is required.

Usage:
  python bootstrap.py bootstrap            # Full pull (~2,682 orders)
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
import html as _html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.OR-AGPublicRecords")

CDM_HOST = "https://cdm17027.contentdm.oclc.org"
DMWS = CDM_HOST + "/digital/bl/dmwebservices/index.php?q="
ALIAS = "p17027coll2"
ITEM_URL = CDM_HOST + "/digital/collection/" + ALIAS + "/id/{rec}"

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"[ \t\r\f\v]+")
NL_RE = re.compile(r"\n{3,}")
ISO_DATE_RE = re.compile(r"(\d{4})-(\d{1,2})-(\d{1,2})")
US_DATE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")


class ORAGPublicRecordsScraper(BaseScraper):

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

    # ------------------------------------------------------------- helpers
    @classmethod
    def _clean_text(cls, fragment: str) -> str:
        if not fragment:
            return ""
        frag = re.sub(r"(?i)</p\s*>", "\n\n", fragment)
        frag = re.sub(r"(?i)<br\s*/?>", "\n", frag)
        txt = TAG_RE.sub(" ", frag)
        txt = _html.unescape(txt)
        lines = [WS_RE.sub(" ", ln).strip() for ln in txt.split("\n")]
        txt = "\n".join(lines)
        txt = NL_RE.sub("\n\n", txt)
        return txt.strip()

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
        logger.info(f"Discovered {len(seen)} AG public-records order pointers")

    # ------------------------------------------------------- build record
    def _build_raw(self, rec: str) -> dict | None:
        info = self._get_json(f"dmGetItemInfo/{ALIAS}/{rec}/json")
        if not info or not isinstance(info, dict):
            return None
        text = self._clean_text(self._field(info.get("transc")) or "")
        if not text or len(text) < 200:
            logger.warning(f"No usable transcript for rec {rec} "
                           f"({len(text)} chars)")
            return None
        caption = self._field(info.get("title"))
        public_body = self._field(info.get("subjec"))
        title = caption or f"Oregon AG Public Records Order {rec}"
        return {
            "rec": rec,
            "caption": caption,
            "public_body": public_body,
            "title": title,
            "text": text,
            "date": self._iso_date(self._field(info.get("date"))),
            "url": ITEM_URL.format(rec=rec),
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Oregon AG Public Records Orders CONTENTdm API...")
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
            if raw and raw["text"] and len(raw["text"]) > 200:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('caption')} [{raw.get('date')}]")
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
            "_id": f"US/OR-AGPublicRecords/{raw['rec']}",
            "_source": "US/OR-AGPublicRecords",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["rec"],
            "caption": raw.get("caption"),
            "public_body": raw.get("public_body"),
            "issuer": "Oregon Attorney General",
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

    parser = argparse.ArgumentParser(description="US/OR-AGPublicRecords bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = ORAGPublicRecordsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
