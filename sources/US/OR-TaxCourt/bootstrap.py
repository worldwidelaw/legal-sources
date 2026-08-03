#!/usr/bin/env python3
"""
US/OR-TaxCourt -- Oregon Tax Court (Regular + Magistrate Divisions)

Fetches the full text of every decision, opinion and order of the Oregon
Tax Court — the specialised state court with exclusive jurisdiction over
Oregon tax appeals (income, property, timber, corporation excise, etc.).
The corpus is the complete digitised run of the *Oregon Tax Reports*
(OTR) plus the more recent Regular- and Magistrate-Division decisions,
back to volume 1 (1962/1972) and updated as new decisions issue.

Each document resolves a tax controversy (taxpayer v. Department of
Revenue or a county assessor), so the corpus is case_law.

Source of truth: the Oregon Judicial Department publishes the opinions in
a CONTENTdm digital collection (alias ``p17027coll6``) hosted at
``ojd.contentdm.oclc.org``. The public CONTENTdm web-services API exposes
the entire collection with no JavaScript, no CAPTCHA and no auth:

  * dmQuery       — enumerate every item pointer in the collection.
  * dmGetItemInfo — per-item metadata PLUS the full opinion text in the
                    ``transc`` (transcript) field, so no PDF download or
                    OCR is needed. Each record also carries the official
                    case name, citation (NN OTR NNN), decision date,
                    judge, case number and division.

If an item's ``transc`` field is ever empty, the scraper falls back to
downloading the item's PDF (``/digital/api/collection/.../download``) and
extracting its text layer via common.pdf_extract.

Strategy:
  1. Page dmQuery over the whole collection to collect every top-level
     PDF item pointer (~8,260 records).
  2. For each pointer, GET dmGetItemInfo and read ``transc`` + metadata.
  3. Normalize into the standard case_law schema.

Usage:
  python bootstrap.py bootstrap            # Full pull (all decisions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample decisions
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
logger = logging.getLogger("legal-data-hunter.US.OR-TaxCourt")

CDM_HOST = "https://ojd.contentdm.oclc.org"
COLLECTION = "p17027coll6"
DM_API = CDM_HOST + "/digital/bl/dmwebservices/index.php?q="
PAGE_SIZE = 1000


class ORTaxCourtScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120 Safari/537.36"
        )

    # ---- low-level HTTP -------------------------------------------------

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

    def _api_json(self, query: str) -> dict | list | None:
        raw = self._curl_bytes(DM_API + query)
        if not raw:
            return None
        try:
            return json.loads(raw.decode("utf-8", "replace"))
        except Exception as e:
            logger.warning(f"JSON parse failed for {query}: {e}")
            return None

    # ---- discovery ------------------------------------------------------

    def discover_pointers(self, sample: bool = False) -> list[int]:
        """Return every top-level PDF item pointer in the collection.

        In sample mode only the first page is read."""
        pointers: list[int] = []
        seen: set[int] = set()
        start = 1
        while True:
            q = (f"dmQuery/{COLLECTION}/0/dmrecord!filetype!parentobject/"
                 f"title/{PAGE_SIZE}/{start}/0/0/0/0/json")
            data = self._api_json(q)
            if not isinstance(data, dict):
                break
            recs = data.get("records") or []
            for r in recs:
                # Only top-level PDF objects (skip compound-object children).
                if r.get("filetype") != "pdf":
                    continue
                if str(r.get("parentobject", "-1")) not in ("-1", ""):
                    continue
                try:
                    ptr = int(r.get("pointer"))
                except (TypeError, ValueError):
                    continue
                if ptr in seen:
                    continue
                seen.add(ptr)
                pointers.append(ptr)
            total = int(data.get("pager", {}).get("total", 0) or 0)
            logger.info(f"  dmQuery start={start}: cumulative {len(pointers)} "
                        f"pointers (collection total {total})")
            if sample:
                break
            start += PAGE_SIZE
            if start > total or not recs:
                break
        logger.info(f"Discovered {len(pointers)} Oregon Tax Court documents")
        return pointers

    # ---- per-item -------------------------------------------------------

    @staticmethod
    def _field(info: dict, key: str) -> str | None:
        v = info.get(key)
        if v is None or isinstance(v, (dict, list)):
            return None
        v = str(v).strip()
        return v or None

    @staticmethod
    def _slug(info: dict, pointer: int) -> str:
        ident = info.get("identi")
        if isinstance(ident, str) and ident.strip():
            stem = re.sub(r"\.pdf$", "", ident.strip(), flags=re.I)
            stem = re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-")
            if stem:
                return stem[:180]
        return str(pointer)

    @staticmethod
    def _norm_date(raw_date: str | None) -> str | None:
        if not raw_date:
            return None
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw_date)
        if m:
            y, mo, d = (int(x) for x in m.groups())
            if 1900 <= y <= 2035 and 1 <= mo <= 12 and 1 <= d <= 31:
                return f"{y:04d}-{mo:02d}-{d:02d}"
        m = re.match(r"(\d{4})", raw_date)
        if m:
            y = int(m.group(1))
            if 1900 <= y <= 2035:
                return f"{y:04d}-01-01"
        return None

    def _build_raw(self, pointer: int) -> dict | None:
        info = self._api_json(f"dmGetItemInfo/{COLLECTION}/{pointer}/json")
        if not isinstance(info, dict):
            logger.warning(f"No item info for pointer {pointer}")
            return None

        text = self._field(info, "transc")
        if not text or len(text) < 150:
            # Fall back to the PDF text layer.
            dl = (f"{CDM_HOST}/digital/api/collection/{COLLECTION}/"
                  f"id/{pointer}/download")
            pdf_bytes = self._curl_bytes(dl)
            if pdf_bytes and pdf_bytes[:4] == b"%PDF":
                extracted = pdf_extract.extract_pdf_markdown(
                    "US/OR-TaxCourt", str(pointer), pdf_bytes=pdf_bytes,
                    table="case_law", force=True,
                )
                if extracted and len(extracted.strip()) >= 150:
                    text = extracted.strip()
        if not text or len(text.strip()) < 150:
            logger.warning(f"No usable text for pointer {pointer} "
                           f"({len(text) if text else 0} chars)")
            return None

        return {
            "pointer": pointer,
            "slug": self._slug(info, pointer),
            "case_name": self._field(info, "subjec"),
            "parties": self._field(info, "subjec1"),
            "case_number": self._field(info, "relispt"),
            "doc_type": self._field(info, "type"),
            "division": self._field(info, "divisi"),
            "judge": self._field(info, "judge"),
            "citation": self._field(info, "cita"),
            "summary": self._field(info, "descri"),
            "rights": self._field(info, "rights"),
            "text": text.strip(),
            "date": self._norm_date(self._field(info, "dated")),
            "url": f"{CDM_HOST}/digital/collection/{COLLECTION}/id/{pointer}",
        }

    # ---- framework hooks ------------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing Oregon Tax Court CONTENTdm API...")
        try:
            pointers = self.discover_pointers(sample=True)
            if not pointers:
                logger.error("  No pointers discovered")
                return False
            logger.info(f"  Discovered {len(pointers)} pointers (first page)")
            raw = self._build_raw(pointers[0])
            if raw and raw["text"] and len(raw["text"]) > 150:
                logger.info(f"  Full text OK ({len(raw['text'])} chars) — "
                            f"{raw.get('case_name')} [{raw.get('citation')}]")
            else:
                logger.error("  Full text fetch failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard case_law schema."""
        case_name = (raw.get("case_name") or "").strip()
        citation = (raw.get("citation") or "").strip()
        case_number = (raw.get("case_number") or "").strip()
        division = (raw.get("division") or "").strip()
        doc_type = (raw.get("doc_type") or "Opinion").strip()

        if case_name:
            title = case_name
        elif citation:
            title = citation
        else:
            title = f"Oregon Tax Court {doc_type}"
        bits = []
        if citation:
            bits.append(citation)
        if case_number and case_number not in title:
            bits.append(case_number)
        if bits:
            title = f"{title} ({', '.join(bits)})"
        title = title[:300]

        court = "Oregon Tax Court"
        if division:
            court = f"Oregon Tax Court, {division} Division"

        return {
            "_id": f"US/OR-TaxCourt/{raw['pointer']}",
            "_source": "US/OR-TaxCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "case_number": case_number or None,
            "citation": citation or None,
            "document_type": doc_type or None,
            "division": division or None,
            "court": court,
            "judge": raw.get("judge") or None,
            "parties": raw.get("parties") or None,
            "summary": raw.get("summary") or None,
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for ptr in self.discover_pointers(sample=sample):
            raw = self._build_raw(ptr)
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

    parser = argparse.ArgumentParser(description="US/OR-TaxCourt bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = ORTaxCourtScraper()

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
