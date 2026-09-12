#!/usr/bin/env python3
"""
MZ/JurisMZ -- Mozambique Case Law Database (JURIS MZ)

Fetches acórdãos (court decisions) from "Jurisprudência Moçambicana" (JURIS MZ),
the official ECLI-indexed case-law database of the Supreme Court of Mozambique
(Tribunal Supremo), hosted at https://juris.ts.gov.mz and built with technical
support from the Portuguese Superior Council of the Judiciary (CSM).

Strategy:
  1. List all records via the JSON endpoint /items/loadItems (dynatable data URL).
     GET with X-Requested-With header returns {records: [...], totalRecordCount}.
  2. For each ECLI, fetch the HTML record page /juris/{ecli}/ and extract the
     structured full text: Sumário (headnote) + Decisão Texto Parcial (full
     reasoning) + Decisão Texto Integral (operative ruling), plus metadata
     (relator, processo, data do acórdão, área temática).

Full text is clean HTML — no OCR / PDF extraction needed.

Usage:
  python bootstrap.py bootstrap          # Fetch all decisions
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py bootstrap-fast     # Concurrent full run (fleet entry point)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional
from html import unescape

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MZ.JurisMZ")

BASE_URL = "https://juris.ts.gov.mz"
LIST_ENDPOINT = "/items/loadItems"
PER_PAGE = 100

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"[ \t ]+")
MULTINL_RE = re.compile(r"\n{3,}")


def _clean(fragment: str) -> str:
    """Strip HTML tags and collapse whitespace from an HTML fragment."""
    if not fragment:
        return ""
    # Convert block boundaries to newlines so paragraphs stay separated.
    text = re.sub(r"(?i)</(p|div|h[1-6]|li|br)\s*>", "\n", fragment)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = TAG_RE.sub("", text)
    text = unescape(text)
    text = WS_RE.sub(" ", text)
    text = "\n".join(line.strip() for line in text.splitlines())
    text = MULTINL_RE.sub("\n\n", text)
    return text.strip()


def _extract_div_by_id(html: str, div_id: str) -> str:
    """Return the inner HTML of <div id="div_id"> ... </div>, balancing nesting."""
    m = re.search(r'<div[^>]*\bid=["\']' + re.escape(div_id) + r'["\'][^>]*>', html)
    if not m:
        return ""
    start = m.end()
    depth = 1
    pos = start
    tag_re = re.compile(r"<(/?)div\b[^>]*>", re.IGNORECASE)
    for tm in tag_re.finditer(html, start):
        if tm.group(1):  # closing </div>
            depth -= 1
        else:
            depth += 1
        if depth == 0:
            return html[start:tm.start()]
        pos = tm.end()
    return html[start:pos]


class MZJurisMZScraper(BaseScraper):
    """Scraper for MZ/JurisMZ -- Mozambique Supreme Court case-law database."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "application/json, text/html",
            "X-Requested-With": "XMLHttpRequest",
        })

    # ------------------------------------------------------------------ listing
    def _load_items(self, offset: int) -> dict:
        """One page of the listing, starting at `offset`."""
        time.sleep(1.0)
        resp = self.session.get(
            f"{BASE_URL}{LIST_ENDPOINT}",
            params={"perPage": PER_PAGE, "offset": offset},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()

    def _list_records(self) -> list:
        """Fetch the full list of record stubs (ECLI + metadata) from loadItems.

        The endpoint answers HTTP 200 to a `page` parameter but ignores it —
        every page returned the same first 100 stubs, so the old walk appended
        the same batch until `len(records)` passed totalRecordCount and stopped
        with 100 distinct ECLIs of 324 (issue #1600). `offset` is the cursor the
        server actually honours. Dedupe on ECLI and stop as soon as a page adds
        nothing new, so a future silent revert caps the run instead of looping.
        """
        data = self._load_items(0)
        total = int(data.get("totalRecordCount", 0))

        records: list = []
        seen: set = set()
        offset = 0
        while True:
            batch = data.get("records", [])
            if not batch:
                break
            fresh = [r for r in batch if r.get("ecli") not in seen]
            if not fresh:
                logger.warning(
                    "offset=%d returned %d stubs, all already seen — the server "
                    "is ignoring the cursor; stopping at %d of %d",
                    offset, len(batch), len(records), total,
                )
                break
            seen.update(r.get("ecli") for r in fresh)
            records.extend(fresh)
            offset += len(batch)
            if offset >= total or offset > 100_000:  # safety
                break
            data = self._load_items(offset)

        if total and len(records) < total:
            self.record_coverage_gap(
                "listing",
                f"listed {len(records)} of {total} records reported by "
                f"totalRecordCount",
                endpoint=f"{BASE_URL}{LIST_ENDPOINT}",
            )
        logger.info(f"Listed {len(records)} records (totalRecordCount={total})")
        return records

    # ----------------------------------------------------------------- detail
    def _fetch_record_html(self, ecli: str) -> Optional[str]:
        """Fetch the HTML record page for an ECLI."""
        url = f"{BASE_URL}/juris/{ecli}/"
        for attempt in range(3):
            try:
                time.sleep(1.0)
                resp = self.session.get(url, timeout=60)
                if resp.status_code == 200 and resp.text:
                    return resp.text
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Fetch attempt {attempt + 1} for {ecli}: {e}")
                time.sleep(3)
        return None

    def _parse_metadata(self, html: str) -> Dict[str, str]:
        """Extract label:value metadata pairs from the descriptors block."""
        meta: Dict[str, str] = {}
        block = _extract_div_by_id(html, "descriptors") or html
        pair_re = re.compile(
            r'<strong class="content-title">([^<]+?):\s*</strong>\s*(?:&nbsp;)?\s*'
            r'<span class="content">(.*?)</span>',
            re.IGNORECASE | re.DOTALL,
        )
        for label, value in pair_re.findall(block):
            key = _clean(label).strip().lower()
            meta[key] = _clean(value)
        return meta

    def _build_text(self, html: str) -> str:
        """Assemble the full-text field from the structured content sections."""
        parts = []
        summary = _clean(_extract_div_by_id(html, "summary"))
        if summary:
            parts.append(summary if summary.lower().startswith("sum")
                         else "Sumário\n" + summary)
        parcial = _clean(_extract_div_by_id(html, "parcial-text"))
        if parcial:
            parts.append(parcial)
        integral = _clean(_extract_div_by_id(html, "integral-text"))
        if integral:
            parts.append(integral)
        text = "\n\n".join(p for p in parts if p)
        return MULTINL_RE.sub("\n\n", text).strip()

    @staticmethod
    def _iso_date(value: str) -> Optional[str]:
        """Convert dd/mm/yyyy -> yyyy-mm-dd."""
        if not value:
            return None
        m = re.search(r"(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{4})", value)
        if m:
            d, mo, y = m.groups()
            return f"{y}-{int(mo):02d}-{int(d):02d}"
        m = re.search(r"(\d{4})-(\d{2})-(\d{2})", value)
        return m.group(0) if m else None

    # --------------------------------------------------------------- normalize
    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        now = datetime.now(timezone.utc).isoformat()
        ecli = raw.get("ecli", "")
        if not ecli:
            return None

        html = raw.get("_html")
        if html is None:
            html = self._fetch_record_html(ecli)
        if not html:
            return None

        meta = self._parse_metadata(html)
        text = self._build_text(html)
        if not text or len(text) < 80:
            logger.warning(f"  Text too short for {ecli} ({len(text or '')} chars)")
            return None

        date = self._iso_date(meta.get("data do acordão") or raw.get("dataAcordao", ""))
        relator = meta.get("relator") or _clean(str(raw.get("relator", "")))
        processo = meta.get("processo") or meta.get("nº convencional", "")
        area = meta.get("área temática") or _clean(str(raw.get("tematica", "")))
        tribunal = _clean(str(raw.get("tribunal", ""))) or "Tribunal Supremo de Moçambique"

        title = ecli
        if processo:
            title = f"{ecli} — Processo {processo}"

        return {
            "_id": ecli,
            "_source": "MZ/JurisMZ",
            "_type": "case_law",
            "_fetched_at": now,
            "title": title,
            "text": text,
            "date": date,
            "url": f"{BASE_URL}/juris/{ecli}/",
            "ecli": ecli,
            "court": tribunal,
            "relator": relator,
            "process_number": processo,
            "area_tematica": area,
        }

    # ----------------------------------------------------------------- fetch
    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        # Let a listing failure raise: swallowing it here turned an unreachable
        # host into an exit-0 run with zero records, which reads as "the corpus
        # is empty" instead of "we never got to ask".
        records = self._list_records()
        if not records:
            raise RuntimeError(
                f"{BASE_URL}{LIST_ENDPOINT} listed no records — the corpus is "
                "~324 acórdãos, so an empty listing is a fetch failure"
            )

        count = 0
        for stub in records:
            ecli = stub.get("ecli")
            if not ecli:
                continue
            html = self._fetch_record_html(ecli)
            if not html:
                continue
            stub["_html"] = html
            yield stub
            count += 1
            if count % 25 == 0:
                logger.info(f"  Fetched {count}/{len(records)} record pages")
        logger.info(f"Fetched {count} record pages total")

    def fetch_updates(self, since) -> Generator[Dict[str, Any], None, None]:
        # Small curated corpus — re-list everything and let upsert dedup.
        yield from self.fetch_all()

    def test_connection(self):
        records = self._list_records()
        print(f"OK — listed {len(records)} records")
        if records:
            ecli = records[0]["ecli"]
            html = self._fetch_record_html(ecli)
            print(f"First record {ecli}: {len(html or '')} bytes HTML")


if __name__ == "__main__":
    scraper = MZJurisMZScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
    elif command == "bootstrap-fast":
        scraper.bootstrap_fast()
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
