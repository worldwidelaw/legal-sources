#!/usr/bin/env python3
"""
US/VT-LRB -- Vermont Labor Relations Board Decisions

Fetches the full text of the decisions of the Vermont Labor Relations
Board (VLRB), the quasi-judicial state agency that adjudicates public-
and private-sector labor-relations disputes in Vermont under the State
Employees Labor Relations Act (SELRA, 3 V.S.A. ch. 27), the Municipal
Employees Labor Relations Act (21 V.S.A. ch. 22), the Labor Relations
for Teachers Act (16 V.S.A. ch. 57), the Judiciary Employees Labor
Relations Act (JELRA), and the State Employees Grievance system. The
Board decides grievances, unfair-labor-practice charges, representation
and election petitions, unit determinations, and related contested
cases. Each decision resolves a specific case = case_law, and they are
official Vermont state-government works in the public domain
(government edicts, 17 U.S.C. Sec. 105 analogue for state edicts).

BUILD RECIPE (no auth, no CAPTCHA, builds locally): the Board publishes
its complete run of bound decision volumes (Volume 1: 1977-78 through
the current volume) as per-volume ZIP archives linked from

  https://vlrb.vermont.gov/decisions/download

Volumes 1-34 are direct ``/sites/vlrb/files/.../Volume N.zip`` links;
later volumes (35+) are linked via a Drupal node/document page whose
body holds the ZIP link. Each ZIP contains one born-digital PDF per
decision, named ``Volume N/N-PPP <caption>.pdf`` where ``N-PPP`` is the
volume/page citation and the caption names the parties. The scraper
walks the download page, resolves every volume ZIP, downloads each ZIP
once, and extracts full text from each member PDF with the shared
``common.pdf_extract`` extractor. The internal docket number
("DOCKET NO. YY-NN") and the decision date ("dated ... Nth day of
Month, YYYY") are parsed from the decision body.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import io
import json
import logging
import re
import time
import zipfile
import html as _html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import urljoin, quote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.VT-LRB")

HOST = "https://vlrb.vermont.gov"
DOWNLOAD_PAGE = HOST + "/decisions/download"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")

TAG_RE = re.compile(r"<[^>]+>")
# Links on the download page: direct ZIPs, or node/document pages carrying a ZIP.
ANCHOR_RE = re.compile(r'<a\s[^>]*href="([^"]+)"[^>]*>(.*?)</a>', re.IGNORECASE | re.DOTALL)
ZIP_HREF_RE = re.compile(r'href="([^"]+?\.zip)"', re.IGNORECASE)
VOLUME_TXT_RE = re.compile(r"volume", re.IGNORECASE)
MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july",
     "august", "september", "october", "november", "december"], start=1)}
# "20th day of October 2017" / "27th day of April, 2018"
DATED_RE = re.compile(
    r"day\s+of\s+([A-Za-z]+),?\s+(\d{4})", re.IGNORECASE)
DATED_DAY_RE = re.compile(
    r"(\d{1,2})(?:st|nd|rd|th)?\s+day\s+of\s+([A-Za-z]+),?\s+(\d{4})",
    re.IGNORECASE)
DOCKET_RE = re.compile(r"DOCKET\s+NO\.?\s*([0-9A-Za-z][0-9A-Za-z\-/]*)", re.IGNORECASE)
# Volume label year, e.g. "Volume 34: 2017-2018" -> capture last 4-digit year.
LABEL_YEAR_RE = re.compile(r"(\d{4})")


class VLRBScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.6
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": UA})
        self._existing: set[str] = set()

    # ---------------------------------------------------------------- http
    def _get_text(self, url: str) -> str | None:
        for attempt in range(5):
            time.sleep(self.delay)
            try:
                resp = self._session.get(url, timeout=(15, 90))
                resp.raise_for_status()
                resp.encoding = resp.apparent_encoding or "utf-8"
                return resp.text
            except Exception as e:
                logger.warning(f"GET failed ({url[:90]}) attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return None

    def _get_bytes(self, url: str) -> bytes | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                resp = self._session.get(url, timeout=(15, 180), stream=True)
                resp.raise_for_status()
                return resp.content
            except Exception as e:
                logger.warning(f"ZIP GET failed ({url[:90]}) attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _clean(cell: str) -> str:
        txt = _html.unescape(TAG_RE.sub(" ", cell)).replace("\xa0", " ")
        return re.sub(r"\s+", " ", txt).strip()

    @staticmethod
    def _slug(text: str) -> str:
        return re.sub(r"[^A-Za-z0-9]+", "-", text).strip("-")[:120]

    @classmethod
    def _body_date(cls, text: str, fallback_year: str | None) -> str | None:
        # Prefer the LAST full "Nth day of Month, YYYY" (the signature date).
        matches = list(DATED_DAY_RE.finditer(text or ""))
        if matches:
            m = matches[-1]
            dd, mon, yy = int(m.group(1)), m.group(2).lower(), int(m.group(3))
            mm = MONTHS.get(mon)
            if mm and 1 <= dd <= 31 and 1970 <= yy <= 2100:
                return f"{yy:04d}-{mm:02d}-{dd:02d}"
        if fallback_year and re.fullmatch(r"\d{4}", fallback_year):
            return f"{fallback_year}-01-01"
        return None

    # --------------------------------------------------------- discovery
    def _volume_zips(self) -> list[dict]:
        """Return a list of {label, zip_url} for every decision volume."""
        html = self._get_text(DOWNLOAD_PAGE)
        if not html:
            return []
        vols: list[dict] = []
        seen: set[str] = set()
        for href, raw_txt in ANCHOR_RE.findall(html):
            label = self._clean(raw_txt)
            if not VOLUME_TXT_RE.search(label):
                continue
            abs_url = urljoin(HOST + "/", href)
            if abs_url.lower().endswith(".zip"):
                zip_url = abs_url
            elif "/node/" in abs_url or "/document/" in abs_url:
                # Drupal doc page -> resolve the inner ZIP link.
                page = self._get_text(abs_url)
                m = ZIP_HREF_RE.search(page or "")
                if not m:
                    logger.warning(f"No ZIP found on volume page {abs_url}")
                    continue
                zip_url = urljoin(HOST + "/", m.group(1))
            else:
                continue
            if zip_url in seen:
                continue
            seen.add(zip_url)
            vols.append({"label": label, "zip_url": zip_url})
        return vols

    # ------------------------------------------------------- build record
    def _iter_volume(self, vol: dict, sample: bool) -> Generator[dict, None, None]:
        year_m = None
        for y in LABEL_YEAR_RE.findall(vol["label"]):
            year_m = y  # last year in the label range
        blob = self._get_bytes(vol["zip_url"])
        if not blob:
            return
        try:
            zf = zipfile.ZipFile(io.BytesIO(blob))
        except Exception as e:
            logger.warning(f"Bad ZIP {vol['zip_url']}: {e}")
            return
        for info in zf.infolist():
            name = info.filename
            if info.is_dir() or not name.lower().endswith(".pdf"):
                continue
            member = name.rsplit("/", 1)[-1]
            stem = re.sub(r"(?i)\.pdf$", "", member)
            record_id = self._slug(stem)
            if not record_id or record_id in self._existing:
                continue
            try:
                pdf_bytes = zf.read(info)
            except Exception as e:
                logger.warning(f"Read failed {name}: {e}")
                continue
            text = extract_pdf_markdown(
                "US/VT-LRB", record_id, pdf_bytes=pdf_bytes, table="case_law"
            )
            if not text or len(text.strip()) < 400:
                logger.warning(f"No usable text for {member} "
                               f"({len(text or '')} chars) — skipping")
                continue
            text = text.strip()
            dk = DOCKET_RE.search(text)
            docket = self._clean(dk.group(1)) if dk else None
            date = self._body_date(text, year_m)
            title = re.sub(r"\s+", " ", stem).strip()
            yield {
                "record_id": record_id,
                "docket": docket,
                "volume": vol["label"],
                "title": title[:500],
                "text": text,
                "date": date,
                "url": vol["zip_url"],
            }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Vermont Labor Relations Board download page...")
        try:
            vols = self._volume_zips()
            if not vols:
                logger.error("  No decision volumes discovered")
                return False
            logger.info(f"  Discovered {len(vols)} decision volumes")
            raw = None
            for rec in self._iter_volume(vols[-1], sample=True):
                raw = rec
                break
            if raw and raw["text"] and len(raw["text"]) > 400:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw['record_id']} docket={raw['docket']} "
                            f"[{raw['date']}]")
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
            "_id": f"US/VT-LRB/{raw['record_id']}",
            "_source": "US/VT-LRB",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["record_id"],
            "issuer": "Vermont Labor Relations Board",
            "docket": raw.get("docket"),
            "volume": raw.get("volume"),
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-VT",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        if not sample:
            try:
                self._existing = preload_existing_ids("US/VT-LRB", "case_law")
            except Exception as e:
                logger.warning(f"preload_existing_ids failed: {e}")
                self._existing = set()
        # Process newest volumes first: recent decisions are born-digital
        # (clean text, reliable docket/date), and a sample of the first N
        # records is then representative of the modern corpus. A full pull
        # still covers every volume; order does not affect completeness.
        vols = list(reversed(self._volume_zips()))
        emitted = 0
        for vol in vols:
            logger.info(f"Volume: {vol['label']} -> {vol['zip_url']}")
            for raw in self._iter_volume(vol, sample=sample):
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

    parser = argparse.ArgumentParser(description="US/VT-LRB bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = VLRBScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
