#!/usr/bin/env python3
"""
US/SEC-AdminProceedings -- SEC Administrative Proceedings (orders & decisions)

Fetches the full text of the U.S. Securities and Exchange Commission's
published Administrative Proceeding documents -- the orders instituting
proceedings, ALJ initial decisions, Commission opinions and orders,
settlement orders, and related determinations issued in the SEC's
administrative enforcement and regulatory adjudications. Each release
resolves or advances a specific administrative proceeding against named
respondents = case_law, and they are official works of the U.S. federal
government in the public domain (17 U.S.C. 105).

BUILD RECIPE (no auth, no CAPTCHA, builds locally): the SEC publishes the
proceedings as a paginated Drupal View table at

  https://www.sec.gov/enforcement-litigation/administrative-proceedings?page=N

Each row (``<tr class="pr-list-page-row">``) carries a ``<time datetime>``
publish date, the respondent name as the anchor text of a link to the
document PDF at ``/files/litigation/admin/{YYYY}/{release}[-suffix].pdf``,
and the SEC Release No. (e.g. ``34-105843``). The scraper walks the pager
from page 0 until a page yields no proceeding rows, downloads each PDF,
and extracts full text with the shared ``common.pdf_extract`` extractor
(born-digital PDFs have a clean text layer; older scans fall back to OCR).
``record_id`` is the PDF filename stem (e.g. ``34-105843``), which is
stable and unique.

NOTE: sec.gov requires a declared User-Agent identifying the requester
(SEC automated-access policy); the scraper sends one.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import os
import sys
import json
import logging
import re
import time
import html as _html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import urljoin

import requests

# Make locally-installed OCR tools (tesseract/poppler) discoverable so that
# common.pdf_extract's OCR fallback works for any older scanned order.
os.environ["PATH"] = "/opt/homebrew/bin:" + os.environ.get("PATH", "")

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.SEC-AdminProceedings")

HOST = "https://www.sec.gov"
LIST_PAGE = HOST + "/enforcement-litigation/administrative-proceedings?page={n}"

# SEC automated-access policy requires a descriptive UA with contact info.
UA = "LegalDataHunter research zacharie@goodlegal.fr"

ROW_RE = re.compile(r'<tr class="pr-list-page-row">(.*?)</tr>',
                    re.IGNORECASE | re.DOTALL)
TIME_RE = re.compile(r'<time[^>]*datetime="([^"]+)"', re.IGNORECASE)
PDF_HREF_RE = re.compile(
    r"href='?\"?([^'\"]*?/files/litigation/admin/[^'\"]+?\.pdf)", re.IGNORECASE)
ANCHOR_TEXT_RE = re.compile(
    r"/files/litigation/admin/[^'\"]+?\.pdf'?\"?[^>]*>(.*?)</a>",
    re.IGNORECASE | re.DOTALL)
RELEASE_RE = re.compile(
    r'release_number[^>]*>\s*<span[^>]*>Release No\.</span>\s*'
    r'<span[^>]*>(.*?)</span>', re.IGNORECASE | re.DOTALL)
TAG_RE = re.compile(r"<[^>]+>")

MAX_PAGES = 2000  # generous upper bound; loop stops on the first empty page


class SECAdminScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.7
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": UA})
        self._existing: set[str] = set()

    # ---------------------------------------------------------------- http
    def _get_text(self, url: str) -> str | None:
        for attempt in range(5):
            time.sleep(self.delay)
            try:
                resp = self._session.get(url, timeout=(15, 90))
                if resp.status_code == 404:
                    return None
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
                resp = self._session.get(url, timeout=(15, 120), stream=True)
                resp.raise_for_status()
                return resp.content
            except Exception as e:
                logger.warning(f"PDF GET failed ({url[:90]}) attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _clean(s: str) -> str:
        return re.sub(r"\s+", " ", _html.unescape(TAG_RE.sub(" ", s or ""))).strip()

    @staticmethod
    def _iso(dt: str) -> str | None:
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})", dt or "")
        return m.group(0) if m else None

    @staticmethod
    def _stem(pdf_url: str) -> str:
        name = pdf_url.rsplit("/", 1)[-1]
        return re.sub(r"(?i)\.pdf$", "", name)

    # --------------------------------------------------------- discovery
    def _rows_from_page(self, html: str) -> Generator[dict, None, None]:
        for raw in ROW_RE.findall(html):
            mp = PDF_HREF_RE.search(raw)
            if not mp:
                continue
            pdf_url = urljoin(HOST, mp.group(1))
            date = None
            mt = TIME_RE.search(raw)
            if mt:
                date = self._iso(mt.group(1))
            ma = ANCHOR_TEXT_RE.search(raw)
            respondent = self._clean(ma.group(1)) if ma else ""
            mr = RELEASE_RE.search(raw)
            release = self._clean(mr.group(1)) if mr else ""
            yield {
                "pdf_url": pdf_url,
                "record_id": self._stem(pdf_url),
                "respondent": respondent,
                "release": release,
                "date": date,
            }

    def discover(self, sample: bool = False) -> Generator[dict, None, None]:
        seen: set[str] = set()
        page = 0
        empty_pages = 0
        while page < MAX_PAGES:
            html = self._get_text(LIST_PAGE.format(n=page))
            if not html:
                break
            rows = list(self._rows_from_page(html))
            if not rows:
                empty_pages += 1
                if empty_pages >= 2:
                    break
                page += 1
                continue
            empty_pages = 0
            for rec in rows:
                key = rec["record_id"].lower()
                if not key or key in seen:
                    continue
                seen.add(key)
                yield rec
                if sample and len(seen) >= 20:
                    logger.info(f"Sample: stopped after {len(seen)} pointers")
                    return
            page += 1
        logger.info(f"Discovered {len(seen)} SEC admin-proceeding pointers "
                    f"across {page + 1} page(s)")

    # ------------------------------------------------------- build record
    def _build_raw(self, entry: dict) -> dict | None:
        rid = entry["record_id"]
        if rid in self._existing:
            return None
        pdf_bytes = self._get_bytes(entry["pdf_url"])
        if not pdf_bytes:
            return None
        if not pdf_bytes[:5].startswith(b"%PDF"):
            logger.warning(f"{rid}: response is not a PDF — skipping")
            return None
        text = extract_pdf_markdown(
            "US/SEC-AdminProceedings", rid, pdf_bytes=pdf_bytes, table="case_law")
        if not text or len(text.strip()) < 300:
            logger.warning(f"No usable text for {rid} "
                           f"({len(text or '')} chars) — skipping")
            return None
        text = text.strip()
        respondent = entry.get("respondent") or ""
        release = entry.get("release") or ""
        if respondent and release:
            title = f"In re {respondent} (SEC Release No. {release})"
        elif respondent:
            title = f"In re {respondent} (SEC Administrative Proceeding {rid})"
        else:
            title = f"SEC Administrative Proceeding {rid}"
        return {
            "record_id": rid,
            "respondent": respondent or None,
            "release": release or None,
            "title": title[:500],
            "text": text,
            "date": entry.get("date"),
            "url": entry["pdf_url"],
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing SEC administrative-proceedings list...")
        try:
            entries = list(self.discover(sample=True))
            if not entries:
                logger.error("  No proceeding pointers discovered")
                return False
            logger.info(f"  Discovered {len(entries)} pointers (sample)")
            raw = None
            for e in entries:
                raw = self._build_raw(e)
                if raw:
                    break
            if raw and raw["text"] and len(raw["text"]) > 300:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw['record_id']} [{raw['date']}]")
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
            "_id": f"US/SEC-AdminProceedings/{raw['record_id']}",
            "_source": "US/SEC-AdminProceedings",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["record_id"],
            "issuer": "U.S. Securities and Exchange Commission (SEC)",
            "respondent": raw.get("respondent"),
            "release_no": raw.get("release"),
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        if not sample:
            try:
                self._existing = preload_existing_ids(
                    "US/SEC-AdminProceedings", "case_law")
            except Exception as e:
                logger.warning(f"preload_existing_ids failed: {e}")
                self._existing = set()
        emitted = 0
        for entry in self.discover(sample=sample):
            raw = self._build_raw(entry)
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

    parser = argparse.ArgumentParser(description="US/SEC-AdminProceedings bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = SECAdminScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
