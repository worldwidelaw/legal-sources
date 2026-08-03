#!/usr/bin/env python3
"""
US/NM-TaxDecisions -- New Mexico Taxation and Revenue Department
(Decisions & Orders -- hearing-officer adjudicatory decisions)

Fetches the full text of the "Decisions & Orders" the New Mexico Taxation
and Revenue Department (TRD) publishes at
https://www.tax.newmexico.gov/all-nm-taxes/tax-decisions-orders/.

These are written Decisions and Orders issued by an Administrative Hearings
Office hearing officer resolving a taxpayer's protest of an assessment or
denial -- adjudications of a contested case -- so the corpus is `case_law`.
(The TRD interpretive "Rulings" are a separate `doctrine` collection,
US/NM-TaxRulings.)

Access (no JavaScript-rendered scraping needed, no CAPTCHA, no auth):
  The page embeds the same "RealFile" (rtsclients.com) file-browser widget
  used by US/NM-TaxRulings, but a different folder/widget. The folder tree
  and files are served by a public JSON API:

      GET https://klvg4oyd4j.execute-api.us-west-2.amazonaws.com/prod/GetWidgetFiles
          ?widgetId=...&folderId=...&rootFolderId=...
           &accountGUID=...&publicTokenGUID=...

  The root folder holds one subfolder per year (1994..present); each year
  folder holds the decision PDFs, named like "25-04 Raymond Merrick.pdf"
  (decision number + taxpayer caption). The born-digital PDF downloads from:

      GET https://klvg4oyd4j.execute-api.us-west-2.amazonaws.com/prod/
          PublicFiles/{accountGUID}/{fileId}/{fileName}

  Full text lives only in the PDF, so PDF extraction is mandatory.

Strategy:
  1. Recursively walk the widget folder tree from the root folder,
     collecting every PDF descriptor (fileId, name, title, uploaded) and
     tagging it with the enclosing year folder.
  2. Download each PDF and extract text via the shared, OOM-hardened
     common.pdf_extract helper (pdfplumber -> pypdf -> OCR fallback).
  3. Normalize into the standard case_law schema.

Usage:
  python bootstrap.py bootstrap            # Full pull
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
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NM-TaxDecisions")

# RealFile public file-browser widget backing
# tax.newmexico.gov/all-nm-taxes/tax-decisions-orders/
API_BASE = "https://klvg4oyd4j.execute-api.us-west-2.amazonaws.com/prod"
ACCOUNT_GUID = "34821a9573ca43e7b06dfad20f5183fd"
WIDGET_ID = "be24c963-0ff1-4a56-b188-dbc2ee378313"
ROOT_FOLDER_ID = "8224075e-df4f-429b-8ca4-d1d7992afe4a"
PUBLIC_TOKEN_GUID = "0f69200a-5699-48e6-a041-ce3d1324140c"
PAGE_URL = "https://www.tax.newmexico.gov/all-nm-taxes/tax-decisions-orders/"

MIN_TEXT_CHARS = 200
MAX_DEPTH = 4
YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _epoch_ms_to_iso(ms) -> str | None:
    try:
        ms = int(ms)
        if ms <= 0:
            return None
        dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        if 1960 <= dt.year <= 2100:
            return dt.date().isoformat()
    except (TypeError, ValueError, OverflowError, OSError):
        return None
    return None


class NMTaxDecisionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json,text/plain,*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": PAGE_URL,
            },
            timeout=90,
        )
        self.delay = 1.0

    # ---- fetch helpers -------------------------------------------------

    def _list_folder(self, folder_id: str, retries: int = 4) -> dict | None:
        params = {
            "widgetId": WIDGET_ID,
            "folderId": folder_id,
            "rootFolderId": ROOT_FOLDER_ID,
            "accountGUID": ACCOUNT_GUID,
            "publicTokenGUID": PUBLIC_TOKEN_GUID,
        }
        url = f"{API_BASE}/GetWidgetFiles?" + urllib.parse.urlencode(params)
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    payload = resp.json()
                    if payload.get("status") == "ok":
                        return payload.get("data") or {}
                    logger.warning(f"API status {payload.get('status')} for {folder_id}")
                else:
                    logger.warning(f"HTTP {resp.status_code} for folder {folder_id}")
            except Exception as e:
                logger.warning(f"Error listing folder {folder_id} "
                               f"(attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _get_bytes(self, url: str, retries: int = 3) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.content:
                    return resp.content
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching PDF {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- discovery -----------------------------------------------------

    @staticmethod
    def _slug(file_id: str, name: str) -> str:
        base = urllib.parse.unquote(name or "").rsplit(".", 1)[0]
        base = re.sub(r"[^A-Za-z0-9._-]+", "-", base).strip("-")[:80]
        short = (file_id or "")[:8]
        return f"{base}-{short}" if base else short

    @staticmethod
    def _file_url(file_id: str, name: str) -> str:
        path = f"PublicFiles/{ACCOUNT_GUID}/{file_id}/{name}"
        return urllib.parse.urljoin(API_BASE + "/", urllib.parse.quote(path))

    def discover_documents(self, sample: bool = False) -> Generator[dict, None, None]:
        """Recursively walk the RealFile widget folder tree, yielding PDF descriptors."""
        seen: set[str] = set()
        total = 0
        # (folder_id, year_label, depth). Years are direct children of root.
        stack: list[tuple[str, str | None, int]] = [(ROOT_FOLDER_ID, None, 0)]
        visited_folders: set[str] = set()

        while stack:
            folder_id, year, depth = stack.pop()
            if folder_id in visited_folders or depth > MAX_DEPTH:
                continue
            visited_folders.add(folder_id)

            data = self._list_folder(folder_id)
            if data is None:
                logger.error(f"Failed to list folder {folder_id} (year={year})")
                continue

            for sub in (data.get("folders") or []):
                sub_id = sub.get("folderId")
                if not sub_id or sub_id in visited_folders:
                    continue
                sub_name = (sub.get("name") or sub.get("title") or "").strip()
                ym = YEAR_RE.search(sub_name)
                sub_year = year or (ym.group(0) if ym else None)
                stack.append((sub_id, sub_year, depth + 1))

            for f in (data.get("files") or []):
                file_id = f.get("fileId")
                name = (f.get("name") or "").strip()
                if not file_id or not name:
                    continue
                if (f.get("type") or "").lower() != "pdf" and not name.lower().endswith(".pdf"):
                    continue
                if file_id in seen:
                    continue
                seen.add(file_id)
                title = (f.get("title")
                         or (f.get("metadata") or {}).get("4")
                         or name).strip()
                date = _epoch_ms_to_iso(f.get("uploaded"))
                if not date and year:
                    date = f"{year}-01-01"
                total += 1
                yield {
                    "file_id": file_id,
                    "name": name,
                    "pdf_url": self._file_url(file_id, name),
                    "title": title or None,
                    "year": year,
                    "date": date,
                    "slug": self._slug(file_id, name),
                }
                if sample and total >= 40:
                    return
        logger.info(f"Discovered {total} NM TRD Decision & Order PDFs")

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._get_bytes(doc["pdf_url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/NM-TaxDecisions",
            doc["slug"],
            pdf_bytes=pdf_bytes,
            table="case_law",
            force=True,
        )
        text = clean_text(text or "")
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text ({len(text)} chars), likely "
                           f"scanned: {doc['slug']}")
            return None
        doc = dict(doc)
        doc["text"] = text
        return doc

    # ---- public interface ----------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing New Mexico TRD Decisions & Orders (RealFile widget API)...")
        try:
            docs = []
            for d in self.discover_documents(sample=True):
                docs.append(d)
                if len(docs) >= 5:
                    break
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)}+ documents (partial crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) >= MIN_TEXT_CHARS:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('name')}")
            else:
                logger.error("  Text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    @staticmethod
    def _parse_caption(name: str) -> tuple[str | None, str | None]:
        """Parse the decision number and taxpayer caption from a file name.

        Handles both space- and underscore-separated names, e.g.
        '25-04 Raymond Merrick.pdf'      -> ('25-04', 'Raymond Merrick')
        '12-04_california_closets.pdf'   -> ('12-04', 'california closets')
        '12-23__cordero_transport.pdf'   -> ('12-23', 'cordero transport')
        """
        stem = (name or "").rsplit(".", 1)[0].strip()
        # Decision numbers look like NN-NN / NN-NNN with an optional letter
        # suffix (e.g. 25-08, 12-04, 04-13A), followed by a space/underscore
        # separator and then the caption.
        m = re.match(r"^(\d{2,4}-\d{1,4}[A-Za-z]?)[\s_]+(.*)$", stem)
        if m:
            number = m.group(1)
            caption = re.sub(r"[\s_]+", " ", m.group(2)).strip(" -")
            return number or None, caption or None
        return None, re.sub(r"[\s_]+", " ", stem).strip() or None

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard case_law schema."""
        number, caption = self._parse_caption(raw.get("name"))
        # Prefer the widget 'title' if it carries the caption text.
        wt = (raw.get("title") or "").rsplit(".", 1)[0].strip()
        if wt and number and wt != number and len(wt) > len(number) + 1:
            _, wt_cap = self._parse_caption((raw.get("title") or "") + ".pdf")
            caption = wt_cap or caption
        case_name = caption
        if number and case_name:
            title = f"NM TRD Decision & Order {number}: {case_name}"
        elif number:
            title = f"NM TRD Decision & Order {number}"
        elif case_name:
            title = f"NM TRD Decision & Order: {case_name}"
        else:
            title = "NM TRD Decision & Order"
        title = title[:300]
        return {
            "_id": f"US/NM-TaxDecisions/{raw['slug']}",
            "_source": "US/NM-TaxDecisions",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "docket_number": number,
            "court": "New Mexico Taxation and Revenue Department, "
                     "Administrative Hearings Office",
            "case_name": case_name,
            "title": title,
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "year": raw.get("year") or None,
            "jurisdiction": "US-NM",
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        examined = 0
        for doc in self.discover_documents(sample=sample):
            examined += 1
            raw = self._build_raw(doc)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return
            if sample and examined >= 40:
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

    parser = argparse.ArgumentParser(description="US/NM-TaxDecisions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NMTaxDecisionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
