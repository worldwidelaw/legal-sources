#!/usr/bin/env python3
"""
US/NM-TaxRulings -- New Mexico Taxation and Revenue Department (Rulings)

Fetches the full text of the interpretive Rulings the New Mexico Taxation
and Revenue Department (TRD) publishes at
https://www.tax.newmexico.gov/all-nm-taxes/rulings/.

A Ruling is a written statement of the Department's interpretation of how
the tax laws and regulations apply to a stated set of facts. They are
official state-government interpretive guidance, not adjudications of a
contested case, so the corpus is `doctrine`. (The adjudicatory
hearing-officer "Decisions & Orders" are a separate, case_law collection.)

Access (no JavaScript-rendered scraping needed, no CAPTCHA, no auth):
  The rulings page embeds a "RealFile" (rtsclients.com) file-browser widget.
  The folder tree and files are served by a public JSON API:

      GET https://klvg4oyd4j.execute-api.us-west-2.amazonaws.com/prod/GetWidgetFiles
          ?widgetId=...&folderId=...&rootFolderId=...
           &accountGUID=...&publicTokenGUID=...

  which returns {data: {folders: [...], files: [...]}}. Files are grouped
  into category folders (e.g. "400-Gross Receipts and Compensating Tax Act"),
  most of which contain tax-type subfolders. Each file has a fileId, name
  (e.g. "200-11-01.pdf") and title; the born-digital PDF downloads from:

      GET https://klvg4oyd4j.execute-api.us-west-2.amazonaws.com/prod/
          PublicFiles/{accountGUID}/{fileId}/{fileName}

  Full text lives only in the PDF, so PDF extraction is mandatory.

Strategy:
  1. Recursively walk the widget folder tree from the root folder,
     collecting every PDF file descriptor (fileId, name, title, uploaded).
  2. Download each PDF and extract text via the shared, OOM-hardened
     common.pdf_extract helper (pdfplumber -> pypdf -> OCR fallback).
  3. Normalize into the standard doctrine schema.

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
logger = logging.getLogger("legal-data-hunter.US.NM-TaxRulings")

# RealFile public file-browser widget backing tax.newmexico.gov/all-nm-taxes/rulings/
API_BASE = "https://klvg4oyd4j.execute-api.us-west-2.amazonaws.com/prod"
ACCOUNT_GUID = "34821a9573ca43e7b06dfad20f5183fd"
WIDGET_ID = "5e9e105f-f1a6-4c2e-825f-9ce2c9d55e3f"
ROOT_FOLDER_ID = "69bf9c97-04f7-4383-803c-83122c9d2f0a"
PUBLIC_TOKEN_GUID = "0f69200a-5699-48e6-a041-ce3d1324140c"
PAGE_URL = "https://www.tax.newmexico.gov/all-nm-taxes/rulings/"

MIN_TEXT_CHARS = 200
MAX_DEPTH = 6


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


class NMTaxRulingsScraper(BaseScraper):

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

    def discover_documents(self) -> Generator[dict, None, None]:
        """Recursively walk the RealFile widget folder tree, yielding PDF descriptors."""
        seen: set[str] = set()
        total = 0
        stack: list[tuple[str, str, int]] = [(ROOT_FOLDER_ID, "", 0)]
        # Avoid revisiting folders (the tree includes a "000-Table of Contents"
        # folder that may shortcut into the same categories).
        visited_folders: set[str] = set()

        while stack:
            folder_id, category, depth = stack.pop()
            if folder_id in visited_folders or depth > MAX_DEPTH:
                continue
            visited_folders.add(folder_id)

            data = self._list_folder(folder_id)
            if data is None:
                logger.error(f"Failed to list folder {folder_id} ({category})")
                continue

            for sub in (data.get("folders") or []):
                sub_id = sub.get("folderId")
                if not sub_id or sub_id in visited_folders:
                    continue
                sub_name = (sub.get("name") or sub.get("title") or "").strip()
                stack.append((sub_id, category or sub_name, depth + 1))

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
                total += 1
                yield {
                    "file_id": file_id,
                    "name": name,
                    "pdf_url": self._file_url(file_id, name),
                    "title": title or None,
                    "category": category or None,
                    "date": _epoch_ms_to_iso(f.get("uploaded")),
                    "slug": self._slug(file_id, name),
                }
        logger.info(f"Discovered {total} NM TRD ruling PDFs")

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._get_bytes(doc["pdf_url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/NM-TaxRulings",
            doc["slug"],
            pdf_bytes=pdf_bytes,
            table="doctrine",
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
        logger.info("Testing New Mexico TRD Rulings (RealFile widget API)...")
        try:
            docs = []
            for d in self.discover_documents():
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
    def _ruling_number(name: str) -> str | None:
        # File names look like "200-11-01.pdf" -> ruling number "200-11-01".
        stem = (name or "").rsplit(".", 1)[0].strip()
        if re.fullmatch(r"[0-9][0-9A-Za-z._-]+", stem):
            return stem
        return None

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard doctrine schema."""
        number = self._ruling_number(raw.get("name"))
        subject = (raw.get("title") or "").strip()
        if subject and number and subject != number:
            title = f"New Mexico TRD Ruling {subject}"
        elif number:
            title = f"New Mexico TRD Ruling {number}"
        elif subject:
            title = f"New Mexico TRD Ruling: {subject}"
        else:
            title = "New Mexico TRD Ruling"
        title = title[:300]
        return {
            "_id": f"US/NM-TaxRulings/{raw['slug']}",
            "_source": "US/NM-TaxRulings",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "ruling_number": number,
            "ruling_type": "Ruling",
            "tax_category": raw.get("category") or None,
            "issuer": "New Mexico Taxation and Revenue Department",
            "title": title,
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-NM",
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        examined = 0
        for doc in self.discover_documents():
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

    parser = argparse.ArgumentParser(description="US/NM-TaxRulings bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NMTaxRulingsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
