#!/usr/bin/env python3
"""
South Africa Open Gazettes Data Fetcher

Fetches ~42,254 South African government gazette documents from opengazettes.org.za.
Uses JSONL index for metadata + common/pdf_extract for PDF text extraction.

Index: archive.opengazettes.org.za/index/gazette-index-latest.jsonlines
PDFs: archive.opengazettes.org.za/archive/{jurisdiction}/{year}/{filename}.pdf

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test connectivity
"""

import json
import logging
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ZA.OpenGazettes")

INDEX_URL = "https://archive.opengazettes.org.za/index/gazette-index-latest.jsonlines"
MAX_PDF_BYTES = 50 * 1024 * 1024
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) LegalDataHunter/1.0"}
SOURCE_ID = "ZA/OpenGazettes"


def http_get_bytes(url: str, timeout: int = 60) -> Optional[bytes]:
    """Fetch URL and return raw bytes."""
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            cl = resp.headers.get("Content-Length")
            if cl and int(cl) > MAX_PDF_BYTES:
                logger.warning(f"PDF too large ({int(cl)} bytes), skipping: {url[:100]}")
                return None
            data = resp.read(MAX_PDF_BYTES + 1)
            if len(data) > MAX_PDF_BYTES:
                logger.warning(f"PDF exceeded {MAX_PDF_BYTES} bytes, skipping: {url[:100]}")
                return None
            return data
    except Exception as e:
        logger.warning(f"HTTP GET failed for {url[:120]}: {e}")
        return None


def parse_date(date_str: Optional[str]) -> Optional[str]:
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d", "%d %B %Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    m = re.search(r"dated\s+(\d{1,2}\s+\w+\s+\d{4})", date_str)
    if m:
        try:
            return datetime.strptime(m.group(1), "%d %B %Y").strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


class OpenGazettesScraper(BaseScraper):
    """Scraper for ZA/OpenGazettes - South Africa Open Gazettes."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self._index = None

    def _load_index(self) -> List[Dict[str, Any]]:
        if self._index is not None:
            return self._index

        logger.info("Downloading gazette index...")
        req = urllib.request.Request(INDEX_URL, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = resp.read().decode("utf-8")

        entries = []
        for line in data.strip().split("\n"):
            if line.strip():
                entries.append(json.loads(line))

        logger.info(f"Loaded {len(entries)} gazette entries from index")
        self._index = entries
        return entries

    def _fetch_document(self, entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        archive_url = entry.get("archive_url")
        if not archive_url:
            return None

        unique_id = entry.get("unique_id", "")
        if not unique_id:
            unique_id = entry.get("archive_path", "").replace("/", "-").replace(".pdf", "")

        pdf_bytes = http_get_bytes(archive_url)
        if not pdf_bytes:
            return None

        text = extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=unique_id,
            pdf_bytes=pdf_bytes,
            table="legislation",
        ) or ""

        if not text or len(text) < 50:
            return None

        pub_date = parse_date(entry.get("publication_date"))
        if not pub_date:
            pub_date = parse_date(entry.get("issue_title", ""))

        return {
            "unique_id": unique_id,
            "title": entry.get("issue_title") or entry.get("publication_title", ""),
            "text": text,
            "date": pub_date,
            "url": archive_url,
            "jurisdiction": entry.get("jurisdiction_name", "South Africa"),
            "jurisdiction_code": entry.get("jurisdiction_code", "ZA"),
            "publication_title": entry.get("publication_title"),
            "issue_number": str(entry["issue_number"]) if entry.get("issue_number") is not None else None,
            "volume_number": str(entry["volume_number"]) if entry.get("volume_number") is not None else None,
            "page_count": entry.get("pagecount"),
        }

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = raw.get("text", "")
        if not text or len(text) < 50:
            return None

        unique_id = raw.get("unique_id", "unknown")
        return {
            "_id": f"ZA-OG-{unique_id}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "jurisdiction": raw.get("jurisdiction", "South Africa"),
            "jurisdiction_code": raw.get("jurisdiction_code", "ZA"),
            "publication_title": raw.get("publication_title"),
            "issue_number": str(raw["issue_number"]) if raw.get("issue_number") is not None else None,
            "volume_number": str(raw["volume_number"]) if raw.get("volume_number") is not None else None,
            "page_count": raw.get("page_count"),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        entries = self._load_index()
        for i, entry in enumerate(entries):
            if i % 100 == 0:
                logger.info(f"Progress: {i}/{len(entries)}")
            self.rate_limiter.wait()
            doc = self._fetch_document(entry)
            if doc:
                yield doc

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        since_dt = datetime.fromisoformat(since)
        entries = self._load_index()
        for entry in entries:
            pub_date = parse_date(entry.get("publication_date"))
            if pub_date:
                try:
                    entry_dt = datetime.fromisoformat(pub_date)
                    if entry_dt < since_dt:
                        continue
                except (ValueError, TypeError):
                    pass
            self.rate_limiter.wait()
            doc = self._fetch_document(entry)
            if doc:
                yield doc


if __name__ == "__main__":
    scraper = OpenGazettesScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing OpenGazettes index...")
        try:
            entries = scraper._load_index()
            logger.info(f"Index has {len(entries)} entries")
            if entries:
                e = entries[0]
                logger.info(f"First: {e.get('issue_title', 'N/A')[:60]}")
            print("Test PASSED")
        except Exception as e:
            print(f"Test FAILED: {e}")
            sys.exit(1)

    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
