#!/usr/bin/env python3
"""
South Africa Open Gazettes Data Fetcher

Fetches ~42,254 South African government gazette documents from opengazettes.org.za.
Uses JSONL index for metadata + PyMuPDF for PDF text extraction.

Index: archive.opengazettes.org.za/index/gazette-index-latest.jsonlines
PDFs: archive.opengazettes.org.za/archive/{jurisdiction}/{year}/{filename}.pdf
"""

import json
import logging
import os
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

INDEX_URL = "https://archive.opengazettes.org.za/index/gazette-index-latest.jsonlines"
DELAY = 1.5
MAX_PDF_PAGES = 500
MAX_PDF_BYTES = 50 * 1024 * 1024  # 50MB limit per PDF
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) LegalDataHunter/1.0"}


def http_get_bytes(url: str, timeout: int = 60, max_bytes: int = MAX_PDF_BYTES) -> Optional[bytes]:
    """Fetch URL and return raw bytes."""
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            # Check content-length if available
            cl = resp.headers.get("Content-Length")
            if cl and int(cl) > max_bytes:
                logger.warning(f"PDF too large ({int(cl)} bytes), skipping: {url[:100]}")
                return None
            data = resp.read(max_bytes + 1)
            if len(data) > max_bytes:
                logger.warning(f"PDF exceeded {max_bytes} bytes, skipping: {url[:100]}")
                return None
            return data
    except Exception as e:
        logger.warning(f"HTTP GET failed for {url[:120]}: {e}")
        return None


def extract_text_from_pdf(pdf_bytes: bytes) -> Optional[str]:
    """Extract text from PDF bytes using PyMuPDF."""
    if fitz is None:
        logger.error("PyMuPDF (fitz) not available")
        return None
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        if doc.page_count > MAX_PDF_PAGES:
            logger.warning(f"PDF has {doc.page_count} pages (>{MAX_PDF_PAGES}), skipping")
            doc.close()
            return None
        text_parts = []
        for page in doc:
            text_parts.append(page.get_text())
        doc.close()
        text = "\n".join(text_parts).strip()
        # Clean up common artifacts
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text if len(text) > 50 else None
    except Exception as e:
        logger.warning(f"PDF extraction failed: {e}")
        return None


def parse_date(date_str: Optional[str]) -> Optional[str]:
    """Parse date to ISO 8601."""
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d", "%d %B %Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Try extracting from issue_title patterns like "dated 08 September 2015"
    m = re.search(r"dated\s+(\d{1,2}\s+\w+\s+\d{4})", date_str)
    if m:
        try:
            return datetime.strptime(m.group(1), "%d %B %Y").strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


class OpenGazettesFetcher:
    """Fetcher for South African government gazettes."""

    def __init__(self):
        self.delay = DELAY
        self._index = None

    def load_index(self) -> List[Dict[str, Any]]:
        """Download and parse the JSONL index."""
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

    def fetch_document(self, entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch and extract text from a single gazette entry."""
        archive_url = entry.get("archive_url")
        if not archive_url:
            return None

        unique_id = entry.get("unique_id", "")
        if not unique_id:
            # Generate from archive path
            unique_id = entry.get("archive_path", "").replace("/", "-").replace(".pdf", "")

        # Download PDF
        pdf_bytes = http_get_bytes(archive_url)
        if not pdf_bytes:
            return None

        # Extract text
        text = extract_text_from_pdf(pdf_bytes)
        if not text:
            logger.warning(f"No text extracted from {unique_id}")
            return None

        pub_date = parse_date(entry.get("publication_date"))
        if not pub_date:
            # Try extracting from issue_title
            pub_date = parse_date(entry.get("issue_title", ""))

        title = entry.get("issue_title") or entry.get("publication_title", "")
        jurisdiction = entry.get("jurisdiction_name", "South Africa")
        jurisdiction_code = entry.get("jurisdiction_code", "ZA")

        return {
            "_id": f"ZA-OG-{unique_id}",
            "_source": "ZA/OpenGazettes",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": pub_date,
            "url": archive_url,
            "jurisdiction": jurisdiction,
            "jurisdiction_code": jurisdiction_code,
            "publication_title": entry.get("publication_title"),
            "publication_subtitle": entry.get("publication_subtitle"),
            "issue_number": entry.get("issue_number"),
            "volume_number": entry.get("volume_number"),
            "page_count": entry.get("pagecount"),
            "special_issue": entry.get("special_issue"),
            "language_edition": entry.get("language_edition"),
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Already normalized during fetch."""
        return raw

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Yield all gazette documents."""
        entries = self.load_index()
        for i, entry in enumerate(entries):
            if i % 100 == 0:
                logger.info(f"Progress: {i}/{len(entries)}")
            doc = self.fetch_document(entry)
            if doc:
                yield doc
            time.sleep(self.delay)

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        """Fetch gazettes published since a date."""
        since_dt = datetime.fromisoformat(since)
        entries = self.load_index()
        for entry in entries:
            pub_date = parse_date(entry.get("publication_date"))
            if pub_date:
                try:
                    entry_dt = datetime.fromisoformat(pub_date)
                    if entry_dt < since_dt:
                        continue
                except (ValueError, TypeError):
                    pass
            doc = self.fetch_document(entry)
            if doc:
                yield doc
            time.sleep(self.delay)


def bootstrap_sample(sample_dir: Path, count: int = 15):
    """Fetch sample gazette documents."""
    if fitz is None:
        logger.error("PyMuPDF (fitz) is required for PDF text extraction. Install with: pip install PyMuPDF")
        sys.exit(1)

    sample_dir.mkdir(parents=True, exist_ok=True)
    fetcher = OpenGazettesFetcher()

    entries = fetcher.load_index()
    logger.info(f"Total gazette entries: {len(entries)}")

    # Sample from different jurisdictions and small page counts for speed
    small_entries = [e for e in entries if e.get("pagecount") and int(e.get("pagecount", 999)) <= 20]
    logger.info(f"Entries with <=20 pages: {len(small_entries)}")

    # Pick diverse samples
    import random
    random.seed(42)
    if len(small_entries) > count * 3:
        candidates = random.sample(small_entries, count * 3)
    else:
        candidates = small_entries[:count * 3]

    saved = 0
    for entry in candidates:
        if saved >= count:
            break

        uid = entry.get("unique_id", "?")
        logger.info(f"Fetching gazette {uid} ({entry.get('pagecount', '?')} pages)")

        doc = fetcher.fetch_document(entry)
        if not doc:
            continue

        text_len = len(doc.get("text", ""))
        logger.info(f"  Title: {doc.get('title', 'N/A')[:80]}")
        logger.info(f"  Text: {text_len} chars, Jurisdiction: {doc.get('jurisdiction')}")

        out_file = sample_dir / f"{doc['_id']}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)

        saved += 1
        logger.info(f"  Saved ({saved}/{count})")
        time.sleep(fetcher.delay)

    logger.info(f"Bootstrap complete: {saved} documents saved to {sample_dir}")
    return saved


if __name__ == "__main__":
    source_dir = Path(__file__).parent
    sample_dir = source_dir / "sample"

    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap":
        sample_flag = "--sample" in sys.argv
        count = 15 if sample_flag else 50
        saved = bootstrap_sample(sample_dir, count)
        if saved < 10:
            logger.error(f"Only {saved} documents saved, expected at least 10")
            sys.exit(1)
    else:
        print("Usage: python3 bootstrap.py bootstrap [--sample]")
        print("  bootstrap --sample  Fetch 15 sample documents")
        print("  bootstrap           Fetch 50 sample documents")
