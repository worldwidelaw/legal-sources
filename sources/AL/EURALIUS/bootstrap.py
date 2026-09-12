#!/usr/bin/env python3
"""
EURALIUS Albanian Legislation (English Translations)

EU-funded English translations of key Albanian codes and laws.
Published via jDownloads on euralius.eu.

Structure:
  /index.php/en/library/albanian-legislation/category/360-laws → 54 categories
  Each category → list of PDF downloads via task=download.send&id=X&catid=Y

Small corpus (~100-150 documents) but high value: official English translations
of Albanian Constitution, codes, and key legislation.
"""

import json
import logging
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests

# PDF extraction
PDF_AVAILABLE = False
try:
    import pdfplumber
    PDF_AVAILABLE = True
except ImportError:
    try:
        from PyPDF2 import PdfReader
        PDF_AVAILABLE = True
    except ImportError:
        pass

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://euralius.eu"
LAWS_URL = f"{BASE_URL}/index.php/en/library/albanian-legislation/category/360-laws"
SOURCE_ID = "AL/EURALIUS"
RATE_LIMIT = 2.0


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using available library."""
    try:
        import pdfplumber
        import io
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            return "\n\n".join(pages)
    except Exception:
        pass

    try:
        from PyPDF2 import PdfReader
        import io
        reader = PdfReader(io.BytesIO(pdf_bytes))
        pages = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                pages.append(text)
            try:
                page.flush_cache(); page.get_textmap.cache_clear()
            except Exception:
                pass
        return "\n\n".join(pages)
    except Exception:
        pass

    return ""


class EuraliusFetcher:
    """Fetcher for EURALIUS Albanian legislation translations."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (LegalDataHunter/1.0; open-data-research)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        self._last_request = 0.0

    def _rate_limit(self):
        elapsed = time.time() - self._last_request
        if elapsed < RATE_LIMIT:
            time.sleep(RATE_LIMIT - elapsed)
        self._last_request = time.time()

    def _get(self, url: str) -> str:
        self._rate_limit()
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text

    def _get_bytes(self, url: str) -> bytes:
        self._rate_limit()
        resp = self.session.get(url, timeout=60)
        resp.raise_for_status()
        return resp.content

    def _discover_categories(self) -> List[Tuple[str, str]]:
        """Discover all law categories from the main laws page."""
        html = self._get(LAWS_URL)
        pattern = r'href="(/index\.php/en/library/albanian-legislation/category/[^"]+)"[^>]*>([^<]+)</a>'
        cats = re.findall(pattern, html)
        result = []
        seen = set()
        for url_path, name in cats:
            name = unescape(name).strip()
            if url_path not in seen and name:
                seen.add(url_path)
                result.append((f"{BASE_URL}{url_path}", name))
        logger.info(f"Found {len(result)} categories")
        return result

    def _discover_downloads(self, cat_url: str, cat_name: str) -> List[Dict[str, str]]:
        """Discover downloadable documents in a category."""
        html = self._get(cat_url)
        downloads = []

        # Find download links with titles
        # Pattern: <a ... class="jd_download_url">TITLE</a> with download.send link
        pattern = r'href="(/index\.php/en/library/albanian-legislation\?task=download\.send&amp;id=(\d+)&amp;catid=(\d+)[^"]*)"[^>]*class="jd_download_url"[^>]*>([^<]+)</a>'
        matches = re.findall(pattern, html)

        seen_ids = set()
        for url_path, doc_id, cat_id, title in matches:
            if doc_id in seen_ids:
                continue
            seen_ids.add(doc_id)
            clean_url = unescape(url_path)
            downloads.append({
                "url": f"{BASE_URL}{clean_url}",
                "id": doc_id,
                "cat_id": cat_id,
                "title": unescape(title).strip(),
                "category": cat_name,
            })

        return downloads

    def fetch_all(self, limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        """Yield raw documents with downloaded PDF text."""
        count = 0
        categories = self._discover_categories()

        for cat_url, cat_name in categories:
            downloads = self._discover_downloads(cat_url, cat_name)
            logger.info(f"Category '{cat_name}': {len(downloads)} downloads")

            for dl in downloads:
                if limit and count >= limit:
                    return

                try:
                    pdf_bytes = self._get_bytes(dl["url"])
                except Exception as e:
                    logger.warning(f"Failed to download {dl['title']}: {e}")
                    continue

                if len(pdf_bytes) < 100:
                    logger.warning(f"Tiny file for {dl['title']} ({len(pdf_bytes)} bytes), skipping")
                    continue

                text = extract_text_from_pdf(pdf_bytes)
                if not text or len(text) < 100:
                    logger.warning(f"No text extracted from {dl['title']}")
                    continue

                dl["text"] = text
                dl["file_size"] = len(pdf_bytes)
                count += 1
                yield dl

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw document into standard schema."""
        title = raw.get("title", "")
        category = raw.get("category", "")
        doc_id = raw.get("id", "")

        return {
            "_id": f"AL-EURALIUS-{doc_id}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": None,
            "url": raw.get("url", ""),
            "category": category,
            "language": "en",
            "file_size": raw.get("file_size", 0),
        }


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap":
        if not PDF_AVAILABLE:
            logger.error("No PDF library available (pdfplumber or PyPDF2). Install one.")
            sys.exit(1)

        fetcher = EuraliusFetcher()
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap of AL/EURALIUS...")

        sample_count = 0
        target = 15 if "--sample" in sys.argv else 50

        for raw_doc in fetcher.fetch_all(limit=target):
            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get("text", ""))

            doc_id = normalized["_id"]
            filepath = sample_dir / f"{doc_id}.json"

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            sample_count += 1
            logger.info(
                f"Saved [{sample_count}/{target}]: {normalized.get('title', doc_id)[:60]} "
                f"({text_len:,} chars)"
            )

        logger.info(f"Bootstrap complete. {sample_count} documents saved to {sample_dir}")

        files = list(sample_dir.glob("*.json"))
        total_chars = 0
        for fp in files:
            with open(fp, encoding="utf-8") as f:
                doc = json.load(f)
            total_chars += len(doc.get("text", ""))

        logger.info(f"Summary: {len(files)} files, {total_chars:,} total text chars")
        if files:
            logger.info(f"Average: {total_chars // len(files):,} chars/document")

    else:
        print("Usage: python bootstrap.py bootstrap [--sample]")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
