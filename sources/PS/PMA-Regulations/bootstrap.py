#!/usr/bin/env python3
"""
PS/PMA-Regulations — Palestine Monetary Authority Regulatory Documents

Fetches regulatory documents (laws, circulars, instructions, regulations) from
the PMA's Supabase backend. Each document is a PDF stored in Supabase storage.

Strategy:
  1. Query Supabase REST API for documents table (paginated, 1000/page)
  2. Filter for regulatory categories (laws, circulars, instructions, regulations)
  3. Download PDFs from Supabase storage and extract text
  4. Normalize into standard schema

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py test-api            # Quick connectivity test
"""

import io
import sys
import json
import logging
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PS.PMA-Regulations")

SUPABASE_URL = "https://fayupjvyvxedvgathafk.supabase.co"
ANON_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZheXVwanZ5dnhlZHZnYXRoYWZrIiwi"
    "cm9sZSI6ImFub24iLCJpYXQiOjE3NjU0MjIxMjAsImV4cCI6MjA4MDk5ODEyMH0."
    "Ksw0jgcfUIAuepOfrUkH1IB5Q44eFUQprNR_Gq7feg8"
)
REST_URL = f"{SUPABASE_URL}/rest/v1/documents"
SOURCE_ID = "PS/PMA-Regulations"
REGULATORY_CATEGORIES = {"laws", "circulars", "instructions", "regulations"}
PAGE_SIZE = 1000


def _extract_text_pdfplumber(pdf_bytes: bytes) -> str:
    import pdfplumber
    parts = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                parts.append(t)
            try:
                page.flush_cache(); page.get_textmap.cache_clear()
            except Exception:
                pass
    return "\n\n".join(parts)


def _extract_text_pymupdf(pdf_bytes: bytes) -> str:
    import fitz
    parts = []
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    for page in doc:
        t = page.get_text()
        if t:
            parts.append(t)
    doc.close()
    return "\n\n".join(parts)


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    text = ""
    try:
        text = _extract_text_pdfplumber(pdf_bytes)
    except Exception as e:
        logger.debug(f"pdfplumber failed: {e}")
    if not text.strip():
        try:
            text = _extract_text_pymupdf(pdf_bytes)
        except Exception as e:
            logger.warning(f"Both PDF extractors failed: {e}")
    return text.strip()


class PMARegulationsScraper(BaseScraper):
    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(max_retries=3, timeout=60)
        self.headers = {
            "apikey": ANON_KEY,
            "Authorization": f"Bearer {ANON_KEY}",
        }

    def test_api(self) -> bool:
        try:
            resp = self.http.get(
                REST_URL,
                params={"select": "id,category", "limit": "1"},
                headers=self.headers,
            )
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and len(data) > 0:
                    logger.info("API test passed — Supabase documents table accessible")
                    return True
            logger.error(f"API test failed — status {resp.status_code}")
            return False
        except Exception as e:
            logger.error(f"API test failed: {e}")
            return False

    def _fetch_all_metadata(self, categories: set = None) -> list[dict]:
        if categories is None:
            categories = REGULATORY_CATEGORIES
        all_docs = []
        offset = 0
        while True:
            cat_filter = ",".join(categories)
            params = {
                "select": "id,title_ar,title_en,category,sub_category,file_url,file_name,document_date,created_at,file_size",
                "category": f"in.({cat_filter})",
                "limit": str(PAGE_SIZE),
                "offset": str(offset),
                "order": "created_at.asc",
            }
            logger.info(f"Fetching metadata offset={offset}...")
            try:
                resp = self.http.get(REST_URL, params=params, headers=self.headers)
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"Failed to fetch offset {offset}: {e}")
                break
            batch = resp.json()
            if not batch:
                break
            all_docs.extend(batch)
            logger.info(f"  Got {len(batch)} records (total: {len(all_docs)})")
            if len(batch) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
            time.sleep(0.5)
        logger.info(f"Total metadata records: {len(all_docs)}")
        return all_docs

    def _download_and_extract(self, file_url: str) -> Optional[str]:
        if not file_url:
            return None
        try:
            resp = self.http.get(file_url, timeout=90)
            resp.raise_for_status()
            if len(resp.content) < 100:
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {file_url}")
                return None
            text = extract_text_from_pdf(resp.content)
            if not text or len(text) < 50:
                logger.warning(f"Insufficient text from {file_url}")
                return None
            return text
        except Exception as e:
            logger.warning(f"Failed to download/extract {file_url}: {e}")
            return None

    def normalize(self, raw: dict) -> dict:
        title = raw.get("title_ar") or raw.get("title_en") or raw.get("file_name") or "Untitled"
        doc_id = raw.get("id") or hashlib.sha256(title.encode()).hexdigest()[:16]
        return {
            "_id": str(doc_id),
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": raw.get("document_date"),
            "url": raw.get("file_url", ""),
            "category": raw.get("category", ""),
            "sub_category": raw.get("sub_category", ""),
            "language": "ar",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        metadata_list = self._fetch_all_metadata()
        for i, meta in enumerate(metadata_list):
            title = meta.get("title_ar") or meta.get("file_name") or "Untitled"
            logger.info(f"[{i+1}/{len(metadata_list)}] Downloading: {title[:80]}")
            text = self._download_and_extract(meta.get("file_url"))
            if not text:
                logger.warning(f"Skipping (no text): {title[:80]}")
                continue
            meta["text"] = text
            yield self.normalize(meta)
            time.sleep(0.5)

    def fetch_updates(self, since: str = None) -> Generator[dict, None, None]:
        yield from self.fetch_all()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="PS/PMA-Regulations bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap")
    args = parser.parse_args()

    scraper = PMARegulationsScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        limit = 15 if args.sample else None

        for record in scraper.fetch_all():
            count += 1
            text_len = len(record.get("text", ""))
            logger.info(
                f"  #{count} | {record['title'][:60]} | "
                f"text={text_len} chars | cat={record.get('category', 'N/A')}"
            )
            if args.sample or count <= 15:
                fname = f"{record['_id'][:16]}.json"
                with open(sample_dir / fname, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            if limit and count >= limit:
                logger.info(f"Sample limit reached ({limit} records)")
                break

        logger.info(f"Done. {count} records fetched.")
        print(json.dumps({"_source": SOURCE_ID, "records": count}))


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
