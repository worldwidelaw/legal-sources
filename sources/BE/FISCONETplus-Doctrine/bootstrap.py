#!/usr/bin/env python3
"""
BE/FISCONETplus-Doctrine -- Belgian Tax Doctrine Fetcher

Fetches tax doctrine from FISCONETplus (FPS Finance / SPF Finances) via their
public REST API. Covers circular letters, advance rulings (prior agreements),
administrative comments, communications, and decisions.

API endpoints (all public, no auth):
  - POST /search          — paginated search with filters
  - GET  /document/{guid} — full document with base64-encoded HTML content

Document types fetched (all are "doctrine"):
  - Circular letters:        ~3,700 docs
  - Prior agreements:       ~15,600 docs
  - Comments:                ~6,300 docs
  - Communications:          ~1,300 docs
  - Decisions:               ~1,500 docs
  Total:                    ~28,400 docs

Full text is returned as base64-encoded HTML in the document endpoint.
HTML is stripped to plain text for the normalized "text" field.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental (newest first)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import base64
import re
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BE.FISCONETplus-Doctrine")

CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"

API_BASE = "https://www.minfin.fgov.be/myminfin-rest/fisconetPlus/public"

# Document type GUIDs for doctrine content
DOCTRINE_TYPES = {
    "184c188f-aa63-4b4a-b703-3a5f07a08869": "Circular letters",
    "d17d212c-c8a3-494d-ac40-367fbf7f8ffa": "Prior agreements",
    "c2d03ba9-fd69-4359-93dd-7e2eb73515b2": "Comments",
    "4c1575f6-716d-4c18-88cd-34ebda7e7ac8": "Communications",
    "e9e4b4d3-2020-4f1b-800c-7559feb574d1": "Decisions",
}

DELAY = 1.0  # seconds between requests


def strip_html(raw_html: str) -> str:
    """Strip HTML tags and decode entities to plain text."""
    # Remove style/script blocks
    text = re.sub(r'<(style|script)[^>]*>.*?</\1>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)
    # Replace block elements with newlines
    text = re.sub(r'<(br|p|div|h[1-6]|li|tr)[^>]*/?>', '\n', text, flags=re.IGNORECASE)
    # Remove all remaining tags
    text = re.sub(r'<[^>]+>', '', text)
    # Decode HTML entities
    text = html_module.unescape(text)
    # Decode numeric entities like &#58;
    text = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), text)
    # Normalize whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class FISCONETplusDoctrine(BaseScraper):
    SOURCE_ID = "BE/FISCONETplus-Doctrine"

    def __init__(self):
        self.http = HttpClient(
            base_url=API_BASE,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )

    def search_documents(
        self,
        doc_type_guid: str,
        language: str = "fr",
        page: int = 0,
        page_size: int = 100,
    ) -> Dict[str, Any]:
        """Search FISCONETplus for documents of a given type."""
        payload = {
            "searchCriteria": {
                "searchTerms": "*",
                "language": language,
                "taxonomies": [],
                "documentTypes": [doc_type_guid],
                "keywords": [],
                "orderBy": "NEWEST",
            },
            "paginationParameters": {
                "currentPageNumber": page,
                "pageSize": page_size,
            },
        }
        resp = self.http.post(f"{API_BASE}/search", json_data=payload)
        time.sleep(DELAY)
        if resp is None or resp.status_code != 200:
            return {"data": {"pageProperties": {"total": 0}, "pageContents": []}}
        return resp.json()

    def fetch_document(self, guid: str) -> Optional[Dict[str, Any]]:
        """Fetch full document content by GUID."""
        resp = self.http.get(f"{API_BASE}/document/{guid}")
        time.sleep(DELAY)
        if resp is None or resp.status_code != 200:
            return None
        data = resp.json()
        return data.get("data", data)

    def decode_content(self, doc_data: Dict[str, Any]) -> str:
        """Decode base64-encoded HTML content and strip to plain text."""
        content = doc_data.get("content", {})
        if not content or not content.get("content"):
            return ""
        raw_b64 = content["content"]
        try:
            html_text = base64.b64decode(raw_b64).decode("utf-8", errors="replace")
            # Remove BOM
            html_text = html_text.lstrip("\ufeff")
            return strip_html(html_text)
        except Exception as e:
            logger.warning("Failed to decode content: %s", e)
            return ""

    def normalize(self, search_item: Dict[str, Any], full_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a document into the standard schema."""
        metadata = full_doc.get("metadata", {})
        text = self.decode_content(full_doc)

        # Extract keywords
        keywords = []
        for kw in metadata.get("keywords", []):
            label = kw.get("label", {})
            # Prefer French, then Dutch, then any
            kw_text = label.get("fr") or label.get("nl") or next(iter(label.values()), "")
            if kw_text:
                keywords.append(kw_text)

        # Document type label
        doc_type_info = metadata.get("documentType", {})
        doc_type_label = doc_type_info.get("label", {})
        doc_type = doc_type_label.get("en") or doc_type_label.get("fr") or "Unknown"

        guid = metadata.get("guid") or search_item.get("guid", "")

        return {
            "_id": guid,
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": metadata.get("title") or search_item.get("title", ""),
            "text": text,
            "date": metadata.get("documentDate") or search_item.get("documentDate"),
            "url": f"https://www.minfin.fgov.be/myminfin-web/pages/public/fisconet/document/{guid}",
            "language": metadata.get("language") or search_item.get("language"),
            "document_type": doc_type,
            "summary": metadata.get("summary") or search_item.get("summary", ""),
            "keywords": keywords,
            "publication_date": metadata.get("publicationDate"),
            "status": metadata.get("status") or search_item.get("status"),
            "guid": guid,
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all doctrine documents from FISCONETplus."""
        total_yielded = 0
        sample_limit = 15 if sample else None

        for doc_type_guid, doc_type_name in DOCTRINE_TYPES.items():
            if sample_limit and total_yielded >= sample_limit:
                break

            logger.info("Fetching %s documents...", doc_type_name)

            # First request to get total count
            result = self.search_documents(doc_type_guid, page=0, page_size=100)
            page_data = result.get("data", {})
            total = page_data.get("pageProperties", {}).get("total", 0)
            max_page = page_data.get("pageProperties", {}).get("maxPageNumber", 0)
            logger.info("  %s: %d documents, %d pages", doc_type_name, total, max_page + 1)

            page = 0
            while True:
                if page > 0:
                    result = self.search_documents(doc_type_guid, page=page, page_size=100)
                    page_data = result.get("data", {})

                items = page_data.get("pageContents", [])
                if not items:
                    break

                for item in items:
                    if sample_limit and total_yielded >= sample_limit:
                        break

                    guid = item.get("guid")
                    if not guid:
                        continue

                    # Fetch full document
                    full_doc = self.fetch_document(guid)
                    if not full_doc:
                        logger.warning("Failed to fetch document %s", guid)
                        continue

                    record = self.normalize(item, full_doc)
                    if not record["text"]:
                        logger.warning("Empty text for %s: %s", guid, record["title"][:80])
                        continue

                    yield record
                    total_yielded += 1

                    if total_yielded % 50 == 0:
                        logger.info("  Progress: %d documents fetched", total_yielded)

                if sample_limit and total_yielded >= sample_limit:
                    break

                page += 1
                if page > max_page:
                    break

            logger.info("  Done with %s. Total so far: %d", doc_type_name, total_yielded)

        logger.info("Fetch complete. Total documents: %d", total_yielded)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch documents modified since a given date (YYYY-MM-DD)."""
        # FISCONETplus orders by NEWEST, so we paginate until we hit older docs
        for doc_type_guid, doc_type_name in DOCTRINE_TYPES.items():
            logger.info("Checking updates for %s since %s...", doc_type_name, since)
            page = 0
            found_older = False

            while not found_older:
                result = self.search_documents(doc_type_guid, page=page, page_size=100)
                page_data = result.get("data", {})
                items = page_data.get("pageContents", [])
                if not items:
                    break

                for item in items:
                    doc_date = item.get("documentDate", "")
                    if doc_date and doc_date < since:
                        found_older = True
                        break

                    guid = item.get("guid")
                    if not guid:
                        continue

                    full_doc = self.fetch_document(guid)
                    if not full_doc:
                        continue

                    record = self.normalize(item, full_doc)
                    if record["text"]:
                        yield record

                page += 1
                max_page = page_data.get("pageProperties", {}).get("maxPageNumber", 0)
                if page > max_page:
                    break

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            result = self.search_documents(
                list(DOCTRINE_TYPES.keys())[0], page=0, page_size=1
            )
            total = result.get("data", {}).get("pageProperties", {}).get("total", 0)
            logger.info("Test passed: %d circular letters available", total)
            return total > 0
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


# === CLI entry point ===

def main():
    import argparse

    parser = argparse.ArgumentParser(description="BE/FISCONETplus-Doctrine bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 10-15 sample records")
    parser.add_argument("--since", type=str, help="Date for incremental update (YYYY-MM-DD)")
    args = parser.parse_args()

    scraper = FISCONETplusDoctrine()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            out_file = sample_dir / f"{record['_id']}.json"
            # Sanitize filename
            safe_name = re.sub(r'[^\w\-.]', '_', record['_id'])
            out_file = sample_dir / f"{safe_name}.json"
            out_file.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
            count += 1
            text_len = len(record.get("text", ""))
            logger.info(
                "  [%d] %s | %s | text=%d chars",
                count, record["date"], record["title"][:60], text_len
            )

        logger.info("Bootstrap complete: %d records saved to sample/", count)
        sys.exit(0 if count >= 10 else 1)

    if args.command == "update":
        since = args.since or "2026-01-01"
        count = 0
        for record in scraper.fetch_updates(since):
            count += 1
            logger.info("  [%d] %s: %s", count, record["date"], record["title"][:60])
        logger.info("Update complete: %d new records since %s", count, since)


if __name__ == "__main__":
    main()
