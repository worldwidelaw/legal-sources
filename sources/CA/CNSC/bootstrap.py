#!/usr/bin/env python3
"""
Canadian Nuclear Safety Commission (CNSC) — Records of Decision fetcher.

The CNSC is Canada's independent quasi-judicial nuclear regulator/tribunal.
Its Records of Decision (reasons on licensing hearings, confidentiality
rulings and other adjudicative matters) are case_law.

Enumeration: the CNSC "Search hearing documents" index page is a Gatsby build
whose full document table is pre-rendered into the HTML. Each row carries
[date, reference, hearing-type, applicant, facility, document-type, <a>title</a>].
Rows whose document-type column is "Decision" are Records of Decision.

Full text: the linked PDFs are served by api.cnsc-ccsn.gc.ca and extracted with
the shared common.pdf_extract backend.

License: Open Government Licence – Canada (Crown Copyright, Government of Canada).

Usage:
  python bootstrap.py test                # verify listing + one PDF download
  python bootstrap.py bootstrap --sample  # fetch 15 sample records
  python bootstrap.py bootstrap           # full run
"""

import hashlib
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

SOURCE_DIR = Path(__file__).parent
sys.path.insert(0, str(SOURCE_DIR.parent.parent.parent))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CA.CNSC")

BASE_URL = "https://www.cnsc-ccsn.gc.ca"
LISTING_PATH = "/eng/the-commission/hearings-meetings/search-hearing-documents/"


def _strip_tags(html: str) -> str:
    text = re.sub(r"<[^>]+>", "", html)
    text = (
        text.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", '"')
        .replace("&#39;", "'")
        .replace("&nbsp;", " ")
        .replace("&ndash;", "–")
    )
    return re.sub(r"\s+", " ", text).strip()


def _doc_id(pdf_url: str) -> str:
    filename = pdf_url.rstrip("/").split("/")[-1]
    return hashlib.md5(filename.encode("utf-8")).hexdigest()[:16]


class CNSCScraper(BaseScraper):
    """Scraper for CA/CNSC Records of Decision."""

    def __init__(self):
        super().__init__(str(SOURCE_DIR))
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            },
        )

    def _load_listing(self) -> list[dict]:
        """Parse the pre-rendered decision table from the search index page."""
        resp = self.client.get(LISTING_PATH)
        resp.raise_for_status()
        # The server does not always declare charset; the page is UTF-8.
        html = resp.content.decode("utf-8", errors="replace")

        docs: list[dict] = []
        seen: set[str] = set()
        for row in re.findall(r"<tr>(.*?)</tr>", html, re.S):
            cells = re.findall(r"<td>(.*?)</td>", row, re.S)
            if len(cells) < 7:
                continue
            doc_type = _strip_tags(cells[5])
            if doc_type != "Decision":
                continue
            link = re.search(r'href="([^"]+)"[^>]*>(.*?)</a>', cells[6], re.S)
            if not link:
                continue
            pdf_url = link.group(1).strip()
            if not pdf_url.lower().endswith("/object") and ".pdf" not in pdf_url.lower():
                continue
            if pdf_url in seen:
                continue
            seen.add(pdf_url)

            date = _strip_tags(cells[0])
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
                date = None
            docs.append(
                {
                    "doc_id": _doc_id(pdf_url),
                    "date": date,
                    "reference": _strip_tags(cells[1]),
                    "hearing_type": _strip_tags(cells[2]),
                    "applicant": _strip_tags(cells[3]),
                    "facility": _strip_tags(cells[4]),
                    "title": _strip_tags(link.group(2)),
                    "pdf_url": pdf_url,
                }
            )
        logger.info("Parsed %d Record-of-Decision entries from listing", len(docs))
        return docs

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"CA/CNSC/{raw['doc_id']}",
            "_source": "CA/CNSC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("_prefetched_text", ""),
            "date": raw.get("date"),
            "url": raw.get("pdf_url", ""),
            "doc_id": raw["doc_id"],
            "reference": raw.get("reference", ""),
            "applicant": raw.get("applicant", ""),
            "facility": raw.get("facility", ""),
            "hearing_type": raw.get("hearing_type", ""),
            "pdf_url": raw.get("pdf_url", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        all_docs = self._load_listing()
        limit = 15 if sample else None
        count = 0

        for doc in all_docs:
            if limit and count >= limit:
                break
            try:
                self.rate_limiter.wait()
                resp = self.client.get(doc["pdf_url"])
                resp.raise_for_status()
                pdf_bytes = resp.content
            except Exception as e:
                logger.warning("  Failed to download %s: %s", doc["title"][:60], e)
                continue

            if not pdf_bytes or len(pdf_bytes) < 100:
                logger.warning("  Tiny/empty PDF for %s", doc["title"][:60])
                continue

            text = (
                extract_pdf_markdown(
                    source="CA/CNSC",
                    source_id=doc["doc_id"],
                    pdf_bytes=pdf_bytes,
                    table="case_law",
                )
                or ""
            )
            if not text or len(text) < 200:
                logger.warning(
                    "  Skipping %s — no/short text (%d chars)", doc["title"][:60], len(text)
                )
                continue

            doc["_prefetched_text"] = text
            yield doc
            count += 1
            logger.info("  [%d] %s (%d chars)", count, doc["title"][:60], len(text))

        logger.info("Total records yielded: %d", count)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for doc in self.fetch_all():
            yield doc


if __name__ == "__main__":
    scraper = CNSCScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        docs = scraper._load_listing()
        if not docs:
            print("FAILED - no decisions found")
            sys.exit(1)
        print(f"Loaded {len(docs)} Records of Decision.")
        test_doc = docs[0]
        print(f"  Testing download: {test_doc['title'][:60]}...")
        resp = scraper.client.get(test_doc["pdf_url"])
        resp.raise_for_status()
        print(f"  Download OK: {len(resp.content)} bytes")
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
