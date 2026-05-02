#!/usr/bin/env python3
"""
QA/GTA -- Qatar General Tax Authority

Fetches tax circulars, decisions, and laws from gta.gov.qa. Documents are
published as English-language PDFs across three sections:
  - /en/circulars   — Tax circulars from GTA president
  - /en/decisions   — Emiri, ministerial, and GTA decisions
  - /en/laws        — Income tax, excise tax, and related laws

Full text is extracted from PDFs via common.pdf_extract.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Dict, Any
from urllib.parse import quote, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.QA.GTA")

BASE_URL = "https://gta.gov.qa"
DELAY = 2.0

PAGES = [
    ("/en/circulars", "circular"),
    ("/en/decisions", "decision"),
    ("/en/laws", "law"),
]


def _parse_date(date_str: str) -> Optional[str]:
    """Parse GTA date formats like '22-Jan-2019' to ISO 8601."""
    if not date_str:
        return None
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%d %b %Y", "%d %B %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _make_id(pdf_path: str, doc_type: str) -> str:
    """Generate a stable ID from the PDF filename."""
    name = unquote(pdf_path).split("/")[-1]
    name = re.sub(r"\.pdf$", "", name, flags=re.I)
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name).strip("_")
    if len(name) > 80:
        name = name[:80]
    return f"QA_GTA_{doc_type}_{name}"


class GTAScraper(BaseScraper):
    """Scraper for Qatar General Tax Authority documents."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.http = HttpClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            },
        )

    def _scrape_page(self, path: str, doc_type: str) -> List[Dict[str, Any]]:
        """Scrape a GTA section page for document metadata and PDF links."""
        url = f"{BASE_URL}{path}"
        logger.info("Fetching %s", url)
        resp = self.http.get(url)
        if resp.status_code != 200:
            logger.warning("HTTP %d for %s", resp.status_code, url)
            return []

        html = resp.text
        docs = []
        seen_pdfs = set()

        # Find all PDF links with surrounding context for titles and dates
        for m in re.finditer(r'href="(/assets/pdf/[^"]+\.pdf)"', html, re.I):
            pdf_path = m.group(1)
            if pdf_path in seen_pdfs:
                continue
            seen_pdfs.add(pdf_path)

            # Look backwards for the closest date
            before = html[max(0, m.start() - 2000):m.start()]
            date_matches = re.findall(
                r'class="date-text">([\d]+-\w+-\d{4})</span>', before
            )
            date_str = date_matches[-1] if date_matches else None

            # Look backwards for the closest heading (title)
            title_matches = re.findall(
                r"<h\d[^>]*>(.*?)</h\d>", before, re.DOTALL
            )
            title = ""
            if title_matches:
                title = re.sub(r"<[^>]+>", "", title_matches[-1]).strip()

            # Fallback: use cleaned PDF filename as title
            if not title:
                title = unquote(pdf_path.split("/")[-1])
                title = re.sub(r"\.pdf$", "", title, flags=re.I)

            docs.append({
                "pdf_path": pdf_path,
                "title": title,
                "date": date_str,
                "doc_type": doc_type,
            })

        logger.info("Found %d unique PDFs on %s", len(docs), path)
        return docs

    def _download_and_extract(self, pdf_path: str, doc_id: str) -> Optional[str]:
        """Download a PDF and extract text."""
        # pdf_path may already contain percent-encoded chars (e.g. %E2%80%99)
        # Only encode spaces, not already-encoded sequences
        pdf_url = f"{BASE_URL}{pdf_path.replace(' ', '%20')}"
        try:
            resp = self.http.get(pdf_url, timeout=60)
            if resp.status_code != 200:
                logger.warning("HTTP %d downloading %s", resp.status_code, pdf_url)
                return None
            pdf_bytes = resp.content
            if len(pdf_bytes) < 100:
                logger.warning("PDF too small (%d bytes): %s", len(pdf_bytes), pdf_url)
                return None
            text = extract_pdf_markdown("QA/GTA", doc_id, pdf_bytes=pdf_bytes)
            return text
        except Exception as e:
            logger.warning("Failed to download/extract %s: %s", pdf_url, e)
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all GTA documents with full text from PDFs."""
        all_docs = []
        for path, doc_type in PAGES:
            docs = self._scrape_page(path, doc_type)
            all_docs.extend(docs)
            time.sleep(DELAY)

        logger.info("Total documents to process: %d", len(all_docs))

        for doc in all_docs:
            doc_id = _make_id(doc["pdf_path"], doc["doc_type"])
            logger.info("Processing: %s", doc["title"][:80])

            text = self._download_and_extract(doc["pdf_path"], doc_id)
            if not text or len(text.strip()) < 50:
                logger.warning("Insufficient text for %s, skipping", doc_id)
                continue

            yield {
                "_id": doc_id,
                "title": doc["title"],
                "date": doc["date"],
                "doc_type": doc["doc_type"],
                "pdf_url": f"{BASE_URL}{doc['pdf_path']}",
                "text": text,
            }
            time.sleep(DELAY)

    def fetch_updates(self, since: str = "") -> Generator[dict, None, None]:
        """Fetch updates since a given date. For a small static collection, re-fetch all."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        return {
            "_id": raw["_id"],
            "_source": "QA/GTA",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": _parse_date(raw.get("date")),
            "doc_type": raw.get("doc_type", ""),
            "url": raw.get("pdf_url", ""),
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="QA/GTA bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Run full bootstrap")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--sample-size", type=int, default=12, help="Sample size")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = GTAScraper()

    if args.command == "test":
        for path, doc_type in PAGES:
            docs = scraper._scrape_page(path, doc_type)
            print(f"OK — found {len(docs)} {doc_type} documents on {path}")
        return

    if args.command == "bootstrap":
        sample = args.sample and not args.full
        stats = scraper.bootstrap(sample_mode=sample, sample_size=args.sample_size)
        print(json.dumps(stats, indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
