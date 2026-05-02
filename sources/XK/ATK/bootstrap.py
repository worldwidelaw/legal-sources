#!/usr/bin/env python3
"""
XK/ATK -- Kosovo Tax Administration (Administrata Tatimore e Kosovës)

Fetches tax guidelines, administrative instructions, laws, and informative
materials from atk-ks.org. Documents are published as English-language PDFs
across three sections:
  - /en/udhezues-manuale-dhe-rregullore/  — Guidelines, manuals, regulations
  - /en/legislation/laws/                  — Tax laws
  - /en/materiale-informative/             — Informative materials

Full text is extracted from PDFs via common.pdf_extract.

Note: atk-ks.org has an untrusted SSL certificate; requests use verify=False.

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
import urllib3
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Dict, Any
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

# Suppress InsecureRequestWarning for atk-ks.org SSL issue
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.XK.ATK")

BASE_URL = "https://www.atk-ks.org"
DELAY = 2.0

PAGES = [
    ("/en/udhezues-manuale-dhe-rregullore/", "guideline"),
    ("/en/legislation/laws/", "law"),
    ("/en/materiale-informative/", "informative"),
]


def _make_id(pdf_url: str) -> str:
    """Generate a stable ID from the PDF filename."""
    name = unquote(pdf_url).split("/")[-1]
    name = re.sub(r"\.pdf$", "", name, flags=re.I)
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name).strip("_")
    if len(name) > 80:
        name = name[:80]
    return f"XK_ATK_{name}"


def _extract_date_from_path(pdf_url: str) -> Optional[str]:
    """Try to extract a date from the wp-content/uploads path (YYYY/MM pattern)."""
    m = re.search(r"/uploads/(\d{4})/(\d{2})/", pdf_url)
    if m:
        return f"{m.group(1)}-{m.group(2)}-01"
    return None


def _clean_title(filename: str) -> str:
    """Generate a readable title from the PDF filename."""
    name = unquote(filename)
    name = re.sub(r"\.pdf$", "", name, flags=re.I)
    # Remove common prefixes
    name = re.sub(r"^(ENG-|ANGL-)", "", name)
    # Replace separators with spaces
    name = re.sub(r"[-_]+", " ", name)
    return name.strip()


class ATKScraper(BaseScraper):
    """Scraper for Kosovo Tax Administration documents."""

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
        """Scrape an ATK section page for PDF links."""
        url = f"{BASE_URL}{path}"
        logger.info("Fetching %s", url)
        resp = self.http.get(url, verify=False)
        if resp.status_code != 200:
            logger.warning("HTTP %d for %s", resp.status_code, url)
            return []

        html = resp.text
        docs = []
        seen_pdfs = set()

        for m in re.finditer(
            r'href="(https://www\.atk-ks\.org/wp-content/uploads/[^"]+\.pdf)"',
            html, re.I,
        ):
            pdf_url = m.group(1)
            if pdf_url in seen_pdfs:
                continue
            seen_pdfs.add(pdf_url)

            filename = pdf_url.split("/")[-1]
            title = _clean_title(filename)
            date = _extract_date_from_path(pdf_url)

            docs.append({
                "pdf_url": pdf_url,
                "title": title,
                "date": date,
                "doc_type": doc_type,
            })

        logger.info("Found %d unique PDFs on %s", len(docs), path)
        return docs

    def _download_and_extract(self, pdf_url: str, doc_id: str) -> Optional[str]:
        """Download a PDF and extract text."""
        try:
            resp = self.http.get(pdf_url, verify=False, timeout=60)
            if resp.status_code != 200:
                logger.warning("HTTP %d downloading %s", resp.status_code, pdf_url)
                return None
            pdf_bytes = resp.content
            if len(pdf_bytes) < 100:
                logger.warning("PDF too small (%d bytes): %s", len(pdf_bytes), pdf_url)
                return None
            text = extract_pdf_markdown("XK/ATK", doc_id, pdf_bytes=pdf_bytes)
            return text
        except Exception as e:
            logger.warning("Failed to download/extract %s: %s", pdf_url, e)
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all ATK documents with full text from PDFs."""
        all_docs = []
        for path, doc_type in PAGES:
            docs = self._scrape_page(path, doc_type)
            all_docs.extend(docs)
            time.sleep(DELAY)

        logger.info("Total documents to process: %d", len(all_docs))

        for doc in all_docs:
            doc_id = _make_id(doc["pdf_url"])
            logger.info("Processing: %s", doc["title"][:80])

            text = self._download_and_extract(doc["pdf_url"], doc_id)
            if not text or len(text.strip()) < 50:
                logger.warning("Insufficient text for %s, skipping", doc_id)
                continue

            yield {
                "_id": doc_id,
                "title": doc["title"],
                "date": doc["date"],
                "doc_type": doc["doc_type"],
                "pdf_url": doc["pdf_url"],
                "text": text,
            }
            time.sleep(DELAY)

    def fetch_updates(self, since: str = "") -> Generator[dict, None, None]:
        """Fetch updates — for a small static collection, re-fetch all."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        return {
            "_id": raw["_id"],
            "_source": "XK/ATK",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "doc_type": raw.get("doc_type", ""),
            "url": raw.get("pdf_url", ""),
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="XK/ATK bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Run full bootstrap")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--sample-size", type=int, default=12, help="Sample size")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = ATKScraper()

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
