#!/usr/bin/env python3
"""
RW/RRA -- Rwanda Revenue Authority Tax Laws and Rulings

Fetches tax laws, presidential orders, ministerial orders, commissioner
general's rules, public rulings, and VAT exemption lists from rra.gov.rw.
Documents are published as trilingual (Kinyarwanda/English/French) PDFs
on the laws-policies-and-rulings page.

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
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.RW.RRA")

BASE_URL = "https://www.rra.gov.rw"
PAGE_URL = "/en/laws-policies-and-rulings"
DELAY = 2.0


def _make_id(pdf_path: str) -> str:
    """Generate a stable ID from the PDF filename."""
    name = unquote(pdf_path).split("/")[-1]
    name = re.sub(r"\.pdf$", "", name, flags=re.I)
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name).strip("_")
    if len(name) > 80:
        name = name[:80]
    return f"RW_RRA_{name}"


def _clean_title(filename: str) -> str:
    """Generate a readable title from the PDF filename."""
    name = unquote(filename)
    name = re.sub(r"\.pdf$", "", name, flags=re.I)
    name = re.sub(r"[-_]+", " ", name)
    return name.strip()


def _guess_doc_type(pdf_path: str, title: str) -> str:
    """Classify the document type from its path/title."""
    combined = (pdf_path + " " + title).lower()
    if "law" in combined or "itegeko" in combined or "loi" in combined:
        return "law"
    if "presidential" in combined or "order" in combined:
        return "presidential_order"
    if "ministerial" in combined:
        return "ministerial_order"
    if "ruling" in combined or "commissioner" in combined:
        return "ruling"
    if "exemption" in combined or "exempt" in combined:
        return "exemption_list"
    return "guidance"


def _extract_date_from_title(title: str) -> Optional[str]:
    """Try to extract a year from the title."""
    m = re.search(r'(\b20[12]\d)\b', title)
    if m:
        return f"{m.group(1)}-01-01"
    return None


class RRAScraper(BaseScraper):
    """Scraper for Rwanda Revenue Authority tax documents."""

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

    def _scrape_documents(self) -> List[Dict[str, Any]]:
        """Scrape the laws-policies-and-rulings page for PDF links."""
        url = f"{BASE_URL}{PAGE_URL}"
        logger.info("Fetching %s", url)
        resp = self.http.get(url)
        if resp.status_code != 200:
            logger.warning("HTTP %d for %s", resp.status_code, url)
            return []

        html = resp.text
        docs = []
        seen_pdfs = set()

        for m in re.finditer(r'href="(/fileadmin/[^"]+\.pdf)"', html, re.I):
            pdf_path = m.group(1)
            if pdf_path in seen_pdfs:
                continue
            seen_pdfs.add(pdf_path)

            filename = unquote(pdf_path.split("/")[-1])
            title = _clean_title(filename)
            doc_type = _guess_doc_type(pdf_path, title)
            date = _extract_date_from_title(title)

            docs.append({
                "pdf_path": pdf_path,
                "title": title,
                "date": date,
                "doc_type": doc_type,
            })

        logger.info("Found %d unique PDFs", len(docs))
        return docs

    def _download_and_extract(self, pdf_path: str, doc_id: str) -> Optional[str]:
        """Download a PDF and extract text."""
        pdf_url = f"{BASE_URL}{pdf_path}"
        try:
            resp = self.http.get(pdf_url, timeout=60)
            if resp.status_code != 200:
                logger.warning("HTTP %d downloading %s", resp.status_code, pdf_url)
                return None
            pdf_bytes = resp.content
            if len(pdf_bytes) < 100:
                logger.warning("PDF too small (%d bytes): %s", len(pdf_bytes), pdf_url)
                return None
            text = extract_pdf_markdown("RW/RRA", doc_id, pdf_bytes=pdf_bytes)
            return text
        except Exception as e:
            logger.warning("Failed to download/extract %s: %s", pdf_url, e)
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all RRA documents with full text from PDFs."""
        all_docs = self._scrape_documents()
        logger.info("Total documents to process: %d", len(all_docs))

        for doc in all_docs:
            doc_id = _make_id(doc["pdf_path"])
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
        """Fetch updates — for a small static collection, re-fetch all."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        return {
            "_id": raw["_id"],
            "_source": "RW/RRA",
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

    parser = argparse.ArgumentParser(description="RW/RRA bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Run full bootstrap")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--sample-size", type=int, default=15, help="Sample size")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = RRAScraper()

    if args.command == "test":
        docs = scraper._scrape_documents()
        print(f"OK — found {len(docs)} documents")
        types = {}
        for d in docs:
            t = d["doc_type"]
            types[t] = types.get(t, 0) + 1
        for t, c in sorted(types.items()):
            print(f"  {t}: {c}")
        return

    if args.command == "bootstrap":
        sample = args.sample and not args.full
        stats = scraper.bootstrap(sample_mode=sample, sample_size=args.sample_size)
        print(json.dumps(stats, indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
