#!/usr/bin/env python3
"""
CA/YukonLaws -- Yukon Consolidated Acts & Regulations

Fetches consolidated Yukon legislation PDFs from laws.yukon.ca.
The site exposes Apache directory listings under /cms/images/LEGISLATION/
with two main collections:
  - PRINCIPAL/ (Acts)
  - SUBORDINATE/ (Regulations)

Each document folder may contain multiple versioned PDFs (e.g., 2002-0015_1.pdf,
2002-0015_2.pdf). The highest-numbered version is the current consolidated text.

Titles are scraped from the legislation-by-title index page. Documents without
a matched title use the doc ID as a fallback.

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch 12+ sample records
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import re
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CA.YukonLaws")

BASE_URL = "https://laws.yukon.ca"
LEGISLATION_BASE = "/cms/images/LEGISLATION"


def _parse_dir_listing(html: str) -> List[str]:
    """Extract directory/file names from an Apache directory listing."""
    entries = re.findall(r'href="([^"]+)"', html)
    # Filter out parent directory and absolute paths
    return [e.rstrip("/") for e in entries if not e.startswith("/") and e != "../"]


class YukonLawsScraper(BaseScraper):
    """Scraper for CA/YukonLaws -- Yukon Consolidated Laws."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/pdf,*/*",
            },
            timeout=120,
        )
        self._title_cache: Optional[Dict[str, str]] = None

    def _load_titles(self) -> Dict[str, str]:
        """Scrape titles from legislation-by-title page for all letters.

        Returns a dict mapping doc_id (e.g., '2002-0015') to title string.
        """
        if self._title_cache is not None:
            return self._title_cache

        titles: Dict[str, str] = {}
        logger.info("Loading titles from legislation-by-title index...")

        for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
            self.rate_limiter.wait()
            try:
                resp = self.client.post(
                    "/cms/legislation-by-title.html",
                    data={"submit4": letter, "pointintime_post_alpha": ""},
                )
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"Failed to fetch titles for letter {letter}: {e}")
                continue

            html = resp.text

            # Acts: row0 class links to PRINCIPAL PDFs
            for m in re.finditer(
                r'<a class="row0"[^>]*href="/cms/images/LEGISLATION/'
                r'(?:PRINCIPAL|SUBORDINATE)/\d{4}/([^/]+)/[^"]+\.pdf"[^>]*>'
                r"(.*?)</a>",
                html,
                re.DOTALL,
            ):
                doc_id = m.group(1)
                title = re.sub(r"<[^>]+>", "", m.group(2)).strip()
                title = title.replace("&nbsp;", "").strip()
                if title and doc_id not in titles:
                    titles[doc_id] = title

            # Regulations: titles in modal headers (h5), linked to SUBORDINATE PDFs
            for modal in html.split('class="modal-header"')[1:]:
                h5 = re.search(r"<h5[^>]*>(.*?)</h5>", modal, re.DOTALL)
                if not h5:
                    continue
                title = re.sub(r"<[^>]+>", "", h5.group(1)).strip()

                # Find SUBORDINATE doc IDs in this modal
                sub_ids = re.findall(
                    r"/cms/images/LEGISLATION/SUBORDINATE/\d{4}/([^/]+)/",
                    modal,
                )
                for doc_id in sub_ids:
                    if doc_id not in titles and title:
                        titles[doc_id] = title

        logger.info(f"Loaded {len(titles)} titles from index")
        self._title_cache = titles
        return titles

    def _enumerate_documents(self, collection: str) -> List[Dict[str, Any]]:
        """Enumerate all document folders in a collection (PRINCIPAL or SUBORDINATE).

        For each document folder, finds the current (highest-version) PDF.
        Returns list of dicts with doc_id, year, pdf_path, version.
        """
        docs = []
        base_path = f"{LEGISLATION_BASE}/{collection}/"

        # Get list of years
        self.rate_limiter.wait()
        try:
            resp = self.client.get(base_path)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to list {collection} years: {e}")
            return docs

        years = [e for e in _parse_dir_listing(resp.text) if re.match(r"^\d{4}$", e)]
        logger.info(f"  {collection}: {len(years)} year directories")

        for year in sorted(years):
            self.rate_limiter.wait()
            try:
                resp = self.client.get(f"{base_path}{year}/")
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"Failed to list {collection}/{year}: {e}")
                continue

            folders = [e for e in _parse_dir_listing(resp.text) if not e.endswith(".pdf")]

            for folder in folders:
                self.rate_limiter.wait()
                try:
                    resp = self.client.get(f"{base_path}{year}/{folder}/")
                    resp.raise_for_status()
                except Exception as e:
                    logger.warning(f"Failed to list {collection}/{year}/{folder}: {e}")
                    continue

                # Find all PDFs and pick the highest version
                pdfs = [
                    e for e in _parse_dir_listing(resp.text)
                    if e.endswith(".pdf")
                ]

                if not pdfs:
                    continue

                # Sort by version number (suffix _N before .pdf)
                def version_key(filename: str) -> int:
                    m = re.search(r"_(\d+)\.pdf$", filename)
                    return int(m.group(1)) if m else 0

                best_pdf = max(pdfs, key=version_key)
                version = version_key(best_pdf)

                docs.append({
                    "doc_id": folder,
                    "year": year,
                    "collection": collection,
                    "pdf_filename": best_pdf,
                    "pdf_path": f"{base_path}{year}/{folder}/{best_pdf}",
                    "version": version,
                })

        logger.info(f"  {collection}: {len(docs)} documents found")
        return docs

    def normalize(self, raw: dict) -> dict:
        collection = raw.get("collection", "PRINCIPAL")
        doc_type = "act" if collection == "PRINCIPAL" else "regulation"

        return {
            "_id": f"CA/YukonLaws/{raw['doc_id']}",
            "_source": "CA/YukonLaws",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", raw["doc_id"]),
            "text": raw.get("_prefetched_text", ""),
            "date": raw.get("year", ""),
            "url": f"{BASE_URL}{raw['pdf_path']}",
            "doc_id": raw["doc_id"],
            "doc_type": doc_type,
            "version": raw.get("version", 0),
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        titles = self._load_titles()

        logger.info("Enumerating documents from directory listings...")
        all_docs = []
        for collection in ["PRINCIPAL", "SUBORDINATE"]:
            all_docs.extend(self._enumerate_documents(collection))

        logger.info(f"Total documents to process: {len(all_docs)}")

        limit = 15 if sample else None
        count = 0

        for doc in all_docs:
            if limit and count >= limit:
                break

            doc_id = doc["doc_id"]
            title = titles.get(doc_id, doc_id)
            doc["title"] = title

            # Download PDF
            try:
                self.rate_limiter.wait()
                resp = self.client.get(doc["pdf_path"])
                resp.raise_for_status()
                pdf_bytes = resp.content
            except Exception as e:
                logger.warning(f"  Failed to download PDF for {doc_id}: {e}")
                continue

            if len(pdf_bytes) < 100:
                logger.warning(f"  Skipping {doc_id} - tiny PDF ({len(pdf_bytes)} bytes)")
                continue

            # Extract text
            text = extract_pdf_markdown(
                source="CA/YukonLaws",
                source_id=doc_id,
                pdf_bytes=pdf_bytes,
                table="legislation",
            ) or ""

            if not text or len(text) < 50:
                logger.warning(f"  Skipping {doc_id} - no/short text ({len(text)} chars)")
                continue

            doc["_prefetched_text"] = text
            yield doc
            count += 1
            logger.info(f"  [{count}] {title[:60]} ({len(text)} chars)")

        logger.info(f"Total records yielded: {count}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        yield from self.fetch_all()


if __name__ == "__main__":
    scraper = YukonLawsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        print("Testing Yukon Laws access...")
        # Test directory listing
        resp = scraper.client.get(f"{LEGISLATION_BASE}/PRINCIPAL/")
        resp.raise_for_status()
        years = [e for e in _parse_dir_listing(resp.text) if re.match(r"^\d{4}$", e)]
        print(f"PRINCIPAL years: {len(years)}")

        resp = scraper.client.get(f"{LEGISLATION_BASE}/SUBORDINATE/")
        resp.raise_for_status()
        years = [e for e in _parse_dir_listing(resp.text) if re.match(r"^\d{4}$", e)]
        print(f"SUBORDINATE years: {len(years)}")

        # Test downloading one PDF
        resp = scraper.client.get(
            f"{LEGISLATION_BASE}/PRINCIPAL/2002/2002-0002/2002-0002_1.pdf"
        )
        resp.raise_for_status()
        print(f"Sample PDF download: {len(resp.content)} bytes")
        print("Test PASSED")
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
