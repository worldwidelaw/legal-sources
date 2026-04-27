#!/usr/bin/env python3
"""
CA/NunavutLegislation -- Nunavut Consolidated Acts & Regulations

Fetches consolidated legislation from nunavutlegislation.ca.
The site is a Drupal glossary view; we iterate A-Z letter pages,
extract individual law page links, then follow each to find the
PDF download link and extract full text.

URL patterns:
  Listing:  /en/consolidated-law/current?title={LETTER}
  Law page: /en/consolidated-law/{slug}
  Download: /en/file-download/download/public/{id}

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import hashlib
import logging
import string
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CA.NunavutLegislation")

BASE_URL = "https://www.nunavutlegislation.ca"
LISTING_PATH = "/en/consolidated-law/current"


def _slug_to_title(slug: str) -> str:
    """Convert URL slug to readable title."""
    title = slug.replace("-", " ").replace("_", " ")
    # Remove common suffixes
    for suffix in ["consolidation", "official consolidation", "consolidation legal treatment"]:
        title = re.sub(rf"\s+{suffix}$", "", title, flags=re.IGNORECASE)
    return title.strip().title()


def _classify_doc(title: str) -> str:
    """Classify as act or regulation based on title."""
    lower = title.lower()
    if "regulation" in lower or "order" in lower or "rules" in lower:
        return "regulation"
    return "act"


class NunavutLegislationScraper(BaseScraper):
    """Scraper for CA/NunavutLegislation."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,*/*",
            },
            timeout=120,
        )

    def _fetch_listing(self) -> List[Dict[str, Any]]:
        """Iterate A-Z glossary pages to collect all law page links."""
        docs = []
        seen_slugs = set()

        for letter in string.ascii_uppercase:
            self.rate_limiter.wait()
            url = f"{LISTING_PATH}?title={letter}"
            try:
                resp = self.client.get(url)
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"Failed to fetch letter {letter}: {e}")
                continue

            # Extract links to individual law pages
            links = re.findall(
                r'href="(/en/consolidated-law/[^"?]+)"',
                resp.text,
            )

            for href in links:
                # Skip navigation links (current, original, legislation-notices, letter pages)
                slug = href.rstrip("/").split("/")[-1]
                if slug in ("current", "original", "legislation-notices"):
                    continue
                if slug in seen_slugs:
                    continue
                seen_slugs.add(slug)

                title = _slug_to_title(slug)
                doc_type = _classify_doc(title)
                doc_id = hashlib.md5(slug.encode()).hexdigest()[:12]

                docs.append({
                    "doc_id": doc_id,
                    "title": title,
                    "slug": slug,
                    "doc_type": doc_type,
                    "page_path": href,
                    "page_url": BASE_URL + href,
                })

            logger.info(f"Letter {letter}: {len(links)} links, total unique: {len(docs)}")

        acts = sum(1 for d in docs if d["doc_type"] == "act")
        regs = sum(1 for d in docs if d["doc_type"] == "regulation")
        logger.info(f"Total: {len(docs)} documents (Acts: {acts}, Regulations: {regs})")
        return docs

    def _get_download_url(self, page_path: str) -> Optional[str]:
        """Visit individual law page and extract the PDF download link."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(page_path)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch {page_path}: {e}")
            return None

        # Look for file-download links
        match = re.search(
            r'href="(/en/file-download/download/public/\d+)"',
            resp.text,
        )
        if match:
            return BASE_URL + match.group(1)
        return None

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"CA/NunavutLegislation/{raw['doc_id']}",
            "_source": "CA/NunavutLegislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("_prefetched_text", ""),
            "date": "",
            "url": raw.get("page_url", ""),
            "doc_id": raw["doc_id"],
            "doc_type": raw.get("doc_type", ""),
            "slug": raw.get("slug", ""),
            "download_url": raw.get("download_url", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        all_docs = self._fetch_listing()
        limit = 15 if sample else None
        count = 0

        for doc in all_docs:
            if limit and count >= limit:
                break

            # Get PDF download URL from the law page
            download_url = self._get_download_url(doc["page_path"])
            if not download_url:
                logger.warning(f"  No download link for {doc['title'][:60]}")
                continue
            doc["download_url"] = download_url

            # Download the PDF
            try:
                self.rate_limiter.wait()
                resp = self.client.get(download_url)
                resp.raise_for_status()
                pdf_bytes = resp.content
            except Exception as e:
                logger.warning(f"  Failed to download PDF for {doc['title'][:60]}: {e}")
                continue

            # Extract text
            text = extract_pdf_markdown(
                source="CA/NunavutLegislation",
                source_id=doc["doc_id"],
                pdf_bytes=pdf_bytes,
                table="legislation",
            ) or ""

            if not text or len(text) < 50:
                logger.warning(f"  Skipping {doc['title'][:60]} - no/short text")
                continue

            doc["_prefetched_text"] = text
            yield doc
            count += 1
            logger.info(f"  [{count}] {doc['title'][:60]} ({len(text)} chars)")

        logger.info(f"Total records yielded: {count}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for doc in self.fetch_all():
            yield doc


if __name__ == "__main__":
    scraper = NunavutLegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        print("Testing Nunavut legislation listing...")
        docs = scraper._fetch_listing()
        if docs:
            print(f"Connection OK. Found {len(docs)} legislation documents.")
            acts = sum(1 for d in docs if d["doc_type"] == "act")
            regs = sum(1 for d in docs if d["doc_type"] == "regulation")
            print(f"  Acts: {acts}, Regulations: {regs}")
            print(f"Sample: {docs[0]['title']} -> {docs[0]['page_url'][:80]}")
        else:
            print("Connection FAILED - no documents found")
            sys.exit(1)
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
