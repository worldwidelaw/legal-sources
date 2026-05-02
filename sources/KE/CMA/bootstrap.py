#!/usr/bin/env python3
"""
KE/CMA -- Kenya Capital Markets Authority Regulations & Guidelines

Fetches acts, regulations, guidelines, circulars, policy guidance notes,
EAC directives, and corporate governance documents from the CMA website.

Usage:
  python bootstrap.py bootstrap --sample    # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Dict, Any
from urllib.parse import unquote, urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KE.CMA")

BASE_URL = "https://www.cma.or.ke"
REGULATORY_URL = f"{BASE_URL}/regulatory-framework/"
DELAY = 2.0

# Category slug -> doc_type mapping
CATEGORY_MAP = {
    "acts": "act",
    "regulations": "regulation",
    "guidelines": "guideline",
    "circulars": "circular",
    "policy-guidance-notes": "policy_guidance_note",
    "eac-council-directives": "eac_directive",
    "corporate-governance-for-issuers": "corporate_governance",
    "draft-regulations": "draft_regulation",
    "aml-cft": "aml_cft",
    "enforcement-of-securities-law": "enforcement",
}


def _extract_category(url: str) -> str:
    """Extract category slug from download URL."""
    # URL pattern: /download/{cat_id}/{cat_slug}/{file_id}/{filename}.pdf
    m = re.search(r"/download/\d+/([^/]+)/", url)
    return m.group(1) if m else "unknown"


def _extract_file_id(url: str) -> str:
    """Extract file ID from download URL."""
    m = re.search(r"/download/\d+/[^/]+/(\d+)/", url)
    return m.group(1) if m else ""


def _extract_year(title: str) -> Optional[str]:
    """Extract year from title."""
    m = re.search(r"\b(20\d{2}|19\d{2})\b", title)
    return f"{m.group(1)}-01-01" if m else None


def _make_id(file_id: str, title: str) -> str:
    """Generate a stable document ID."""
    name = re.sub(r"[^a-zA-Z0-9]+", "_", title).strip("_")
    if len(name) > 80:
        name = name[:80]
    return f"KE_CMA_{file_id}_{name}"


class CMAScraper(BaseScraper):
    """Scraper for Kenya CMA regulatory documents."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.http = HttpClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )

    def _discover_documents(self) -> List[Dict[str, Any]]:
        """Discover all PDF documents from the regulatory framework page."""
        from bs4 import BeautifulSoup

        all_docs = []
        seen_urls = set()

        try:
            resp = self.http.get(REGULATORY_URL, timeout=30)
            if resp.status_code != 200:
                logger.error("HTTP %d for %s", resp.status_code, REGULATORY_URL)
                return all_docs

            soup = BeautifulSoup(resp.text, "html.parser")

            for a in soup.find_all("a"):
                href = a.get("href", "")
                text = a.get_text(strip=True)
                if not href or not text:
                    continue

                # Only process download links (PDF/XLSX)
                if "/download/" not in href:
                    continue
                if not href.lower().endswith((".pdf", ".xlsx")):
                    continue
                # Skip non-PDF formats
                if href.lower().endswith(".xlsx"):
                    continue

                # Build absolute URL
                if href.startswith("/"):
                    href = BASE_URL + href
                elif not href.startswith("http"):
                    href = urljoin(REGULATORY_URL, href)

                # Deduplicate
                url_key = href.split("?")[0].lower()
                if url_key in seen_urls:
                    continue
                seen_urls.add(url_key)

                file_id = _extract_file_id(href)
                cat_slug = _extract_category(href)
                doc_type = CATEGORY_MAP.get(cat_slug, "regulatory_document")

                all_docs.append({
                    "doc_id": _make_id(file_id, text),
                    "pdf_url": href,
                    "title": text.strip(),
                    "category": cat_slug,
                    "doc_type": doc_type,
                    "date": _extract_year(text),
                })

        except Exception as e:
            logger.error("Error scraping regulatory framework: %s", e)

        logger.info("Total unique documents discovered: %d", len(all_docs))
        return all_docs

    def _download_and_extract(self, pdf_url: str, doc_id: str) -> Optional[str]:
        """Download a PDF and extract text."""
        try:
            resp = self.http.get(pdf_url, timeout=60)
            if resp.status_code != 200:
                logger.warning("HTTP %d downloading %s", resp.status_code, pdf_url)
                return None
            pdf_bytes = resp.content
            if len(pdf_bytes) < 200:
                logger.warning("PDF too small (%d bytes): %s", len(pdf_bytes), pdf_url)
                return None
            text = extract_pdf_markdown("KE/CMA", doc_id, pdf_bytes=pdf_bytes)
            return text
        except Exception as e:
            logger.warning("Failed to download/extract %s: %s", pdf_url, e)
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all CMA documents with full text."""
        all_docs = self._discover_documents()
        logger.info("Total documents to process: %d", len(all_docs))

        for doc in all_docs:
            doc_id = doc["doc_id"]
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
                "category": doc["category"],
                "pdf_url": doc["pdf_url"],
                "text": text,
            }
            time.sleep(DELAY)

    def fetch_updates(self, since: str = "") -> Generator[dict, None, None]:
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": raw["_id"],
            "_source": "KE/CMA",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "doc_type": raw.get("doc_type", ""),
            "category": raw.get("category", ""),
            "url": raw.get("pdf_url", ""),
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="KE/CMA bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Run full bootstrap")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--sample-size", type=int, default=15, help="Sample size")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = CMAScraper()

    if args.command == "test":
        docs = scraper._discover_documents()
        print(f"OK -- found {len(docs)} documents")
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
