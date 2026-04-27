#!/usr/bin/env python3
"""
AU/ACTLegislation -- ACT Legislation Register Fetcher

Fetches Australian Capital Territory Acts and subordinate legislation from
the authorised electronic statute book at legislation.act.gov.au.

Strategy:
  - Discover document IDs via browse listing pages
  - Download DOCX files via /DownloadFile/{type}/{id}/current/DOCX/{id}.DOCX
  - Extract text from DOCX using python-docx
  - No auth required; free public access

Data:
  - ~319 Acts + ~222 subordinate legislation
  - Full text via DOCX download
  - Language: English

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Re-fetch all (no incremental API)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AU.ACTLegislation")

BASE_URL = "https://www.legislation.act.gov.au"
BROWSE_ACTS = f"{BASE_URL}/results?category=cAct&status=Current&action=browse"
BROWSE_SL = f"{BASE_URL}/results?category=cSub&status=Current&action=browse"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
}

# Respect 10-second crawl delay from robots.txt
CRAWL_DELAY = 10


def _fetch_url(url: str, timeout: int = 60) -> Optional[bytes]:
    """Fetch a URL with error handling."""
    req = Request(url, headers=HEADERS)
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except (URLError, HTTPError) as e:
        logger.debug(f"Failed to fetch {url}: {e}")
        return None


def _discover_documents(browse_url: str, prefix: str) -> List[Tuple[str, str, str]]:
    """Discover document IDs and titles from a browse page.

    Returns list of (doc_id, title, doc_type) tuples.
    doc_type is 'a' for acts, 'sl' for subordinate legislation.
    """
    data = _fetch_url(browse_url)
    if not data:
        logger.error(f"Failed to fetch browse page: {browse_url}")
        return []

    html = data.decode("utf-8", errors="replace")
    pattern = r'<a[^>]+href="(' + re.escape(prefix) + r'(\d{4}-\d+)/)"[^>]*>([^<]+)</a>'
    matches = re.findall(pattern, html)

    results = []
    seen = set()
    doc_type = "a" if prefix == "/a/" else "sl"

    for _path, doc_id, title in matches:
        if doc_id not in seen:
            seen.add(doc_id)
            results.append((doc_id, title.strip(), doc_type))

    logger.info(f"Discovered {len(results)} documents from {browse_url}")
    return results


def _extract_text_from_docx(docx_bytes: bytes) -> str:
    """Extract full text from a DOCX file."""
    try:
        import docx
        doc = docx.Document(io.BytesIO(docx_bytes))
        paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
        return "\n".join(paragraphs)
    except Exception as e:
        logger.warning(f"DOCX extraction failed: {e}")
        return ""


class ACTLegislationScraper(BaseScraper):
    """
    Scraper for AU/ACTLegislation -- ACT Legislation Register.
    Country: AU
    URL: https://www.legislation.act.gov.au/

    Data types: legislation
    Auth: none (free public access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _download_document(self, doc_id: str, doc_type: str) -> Optional[Dict[str, Any]]:
        """Download a DOCX and extract text for a single document."""
        docx_url = f"{BASE_URL}/DownloadFile/{doc_type}/{doc_id}/current/DOCX/{doc_id}.DOCX"
        data = _fetch_url(docx_url, timeout=120)

        if not data or len(data) < 1000:
            # Try PDF as fallback (some older docs may not have DOCX)
            logger.debug(f"DOCX not available for {doc_type}/{doc_id}, skipping")
            return None

        text = _extract_text_from_docx(data)
        if not text or len(text) < 100:
            logger.debug(f"Insufficient text from {doc_type}/{doc_id}: {len(text)} chars")
            return None

        # Extract year from doc_id (e.g., "2001-14" -> "2001")
        year = doc_id.split("-")[0] if "-" in doc_id else None

        return {
            "doc_id": doc_id,
            "doc_type": doc_type,
            "text": text,
            "year": year,
            "docx_url": docx_url,
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record to standard schema."""
        doc_id = raw["doc_id"]
        doc_type = raw.get("doc_type", "a")
        title = raw.get("title", doc_id)

        return {
            "_id": f"{doc_type}/{doc_id}",
            "_source": "AU/ACTLegislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": raw.get("year"),
            "url": f"{BASE_URL}/{doc_type}/{doc_id}/",
            "doc_id": doc_id,
            "doc_type": "act" if doc_type == "a" else "subordinate_legislation",
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all ACT legislation documents."""
        # Phase 1: Discover Acts
        acts = _discover_documents(BROWSE_ACTS, "/a/")
        time.sleep(CRAWL_DELAY)

        # Phase 2: Discover Subordinate Legislation
        sl = _discover_documents(BROWSE_SL, "/sl/")
        time.sleep(CRAWL_DELAY)

        all_docs = acts + sl
        logger.info(f"Total documents to fetch: {len(all_docs)} ({len(acts)} Acts + {len(sl)} SL)")

        for doc_id, title, doc_type in all_docs:
            doc = self._download_document(doc_id, doc_type)
            if doc:
                doc["title"] = title
                yield doc
            else:
                logger.warning(f"Failed to download {doc_type}/{doc_id} ({title})")
            time.sleep(CRAWL_DELAY)

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Re-fetch all documents (no incremental API available)."""
        yield from self.fetch_all()


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="AU/ACTLegislation data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = ACTLegislationScraper()

    if args.command == "test":
        logger.info("Testing DOCX download...")
        doc = scraper._download_document("2001-14", "a")
        if doc:
            logger.info(f"OK — Legislation Act 2001: {len(doc['text'])} chars")
        else:
            logger.error("FAILED — could not fetch a/2001-14")
            sys.exit(1)

        logger.info("Testing browse listing...")
        acts = _discover_documents(BROWSE_ACTS, "/a/")
        logger.info(f"OK — discovered {len(acts)} Acts")

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
