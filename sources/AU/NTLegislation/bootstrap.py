#!/usr/bin/env python3
"""
AU/NTLegislation -- Northern Territory Legislation Fetcher

Fetches Northern Territory Acts and subordinate legislation from
the official register at legislation.nt.gov.au (Sitecore CMS).

Strategy:
  - Discover document slugs via browse listing pages (By-Title)
  - Visit each document page to extract numeric download ID
  - Download PDF via /api/sitecore/Act/PDF?id={NUMERIC_ID}
  - Extract text from PDF using pdfplumber (fallback: pypdf)
  - No auth required; free public access

Data:
  - ~384 Acts + ~304 subordinate legislation
  - Full text via PDF download
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
logger = logging.getLogger("legal-data-hunter.AU.NTLegislation")

BASE_URL = "https://legislation.nt.gov.au"
BROWSE_ACTS = f"{BASE_URL}/en/LegislationPortal/Acts/By-Title"
BROWSE_SL = f"{BASE_URL}/en/LegislationPortal/Subordinate-Legislation/By-Title"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
}

# Courteous delay — no robots.txt crawl-delay, but respectful
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


def _discover_slugs(browse_url: str) -> List[str]:
    """Discover document slugs from a browse-by-title page.

    Returns deduplicated list of uppercase slugs (e.g., 'CRIMINAL-CODE-ACT-1983').
    """
    data = _fetch_url(browse_url)
    if not data:
        logger.error(f"Failed to fetch browse page: {browse_url}")
        return []

    html = data.decode("utf-8", errors="replace")
    # Match links to individual legislation pages
    slugs = re.findall(r'/en/Legislation/([A-Z0-9][A-Z0-9-]+?)(?:["\']|/)', html)
    # Deduplicate while preserving order
    seen = set()
    unique = []
    for s in slugs:
        if s not in seen:
            seen.add(s)
            unique.append(s)

    logger.info(f"Discovered {len(unique)} slugs from {browse_url}")
    return unique


def _extract_numeric_id(slug: str) -> Optional[int]:
    """Visit a legislation page and extract the numeric download ID."""
    url = f"{BASE_URL}/en/Legislation/{slug}"
    data = _fetch_url(url)
    if not data:
        return None

    html = data.decode("utf-8", errors="replace")
    match = re.search(r'/api/sitecore/Act/(?:PDF|Word)\?id=(\d+)', html)
    if match:
        return int(match.group(1))

    # Fallback: try SubordinateLegislation endpoint pattern
    match = re.search(r'/api/sitecore/SubordinateLegislation/(?:PDF|Word)\?id=(\d+)', html)
    if match:
        return int(match.group(1))

    logger.debug(f"No numeric ID found for {slug}")
    return None


def _extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract full text from PDF bytes using pdfplumber (fallback: pypdf)."""
    # Try pdfplumber first (cleaner output)
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = [page.extract_text() or "" for page in pdf.pages]
        text = "\n\n".join(p for p in pages if p.strip())
        if text.strip():
            return text.strip()
    except Exception as e:
        logger.debug(f"pdfplumber failed: {e}")

    # Fallback: pypdf
    try:
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(pdf_bytes))
        pages = [page.extract_text() or "" for page in reader.pages]
        text = "\n\n".join(p for p in pages if p.strip())
        if text.strip():
            return text.strip()
    except Exception as e:
        logger.debug(f"pypdf failed: {e}")

    return ""


def _human_title(slug: str) -> str:
    """Convert a slug like 'CRIMINAL-CODE-ACT-1983' to 'Criminal Code Act 1983'."""
    return slug.replace("-", " ").title()


class NTLegislationScraper(BaseScraper):
    """
    Scraper for AU/NTLegislation -- Northern Territory Legislation Register.
    Country: AU
    URL: https://legislation.nt.gov.au/

    Data types: legislation
    Auth: none (free public access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _download_document(self, slug: str, numeric_id: int) -> Optional[Dict[str, Any]]:
        """Download PDF and extract text for a single document."""
        pdf_url = f"{BASE_URL}/api/sitecore/Act/PDF?id={numeric_id}"
        data = _fetch_url(pdf_url, timeout=120)

        if not data or len(data) < 500:
            logger.debug(f"PDF too small or missing for {slug} (id={numeric_id})")
            return None

        text = _extract_text_from_pdf(data)
        if not text or len(text) < 100:
            logger.debug(f"Insufficient text from {slug}: {len(text)} chars")
            return None

        # Try to extract year from slug (e.g., 'CRIMINAL-CODE-ACT-1983' -> '1983')
        year_match = re.search(r'(\d{4})$', slug)
        year = year_match.group(1) if year_match else None

        return {
            "slug": slug,
            "numeric_id": numeric_id,
            "title": _human_title(slug),
            "text": text,
            "year": year,
            "pdf_url": pdf_url,
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record to standard schema."""
        slug = raw["slug"]
        return {
            "_id": slug,
            "_source": "AU/NTLegislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", _human_title(slug)),
            "text": raw.get("text", ""),
            "date": raw.get("year"),
            "url": f"{BASE_URL}/en/Legislation/{slug}",
            "slug": slug,
            "numeric_id": raw.get("numeric_id"),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all NT legislation documents."""
        # Phase 1: Discover Acts
        act_slugs = _discover_slugs(BROWSE_ACTS)
        time.sleep(CRAWL_DELAY)

        # Phase 2: Discover Subordinate Legislation
        sl_slugs = _discover_slugs(BROWSE_SL)
        time.sleep(CRAWL_DELAY)

        # Merge and deduplicate (some may appear in both lists)
        seen = set()
        all_slugs = []
        for s in act_slugs + sl_slugs:
            if s not in seen:
                seen.add(s)
                all_slugs.append(s)

        logger.info(
            f"Total unique slugs: {len(all_slugs)} "
            f"({len(act_slugs)} Acts + {len(sl_slugs)} SL)"
        )

        for slug in all_slugs:
            # Step 1: Get numeric ID from the legislation page
            numeric_id = _extract_numeric_id(slug)
            time.sleep(CRAWL_DELAY)

            if numeric_id is None:
                logger.warning(f"Could not resolve numeric ID for {slug}, skipping")
                continue

            # Step 2: Download PDF and extract text
            doc = self._download_document(slug, numeric_id)
            if doc:
                yield doc
            else:
                logger.warning(f"Failed to download {slug} (id={numeric_id})")
            time.sleep(CRAWL_DELAY)

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Re-fetch all documents (no incremental API available)."""
        yield from self.fetch_all()


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="AU/NTLegislation data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NTLegislationScraper()

    if args.command == "test":
        logger.info("Testing numeric ID extraction...")
        nid = _extract_numeric_id("CRIMINAL-CODE-ACT-1983")
        if nid:
            logger.info(f"OK — CRIMINAL-CODE-ACT-1983 has ID {nid}")
        else:
            logger.error("FAILED — could not extract ID for CRIMINAL-CODE-ACT-1983")
            sys.exit(1)

        time.sleep(CRAWL_DELAY)

        logger.info("Testing PDF download and text extraction...")
        doc = scraper._download_document("CRIMINAL-CODE-ACT-1983", nid)
        if doc and len(doc["text"]) > 1000:
            logger.info(f"OK — Criminal Code Act 1983: {len(doc['text'])} chars")
        else:
            logger.error("FAILED — could not extract text from PDF")
            sys.exit(1)

        time.sleep(CRAWL_DELAY)

        logger.info("Testing browse listing...")
        slugs = _discover_slugs(BROWSE_ACTS)
        logger.info(f"OK — discovered {len(slugs)} Acts")

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
