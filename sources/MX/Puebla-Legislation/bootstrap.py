#!/usr/bin/env python3
"""
MX/Puebla-Legislation -- Puebla State Legislation Fetcher

Fetches consolidated laws, codes, and the constitution from the Congreso del
Estado de Puebla Joomla DocMan portal. ~146 laws across 3 categories
(Constituciones, Códigos, Leyes), each as a PDF download. Full text via PyPDF2.

Strategy:
  - Scrape DocMan category pages for download links (gid-based)
  - Download each PDF and extract text with PyPDF2
  - Deduplicate by doc_download gid

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Same as bootstrap (small dataset)
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Dict, Any

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MX.Puebla-Legislation")

BASE_URL = "https://www.congresopuebla.gob.mx"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "MX/Puebla-Legislation"

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.5",
}

REQUEST_DELAY = 2  # seconds between requests

# DocMan category IDs
CATEGORIES = {
    "Constituciones": 24,
    "Códigos": 23,
    "Leyes": 25,
}

# Skip these generic sidebar links that appear on every page
SKIP_TITLES = {"Ley Orgánica", "Reglamento Interior"}


class PueblaLegislationScraper(BaseScraper):
    """Scraper for MX/Puebla-Legislation -- Puebla state laws and codes."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _get_law_list(self) -> List[Dict[str, str]]:
        """Scrape all DocMan categories for law download links."""
        all_laws = []
        seen_gids = set()

        for cat_name, cat_gid in CATEGORIES.items():
            url = f"{BASE_URL}/index.php?option=com_docman&task=cat_view&gid={cat_gid}&limit=500&limitstart=0"
            logger.info(f"Fetching category: {cat_name} (gid={cat_gid})")
            time.sleep(REQUEST_DELAY)

            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
            except requests.RequestException as e:
                logger.warning(f"Failed to fetch category {cat_name}: {e}")
                continue

            soup = BeautifulSoup(resp.text, 'html.parser')

            for a in soup.find_all('a', href=True):
                href = a['href']
                if 'doc_download' not in href:
                    continue

                title = a.get_text(strip=True)
                if not title or len(title) < 5 or title in SKIP_TITLES:
                    continue

                gid_match = re.search(r'gid=(\d+)', href)
                if not gid_match:
                    continue

                doc_gid = gid_match.group(1)
                if doc_gid in seen_gids:
                    continue
                seen_gids.add(doc_gid)

                # Build absolute download URL
                if href.startswith('http'):
                    download_url = href
                else:
                    download_url = f"{BASE_URL}{href}"

                all_laws.append({
                    'title': title,
                    'doc_gid': doc_gid,
                    'download_url': download_url,
                    'category': cat_name,
                })

            logger.info(f"  Found {sum(1 for l in all_laws if l['category'] == cat_name)} laws in {cat_name}")

        logger.info(f"Total unique laws: {len(all_laws)}")
        return all_laws

    def _extract_pdf_text(self, pdf_content: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="MX/Puebla-Legislation",
            source_id="",
            pdf_bytes=pdf_content,
            table="legislation",
        ) or ""

    def _fetch_law(self, law: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Download PDF and extract full text for a single law."""
        time.sleep(REQUEST_DELAY)
        try:
            resp = self.session.get(law['download_url'], timeout=60, allow_redirects=True)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to download {law['title'][:50]}: {e}")
            return None

        # Check if it's actually a PDF
        if b'%PDF' not in resp.content[:10]:
            logger.warning(f"Not a PDF for {law['title'][:50]}")
            return None

        text = self._extract_pdf_text(resp.content)
        if len(text) < 100:
            logger.warning(f"Very short text for {law['title'][:50]}: {len(text)} chars")
            return None

        content_hash = hashlib.md5(resp.content).hexdigest()[:12]
        page_count = 0
        try:
            page_count = len(PdfReader(io.BytesIO(resp.content)).pages)
        except Exception:
            pass

        # Extract filename from Content-Disposition if available
        filename = ""
        cd = resp.headers.get('content-disposition', '')
        fn_match = re.search(r'filename="?([^";]+)', cd)
        if fn_match:
            filename = fn_match.group(1)

        return {
            'title': law['title'],
            'text': text,
            'download_url': law['download_url'],
            'doc_gid': law['doc_gid'],
            'category': law['category'],
            'filename': filename,
            'content_hash': content_hash,
            'file_size': len(resp.content),
            'page_count': page_count,
        }

    def normalize(self, raw: dict) -> dict:
        """Transform raw law data into standard schema."""
        return {
            '_id': f"MX-PUE-{raw['content_hash']}",
            '_source': SOURCE_ID,
            '_type': 'legislation',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': raw['title'],
            'text': raw['text'],
            'date': '',  # No date in listing; could parse from filename
            'category': raw.get('category', ''),
            'page_count': raw.get('page_count', 0),
            'url': raw['download_url'],
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Yield all Puebla laws with full text."""
        laws = self._get_law_list()

        if sample:
            step = max(1, len(laws) // 15)
            laws = laws[::step][:15]

        total = 0
        for i, law in enumerate(laws):
            logger.info(f"[{i+1}/{len(laws)}] Downloading: {law['title'][:60]}...")
            raw = self._fetch_law(law)
            if raw:
                record = self.normalize(raw)
                yield record
                total += 1
                logger.info(f"  OK: {len(raw['text'])} chars, {raw['page_count']} pages")
            else:
                logger.warning(f"  SKIP: {law['title'][:60]}")

        logger.info(f"\nTotal laws fetched: {total}")

    def fetch_updates(self, since: str = None) -> Generator[dict, None, None]:
        """Fetch all laws (small dataset, always full refresh)."""
        yield from self.fetch_all(sample=False)


if __name__ == "__main__":
    scraper = PueblaLegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing connectivity to congresopuebla.gob.mx...")
        laws = scraper._get_law_list()
        if laws:
            logger.info(f"Found {len(laws)} laws. Testing first PDF...")
            raw = scraper._fetch_law(laws[0])
            if raw:
                logger.info(f"SUCCESS: {raw['title'][:60]} - {len(raw['text'])} chars, {raw['page_count']} pages")
            else:
                logger.error("FAILED: Could not extract text from first PDF")
                sys.exit(1)
        else:
            logger.error("FAILED: Could not fetch law listing")
            sys.exit(1)

    elif command == "bootstrap":
        SAMPLE_DIR.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_all(sample=sample):
            if sample:
                fname = SAMPLE_DIR / f"{record['_id']}.json"
                with open(fname, 'w', encoding='utf-8') as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Bootstrap complete: {count} records")

    elif command == "update":
        count = 0
        for record in scraper.fetch_updates():
            count += 1
        logger.info(f"Update complete: {count} records")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
