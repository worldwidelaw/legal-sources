#!/usr/bin/env python3
"""
MX/Queretaro-Leg -- Querétaro State Legislature Laws & Codes

Fetches consolidated laws, codes, organic laws, and the state constitution
from the Querétaro State Legislature (LXI Legislatura) website. ~130 documents
across 3 categories, each as a PDF download. Full text via pdf_extract.

Pages scraped:
  - /leyes/            ~111 laws (3-col table: #, title, download)
  - /codigos/          ~7 codes (2-col table: title, download)
  - /leyes-organicas/  ~13 organic laws (3-col table: #, title, download)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Same as bootstrap (small dataset)
  python bootstrap.py test               # Quick connectivity test
"""

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
logger = logging.getLogger("legal-data-hunter.MX.Queretaro-Leg")

BASE_URL = "http://legislaturaqueretaro.gob.mx"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "MX/Queretaro-Leg"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.5",
}

REQUEST_DELAY = 2  # seconds between requests

# Category pages to scrape: (name, url_path, table_has_row_numbers)
# leyes and leyes-organicas have 3-column tables (row#, title, download)
# codigos has 2-column tables (title, download)
CATEGORIES = [
    ("Leyes", "/leyes/", True),
    ("Códigos", "/codigos/", False),
    ("Leyes Orgánicas", "/leyes-organicas/", True),
]


class QueretaroLegScraper(BaseScraper):
    """Scraper for MX/Queretaro-Leg -- Querétaro state laws and codes."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _get_law_list(self) -> List[Dict[str, str]]:
        """Scrape all category pages for law PDF links with titles."""
        all_laws = []
        seen_urls = set()

        for cat_name, url_path, has_row_nums in CATEGORIES:
            url = f"{BASE_URL}{url_path}"
            logger.info(f"Fetching category: {cat_name}")
            time.sleep(REQUEST_DELAY)

            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
            except requests.RequestException as e:
                logger.warning(f"Failed to fetch {cat_name}: {e}")
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            # Find all <a> tags linking to CloudPLQ PDF files
            for a_tag in soup.find_all("a", href=re.compile(r"CloudPLQ/InvEst.*\.pdf", re.I)):
                href = a_tag["href"]
                if href in seen_urls:
                    continue

                # Skip the "Tabla de Aplicabilidad" transparency PDF
                if "Transparencia" in href:
                    continue

                # Get the title from the parent <tr>
                title = ""
                parent_row = a_tag.find_parent("tr")
                if parent_row:
                    tds = parent_row.find_all("td")
                    if has_row_nums and len(tds) >= 2:
                        title = tds[1].get_text(strip=True)
                    elif not has_row_nums and len(tds) >= 1:
                        title = tds[0].get_text(strip=True)

                if not title:
                    # Fallback: derive from filename
                    title = href.rsplit("/", 1)[-1].replace(".pdf", "").replace("-", " ")

                # Extract doc ID from filename
                fname = href.rsplit("/", 1)[-1].replace(".pdf", "")

                seen_urls.add(href)
                all_laws.append({
                    "title": title,
                    "doc_id": fname,
                    "download_url": href,
                    "category": cat_name,
                })

            cat_count = sum(1 for l in all_laws if l["category"] == cat_name)
            logger.info(f"  Found {cat_count} documents in {cat_name}")

        logger.info(f"Total unique documents: {len(all_laws)}")
        return all_laws

    def _extract_pdf_text(self, pdf_content: bytes, doc_id: str) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=doc_id,
            pdf_bytes=pdf_content,
            table="legislation",
        ) or ""

    def _fetch_law(self, law: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Download PDF and extract full text for a single law."""
        for attempt in range(3):
            time.sleep(REQUEST_DELAY)
            try:
                resp = self.session.get(law["download_url"], timeout=60, allow_redirects=True)
                resp.raise_for_status()
                break
            except requests.RequestException as e:
                if attempt < 2:
                    logger.warning(f"Retry {attempt+1} for {law['title'][:50]}: {e}")
                    time.sleep(3)
                    continue
                logger.warning(f"Failed to download {law['title'][:50]}: {e}")
                return None

        if b"%PDF" not in resp.content[:10]:
            logger.warning(f"Not a PDF for {law['title'][:50]}")
            return None

        text = self._extract_pdf_text(resp.content, law["doc_id"])
        if len(text) < 100:
            logger.warning(f"Very short text for {law['title'][:50]}: {len(text)} chars")
            return None

        content_hash = hashlib.md5(resp.content).hexdigest()[:12]

        return {
            "title": law["title"],
            "text": text,
            "download_url": law["download_url"],
            "doc_id": law["doc_id"],
            "category": law["category"],
            "content_hash": content_hash,
            "file_size": len(resp.content),
        }

    def normalize(self, raw: dict) -> dict:
        """Transform raw law data into standard schema."""
        return {
            "_id": f"MX-QRO-{raw['doc_id']}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": "",
            "category": raw.get("category", ""),
            "url": raw["download_url"],
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Yield all Querétaro laws as RAW dicts (full text included).

        Per the BaseScraper contract, fetch_all yields RAW records and the
        framework (bootstrap_fast) calls normalize() on each. Yielding already
        normalized records here caused a double-normalize on the VPS — the
        second normalize() hit KeyError 'doc_id' on every record, so all ~131
        documents were counted as errors and 0 were written (#942).
        """
        laws = self._get_law_list()

        if sample:
            step = max(1, len(laws) // 15)
            laws = laws[::step][:15]

        total = 0
        for i, law in enumerate(laws):
            logger.info(f"[{i+1}/{len(laws)}] Downloading: {law['title'][:60]}...")
            raw = self._fetch_law(law)
            if raw:
                yield raw
                total += 1
                logger.info(f"  OK: {len(raw['text'])} chars")
            else:
                logger.warning(f"  SKIP: {law['title'][:60]}")

        logger.info(f"\nTotal laws fetched: {total}")

    def fetch_updates(self, since: str = None) -> Generator[dict, None, None]:
        """Fetch all laws (small dataset, always full refresh)."""
        yield from self.fetch_all(sample=False)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = QueretaroLegScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing connectivity to legislaturaqueretaro.gob.mx...")
        laws = scraper._get_law_list()
        if laws:
            logger.info(f"Found {len(laws)} laws. Testing first PDF...")
            raw = scraper._fetch_law(laws[0])
            if raw:
                logger.info(f"SUCCESS: {raw['title'][:60]} - {len(raw['text'])} chars")
            else:
                logger.error("FAILED: Could not extract text from first PDF")
                sys.exit(1)
        else:
            logger.error("FAILED: Could not fetch law listing")
            sys.exit(1)

    elif command == "bootstrap":
        SAMPLE_DIR.mkdir(exist_ok=True)
        count = 0
        for raw in scraper.fetch_all(sample=sample):
            record = scraper.normalize(raw)
            if sample:
                fname = SAMPLE_DIR / f"{record['_id']}.json"
                with open(fname, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Bootstrap complete: {count} records")

    elif command == "update":
        count = 0
        for raw in scraper.fetch_updates():
            count += 1
        logger.info(f"Update complete: {count} records")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
