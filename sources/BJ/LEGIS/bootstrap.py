#!/usr/bin/env python3
"""
BJ/LEGIS -- Benin LEGIS Legal Database

Fetches enacted laws from the Benin CDIJ legal database.

Strategy:
  - Paginate through listing pages (5 per page, ~691 pages)
  - Parse HTML to extract titles, dates, UUIDs, descriptions
  - Download PDFs via /{uuid}/open and extract text with pdfplumber
  - Skip scanned-image PDFs (no extractable text)

Data: ~3,451 laws (some scanned PDFs without extractable text)
License: Open access (government legal database)
Rate limit: 0.5 req/sec.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import io
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List
from html import unescape

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

try:
    import pdfplumber
except ImportError:
    print("ERROR: pdfplumber not installed. Run: pip3 install pdfplumber")
    sys.exit(1)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BJ.LEGIS")

BASE_URL = "https://legis.cdij.bj"

# French month names
FRENCH_MONTHS = {
    'janvier': '01', 'février': '02', 'mars': '03', 'avril': '04',
    'mai': '05', 'juin': '06', 'juillet': '07', 'août': '08',
    'septembre': '09', 'octobre': '10', 'novembre': '11', 'décembre': '12',
    'fevrier': '02', 'aout': '08',
}


def parse_french_date(text: str) -> Optional[str]:
    """Parse French date like '21 mars 2025' to ISO format."""
    if not text:
        return None
    m = re.search(r'(\d{1,2})\s+(\w+)\s+(\d{4})', text.lower())
    if not m:
        return None
    day = m.group(1).zfill(2)
    month = FRENCH_MONTHS.get(m.group(2))
    year = m.group(3)
    if not month:
        return None
    return f"{year}-{month}-{day}"


class BJLEGISScraper(BaseScraper):
    """
    Scraper for BJ/LEGIS -- Benin CDIJ.
    Country: BJ
    URL: https://legis.cdij.bj

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    def _get_with_retry(self, url: str, max_retries: int = 3, timeout: int = 60) -> Optional[requests.Response]:
        """GET with retry logic."""
        for attempt in range(max_retries):
            try:
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 200:
                    return resp
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt+1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(3 * (attempt + 1))
        return None

    def _parse_listing_page(self, html: str) -> List[dict]:
        """Parse a listing page to extract document metadata."""
        items = []

        # Find unique UUIDs
        uuids = list(dict.fromkeys(
            re.findall(r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', html)
        ))

        # Find titles (h2/h3/h4 with law text)
        titles = re.findall(r'<h\d[^>]*>\s*(.*?)\s*</h\d>', html, re.S)
        titles = [re.sub(r'<[^>]+>', '', t).strip() for t in titles
                  if re.search(r'(?:Loi|Décret|Ordonnance|Constitution|Arrêté)', t, re.I)]

        # Find descriptions from <p> tags with substantial content
        desc_blocks = re.findall(r'<p[^>]*>\s*(.*?)\s*</p>', html, re.S)
        descriptions = []
        for d in desc_blocks:
            clean = unescape(re.sub(r'<[^>]+>', '', d)).strip()
            if len(clean) > 20 and not clean.startswith('Affichage') and not clean.startswith('©'):
                descriptions.append(clean)

        for i, uuid in enumerate(uuids):
            title = titles[i] if i < len(titles) else f"Document {uuid[:8]}"
            desc = descriptions[i] if i < len(descriptions) else ""

            # Parse date from title
            date_iso = parse_french_date(title)

            items.append({
                "uuid": uuid,
                "title": title,
                "description": desc,
                "date": date_iso,
            })

        return items

    def _extract_text_from_pdf(self, uuid: str) -> Optional[str]:
        """Download PDF and extract text."""
        url = f"{BASE_URL}/{uuid}/open"
        resp = self._get_with_retry(url, timeout=90)
        if not resp:
            return None

        if resp.content[:4] != b'%PDF':
            logger.debug(f"Not a PDF for {uuid}")
            return None

        try:
            pdf = pdfplumber.open(io.BytesIO(resp.content))
            text_parts = []
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    text_parts.append(t)
            pdf.close()

            text = "\n\n".join(text_parts)
            text = re.sub(r'\n{3,}', '\n\n', text)
            text = re.sub(r' {2,}', ' ', text)
            return text.strip() if len(text) > 100 else None
        except Exception as e:
            logger.debug(f"PDF extraction failed for {uuid}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all laws with extractable full text."""
        total = 0
        skipped = 0
        page = 1

        while True:
            time.sleep(2)
            resp = self._get_with_retry(f"{BASE_URL}/lois-promulguees?page={page}")
            if not resp:
                break

            items = self._parse_listing_page(resp.text)
            if not items:
                break

            for item in items:
                time.sleep(2)
                text = self._extract_text_from_pdf(item["uuid"])
                if not text:
                    skipped += 1
                    continue

                item["text"] = text
                total += 1
                yield item

                if total % 50 == 0:
                    logger.info(f"Progress: {total} docs (page {page}, {skipped} skipped)")

            page += 1

        logger.info(f"Scan complete: {total} docs with text, {skipped} skipped (no text)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent laws (first pages, sorted by date)."""
        since_str = since.strftime("%Y-%m-%d")
        logger.info(f"Fetching updates since {since_str}")

        for page in range(1, 50):
            time.sleep(2)
            resp = self._get_with_retry(f"{BASE_URL}/lois-promulguees?page={page}")
            if not resp:
                break

            items = self._parse_listing_page(resp.text)
            if not items:
                break

            for item in items:
                if item.get("date") and item["date"] < since_str:
                    return

                time.sleep(2)
                text = self._extract_text_from_pdf(item["uuid"])
                if text:
                    item["text"] = text
                    yield item

    def fetch_sample(self, count: int = 15) -> Generator[dict, None, None]:
        """Fetch sample laws, scanning multiple pages to find extractable PDFs."""
        found = 0
        page = 50  # Start from older pages (more likely to have text-based PDFs)

        while found < count and page < 200:
            time.sleep(2)
            resp = self._get_with_retry(f"{BASE_URL}/lois-promulguees?page={page}")
            if not resp:
                page += 10
                continue

            items = self._parse_listing_page(resp.text)
            if not items:
                break

            for item in items:
                if found >= count:
                    break

                time.sleep(2)
                text = self._extract_text_from_pdf(item["uuid"])
                if not text:
                    logger.debug(f"No text: {item['uuid'][:8]} ({item.get('title', '')[:50]})")
                    continue

                item["text"] = text
                found += 1
                logger.info(
                    f"Sample {found}/{count}: {item['uuid'][:8]} "
                    f"({len(text)} chars) {item.get('title', '')[:60]}"
                )
                yield item

            page += 10  # Skip pages to get variety

        logger.info(f"Sample complete: {found} records")

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw law record to standard schema."""
        uuid = raw["uuid"]
        title = raw.get("title", uuid)
        title = re.sub(r'\s+', ' ', title).strip()[:500]

        return {
            "_id": f"BJ-LEGIS-{uuid[:8]}",
            "_source": "BJ/LEGIS",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": raw.get("date"),
            "url": f"{BASE_URL}/{uuid}/open",
            "uuid": uuid,
            "description": raw.get("description", ""),
        }

    def test_api(self) -> bool:
        """Test connectivity and PDF extraction."""
        logger.info("Testing Benin LEGIS database...")

        # Test listing page
        resp = self._get_with_retry(f"{BASE_URL}/lois-promulguees?page=100")
        if not resp:
            logger.error("Listing page failed")
            return False

        items = self._parse_listing_page(resp.text)
        if not items:
            logger.error("No items parsed")
            return False
        logger.info(f"Listing OK: {len(items)} items on page 100")

        # Test PDF extraction (try multiple until one works)
        for item in items:
            time.sleep(2)
            text = self._extract_text_from_pdf(item["uuid"])
            if text:
                logger.info(f"PDF extraction OK: {len(text)} chars from {item['uuid'][:8]}")
                logger.info("All tests passed")
                return True

        logger.error("No PDFs with extractable text found on test page")
        return False


# -- CLI entry point ---------------------------------------------------------

if __name__ == "__main__":
    scraper = BJLEGISScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample] [--count N]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        count = 15
        for i, arg in enumerate(sys.argv):
            if arg == "--count" and i + 1 < len(sys.argv):
                count = int(sys.argv[i + 1])

        if sample_mode:
            gen = scraper.fetch_sample(count=count)
        else:
            gen = scraper.fetch_all()

        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in gen:
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1
            logger.info(f"Saved: {out_path.name}")

        logger.info(f"Bootstrap complete: {saved} records saved to {sample_dir}")

    elif command == "update":
        since_str = None
        for i, arg in enumerate(sys.argv):
            if arg == "--since" and i + 1 < len(sys.argv):
                since_str = sys.argv[i + 1]

        if not since_str:
            print("Usage: python bootstrap.py update --since YYYY-MM-DD")
            sys.exit(1)

        since = datetime.strptime(since_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in scraper.fetch_updates(since):
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1

        logger.info(f"Update complete: {saved} records saved")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
