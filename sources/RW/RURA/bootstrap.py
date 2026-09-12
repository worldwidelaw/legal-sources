#!/usr/bin/env python3
"""
RW/RURA -- Rwanda Utilities Regulatory Authority Regulations

Fetches regulatory documents across 5 sectors from the RURA website.
Each sector has a listing page with PDF links in a TYPO3 file list.
Full text is extracted from PDFs via common.pdf_extract.

Sectors:
  - ICT
  - Transport
  - Water & Sanitation
  - Energy
  - Nuclear & Radiation

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
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
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.RW.RURA")

BASE_URL = "https://rura.rw"
DELAY = 2.0

# (sector_label, listing_path)
SECTOR_PAGES = [
    ("ICT", "/sectors/ict/regulatory-instruments/regulations-and-guidelines"),
    ("Transport", "/sectors/transport/regulatory-instruments/regulations-and-guidelines"),
    ("Water & Sanitation", "/sectors/water-sanitation/regulatory-instruments/regulations-and-guidelines"),
    ("Energy", "/sectors/energy/regulatory-instruments/regulations-and-guidelines"),
    ("Nuclear & Radiation", "/sectors/nuclear-radiation/regulations-and-guidelines"),
]

# Exclude non-regulation PDFs
EXCLUDE_FILENAMES = {
    "Strategic_Plan_2022-2027_for_RURA_vision.pdf",
    "RURA_SERVICE_CHARTER.pdf",
}


def _make_id(pdf_path: str) -> str:
    """Generate a stable ID from the PDF file path."""
    filename = pdf_path.split("/")[-1].replace(".pdf", "")
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", filename).strip("_")
    if len(slug) > 80:
        slug = slug[:80]
    return f"RW_RURA_{slug}"


def _title_from_text(anchor_text: str, filename: str) -> str:
    """Get a clean title from anchor text or filename."""
    if anchor_text and len(anchor_text) > 5 and not anchor_text.endswith(".pdf"):
        return anchor_text.strip()
    # Derive from filename
    name = filename.replace(".pdf", "").replace("_", " ").strip()
    return name


def _parse_date(date_str: str) -> Optional[str]:
    """Parse dates like 'Aug 25, 2025' to ISO format."""
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%b %d %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    m = re.search(r"(\d{4})", date_str)
    if m:
        return f"{m.group(1)}-01-01"
    return None


def _extract_year_from_title(title: str) -> Optional[str]:
    """Try to extract a year from the regulation title or number."""
    m = re.search(r"/(\d{4})\b", title)
    if m:
        yr = int(m.group(1))
        if 2000 <= yr <= 2030:
            return f"{yr}-01-01"
    m = re.search(r"\b(20\d{2})\b", title)
    if m:
        return f"{m.group(1)}-01-01"
    return None


class RURAScraper(BaseScraper):
    """Scraper for Rwanda Utilities Regulatory Authority regulations."""

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

    def _scrape_sector_page(self, sector: str, path: str) -> List[Dict[str, Any]]:
        """Scrape a sector listing page for PDF document entries."""
        url = BASE_URL + path
        logger.info("Fetching sector: %s (%s)", sector, url)
        try:
            resp = self.http.get(url, timeout=30)
            if resp.status_code != 200:
                logger.warning("HTTP %d for %s", resp.status_code, url)
                return []
        except Exception as e:
            logger.warning("Error fetching %s: %s", url, e)
            return []

        html = resp.text
        docs = []

        # Parse table rows containing PDF links
        for row_match in re.finditer(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL):
            row = row_match.group(1)
            if ".pdf" not in row:
                continue

            # Extract PDF path
            pdf_match = re.search(
                r'href="(/fileadmin/user_upload/[^"]+\.pdf)"', row
            )
            if not pdf_match:
                continue
            pdf_path = unescape(pdf_match.group(1))
            filename = pdf_path.split("/")[-1]

            if filename in EXCLUDE_FILENAMES:
                continue

            # Extract title from anchor text
            title_match = re.search(
                r'href="' + re.escape(pdf_match.group(1)) + r'"[^>]*>\s*([^<]+)<',
                row,
            )
            anchor_text = title_match.group(1).strip() if title_match else ""

            # Extract date
            date_match = re.search(
                r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},?\s+\d{4}",
                row,
            )
            date_str = date_match.group(0) if date_match else None

            docs.append({
                "pdf_path": pdf_path,
                "filename": filename,
                "title": _title_from_text(anchor_text, filename),
                "date": date_str,
                "sector": sector,
            })

        # Fallback: if no table rows matched, try direct PDF link extraction
        if not docs:
            for m in re.finditer(
                r'<a[^>]+href="(/fileadmin/user_upload/[^"]+\.pdf)"[^>]*>\s*([^<]*)<',
                html,
            ):
                pdf_path = unescape(m.group(1))
                filename = pdf_path.split("/")[-1]
                if filename in EXCLUDE_FILENAMES:
                    continue
                anchor_text = m.group(2).strip()
                if not anchor_text or anchor_text == filename:
                    continue
                docs.append({
                    "pdf_path": pdf_path,
                    "filename": filename,
                    "title": _title_from_text(anchor_text, filename),
                    "date": None,
                    "sector": sector,
                })

        logger.info("  Found %d documents in %s", len(docs), sector)
        return docs

    def _fetch_all_listings(self) -> List[Dict[str, Any]]:
        """Fetch document listings from all sector pages, deduplicated."""
        all_docs = []
        seen_paths = set()
        seen_filenames = set()
        for sector, path in SECTOR_PAGES:
            docs = self._scrape_sector_page(sector, path)
            for doc in docs:
                p = doc["pdf_path"]
                fn = doc["filename"]
                if p in seen_paths or fn in seen_filenames:
                    continue
                seen_paths.add(p)
                seen_filenames.add(fn)
                all_docs.append(doc)
            time.sleep(1.0)
        logger.info("Total unique documents: %d", len(all_docs))
        return all_docs

    def _download_and_extract(self, pdf_path: str, doc_id: str) -> Optional[str]:
        """Download a PDF and extract text."""
        pdf_url = BASE_URL + pdf_path
        try:
            resp = self.http.get(pdf_url, timeout=90)
            if resp.status_code != 200:
                logger.warning("HTTP %d downloading %s", resp.status_code, pdf_path)
                return None
            pdf_bytes = resp.content
            if len(pdf_bytes) < 200:
                logger.warning("File too small (%d bytes): %s", len(pdf_bytes), pdf_path)
                return None
            if not pdf_bytes[:5].startswith(b"%PDF"):
                logger.warning("Not a PDF (%s), skipping", pdf_path)
                return None
            text = extract_pdf_markdown("RW/RURA", doc_id, pdf_bytes=pdf_bytes)
            return text
        except Exception as e:
            logger.warning("Failed to download/extract %s: %s", pdf_path, e)
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all RURA documents with full text from PDFs."""
        all_docs = self._fetch_all_listings()
        logger.info("Total documents to process: %d", len(all_docs))

        for doc in all_docs:
            title = doc["title"]
            pdf_path = doc["pdf_path"]
            doc_id = _make_id(pdf_path)

            logger.info("Processing: %s (%s)", title[:70], doc["sector"])

            text = self._download_and_extract(pdf_path, doc_id)
            if not text or len(text.strip()) < 50:
                logger.warning("Insufficient text for %s, skipping", pdf_path)
                continue

            date = _parse_date(doc.get("date")) or _extract_year_from_title(title)

            yield {
                "_id": doc_id,
                "title": title,
                "date": date,
                "sector": doc["sector"],
                "pdf_url": BASE_URL + pdf_path,
                "text": text,
            }
            time.sleep(DELAY)

    def fetch_updates(self, since: str = "") -> Generator[dict, None, None]:
        """Fetch updates — for a small collection, re-fetch all."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        return {
            "_id": raw["_id"],
            "_source": "RW/RURA",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "sector": raw.get("sector", ""),
            "url": raw.get("pdf_url", ""),
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="RW/RURA bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Run full bootstrap")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--sample-size", type=int, default=15, help="Sample size")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = RURAScraper()

    if args.command == "test":
        docs = scraper._fetch_all_listings()
        print(f"OK — found {len(docs)} unique documents across {len(SECTOR_PAGES)} sectors")
        sectors = {}
        for d in docs:
            s = d.get("sector", "?")
            sectors[s] = sectors.get(s, 0) + 1
        for s, n in sorted(sectors.items()):
            print(f"  {s}: {n}")
        return

    if args.command == "bootstrap":
        sample = args.sample and not args.full
        stats = scraper.bootstrap(sample_mode=sample, sample_size=args.sample_size)
        print(json.dumps(stats, indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
