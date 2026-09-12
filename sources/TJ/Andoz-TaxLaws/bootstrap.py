#!/usr/bin/env python3
"""
TJ/Andoz-TaxLaws -- Tax Committee of Tajikistan — Tax Legislation

Fetches tax legislation, codes, laws, decrees, resolutions, and committee
orders from andoz.tj. PDFs are scraped from /Legislation/ section pages.

Strategy:
  1. Scrape each /Legislation/ category page for PDF links
  2. Download each PDF and extract full text via common.pdf_extract
  3. Skip scanned/image-only PDFs that yield no extractable text
  4. Deduplicate by filename across pages

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap --full
  python bootstrap.py test
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple
from urllib.parse import quote, unquote, urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TJ.Andoz-TaxLaws")

BASE_URL = "https://andoz.tj"
DELAY = 2.0

# Per-PDF download guards. requests' `timeout` is a per-socket-read timeout, not
# a wall-clock cap: a large file that streams slowly but steadily (e.g. a
# multi-volume dictionary over a slow link) never trips it, so `r.content`
# blocks for hours and wedges the whole run (issue #969). Stream the body
# instead and abort on either a total-time deadline or a max-bytes cap.
MAX_PDF_BYTES = 80 * 1024 * 1024   # 80 MB — skip anything larger
PDF_DEADLINE_SECONDS = 180         # hard wall-clock cap per file

# Legislation category pages to scrape: (path, category, doc_type_hint)
LEGISLATION_PAGES = [
    ("Legislation/TaxCode", "tax_code", "Tax Code"),
    ("Legislation/Laws", "law", "Law"),
    ("Legislation/Constitution", "constitution", "Constitution"),
    ("Legislation/Decrees", "decree", "Decree"),
    ("Legislation/Resolutions", "resolution", "Resolution"),
    ("Legislation/InternationalAgreements", "international_agreement", "International Agreement"),
    ("Legislation/OtherCodes", "code", "Code"),
    ("Legislation/CommitteeOrders", "committee_order", "Committee Order"),
]

# Directories to skip (non-legislation PDFs)
SKIP_DIRS = {"bezovtstvennie", "doljniki", "kumita", "savolho"}


def _get_session():
    """Create a requests session with SSL verification disabled."""
    import requests
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    session = requests.Session()
    session.verify = False
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
        "Accept-Language": "en,ru,tg",
    })
    return session


def _scrape_pdf_links(session, page_path: str) -> List[str]:
    """Scrape PDF links from a legislation page."""
    url = f"{BASE_URL}/{page_path}"
    try:
        time.sleep(DELAY)
        r = session.get(url, timeout=30)
        if r.status_code != 200:
            logger.warning("HTTP %d for %s", r.status_code, url)
            return []
    except Exception as e:
        logger.warning("Failed to fetch %s: %s", url, e)
        return []

    # Find all PDF href links
    pdf_links = re.findall(r'href="([^"]*\.pdf)"', r.text, re.IGNORECASE)

    results = []
    for link in pdf_links:
        # Skip non-legislation directories
        if any(skip_dir in link.lower() for skip_dir in SKIP_DIRS):
            continue

        # Resolve relative URLs
        if link.startswith("../"):
            resolved = urljoin(url, link)
        elif link.startswith("/"):
            resolved = BASE_URL + link
        elif link.startswith("\\"):
            # Handle Windows-style backslash paths
            forward = link.replace("\\", "/")
            resolved = BASE_URL + ("" if forward.startswith("/") else "/") + forward
        elif link.startswith("http"):
            resolved = link
        else:
            resolved = urljoin(url, link)

        results.append(resolved)

    return results


def _title_from_filename(filename: str) -> str:
    """Extract a readable title from a PDF filename."""
    # Remove .pdf extension
    name = re.sub(r'\.pdf$', '', filename, flags=re.IGNORECASE)
    # URL decode
    name = unquote(name)
    # Remove common prefixes like l_№XX_
    name = re.sub(r'^l_[№#]\d+_', '', name)
    name = re.sub(r'^Code_[№#]\d+__', '', name)
    # Replace underscores and hyphens with spaces
    name = name.replace('_', ' ').replace('-', ' ')
    # Clean up multiple spaces
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def _make_id(url: str) -> str:
    """Create a stable document ID from URL."""
    # Extract path after /docs/
    match = re.search(r'/docs/(.+)$', url)
    if match:
        path = unquote(match.group(1))
    else:
        path = unquote(url.split('/')[-1])
    slug = re.sub(r'[^a-zA-Z0-9]+', '_', path).strip('_').lower()
    slug = re.sub(r'_pdf$', '', slug)
    return f"TJ_andoz_{slug}"[:120]


def _download_pdf(session, url: str) -> Optional[bytes]:
    """Download a PDF from andoz.tj.

    Streams the body so a single slow/oversized file cannot wedge the run:
    aborts if the download exceeds PDF_DEADLINE_SECONDS of wall-clock time or
    MAX_PDF_BYTES of data (issue #969).
    """
    for attempt in range(3):
        try:
            time.sleep(DELAY)
            with session.get(url, timeout=60, stream=True) as r:
                if r.status_code != 200:
                    logger.warning("PDF download attempt %d: HTTP %d for %s",
                                   attempt + 1, r.status_code, url)
                    if attempt < 2:
                        time.sleep(3)
                    continue

                # Skip if the server advertises an oversized file upfront.
                clen = r.headers.get("Content-Length")
                if clen and clen.isdigit() and int(clen) > MAX_PDF_BYTES:
                    logger.warning("Skip oversized PDF (%s bytes): %s", clen, url)
                    return None

                start = time.monotonic()
                chunks = bytearray()
                aborted = False
                for chunk in r.iter_content(chunk_size=65536):
                    if not chunk:
                        continue
                    chunks.extend(chunk)
                    if len(chunks) > MAX_PDF_BYTES:
                        logger.warning("Abort oversized PDF (>%d bytes): %s",
                                       MAX_PDF_BYTES, url)
                        aborted = True
                        break
                    if time.monotonic() - start > PDF_DEADLINE_SECONDS:
                        logger.warning("Abort slow PDF (>%ds): %s",
                                       PDF_DEADLINE_SECONDS, url)
                        aborted = True
                        break

                if aborted:
                    return None
                if len(chunks) > 200:
                    return bytes(chunks)
                logger.warning("PDF download attempt %d: %d bytes for %s",
                               attempt + 1, len(chunks), url)
        except Exception as e:
            logger.warning("PDF download attempt %d: %s for %s", attempt + 1, e, url)
        if attempt < 2:
            time.sleep(3)
    return None


class AndozTaxLawsScraper(BaseScraper):
    """Scraper for TJ/Andoz-TaxLaws."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("doc_id", ""),
            "_source": "TJ/Andoz-TaxLaws",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "category": raw.get("category", ""),
            "language": raw.get("language", "tg"),
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        session = _get_session()
        seen_urls = set()
        count = 0

        for page_path, category, category_label in LEGISLATION_PAGES:
            if max_records and count >= max_records:
                return

            logger.info("Scraping page: %s (%s)", page_path, category_label)
            pdf_urls = _scrape_pdf_links(session, page_path)
            logger.info("Found %d PDF links on %s", len(pdf_urls), page_path)

            for url in pdf_urls:
                if max_records and count >= max_records:
                    return

                # Deduplicate across pages
                normalized_url = unquote(url).lower()
                if normalized_url in seen_urls:
                    continue
                seen_urls.add(normalized_url)

                filename = unquote(url.split('/')[-1])
                title = _title_from_filename(filename)
                doc_id = _make_id(url)

                # Detect language
                lang = "tg"
                if "_en.pdf" in filename.lower() or "_eng_" in filename.lower() or "ENG" in filename:
                    lang = "en"
                elif "_ru" in filename.lower():
                    lang = "ru"

                # Try to extract year from filename
                year_match = re.search(r'(19|20)\d{2}', filename)
                date = f"{year_match.group()}-01-01" if year_match else None

                logger.info("Downloading [%d]: %s", count + 1, title[:60])
                pdf_bytes = _download_pdf(session, url)
                if pdf_bytes is None:
                    logger.warning("Failed to download: %s", filename)
                    continue
                if not pdf_bytes[:5].startswith(b"%PDF"):
                    logger.warning("Not a PDF: %s", filename)
                    continue

                try:
                    text = extract_pdf_markdown(
                        source="TJ/Andoz-TaxLaws",
                        source_id=doc_id,
                        pdf_bytes=pdf_bytes,
                    )
                except Exception as e:
                    logger.warning("PDF extraction failed for %s: %s", filename, e)
                    continue

                if not text or len(text) < 50:
                    logger.warning("Insufficient text (%d chars), skipping: %s",
                                   len(text or ""), title)
                    continue

                raw = {
                    "doc_id": doc_id,
                    "title": title,
                    "text": text,
                    "date": date,
                    "url": url,
                    "category": category,
                    "language": lang,
                }
                count += 1
                yield raw

        logger.info("Completed: %d documents with extractable text", count)

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def test(self) -> bool:
        logger.info("Testing access to andoz.tj...")
        session = _get_session()
        pdf_bytes = _download_pdf(session, f"{BASE_URL}/docs/kodex/Kodex_14_05_2025_Nav_ENG_en.pdf")
        if pdf_bytes and len(pdf_bytes) > 200:
            logger.info("PDF download OK: %d bytes", len(pdf_bytes))
            return True
        logger.error("Cannot download PDFs from andoz.tj")
        return False


def main():
    parser = argparse.ArgumentParser(description="TJ/Andoz-TaxLaws data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = AndozTaxLawsScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        scraper.bootstrap(sample_mode=args.sample, sample_size=15)
    elif args.command == "update":
        scraper.bootstrap(sample_mode=False)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
