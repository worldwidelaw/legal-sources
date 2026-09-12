#!/usr/bin/env python3
"""
TJ/NBT-Regulations -- National Bank of Tajikistan — Banking Laws & Regulations

Fetches banking laws, regulations, instructions, and normative acts from nbt.tj.
PDFs are scraped from multiple /tj/, /ru/, and /en/ section pages.

Strategy:
  1. Scrape each section page for PDF links
  2. Download each PDF and extract full text via common.pdf_extract
  3. Skip scanned/image-only PDFs that yield no extractable text
  4. Deduplicate by PDF path across pages and languages

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
from typing import Any, Dict, Generator, List, Optional
from urllib.parse import quote, unquote, urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TJ.NBT-Regulations")

BASE_URL = "https://www.nbt.tj"
DELAY = 2.0

# Section pages to scrape: (path, category, doc_type_hint)
# We scrape both Tajik and English where available for broader coverage
SECTION_PAGES = [
    ("/tj/laws/", "banking_law", "Banking Law"),
    ("/tj/normative/", "instruction", "Instruction/Regulation"),
    ("/tj/sugurta/konunho_sugurta.php", "insurance_law", "Insurance Law"),
    ("/tj/islamic_banking/", "islamic_banking", "Islamic Banking Regulation"),
    ("/tj/regulatory_legal/index.php", "payment_regulation", "Payment Services Regulation"),
    ("/tj/projects/", "draft_act", "Draft Legal Act"),
    ("/tj/documents/", "chart_of_accounts", "Chart of Accounts"),
    ("/ru/laws/", "banking_law", "Banking Law (RU)"),
    ("/ru/normative/", "instruction", "Instruction/Regulation (RU)"),
    ("/ru/sugurta/konunho_sugurta.php", "insurance_law", "Insurance Law (RU)"),
    ("/ru/islamic_banking/", "islamic_banking", "Islamic Banking Regulation (RU)"),
    ("/ru/regulatory_legal/index.php", "payment_regulation", "Payment Services Regulation (RU)"),
    ("/en/laws/", "banking_law", "Banking Law (EN)"),
    ("/en/normative/", "instruction", "Instruction/Regulation (EN)"),
    ("/en/sugurta/konunho_sugurta.php", "insurance_law", "Insurance Law (EN)"),
    ("/en/islamic_banking/", "islamic_banking", "Islamic Banking Regulation (EN)"),
    ("/en/regulatory_legal/index.php", "payment_regulation", "Payment Services Regulation (EN)"),
]


def _get_session():
    """Create a requests session."""
    import requests
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
        "Accept-Language": "en,ru,tg",
    })
    return session


def _scrape_pdf_links(session, page_path: str) -> List[str]:
    """Scrape PDF links from a section page."""
    url = BASE_URL + page_path
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
        if link.startswith("http"):
            resolved = link
        elif link.startswith("/"):
            resolved = BASE_URL + link
        else:
            resolved = urljoin(url, link)
        results.append(resolved)

    return results


def _title_from_filename(filename: str) -> str:
    """Extract a readable title from a PDF filename."""
    name = re.sub(r'\.pdf$', '', filename, flags=re.IGNORECASE)
    name = unquote(name)
    name = name.replace('_', ' ').replace('-', ' ')
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def _make_id(url: str) -> str:
    """Create a stable document ID from URL."""
    match = re.search(r'/upload/iblock/([a-f0-9]+)/', url)
    if match:
        block_id = match.group(1)
        filename = unquote(url.split('/')[-1])
        slug = re.sub(r'[^a-zA-Z0-9]+', '_', filename).strip('_').lower()
        slug = re.sub(r'_pdf$', '', slug)
        return f"TJ_nbt_{block_id}_{slug}"[:120]
    filename = unquote(url.split('/')[-1])
    slug = re.sub(r'[^a-zA-Z0-9]+', '_', filename).strip('_').lower()
    slug = re.sub(r'_pdf$', '', slug)
    return f"TJ_nbt_{slug}"[:120]


def _download_pdf(session, url: str) -> Optional[bytes]:
    """Download a PDF from nbt.tj."""
    for attempt in range(3):
        try:
            time.sleep(DELAY)
            # URL-encode the path portion for Cyrillic filenames
            r = session.get(url, timeout=60)
            if r.status_code == 200 and len(r.content) > 200:
                return r.content
            logger.warning("PDF download attempt %d: HTTP %d, %d bytes for %s",
                           attempt + 1, r.status_code, len(r.content), url)
        except Exception as e:
            logger.warning("PDF download attempt %d: %s for %s", attempt + 1, e, url)
        if attempt < 2:
            time.sleep(3)
    return None


def _detect_language(url: str, filename: str) -> str:
    """Detect language from URL path and filename."""
    if "/en/" in url or "_eng" in filename.lower() or "_en." in filename.lower():
        return "en"
    if "/ru/" in url or "_rus" in filename.lower() or "_ru." in filename.lower():
        return "ru"
    return "tg"


class NBTRegulationsScraper(BaseScraper):
    """Scraper for TJ/NBT-Regulations."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("doc_id", ""),
            "_source": "TJ/NBT-Regulations",
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
        seen_paths = set()  # Deduplicate by /upload/iblock/... path
        count = 0

        for page_path, category, category_label in SECTION_PAGES:
            if max_records and count >= max_records:
                return

            logger.info("Scraping page: %s (%s)", page_path, category_label)
            pdf_urls = _scrape_pdf_links(session, page_path)
            logger.info("Found %d PDF links on %s", len(pdf_urls), page_path)

            for url in pdf_urls:
                if max_records and count >= max_records:
                    return

                # Deduplicate by PDF path (same PDF may appear on /tj/ and /en/)
                path_match = re.search(r'/upload/iblock/.+$', url)
                dedup_key = path_match.group() if path_match else url
                if dedup_key.lower() in seen_paths:
                    continue
                seen_paths.add(dedup_key.lower())

                filename = unquote(url.split('/')[-1])
                title = _title_from_filename(filename)
                doc_id = _make_id(url)
                lang = _detect_language(url, filename)

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
                        source="TJ/NBT-Regulations",
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

                # Try to extract year from filename
                year_match = re.search(r'(19|20)\d{2}', filename)
                date = f"{year_match.group()}-01-01" if year_match else None

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
        logger.info("Testing access to nbt.tj...")
        session = _get_session()
        pdf_bytes = _download_pdf(session, f"{BASE_URL}/upload/iblock/e8c/Zakon_nbt_eng_2018.pdf")
        if pdf_bytes and len(pdf_bytes) > 200:
            logger.info("PDF download OK: %d bytes", len(pdf_bytes))
            return True
        logger.error("Cannot download PDFs from nbt.tj")
        return False


def main():
    parser = argparse.ArgumentParser(description="TJ/NBT-Regulations data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NBTRegulationsScraper()

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
