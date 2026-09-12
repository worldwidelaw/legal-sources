#!/usr/bin/env python3
"""
TM/TaxGov-Legislation -- Turkmenistan State Tax Directorate — Normative Legal Acts

Fetches tax laws, codes, decrees, and regulations from tax.gov.tm.
PDFs are scraped from three tab panels on the homepage: Kanunlar (Laws),
Düzgünnamalar (Regulations), Görkezmeler (Decrees/Instructions).

Strategy:
  1. Scrape the homepage for tab-panel PDF links with their titles
  2. Download each PDF and extract full text via common.pdf_extract
  3. Deduplicate by PDF URL

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap --full
  python bootstrap.py test
"""

import argparse
import html as html_mod
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional
from urllib.parse import unquote, urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TM.TaxGov-Legislation")

BASE_URL = "https://tax.gov.tm"
DELAY = 2.0

# Tab panel IDs on homepage → category
TAB_PANELS = [
    ("kanunlar", "law"),
    ("düzgünnamalar", "regulation"),
    ("görkezmeler", "decree"),
]


def _get_session():
    """Create a requests session."""
    import requests
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
        "Accept-Language": "tk,en",
    })
    return session


def _scrape_homepage_pdfs(session) -> List[dict]:
    """Scrape all PDF links with titles from the homepage tab panels."""
    url = BASE_URL + "/"
    try:
        r = session.get(url, timeout=30)
        if r.status_code != 200:
            logger.warning("HTTP %d for %s", r.status_code, url)
            return []
    except Exception as e:
        logger.warning("Failed to fetch %s: %s", url, e)
        return []

    page_text = r.text
    results = []
    seen_urls = set()

    for panel_id, category in TAB_PANELS:
        # Find the panel content
        pattern = rf'id="{re.escape(panel_id)}"[^>]*role="tabpanel"[^>]*>(.*?)(?=<div[^>]*class="tab-pane|$)'
        match = re.search(pattern, page_text, re.DOTALL)
        if not match:
            logger.warning("Tab panel '%s' not found", panel_id)
            continue

        panel_content = match.group(1)

        # Extract PDF links: <a href="...pdf" ...>...title text...</a>
        pdf_links = re.findall(
            r'<a\s+href="(https://tax\.gov\.tm/pdf/[^"]*\.pdf)"[^>]*>(.*?)</a>',
            panel_content, re.DOTALL | re.IGNORECASE
        )

        for pdf_url, inner_html in pdf_links:
            if pdf_url.lower() in seen_urls:
                continue
            seen_urls.add(pdf_url.lower())

            # Clean title from inner HTML
            title = re.sub(r'<[^>]+>', ' ', inner_html)
            title = html_mod.unescape(title)
            title = re.sub(r'\s+', ' ', title).strip()
            # Remove "PDF / X.XX MB" prefix
            title = re.sub(r'^-->\s*', '', title)
            title = re.sub(r'^PDF\s*/\s*[\d.]*\s*MB\s*', '', title).strip()

            if not title:
                title = _title_from_filename(pdf_url)

            results.append({
                "url": pdf_url,
                "title": title,
                "category": category,
            })

        logger.info("Panel '%s' (%s): %d PDFs", panel_id, category,
                     sum(1 for r in results if r["category"] == category))

    return results


def _title_from_filename(url: str) -> str:
    """Extract a readable title from a PDF URL."""
    filename = unquote(url.split("/")[-1])
    name = re.sub(r'\.pdf$', '', filename, flags=re.IGNORECASE)
    name = name.replace('_', ' ').replace('-', ' ')
    name = re.sub(r'\s+', ' ', name).strip()
    return name if name else "Untitled"


def _make_id(url: str) -> str:
    """Create a stable document ID from URL."""
    match = re.search(r'/attachments/(\d+)/(.+?)\.pdf', url, re.IGNORECASE)
    if match:
        num = match.group(1)
        slug = re.sub(r'[^a-zA-Z0-9]+', '_', unquote(match.group(2))).strip('_').lower()
        return f"TM_taxgov_{num}_{slug}"[:120]
    filename = unquote(url.split("/")[-1])
    slug = re.sub(r'[^a-zA-Z0-9]+', '_', filename).strip('_').lower()
    slug = re.sub(r'_pdf$', '', slug)
    return f"TM_taxgov_{slug}"[:120]


def _download_pdf(session, url: str) -> Optional[bytes]:
    """Download a PDF."""
    for attempt in range(3):
        try:
            time.sleep(DELAY)
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


class TaxGovLegislationScraper(BaseScraper):
    """Scraper for TM/TaxGov-Legislation."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw["doc_id"],
            "_source": "TM/TaxGov-Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "category": raw.get("category", ""),
            "language": "tk",
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        session = _get_session()
        count = 0

        logger.info("Scraping homepage for PDF links...")
        pdf_items = _scrape_homepage_pdfs(session)
        logger.info("Found %d unique PDF documents", len(pdf_items))

        for item in pdf_items:
            if max_records and count >= max_records:
                return

            url = item["url"]
            title = item["title"]
            category = item["category"]
            doc_id = _make_id(url)

            logger.info("Downloading [%d/%d]: %s", count + 1, len(pdf_items), title[:60])
            pdf_bytes = _download_pdf(session, url)
            if pdf_bytes is None:
                logger.warning("Failed to download: %s", url)
                continue
            if not pdf_bytes[:5].startswith(b"%PDF"):
                logger.warning("Not a PDF: %s", url)
                continue

            try:
                text = extract_pdf_markdown(
                    source="TM/TaxGov-Legislation",
                    source_id=doc_id,
                    pdf_bytes=pdf_bytes,
                )
            except Exception as e:
                logger.warning("PDF extraction failed for %s: %s", title[:60], e)
                continue

            if not text or len(text) < 50:
                logger.warning("Insufficient text (%d chars), skipping: %s",
                               len(text or ""), title[:60])
                continue

            # Try to extract date from title
            date_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', title)
            date = None
            if date_match:
                parts = date_match.group(1).split('.')
                date = f"{parts[2]}-{parts[1]}-{parts[0]}"

            raw = {
                "doc_id": doc_id,
                "title": title,
                "text": text,
                "date": date,
                "url": url,
                "category": category,
            }
            count += 1
            yield raw

        logger.info("Completed: %d documents with extractable text", count)

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def test(self) -> bool:
        logger.info("Testing access to tax.gov.tm...")
        session = _get_session()
        pdf_bytes = _download_pdf(session, f"{BASE_URL}/pdf/ckeditor_assets/attachments/9/1.pdf")
        if pdf_bytes and len(pdf_bytes) > 200:
            logger.info("PDF download OK: %d bytes", len(pdf_bytes))
            return True
        logger.error("Cannot download PDFs from tax.gov.tm")
        return False


def main():
    parser = argparse.ArgumentParser(description="TM/TaxGov-Legislation data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = TaxGovLegislationScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        scraper.bootstrap(sample_mode=args.sample, sample_size=20)
    elif args.command == "update":
        scraper.bootstrap(sample_mode=False)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
