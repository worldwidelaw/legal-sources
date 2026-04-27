#!/usr/bin/env python3
"""
KW/MOF-TaxGuidance -- Kuwait Ministry of Finance Tax Guidance

Scrapes tax laws, decree-laws, ministerial decisions, circulars,
guidance manuals, DMTT legislation, and FATCA/CRS docs from mof.gov.kw.

Strategy:
  - Scrape ~10 ASPX pages to extract PDF links + titles
  - Download each PDF and extract text via common/pdf_extract
  - ~252 PDFs total. No auth required.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import re
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KW.MOF-TaxGuidance")

BASE_URL = "https://www.mof.gov.kw"

# Pages to scrape with their section labels
PAGES = [
    ("/MOFDesicions/mofDesicions1.aspx", "laws"),
    ("/MOFDesicions/mofDesicions2.aspx", "decree_laws"),
    ("/MOFDesicions/mofDesicions3.aspx", "ministerial_decisions"),
    ("/MOFDesicions/mofDesicions4.aspx", "circulars"),
    ("/MOFDesicions/mofDesicions5.aspx", "periodic_letters"),
    ("/MOFDesicions/mofDesicions6.aspx", "guidance_manuals"),
    ("/MOFServices/CompServ1.aspx", "corporate_tax_procedures"),
    ("/MOFServices/CompServ3.aspx", "institutional_tax_procedures"),
    ("/MOFServices/DMTTLegislation.aspx", "dmtt_legislation"),
    ("/FATCA.aspx", "fatca_crs"),
    ("/MOFAgreements/mofCRS.aspx", "crs"),
]


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0 (compatible; LegalDataHunter/1.0)"
    s.headers["Accept"] = "text/html,application/xhtml+xml,*/*"
    retry = Retry(total=3, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def _extract_pdf_links(html: str, page_url: str) -> list[dict]:
    """Extract PDF links and their surrounding text context from HTML."""
    results = []
    seen_urls = set()

    # Find all <a> tags with href ending in .pdf (case-insensitive)
    pattern = re.compile(
        r'<a[^>]*href\s*=\s*["\']([^"\']*\.pdf)["\'][^>]*>(.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )

    for match in pattern.finditer(html):
        href = match.group(1).strip()
        link_text = re.sub(r"<[^>]+>", "", match.group(2)).strip()

        # Resolve relative URL
        pdf_url = urljoin(page_url, href)

        if pdf_url in seen_urls:
            continue
        seen_urls.add(pdf_url)

        # Use link text as title, or derive from filename
        if not link_text or len(link_text) < 3:
            filename = unquote(pdf_url.split("/")[-1])
            link_text = filename.replace(".pdf", "").replace("_", " ")

        results.append({
            "pdf_url": pdf_url,
            "title": link_text,
        })

    return results


class KuwaitMOFScraper(BaseScraper):
    """
    Scraper for KW/MOF-TaxGuidance -- Kuwait Ministry of Finance Tax Guidance.
    Country: KW
    URL: https://www.mof.gov.kw

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = _make_session()

    def _scrape_all_pdf_links(self) -> list[dict]:
        """Scrape all pages and collect PDF links with metadata."""
        all_links = []
        seen_urls = set()

        for path, section in PAGES:
            page_url = BASE_URL + path
            logger.info(f"Scraping {section}: {page_url}")
            try:
                resp = self.session.get(page_url, timeout=30)
                if resp.status_code != 200:
                    logger.warning(f"HTTP {resp.status_code} for {page_url}")
                    continue

                links = _extract_pdf_links(resp.text, page_url)
                for link in links:
                    if link["pdf_url"] not in seen_urls:
                        seen_urls.add(link["pdf_url"])
                        link["section"] = section
                        link["source_page"] = page_url
                        all_links.append(link)

                logger.info(f"  Found {len(links)} PDFs in {section}")
                time.sleep(1.0)
            except Exception as e:
                logger.error(f"Failed to scrape {page_url}: {e}")

        logger.info(f"Total unique PDFs found: {len(all_links)}")
        return all_links

    def _download_and_extract(self, pdf_url: str, section: str = "") -> Optional[str]:
        """Download PDF and extract text."""
        try:
            resp = self.session.get(pdf_url, timeout=120)
            if resp.status_code != 200:
                logger.debug(f"HTTP {resp.status_code} downloading {pdf_url}")
                return None
            if len(resp.content) < 100:
                return None
            # Derive source_id from filename
            filename = unquote(pdf_url.split("/")[-1]).replace(".pdf", "")
            source_id = f"{section}-{filename}" if section else filename
            text = extract_pdf_markdown(
                source="KW/MOF-TaxGuidance",
                source_id=source_id,
                pdf_bytes=resp.content,
                table="doctrine",
            )
            if text and len(text.strip()) > 50:
                return text.strip()
            return None
        except Exception as e:
            logger.debug(f"PDF download/extract failed for {pdf_url}: {e}")
            return None

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw record into standard schema."""
        text = raw.get("_text", "")
        if not text or len(text) < 50:
            return None

        pdf_url = raw.get("pdf_url", "")
        # Create a stable ID from the PDF filename
        filename = unquote(pdf_url.split("/")[-1]).replace(".pdf", "")
        doc_id = f"KW-MOF-{filename}"

        return {
            "_id": doc_id,
            "_source": "KW/MOF-TaxGuidance",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", filename),
            "text": text,
            "date": None,
            "url": pdf_url,
            "section": raw.get("section", ""),
            "source_page": raw.get("source_page", ""),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all MOF tax documents with full text."""
        links = self._scrape_all_pdf_links()
        total_yielded = 0
        total_skipped = 0

        for i, link in enumerate(links):
            logger.info(f"[{i+1}/{len(links)}] Downloading {link['pdf_url']}")
            time.sleep(1.5)

            text = self._download_and_extract(link["pdf_url"], link.get("section", ""))
            if not text:
                logger.debug(f"  No text extracted, skipping")
                total_skipped += 1
                continue

            total_yielded += 1
            yield {
                "pdf_url": link["pdf_url"],
                "title": link["title"],
                "section": link["section"],
                "source_page": link["source_page"],
                "_text": text,
            }

        logger.info(f"Done: {total_yielded} documents with text, {total_skipped} skipped")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """No date filtering available — yields all."""
        yield from self.fetch_all()


# -- CLI ----------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="KW/MOF-TaxGuidance Data Fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = KuwaitMOFScraper()

    if args.command == "test-api":
        logger.info("Testing mof.gov.kw connectivity...")
        links = scraper._scrape_all_pdf_links()
        if links:
            logger.info(f"Found {len(links)} PDFs total")
            logger.info(f"Testing PDF download: {links[0]['pdf_url']}")
            text = scraper._download_and_extract(links[0]["pdf_url"])
            if text:
                logger.info(f"PDF text extracted: {len(text)} chars")
                logger.info(f"Preview: {text[:200]}...")
            else:
                logger.error("PDF text extraction failed")
        else:
            logger.error("No PDF links found")
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
