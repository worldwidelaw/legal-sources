#!/usr/bin/env python3
"""
KW/CBK -- Central Bank of Kuwait Regulations & Instructions

Downloads ~80+ PDF documents from CBK's "Legislation and Regulation" pages,
covering conventional banks, Islamic banks, finance companies, exchange
companies, credit information companies, and e-payment service providers.

Strategy:
  - Scrape 6 category index pages to extract PDF links + titles
  - Download each PDF and extract text via common/pdf_extract
  - No auth required. All documents publicly available in English.

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
logger = logging.getLogger("legal-data-hunter.KW.CBK")

BASE_URL = "https://www.cbk.gov.kw"

# Category pages to scrape — (path, section_label)
PAGES = [
    (
        "/en/legislation-and-regulation/cbk-regulations-and-instructions/instructions-to-conventional-banks",
        "conventional_banks",
    ),
    (
        "/en/supervision/cbk-regulations-and-instructions/instructions-for-islamic-banks",
        "islamic_banks",
    ),
    (
        "/en/legislation-and-regulation/cbk-regulations-and-instructions/instructions-for-finance-companies",
        "finance_companies",
    ),
    (
        "/en/supervision/cbk-regulations-and-instructions/instructions-for-exchange-companies",
        "exchange_companies",
    ),
    (
        "/en/legislation-and-regulation/cbk-regulations-and-instructions/Instructions-for-Credit-Information-Companies",
        "credit_information",
    ),
    (
        "/en/legislation-and-regulation/cbk-regulations-and-instructions/regulatory-instructions-e-payment-services",
        "epayment_services",
    ),
]

PDF_LINK_RE = re.compile(
    r'<a[^>]*href\s*=\s*["\']([^"\']*\.pdf)["\'][^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)


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
    """Extract PDF links and titles from an HTML page."""
    results = []
    seen = set()

    for m in PDF_LINK_RE.finditer(html):
        href = m.group(1).strip()
        link_text = re.sub(r"<[^>]+>", "", m.group(2)).strip()
        pdf_url = urljoin(page_url, href)

        if pdf_url in seen:
            continue
        seen.add(pdf_url)

        if not link_text or len(link_text) < 3:
            filename = unquote(pdf_url.split("/")[-1])
            link_text = filename.replace(".pdf", "").replace("_", " ").replace("-", " ")

        results.append({"pdf_url": pdf_url, "title": link_text})

    return results


class CBKScraper(BaseScraper):
    """
    Scraper for KW/CBK — Central Bank of Kuwait Regulations.
    Country: KW
    URL: https://www.cbk.gov.kw

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = _make_session()

    def _scrape_all_pdf_links(self) -> list[dict]:
        """Scrape all category pages and collect PDF links."""
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
        """Download a PDF and extract its text."""
        try:
            resp = self.session.get(pdf_url, timeout=120)
            if resp.status_code != 200:
                logger.debug(f"HTTP {resp.status_code} downloading {pdf_url}")
                return None
            if len(resp.content) < 100:
                return None

            filename = unquote(pdf_url.split("/")[-1]).replace(".pdf", "")
            source_id = f"{section}-{filename}" if section else filename

            text = extract_pdf_markdown(
                "KW/CBK",
                source_id,
                pdf_bytes=resp.content,
                table="legislation",
            )
            if text and len(text.strip()) > 50:
                return text.strip()
            return None
        except Exception as e:
            logger.debug(f"PDF extract failed for {pdf_url}: {e}")
            return None

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw record into standard schema."""
        text = raw.get("_text", "")
        if not text or len(text) < 50:
            return None

        pdf_url = raw.get("pdf_url", "")
        filename = unquote(pdf_url.split("/")[-1]).replace(".pdf", "")
        # Extract tcm10-NNNNN as stable ID
        tcm_match = re.search(r"tcm10-(\d+)", filename)
        doc_id = f"KW-CBK-{tcm_match.group(1)}" if tcm_match else f"KW-CBK-{filename}"

        return {
            "_id": doc_id,
            "_source": "KW/CBK",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", filename),
            "text": text,
            "date": None,
            "url": pdf_url,
            "section": raw.get("section", ""),
            "source_page": raw.get("source_page", ""),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all CBK regulation documents with full text."""
        links = self._scrape_all_pdf_links()
        yielded = 0
        skipped = 0

        for i, link in enumerate(links):
            logger.info(f"[{i+1}/{len(links)}] Downloading {link['pdf_url']}")
            time.sleep(1.5)

            text = self._download_and_extract(link["pdf_url"], link.get("section", ""))
            if not text:
                logger.debug("  No text extracted, skipping")
                skipped += 1
                continue

            yielded += 1
            yield {
                "pdf_url": link["pdf_url"],
                "title": link["title"],
                "section": link["section"],
                "source_page": link["source_page"],
                "_text": text,
            }

        logger.info(f"Done: {yielded} documents with text, {skipped} skipped")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """No date filtering — yields all."""
        yield from self.fetch_all()


# -- CLI ----------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="KW/CBK Data Fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CBKScraper()

    if args.command == "test-api":
        logger.info("Testing cbk.gov.kw connectivity...")
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
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
