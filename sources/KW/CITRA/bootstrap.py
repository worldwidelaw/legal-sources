#!/usr/bin/env python3
"""
KW/CITRA -- Kuwait Communication & IT Regulatory Authority

Downloads ~50 PDF regulatory documents from CITRA's SharePoint-based
regulations listing and law directory. Extracts text from text-based PDFs
and skips scanned-image PDFs (which yield 0 chars without OCR).

Strategy:
  - Scrape paginated regulations listing via SharePoint __doPostBack
  - Also include LawofCITRA directory documents
  - Download each PDF and extract text via common/pdf_extract
  - Skip scanned PDFs that produce no extractable text

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
from urllib.parse import urljoin, unquote, quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KW.CITRA")

BASE_URL = "https://www.citra.gov.kw"
REGULATIONS_URL = f"{BASE_URL}/sites/en/Pages/regulations.aspx"

# Additional known law directory PDFs not listed on the regulations page
LAW_PDFS = [
    {
        "pdf_url": f"{BASE_URL}/sites/en/LawofCITRA/Law%20No.%2037-%202014.pdf",
        "title": "Law No. 37 of 2014 — Establishment of CITRA",
        "section": "LawofCITRA",
    },
    {
        "pdf_url": f"{BASE_URL}/sites/en/LawofCITRA/regulations.pdf",
        "title": "Executive Regulations of CITRA Law",
        "section": "LawofCITRA",
    },
]


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0 (compatible; LegalDataHunter/1.0)"
    s.headers["Accept"] = "text/html,application/xhtml+xml,*/*"
    s.verify = False
    retry = Retry(total=3, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def _extract_pdf_links_from_page(soup: BeautifulSoup) -> list[dict]:
    """Extract PDF links from a regulations page."""
    results = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".pdf" not in href.lower():
            continue

        pdf_url = urljoin(BASE_URL, href)
        if pdf_url in seen:
            continue
        seen.add(pdf_url)

        # Derive title from filename
        filename = unquote(pdf_url.split("/")[-1]).replace(".pdf", "")
        title = filename.replace("_", " ").replace("-", " ")
        # Clean up encoded chars
        title = re.sub(r"\s+", " ", title).strip()

        section = "LegalReferences"
        if "/LawofCITRA/" in pdf_url:
            section = "LawofCITRA"

        results.append({
            "pdf_url": pdf_url,
            "title": title,
            "section": section,
        })

    return results


class CITRAScraper(BaseScraper):
    """
    Scraper for KW/CITRA — CITRA Regulations & Decisions.
    Country: KW
    URL: https://www.citra.gov.kw

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = _make_session()

    def _scrape_all_pdf_links(self) -> list[dict]:
        """Scrape all pages of the regulations listing."""
        all_links = []
        seen_urls = set()

        # Page 1
        logger.info(f"Fetching regulations page 1: {REGULATIONS_URL}")
        resp = self.session.get(REGULATIONS_URL, timeout=30)
        if resp.status_code != 200:
            logger.error(f"HTTP {resp.status_code} for {REGULATIONS_URL}")
            return all_links

        soup = BeautifulSoup(resp.text, "html.parser")
        page1_links = _extract_pdf_links_from_page(soup)
        for link in page1_links:
            if link["pdf_url"] not in seen_urls:
                seen_urls.add(link["pdf_url"])
                all_links.append(link)
        logger.info(f"  Page 1: {len(page1_links)} PDFs")

        # Extract form data for pagination
        viewstate_el = soup.find("input", {"name": "__VIEWSTATE"})
        ev_el = soup.find("input", {"name": "__EVENTVALIDATION"})
        rd_el = soup.find("input", {"name": "__REQUESTDIGEST"})

        viewstate = viewstate_el["value"] if viewstate_el else ""
        eventvalidation = ev_el["value"] if ev_el else ""
        requestdigest = rd_el["value"] if rd_el else ""

        # Navigate subsequent pages
        paging_links = soup.find_all(
            "a", href=lambda h: h and "__doPostBack" in str(h)
        )

        for pg_link in paging_links:
            pg_text = pg_link.get_text(strip=True)
            href = pg_link["href"]
            match = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", href)
            if not match:
                continue

            event_target = match.group(1)
            event_arg = match.group(2)

            data = {
                "__EVENTTARGET": event_target,
                "__EVENTARGUMENT": event_arg,
                "__VIEWSTATE": viewstate,
                "__EVENTVALIDATION": eventvalidation,
                "__REQUESTDIGEST": requestdigest,
            }

            logger.info(f"Fetching regulations page {pg_text}")
            try:
                r2 = self.session.post(REGULATIONS_URL, data=data, timeout=30)
                if r2.status_code != 200:
                    logger.warning(f"HTTP {r2.status_code} for page {pg_text}")
                    continue

                soup2 = BeautifulSoup(r2.text, "html.parser")
                page_links = _extract_pdf_links_from_page(soup2)
                new_count = 0
                for link in page_links:
                    if link["pdf_url"] not in seen_urls:
                        seen_urls.add(link["pdf_url"])
                        all_links.append(link)
                        new_count += 1
                logger.info(f"  Page {pg_text}: {new_count} new PDFs")

                # Update form state for next page
                vs2 = soup2.find("input", {"name": "__VIEWSTATE"})
                ev2 = soup2.find("input", {"name": "__EVENTVALIDATION"})
                rd2 = soup2.find("input", {"name": "__REQUESTDIGEST"})
                if vs2:
                    viewstate = vs2["value"]
                if ev2:
                    eventvalidation = ev2["value"]
                if rd2:
                    requestdigest = rd2["value"]

                time.sleep(1.0)
            except Exception as e:
                logger.error(f"Failed to fetch page {pg_text}: {e}")

        # Add law directory PDFs
        for law in LAW_PDFS:
            if law["pdf_url"] not in seen_urls:
                seen_urls.add(law["pdf_url"])
                all_links.append(law)

        logger.info(f"Total unique PDFs found: {len(all_links)}")
        return all_links

    def _download_and_extract(self, pdf_url: str) -> Optional[str]:
        """Download a PDF and extract its text. Returns None for scanned PDFs."""
        try:
            resp = self.session.get(pdf_url, timeout=120)
            if resp.status_code != 200:
                logger.debug(f"HTTP {resp.status_code} downloading {pdf_url}")
                return None
            if len(resp.content) < 100:
                return None

            filename = unquote(pdf_url.split("/")[-1]).replace(".pdf", "")

            text = extract_pdf_markdown(
                "KW/CITRA",
                filename,
                pdf_bytes=resp.content,
                table="doctrine",
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
        # Create stable ID from filename
        safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", filename)[:120]
        doc_id = f"KW-CITRA-{safe_id}"

        # Try to extract date from title/filename
        date = None
        date_match = re.search(r"\b(20\d{2})\b", raw.get("title", ""))
        if date_match:
            date = f"{date_match.group(1)}-01-01"

        return {
            "_id": doc_id,
            "_source": "KW/CITRA",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", filename),
            "text": text,
            "date": date,
            "url": pdf_url,
            "section": raw.get("section", ""),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all CITRA regulation documents with full text."""
        links = self._scrape_all_pdf_links()
        yielded = 0
        skipped = 0

        for i, link in enumerate(links):
            logger.info(f"[{i+1}/{len(links)}] Downloading {link['pdf_url']}")
            time.sleep(1.5)

            text = self._download_and_extract(link["pdf_url"])
            if not text:
                logger.debug("  No text extracted (scanned PDF or error), skipping")
                skipped += 1
                continue

            yielded += 1
            yield {
                "pdf_url": link["pdf_url"],
                "title": link["title"],
                "section": link.get("section", ""),
                "_text": text,
            }

        logger.info(f"Done: {yielded} documents with text, {skipped} skipped (scanned/errors)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """No date filtering — yields all."""
        yield from self.fetch_all()


# -- CLI ----------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="KW/CITRA Data Fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CITRAScraper()

    if args.command == "test-api":
        logger.info("Testing citra.gov.kw connectivity...")
        links = scraper._scrape_all_pdf_links()
        if links:
            logger.info(f"Found {len(links)} PDFs total")
            logger.info(f"Testing PDF download: {links[0]['pdf_url']}")
            text = scraper._download_and_extract(links[0]["pdf_url"])
            if text:
                logger.info(f"PDF text extracted: {len(text)} chars")
                logger.info(f"Preview: {text[:200]}...")
            else:
                logger.warning("First PDF yielded no text (may be scanned)")
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
