#!/usr/bin/env python3
"""
IE/Revenue-TDM -- Irish Revenue Tax and Duty Manuals Fetcher

Fetches Tax and Duty Manuals (TDMs) from Revenue.ie.  These are official
Irish tax guidance documents published as PDFs covering income tax, CGT,
corporation tax, VAT, customs, excise, stamp duty, and more.

Strategy:
  - Recursively crawl the TDM index pages to discover all PDF URLs
  - Filter to current versions only (exclude timestamped archive copies)
  - Download each PDF and extract text with pdfplumber
  - ~1,350 current documents across 15 categories

Usage:
  python bootstrap.py bootstrap          # Full fetch
  python bootstrap.py bootstrap --sample # Fetch 15 samples
  python bootstrap.py test               # Connectivity test
"""

import sys
import json
import logging
import re
import io
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IE.Revenue-TDM")

BASE_URL = "https://www.revenue.ie"
TDM_INDEX = "/en/tax-professionals/tdm/index.aspx"
DELAY = 2.0

CATEGORY_MAP = {
    "appeals": "appeals",
    "capital-acquisitions-tax": "capital_acquisitions_tax",
    "collection": "collection",
    "compliance": "compliance",
    "customs": "customs",
    "excise": "excise",
    "income-tax-capital-gains-tax-corporation-tax": "income_tax",
    "investigations-prosecutions-enforcement": "investigations",
    "local-property-tax": "local_property_tax",
    "pensions": "pensions",
    "powers": "powers",
    "share-schemes": "share_schemes",
    "stamp-duty": "stamp_duty",
    "value-added-tax": "vat",
    "vehicle-registration-tax": "vrt",
}


def extract_pdf_text(pdf_bytes: bytes) -> Optional[str]:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="IE/Revenue-TDM",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="doctrine",
    ) or ""

def url_to_category(url: str) -> str:
    """Derive category from the URL path."""
    path = url.replace(BASE_URL, "").replace("/en/tax-professionals/tdm/", "")
    for key, cat in CATEGORY_MAP.items():
        if path.startswith(key):
            return cat
    return "other"


def url_to_id(url: str) -> str:
    """Derive a stable document ID from the PDF URL."""
    # e.g. /en/tax-professionals/tdm/income-tax-.../part-01/01-00-01.pdf -> 01-00-01
    filename = url.rstrip("/").split("/")[-1]
    return filename.replace(".pdf", "")


class RevenueTDM(BaseScraper):
    SOURCE_ID = "IE/Revenue-TDM"

    def __init__(self):
        self.http = HttpClient(base_url=BASE_URL)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return raw

    def crawl_index_pages(self) -> List[Dict[str, str]]:
        """Recursively crawl TDM index pages to discover all current PDF URLs."""
        visited = set()
        pdf_urls = []

        def crawl(path: str):
            if path in visited:
                return
            visited.add(path)

            url = BASE_URL + path
            try:
                resp = self.http.get(url)
                time.sleep(0.5)
                if not resp or resp.status_code != 200:
                    logger.warning("Failed to fetch index: %s (status %s)",
                                   path, resp.status_code if resp else "None")
                    return
            except Exception as e:
                logger.warning("Error fetching %s: %s", path, e)
                return

            html = resp.text

            # Extract page title for context
            title_match = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL | re.IGNORECASE)
            page_title = re.sub(r'<[^>]+>', '', title_match.group(1)).strip() if title_match else ""

            # Find PDF links (both /tdm/ and /tdm-wm/ paths)
            pdfs = re.findall(r'href="([^"]+\.pdf)"', html)
            for pdf_href in pdfs:
                full_url = pdf_href if pdf_href.startswith("http") else BASE_URL + pdf_href
                # Skip timestamped archive versions (e.g. 01-00-01-20230531121115.pdf)
                if re.search(r'-\d{14}\.pdf$', full_url):
                    continue
                if full_url not in {p["url"] for p in pdf_urls}:
                    # Extract link text for title
                    # Find the <a> tag containing this href
                    link_pattern = re.escape(pdf_href) + r'["\'][^>]*>(.*?)</a>'
                    link_match = re.search(link_pattern, html, re.DOTALL | re.IGNORECASE)
                    link_text = ""
                    if link_match:
                        link_text = re.sub(r'<[^>]+>', '', link_match.group(1)).strip()

                    pdf_urls.append({
                        "url": full_url,
                        "link_text": link_text,
                        "page_title": page_title,
                        "category": url_to_category(full_url),
                    })

            # Find sub-index pages
            sub_pages = re.findall(
                r'href="(/en/tax-professionals/tdm/[^"]+/index\.aspx)"', html
            )
            for sub in set(sub_pages):
                if sub not in visited:
                    crawl(sub)

        crawl(TDM_INDEX)
        logger.info("Crawled %d index pages, found %d current PDFs", len(visited), len(pdf_urls))
        return pdf_urls

    def fetch_pdf(self, url: str) -> Optional[Dict[str, Any]]:
        """Download a PDF and extract its text."""
        try:
            resp = self.http.get(url)
            time.sleep(DELAY)
            if not resp or resp.status_code != 200:
                logger.warning("Failed to download PDF: %s (status %s)",
                               url, resp.status_code if resp else "None")
                return None
        except Exception as e:
            logger.warning("Error downloading %s: %s", url, e)
            return None

        text = extract_pdf_text(resp.content)
        if not text or len(text) < 50:
            logger.warning("No text extracted from %s", url)
            return None

        # Try to extract title from first line of PDF text
        lines = text.strip().split("\n")
        # Skip "Tax and Duty Manual" header line
        pdf_title = ""
        for line in lines[:5]:
            line = line.strip()
            if line and line.lower() != "tax and duty manual" and len(line) > 5:
                pdf_title = line
                break

        return {
            "text": text,
            "pdf_title": pdf_title,
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all TDM documents."""
        pdf_entries = self.crawl_index_pages()
        if not pdf_entries:
            logger.error("No PDF URLs found")
            return

        sample_limit = 15 if sample else len(pdf_entries)
        total_yielded = 0

        for entry in pdf_entries:
            if total_yielded >= sample_limit:
                break

            url = entry["url"]
            result = self.fetch_pdf(url)
            if not result:
                continue

            doc_id = url_to_id(url)
            title = entry["link_text"] or result["pdf_title"] or doc_id

            record = {
                "_id": f"revenue-tdm-{doc_id}",
                "_source": self.SOURCE_ID,
                "_type": "doctrine",
                "_fetched_at": datetime.now(timezone.utc).isoformat(),
                "title": title,
                "text": result["text"],
                "date": None,
                "url": url,
                "language": "en",
                "category": entry["category"],
                "page_title": entry["page_title"],
            }

            yield record
            total_yielded += 1

            if total_yielded % 25 == 0:
                logger.info("  Progress: %d/%d documents", total_yielded, len(pdf_entries))

        logger.info("Fetch complete. %d documents yielded from %d PDFs",
                     total_yielded, len(pdf_entries))

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch updates - re-downloads all since PDFs don't have lastmod dates."""
        yield from self.fetch_all(sample=False)

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            pdf_entries = self.crawl_index_pages()
            if pdf_entries:
                logger.info("Test passed: found %d TDM PDFs", len(pdf_entries))
                # Test one PDF download
                result = self.fetch_pdf(pdf_entries[0]["url"])
                if result and result["text"]:
                    logger.info("Test passed: extracted %d chars from first PDF",
                                len(result["text"]))
                    return True
            logger.error("Test failed: no PDFs found or extraction failed")
            return False
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="IE/Revenue-TDM bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    args = parser.parse_args()

    scraper = RevenueTDM()

    if args.command == "test":
        sys.exit(0 if scraper.test() else 1)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            safe_name = re.sub(r'[^\w\-.]', '_', record['_id'])[:100]
            out_file = sample_dir / f"{safe_name}.json"
            out_file.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
            count += 1
            text_len = len(record.get("text", ""))
            logger.info("  [%d] %s | %s | text=%d chars",
                        count, record["category"], record["title"][:60], text_len)

        logger.info("Bootstrap complete: %d records saved to sample/", count)
        sys.exit(0 if count >= 10 else 1)

    if args.command == "update":
        count = sum(1 for _ in scraper.fetch_updates("2026-01-01"))
        logger.info("Update complete: %d documents", count)


if __name__ == "__main__":
    main()
