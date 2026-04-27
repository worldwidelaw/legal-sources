#!/usr/bin/env python3
"""
AE/FTA-PublicClarifications -- UAE Federal Tax Authority Guides & Clarifications

Fetches VAT, Corporate Tax, and Excise Tax guides, public clarifications,
and related doctrine from tax.gov.ae. Handles ASP.NET ViewState pagination
to crawl all pages, then downloads PDFs for full text extraction.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records
  python bootstrap.py test-api           # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from html import unescape
from typing import Generator
from urllib.parse import unquote, quote

from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AE.FTA-PublicClarifications")

# Category pages to crawl
CATEGORY_PAGES = [
    {
        "url": "https://tax.gov.ae/en/taxes/Vat/guides.references.aspx",
        "tax_type": "vat",
        "label": "VAT Guides & Public Clarifications",
    },
    {
        "url": "https://tax.gov.ae/en/taxes/corporate.tax/corporate.tax.guides.references.aspx",
        "tax_type": "corporate_tax",
        "label": "Corporate Tax Guides & Public Clarifications",
    },
    {
        "url": "https://tax.gov.ae/en/taxes/excise.tax/guides.listing.aspx",
        "tax_type": "excise_tax",
        "label": "Excise Tax Guides & Public Clarifications",
    },
]

SOURCE_ID = "AE/FTA-PublicClarifications"


def parse_date(date_str: str) -> str:
    """Parse 'Apr 10, 2026' style dates to ISO 8601."""
    if not date_str:
        return ""
    date_str = date_str.strip()
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def slug_from_title(title: str) -> str:
    """Generate a stable slug ID from title."""
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
    if len(slug) > 80:
        slug = slug[:80].rsplit("-", 1)[0]
    return slug


class FTAClarificationsScraper(BaseScraper):
    """
    Scraper for AE/FTA-PublicClarifications.
    Country: AE
    URL: https://tax.gov.ae

    Data types: doctrine
    Auth: none (public PDF downloads)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=120,
        )
        self.session = None

    def _get_session(self):
        """Get or create a requests session for ViewState tracking."""
        if self.session is None:
            import requests
            self.session = requests.Session()
            self.session.headers.update({
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            })
        return self.session

    def _parse_entries_from_html(self, html: str, tax_type: str) -> list[dict]:
        """Parse document entries from an HTML page."""
        soup = BeautifulSoup(html, "html.parser")
        entries = []

        # The listing table is in div.commonTableNew > div.row entries
        # Skip the header row (has class 'headerTable')
        container = soup.find("div", class_="commonTableNew")
        if not container:
            logger.warning("No commonTableNew container found")
            return entries

        rows = container.find_all("div", class_="row", recursive=False)

        for row in rows:
            # Skip header row
            if "headerTable" in row.get("class", []):
                continue

            # Get title from the d-flex div inside col-md-7
            col7 = row.find("div", class_="col-md-7")
            if not col7:
                continue
            title_div = col7.find("div", class_="d-flex")
            if not title_div:
                continue
            # Get direct text only, excluding child elements like "New" badge
            title_text = ""
            for child in title_div.children:
                if isinstance(child, str):
                    title_text += child.strip() + " "
            title_text = title_text.strip()
            if not title_text:
                continue

            # Get date
            date_span = col7.find("span", class_="lastmodifiedDate")
            date_str = date_span.get_text(strip=True) if date_span else ""
            iso_date = parse_date(date_str)

            # Get subcategory
            cat_span = col7.find("span", class_="tag_category")
            subcategory = cat_span.get_text(strip=True) if cat_span else ""

            # Get PDF/document links from the download section
            pdf_links = []
            download_div = row.find("div", class_="downloadlinks")
            if download_div:
                for a_tag in download_div.find_all("a", href=True):
                    href = a_tag["href"].strip()
                    if href and (
                        ".pdf" in href.lower()
                        or ".docx" in href.lower()
                        or "DownloadOpenTextFile" in href
                    ):
                        pdf_links.append(href)

            if not pdf_links:
                continue

            # Use first PDF link as primary
            pdf_url = pdf_links[0]
            if not pdf_url.startswith("http"):
                pdf_url = "https://tax.gov.ae" + pdf_url

            # Generate ID from title
            doc_id = f"FTA-{tax_type.upper()}-{slug_from_title(title_text)}"

            entries.append({
                "title": title_text,
                "date": iso_date,
                "pdf_url": pdf_url,
                "tax_type": tax_type,
                "subcategory": subcategory,
                "doc_id": doc_id,
            })

        return entries

    def _extract_viewstate(self, html: str) -> dict:
        """Extract ASP.NET ViewState fields for postback pagination."""
        soup = BeautifulSoup(html, "html.parser")
        fields = {}
        for field_name in ("__VIEWSTATE", "__VIEWSTATEGENERATOR",
                           "__EVENTVALIDATION", "__EVENTTARGET", "__EVENTARGUMENT"):
            inp = soup.find("input", {"name": field_name})
            if inp:
                fields[field_name] = inp.get("value", "")
        return fields

    def _get_next_page_target(self, html: str):
        """Find the ASP.NET postback target for the 'Next' pagination button."""
        # HTML may use &#39; or ' for quotes in doPostBack
        match = re.search(
            r"doPostBack\((?:&#39;|')([^'&]*LinkButtonNext[^'&]*)(?:&#39;|')",
            html
        )
        if match:
            target = match.group(1)
            # Check if the Next button is disabled (aspNetDisabled)
            next_id = target.replace("$", "_")
            disabled_check = re.search(
                rf'id="{re.escape(next_id)}"[^>]*aspNetDisabled', html
            )
            if disabled_check:
                return None
            return target
        return None

    def _crawl_category(self, page_url: str, tax_type: str) -> list[dict]:
        """Crawl all pages of a category, handling ASP.NET pagination."""
        session = self._get_session()
        all_entries = []
        seen_ids = set()

        # Fetch first page
        self.rate_limiter.wait()
        logger.info(f"Fetching page 1 of {tax_type}...")
        resp = session.get(page_url, timeout=60)
        resp.raise_for_status()
        html = resp.text

        page_num = 1
        max_pages = 30  # Safety limit

        while page_num <= max_pages:
            entries = self._parse_entries_from_html(html, tax_type)
            new_count = 0
            for entry in entries:
                if entry["doc_id"] not in seen_ids:
                    seen_ids.add(entry["doc_id"])
                    all_entries.append(entry)
                    new_count += 1

            logger.info(f"  Page {page_num}: {new_count} new entries (total: {len(all_entries)})")

            if new_count == 0:
                break

            # Check for next page
            next_target = self._get_next_page_target(html)
            if not next_target:
                logger.info(f"  No next page button found, done.")
                break

            # Extract ViewState for postback
            vs_fields = self._extract_viewstate(html)
            if not vs_fields.get("__VIEWSTATE"):
                logger.warning("  No ViewState found, stopping pagination.")
                break

            # POST to get next page
            post_data = {
                "__EVENTTARGET": next_target,
                "__EVENTARGUMENT": "",
                "__VIEWSTATE": vs_fields.get("__VIEWSTATE", ""),
                "__VIEWSTATEGENERATOR": vs_fields.get("__VIEWSTATEGENERATOR", ""),
                "__EVENTVALIDATION": vs_fields.get("__EVENTVALIDATION", ""),
            }

            self.rate_limiter.wait()
            page_num += 1
            logger.info(f"Fetching page {page_num} of {tax_type}...")
            resp = session.post(page_url, data=post_data, timeout=60)
            resp.raise_for_status()
            html = resp.text

        return all_entries

    def _extract_pdf_text(self, pdf_url: str, source_id: str) -> str:
        """Extract text from a PDF document."""
        if not pdf_url:
            return ""
        return extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=source_id,
            pdf_url=pdf_url,
            table="doctrine",
        ) or ""

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents from all FTA category pages."""
        for cat in CATEGORY_PAGES:
            logger.info(f"Crawling {cat['label']}...")
            try:
                entries = self._crawl_category(cat["url"], cat["tax_type"])
                logger.info(f"  Total for {cat['tax_type']}: {len(entries)} entries")
                for entry in entries:
                    yield entry
            except Exception as e:
                logger.error(f"Error crawling {cat['label']}: {e}")
            # Reset session between categories for clean ViewState
            self.session = None

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield all documents (no incremental update support)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw entry into standard schema."""
        pdf_url = raw.get("pdf_url", "")
        source_id = raw.get("doc_id", "")

        # Extract full text from PDF
        full_text = self._extract_pdf_text(pdf_url, source_id)

        return {
            "_id": source_id,
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": full_text,
            "date": raw.get("date", ""),
            "url": pdf_url,
            "tax_type": raw.get("tax_type", ""),
            "subcategory": raw.get("subcategory", ""),
            "country": "AE",
            "language": "en",
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity test."""
        print("Testing UAE FTA pages...")
        total = 0
        for cat in CATEGORY_PAGES:
            try:
                entries = self._crawl_category(cat["url"], cat["tax_type"])
                print(f"  {cat['tax_type']}: {len(entries)} documents")
                total += len(entries)
                if entries:
                    print(f"    First: {entries[0]['title'][:80]}")
                    print(f"    Date: {entries[0]['date']}")
                    print(f"    PDF: {entries[0]['pdf_url'][:100]}")
                # Reset session between categories
                self.session = None
            except Exception as e:
                print(f"  {cat['tax_type']}: ERROR - {e}")

        print(f"\n  Total documents: {total}")

        # Test PDF extraction on first document
        if total > 0:
            entries = self._crawl_category(
                CATEGORY_PAGES[0]["url"], CATEGORY_PAGES[0]["tax_type"]
            )
            if entries:
                doc = entries[0]
                print(f"\n  Testing PDF extraction: {doc['title'][:60]}...")
                text = self._extract_pdf_text(doc["pdf_url"], doc["doc_id"])
                if text:
                    print(f"  PDF extraction: SUCCESS ({len(text)} chars)")
                    print(f"  Sample: {text[:200]}...")
                else:
                    print("  PDF extraction: FAILED")

        print("\nTest completed!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = FTAClarificationsScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
