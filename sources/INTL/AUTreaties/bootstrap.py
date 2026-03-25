#!/usr/bin/env python3
"""
INTL/AUTreaties -- African Union Treaties, Conventions & Protocols

Fetches AU/OAU treaties from au.int/en/treaties.

Strategy:
  - Scrapes the treaty listing page to get individual treaty URLs
  - For each treaty, scrapes the detail page for metadata and PDF links
  - Downloads English PDF treaty text, extracts with pdfplumber
  - Falls back to PyPDF2 or pdfminer if pdfplumber fails

Data:
  - ~79 treaties/conventions/protocols from 1963 to present
  - Full text extracted from PDF documents
  - Metadata: title, adoption date, entry into force, treaty category
  - CC-BY-4.0 license, multiple languages (EN/FR/AR/PT)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Same as bootstrap (small dataset)
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

import requests
import pdfplumber
from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.AUTreaties")

BASE_URL = "https://au.int"
TREATIES_URL = f"{BASE_URL}/en/treaties"


class AUTreatiesScraper(BaseScraper):
    """
    Scraper for INTL/AUTreaties -- African Union Treaties.
    Country: INTL
    URL: https://au.int/en/treaties

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; contact@example.com)",
        })

    def _get_treaty_urls(self) -> list:
        """Get all individual treaty URLs from the listing page."""
        r = self.session.get(TREATIES_URL, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        treaty_links = []
        seen = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if href.startswith("/en/treaties/") and href != "/en/treaties":
                last = href.rstrip("/").split("/")[-1]
                # Skip category pages (numbered 1158-1170)
                if last.isdigit() and int(last) in range(1150, 1180):
                    continue
                if text and len(text) > 5 and href not in seen:
                    seen.add(href)
                    treaty_links.append((f"{BASE_URL}{href}", text))

        return treaty_links

    def _parse_treaty_page(self, url: str) -> Optional[dict]:
        """Parse an individual treaty detail page."""
        try:
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch treaty page {url}: {e}")
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        # Extract title from h1 or page title
        h1 = soup.find("h1")
        title = h1.get_text(strip=True) if h1 else ""

        # Extract dates from the article content
        adoption_date = ""
        entry_into_force = ""
        category = ""

        article = soup.find("article") or soup.find("div", class_="node")
        if article:
            text_content = article.get_text(separator="\n", strip=True)
            # Parse dates
            adopt_match = re.search(r'Date of Adoption:\s*\n?\s*(.+?)(?:\n|$)', text_content)
            if adopt_match:
                adoption_date = adopt_match.group(1).strip()

            force_match = re.search(r'Date entry into force:\s*\n?\s*(.+?)(?:\n|$)', text_content)
            if force_match:
                entry_into_force = force_match.group(1).strip()

        # Extract PDF links for treaty text
        pdf_links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if ".pdf" in href.lower() and "treaty" in href.lower():
                full_url = href if href.startswith("http") else urljoin(BASE_URL, href)
                lang = "EN"
                if "_f.pdf" in href.lower() or "(FR)" in text:
                    lang = "FR"
                elif "_a.pdf" in href.lower() or "(AR)" in text:
                    lang = "AR"
                elif "_p.pdf" in href.lower() or "(PO)" in text:
                    lang = "PT"
                pdf_links.append({"url": full_url, "text": text, "lang": lang})

        # Determine category from breadcrumb or content
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if "/en/treaties/" in href:
                last = href.rstrip("/").split("/")[-1]
                if last.isdigit() and int(last) in range(1150, 1180):
                    category = text
                    break

        return {
            "url": url,
            "title": title,
            "adoption_date": adoption_date,
            "entry_into_force": entry_into_force,
            "category": category,
            "pdf_links": pdf_links,
        }

    def _extract_pdf_text(self, pdf_url: str) -> str:
        """Download PDF and extract text."""
        try:
            r = self.session.get(pdf_url, timeout=60)
            r.raise_for_status()

            if len(r.content) < 100:
                return ""

            # Try pdfplumber first
            with pdfplumber.open(io.BytesIO(r.content)) as pdf:
                pages_text = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages_text.append(text)
                if pages_text:
                    return "\n\n".join(pages_text)

            # Fallback: PyPDF2
            import PyPDF2
            reader = PyPDF2.PdfReader(io.BytesIO(r.content))
            pages_text = []
            for page in reader.pages:
                text = page.extract_text()
                if text and text.strip():
                    pages_text.append(text)
            if pages_text:
                return "\n\n".join(pages_text)

            return ""
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url}: {e}")
            return ""

    def _parse_date_to_iso(self, date_str: str) -> Optional[str]:
        """Convert AU date formats to ISO 8601."""
        if not date_str:
            return None
        # Try various formats
        for fmt in ["%B %d, %Y", "%d %B %Y", "%B %Y", "%d/%m/%Y", "%Y-%m-%d"]:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all AU treaties with full text."""
        treaty_links = self._get_treaty_urls()
        logger.info(f"Found {len(treaty_links)} treaties")

        for url, listing_title in treaty_links:
            logger.info(f"Processing: {listing_title[:60]}...")
            treaty_data = self._parse_treaty_page(url)
            if not treaty_data:
                continue

            # Find English PDF
            en_pdf = None
            for pdf in treaty_data["pdf_links"]:
                if pdf["lang"] == "EN":
                    en_pdf = pdf
                    break
            # Fallback to first PDF
            if not en_pdf and treaty_data["pdf_links"]:
                en_pdf = treaty_data["pdf_links"][0]

            text = ""
            pdf_url = ""
            if en_pdf:
                pdf_url = en_pdf["url"]
                text = self._extract_pdf_text(pdf_url)

            treaty_data["text"] = text
            treaty_data["pdf_url"] = pdf_url
            treaty_data["listing_title"] = listing_title

            yield treaty_data
            time.sleep(1)  # Rate limit

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Same as fetch_all for this small dataset."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw treaty data into standard schema."""
        text = raw.get("text", "")
        title = raw.get("title") or raw.get("listing_title", "")
        url = raw.get("url", "")

        # Skip if no text extracted (scanned PDF)
        if not text or len(text.strip()) < 50:
            logger.warning(f"No text for: {title}")
            return None

        # Generate ID from URL slug
        slug = url.rstrip("/").split("/")[-1]
        doc_id = f"AU-{slug}"

        adoption_date = self._parse_date_to_iso(raw.get("adoption_date", ""))

        return {
            "_id": doc_id,
            "_source": "INTL/AUTreaties",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": adoption_date,
            "url": url,
            "pdf_url": raw.get("pdf_url", ""),
            "adoption_date": adoption_date,
            "entry_into_force": self._parse_date_to_iso(raw.get("entry_into_force", "")),
            "category": raw.get("category", ""),
            "organization": "African Union",
        }


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="INTL/AUTreaties data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = AUTreatiesScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            treaties = scraper._get_treaty_urls()
            logger.info(f"OK: Found {len(treaties)} treaties")
            logger.info(f"First: {treaties[0][1][:60]}")
            logger.info("Connectivity test passed!")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
