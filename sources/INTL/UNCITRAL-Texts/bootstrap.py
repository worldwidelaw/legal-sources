#!/usr/bin/env python3
"""
INTL/UNCITRAL-Texts -- UNCITRAL Model Laws and Legislative Guides

Fetches UNCITRAL instruments (model laws, conventions, legislative guides,
rules, recommendations) from uncitral.un.org with full text from PDFs.

Strategy:
  - Scrape 12 category pages to discover all instrument URLs
  - Fetch each instrument page for description + PDF download links
  - Download PDFs and extract full text via pdfplumber
  - ~50 instruments total

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
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
from typing import Generator, Optional, Dict, Any, List, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.UNCITRAL-Texts")

BASE_URL = "https://uncitral.un.org"
TEXTS_URL = f"{BASE_URL}/en/texts"

CATEGORIES = [
    ("arbitration", "International Commercial Arbitration"),
    ("mediation", "International Commercial Mediation"),
    ("isds", "Investor-State Dispute Settlement"),
    ("ecommerce", "Electronic Commerce"),
    ("salegoods", "International Sale of Goods"),
    ("msmes", "Micro, Small and Medium-sized Enterprises"),
    ("insolvency", "Insolvency"),
    ("securityinterests", "Security Interests"),
    ("onlinedispute", "Online Dispute Resolution"),
    ("payments", "Payments and Trade Finance"),
    ("procurement", "Procurement and Public-Private Partnerships"),
    ("transportgoods", "International Transport of Goods"),
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
}


class UNCITRALTextsScraper(BaseScraper):
    SOURCE_ID = "INTL/UNCITRAL-Texts"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _fetch_page(self, url: str) -> Optional[str]:
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=60)
                resp.raise_for_status()
                return resp.text
            except requests.RequestException as e:
                if attempt == 2:
                    logger.warning("Failed to fetch %s: %s", url, e)
                    return None
                time.sleep(2 * (attempt + 1))

    def _download_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="INTL/UNCITRAL-Texts",
            source_id="",
            pdf_url=pdf_url,
            table="legislation",
        ) or ""

    def _discover_instruments(self) -> List[Dict]:
        """Discover all instrument pages from the 12 category pages."""
        seen_urls: Set[str] = set()
        instruments = []

        for cat_slug, cat_name in CATEGORIES:
            cat_url = f"{TEXTS_URL}/{cat_slug}"
            logger.info("Scanning category: %s", cat_name)
            self.rate_limiter.wait()
            html = self._fetch_page(cat_url)
            if not html:
                continue

            soup = BeautifulSoup(html, "html.parser")
            # Find links to instrument pages (typically under /en/texts/{cat}/...)
            for link in soup.find_all("a", href=True):
                href = link["href"]
                full_url = urljoin(BASE_URL, href)
                # Filter for instrument pages (not status, travaux, or external)
                if not full_url.startswith(f"{BASE_URL}/en/texts/{cat_slug}/"):
                    continue
                # Skip status pages, travaux pages, working group pages
                if any(skip in full_url for skip in ["/status", "/travaux", "/working_group", "/clout", "#"]):
                    continue
                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                title = link.get_text(strip=True)
                if not title or len(title) < 5:
                    continue

                instruments.append({
                    "url": full_url,
                    "title": title,
                    "category": cat_name,
                    "category_slug": cat_slug,
                })

        logger.info("Discovered %d unique instrument pages", len(instruments))
        return instruments

    def _extract_pdf_urls(self, html: str) -> List[str]:
        """Extract PDF download links from an instrument page."""
        soup = BeautifulSoup(html, "html.parser")
        pdf_urls = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if href.endswith(".pdf"):
                full_url = urljoin(BASE_URL, href)
                # Prefer English PDFs
                if full_url not in pdf_urls:
                    pdf_urls.append(full_url)
        return pdf_urls

    def _extract_description(self, html: str) -> str:
        """Extract the description text from the instrument page."""
        soup = BeautifulSoup(html, "html.parser")
        # Main content area
        content = soup.find("div", class_="field--name-body") or soup.find("article")
        if content:
            for tag in content.find_all(["script", "style", "nav"]):
                tag.decompose()
            text = content.get_text(separator="\n")
            text = re.sub(r"\n{3,}", "\n\n", text)
            return text.strip()
        return ""

    def _extract_date(self, text: str, title: str) -> Optional[str]:
        """Try to extract a year/date from the instrument text or title."""
        # Common patterns: "(2006)", "adopted in 2010", "1980"
        combined = f"{title} {text[:500]}"
        years = re.findall(r'\b(19[6-9]\d|20[0-2]\d)\b', combined)
        if years:
            return f"{years[0]}-01-01"
        return None

    def test_connection(self) -> bool:
        try:
            html = self._fetch_page(TEXTS_URL)
            if html and "UNCITRAL" in html:
                logger.info("Connection OK: UNCITRAL texts index accessible")
                return True
            return False
        except Exception as e:
            logger.error("Connection failed: %s", e)
            return False

    def fetch_all(self) -> Generator[Dict, None, None]:
        instruments = self._discover_instruments()
        logger.info("Processing %d instruments...", len(instruments))

        for i, inst in enumerate(instruments):
            logger.info("[%d/%d] %s", i + 1, len(instruments), inst["title"][:70])
            self.rate_limiter.wait()
            html = self._fetch_page(inst["url"])
            if not html:
                continue

            description = self._extract_description(html)
            pdf_urls = self._extract_pdf_urls(html)

            # Try to get full text from PDF
            full_text = ""
            for pdf_url in pdf_urls:
                # Prefer English PDFs
                if "_e_" in pdf_url.lower() or "/en/" in pdf_url.lower() or not full_text:
                    logger.info("  Downloading PDF: %s", pdf_url.split("/")[-1])
                    self.rate_limiter.wait()
                    text = self._download_pdf_text(pdf_url)
                    if text and len(text) > len(full_text):
                        full_text = text
                    if full_text and "_e_" in pdf_url.lower():
                        break  # Got English version

            # Use description as fallback if no PDF text
            if not full_text:
                full_text = description

            if not full_text:
                logger.warning("No text for: %s", inst["title"])
                continue

            yield {
                "title": inst["title"],
                "url": inst["url"],
                "category": inst["category"],
                "text": full_text,
                "description": description,
                "pdf_urls": pdf_urls,
            }

    def fetch_updates(self, since: datetime) -> Generator[Dict, None, None]:
        return
        yield

    def normalize(self, raw: dict) -> dict:
        title = raw["title"]
        url = raw["url"]
        # Create a stable ID from the URL path
        path = url.replace(f"{BASE_URL}/en/texts/", "").strip("/")
        safe_id = re.sub(r'[^a-zA-Z0-9_-]', '_', path)

        date_str = self._extract_date(raw.get("text", ""), title)

        return {
            "_id": f"UNCITRAL-{safe_id}",
            "_source": "INTL/UNCITRAL-Texts",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": date_str,
            "url": url,
            "category": raw.get("category", ""),
            "description": raw.get("description", ""),
        }

    def run_bootstrap(self, sample: bool = False):
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for raw in self.fetch_all():
            normalized = self.normalize(raw)
            fname = re.sub(r'[^\w\-.]', '_', f"{normalized['_id'][:80]}.json")
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info("  -> %d chars of text", len(normalized["text"]))

            if sample and count >= 15:
                break

        logger.info("Bootstrap complete: %d records saved", count)
        return count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="INTL/UNCITRAL-Texts Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = UNCITRALTextsScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        scraper.run_bootstrap(sample=args.sample)
    elif args.command == "update":
        logger.info("No update mechanism (instruments rarely change)")


if __name__ == "__main__":
    main()
