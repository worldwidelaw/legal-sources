#!/usr/bin/env python3
"""
INTL/WTOAnalyticalIndex — WTO Analytical Index (Jurisprudence Guide)

Fetches the WTO Analytical Index, a comprehensive guide containing extracts
of key WTO jurisprudence organized by agreement and article.

Strategy:
  - Parse section HTML pages to discover PDF links
  - Download PDFs and extract full text with pdfplumber
  - ~573 PDF documents covering 24+ WTO agreements

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update --since 2024-01-01
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
from html.parser import HTMLParser

import requests
import pdfplumber

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.WTOAnalyticalIndex")

BASE_URL = "https://www.wto.org/english/res_e/publications_e/ai17_e/"

# All 24+ section pages with their agreement names
SECTIONS = [
    ("wto_agree_e.htm", "Marrakesh Agreement Establishing the WTO"),
    ("gin_e.htm", "General Interpretative Note to Annex 1A"),
    ("gatt1994_e.htm", "GATT 1994"),
    ("agriculture_e.htm", "Agreement on Agriculture"),
    ("sps_e.htm", "Agreement on SPS Measures"),
    ("textiles_e.htm", "Agreement on Textiles and Clothing"),
    ("tbt_e.htm", "Agreement on TBT"),
    ("trims_e.htm", "Agreement on TRIMs"),
    ("anti_dumping_e.htm", "Anti-Dumping Agreement"),
    ("cusval_e.htm", "Agreement on Customs Valuation"),
    ("psi_e.htm", "Agreement on Preshipment Inspection"),
    ("roi_e.htm", "Agreement on Rules of Origin"),
    ("licensing_e.htm", "Agreement on Import Licensing"),
    ("subsidies_e.htm", "Agreement on Subsidies and CVD"),
    ("safeguards_e.htm", "Agreement on Safeguards"),
    ("tfa_e.htm", "Trade Facilitation Agreement"),
    ("gats_e.htm", "GATS"),
    ("trips_e.htm", "TRIPS Agreement"),
    ("dsu_e.htm", "Dispute Settlement Understanding"),
    ("tprm_e.htm", "Trade Policy Review Mechanism"),
    ("aircraft_e.htm", "Agreement on Trade in Civil Aircraft"),
    ("gpa_e.htm", "Agreement on Government Procurement (1994)"),
    ("gpa2012_e.htm", "Agreement on Government Procurement (2012)"),
    ("ida_e.htm", "International Dairy/Bovine Meat Agreements"),
    ("wpar_e.htm", "Working Procedures for Appellate Review"),
    ("rcdsu_e.htm", "Rules of Conduct for DSU"),
]

# Content type labels
CONTENT_TYPES = {
    "_jur": "DS Reports (Jurisprudence)",
    "_oth": "Other Practice",
    "_gatt47": "GATT 1947 Historical",
}


class LinkExtractor(HTMLParser):
    """Extract PDF links and their context from section HTML pages."""

    def __init__(self):
        super().__init__()
        self.links = []
        self._current_href = None
        self._current_text = []
        self._in_link = False

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            attrs_dict = dict(attrs)
            href = attrs_dict.get("href", "")
            if href.lower().endswith(".pdf"):
                self._current_href = href
                self._current_text = []
                self._in_link = True

    def handle_data(self, data):
        if self._in_link:
            self._current_text.append(data.strip())

    def handle_endtag(self, tag):
        if tag == "a" and self._in_link:
            text = " ".join(t for t in self._current_text if t)
            self.links.append((self._current_href, text))
            self._in_link = False
            self._current_href = None


class WTOAnalyticalIndexScraper(BaseScraper):
    """
    Scraper for INTL/WTOAnalyticalIndex — WTO Analytical Index.
    Country: INTL
    URL: https://www.wto.org/english/res_e/publications_e/ai17_e/ai17_e.htm
    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        })

    def _extract_text_from_pdf(self, pdf_content: bytes) -> str:
        """Extract text from PDF bytes using pdfplumber."""
        try:
            with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                pages_text = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages_text.append(text)
                return "\n\n".join(pages_text)
        except Exception as e:
            logger.warning(f"PDF text extraction failed: {e}")
            return ""

    def _download_pdf(self, url: str, retries: int = 3) -> Optional[bytes]:
        """Download a PDF with retries."""
        for attempt in range(retries):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=120)
                if resp.status_code == 200 and len(resp.content) > 500:
                    return resp.content
                logger.warning(f"Unexpected response {resp.status_code} for {url} (size={len(resp.content)})")
            except requests.exceptions.RequestException as e:
                logger.warning(f"Download attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(5 * (attempt + 1))
        return None

    def _classify_content_type(self, filename: str) -> str:
        """Classify a PDF by its filename suffix."""
        for suffix, label in CONTENT_TYPES.items():
            if suffix in filename.lower():
                return label
        return "Other"

    def _extract_article_info(self, filename: str) -> str:
        """Extract article identifier from filename."""
        # e.g., gatt1994_art1_jur.pdf -> art1
        match = re.search(r'_(art\w+|preamble|general|ann\w+|incorp\w*|tableofcases)', filename.lower())
        if match:
            return match.group(1)
        return ""

    def _resolve_pdf_url(self, href: str) -> str:
        """Resolve a possibly-relative PDF URL to an absolute URL."""
        if href.startswith("http://") or href.startswith("https://"):
            return href
        # Handle relative paths like ../ai17_e/file.pdf or ./file.pdf
        if href.startswith("../") or href.startswith("./"):
            # All PDFs are in the ai17_e directory
            filename = href.split("/")[-1]
            return BASE_URL + filename
        return BASE_URL + href

    def _discover_pdfs(self) -> Generator[dict, None, None]:
        """Discover all PDF links from the section HTML pages."""
        seen_urls = set()

        for section_file, agreement_name in SECTIONS:
            section_url = BASE_URL + section_file
            logger.info(f"Scanning section: {agreement_name} ({section_url})")

            try:
                time.sleep(1)
                resp = self.session.get(section_url, timeout=30)
                if resp.status_code != 200:
                    logger.warning(f"Failed to fetch section {section_file}: {resp.status_code}")
                    continue

                parser = LinkExtractor()
                parser.feed(resp.text)

                for href, link_text in parser.links:
                    pdf_url = self._resolve_pdf_url(href)
                    if pdf_url in seen_urls:
                        continue
                    seen_urls.add(pdf_url)

                    filename = pdf_url.split("/")[-1]
                    content_type = self._classify_content_type(filename)
                    article = self._extract_article_info(filename)

                    yield {
                        "pdf_url": pdf_url,
                        "filename": filename,
                        "agreement": agreement_name,
                        "article": article,
                        "content_type": content_type,
                        "link_text": link_text,
                        "section_url": section_url,
                    }

            except Exception as e:
                logger.error(f"Error scanning section {section_file}: {e}")

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all WTO Analytical Index PDFs."""
        for pdf_info in self._discover_pdfs():
            pdf_url = pdf_info["pdf_url"]
            logger.info(f"Downloading: {pdf_info['filename']} ({pdf_info['agreement']})")

            pdf_bytes = self._download_pdf(pdf_url)
            if not pdf_bytes:
                logger.warning(f"Could not download: {pdf_url}")
                continue

            text = self._extract_text_from_pdf(pdf_bytes)
            if not text or len(text) < 100:
                logger.warning(f"No/insufficient text extracted from {pdf_info['filename']}")
                continue

            pdf_info["text"] = text
            pdf_info["pdf_size"] = len(pdf_bytes)
            yield pdf_info

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch updates — re-fetches all since content is static."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw PDF info into standardized schema."""
        filename = raw["filename"]
        # Build a unique ID from the filename (without extension)
        doc_id = filename.replace(".pdf", "").replace(" ", "_")

        # Build a descriptive title
        parts = [raw["agreement"]]
        if raw.get("article"):
            # Clean up article label: art1 -> Article 1, ann1 -> Annex 1
            art = raw["article"]
            art = re.sub(r'^art(\d+)', r'Article \1', art)
            art = re.sub(r'^ann(\d+)', r'Annex \1', art)
            art = art.replace("preamble", "Preamble")
            art = art.replace("general", "General")
            art = art.replace("incorp", "Incorporation")
            parts.append(art)
        if raw.get("content_type") and raw["content_type"] != "Other":
            parts.append(f"({raw['content_type']})")
        title = " — ".join(parts[:2])
        if len(parts) > 2:
            title += f" {parts[2]}"

        return {
            "_id": f"INTL/WTOAnalyticalIndex/{doc_id}",
            "_source": "INTL/WTOAnalyticalIndex",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": None,  # Static publication, no specific date per article
            "url": raw["pdf_url"],
            "agreement": raw["agreement"],
            "article": raw.get("article", ""),
            "content_type": raw.get("content_type", ""),
            "link_text": raw.get("link_text", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="INTL/WTOAnalyticalIndex bootstrap")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    boot_parser = subparsers.add_parser("bootstrap", help="Full bootstrap or sample")
    boot_parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    boot_parser.add_argument("--sample-size", type=int, default=15, help="Number of sample records")

    update_parser = subparsers.add_parser("update", help="Incremental update")
    update_parser.add_argument("--since", required=True, help="ISO date (e.g. 2024-01-01)")

    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    scraper = WTOAnalyticalIndexScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        resp = scraper.session.get(BASE_URL + "ai17_e.htm", timeout=15)
        logger.info(f"Main page: HTTP {resp.status_code}, {len(resp.content)} bytes")
        # Test one PDF
        resp2 = scraper.session.get(BASE_URL + "gatt1994_art1_jur.pdf", timeout=30)
        logger.info(f"Sample PDF: HTTP {resp2.status_code}, {len(resp2.content)} bytes")
        logger.info("Connectivity test passed!")

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        since = datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc)
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
