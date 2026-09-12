#!/usr/bin/env python3
"""
VC/GazetteVC -- St Vincent & Grenadines Government Gazette

Scrapes gazette PDFs from legal.gov.vc, extracts full text.

Strategy:
  1. Scrape publications index page to find year category links
  2. For each year category, parse links to gazette PDFs
  3. Download each PDF and extract full text via pdfplumber/pypdf

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Re-fetch all
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from html.parser import HTMLParser

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.VC.GazetteVC")

BASE_URL = "https://legal.gov.vc"
PUBLICATIONS_URL = "/legal/index.php/publications"


class _YearCategoryParser(HTMLParser):
    """Extract year category links from the publications index page."""

    def __init__(self):
        super().__init__()
        self.categories: List[Tuple[str, str]] = []  # (href, text)
        self._in_a = False
        self._current_href: Optional[str] = None
        self._current_text: List[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            href = dict(attrs).get("href", "")
            if "/publications/" in href and "gazette" in href.lower():
                self._in_a = True
                self._current_href = href
                self._current_text = []

    def handle_data(self, data):
        if self._in_a:
            self._current_text.append(data)

    def handle_endtag(self, tag):
        if tag == "a" and self._in_a:
            self._in_a = False
            if self._current_href:
                text = " ".join(self._current_text).strip()
                self.categories.append((self._current_href, text))
            self._current_href = None
            self._current_text = []


class _GazettePDFParser(HTMLParser):
    """Extract PDF download links from a year category page."""

    def __init__(self):
        super().__init__()
        self.pdfs: List[Tuple[str, str]] = []  # (href, link_text)
        self._in_a = False
        self._current_href: Optional[str] = None
        self._current_text: List[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            href = dict(attrs).get("href", "")
            if href.lower().endswith(".pdf") and "gazette" in href.lower():
                self._in_a = True
                self._current_href = href
                self._current_text = []

    def handle_data(self, data):
        if self._in_a:
            self._current_text.append(data)

    def handle_endtag(self, tag):
        if tag == "a" and self._in_a:
            self._in_a = False
            if self._current_href:
                text = " ".join(self._current_text).strip()
                self.pdfs.append((self._current_href, text))
            self._current_href = None
            self._current_text = []


def _parse_gazette_info(href: str, link_text: str) -> Dict[str, Any]:
    """Parse gazette metadata from PDF URL and link text."""
    # Extract number and year from filename like Gazette_No_19_2026.pdf
    m = re.search(r"Gazette_No_(\d+)_(\d{4})", href, re.IGNORECASE)
    if not m:
        return {}

    number = int(m.group(1))
    year = int(m.group(2))
    extra_ordinary = "extra-ordinary" in href.lower() or "extra_ordinary" in href.lower()

    # Extract volume from link text like "Gazette Vol 159, No. 19"
    vol_match = re.search(r"Vol\s*(\d+)", link_text)
    volume = int(vol_match.group(1)) if vol_match else None

    return {
        "number": number,
        "year": year,
        "volume": volume,
        "extra_ordinary": extra_ordinary,
        "pdf_href": href,
    }


class GazetteVCScraper(BaseScraper):
    """Scraper for VC/GazetteVC -- SVG Government Gazette."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
            },
            timeout=120,
        )

    def _fetch_page(self, path: str) -> str:
        try:
            self.rate_limiter.wait()
            resp = self.client.get(path)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {path}: {e}")
            return ""

    def _download_pdf(self, path: str) -> Optional[bytes]:
        try:
            self.rate_limiter.wait()
            resp = self.client.get(path)
            resp.raise_for_status()
            content = resp.content
            if content and (content[:5] == b"%PDF-" or len(content) > 500):
                return content
            logger.warning(f"Empty or invalid response for {path}")
            return None
        except Exception as e:
            logger.warning(f"Failed to download PDF {path}: {e}")
            return None

    def _get_year_categories(self) -> List[Tuple[str, str]]:
        """Get all year category links from publications page."""
        html = self._fetch_page(PUBLICATIONS_URL)
        if not html:
            return []

        parser = _YearCategoryParser()
        parser.feed(html)
        return parser.categories

    def _get_gazette_pdfs(self, category_path: str) -> List[Tuple[str, str]]:
        """Get all gazette PDF links from a year category page."""
        html = self._fetch_page(category_path)
        if not html:
            return []

        parser = _GazettePDFParser()
        parser.feed(html)
        return parser.pdfs

    def _crawl_gazettes(self) -> Generator[Dict[str, Any], None, None]:
        """Crawl all year categories and yield gazette metadata."""
        categories = self._get_year_categories()
        if not categories:
            logger.error("No year categories found on publications page")
            return

        logger.info(f"Found {len(categories)} year categories")

        for cat_href, cat_text in categories:
            # Normalize category path
            if cat_href.startswith("http"):
                cat_path = cat_href.replace(BASE_URL, "")
            elif not cat_href.startswith("/"):
                cat_path = f"/{cat_href}"
            else:
                cat_path = cat_href

            pdfs = self._get_gazette_pdfs(cat_path)
            logger.info(f"  {cat_text}: {len(pdfs)} gazette PDFs")

            for pdf_href, pdf_text in pdfs:
                info = _parse_gazette_info(pdf_href, pdf_text)
                if not info:
                    logger.warning(f"  Could not parse: {pdf_href}")
                    continue

                # Normalize PDF path
                href = info["pdf_href"]
                if href.startswith("http"):
                    pdf_path = href.replace(BASE_URL, "")
                elif not href.startswith("/"):
                    pdf_path = f"/legal/images/PDF/{info['year']}Gazettes/{href}"
                else:
                    pdf_path = href

                eo_suffix = " (Extra-Ordinary)" if info["extra_ordinary"] else ""
                vol_str = f"Vol {info['volume']}, " if info["volume"] else ""
                title = f"SVG Government Gazette {vol_str}No. {info['number']}, {info['year']}{eo_suffix}"

                yield {
                    "gazette_id": f"gazette-{info['year']}-{info['number']}{'eo' if info['extra_ordinary'] else ''}",
                    "title": title,
                    "number": info["number"],
                    "year": info["year"],
                    "volume": info["volume"],
                    "extra_ordinary": info["extra_ordinary"],
                    "pdf_path": pdf_path,
                    "pdf_url": f"{BASE_URL}{pdf_path}",
                }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        return {
            "_id": f"VC/GazetteVC/{raw.get('gazette_id', '')}",
            "_source": "VC/GazetteVC",
            "_type": "legislation",
            "_fetched_at": now,
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": str(raw.get("year", "")),
            "url": raw.get("pdf_url", ""),
            "gazette_id": raw.get("gazette_id", ""),
            "number": raw.get("number"),
            "volume": raw.get("volume"),
            "year": raw.get("year"),
            "extra_ordinary": raw.get("extra_ordinary", False),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        count = 0
        errors = 0

        for item in self._crawl_gazettes():
            pdf_bytes = self._download_pdf(item["pdf_path"])
            if not pdf_bytes:
                errors += 1
                continue

            text = extract_pdf_markdown(
                source="VC/GazetteVC",
                source_id=item["gazette_id"],
                pdf_bytes=pdf_bytes,
                table="legislation",
            ) or ""

            if not text or len(text.strip()) < 50:
                logger.warning(
                    f"Insufficient text for {item['gazette_id']}: {len(text)} chars"
                )
                errors += 1
                continue

            item["text"] = text
            yield item
            count += 1

            if count % 25 == 0:
                logger.info(f"Progress: {count} gazette records, {errors} errors")

        logger.info(f"Complete: {count} gazette records, {errors} errors")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()


# ── CLI ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = GazetteVCScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing publications page...")
        categories = scraper._get_year_categories()
        if not categories:
            logger.error("FAILED — no year categories found")
            sys.exit(1)
        logger.info(f"OK — {len(categories)} year categories found")

        # Test one PDF download
        cat_href, cat_text = categories[0]
        cat_path = cat_href if cat_href.startswith("/") else f"/{cat_href}"
        pdfs = scraper._get_gazette_pdfs(cat_path)
        if pdfs:
            pdf_href, pdf_text = pdfs[0]
            info = _parse_gazette_info(pdf_href, pdf_text)
            if info:
                href = info["pdf_href"]
                if not href.startswith("/"):
                    href = f"/legal/images/PDF/{info['year']}Gazettes/{href}"
                pdf_bytes = scraper._download_pdf(href)
                if pdf_bytes:
                    text = extract_pdf_markdown(
                        source="VC/GazetteVC",
                        source_id="test",
                        pdf_bytes=pdf_bytes,
                        table="legislation",
                    ) or ""
                    logger.info(f"OK — PDF text: {len(text)} chars")
                else:
                    logger.warning("PDF download failed")
        else:
            logger.warning(f"No PDFs in first category: {cat_text}")

    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
