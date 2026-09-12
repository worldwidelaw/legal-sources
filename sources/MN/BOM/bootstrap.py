#!/usr/bin/env python3
"""
MN/BOM -- Bank of Mongolia Regulations Fetcher

Fetches English-language regulatory documents from the Bank of Mongolia website.

Strategy:
  - Scrape category pages (/en/p/2071..2075) to discover regulation links
  - For each /en/r/{id} page, extract the PDF download link
  - Download PDF and extract text with pdfplumber
  - For /en/p/{id} pages with inline text, extract from HTML

Categories:
  2071: Bank of Mongolia (core laws)
  2072: Banking Supervision (prudential, AML, governance)
  2073: Monetary Policy (reserve requirements, repo, bills)
  2074: Forex Regulation (currency settlements, exchange rates)
  2075: Accounting and Payment Systems

Data:
  - ~35 English-translated regulations and guidelines
  - Coverage: central bank law, banking supervision, monetary policy, forex, payments
  - Language: English
  - License: Open government data (public regulatory documents)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Set

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MN.BOM")

BASE_URL = "https://www.mongolbank.mn"

# Category pages that list regulations
CATEGORY_PAGES = [
    "/en/p/2071",  # Bank of Mongolia
    "/en/p/2072",  # Banking Supervision
    "/en/p/2073",  # Monetary Policy
    "/en/p/2074",  # Forex Regulation
    "/en/p/2075",  # Accounting and Payment Systems
]

# Category names for metadata
CATEGORY_NAMES = {
    "2071": "Bank of Mongolia",
    "2072": "Banking Supervision",
    "2073": "Monetary Policy",
    "2074": "Forex Regulation",
    "2075": "Accounting and Payment Systems",
}


def _extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    try:
        import pdfplumber
    except ImportError:
        logger.error("pdfplumber not installed — cannot extract PDF text")
        return ""

    text_parts = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text)
            try:
                page.flush_cache(); page.get_textmap.cache_clear()
            except Exception:
                pass
    return "\n\n".join(text_parts)


def _clean_html_text(html_str: str) -> str:
    """Strip HTML tags and clean up whitespace."""
    import html as html_mod
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_str, flags=re.DOTALL)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = html_mod.unescape(text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


class BOMScraper(BaseScraper):
    """
    Scraper for MN/BOM -- Bank of Mongolia Regulations.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en,mn;q=0.5",
            },
            timeout=60,
        )

    def _discover_regulation_urls(self) -> List[Dict[str, str]]:
        """
        Scrape category pages to discover regulation page URLs.
        Returns list of dicts with 'url', 'category'.
        """
        seen_urls: Set[str] = set()
        regulations = []

        for cat_path in CATEGORY_PAGES:
            cat_id = cat_path.split("/")[-1]
            cat_name = CATEGORY_NAMES.get(cat_id, "Unknown")
            logger.info(f"Discovering regulations from category: {cat_name} ({cat_path})")

            try:
                resp = self.client.get(cat_path)
                if resp.status_code != 200:
                    logger.warning(f"Category {cat_path} returned {resp.status_code}")
                    continue

                html = resp.text
                # Find /en/r/{id} links (regulation pages with PDFs)
                r_links = re.findall(r'href="(/en/r/\d+)"', html)
                # Find /en/p/{id} links within the content area (not navigation)
                # We only want p-links that appear as document links inside the category
                p_links = re.findall(r'href="(/en/p/\d+)"', html)

                for link in r_links:
                    if link not in seen_urls:
                        seen_urls.add(link)
                        regulations.append({"url": link, "category": cat_name})

                time.sleep(1)

            except Exception as e:
                logger.warning(f"Error fetching category {cat_path}: {e}")

        logger.info(f"Discovered {len(regulations)} unique regulation pages")
        return regulations

    def _fetch_regulation_page(self, url: str) -> Dict[str, Any]:
        """
        Fetch a single regulation page.
        Returns dict with title, date, text, pdf_url, page_url.
        """
        result = {
            "page_url": url,
            "title": "",
            "date": None,
            "text": "",
            "pdf_url": None,
        }

        try:
            resp = self.client.get(url)
            if resp.status_code != 200:
                logger.warning(f"Page {url} returned {resp.status_code}")
                return result

            html = resp.text

            # Extract title from <title> tag
            title_match = re.search(r'<title>([^<]+)</title>', html)
            if title_match:
                result["title"] = _clean_html_text(title_match.group(1)).strip()

            # Try og:title as backup (often cleaner)
            og_title = re.search(r'property="og:title"\s+content="([^"]+)"', html)
            if og_title:
                result["title"] = og_title.group(1).strip()

            # Extract date from page content
            # Look for date patterns like "2019-06-11" or "June 11, 2019"
            # mongolbank dates often appear near the title
            date_match = re.search(r'(\d{4}-\d{2}-\d{2})', html)
            if date_match:
                result["date"] = date_match.group(1)

            # Find PDF download links
            pdf_links = re.findall(r'href="(/file/[^"]+\.pdf)"', html)
            if not pdf_links:
                # Try broader file link pattern
                pdf_links = re.findall(r'href="(/file/[^"]+)"', html)

            if pdf_links:
                result["pdf_url"] = pdf_links[0]
                # Download and extract text from PDF
                logger.info(f"Downloading PDF: {pdf_links[0]}")
                try:
                    pdf_resp = self.client.get(pdf_links[0])
                    if pdf_resp.status_code == 200 and len(pdf_resp.content) > 100:
                        text = _extract_text_from_pdf(pdf_resp.content)
                        if text and len(text) > 50:
                            result["text"] = text
                            logger.info(f"Extracted {len(text)} chars from PDF")
                        else:
                            logger.warning(f"PDF text extraction yielded insufficient text")
                    else:
                        logger.warning(f"PDF download failed: {pdf_resp.status_code}")
                except Exception as e:
                    logger.warning(f"Error downloading PDF {pdf_links[0]}: {e}")
            else:
                # No PDF link — try extracting inline text from page body
                # Look for content in the main article/content area
                body_match = re.search(
                    r'<div[^>]*class="[^"]*content[^"]*"[^>]*>(.*?)</div>',
                    html, re.DOTALL
                )
                if not body_match:
                    body_match = re.search(
                        r'<article[^>]*>(.*?)</article>',
                        html, re.DOTALL
                    )
                if body_match:
                    inline_text = _clean_html_text(body_match.group(1))
                    if len(inline_text) > 100:
                        result["text"] = inline_text

        except Exception as e:
            logger.warning(f"Error fetching page {url}: {e}")

        return result

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all BOM regulations."""
        regulations = self._discover_regulation_urls()

        for i, reg in enumerate(regulations):
            url = reg["url"]
            category = reg["category"]
            logger.info(f"Fetching [{i+1}/{len(regulations)}]: {url}")

            page_data = self._fetch_regulation_page(url)
            if page_data["text"] and len(page_data["text"]) > 50:
                page_data["category"] = category
                yield page_data
            else:
                logger.warning(f"Skipping {url}: no text extracted")

            time.sleep(1.5)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """BOM doesn't have a date-filtered endpoint; re-fetch all."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw regulation data into standard schema."""
        url = raw.get("page_url", "")
        # Extract doc ID from URL
        doc_id_match = re.search(r'/en/[rp]/(\d+)', url)
        doc_id = doc_id_match.group(1) if doc_id_match else url

        title = raw.get("title", "").strip()
        text = raw.get("text", "").strip()
        date = raw.get("date")
        category = raw.get("category", "")

        if not text:
            return None

        return {
            "_id": f"MN-BOM-{doc_id}",
            "_source": "MN/BOM",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_id": doc_id,
            "title": title,
            "text": text,
            "date": date,
            "url": f"{BASE_URL}{url}" if url.startswith("/") else url,
            "category": category,
            "pdf_url": f"{BASE_URL}{raw['pdf_url']}" if raw.get("pdf_url") else None,
            "language": "en",
        }


# ── CLI entry point ─────────────────────────────────────────────────

if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = BOMScraper()
    args = sys.argv[1:]

    if not args:
        print("Usage: python bootstrap.py [bootstrap|bootstrap --sample|test]")
        sys.exit(1)

    command = args[0]

    if command == "test":
        print("Testing connectivity to mongolbank.mn ...")
        try:
            resp = scraper.client.get("/en/p/2071")
            print(f"Status: {resp.status_code}")
            print(f"Content length: {len(resp.text)} bytes")
            print("Connection OK")
        except Exception as e:
            print(f"Connection failed: {e}")
            sys.exit(1)

    elif command == "bootstrap":
        sample = "--sample" in args
        if sample:
            print("Running in SAMPLE mode (12 records) ...")
            stats = scraper.bootstrap(sample_mode=True, sample_size=12)
        else:
            print("Running FULL bootstrap ...")
            stats = scraper.bootstrap()
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
