#!/usr/bin/env python3
"""
JE/RevenueJersey-TaxGuidance -- Revenue Jersey Tax Technical Guidance

Fetches tax guidance documents from gov.je:
  - Concessions & Practices (P1-P28, B1-B23, R1-R5, I1-I19, M1-M8)
  - General Tax Rulings (GTR 1-5)
  - Leasing guidance (6 pages)
  - Company guidelines (14 pages)
  - Pillar Two / MCIT guidance
  - Practice Notes (PDFs)
  - Other technical guidelines

Full text from HTML pages and PDF documents.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Connectivity test
"""

import sys
import re
import logging
import time
import json
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.JE.RevenueJersey-TaxGuidance")

BASE_URL = "https://www.gov.je"
SITEMAP_URL = "https://www.gov.je/sitemaps/taxes_your_money.xml"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# URL patterns for technical guidance content
TECH_PATTERNS = [
    "/TaxesMoney/IncomeTax/Technical/",
    "/TaxesMoney/IncomeTax/Companies/Guidelines/",
    "/TaxesMoney/IncomeTax/PillarTwo/",
]

# Skip these URLs (index pages with no unique content, or covered by TaxCommissioners source)
SKIP_URLS = [
    "/Technical/Pages/default.aspx",
    "/Technical/Guidelines/Pages/index.aspx",
    "/Technical/Legislation/Pages/index.aspx",
    "/Companies/Guidelines/Pages/index.aspx",
    "/PillarTwo/Pages/index.aspx",
    "/CommissionerOfAppealTaxes/",
]

# The Concessions page has many individual items to split
CONCESSIONS_PATH = "/TaxesMoney/IncomeTax/Technical/Guidelines/Pages/ConcessionsPractices.aspx"


def strip_html(html: str) -> str:
    """Remove HTML tags and clean up text."""
    # Remove hidden label divs
    html = re.sub(r"<div[^>]*style='display:none'[^>]*>.*?</div>", "", html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"</(?:p|div|li|h[1-6]|tr)>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<li[^>]*>", "- ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    text = text.replace("\u200b", "").replace("\xa0", " ")
    # Remove "Body Content" artifact from SharePoint label
    text = re.sub(r"^\s*Body Content\s*\n", "", text)
    # Collapse multiple blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def make_id_from_url(url: str) -> str:
    """Create a stable ID from URL path."""
    path = url.replace(BASE_URL, "").strip("/")
    # Remove common prefixes
    path = path.replace("TaxesMoney/IncomeTax/", "")
    path = path.replace("/Pages/", "/").replace(".aspx", "")
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", path).strip("-").lower()
    return f"JE-RJ-{slug}"


def categorize_url(url: str) -> str:
    """Determine the category from URL path."""
    if "/Leasing/" in url:
        return "leasing"
    if "/Companies/Guidelines/" in url:
        return "company"
    if "/PillarTwo/" in url:
        return "pillar_two"
    if "/TaxRulings/" in url:
        return "ruling"
    if "ConcessionsPractices" in url:
        return "concession"
    if "PracticeNotes" in url:
        return "practice_note"
    return "guidance"


class RevenueJerseyTaxGuidanceScraper(BaseScraper):
    """Scraper for Revenue Jersey Tax Technical Guidance."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _get_tech_urls(self) -> list[str]:
        """Fetch sitemap and filter for technical guidance URLs."""
        try:
            resp = self.session.get(SITEMAP_URL, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch sitemap: {e}")
            return []

        urls = re.findall(r"<loc>(.*?)</loc>", resp.text)
        tech_urls = []
        for url in urls:
            if any(p in url for p in TECH_PATTERNS):
                if not any(skip in url for skip in SKIP_URLS):
                    tech_urls.append(url)

        logger.info(f"Found {len(tech_urls)} technical guidance URLs in sitemap")
        return tech_urls

    def _fetch_page_content(self, url: str) -> tuple[str, str]:
        """Fetch a page and return (title, article_html)."""
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return "", ""

        html = resp.text

        # Extract H1 title
        h1_match = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.DOTALL)
        title = strip_html(h1_match.group(1)) if h1_match else ""

        # Extract article content
        article_match = re.search(
            r'<article[^>]*class="[^"]*pagecontent[^"]*"[^>]*>(.*?)</article>',
            html, re.DOTALL | re.IGNORECASE,
        )
        if article_match:
            return title, article_match.group(1)

        return title, ""

    def _split_concessions(self, html: str) -> list[dict]:
        """Split the Concessions & Practices page into individual items."""
        items = []

        # Split by h2/h3 headers that represent individual concessions
        # Pattern: anchors like PersonalTax1, BusinessTax1, etc.
        # Split on <a> anchors with known series patterns
        series_anchors = re.findall(
            r'<a[^>]*(?:name|id)="((?:PersonalTax|BusinessTax|TaxResidence|'
            r'InterestTaxRelief|Miscellaneous|Historic)\d+)"[^>]*>',
            html, re.IGNORECASE,
        )

        if not series_anchors:
            return items

        # Split content by these anchors
        for i, anchor in enumerate(series_anchors):
            # Find the content between this anchor and the next
            pattern = re.escape(anchor)
            if i + 1 < len(series_anchors):
                next_pattern = re.escape(series_anchors[i + 1])
                match = re.search(
                    f'id="{pattern}"[^>]*>(.*?)(?=<a[^>]*id="{next_pattern}")',
                    html, re.DOTALL | re.IGNORECASE,
                )
            else:
                match = re.search(
                    f'id="{pattern}"[^>]*>(.*?)$',
                    html, re.DOTALL | re.IGNORECASE,
                )

            if not match:
                continue

            section_html = match.group(1)

            # The title is the text inside the anchor tag (before </a>)
            title_match = re.match(r"([^<]*)</a>", section_html)
            if title_match:
                title = unescape(title_match.group(1)).replace("\xa0", " ").strip()
            else:
                # Fallback: extract first heading
                heading_match = re.search(
                    r"<h[23][^>]*>(.*?)</h[23]>", section_html, re.DOTALL,
                )
                title = strip_html(heading_match.group(1)) if heading_match else anchor

            # Extract code from title (P1, B5, R3, I12, M7)
            code_match = re.match(r"([PBIRM]\d+)\s*:", title)
            code = code_match.group(1) if code_match else None

            # Map anchor prefix to category code prefix
            prefix_map = {
                "PersonalTax": "P",
                "BusinessTax": "B",
                "TaxResidence": "R",
                "InterestTaxRelief": "I",
                "Miscellaneous": "M",
                "Historic": "H",
            }

            if not code:
                # Derive code from anchor name
                for prefix, letter in prefix_map.items():
                    if anchor.startswith(prefix):
                        num = anchor[len(prefix):]
                        code = f"{letter}{num}"
                        break

            text = strip_html(section_html)
            if len(text) < 20:
                continue

            guidance_id = f"JE-RJ-concession-{code.lower()}" if code else f"JE-RJ-concession-{anchor.lower()}"

            items.append({
                "guidance_id": guidance_id,
                "title": title,
                "text": text,
                "code": code,
                "category": "concession",
                "url": f"{BASE_URL}{CONCESSIONS_PATH}#{anchor}",
                "date": None,
            })

        logger.info(f"Split concessions page into {len(items)} items")
        return items

    def _extract_pdfs_from_html(self, html: str, page_url: str) -> list[str]:
        """Find PDF links in page HTML."""
        pdf_links = re.findall(r'href="([^"]*\.pdf[^"]*)"', html, re.IGNORECASE)
        return [urljoin(BASE_URL, link) for link in pdf_links]

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all tax guidance documents with full text."""
        tech_urls = self._get_tech_urls()

        for i, url in enumerate(tech_urls):
            time.sleep(1.5)

            # Special handling for Concessions page
            if CONCESSIONS_PATH in url:
                title, html = self._fetch_page_content(url)
                if html:
                    items = self._split_concessions(html)
                    for item in items:
                        yield item
                continue

            title, html = self._fetch_page_content(url)
            if not html:
                continue

            text = strip_html(html)
            if len(text) < 30:
                continue

            category = categorize_url(url)
            guidance_id = make_id_from_url(url)

            # Extract date if present in text (common pattern: "Publication date DD Month YYYY")
            date = None
            date_match = re.search(
                r"[Pp]ublication\s+date\s+(\d{1,2}\s+\w+\s+\d{4})", text,
            )
            if date_match:
                try:
                    parsed = datetime.strptime(date_match.group(1), "%d %B %Y")
                    date = parsed.strftime("%Y-%m-%d")
                except ValueError:
                    pass

            # Extract any code from title (GTR X, etc.)
            code = None
            code_match = re.search(r"\(GTR\s*(\d+)[^)]*\)", title)
            if code_match:
                code = f"GTR{code_match.group(1)}"

            # Check for PDFs on this page
            pdf_urls = self._extract_pdfs_from_html(html, url)
            if pdf_urls and len(text) < 200:
                # Page is mostly links to PDFs - fetch PDF content
                for pdf_url in pdf_urls[:3]:  # Limit PDFs per page
                    time.sleep(1)
                    pdf_text = extract_pdf_markdown(
                        source="JE/RevenueJersey-TaxGuidance",
                        source_id=guidance_id,
                        pdf_url=pdf_url,
                        table="doctrine",
                    )
                    if pdf_text and len(pdf_text) > 100:
                        pdf_filename = unquote(pdf_url.split("/")[-1])
                        pdf_id = f"{guidance_id}-pdf-{hashlib.md5(pdf_url.encode()).hexdigest()[:8]}"
                        yield {
                            "guidance_id": pdf_id,
                            "title": f"{title} - {pdf_filename}",
                            "text": pdf_text,
                            "code": code,
                            "category": category,
                            "url": pdf_url,
                            "date": date,
                        }
            else:
                yield {
                    "guidance_id": guidance_id,
                    "title": title,
                    "text": text,
                    "code": code,
                    "category": category,
                    "url": url,
                    "date": date,
                }

            logger.info(f"Processed {i + 1}/{len(tech_urls)}: {title[:60]}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch all (small corpus, always full refresh)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw entry into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 30:
            return None

        return {
            "_id": raw["guidance_id"],
            "_source": "JE/RevenueJersey-TaxGuidance",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "guidance_id": raw["guidance_id"],
            "title": raw.get("title", ""),
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "category": raw.get("category"),
            "code": raw.get("code"),
            "language": "eng",
        }

    def test_connection(self) -> bool:
        """Test connectivity to gov.je."""
        try:
            resp = self.session.get(SITEMAP_URL, timeout=30)
            resp.raise_for_status()
            if "taxes_your_money" in resp.url or "<loc>" in resp.text:
                logger.info("Connection test passed")
                return True
            logger.error("Connection test: unexpected response")
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


if __name__ == "__main__":
    scraper = RevenueJerseyTaxGuidanceScraper()

    if len(sys.argv) < 2:
        print("Usage: bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        success = scraper.test_connection()
        sys.exit(0 if success else 1)
    elif command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample_mode)
        print(f"Bootstrap complete: {result}")
    elif command == "update":
        result = scraper.update()
        print(f"Update complete: {result}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
