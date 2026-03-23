#!/usr/bin/env python3
"""
DK/KFST -- Danish Competition and Consumer Authority Decision Fetcher

Fetches competition law decisions from Konkurrence- og Forbrugerstyrelsen.

Strategy:
  - Discovery: Parse XML sitemap at /sitemap for decision URLs
  - Filter: Match URLs with pattern /raads-og-styrelsesafgoerelser/YYYY/slug
  - Full text: Fetch decision HTML page, extract PDF URL, download and extract text

URL patterns:
  - Sitemap: https://kfst.dk/sitemap
  - Decision: https://kfst.dk/konkurrenceforhold/afgoerelser/afgoerelser-paa-konkurrenceomraadet/raads-og-styrelsesafgoerelser/YYYY/slug
  - PDF: https://kfst.dk/media/{hash}/{filename}.pdf

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update             # Incremental update (new decisions)
  python bootstrap.py test-api           # Quick API connectivity test
"""

import sys
import io
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from xml.etree import ElementTree as ET

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

# Try to import pypdf for PDF text extraction
try:
    from pypdf import PdfReader
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.DK.KFST")

# Constants
BASE_URL = "https://kfst.dk"
SITEMAP_URL = "/sitemap"

# URL pattern for competition decisions
DECISION_URL_PATTERN = re.compile(
    r"/konkurrenceforhold/afgoerelser/afgoerelser-paa-konkurrenceomraadet/"
    r"raads-og-styrelsesafgoerelser/(\d{4})/([^/]+)$"
)


class KFSTScraper(BaseScraper):
    """
    Scraper for DK/KFST -- Danish Competition and Consumer Authority.
    Country: DK
    URL: https://kfst.dk

    Data types: regulatory_decisions
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.http = HttpClient(
            base_url=BASE_URL,
            headers={"User-Agent": "WorldWideLaw/1.0 (Open Data Research)"},
            timeout=60,
        )

    def _fetch_sitemap(self) -> list[str]:
        """
        Fetch and parse the sitemap to extract decision URLs.

        Returns:
            List of decision page URLs.
        """
        logger.info("Fetching sitemap...")
        self.rate_limiter.wait()
        resp = self.http.get(SITEMAP_URL)
        resp.raise_for_status()

        # Parse XML sitemap
        root = ET.fromstring(resp.content)
        namespace = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

        decision_urls = []
        for url_elem in root.findall(".//ns:url/ns:loc", namespace):
            url = url_elem.text
            if url and DECISION_URL_PATTERN.search(url):
                decision_urls.append(url)

        logger.info(f"Found {len(decision_urls)} decision URLs in sitemap")
        return decision_urls

    def _fetch_decision_page(self, url: str) -> Optional[dict]:
        """
        Fetch a decision page and extract metadata + PDF URL.

        Returns:
            Dict with raw content and metadata, or None on error.
        """
        try:
            self.rate_limiter.wait()
            resp = self.http.get(url)

            if resp.status_code == 404:
                logger.warning(f"Decision not found: {url}")
                return None

            resp.raise_for_status()

            # Extract URL parts for metadata
            match = DECISION_URL_PATTERN.search(url)
            if not match:
                return None

            year, slug = match.groups()

            return {
                "_raw_html": resp.text,
                "_url": url,
                "_year": year,
                "_slug": slug,
            }

        except Exception as e:
            logger.warning(f"Error fetching {url}: {e}")
            return None

    def _extract_pdf_url(self, html_content: str, slug: str) -> Optional[str]:
        """
        Extract PDF URL from decision page HTML.

        Decision PDFs have the same slug as the URL and are typically in
        a RectangleButton element, not in the navigation menus.
        """
        # First, try to find PDF with the same slug (most reliable)
        slug_pattern = re.compile(
            r'href="(/media/[^"]+/' + re.escape(slug) + r'\.pdf)"',
            re.IGNORECASE
        )
        match = slug_pattern.search(html_content)
        if match:
            pdf_url = match.group(1)
            return BASE_URL + pdf_url

        # Also try with date prefix variations from slug (YYYYMMDD-title)
        date_prefix = slug[:8] if len(slug) > 8 else ""
        if date_prefix.isdigit():
            date_pattern = re.compile(
                r'href="(/media/[^"]+/' + date_prefix + r'[^"]*\.pdf)"',
                re.IGNORECASE
            )
            match = date_pattern.search(html_content)
            if match:
                pdf_url = match.group(1)
                return BASE_URL + pdf_url

        # Look for PDF in download button (class="Button RectangleButton")
        button_pattern = re.compile(
            r'class="[^"]*RectangleButton[^"]*"[^>]*href="(/media/[^"]+\.pdf)"',
            re.IGNORECASE
        )
        match = button_pattern.search(html_content)
        if match:
            pdf_url = match.group(1)
            return BASE_URL + pdf_url

        # Fallback: find any PDF link with "afgoerelse" or decision-like patterns
        decision_patterns = [
            r'href="(/media/[^"]*(?:afgoerelse|afgoerelser|decision)[^"]*\.pdf)"',
            r'href="(/media/[^"]*\d{8}[^"]*\.pdf)"',  # Date-prefixed PDFs
        ]
        for pattern in decision_patterns:
            match = re.search(pattern, html_content, re.IGNORECASE)
            if match:
                pdf_url = match.group(1)
                return BASE_URL + pdf_url

        return None

    def _fetch_pdf_text(self, pdf_url: str) -> str:
        """
        Download PDF and extract text content.

        Returns:
            Extracted text or empty string on failure.
        """
        if not HAS_PYPDF:
            logger.warning("pypdf not available, cannot extract PDF text")
            return ""

        try:
            self.rate_limiter.wait()
            resp = self.http.get(pdf_url)
            resp.raise_for_status()

            # Extract text from PDF
            reader = PdfReader(io.BytesIO(resp.content))
            text_parts = []

            for page in reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text.strip())

            full_text = "\n\n".join(text_parts)

            # Clean up common PDF extraction artifacts
            full_text = re.sub(r'\n{3,}', '\n\n', full_text)
            full_text = re.sub(r' {2,}', ' ', full_text)
            full_text = re.sub(r'-\n', '', full_text)  # Join hyphenated words

            return full_text

        except Exception as e:
            logger.warning(f"Error extracting PDF text from {pdf_url}: {e}")
            return ""

    def _extract_title_from_html(self, html_content: str) -> str:
        """Extract the decision title from HTML."""
        # Try <h1> first
        match = re.search(r"<h1[^>]*>(.*?)</h1>", html_content, re.DOTALL | re.IGNORECASE)
        if match:
            title = re.sub(r"<[^>]+>", "", match.group(1))
            return html.unescape(title).strip()

        # Try <title> tag
        match = re.search(r"<title[^>]*>(.*?)</title>", html_content, re.DOTALL | re.IGNORECASE)
        if match:
            title = match.group(1).split("|")[0].strip()
            return html.unescape(title).strip()

        return ""

    def _extract_case_number(self, html_content: str) -> str:
        """
        Extract the case number (sagsnummer) from HTML.

        KFST uses format like: 24/06957
        """
        patterns = [
            r"[Ss]agsnummer:?\s*(\d{2}/\d{4,})",
            r"[Jj]ournalnummer:?\s*(\d{2}/\d{4,})",
            r"[Ss]ags?\.?\s*nr\.?:?\s*(\d{2}/\d{4,})",
            r"\b(\d{2}/\d{5})\b",  # Generic case number pattern YY/NNNNN
        ]

        for pattern in patterns:
            match = re.search(pattern, html_content)
            if match:
                return match.group(1)

        return ""

    def _extract_date(self, html_content: str, slug: str) -> str:
        """
        Extract the decision date from HTML or slug.

        Slug format is typically: YYYYMMDD-title-slug
        """
        # Try to extract from slug first (most reliable)
        date_match = re.match(r"(\d{4})(\d{2})(\d{2})-", slug)
        if date_match:
            year, month, day = date_match.groups()
            return f"{year}-{month}-{day}"

        # Look for date in meta tags
        date_patterns = [
            r'<meta[^>]*property="article:published_time"[^>]*content="([^"]+)"',
            r'<meta[^>]*name="date"[^>]*content="([^"]+)"',
            r'<time[^>]*datetime="([^"]+)"',
        ]

        for pattern in date_patterns:
            match = re.search(pattern, html_content, re.IGNORECASE)
            if match:
                date_str = match.group(1)
                try:
                    if "T" in date_str:
                        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                        return dt.strftime("%Y-%m-%d")
                    return date_str[:10]
                except ValueError:
                    pass

        # Look for date in content (Danish format)
        date_match = re.search(r"(\d{1,2})\.\s*([a-z]+)\s*(\d{4})", html_content, re.IGNORECASE)
        if date_match:
            day, month_name, year = date_match.groups()
            month_map = {
                "januar": "01", "februar": "02", "marts": "03", "april": "04",
                "maj": "05", "juni": "06", "juli": "07", "august": "08",
                "september": "09", "oktober": "10", "november": "11", "december": "12",
            }
            month = month_map.get(month_name.lower())
            if month:
                return f"{year}-{month}-{int(day):02d}"

        return ""

    def _extract_status(self, html_content: str) -> str:
        """Extract decision status (e.g., concluded, pending)."""
        if re.search(r"afsluttet|concluded", html_content, re.IGNORECASE):
            return "concluded"
        if re.search(r"verserende|pending", html_content, re.IGNORECASE):
            return "pending"
        return ""

    def _extract_legal_basis(self, html_content: str) -> str:
        """Extract legal basis (e.g., § 6 / art. 101)."""
        patterns = [
            r"§\s*\d+\s*/?\s*(?:art\.?\s*\d+)?",
            r"article?\s*\d+",
        ]
        for pattern in patterns:
            match = re.search(pattern, html_content, re.IGNORECASE)
            if match:
                return match.group(0).strip()
        return ""

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all decision documents from KFST.

        Iterates through all decision URLs from the sitemap.
        """
        decision_urls = self._fetch_sitemap()

        for url in decision_urls:
            raw = self._fetch_decision_page(url)
            if raw:
                yield raw

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield decisions modified since the given date.

        Since we don't have a change API, we re-fetch recent years.
        """
        current_year = datetime.now().year
        target_years = [str(current_year), str(current_year - 1)]

        decision_urls = self._fetch_sitemap()

        # Filter to recent years only for updates
        recent_urls = [
            url for url in decision_urls
            if any(f"/{year}/" in url for year in target_years)
        ]

        logger.info(f"Checking {len(recent_urls)} recent decisions for updates")

        for url in recent_urls:
            raw = self._fetch_decision_page(url)
            if raw:
                yield raw

    def normalize(self, raw: dict) -> Optional[dict]:
        """
        Transform raw HTML document into standard schema.

        CRITICAL: Downloads PDF and extracts FULL TEXT.
        """
        html_content = raw["_raw_html"]
        url = raw["_url"]
        year = raw["_year"]
        slug = raw["_slug"]

        # Extract PDF URL and fetch full text
        pdf_url = self._extract_pdf_url(html_content, slug)
        full_text = ""

        if pdf_url:
            logger.info(f"Fetching PDF: {pdf_url}")
            full_text = self._fetch_pdf_text(pdf_url)
        else:
            logger.warning(f"No PDF found for {url}")

        # Skip if no meaningful text
        if len(full_text) < 100:
            logger.warning(f"Skipping {url}: text too short ({len(full_text)} chars)")
            return None

        # Extract metadata
        title = self._extract_title_from_html(html_content)
        date = self._extract_date(html_content, slug)
        case_number = self._extract_case_number(html_content)
        status = self._extract_status(html_content)
        legal_basis = self._extract_legal_basis(html_content)

        # Build ID from case number or URL components
        if case_number:
            doc_id = f"DK-KFST-{case_number.replace('/', '-')}"
        else:
            doc_id = f"DK-KFST-{year}-{slug}"

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "DK/KFST",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            "pdf_url": pdf_url,
            # Source-specific fields
            "case_number": case_number,
            "year": int(year),
            "slug": slug,
            "status": status,
            "legal_basis": legal_basis,
            "authority": "Konkurrence- og Forbrugerstyrelsen",
            "authority_en": "Danish Competition and Consumer Authority",
            "country": "DK",
            "language": "da",
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing KFST endpoints...")

        if not HAS_PYPDF:
            print("WARNING: pypdf not installed. PDF text extraction will fail.")
            print("Install with: pip install pypdf")

        # Test sitemap
        print("\n1. Testing sitemap endpoint...")
        try:
            urls = self._fetch_sitemap()
            print(f"   Found {len(urls)} decision URLs")
            if urls:
                print(f"   First URL: {urls[0]}")
                print(f"   Last URL: {urls[-1]}")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        # Test fetching a decision
        print("\n2. Testing decision fetch...")
        if urls:
            raw = self._fetch_decision_page(urls[0])
            if raw:
                print(f"   Fetched: {raw['_url']}")
                print(f"   Year: {raw['_year']}")
                print(f"   Slug: {raw['_slug']}")

                # Test PDF extraction
                pdf_url = self._extract_pdf_url(raw["_raw_html"], raw["_slug"])
                if pdf_url:
                    print(f"   PDF URL: {pdf_url}")

                    print("\n3. Testing PDF text extraction...")
                    text = self._fetch_pdf_text(pdf_url)
                    print(f"   Text length: {len(text)} characters")
                    if text:
                        print(f"   First 300 chars: {text[:300]}...")

                        # Test normalization
                        record = self.normalize(raw)
                        if record:
                            print(f"\n4. Normalized record:")
                            print(f"   ID: {record['_id']}")
                            print(f"   Title: {record['title'][:80] if record['title'] else 'N/A'}...")
                            print(f"   Date: {record['date']}")
                            print(f"   Case #: {record['case_number']}")
                            print(f"   Text length: {len(record['text'])} chars")
                else:
                    print("   ERROR: No PDF URL found")
            else:
                print("   ERROR: Could not fetch decision")

        print("\nAPI test complete!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = KFSTScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
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
