#!/usr/bin/env python3
"""
BY/CIS-Legislation -- Belarus Codes via CIS Legislation Database

Fetches Belarusian codified laws (codes) from cis-legislation.com.
This is an alternative source since pravo.by is geo-restricted.
Documents are available in English translation.

Strategy:
  - Use a predefined list of known Belarus code RGN IDs
  - Fetch full text HTML for each code
  - Parse and clean the HTML to extract text content

Data Coverage:
  - ~20 major Belarusian codified laws
  - Full text in English translation
  - Consolidated (current) versions

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py test-api            # Quick API connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BY.CIS-Legislation")

# Site configuration
BASE_URL = "https://cis-legislation.com"
DOC_ENDPOINT = "/document.fwx?rgn="

# Known Belarus codes with their RGN IDs from cis-legislation.com
# Compiled from web searches and site exploration
KNOWN_CODES = [
    {"rgn": "1822", "name_en": "Civil Code of the Republic of Belarus"},
    {"rgn": "1977", "name_en": "Criminal Code of the Republic of Belarus"},
    {"rgn": "2002", "name_en": "Code of Criminal Procedure of the Republic of Belarus"},
    {"rgn": "1987", "name_en": "Code of Civil Procedure of the Republic of Belarus"},
    {"rgn": "2562", "name_en": "Labour Code of the Republic of Belarus"},
    {"rgn": "3131", "name_en": "Tax Code of the Republic of Belarus (General Part)"},
    {"rgn": "30488", "name_en": "Tax Code of the Republic of Belarus (Special Part)"},
    {"rgn": "14871", "name_en": "Customs Code of the Republic of Belarus"},
    {"rgn": "2040", "name_en": "Code of the Republic of Belarus on Marriage and Family"},
    {"rgn": "54188", "name_en": "Housing Code of the Republic of Belarus"},
    {"rgn": "32756", "name_en": "Code of the Republic of Belarus on Education"},
    {"rgn": "24126", "name_en": "Budget Code of the Republic of Belarus"},
    {"rgn": "1991", "name_en": "Bank Code of the Republic of Belarus"},
    {"rgn": "129886", "name_en": "Code of the Republic of Belarus on Administrative Offenses"},
    {"rgn": "129893", "name_en": "Procedural and Executive Code on Administrative Offenses"},
    {"rgn": "68800", "name_en": "Water Code of the Republic of Belarus"},
    {"rgn": "1877", "name_en": "Electoral Code of the Republic of Belarus"},
    {"rgn": "14101", "name_en": "Code on Judicial System and Status of Judges"},
    {"rgn": "11847", "name_en": "Air Code of the Republic of Belarus"},
    {"rgn": "157666", "name_en": "Code of Civil Legal Proceedings of the Republic of Belarus"},
]


class CISLegislationScraper(BaseScraper):
    """
    Scraper for BY/CIS-Legislation -- Belarus Codes via CIS Legislation.
    Country: BY
    URL: https://cis-legislation.com

    Data types: legislation (codified laws / codes)
    Auth: none (Open access for document viewing)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })

    def _fetch_page(self, url: str, timeout: int = 60) -> str:
        """Fetch HTML page content."""
        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=timeout)
            resp.raise_for_status()
            resp.encoding = 'utf-8'
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return ""

    def _extract_text_from_html(self, html_content: str) -> str:
        """
        Extract clean text from cis-legislation.com document HTML.

        The document content uses H2-H6 tags for structure and P tags for content.
        Content is within a main document div.
        """
        if not html_content:
            return ""

        text_parts = []

        # Extract title from page
        title_match = re.search(r'<title>([^<]+)</title>', html_content)
        if title_match:
            title = html.unescape(title_match.group(1).strip())
            # Remove site suffix if present
            title = re.sub(r'\s*\|\s*CIS Legislation.*$', '', title)
            text_parts.append(title)
            text_parts.append("")

        # Extract structured content - headers and content blocks
        # Pattern matches H2-H6 headers and their anchor-tagged content
        header_pattern = r'<H([2-6])><A[^>]*name=([^>]+)></A>([^<]+)</H\1>'
        for match in re.finditer(header_pattern, html_content, re.IGNORECASE):
            level = match.group(1)
            content = html.unescape(match.group(3).strip())
            if content:
                # Add appropriate spacing based on header level
                if level in ['2', '3']:
                    text_parts.append("")
                text_parts.append(content)

        # Extract paragraph content
        # Match P tags and extract their text content
        p_pattern = r'<P[^>]*>(.*?)</P>'
        for match in re.finditer(p_pattern, html_content, re.DOTALL | re.IGNORECASE):
            p_content = match.group(1)
            # Remove HTML tags from within paragraph
            text = re.sub(r'<[^>]+>', ' ', p_content)
            text = html.unescape(text)
            text = re.sub(r'\s+', ' ', text).strip()
            if text and len(text) > 5:
                text_parts.append(text)

        # If we got very little content, try a broader extraction
        if len('\n'.join(text_parts)) < 1000:
            logger.debug("Using fallback text extraction")
            # Extract all text between tags
            broader_pattern = r'>([^<]{10,})<'
            for match in re.finditer(broader_pattern, html_content):
                text = html.unescape(match.group(1).strip())
                text = re.sub(r'\s+', ' ', text)
                if text and len(text) > 10 and text not in text_parts:
                    text_parts.append(text)

        full_text = '\n'.join(text_parts)

        # Clean up
        full_text = re.sub(r'\n{3,}', '\n\n', full_text)
        full_text = re.sub(r' +', ' ', full_text)

        return full_text.strip()

    def _extract_metadata(self, html_content: str) -> Dict[str, Any]:
        """Extract metadata from document page."""
        metadata = {}

        # Title
        title_match = re.search(r'<title>([^<]+)</title>', html_content)
        if title_match:
            title = html.unescape(title_match.group(1).strip())
            title = re.sub(r'\s*\|\s*CIS Legislation.*$', '', title)
            metadata['title'] = title

        # Meta description
        desc_match = re.search(r'<meta name="description" content="([^"]+)"', html_content)
        if desc_match:
            metadata['description'] = html.unescape(desc_match.group(1).strip())
            # Try to extract date from description (e.g., "of December 7, 1998")
            date_match = re.search(r'of\s+(\w+)\s+(\d{1,2}),?\s+(\d{4})', metadata['description'])
            if date_match:
                month_name = date_match.group(1)
                day = date_match.group(2)
                year = date_match.group(3)
                try:
                    dt = datetime.strptime(f"{month_name} {day}, {year}", "%B %d, %Y")
                    metadata['date'] = dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

        return metadata

    def _fetch_code(self, rgn: str, name_en: str) -> Optional[Dict[str, Any]]:
        """Fetch a single code document."""
        url = f"{BASE_URL}{DOC_ENDPOINT}{rgn}"
        logger.info(f"Fetching code: {name_en} (rgn={rgn})")

        html_content = self._fetch_page(url, timeout=90)
        if not html_content:
            logger.warning(f"Could not fetch rgn={rgn}")
            return None

        # Check for 404 or error page
        if '404' in html_content[:1000] or 'Document not found' in html_content:
            logger.warning(f"Code not found: rgn={rgn}")
            return None

        # Extract metadata
        metadata = self._extract_metadata(html_content)

        # Extract full text
        full_text = self._extract_text_from_html(html_content)

        if not full_text or len(full_text) < 500:
            logger.warning(f"Insufficient text extracted for rgn={rgn}: {len(full_text) if full_text else 0} chars")
            return None

        return {
            "rgn": rgn,
            "name_en": name_en,
            "url": url,
            "full_text": full_text,
            "metadata": metadata,
            "html_length": len(html_content),
            "text_length": len(full_text),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all Belarusian codes from CIS Legislation.

        Iterates through the known codes list and fetches full text for each.
        """
        logger.info(f"Starting Belarus codes fetch from CIS Legislation ({len(KNOWN_CODES)} codes)...")

        for code_info in KNOWN_CODES:
            result = self._fetch_code(code_info["rgn"], code_info["name_en"])
            if result:
                yield result
            else:
                logger.warning(f"Skipping rgn={code_info['rgn']} - could not fetch")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield codes updated since the given date.

        For codes, we refetch all since they change infrequently
        but are always current consolidated versions.
        """
        logger.info(f"Checking for code updates since {since}...")
        # Codes are consolidated versions, so we just refetch all
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        rgn = raw.get("rgn", "")
        metadata = raw.get("metadata", {})

        # Get title
        title = metadata.get("title") or raw.get("name_en", "")

        # Get date
        date = metadata.get("date", "")

        return {
            # Required base fields
            "_id": f"BY/code/{rgn}",
            "_source": "BY/CIS-Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": raw.get("full_text", ""),  # MANDATORY FULL TEXT
            "date": date,
            "url": raw.get("url", ""),
            # Source-specific fields
            "rgn": rgn,
            "name_en": raw.get("name_en", ""),
            "description": metadata.get("description", ""),
            "html_length": raw.get("html_length", 0),
            "text_length": raw.get("text_length", 0),
            "language": "en",
            "document_type": "code",
            "jurisdiction": "BY",
        }

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing CIS Legislation (Belarus codes) access...")

        # Test main page
        print("\n1. Testing main page connectivity...")
        main_html = self._fetch_page(BASE_URL, timeout=15)
        if main_html:
            print(f"   Main page: {len(main_html)} bytes")
        else:
            print("   ERROR: Cannot reach cis-legislation.com")
            return

        # Test fetching a specific code (Civil Code)
        print("\n2. Testing specific code fetch (Civil Code, rgn=1822)...")
        result = self._fetch_code("1822", "Civil Code of the Republic of Belarus")
        if result:
            print(f"   Title: {result['metadata'].get('title', 'N/A')}")
            print(f"   HTML size: {result['html_length']} bytes")
            print(f"   Text size: {result['text_length']} chars")
            print(f"   Text preview: {result['full_text'][:300]}...")
        else:
            print("   ERROR: Could not fetch Civil Code")

        # Test fetching another code
        print("\n3. Testing Criminal Code fetch (rgn=1977)...")
        result2 = self._fetch_code("1977", "Criminal Code of the Republic of Belarus")
        if result2:
            print(f"   Title: {result2['metadata'].get('title', 'N/A')}")
            print(f"   Text size: {result2['text_length']} chars")
        else:
            print("   ERROR: Could not fetch Criminal Code")

        print("\nAPI test complete!")


def main():
    scraper = CISLegislationScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
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
