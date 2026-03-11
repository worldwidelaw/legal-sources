#!/usr/bin/env python3
"""
BY/PravoBy -- Belarus National Legal Portal Data Fetcher

Fetches codified laws (codes) from pravo.by, the official Belarusian
National Legal Internet Portal.

Strategy:
  - Parse the codes listing page at /pravovaya-informatsiya/normativnye-dokumenty/kodeksy-respubliki-belarus/
  - Extract registration numbers (regnum) from links
  - Fetch full text of each code from /document/?guid=3871&p0={regnum}
  - Parse HTML to extract clean text content

Data Coverage:
  - All 26 Belarusian codified laws (codes)
  - Full text in Russian
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
logger = logging.getLogger("legal-data-hunter.BY.PravoBy")

# Site configuration
BASE_URL = "https://pravo.by"
CODES_PAGE = "/pravovaya-informatsiya/normativnye-dokumenty/kodeksy-respubliki-belarus/"
DOC_ENDPOINT = "/document/?guid=3871&p0="

# Known codes with their registration numbers (from the codes page)
KNOWN_CODES = [
    {"regnum": "HK0000441", "name_ru": "Банковский кодекс Республики Беларусь"},
    {"regnum": "Hk0800412", "name_ru": "Бюджетный кодекс Республики Беларусь"},
    {"regnum": "Hk1400149", "name_ru": "Водный кодекс Республики Беларусь"},
    {"regnum": "Hk0600117", "name_ru": "Воздушный кодекс Республики Беларусь"},
    {"regnum": "HK9800218", "name_ru": "Гражданский кодекс Республики Беларусь"},
    {"regnum": "hk1200428", "name_ru": "Жилищный кодекс Республики Беларусь"},
    {"regnum": "HK0000370", "name_ru": "Избирательный кодекс Республики Беларусь"},
    {"regnum": "Hk0200118", "name_ru": "Кодекс внутреннего водного транспорта Республики Беларусь"},
    {"regnum": "hk2400359", "name_ru": "Кодекс гражданского судопроизводства Республики Беларусь"},
    {"regnum": "HK9900278", "name_ru": "Кодекс Республики Беларусь о браке и семье"},
    {"regnum": "Hk0800425", "name_ru": "Кодекс Республики Беларусь о земле"},
    {"regnum": "Hk0800406", "name_ru": "Кодекс Республики Беларусь о недрах"},
    {"regnum": "Hk0600139", "name_ru": "Кодекс Республики Беларусь о судоустройстве и статусе судей"},
    {"regnum": "HK2100091", "name_ru": "Кодекс Республики Беларусь об административных правонарушениях"},
    {"regnum": "HK2100092", "name_ru": "Процессуально-исполнительный кодекс Республики Беларусь об административных правонарушениях"},
    {"regnum": "hk2300289", "name_ru": "Кодекс Республики Беларусь об архитектурной, градостроительной и строительной деятельности"},
    {"regnum": "Hk0900071", "name_ru": "Кодекс Республики Беларусь об образовании"},
    {"regnum": "hk1500332", "name_ru": "Кодекс торгового мореплавания Республики Беларусь"},
    {"regnum": "Hk0200166", "name_ru": "Лесной кодекс Республики Беларусь"},
    {"regnum": "HK0000365", "name_ru": "Налоговый кодекс Республики Беларусь (Общая часть)"},
    {"regnum": "HK9900275", "name_ru": "Трудовой кодекс Республики Беларусь"},
    {"regnum": "HK9900295", "name_ru": "Уголовно-исполнительный кодекс Республики Беларусь"},
    {"regnum": "HK9900296", "name_ru": "Уголовно-процессуальный кодекс Республики Беларусь"},
    {"regnum": "HK9900321", "name_ru": "Уголовный кодекс Республики Беларусь"},
    {"regnum": "Hk1600413", "name_ru": "Хозяйственный процессуальный кодекс Республики Беларусь"},
    {"regnum": "Hk1100243", "name_ru": "Процессуально-исполнительный кодекс Республики Беларусь о административных правонарушениях"},
]


class PravoByScraper(BaseScraper):
    """
    Scraper for BY/PravoBy -- Belarus National Legal Portal.
    Country: BY
    URL: https://pravo.by

    Data types: legislation (codified laws / codes)
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru,en;q=0.9",
        })

    def _fetch_page(self, url: str, timeout: int = 30) -> str:
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
        Extract clean text from pravo.by document HTML.

        The document content is in a nested HTML structure within the main page.
        Articles are in <p class="article">, content in <p class="point">, etc.
        """
        if not html_content:
            return ""

        text_parts = []

        # Extract title
        title_match = re.search(r'<p class="titlek"[^>]*>([^<]+)</p>', html_content)
        if title_match:
            text_parts.append(title_match.group(1).strip())
            text_parts.append("")

        # Extract date and number
        date_match = re.search(r'<span class="datepr"[^>]*>([^<]+)</span>', html_content)
        num_match = re.search(r'<span class="number"[^>]*>([^<]+)</span>', html_content)
        if date_match:
            text_parts.append(date_match.group(1).strip())
        if num_match:
            text_parts.append(num_match.group(1).strip())
        if date_match or num_match:
            text_parts.append("")

        # Extract all content paragraphs
        content_patterns = [
            r'<p class="article"[^>]*>(.*?)</p>',
            r'<p class="point"[^>]*>(.*?)</p>',
            r'<p class="newncpi"[^>]*>(.*?)</p>',
            r'<p class="chapter"[^>]*>(.*?)</p>',
            r'<p class="glava"[^>]*>(.*?)</p>',
            r'<p class="razdel"[^>]*>(.*?)</p>',
            r'<p class="abzac"[^>]*>(.*?)</p>',
            r'<p class="punct"[^>]*>(.*?)</p>',
        ]

        for pattern in content_patterns:
            for match in re.findall(pattern, html_content, re.DOTALL | re.IGNORECASE):
                # Clean HTML tags from content
                text = re.sub(r'<[^>]+>', ' ', match)
                text = html.unescape(text)
                text = re.sub(r'\s+', ' ', text).strip()
                if text and len(text) > 2:
                    text_parts.append(text)

        # If structured extraction didn't get much, try broader extraction
        if len('\n'.join(text_parts)) < 1000:
            # Look for Section1 div which contains the main content
            section_match = re.search(r'<div class="Section1">(.*?)</div>\s*</body>', html_content, re.DOTALL)
            if section_match:
                section_content = section_match.group(1)
                # Extract text between tags
                for match in re.findall(r'>([^<]+)<', section_content):
                    text = match.strip()
                    if len(text) > 3:
                        text = html.unescape(text)
                        text = re.sub(r'\s+', ' ', text).strip()
                        if text:
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
            metadata['page_title'] = html.unescape(title_match.group(1).strip())

        # Description (often contains date info)
        desc_match = re.search(r'<meta name="description" content="([^"]+)"', html_content)
        if desc_match:
            metadata['description'] = html.unescape(desc_match.group(1).strip())
            # Try to extract date from description (e.g., "от 07.12.1998 г.")
            date_in_desc = re.search(r'от (\d{2}\.\d{2}\.\d{4})', metadata['description'])
            if date_in_desc:
                try:
                    dt = datetime.strptime(date_in_desc.group(1), "%d.%m.%Y")
                    metadata['date'] = dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

        # Document title from content
        title_k_match = re.search(r'<p class="titlek"[^>]*>([^<]+)</p>', html_content)
        if title_k_match:
            metadata['title'] = html.unescape(title_k_match.group(1).strip())

        return metadata

    def _fetch_code(self, regnum: str, name_ru: str) -> Optional[Dict[str, Any]]:
        """Fetch a single code document."""
        url = f"{BASE_URL}{DOC_ENDPOINT}{regnum}"
        logger.info(f"Fetching code: {name_ru} ({regnum})")

        html_content = self._fetch_page(url, timeout=60)
        if not html_content:
            logger.warning(f"Could not fetch {regnum}")
            return None

        # Check for 404 or error page
        if '<title>404' in html_content or 'Страница не найдена' in html_content:
            logger.warning(f"Code not found: {regnum}")
            return None

        # Extract metadata
        metadata = self._extract_metadata(html_content)

        # Extract full text
        full_text = self._extract_text_from_html(html_content)

        if not full_text or len(full_text) < 500:
            logger.warning(f"Insufficient text extracted for {regnum}: {len(full_text) if full_text else 0} chars")
            return None

        return {
            "regnum": regnum,
            "name_ru": name_ru,
            "url": url,
            "full_text": full_text,
            "metadata": metadata,
            "html_length": len(html_content),
            "text_length": len(full_text),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all Belarusian codes.

        Iterates through the known codes list and fetches full text for each.
        """
        logger.info(f"Starting full Belarusian codes fetch ({len(KNOWN_CODES)} codes)...")

        for code_info in KNOWN_CODES:
            result = self._fetch_code(code_info["regnum"], code_info["name_ru"])
            if result:
                yield result
            else:
                logger.warning(f"Skipping {code_info['regnum']} - could not fetch")

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
        regnum = raw.get("regnum", "")
        metadata = raw.get("metadata", {})

        # Get title from various sources
        title = raw.get("name_ru") or metadata.get("title") or metadata.get("page_title", "")

        # Clean up title
        title = re.sub(r'\s+–\s+тематические подборки НПА на Pravo\.by', '', title)
        title = re.sub(r'«(.+)»', r'\1', title).strip()

        # Get date
        date = metadata.get("date", "")

        return {
            # Required base fields
            "_id": f"BY/code/{regnum}",
            "_source": "BY/PravoBy",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": raw.get("full_text", ""),  # MANDATORY FULL TEXT
            "date": date,
            "url": raw.get("url", ""),
            # Source-specific fields
            "regnum": regnum,
            "name_ru": raw.get("name_ru", ""),
            "description": metadata.get("description", ""),
            "html_length": raw.get("html_length", 0),
            "text_length": raw.get("text_length", 0),
            "language": "ru",
            "document_type": "code",
            "jurisdiction": "BY",
        }

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing Belarus Pravo.by access...")

        # Test main page
        print("\n1. Testing main page connectivity...")
        main_html = self._fetch_page(BASE_URL, timeout=15)
        if main_html:
            print(f"   Main page: {len(main_html)} bytes")
        else:
            print("   ERROR: Cannot reach pravo.by")
            return

        # Test codes listing page
        print("\n2. Testing codes listing page...")
        codes_url = f"{BASE_URL}{CODES_PAGE}"
        codes_html = self._fetch_page(codes_url, timeout=15)
        if codes_html:
            print(f"   Codes page: {len(codes_html)} bytes")
            # Count code links
            links = re.findall(r'regnum=([A-Za-z0-9]+)', codes_html)
            print(f"   Found {len(set(links))} unique code references")
        else:
            print("   ERROR: Cannot reach codes page")

        # Test fetching a specific code
        print("\n3. Testing specific code fetch (Civil Code)...")
        result = self._fetch_code("HK9800218", "Гражданский кодекс Республики Беларусь")
        if result:
            print(f"   Title: {result['metadata'].get('title', 'N/A')}")
            print(f"   HTML size: {result['html_length']} bytes")
            print(f"   Text size: {result['text_length']} chars")
            print(f"   Text preview: {result['full_text'][:200]}...")
        else:
            print("   ERROR: Could not fetch Civil Code")

        print("\nAPI test complete!")


def main():
    scraper = PravoByScraper()

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
