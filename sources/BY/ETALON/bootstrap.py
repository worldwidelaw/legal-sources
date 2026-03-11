#!/usr/bin/env python3
"""
BY/ETALON -- Belarus ETALON Legal Database Fetcher

Fetches codified laws (codes) from etalonline.by, the official Belarusian
legal information database maintained by the National Center for Legal Information.

Strategy:
  - Use the known list of 26 Belarusian codes with their registration numbers
  - Fetch full text from https://etalonline.by/document/?regnum={regnum}
  - Parse HTML to extract clean text content
  - Use very long timeouts due to bandwidth throttling

IMPORTANT: Site has bandwidth throttling (~2KB/s). Each document may take
5-15 minutes to download. Use --timeout flag to adjust.

Usage:
  python bootstrap.py bootstrap           # Full initial pull (may take hours)
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py test-api            # Quick API connectivity test
  python bootstrap.py bootstrap --timeout 1800  # Custom timeout (30 min)
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BY.ETALON")

# Site configuration
ETALON_BASE = "https://etalonline.by"
DOC_ENDPOINT = "/document/?regnum="
PRAVO_BASE = "https://pravo.by"
CODES_PAGE = "/pravovaya-informatsiya/normativnye-dokumenty/kodeksy-respubliki-belarus/"

# Default timeout in seconds (15 minutes due to bandwidth throttling)
DEFAULT_TIMEOUT = 900

# Known Belarusian codes (26 total)
KNOWN_CODES = [
    {"regnum": "HK0000441", "name_ru": "Банковский кодекс Республики Беларусь", "name_en": "Banking Code"},
    {"regnum": "Hk0800412", "name_ru": "Бюджетный кодекс Республики Беларусь", "name_en": "Budget Code"},
    {"regnum": "Hk1400149", "name_ru": "Водный кодекс Республики Беларусь", "name_en": "Water Code"},
    {"regnum": "Hk0600117", "name_ru": "Воздушный кодекс Республики Беларусь", "name_en": "Air Code"},
    {"regnum": "HK9800218", "name_ru": "Гражданский кодекс Республики Беларусь", "name_en": "Civil Code"},
    {"regnum": "hk1200428", "name_ru": "Жилищный кодекс Республики Беларусь", "name_en": "Housing Code"},
    {"regnum": "HK0000370", "name_ru": "Избирательный кодекс Республики Беларусь", "name_en": "Electoral Code"},
    {"regnum": "Hk0200118", "name_ru": "Кодекс внутреннего водного транспорта", "name_en": "Inland Water Transport Code"},
    {"regnum": "hk2400359", "name_ru": "Кодекс гражданского судопроизводства", "name_en": "Civil Procedure Code"},
    {"regnum": "HK9900278", "name_ru": "Кодекс о браке и семье", "name_en": "Marriage and Family Code"},
    {"regnum": "Hk0800425", "name_ru": "Кодекс о земле", "name_en": "Land Code"},
    {"regnum": "Hk0800406", "name_ru": "Кодекс о недрах", "name_en": "Subsoil Code"},
    {"regnum": "Hk0600139", "name_ru": "Кодекс о судоустройстве и статусе судей", "name_en": "Judicial System Code"},
    {"regnum": "HK2100091", "name_ru": "Кодекс об административных правонарушениях", "name_en": "Administrative Offenses Code"},
    {"regnum": "HK2100092", "name_ru": "Процессуально-исполнительный кодекс об АП", "name_en": "Administrative Offenses Procedure Code"},
    {"regnum": "hk2300289", "name_ru": "Кодекс об архитектурной деятельности", "name_en": "Architecture and Construction Code"},
    {"regnum": "Hk0900071", "name_ru": "Кодекс об образовании", "name_en": "Education Code"},
    {"regnum": "hk1500332", "name_ru": "Кодекс торгового мореплавания", "name_en": "Merchant Shipping Code"},
    {"regnum": "Hk0200166", "name_ru": "Лесной кодекс", "name_en": "Forest Code"},
    {"regnum": "HK0000365", "name_ru": "Налоговый кодекс (Общая часть)", "name_en": "Tax Code (General Part)"},
    {"regnum": "HK9900275", "name_ru": "Уголовный кодекс", "name_en": "Criminal Code"},
    {"regnum": "HK9900295", "name_ru": "Уголовно-процессуальный кодекс", "name_en": "Criminal Procedure Code"},
    {"regnum": "HK9900296", "name_ru": "Уголовно-исполнительный кодекс", "name_en": "Penal Code"},
    {"regnum": "HK9900321", "name_ru": "Трудовой кодекс", "name_en": "Labor Code"},
    {"regnum": "Hk1600413", "name_ru": "Хозяйственный процессуальный кодекс", "name_en": "Economic Procedure Code"},
    {"regnum": "Hk1100243", "name_ru": "Таможенный кодекс", "name_en": "Customs Code"},
]


class EtalonScraper(BaseScraper):
    """
    Scraper for BY/ETALON -- Belarus ETALON Legal Database.
    Country: BY
    URL: https://etalonline.by

    Data types: legislation (codified laws / codes)
    Auth: none (Open Government Data)

    Note: Site has severe bandwidth throttling. Use long timeouts.
    """

    def __init__(self, timeout: int = DEFAULT_TIMEOUT):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.timeout = timeout

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru,en;q=0.9",
            "Accept-Encoding": "gzip, deflate",
        })

    def _fetch_page(self, url: str) -> str:
        """Fetch HTML page with long timeout for slow bandwidth."""
        try:
            self.rate_limiter.wait()
            logger.info(f"Fetching (timeout={self.timeout}s): {url}")
            resp = self.session.get(url, timeout=self.timeout)
            resp.raise_for_status()
            resp.encoding = 'utf-8'
            logger.info(f"Received {len(resp.text)} bytes from {url}")
            return resp.text
        except requests.exceptions.Timeout:
            logger.error(f"Timeout after {self.timeout}s: {url}")
            return ""
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return ""

    def _extract_text_from_etalon(self, html_content: str) -> str:
        """
        Extract clean text from etalonline.by document HTML.

        Document content is in various div/p elements with classes like:
        - document-content
        - text-content
        - article, chapter, section markers
        """
        if not html_content:
            return ""

        text_parts = []

        # Look for main content area
        content_markers = [
            r'<div[^>]*class="[^"]*document[^"]*"[^>]*>(.*?)</div>',
            r'<div[^>]*class="[^"]*text-content[^"]*"[^>]*>(.*?)</div>',
            r'<div[^>]*class="[^"]*content[^"]*"[^>]*>(.*?)</div>',
            r'<div[^>]*id="[^"]*content[^"]*"[^>]*>(.*?)</div>',
        ]

        main_content = html_content

        # Try to find main content div
        for pattern in content_markers:
            match = re.search(pattern, html_content, re.DOTALL | re.IGNORECASE)
            if match and len(match.group(1)) > 1000:
                main_content = match.group(1)
                break

        # Extract paragraphs and text blocks
        paragraph_patterns = [
            r'<p[^>]*>(.*?)</p>',
            r'<div[^>]*class="[^"]*article[^"]*"[^>]*>(.*?)</div>',
            r'<span[^>]*class="[^"]*text[^"]*"[^>]*>(.*?)</span>',
        ]

        for pattern in paragraph_patterns:
            for match in re.findall(pattern, main_content, re.DOTALL | re.IGNORECASE):
                # Strip HTML tags
                text = re.sub(r'<[^>]+>', ' ', match)
                text = html.unescape(text)
                text = re.sub(r'\s+', ' ', text).strip()
                if text and len(text) > 5:
                    text_parts.append(text)

        # If structured extraction didn't find much, try broader approach
        if len('\n'.join(text_parts)) < 1000:
            # Just extract all text between tags
            for match in re.findall(r'>([^<]{10,})<', main_content):
                text = html.unescape(match.strip())
                text = re.sub(r'\s+', ' ', text).strip()
                # Filter out script/style content and menu items
                if text and not any(x in text.lower() for x in ['function(', 'var ', 'document.', '{', '}']):
                    text_parts.append(text)

        full_text = '\n'.join(text_parts)

        # Clean up
        full_text = re.sub(r'\n{3,}', '\n\n', full_text)
        full_text = re.sub(r' +', ' ', full_text)

        return full_text.strip()

    def _extract_metadata(self, html_content: str) -> Dict[str, Any]:
        """Extract metadata from document page."""
        metadata = {}

        # Title from <title> tag
        title_match = re.search(r'<title>([^<]+)</title>', html_content, re.IGNORECASE)
        if title_match:
            title = html.unescape(title_match.group(1).strip())
            # Clean up title
            title = re.sub(r'\s*[-|]\s*ЭТАЛОН.*', '', title)
            title = re.sub(r'\s*[-|]\s*etalonline.*', '', title, flags=re.IGNORECASE)
            metadata['title'] = title.strip()

        # Look for date patterns
        date_patterns = [
            r'от\s+(\d{1,2})[.\s]+(\w+)[.\s]+(\d{4})',  # "от 7 декабря 1998"
            r'(\d{2})\.(\d{2})\.(\d{4})',  # "07.12.1998"
        ]

        for pattern in date_patterns:
            match = re.search(pattern, html_content)
            if match:
                try:
                    groups = match.groups()
                    if len(groups) == 3:
                        # Try to parse
                        if groups[1].isdigit():
                            # DD.MM.YYYY format
                            metadata['date'] = f"{groups[2]}-{groups[1]}-{groups[0]}"
                        else:
                            # Russian month name
                            months_ru = {
                                'января': '01', 'февраля': '02', 'марта': '03',
                                'апреля': '04', 'мая': '05', 'июня': '06',
                                'июля': '07', 'августа': '08', 'сентября': '09',
                                'октября': '10', 'ноября': '11', 'декабря': '12'
                            }
                            month = months_ru.get(groups[1].lower(), '01')
                            day = groups[0].zfill(2)
                            metadata['date'] = f"{groups[2]}-{month}-{day}"
                        break
                except (ValueError, IndexError):
                    pass

        return metadata

    def _fetch_code(self, regnum: str, name_ru: str, name_en: str) -> Optional[Dict[str, Any]]:
        """Fetch a single code document from ETALON."""
        url = f"{ETALON_BASE}{DOC_ENDPOINT}{regnum}"
        logger.info(f"Fetching: {name_en} ({regnum})")

        html_content = self._fetch_page(url)
        if not html_content:
            logger.warning(f"Could not fetch {regnum}")
            return None

        # Check for error page
        if '<title>404' in html_content or 'не найден' in html_content.lower():
            logger.warning(f"Document not found: {regnum}")
            return None

        # Check if we got the search interface instead of document
        # The search interface is typically small (<100KB) and contains form elements
        if 'Выберите орган принятия' in html_content and len(html_content) < 100000:
            logger.warning(f"Got search interface instead of document for {regnum}")
            return None

        # Extract metadata
        metadata = self._extract_metadata(html_content)

        # Extract full text
        full_text = self._extract_text_from_etalon(html_content)

        if not full_text or len(full_text) < 500:
            logger.warning(f"Insufficient text extracted for {regnum}: {len(full_text) if full_text else 0} chars")
            # Try to save HTML for debugging
            return None

        logger.info(f"Successfully extracted {len(full_text)} chars from {name_en}")

        return {
            "regnum": regnum,
            "name_ru": name_ru,
            "name_en": name_en,
            "url": url,
            "full_text": full_text,
            "metadata": metadata,
            "html_length": len(html_content),
            "text_length": len(full_text),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all Belarusian codes.

        WARNING: Due to bandwidth throttling, this may take several hours.
        Each code document can take 5-15 minutes to download.
        """
        logger.info(f"Starting ETALON codes fetch ({len(KNOWN_CODES)} codes, timeout={self.timeout}s each)...")

        success_count = 0
        for i, code_info in enumerate(KNOWN_CODES, 1):
            logger.info(f"[{i}/{len(KNOWN_CODES)}] Processing {code_info['name_en']}...")
            result = self._fetch_code(
                code_info["regnum"],
                code_info["name_ru"],
                code_info["name_en"]
            )
            if result:
                success_count += 1
                yield result
            else:
                logger.warning(f"Skipping {code_info['regnum']} - could not fetch")

        logger.info(f"Completed: {success_count}/{len(KNOWN_CODES)} codes fetched successfully")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield codes updated since the given date.

        For codes, we refetch all since they change infrequently
        but are always current consolidated versions.
        """
        logger.info(f"Checking for code updates since {since}...")
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        regnum = raw.get("regnum", "")
        metadata = raw.get("metadata", {})

        title = raw.get("name_ru") or metadata.get("title", "")

        return {
            # Required base fields
            "_id": f"BY/ETALON/{regnum}",
            "_source": "BY/ETALON",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": raw.get("full_text", ""),  # MANDATORY FULL TEXT
            "date": metadata.get("date", ""),
            "url": raw.get("url", ""),
            # Source-specific fields
            "regnum": regnum,
            "name_ru": raw.get("name_ru", ""),
            "name_en": raw.get("name_en", ""),
            "html_length": raw.get("html_length", 0),
            "text_length": raw.get("text_length", 0),
            "language": "ru",
            "document_type": "code",
            "jurisdiction": "BY",
        }

    def test_api(self):
        """Quick connectivity test (uses shorter timeout)."""
        print("Testing ETALON access...")
        print(f"Note: Site has bandwidth throttling (~2KB/s)")

        # Test with shorter timeout for quick check
        old_timeout = self.timeout
        self.timeout = 30

        print("\n1. Testing main page connectivity...")
        try:
            resp = self.session.head(ETALON_BASE, timeout=15)
            print(f"   Status: {resp.status_code}")
            print(f"   Headers: {dict(list(resp.headers.items())[:3])}")
        except Exception as e:
            print(f"   Error: {e}")

        print("\n2. Checking document endpoint headers...")
        test_regnum = "HK9900321"  # Labor Code (smaller)
        test_url = f"{ETALON_BASE}{DOC_ENDPOINT}{test_regnum}"
        try:
            resp = self.session.head(test_url, timeout=30)
            print(f"   URL: {test_url}")
            print(f"   Status: {resp.status_code}")
            content_length = resp.headers.get('Content-Length', 'unknown')
            print(f"   Content-Length: {content_length}")
            if content_length.isdigit():
                size_mb = int(content_length) / 1024 / 1024
                est_time = int(content_length) / 2048  # 2KB/s
                print(f"   Estimated size: {size_mb:.2f} MB")
                print(f"   Est. download time at 2KB/s: {est_time/60:.1f} minutes")
        except Exception as e:
            print(f"   Error: {e}")

        print("\n3. Testing pravo.by codes listing...")
        try:
            codes_url = f"{PRAVO_BASE}{CODES_PAGE}"
            resp = self.session.get(codes_url, timeout=30)
            if resp.status_code == 200:
                regnums = re.findall(r'regnum=([A-Za-z0-9]+)', resp.text)
                print(f"   Status: {resp.status_code}")
                print(f"   Found {len(set(regnums))} unique code references")
        except Exception as e:
            print(f"   Error: {e}")

        self.timeout = old_timeout
        print("\nTest complete!")
        print(f"\nTo fetch with custom timeout: python bootstrap.py bootstrap --timeout 1800")


def main():
    timeout = DEFAULT_TIMEOUT

    # Parse --timeout flag
    if "--timeout" in sys.argv:
        idx = sys.argv.index("--timeout")
        if idx + 1 < len(sys.argv):
            timeout = int(sys.argv[idx + 1])

    scraper = EtalonScraper(timeout=timeout)

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N] [--timeout SECONDS]"
        )
        print(f"\nDefault timeout: {DEFAULT_TIMEOUT}s (due to bandwidth throttling)")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 5  # Fewer samples due to slow downloads
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
