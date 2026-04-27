#!/usr/bin/env python3
"""
BY/PravoBy -- Belarus National Legal Portal Data Fetcher

Fetches codified laws (codes) from pravo.by / etalonline.by, the official
Belarusian legal information portals.

Strategy:
  - Use a curated list of known code registration numbers (regnum)
  - Try etalonline.by first (more reliable, accessible from VPS)
  - Fall back to pravo.by if etalonline.by fails
  - Both sites share the same HTML structure (Section1 div, titlek, etc.)
  - Parse HTML to extract clean full text content

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

# Site configuration -- etalonline.by is primary (more reliable from VPS/datacenter IPs),
# pravo.by is fallback.  Both use the same HTML structure for document content.
ETALON_BASE = "https://etalonline.by"
ETALON_DOC_ENDPOINT = "/document/?regnum="

PRAVO_BASE = "https://pravo.by"
PRAVO_DOC_ENDPOINT = "/document/?guid=3871&p0="
CODES_PAGE = "/pravovaya-informatsiya/normativnye-dokumenty/kodeksy-respubliki-belarus/"

# Timeout in seconds -- etalonline.by can be bandwidth-throttled
DOC_TIMEOUT = 300

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
    URLs: https://etalonline.by (primary), https://pravo.by (fallback)

    Data types: legislation (codified laws / codes)
    Auth: none (Open Government Data)

    Note: pravo.by blocks some VPS/datacenter IPs (returns 403).
    etalonline.by is the same database and is more reliable.
    Both sites share identical HTML structure for document content.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru,en;q=0.9",
            "Accept-Encoding": "gzip, deflate",
        })

    def _fetch_page(self, url: str, timeout: int = 60) -> str:
        """Fetch HTML page content."""
        try:
            self.rate_limiter.wait()
            logger.debug(f"Fetching (timeout={timeout}s): {url}")
            resp = self.session.get(url, timeout=timeout)
            resp.raise_for_status()
            resp.encoding = 'utf-8'
            logger.debug(f"Received {len(resp.text)} bytes from {url}")
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return ""

    def _extract_text_from_html(self, html_content: str) -> str:
        """
        Extract clean text from pravo.by / etalonline.by document HTML.

        Both sites share the same HTML structure:
        - Content is in <div class="Section1">
        - Title in <p class="titlek"> (may contain inner <a> tags)
        - Date in <span class="datepr">, number in <span class="number">
        - Articles in <p class="article">, content in <p class="point">, etc.
        """
        if not html_content:
            return ""

        text_parts = []

        # Extract title -- the <p class="titlek"> may contain <a> tags before the text
        title_match = re.search(r'<p class="titlek"[^>]*>(.*?)</p>', html_content, re.DOTALL)
        if title_match:
            title_text = re.sub(r'<[^>]+>', '', title_match.group(1)).strip()
            if title_text:
                text_parts.append(title_text)
                text_parts.append("")

        # Extract date and number
        date_match = re.search(r'<span class="datepr"[^>]*>([^<]+)</span>', html_content)
        num_match = re.search(r'<span class="number"[^>]*>([^<]+)</span>', html_content)
        if date_match:
            text_parts.append(html.unescape(date_match.group(1).strip()))
        if num_match:
            text_parts.append(html.unescape(num_match.group(1).strip()))
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
        """Extract metadata from document page (works for both pravo.by and etalonline.by)."""
        metadata = {}

        MONTHS_RU = {
            'января': '01', 'февраля': '02', 'марта': '03',
            'апреля': '04', 'мая': '05', 'июня': '06',
            'июля': '07', 'августа': '08', 'сентября': '09',
            'октября': '10', 'ноября': '11', 'декабря': '12',
        }

        # Title from <title> tag
        title_match = re.search(r'<title>([^<]+)</title>', html_content)
        if title_match:
            page_title = html.unescape(title_match.group(1).strip())
            # Clean etalonline.by suffix
            page_title = re.sub(r'\s*[-|]\s*ЭТАЛОН.*', '', page_title)
            page_title = re.sub(r'\s*[-|]\s*etalonline.*', '', page_title, flags=re.IGNORECASE)
            metadata['page_title'] = page_title

        # Description (often contains date info)
        desc_match = re.search(r'<meta name="description" content="([^"]+)"', html_content)
        if desc_match:
            metadata['description'] = html.unescape(desc_match.group(1).strip())

        # Extract date -- try multiple patterns
        # Pattern 1: "от DD.MM.YYYY" (pravo.by style)
        date_found = False
        for text_to_search in [metadata.get('description', ''), metadata.get('page_title', ''), html_content[:5000]]:
            if date_found:
                break
            date_match = re.search(r'от\s+(\d{1,2})\.\s*(\d{2})\.\s*(\d{4})', text_to_search)
            if date_match:
                try:
                    d, m, y = date_match.group(1).zfill(2), date_match.group(2), date_match.group(3)
                    metadata['date'] = f"{y}-{m}-{d}"
                    date_found = True
                except (ValueError, IndexError):
                    pass

            # Pattern 2: "от DD месяца YYYY" (Russian month name, etalonline.by style)
            if not date_found:
                date_match = re.search(r'от\s+(\d{1,2})\s+(\w+)\s+(\d{4})', text_to_search)
                if date_match:
                    day = date_match.group(1).zfill(2)
                    month = MONTHS_RU.get(date_match.group(2).lower())
                    year = date_match.group(3)
                    if month:
                        metadata['date'] = f"{year}-{month}-{day}"
                        date_found = True

        # Document title from content -- handle <a> tags inside <p class="titlek">
        title_k_match = re.search(r'<p class="titlek"[^>]*>(.*?)</p>', html_content, re.DOTALL)
        if title_k_match:
            title_text = re.sub(r'<[^>]+>', '', title_k_match.group(1)).strip()
            if title_text:
                metadata['title'] = html.unescape(title_text)

        return metadata

    def _try_fetch_document(self, url: str, regnum: str) -> Optional[str]:
        """
        Fetch document HTML from a URL.  Returns HTML string or None on failure.
        Validates that the response contains actual document content (not a 404
        or search-interface page).
        """
        html_content = self._fetch_page(url, timeout=DOC_TIMEOUT)
        if not html_content:
            return None

        # Check for 404 / error page
        if '<title>404' in html_content or 'Страница не найдена' in html_content:
            logger.warning(f"Document not found at {url}")
            return None

        # etalonline.by may return the search interface instead of a document
        if 'Выберите орган принятия' in html_content and len(html_content) < 100000:
            logger.warning(f"Got search interface instead of document at {url}")
            return None

        # Sanity check: must contain Section1 or at least some content paragraphs
        if 'Section1' not in html_content and 'class="titlek"' not in html_content:
            logger.warning(f"No document structure found at {url} ({len(html_content)} bytes)")
            return None

        return html_content

    def _fetch_code(self, regnum: str, name_ru: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a single code document.

        Tries etalonline.by first (reliable from VPS), falls back to pravo.by.
        Both sites use the same HTML structure so the same parser works.
        """
        logger.info(f"Fetching code: {name_ru} ({regnum})")

        # Try etalonline.by first (primary -- works from VPS/datacenter IPs)
        etalon_url = f"{ETALON_BASE}{ETALON_DOC_ENDPOINT}{regnum}"
        html_content = self._try_fetch_document(etalon_url, regnum)
        source_url = etalon_url

        # Fall back to pravo.by if etalonline.by failed
        if not html_content:
            logger.info(f"etalonline.by failed for {regnum}, trying pravo.by...")
            pravo_url = f"{PRAVO_BASE}{PRAVO_DOC_ENDPOINT}{regnum}"
            html_content = self._try_fetch_document(pravo_url, regnum)
            source_url = pravo_url

        if not html_content:
            logger.warning(f"Could not fetch {regnum} from either source")
            return None

        # Extract metadata
        metadata = self._extract_metadata(html_content)

        # Extract full text
        full_text = self._extract_text_from_html(html_content)

        if not full_text or len(full_text) < 500:
            logger.warning(f"Insufficient text extracted for {regnum}: {len(full_text) if full_text else 0} chars")
            return None

        logger.info(f"Successfully extracted {len(full_text)} chars for {name_ru}")

        return {
            "regnum": regnum,
            "name_ru": name_ru,
            "url": source_url,
            "full_text": full_text,
            "metadata": metadata,
            "html_length": len(html_content),
            "text_length": len(full_text),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all Belarusian codes.

        Iterates through the known codes list and fetches full text for each.
        Uses etalonline.by as primary source, pravo.by as fallback.
        """
        logger.info(f"Starting full Belarusian codes fetch ({len(KNOWN_CODES)} codes)...")

        success_count = 0
        for i, code_info in enumerate(KNOWN_CODES, 1):
            logger.info(f"[{i}/{len(KNOWN_CODES)}] Processing {code_info['name_ru']}...")
            result = self._fetch_code(code_info["regnum"], code_info["name_ru"])
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
        """Quick connectivity and API test for both sources."""
        print("Testing Belarus legal portal access...")

        # Test etalonline.by (primary)
        print("\n1. Testing etalonline.by (primary source)...")
        etalon_html = self._fetch_page(ETALON_BASE, timeout=15)
        if etalon_html:
            print(f"   etalonline.by: OK ({len(etalon_html)} bytes)")
        else:
            print("   etalonline.by: FAILED")

        # Test pravo.by (fallback)
        print("\n2. Testing pravo.by (fallback source)...")
        pravo_html = self._fetch_page(PRAVO_BASE, timeout=15)
        if pravo_html:
            print(f"   pravo.by: OK ({len(pravo_html)} bytes)")
        else:
            print("   pravo.by: FAILED (may be blocked from this IP)")

        # Test fetching a specific code (uses fallback logic)
        print("\n3. Testing code fetch with fallback (Subsoil Code)...")
        result = self._fetch_code("Hk0800406", "Кодекс Республики Беларусь о недрах")
        if result:
            print(f"   Source URL: {result['url']}")
            print(f"   Title: {result['metadata'].get('title', 'N/A')}")
            print(f"   Date: {result['metadata'].get('date', 'N/A')}")
            print(f"   HTML size: {result['html_length']} bytes")
            print(f"   Text size: {result['text_length']} chars")
            print(f"   Text preview: {result['full_text'][:200]}...")
        else:
            print("   ERROR: Could not fetch from either source")

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
