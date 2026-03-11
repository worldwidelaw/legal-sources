#!/usr/bin/env python3
"""
SK/CollectionOfLaws -- Slovak Collection of Laws (Zbierka zákonov)

Fetches Slovak legislation from static.slov-lex.sk, the Ministry of Justice portal.

Strategy:
  - Uses the static HTML version which doesn't require JavaScript
  - Year listing: /static/SK/ZZ/{year}/ - lists all laws for that year
  - Law index: /static/SK/ZZ/{year}/{number}/ - lists versions with dates
  - Law version: /static/SK/ZZ/{year}/{number}/{date}.html - full text HTML

Data:
  - All Slovak legislation from 1918 to present
  - Full text in structured HTML with articles, paragraphs, points
  - License: Open Government Data

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent years only)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SK.collection-of-laws")

# Base URL for Slovak legislation static portal
BASE_URL = "https://static.slov-lex.sk"

# Years to scrape (most recent first for sample mode)
YEARS_TO_SCRAPE = list(range(2026, 1989, -1))  # 2026 down to 1990


class SlovLexScraper(BaseScraper):
    """
    Scraper for SK/CollectionOfLaws -- Slovak Collection of Laws.
    Country: SK
    URL: https://static.slov-lex.sk

    Data types: legislation
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept-Language": "sk,en",
                "Accept": "text/html,application/xhtml+xml",
            },
            timeout=60,
        )

    def _get_text_utf8(self, resp) -> str:
        """Get response text, forcing UTF-8 encoding."""
        # The server doesn't always specify charset, but content is UTF-8
        resp.encoding = 'utf-8'
        return resp.text

    def _get_years(self) -> List[int]:
        """
        Get list of available years from the main index.
        """
        url = "/static/SK/ZZ/"

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()

            text = self._get_text_utf8(resp)
            # Extract years from links like <a href='/static/SK/ZZ/2024/'>2024</a>
            pattern = re.compile(r"href='/static/SK/ZZ/(\d{4})/'")
            years = [int(m) for m in pattern.findall(text)]
            return sorted(years, reverse=True)

        except Exception as e:
            logger.warning(f"Failed to get years: {e}")
            return YEARS_TO_SCRAPE

    def _get_laws_for_year(self, year: int) -> List[Tuple[int, str]]:
        """
        Get list of laws (number, title) for a specific year.

        Returns list of (number, title) tuples.
        """
        url = f"/static/SK/ZZ/{year}/"

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()

            # Pattern matches rows like:
            # <td>1/2024&nbsp;Z.&nbsp;z.</td>
            # <td><a href="1/">Title...</a></td>
            laws = []

            # Decode HTML entities first to normalize the text
            text = html.unescape(self._get_text_utf8(resp))

            # Find all table rows - pattern accounts for nbsp entities
            # Format: <td>N/YYYY Z. z.</td>\n<td><a href="N/">Title</a></td>
            row_pattern = re.compile(
                r'<td>(\d+)/\d+\s+Z\.\s+z\.</td>\s*<td><a href="(\d+)/"[^>]*>([^<]+)</a></td>',
                re.DOTALL | re.IGNORECASE
            )

            for match in row_pattern.finditer(text):
                number = int(match.group(2))
                title = match.group(3).strip()
                laws.append((number, title))

            logger.info(f"Found {len(laws)} laws for year {year}")
            return laws

        except Exception as e:
            logger.warning(f"Failed to get laws for year {year}: {e}")
            return []

    def _get_latest_version(self, year: int, number: int) -> Optional[str]:
        """
        Get the latest version date for a law.

        Returns the date string (e.g., "20240201") or None.
        """
        url = f"/static/SK/ZZ/{year}/{number}/"

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()

            # Look for version links in the history table
            # Pattern: <a href="20240201.html"><span>01.02.2024 - </span></a>
            # or <a href="vyhlasene_znenie.html">Vyhlásené znenie</a>

            text = self._get_text_utf8(resp)
            # Find all date versions (YYYYMMDD.html pattern)
            date_pattern = re.compile(r'href="(\d{8})\.html"')
            dates = date_pattern.findall(text)

            if dates:
                # Return the most recent date (they should already be in order, but sort to be sure)
                dates.sort(reverse=True)
                return dates[0]

            # Fall back to "vyhlasene_znenie" (published version) if no dated versions
            if 'vyhlasene_znenie.html' in text:
                return "vyhlasene_znenie"

            return None

        except Exception as e:
            logger.warning(f"Failed to get versions for {year}/{number}: {e}")
            return None

    def _extract_text_from_html(self, html_content: str) -> str:
        """
        Extract clean law text from the HTML page.

        The law text is in structured divs with classes like:
        - predpis, predpisNadpis, predpisTyp, predpisDatum
        - clanok, clanokOznacenie (articles)
        - paragraf, paragrafOznacenie (paragraphs/sections)
        - odsek, odsekOznacenie (subsections)
        - pismeno, pismenoOznacenie (points)
        - bod, bodOznacenie (sub-points)
        - text (actual content text)
        """
        text_parts = []

        # Extract the law type and date header
        typ_match = re.search(r'<div class="predpisTyp"[^>]*>([^<]+)</div>', html_content)
        if typ_match:
            text_parts.append(typ_match.group(1).strip())

        datum_match = re.search(r'<div class="predpisDatum"[^>]*>([^<]+)</div>', html_content)
        if datum_match:
            text_parts.append(datum_match.group(1).strip())

        nadpis_match = re.search(r'<div class="predpisNadpis[^"]*"[^>]*>(.*?)</div>', html_content, re.DOTALL)
        if nadpis_match:
            text = self._clean_html_text(nadpis_match.group(1))
            if text:
                text_parts.append(text)

        # Extract all text divs with id containing ".text"
        # These contain the actual content of each section
        text_pattern = re.compile(r'<div class="text"[^>]*id="[^"]*\.text[^"]*"[^>]*>(.*?)</div>', re.DOTALL)
        for match in text_pattern.finditer(html_content):
            text = self._clean_html_text(match.group(1))
            if text and len(text) > 5:
                text_parts.append(text)

        # Also extract text2 blocks (used for citations/quotes within amendments)
        text2_pattern = re.compile(r'<div class="text2"[^>]*>(.*?)</div><!--', re.DOTALL)
        for match in text2_pattern.finditer(html_content):
            text = self._clean_html_text(match.group(1))
            if text and len(text) > 10:
                text_parts.append(text)

        # Extract article and section headers
        for class_name in ['clanokOznacenie', 'paragrafOznacenie', 'castOznacenie']:
            header_pattern = re.compile(rf'<div class="{class_name}"[^>]*>([^<]+)')
            for match in header_pattern.finditer(html_content):
                header = match.group(1).strip()
                if header:
                    text_parts.append(f"\n{header}")

        # If we got nothing from structured extraction, try a more aggressive approach
        if len(text_parts) < 5:
            # Get content from predpis div
            predpis_match = re.search(r'<div class="predpis[^"]*"[^>]*id="predpis"[^>]*>(.*?)</div>\s*</div>\s*</div>\s*</div>\s*<div id="footer"', html_content, re.DOTALL)
            if predpis_match:
                predpis_content = predpis_match.group(1)
                # Extract all text from this section
                all_text = self._clean_html_text(predpis_content)
                if all_text:
                    text_parts = [all_text]

        full_text = '\n\n'.join(text_parts)
        return full_text.strip()

    def _clean_html_text(self, html_text: str) -> str:
        """Remove HTML tags and clean up text."""
        # Remove HTML tags but preserve some structure
        # First, add newlines for block elements
        text = re.sub(r'</div>\s*<div', '</div>\n<div', html_text)
        text = re.sub(r'</p>\s*<p', '</p>\n<p', text)
        # Remove all HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)
        # Decode HTML entities
        text = html.unescape(text)
        # Normalize whitespace but preserve paragraph breaks
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n\s*\n', '\n\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _extract_metadata(self, html_content: str, year: int, number: int, version: str) -> Dict[str, Any]:
        """
        Extract metadata from the HTML page.
        """
        metadata = {
            "doc_id": f"SK/ZZ/{year}/{number}/{version}",
            "year": year,
            "number": number,
            "version": version,
            "title": "",
            "doc_type": "",
            "effective_from": "",
            "effective_to": "",
        }

        # Extract title from the sidebar or header
        title_match = re.search(
            r'<tr><td class="title">Názov:</td><td class="value">([^<]+)</td></tr>',
            html_content
        )
        if title_match:
            metadata["title"] = html.unescape(title_match.group(1).strip())
        else:
            # Try alternate pattern in header
            h1_match = re.search(r'<h1>(\d+/\d+ Z\. z\.)</h1>', html_content)
            nadpis_match = re.search(r'<div class="predpisNadpis[^"]*"[^>]*>(.*?)</div>', html_content, re.DOTALL)
            if h1_match and nadpis_match:
                metadata["title"] = f"{h1_match.group(1)} {self._clean_html_text(nadpis_match.group(1))}"

        # Extract document type
        typ_match = re.search(
            r'<tr><td class="title">Typ:</td><td class="value_bold">([^<]+)</td></tr>',
            html_content
        )
        if typ_match:
            metadata["doc_type"] = html.unescape(typ_match.group(1).strip())
        else:
            # Try from predpisTyp div
            typ_div = re.search(r'<div class="predpisTyp"[^>]*>([^<]+)</div>', html_content)
            if typ_div:
                metadata["doc_type"] = typ_div.group(1).strip()

        # Extract effective dates from header
        ucinnost_match = re.search(
            r'data-from="(\d{4}-\d{2}-\d{2})"[^>]*data-to="([^"]*)"',
            html_content
        )
        if ucinnost_match:
            metadata["effective_from"] = ucinnost_match.group(1)
            metadata["effective_to"] = ucinnost_match.group(2) if ucinnost_match.group(2) else ""

        return metadata

    def _fetch_document(self, year: int, number: int, version: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a single document version.

        Returns dict with metadata and full_text, or None if failed.
        """
        url = f"/static/SK/ZZ/{year}/{number}/{version}.html"

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()

            content = self._get_text_utf8(resp)

            # Extract text and metadata
            full_text = self._extract_text_from_html(content)
            metadata = self._extract_metadata(content, year, number, version)

            if not full_text or len(full_text) < 50:
                logger.warning(f"Insufficient text content for {year}/{number}: {len(full_text)} chars")
                return None

            metadata["full_text"] = full_text
            metadata["url"] = f"{BASE_URL}{url}"

            return metadata

        except Exception as e:
            logger.warning(f"Failed to fetch document {year}/{number}/{version}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from Slovak Collection of Laws.

        Iterates through years, then laws, fetching the latest version of each.
        """
        for year in YEARS_TO_SCRAPE:
            logger.info(f"Fetching documents from {year}...")

            laws = self._get_laws_for_year(year)
            if not laws:
                continue

            for number, title in laws:
                # Get the latest version
                version = self._get_latest_version(year, number)
                if not version:
                    logger.warning(f"No version found for {year}/{number}")
                    continue

                doc = self._fetch_document(year, number, version)
                if doc:
                    # Use the title from the listing if we didn't get one
                    if not doc.get("title"):
                        doc["title"] = f"{number}/{year} Z. z. {title}"
                    yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents from recent years.

        Since we can't query by modification date, fetch recent years.
        """
        since_year = since.year
        current_year = datetime.now().year

        years_to_check = list(range(current_year, since_year - 1, -1))

        for year in years_to_check:
            logger.info(f"Checking year {year} for updates...")

            laws = self._get_laws_for_year(year)
            for number, title in laws:
                version = self._get_latest_version(year, number)
                if not version:
                    continue

                doc = self._fetch_document(year, number, version)
                if doc:
                    if not doc.get("title"):
                        doc["title"] = f"{number}/{year} Z. z. {title}"

                    # Filter by effective date if available
                    eff_date = doc.get("effective_from", "")
                    if eff_date:
                        try:
                            eff_datetime = datetime.strptime(eff_date, "%Y-%m-%d")
                            if eff_datetime.replace(tzinfo=timezone.utc) < since:
                                continue
                        except:
                            pass
                    yield doc

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        doc_id = raw.get("doc_id", "")
        year = raw.get("year", 0)
        number = raw.get("number", 0)
        version = raw.get("version", "")

        title = raw.get("title", "")
        if not title:
            title = f"{number}/{year} Z. z."

        full_text = raw.get("full_text", "")

        # Determine effective date
        date = raw.get("effective_from", "")
        if not date and version and version != "vyhlasene_znenie":
            # Parse date from version string like "20240201"
            try:
                date = f"{version[:4]}-{version[4:6]}-{version[6:8]}"
            except:
                pass

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "SK/CollectionOfLaws",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": raw.get("url", f"{BASE_URL}/static/SK/ZZ/{year}/{number}/"),
            # Additional metadata
            "doc_id": doc_id,
            "doc_type": raw.get("doc_type", ""),
            "year": year,
            "number": number,
            "version": version,
            "effective_from": raw.get("effective_from", ""),
            "effective_to": raw.get("effective_to", ""),
            "language": "sk",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Slovak Slov-Lex endpoints...")

        # Test year listing
        print("\n1. Testing year listing...")
        try:
            years = self._get_years()
            print(f"   Found {len(years)} years")
            if years:
                print(f"   Recent years: {years[:5]}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test law listing
        print("\n2. Testing law listing for 2024...")
        try:
            laws = self._get_laws_for_year(2024)
            print(f"   Found {len(laws)} laws")
            if laws:
                print(f"   Sample: {laws[0]}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test version listing
        print("\n3. Testing version listing...")
        try:
            if laws:
                test_num = laws[0][0]
                version = self._get_latest_version(2024, test_num)
                print(f"   Law 2024/{test_num} latest version: {version}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test document fetch
        print("\n4. Testing full document fetch...")
        try:
            if laws and version:
                test_num = laws[0][0]
                doc = self._fetch_document(2024, test_num, version)
                if doc:
                    print(f"   Title: {doc.get('title', 'N/A')[:80]}...")
                    print(f"   Type: {doc.get('doc_type', 'N/A')}")
                    print(f"   Text length: {len(doc.get('full_text', ''))} characters")
                    print(f"   Effective from: {doc.get('effective_from', 'N/A')}")
                    if doc.get('full_text'):
                        preview = doc['full_text'][:300].replace('\n', ' ')
                        print(f"   Text preview: {preview}...")
                else:
                    print("   ERROR: No document returned")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = SlovLexScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

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
