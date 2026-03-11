#!/usr/bin/env python3
"""
IE/Acts -- Irish Statute Book Data Fetcher

Fetches Irish legislation from the electronic Irish Statute Book (eISB).

Strategy:
  - Uses ELI (European Legislation Identifier) URIs for discovery and full text.
  - Year listing pages at /eli/{year}/act/ list all acts for that year.
  - Full text HTML: /eli/{year}/act/{number}/enacted/en/html
  - Also fetches Statutory Instruments: /eli/{year}/si/{number}/made/en/html

Endpoints:
  - Acts listing: https://www.irishstatutebook.ie/eli/2024/act/
  - Act full text: https://www.irishstatutebook.ie/eli/2024/act/1/enacted/en/html
  - SI full text: https://www.irishstatutebook.ie/eli/2024/si/1/made/en/html

Data:
  - Acts from 1922 to present
  - Statutory Instruments from 1922 to present
  - Language: English (some also in Irish)
  - Rate limit: conservative 2 requests/second

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
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IE.Acts")

# Base URL for Irish Statute Book
BASE_URL = "https://www.irishstatutebook.ie"

# Years to scrape (most recent first for sample mode)
CURRENT_YEAR = datetime.now().year
YEARS_TO_SCRAPE = list(range(CURRENT_YEAR, 1921, -1))  # Current year down to 1922


class IrishStatuteBookScraper(BaseScraper):
    """
    Scraper for IE/Acts -- Irish Statute Book.
    Country: IE
    URL: https://www.irishstatutebook.ie

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
                "Accept-Language": "en,ga",
            },
            timeout=60,
        )

    def _get_acts_for_year(self, year: int) -> List[Dict[str, Any]]:
        """
        Parse the year listing page to get all acts for that year.

        Returns list of dicts with: number, title
        """
        acts = []

        try:
            self.rate_limiter.wait()
            resp = self.client.get(f"/eli/{year}/act/")

            if resp.status_code == 404:
                logger.debug(f"No acts found for year {year}")
                return []

            resp.raise_for_status()
            content = resp.text

            # Parse the HTML table rows
            # Format: <tr><td>1</td><td><a href="en/act/pub/0001/index.html">Title Act 2024</a></td>...
            # The links use format: en/act/pub/NNNN/index.html
            row_pattern = re.compile(
                r'<tr>\s*<td[^>]*>\s*(\d+)\s*</td>\s*<td[^>]*>\s*<a[^>]*href="[^"]*?(?:en/act/pub/|/eli/\d+/act/)(\d+)[^"]*"[^>]*>([^<]+)</a>',
                re.IGNORECASE | re.DOTALL
            )

            seen_numbers = set()

            for match in row_pattern.finditer(content):
                display_num = int(match.group(1))
                link_num = int(match.group(2))
                title = match.group(3).strip()
                title = html.unescape(title)
                title = re.sub(r'\s+', ' ', title).strip()

                # Use display number as the act number (1-indexed)
                number = display_num

                if number not in seen_numbers and title:
                    # Skip if title is just "PDF" or navigation text
                    if title.lower() in ['pdf', 'html', 'hyperlinked', 'print', 'not yet available']:
                        continue
                    if len(title) < 5:
                        continue

                    seen_numbers.add(number)
                    acts.append({
                        "number": number,
                        "title": title,
                        "year": year,
                    })

            # Sort by act number
            acts.sort(key=lambda x: x["number"])
            logger.info(f"Found {len(acts)} acts for year {year}")
            return acts

        except Exception as e:
            logger.warning(f"Failed to get acts for year {year}: {e}")
            return []

    def _get_sis_for_year(self, year: int) -> List[Dict[str, Any]]:
        """
        Parse the year listing page to get all statutory instruments for that year.

        Returns list of dicts with: number, title
        """
        sis = []

        try:
            self.rate_limiter.wait()
            resp = self.client.get(f"/eli/{year}/si/")

            if resp.status_code == 404:
                logger.debug(f"No SIs found for year {year}")
                return []

            resp.raise_for_status()
            content = resp.text

            # Parse the HTML table rows
            # Format: <tr><td>1</td><td><a href="/2024/en/si/0001.html">Title 2024</a></td>...
            row_pattern = re.compile(
                r'<tr>\s*<td[^>]*>\s*(\d+)\s*</td>\s*<td[^>]*>\s*<a[^>]*href="[^"]*?(?:/\d+/en/si/|/eli/\d+/si/)(\d+)[^"]*"[^>]*>([^<]+)</a>',
                re.IGNORECASE | re.DOTALL
            )

            seen_numbers = set()

            for match in row_pattern.finditer(content):
                display_num = int(match.group(1))
                link_num = int(match.group(2))
                title = match.group(3).strip()
                title = html.unescape(title)
                title = re.sub(r'\s+', ' ', title).strip()

                # Use display number as the SI number
                number = display_num

                if number not in seen_numbers and title:
                    if title.lower() in ['pdf', 'html', 'hyperlinked', 'print', 'not yet available']:
                        continue
                    if len(title) < 5:
                        continue

                    seen_numbers.add(number)
                    sis.append({
                        "number": number,
                        "title": title,
                        "year": year,
                    })

            # Sort by SI number
            sis.sort(key=lambda x: x["number"])
            logger.info(f"Found {len(sis)} SIs for year {year}")
            return sis

        except Exception as e:
            logger.warning(f"Failed to get SIs for year {year}: {e}")
            return []

    def _fetch_act_full_text(self, year: int, number: int) -> tuple:
        """
        Fetch full text of an act from ELI endpoint.

        Returns (full_text, date, long_title) tuple.
        """
        try:
            url = f"/eli/{year}/act/{number}/enacted/en/html"

            self.rate_limiter.wait()
            resp = self.client.get(url)

            if resp.status_code == 404:
                logger.debug(f"Act {year}/{number} not found")
                return "", "", ""

            resp.raise_for_status()
            content = resp.text

            # Extract the date from the page
            # Format in HTML: [7<i>th February</i>, 2024] or [7th February, 2024]
            date_str = ""
            # Try the bracketed format first
            date_pattern = re.compile(
                r'\[(\d{1,2})(?:<i>)?(st|nd|rd|th)?(?:</i>)?\s*(\w+),?\s*(\d{4})\]',
                re.IGNORECASE
            )
            date_match = date_pattern.search(content)
            if date_match:
                day = date_match.group(1)
                month = date_match.group(3)
                year_d = date_match.group(4)
                date_str = self._parse_date(f"{day} {month}, {year_d}")
            else:
                # Fallback to general date pattern
                date_pattern2 = re.compile(
                    r'(\d{1,2})(?:st|nd|rd|th)?\s+(\w+),?\s+(\d{4})',
                    re.IGNORECASE
                )
                date_match2 = date_pattern2.search(content[:10000])
                if date_match2:
                    date_str = self._parse_date(f"{date_match2.group(1)} {date_match2.group(2)}, {date_match2.group(3)}")

            # Extract long title (the enacting text)
            long_title = ""
            long_title_pattern = re.compile(
                r'An Act to\s+(.+?)(?:Be it enacted|</p>|</div>)',
                re.IGNORECASE | re.DOTALL
            )
            lt_match = long_title_pattern.search(content)
            if lt_match:
                long_title = "An Act to " + lt_match.group(1).strip()
                long_title = self._clean_html_text(long_title)
                long_title = long_title[:500]  # Limit length

            # Extract full text
            full_text = self._extract_text_from_html(content)

            return full_text, date_str, long_title

        except Exception as e:
            logger.warning(f"Failed to fetch act {year}/{number}: {e}")
            return "", "", ""

    def _fetch_si_full_text(self, year: int, number: int) -> tuple:
        """
        Fetch full text of a statutory instrument from ELI endpoint.

        Returns (full_text, date) tuple.
        """
        try:
            url = f"/eli/{year}/si/{number}/made/en/html"

            self.rate_limiter.wait()
            resp = self.client.get(url)

            if resp.status_code == 404:
                logger.debug(f"SI {year}/{number} not found")
                return "", ""

            resp.raise_for_status()
            content = resp.text

            # Extract date - for SIs the format is often:
            # "Iris Oifigiúil" of 9<i>th January,</i> 2026
            # or just: 9th January, 2026
            date_str = ""
            # Try pattern with <i> tags around ordinal
            date_pattern = re.compile(
                r'(\d{1,2})(?:<i>)?(st|nd|rd|th)?(?:</i>)?\s*(\w+),?(?:</i>)?\s*(\d{4})',
                re.IGNORECASE
            )
            date_match = date_pattern.search(content[:15000])
            if date_match:
                day = date_match.group(1)
                month = date_match.group(3)
                year_d = date_match.group(4)
                # Only use if month is a valid month name
                valid_months = ['january', 'february', 'march', 'april', 'may', 'june',
                               'july', 'august', 'september', 'october', 'november', 'december']
                if month.lower() in valid_months:
                    date_str = self._parse_date(f"{day} {month}, {year_d}")

            # Extract full text
            full_text = self._extract_text_from_html(content)

            return full_text, date_str

        except Exception as e:
            logger.warning(f"Failed to fetch SI {year}/{number}: {e}")
            return "", ""

    def _parse_date(self, date_text: str) -> str:
        """Parse Irish date format to ISO 8601."""
        if not date_text:
            return ""

        # Remove ordinal suffixes
        date_text = re.sub(r'(\d)(st|nd|rd|th)', r'\1', date_text)

        # Try various date formats
        formats = [
            "%d %B, %Y",
            "%d %B %Y",
            "%d %b, %Y",
            "%d %b %Y",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_text.strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        return ""

    def _extract_text_from_html(self, content: str) -> str:
        """
        Extract clean text from HTML content.

        This removes scripts, styles, navigation, and extracts the main
        legislation text.
        """
        # Try to extract just the main content section
        main_content_match = re.search(
            r'<section[^>]*class="[^"]*main-content[^"]*"[^>]*>(.*?)</section>',
            content, re.DOTALL | re.IGNORECASE
        )
        if main_content_match:
            content = main_content_match.group(1)

        # Remove script and style tags
        content = re.sub(r'<script[^>]*>.*?</script>', '', content, flags=re.DOTALL | re.IGNORECASE)
        content = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL | re.IGNORECASE)
        content = re.sub(r'<noscript[^>]*>.*?</noscript>', '', content, flags=re.DOTALL | re.IGNORECASE)

        # Remove HTML comments
        content = re.sub(r'<!--.*?-->', '', content, flags=re.DOTALL)

        # Remove navigation/header/footer elements
        content = re.sub(r'<nav[^>]*>.*?</nav>', '', content, flags=re.DOTALL | re.IGNORECASE)
        content = re.sub(r'<header[^>]*>.*?</header>', '', content, flags=re.DOTALL | re.IGNORECASE)
        content = re.sub(r'<footer[^>]*>.*?</footer>', '', content, flags=re.DOTALL | re.IGNORECASE)
        content = re.sub(r'<ol[^>]*class="[^"]*breadcrumb[^"]*"[^>]*>.*?</ol>', '', content, flags=re.DOTALL | re.IGNORECASE)

        text_parts = []

        # Extract text from tags
        texts = re.findall(r'>([^<]+)<', content)

        for t in texts:
            t = t.strip()
            if len(t) < 2:
                continue

            # Skip navigation and boilerplate
            skip_patterns = [
                'cookie', 'javascript', 'navigation', 'menu',
                'footer', 'header', 'sidebar', 'print', 'email',
                'share', 'facebook', 'twitter', 'linkedin',
                'irishstatutebook.ie', 'www.', 'http',
                'disclaimer', 'copyright', 'privacy',
                'skip to', 'jump to', 'back to top',
                'home', 'baile', 'acts', 'achtanna',
                'statutory instruments', 'ionstraim',
                'gaeilge', 'english', 'feedback', 'helpdesk',
                'permanent page url', 'search', 'cuardach',
            ]

            lower_t = t.lower()
            if any(x in lower_t for x in skip_patterns):
                continue

            # Skip single-word items that are likely navigation
            if len(t.split()) <= 1 and len(t) < 20:
                # Allow numbers and short section references
                if not re.match(r'^[\d\.\(\)]+$', t) and not t.isdigit():
                    continue

            # Clean up the text
            clean_t = html.unescape(t)
            clean_t = re.sub(r'\s+', ' ', clean_t).strip()

            if clean_t and len(clean_t) > 1:
                text_parts.append(clean_t)

        full_text = '\n'.join(text_parts)

        # If very little text extracted, try broader extraction
        if len(full_text) < 500:
            text_parts = []
            for match in re.findall(r'>([^<]{5,})<', content):
                clean = html.unescape(match.strip())
                clean = re.sub(r'\s+', ' ', clean).strip()
                if clean:
                    text_parts.append(clean)
            full_text = '\n'.join(text_parts)

        return full_text.strip()

    def _clean_html_text(self, text: str) -> str:
        """Clean HTML from text."""
        # Remove tags
        text = re.sub(r'<[^>]+>', ' ', text)
        # Unescape HTML entities
        text = html.unescape(text)
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from the Irish Statute Book.

        Iterates through years (newest first), fetching acts and
        statutory instruments.
        """
        documents_yielded = 0

        for year in YEARS_TO_SCRAPE:
            logger.info(f"Processing year {year}...")

            # Fetch acts for this year
            acts = self._get_acts_for_year(year)

            for act in acts:
                number = act["number"]
                title = act["title"]

                # Fetch full text
                full_text, date_str, long_title = self._fetch_act_full_text(year, number)

                if not full_text:
                    logger.warning(f"No full text for Act {year}/{number}, skipping")
                    continue

                # Use title from listing if we got it, otherwise extract from text
                if not title and full_text:
                    # First meaningful line is often the title
                    lines = [l.strip() for l in full_text.split('\n') if l.strip()]
                    if lines:
                        title = lines[0][:200]

                yield {
                    "doc_type": "act",
                    "year": year,
                    "number": number,
                    "title": title,
                    "long_title": long_title,
                    "date": date_str,
                    "full_text": full_text,
                }

                documents_yielded += 1

            # Also fetch statutory instruments for this year
            sis = self._get_sis_for_year(year)

            for si in sis:
                number = si["number"]
                title = si["title"]

                full_text, date_str = self._fetch_si_full_text(year, number)

                if not full_text:
                    logger.warning(f"No full text for SI {year}/{number}, skipping")
                    continue

                if not title and full_text:
                    lines = [l.strip() for l in full_text.split('\n') if l.strip()]
                    if lines:
                        title = lines[0][:200]

                yield {
                    "doc_type": "si",
                    "year": year,
                    "number": number,
                    "title": title,
                    "long_title": "",
                    "date": date_str,
                    "full_text": full_text,
                }

                documents_yielded += 1

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents updated since the given date.

        Since ELI doesn't have a direct "modified since" filter, we
        fetch recent years and filter by date.
        """
        since_year = since.year
        current_year = datetime.now().year

        # Only check years since the "since" date
        years_to_check = [y for y in range(current_year, since_year - 1, -1)]

        for year in years_to_check:
            logger.info(f"Checking year {year} for updates...")

            # Fetch acts
            acts = self._get_acts_for_year(year)

            for act in acts:
                number = act["number"]
                title = act["title"]

                full_text, date_str, long_title = self._fetch_act_full_text(year, number)

                if not full_text:
                    continue

                # Check if document is newer than since date
                if date_str:
                    try:
                        doc_date = datetime.strptime(date_str, "%Y-%m-%d")
                        doc_date = doc_date.replace(tzinfo=timezone.utc)
                        if doc_date < since:
                            continue
                    except:
                        pass

                if not title and full_text:
                    lines = [l.strip() for l in full_text.split('\n') if l.strip()]
                    if lines:
                        title = lines[0][:200]

                yield {
                    "doc_type": "act",
                    "year": year,
                    "number": number,
                    "title": title,
                    "long_title": long_title,
                    "date": date_str,
                    "full_text": full_text,
                }

            # Fetch SIs
            sis = self._get_sis_for_year(year)

            for si in sis:
                number = si["number"]
                title = si["title"]

                full_text, date_str = self._fetch_si_full_text(year, number)

                if not full_text:
                    continue

                if date_str:
                    try:
                        doc_date = datetime.strptime(date_str, "%Y-%m-%d")
                        doc_date = doc_date.replace(tzinfo=timezone.utc)
                        if doc_date < since:
                            continue
                    except:
                        pass

                if not title and full_text:
                    lines = [l.strip() for l in full_text.split('\n') if l.strip()]
                    if lines:
                        title = lines[0][:200]

                yield {
                    "doc_type": "si",
                    "year": year,
                    "number": number,
                    "title": title,
                    "long_title": "",
                    "date": date_str,
                    "full_text": full_text,
                }

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        doc_type = raw.get("doc_type", "act")
        year = raw.get("year", 0)
        number = raw.get("number", 0)

        # Create unique document ID
        doc_id = f"{doc_type}/{year}/{number}"

        title = raw.get("title", "")
        full_text = raw.get("full_text", "")
        date_str = raw.get("date", "")
        long_title = raw.get("long_title", "")

        # Build ELI URL
        if doc_type == "act":
            eli_url = f"{BASE_URL}/eli/{year}/act/{number}/enacted/en/html"
        else:  # si
            eli_url = f"{BASE_URL}/eli/{year}/si/{number}/made/en/html"

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "IE/Acts",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_str,
            "url": eli_url,
            # Additional metadata
            "doc_id": doc_id,
            "doc_type": doc_type,
            "year": year,
            "number": number,
            "long_title": long_title,
            "language": "en",
            "eli_uri": eli_url,
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Irish Statute Book endpoints...")

        # Test acts listing
        print("\n1. Testing acts listing page (2024)...")
        try:
            resp = self.client.get("/eli/2024/act/")
            print(f"   Status: {resp.status_code}")
            acts = self._get_acts_for_year(2024)
            print(f"   Found {len(acts)} acts for 2024")
            if acts:
                print(f"   Sample: Act {acts[0]['number']} - {acts[0]['title'][:50]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test act full text
        print("\n2. Testing act full text endpoint...")
        try:
            full_text, date_str, long_title = self._fetch_act_full_text(2024, 1)
            print(f"   Text length: {len(full_text)} characters")
            print(f"   Date: {date_str}")
            if full_text:
                print(f"   Sample: {full_text[:150]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test SI listing
        print("\n3. Testing SI listing page (2024)...")
        try:
            sis = self._get_sis_for_year(2024)
            print(f"   Found {len(sis)} SIs for 2024")
            if sis:
                print(f"   Sample: SI {sis[0]['number']} - {sis[0]['title'][:50]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test SI full text
        print("\n4. Testing SI full text endpoint...")
        try:
            full_text, date_str = self._fetch_si_full_text(2024, 1)
            print(f"   Text length: {len(full_text)} characters")
            print(f"   Date: {date_str}")
            if full_text:
                print(f"   Sample: {full_text[:150]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = IrishStatuteBookScraper()

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
