#!/usr/bin/env python3
"""
PT/ConstitutionalCourt -- Portuguese Constitutional Court Case Law Fetcher

Fetches Constitutional Court decisions (Acórdãos) from the official website
of the Tribunal Constitucional de Portugal.

Strategy:
  - Direct URL access: decisions follow pattern /tc/acordaos/{YYYY}{NNNN}.html
  - Full text: HTML pages contain complete decision text in div.textoacordao
  - Metadata: extracted from HTML structure (case number, rapporteur, date, etc.)
  - Enumeration: iterate through year/number combinations

Endpoints:
  - Decision page: https://www.tribunalconstitucional.pt/tc/acordaos/{YYYY}{NNNN}.html
  - Search interface: https://acordaosv22.tribunalconstitucional.pt/ (for reference)

Data:
  - Case types: Abstract/concrete judicial review, electoral, political parties
  - Coverage: 1983 to present
  - License: Public (open government data)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
  python bootstrap.py update             # Incremental update (recent decisions)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone, timedelta
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
logger = logging.getLogger("legal-data-hunter.PT.constitutionalcourt")

# Base URL for Constitutional Court
BASE_URL = "https://www.tribunalconstitucional.pt"

# URL pattern for decisions
DECISION_URL_PATTERN = "/tc/acordaos/{year}{number:04d}.html"


class ConstitutionalCourtScraper(BaseScraper):
    """
    Scraper for PT/ConstitutionalCourt -- Portuguese Constitutional Court.
    Country: PT
    URL: https://www.tribunalconstitucional.pt

    Data types: case_law
    Auth: none (Public government data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
                "Connection": "close",  # Required: server has issues with keep-alive
            },
            timeout=60,
        )

    def _build_decision_url(self, year: int, number: int) -> str:
        """Build URL for a specific decision."""
        return DECISION_URL_PATTERN.format(year=year, number=number)

    def _fetch_decision(self, year: int, number: int) -> Optional[Dict[str, Any]]:
        """
        Fetch a single decision by year and number.

        Returns dict with raw data or None if not found.
        """
        url = self._build_decision_url(year, number)

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)

            if resp.status_code == 404:
                return None

            # Check if we got an error page
            if resp.status_code != 200:
                logger.debug(f"Non-200 status for {url}: {resp.status_code}")
                return None

            content = resp.content.decode("utf-8", errors="replace")

            # Verify it's an actual decision page (has textoacordao div)
            if 'class="textoacordao' not in content:
                return None

            # Extract full text and metadata
            full_text = self._extract_text(content)
            if not full_text or len(full_text) < 100:
                logger.warning(f"No substantial text for decision {year}/{number}")
                return None

            metadata = self._extract_metadata(content, year, number)

            return {
                "year": year,
                "number": number,
                "url": f"{BASE_URL}{url}",
                "full_text": full_text,
                "metadata": metadata,
            }

        except Exception as e:
            logger.warning(f"Error fetching decision {year}/{number}: {e}")
            return None

    def _extract_text(self, html_content: str) -> str:
        """
        Extract full text from the decision HTML page.

        The main content is in <div class="textoacordao ...">
        """
        # Find the textoacordao div — use GREEDY match to capture all content
        match = re.search(
            r'<div class="textoacordao[^"]*"[^>]*>(.*)</article>',
            html_content,
            re.DOTALL | re.IGNORECASE
        )

        if not match:
            # Fallback: greedy match to end-of-content markers
            match = re.search(
                r'<div class="textoacordao[^"]*"[^>]*>(.*?)(?:</div>\s*</div>\s*<!--\s*fim)',
                html_content,
                re.DOTALL | re.IGNORECASE
            )

        if not match:
            # Last fallback: grab everything after textoacordao until footer/nav
            match = re.search(
                r'<div class="textoacordao[^"]*"[^>]*>(.*?)(?:<footer|<nav|</body)',
                html_content,
                re.DOTALL | re.IGNORECASE
            )

        if not match:
            return ""

        text = match.group(1)

        # Strip HTML to plain text
        text = self._strip_html(text)

        return text.strip()

    def _strip_html(self, text: str) -> str:
        """Remove all HTML tags and clean up text content."""
        # Remove HTML comments
        text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)

        # Remove style tags and their content
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)

        # Remove script tags
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)

        # Convert <br> and block-level closing tags to newlines
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</(?:p|div|li|tr|h[1-6]|blockquote|article|section)>', '\n', text, flags=re.IGNORECASE)

        # Remove ALL remaining HTML tags (handles nested tags in anchors etc.)
        text = re.sub(r'<[^>]+>', '', text)

        # Decode HTML entities
        text = html.unescape(text)

        # Clean up whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' +', ' ', text)
        text = re.sub(r'\t+', ' ', text)

        # Strip lines
        lines = [line.strip() for line in text.split('\n')]
        text = '\n'.join(lines)

        return text

    def _extract_metadata(self, html_content: str, year: int, number: int) -> Dict[str, Any]:
        """
        Extract metadata from the decision HTML.

        Looks for:
        - Processo n.º (case number)
        - Relator (rapporteur)
        - Date of decision
        - Type of proceeding
        """
        metadata = {
            "decision_number": number,
            "decision_year": year,
        }

        # Try to extract title
        title_match = re.search(r'<title>TC[^>]*>\s*([^<]+)</title>', html_content)
        if title_match:
            metadata["title"] = html.unescape(title_match.group(1).strip())

        # Extract case number (Processo n.º)
        processo_match = re.search(
            r'Processo\s+n\.?[ºo°]\s*(\d+[-/]?\d*)',
            html_content,
            re.IGNORECASE
        )
        if processo_match:
            metadata["case_number"] = processo_match.group(1)

        # Extract rapporteur (Relator)
        relator_match = re.search(
            r'Relator[a]?:\s*(?:Conselheiro|Conselheira)?\s*([^<\n]+)',
            html_content,
            re.IGNORECASE
        )
        if relator_match:
            rapporteur = relator_match.group(1).strip()
            # Clean up HTML entities and extra whitespace
            rapporteur = html.unescape(rapporteur)
            rapporteur = re.sub(r'\s+', ' ', rapporteur).strip()
            if rapporteur and len(rapporteur) < 100:  # Sanity check
                metadata["rapporteur"] = rapporteur

        # Extract formation (Plenário, 1ª Secção, 2ª Secção, 3ª Secção)
        formation_match = re.search(
            r'(Plen[áa]rio|1[ªa]\s*Sec[çc][ãa]o|2[ªa]\s*Sec[çc][ãa]o|3[ªa]\s*Sec[çc][ãa]o)',
            html_content,
            re.IGNORECASE
        )
        if formation_match:
            metadata["formation"] = formation_match.group(1)

        return metadata

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all decisions from the Portuguese Constitutional Court.

        Iterates through years and decision numbers sequentially.
        Stops when hitting consecutive failures (end of decisions for year).
        """
        current_year = datetime.now().year
        start_year = 1983  # First year of Constitutional Court

        for year in range(current_year, start_year - 1, -1):
            logger.info(f"Processing year {year}...")

            consecutive_failures = 0
            max_consecutive_failures = 10  # Stop after 10 consecutive failures
            number = 1
            found_count = 0

            while consecutive_failures < max_consecutive_failures and number <= 2000:
                decision = self._fetch_decision(year, number)

                if decision:
                    consecutive_failures = 0
                    found_count += 1
                    yield decision
                else:
                    consecutive_failures += 1

                number += 1

            logger.info(f"Year {year}: found {found_count} decisions")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield recent decisions.

        Fetches decisions from the current year and optionally previous year.
        """
        current_year = datetime.now().year
        years_to_check = [current_year]

        if since.year < current_year:
            years_to_check.append(current_year - 1)

        for year in years_to_check:
            logger.info(f"Checking year {year} for updates...")

            consecutive_failures = 0
            max_consecutive_failures = 10
            number = 1

            while consecutive_failures < max_consecutive_failures and number <= 2000:
                decision = self._fetch_decision(year, number)

                if decision:
                    consecutive_failures = 0
                    yield decision
                else:
                    consecutive_failures += 1

                number += 1

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw decision data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        year = raw.get("year", 0)
        number = raw.get("number", 0)
        metadata = raw.get("metadata", {})

        # Build ID
        doc_id = f"TC-{year}-{number:04d}"

        # Build title
        title = metadata.get("title", f"Acórdão {number}/{year}")
        if not title or title.startswith("TC >"):
            title = f"Acórdão {number}/{year}"

        # Extract date from case number if available (format: NNNN/YYYY)
        case_number = metadata.get("case_number", "")
        date_str = f"{year}-01-01"  # Default to year start

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "PT/ConstitutionalCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": raw.get("full_text", ""),  # MANDATORY FULL TEXT
            "date": date_str,
            "url": raw.get("url", f"{BASE_URL}/tc/acordaos/{year}{number:04d}.html"),
            # Specific fields for case law
            "decision_number": number,
            "decision_year": year,
            "case_number": case_number,
            "rapporteur": metadata.get("rapporteur", ""),
            "formation": metadata.get("formation", ""),
            # Source info
            "court": "Tribunal Constitucional",
            "jurisdiction": "PT",
            "language": "pt",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Portuguese Constitutional Court endpoints...")

        # Test 1: Recent decision
        print("\n1. Testing recent decision fetch...")
        current_year = datetime.now().year
        for year in [current_year, current_year - 1]:
            for num in [1, 2, 3, 5, 10]:
                try:
                    decision = self._fetch_decision(year, num)
                    if decision:
                        print(f"   Found decision {year}/{num}")
                        print(f"   Text length: {len(decision['full_text'])} characters")
                        print(f"   URL: {decision['url']}")
                        if decision["metadata"].get("rapporteur"):
                            print(f"   Rapporteur: {decision['metadata']['rapporteur']}")
                        print(f"   Sample text: {decision['full_text'][:300]}...")
                        break
                except Exception as e:
                    print(f"   Error for {year}/{num}: {e}")
            else:
                continue
            break

        # Test 2: Historical decision
        print("\n2. Testing historical decision (2020)...")
        try:
            decision = self._fetch_decision(2020, 1)
            if decision:
                print(f"   Found decision 2020/1")
                print(f"   Text length: {len(decision['full_text'])} characters")
                if decision["metadata"].get("case_number"):
                    print(f"   Case number: {decision['metadata']['case_number']}")
        except Exception as e:
            print(f"   Error: {e}")

        # Test 3: Very old decision
        print("\n3. Testing oldest decisions (1983)...")
        try:
            decision = self._fetch_decision(1983, 1)
            if decision:
                print(f"   Found decision 1983/1")
                print(f"   Text length: {len(decision['full_text'])} characters")
            else:
                # Try a few more numbers
                for num in range(2, 20):
                    decision = self._fetch_decision(1983, num)
                    if decision:
                        print(f"   Found decision 1983/{num}")
                        print(f"   Text length: {len(decision['full_text'])} characters")
                        break
        except Exception as e:
            print(f"   Error: {e}")

        print("\nTest complete!")


def main():
    scraper = ConstitutionalCourtScraper()

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
