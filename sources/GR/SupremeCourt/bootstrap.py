#!/usr/bin/env python3
"""
GR/SupremeCourt -- Greek Supreme Court (Areios Pagos) Data Fetcher

Fetches Greek Supreme Court case law from the official website.

Strategy:
  - Search endpoint returns all decisions for a given year
  - Decision pages contain full text of court rulings
  - HTML content is in windows-1253 encoding (Greek)

Endpoints:
  - Search: POST https://www.areiospagos.gr/nomologia/apofaseis_result.asp?S=1
  - Display: GET https://www.areiospagos.gr/nomologia/apofaseis_DISPLAY.asp?cd={cd}&apof={num}_{year}

Data:
  - Decisions from 2006 to present
  - Language: Greek (EL)
  - Rate limit: 1-2 requests/second

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
from urllib.parse import urlencode, parse_qs, urlparse

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GR.supremecourt")

# Base URL for Greek Supreme Court
BASE_URL = "https://www.areiospagos.gr"

# Years to scrape (most recent first)
YEARS_TO_SCRAPE = list(range(2025, 2005, -1))  # 2025 down to 2006


class GreekSupremeCourtScraper(BaseScraper):
    """
    Scraper for GR/SupremeCourt -- Greek Supreme Court (Areios Pagos).
    Country: GR
    URL: https://www.areiospagos.gr

    Data types: case_law
    Auth: none (Open public access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "el,en",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=60,
        )

    def _search_year(self, year: int) -> List[Dict[str, Any]]:
        """
        Search for all decisions in a given year.

        Returns list of dicts with: cd, apof, number, year, chamber
        """
        decisions = []

        try:
            # Prepare search form data
            form_data = {
                "X_TMHMA": "6",  # All chambers
                "X_SUB_TMHMA": "1",  # All sub-chambers
                "X_TELESTIS_number": "1",  # = operator
                "x_number": "",  # No specific number
                "X_TELESTIS_ETOS": "1",  # = operator
                "x_ETOS": str(year),  # Target year
            }

            self.rate_limiter.wait()
            resp = self.client.session.post(
                f"{BASE_URL}/nomologia/apofaseis_result.asp?S=1",
                data=form_data,
                headers={
                    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "text/html,application/xhtml+xml",
                },
                timeout=60,
            )
            resp.raise_for_status()

            # Decode from windows-1253
            content = resp.content.decode('windows-1253', errors='replace')

            # Extract decision links
            # Pattern: href="apofaseis_DISPLAY.asp?cd=XXX&apof=NUM_YEAR&info=CHAMBER"
            pattern = re.compile(
                r'href="apofaseis_DISPLAY\.asp\?cd=([^&]+)&apof=(\d+)_(\d{4})(?:&info=([^"]*))?"',
                re.IGNORECASE
            )

            seen = set()
            for match in pattern.finditer(content):
                cd = match.group(1)
                number = int(match.group(2))
                dec_year = int(match.group(3))
                chamber = match.group(4) or ""

                # Decode chamber info
                chamber = html.unescape(chamber)

                key = f"{number}_{dec_year}"
                if key in seen:
                    continue
                seen.add(key)

                decisions.append({
                    "cd": cd,
                    "apof": f"{number}_{dec_year}",
                    "number": number,
                    "year": dec_year,
                    "chamber": chamber,
                })

            logger.info(f"Found {len(decisions)} decisions for year {year}")
            return decisions

        except Exception as e:
            logger.error(f"Failed to search year {year}: {e}")
            return []

    def _fetch_decision(self, cd: str, apof: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a single decision's full text and metadata.

        Returns dict with: title, text, date, chamber, or None on failure.
        """
        try:
            url = f"/nomologia/apofaseis_DISPLAY.asp?cd={cd}&apof={apof}"

            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()

            # Decode from windows-1253
            content = resp.content.decode('windows-1253', errors='replace')

            # Extract text from the main content
            # The decision text is in a <p align=justify style='line-height: 160%'> block
            text = self._extract_text(content)

            if not text or len(text) < 100:
                logger.warning(f"Very short text for {apof}: {len(text)} chars")

            # Extract metadata from the page
            title = self._extract_title(content, apof)
            date = self._extract_date(content)
            chamber = self._extract_chamber(content)

            return {
                "title": title,
                "text": text,
                "date": date,
                "chamber": chamber,
            }

        except Exception as e:
            logger.warning(f"Failed to fetch decision {apof}: {e}")
            return None

    def _extract_text(self, content: str) -> str:
        """Extract clean text from decision HTML."""
        text_parts = []

        # Find the main content block
        # Looking for text after the title line "Απόφαση X / YYYY"
        start_marker = re.search(r"Απόφαση\s+<b>\d+\s*/\s*\d{4}</b>", content)
        if start_marker:
            content = content[start_marker.end():]

        # Remove script and style tags
        content = re.sub(r'<script[^>]*>.*?</script>', '', content, flags=re.DOTALL | re.IGNORECASE)
        content = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL | re.IGNORECASE)

        # Remove HTML comments
        content = re.sub(r'<!--.*?-->', '', content, flags=re.DOTALL)

        # Find the main justified text block
        justified = re.search(r"<p[^>]*align=justify[^>]*>(.*?)</p>", content, re.DOTALL | re.IGNORECASE)
        if justified:
            content = justified.group(1)

        # Remove HTML tags but preserve paragraph structure
        content = re.sub(r'<br\s*/?>', '\n', content, flags=re.IGNORECASE)
        content = re.sub(r'</p>', '\n\n', content, flags=re.IGNORECASE)
        content = re.sub(r'<[^>]+>', ' ', content)

        # Decode HTML entities
        text = html.unescape(content)

        # Clean up whitespace
        lines = []
        for line in text.split('\n'):
            line = re.sub(r'\s+', ' ', line).strip()
            if line:
                lines.append(line)

        return '\n'.join(lines)

    def _extract_title(self, content: str, apof: str) -> str:
        """Extract decision title from HTML."""
        # Title format: "ΑΡΕΙΟΣ ΠΑΓΟΣ - ΑΠΟΦΑΣΗ 740/2024 (Α2, ΠΟΛΙΤΙΚΕΣ)"
        title_match = re.search(r'<title>([^<]+)</title>', content, re.IGNORECASE)
        if title_match:
            title = html.unescape(title_match.group(1)).strip()
            # Clean up encoding issues
            title = title.replace('οΏ½', '')
            if title:
                return title

        # Fallback: construct from apof
        parts = apof.split('_')
        if len(parts) == 2:
            return f"ΑΡΕΙΟΣ ΠΑΓΟΣ - ΑΠΟΦΑΣΗ {parts[0]}/{parts[1]}"

        return f"ΑΡΕΙΟΣ ΠΑΓΟΣ - ΑΠΟΦΑΣΗ {apof}"

    def _extract_date(self, content: str) -> str:
        """Extract decision date from text."""
        # Greek date patterns
        # "στις 30 Ιανουαρίου 2023"
        months = {
            'Ιανουαρίου': '01', 'Φεβρουαρίου': '02', 'Μαρτίου': '03',
            'Απριλίου': '04', 'Μαΐου': '05', 'Ιουνίου': '06',
            'Ιουλίου': '07', 'Αυγούστου': '08', 'Σεπτεμβρίου': '09',
            'Οκτωβρίου': '10', 'Νοεμβρίου': '11', 'Δεκεμβρίου': '12',
        }

        pattern = re.compile(
            r'(?:στις|της)\s+(\d{1,2})\s+(' + '|'.join(months.keys()) + r')\s+(\d{4})',
            re.IGNORECASE
        )

        match = pattern.search(content)
        if match:
            day = match.group(1).zfill(2)
            month = months.get(match.group(2), '01')
            year = match.group(3)
            return f"{year}-{month}-{day}"

        # Try simpler date patterns like "30/1/2023"
        simple = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', content)
        if simple:
            day = simple.group(1).zfill(2)
            month = simple.group(2).zfill(2)
            year = simple.group(3)
            return f"{year}-{month}-{day}"

        return ""

    def _extract_chamber(self, content: str) -> str:
        """Extract chamber/division info from content."""
        # Look for patterns like "Α2' Πολιτικό Τμήμα" or "Ποινικό Τμήμα"
        patterns = [
            r'([ΑΒΓΔΕΣΤΖʹ]\d?[\'\']?\s*(?:Πολιτικ[οό]|Ποινικ[οό])\s*Τμήμα)',
            r'(Ολομέλεια)',
            r'(Ποινικ[οό]\s*Τμήμα)',
            r'(Πολιτικ[οό]\s*Τμήμα)',
        ]

        for pattern in patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                return match.group(1).strip()

        return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from the Greek Supreme Court.

        Iterates through years (newest first), fetching all decisions.
        """
        documents_yielded = 0

        for year in YEARS_TO_SCRAPE:
            logger.info(f"Processing year {year}...")

            decisions = self._search_year(year)

            for dec_info in decisions:
                cd = dec_info["cd"]
                apof = dec_info["apof"]

                # Fetch full decision
                decision = self._fetch_decision(cd, apof)

                if not decision:
                    continue

                if not decision.get("text") or len(decision.get("text", "")) < 100:
                    logger.warning(f"Skipping {apof}: no/short text")
                    continue

                yield {
                    "cd": cd,
                    "apof": apof,
                    "number": dec_info["number"],
                    "year": dec_info["year"],
                    "chamber_from_search": dec_info.get("chamber", ""),
                    "title": decision.get("title", ""),
                    "text": decision.get("text", ""),
                    "date": decision.get("date", ""),
                    "chamber": decision.get("chamber", "") or dec_info.get("chamber", ""),
                }

                documents_yielded += 1

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents from recent years.

        Since the search doesn't support date filtering, we fetch
        the last 2 years worth of decisions.
        """
        since_year = since.year
        current_year = datetime.now().year

        years_to_check = [y for y in YEARS_TO_SCRAPE if y >= since_year - 1]

        for year in years_to_check:
            logger.info(f"Checking year {year} for updates...")

            decisions = self._search_year(year)

            for dec_info in decisions:
                cd = dec_info["cd"]
                apof = dec_info["apof"]

                decision = self._fetch_decision(cd, apof)

                if not decision:
                    continue

                if not decision.get("text") or len(decision.get("text", "")) < 100:
                    continue

                yield {
                    "cd": cd,
                    "apof": apof,
                    "number": dec_info["number"],
                    "year": dec_info["year"],
                    "chamber_from_search": dec_info.get("chamber", ""),
                    "title": decision.get("title", ""),
                    "text": decision.get("text", ""),
                    "date": decision.get("date", ""),
                    "chamber": decision.get("chamber", "") or dec_info.get("chamber", ""),
                }

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        number = raw.get("number", 0)
        year = raw.get("year", 0)
        apof = raw.get("apof", f"{number}_{year}")

        # Create unique document ID
        doc_id = f"AP/{apof}"

        title = raw.get("title", "")
        text = raw.get("text", "")
        date_str = raw.get("date", "")
        chamber = raw.get("chamber", "")
        cd = raw.get("cd", "")

        # Build URL
        url = f"{BASE_URL}/nomologia/apofaseis_DISPLAY.asp?cd={cd}&apof={apof}"

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "GR/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": text,  # MANDATORY FULL TEXT
            "date": date_str,
            "url": url,
            # Additional metadata
            "number": number,
            "year": year,
            "chamber": chamber,
            "language": "el",
            "court": "Άρειος Πάγος",
            "court_en": "Supreme Court of Greece",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Greek Supreme Court endpoints...")

        # Test search page
        print("\n1. Testing search page...")
        try:
            resp = self.client.get("/nomologia/apofaseis.asp")
            print(f"   Status: {resp.status_code}")
            content = resp.content.decode('windows-1253', errors='replace')
            print(f"   Page length: {len(content)} chars")
            if "ΑΝΑΖΗΤΗΣΗ" in content or "αναζήτηση" in content.lower():
                print("   Search form found: YES")
            else:
                print("   Search form found: NO")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test year search
        print("\n2. Testing year search (2024)...")
        try:
            decisions = self._search_year(2024)
            print(f"   Found {len(decisions)} decisions")
            if decisions:
                print(f"   Sample: {decisions[0]['apof']} ({decisions[0]['chamber']})")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test decision fetch
        print("\n3. Testing decision fetch...")
        try:
            if decisions:
                dec = decisions[0]
                result = self._fetch_decision(dec["cd"], dec["apof"])
                if result:
                    print(f"   Title: {result['title'][:60]}...")
                    print(f"   Date: {result['date']}")
                    print(f"   Text length: {len(result.get('text', ''))} chars")
                    if result.get('text'):
                        print(f"   Sample text: {result['text'][:150]}...")
                else:
                    print("   ERROR: No result returned")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = GreekSupremeCourtScraper()

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
