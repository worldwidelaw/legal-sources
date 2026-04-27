#!/usr/bin/env python3
"""
GR/NSK -- Greek Legal Council of the State (Νομικό Συμβούλιο του Κράτους)

Fetches legal opinions (γνωμοδοτήσεις) from NSK, the official legal advisory body
that provides binding legal advice to the Greek government since 1951.

Strategy:
  - Search API via Liferay portal POST endpoint
  - Fetch opinion detail pages for metadata extraction
  - Optional PDF download for full document (if metadata isn't sufficient)
  - Full text from summary (Περίληψη) field which contains the legal conclusion

Data types: doctrine (official government legal opinions)
Auth: none (open data)
License: Public domain (official government acts)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from html import unescape
from html.parser import HTMLParser

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GR.NSK")

# API configuration
BASE_URL = "https://www.nsk.gr"
SEARCH_URL = f"{BASE_URL}/web/nsk/anazitisi-gnomodoteseon"
PORTLET_ID = "nskconsulatories_WAR_nskplatformportlet"

# Status codes
STATUS_MAP = {
    "1": "Αποδεκτή",         # Accepted
    "0": "Μη αποδεκτή",      # Not Accepted
    "2": "Εν μέρει αποδεκτή", # Partially Accepted
    "-1": "Εκκρεμεί αποδοχή", # Pending
    "3": "Ανακλήθηκε το ερώτημα", # Withdrawn
    "4": "Για την αποδοχή ή μη επικοινωνήστε με τον Σχ. Επιστ. Δραστηριοτήτων κ Δημοσίων Σχέσεων"
}


class MLStripper(HTMLParser):
    """Simple HTML tag stripper."""
    def __init__(self):
        super().__init__()
        self.reset()
        self.fed = []

    def handle_data(self, d):
        self.fed.append(d)

    def get_data(self):
        return ''.join(self.fed)


def strip_html(html: str) -> str:
    """Remove HTML tags and decode entities."""
    if not html:
        return ""
    s = MLStripper()
    try:
        s.feed(html)
        text = s.get_data()
    except Exception:
        # Fallback: regex
        text = re.sub(r'<[^>]+>', ' ', html)
    text = unescape(text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


class NSKScraper(BaseScraper):
    """
    Scraper for GR/NSK -- Greek Legal Council of the State.
    Country: GR
    URL: https://www.nsk.gr

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "el-GR,el;q=0.9,en;q=0.8",
        })

    def _search_opinions_by_year(self, year: int, status: str = "1") -> List[int]:
        """
        Search for opinion IDs by year and status.

        Args:
            year: Year to search (e.g., 2024)
            status: Status filter ("1" = accepted, "null" = all)

        Returns:
            List of opinion IDs (consultId values)
        """
        self.rate_limiter.wait()

        try:
            # POST request to search endpoint
            url = f"{SEARCH_URL}?p_p_id={PORTLET_ID}&p_p_lifecycle=0&p_p_state=normal&p_p_mode=view&p_p_col_id=column-4&p_p_col_pos=2&p_p_col_count=3"

            data = {
                f"_{PORTLET_ID}_isSearch": "1",
                f"_{PORTLET_ID}_inputDatefrom": str(year),
                f"_{PORTLET_ID}_consulState": status,
                f"_{PORTLET_ID}_inputKeywords": "",
                f"_{PORTLET_ID}_inputRelated": "",
                f"_{PORTLET_ID}_inputSuggestionNo": "",
            }

            resp = self.session.post(url, data=data, timeout=60)
            resp.raise_for_status()

            # Extract opinion IDs from response HTML
            # Pattern: consultId=NNNNNN
            ids = re.findall(r'consultId=(\d+)', resp.text)
            # Remove duplicates while preserving order
            seen = set()
            unique_ids = []
            for id_str in ids:
                if id_str not in seen:
                    seen.add(id_str)
                    unique_ids.append(int(id_str))

            logger.debug(f"Year {year}: found {len(unique_ids)} opinions")
            return unique_ids

        except Exception as e:
            logger.warning(f"Error searching opinions for year {year}: {e}")
            return []

    def _fetch_opinion_detail(self, consult_id: int) -> Optional[Dict[str, Any]]:
        """
        Fetch full details for a single opinion.

        Args:
            consult_id: The opinion ID

        Returns:
            Dict with opinion data or None if failed
        """
        self.rate_limiter.wait()

        try:
            url = f"{SEARCH_URL}?p_p_id={PORTLET_ID}&p_p_lifecycle=0&p_p_state=normal&p_p_mode=view&_{PORTLET_ID}_jspPage=%2Fjsps%2Fconsulatories%2Fview-consultatory.jsp&_{PORTLET_ID}_consultId={consult_id}"

            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()

            html = resp.text

            # Extract data from HTML table
            data = {
                "consult_id": consult_id,
                "number": None,
                "year": None,
                "title": None,
                "president": None,
                "rapporteur": None,
                "summary": None,
                "provisions": None,
                "keywords": None,
                "status": None,
            }

            # Parse each field from HTML
            # Number: Αριθμός
            number_match = re.search(r'<td[^>]*><strong>Αριθμός\s*</strong></td>\s*<td[^>]*>:</td>\s*<td[^>]*>\s*(\d+)', html)
            if number_match:
                data["number"] = int(number_match.group(1).strip())

            # Year: Έτος
            year_match = re.search(r'<td[^>]*><strong>Έτος\s*</strong></td>\s*<td[^>]*>:</td>\s*<td[^>]*>\s*(\d{4})', html)
            if year_match:
                data["year"] = int(year_match.group(1).strip())

            # Title: Τίτλος
            title_match = re.search(r'<td[^>]*><strong>Τίτλος\s*</strong></td>\s*<td[^>]*>:</td>\s*<td[^>]*>(.*?)</td>', html, re.DOTALL)
            if title_match:
                data["title"] = strip_html(title_match.group(1))

            # President: Πρόεδρος/Προεδρεύων
            pres_match = re.search(r'<td[^>]*><strong>Πρόεδρος/Προεδρεύων\s*</strong></td>\s*<td[^>]*>:</td>\s*<td[^>]*>(.*?)</td>', html, re.DOTALL)
            if pres_match:
                data["president"] = strip_html(pres_match.group(1))

            # Rapporteur: Εισηγητής/Γνωμοδοτών
            rapp_match = re.search(r'<td[^>]*><strong>Εισηγητής/Γνωμοδοτών\s*</strong></td>\s*<td[^>]*>:</td>\s*<td[^>]*>(.*?)</td>', html, re.DOTALL)
            if rapp_match:
                data["rapporteur"] = strip_html(rapp_match.group(1))

            # Summary (this is the main content): Περίληψη
            summary_match = re.search(r'<td[^>]*><strong>Περίληψη\s*</strong></td>\s*<td[^>]*>:</td>\s*<td[^>]*>(.*?)</td>', html, re.DOTALL)
            if summary_match:
                data["summary"] = strip_html(summary_match.group(1))

            # Provisions: Διατάξεις
            prov_match = re.search(r'<td[^>]*><strong>Διατάξεις\s*</strong></td>\s*<td[^>]*>:</td>\s*<td[^>]*>(.*?)</td>', html, re.DOTALL)
            if prov_match:
                data["provisions"] = strip_html(prov_match.group(1))

            # Keywords: Λήμματα
            kw_match = re.search(r'<td[^>]*><strong>Λήμματα</strong></td>\s*<td[^>]*>:</td>\s*<td[^>]*>(.*?)</td>', html, re.DOTALL)
            if kw_match:
                data["keywords"] = strip_html(kw_match.group(1))

            # Status: Κατάσταση
            status_match = re.search(r'<td[^>]*><strong>Κατάσταση</strong>\s*</td>\s*<td[^>]*>:</td>\s*<td[^>]*>(.*?)</td>', html, re.DOTALL)
            if status_match:
                data["status"] = strip_html(status_match.group(1))

            # Validate we got essential fields
            if not data["number"] or not data["year"]:
                logger.warning(f"Missing number or year for opinion {consult_id}")
                return None

            return data

        except Exception as e:
            logger.warning(f"Error fetching opinion {consult_id}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all NSK opinions from all years."""
        current_year = datetime.now().year

        # Start from current year and go back to 1951
        for year in range(current_year, 1950, -1):
            logger.info(f"Fetching opinions for year {year}...")

            # Get all opinion IDs for this year (all statuses)
            opinion_ids = self._search_opinions_by_year(year, status="null")

            if not opinion_ids:
                logger.debug(f"No opinions found for year {year}")
                continue

            logger.info(f"Year {year}: {len(opinion_ids)} opinions to fetch")

            for consult_id in opinion_ids:
                opinion = self._fetch_opinion_detail(consult_id)
                if opinion:
                    yield opinion

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield opinions published since the given date."""
        current_year = datetime.now().year
        since_year = since.year

        # Fetch from current year back to since_year
        for year in range(current_year, since_year - 1, -1):
            logger.info(f"Checking updates for year {year}...")

            opinion_ids = self._search_opinions_by_year(year, status="null")

            for consult_id in opinion_ids:
                opinion = self._fetch_opinion_detail(consult_id)
                if opinion:
                    yield opinion

    def normalize(self, raw: dict) -> dict:
        """Transform raw NSK opinion data to standard schema."""
        consult_id = raw["consult_id"]
        number = raw.get("number")
        year = raw.get("year")

        # Build unique ID
        doc_id = f"NSK-{year}-{number}" if number and year else f"NSK-{consult_id}"

        # Build title: if no title, use number/year
        title = raw.get("title")
        if not title:
            title = f"Γνωμοδότηση ΝΣΚ {number}/{year}" if number and year else f"Γνωμοδότηση {consult_id}"
        elif len(title) > 200:
            title = title[:200] + "..."

        # Build full text from available content
        # The main content is in the summary (Περίληψη) which contains the legal conclusion
        # Plus the title which often contains the legal question
        text_parts = []

        if raw.get("title"):
            text_parts.append(f"ΕΡΩΤΗΜΑ:\n{raw['title']}")

        if raw.get("summary"):
            text_parts.append(f"\nΑΠΑΝΤΗΣΗ:\n{raw['summary']}")

        full_text = "\n".join(text_parts)

        # Parse keywords into list
        keywords = []
        if raw.get("keywords"):
            keywords = [k.strip() for k in raw["keywords"].split(",")]

        # Build date (year only available, use Jan 1)
        date = None
        if year:
            try:
                date = datetime(year, 1, 1, tzinfo=timezone.utc).isoformat()
            except (ValueError, TypeError):
                pass

        # Build URL to detail page
        url = f"{SEARCH_URL}?p_p_id={PORTLET_ID}&p_p_lifecycle=0&p_p_state=normal&p_p_mode=view&_{PORTLET_ID}_jspPage=%2Fjsps%2Fconsulatories%2Fview-consultatory.jsp&_{PORTLET_ID}_consultId={consult_id}"

        return {
            "_id": doc_id,
            "_source": "GR/NSK",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": date,
            "url": url,
            "consult_id": consult_id,
            "opinion_number": number,
            "year": year,
            "president": raw.get("president"),
            "rapporteur": raw.get("rapporteur"),
            "provisions": raw.get("provisions"),
            "keywords": keywords,
            "status": raw.get("status"),
        }

    def _fetch_sample(self, sample_size: int = 12) -> list:
        """Fetch sample records for validation."""
        samples = []
        current_year = datetime.now().year

        # Sample from recent years
        years_to_check = [current_year, current_year - 1, current_year - 2, 2020, 2010, 2000, 1990]

        for year in years_to_check:
            if len(samples) >= sample_size:
                break

            logger.info(f"Fetching sample opinions from year {year}...")
            opinion_ids = self._search_opinions_by_year(year, status="1")  # Just accepted ones

            # Take first few from each year
            for consult_id in opinion_ids[:3]:
                if len(samples) >= sample_size:
                    break

                opinion = self._fetch_opinion_detail(consult_id)
                if opinion:
                    normalized = self.normalize(opinion)
                    samples.append(normalized)
                    text_len = len(normalized.get("text", ""))
                    logger.info(f"  -> {normalized['_id']}: {text_len} chars")

        return samples


def main():
    import argparse

    parser = argparse.ArgumentParser(description="GR/NSK Data Fetcher - Greek Legal Council of the State")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch sample records for validation")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NSKScraper()

    if args.command == "test":
        print("Testing GR/NSK API connection...")

        # Test search for recent year
        print(f"Searching opinions for year 2024...")
        ids = scraper._search_opinions_by_year(2024, status="1")
        print(f"Found {len(ids)} accepted opinions")

        if ids:
            # Test fetching one opinion
            test_id = ids[0]
            print(f"\nFetching opinion {test_id}...")
            opinion = scraper._fetch_opinion_detail(test_id)

            if opinion:
                print(f"SUCCESS: Retrieved opinion {test_id}")
                print(f"  Number: {opinion.get('number')}/{opinion.get('year')}")
                print(f"  Title: {opinion.get('title', 'N/A')[:80]}...")
                print(f"  Summary: {len(opinion.get('summary', ''))} chars")
                print(f"  Status: {opinion.get('status')}")

                normalized = scraper.normalize(opinion)
                print(f"\nNormalized record:")
                print(f"  _id: {normalized['_id']}")
                print(f"  Text length: {len(normalized.get('text', ''))} chars")
            else:
                print("FAILED: Could not fetch opinion details")
                sys.exit(1)
        else:
            print("FAILED: Could not find any opinions")
            sys.exit(1)

    elif args.command == "bootstrap":
        if args.sample:
            print("Fetching sample records from GR/NSK (Legal Council of the State)...")

            samples = scraper._fetch_sample(sample_size=12)

            # Save samples
            sample_dir = scraper.source_dir / "sample"
            sample_dir.mkdir(exist_ok=True)

            for record in samples:
                filepath = sample_dir / f"{record['_id']}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"\nSaved {len(samples)} sample records to {sample_dir}/")

            # Print summary
            if samples:
                text_lengths = [len(s.get("text", "")) for s in samples]
                avg_len = sum(text_lengths) / len(text_lengths)
                print(f"Average text length: {avg_len:.0f} characters")
                print(f"Min text length: {min(text_lengths)} chars")
                print(f"Max text length: {max(text_lengths)} chars")

                # Verify all have text
                empty = sum(1 for s in samples if not s.get("text"))
                if empty:
                    print(f"WARNING: {empty} records have no text!")
                else:
                    print("All records have text content.")
        else:
            print("Full bootstrap would fetch all opinions from 1951 to present.")
            print("Use --sample flag to fetch sample records first.")

    elif args.command == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=365)
        print(f"Fetching updates since {since.isoformat()}...")

        count = 0
        for raw in scraper.fetch_updates(since):
            normalized = scraper.normalize(raw)
            print(f"  {normalized['_id']}: {len(normalized.get('text', ''))} chars")
            count += 1
            if count >= 20:  # Limit for update demo
                print("  ... (limited to 20 for demo)")
                break

        print(f"\nFetched {count} opinions")


if __name__ == "__main__":
    main()
