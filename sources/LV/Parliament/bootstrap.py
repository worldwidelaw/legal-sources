#!/usr/bin/env python3
"""
LV/Parliament -- Latvian Parliament (Saeima) Transcripts Data Fetcher

Fetches parliamentary debate transcripts from the Latvian Saeima website.

Strategy:
  - Discover transcripts from category pages at /lv/transcripts/category/{ID}
  - Category IDs correspond to Saeima terms (5th-14th Saeima)
  - Fetch full transcript HTML at /lv/transcripts/view/{ID}
  - Extract speaker names, debate text, and session metadata

Endpoints:
  - Category pages: https://www.saeima.lv/lv/transcripts/category/{cat_id}
  - Transcripts: https://www.saeima.lv/lv/transcripts/view/{transcript_id}

Data:
  - Transcripts from 5th Saeima (1993) to 14th Saeima (current)
  - Full stenogram text with speaker attributions
  - Approximately 1,600+ transcripts total

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (latest sessions)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Set
from urllib.parse import urljoin

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.LV.parliament")

# Base URLs
BASE_URL = "https://www.saeima.lv"

# Category IDs for each Saeima term (discovered from website)
# Categories map to Saeima numbers
SAEIMA_CATEGORIES = {
    24: "5th Saeima (1993-1995)",
    25: "6th Saeima (1995-1998)",
    26: "7th Saeima (1998-2002)",
    27: "8th Saeima (2002-2006)",
    17: "11th Saeima (2011-2014)",
    21: "13th Saeima (2018-2022)",
    28: "12th Saeima (2014-2018)",
    29: "14th Saeima (2022-present)",
}

# Headers for requests
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5,lv;q=0.3",
}


class ParliamentScraper(BaseScraper):
    """
    Scraper for LV/Parliament -- Latvian Saeima Parliamentary Transcripts.
    Country: LV
    URL: https://www.saeima.lv/lv/transcripts/

    Data types: parliamentary_proceedings
    Auth: none (public)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content extracted from HTML."""
        if not text:
            return ""

        # Decode HTML entities
        text = html.unescape(text)

        # Remove excessive whitespace while preserving paragraph structure
        text = re.sub(r'\r\n', '\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)

        # Replace non-breaking spaces
        text = text.replace('\xa0', ' ')

        # Strip leading/trailing whitespace from each line
        lines = [line.strip() for line in text.split('\n')]
        text = '\n'.join(lines)

        return text.strip()

    def _extract_text_from_html(self, html_content: str) -> str:
        """
        Extract full text content from a Saeima transcript HTML page.

        The debate text is contained in <p> tags in the main content area.
        """
        if not html_content:
            return ""

        # Find the main content area (after the header section)
        # Transcripts have content after the title and metadata

        # Remove script and style tags
        html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)

        # Find all paragraph content
        p_matches = re.findall(r'<p[^>]*>(.*?)</p>', html_content, re.DOTALL | re.IGNORECASE)

        if not p_matches:
            return ""

        # Clean each paragraph and join
        paragraphs = []
        for p in p_matches:
            # Strip HTML tags within paragraph
            clean_p = re.sub(r'<[^>]+>', ' ', p)
            clean_p = self._clean_text(clean_p)
            # Skip very short paragraphs that are likely navigation or buttons
            if clean_p and len(clean_p) > 10:
                # Skip common navigation text
                if any(skip in clean_p.lower() for skip in [
                    'satura rādītājs', 'balsojumi', 'video translācija',
                    'cookie', 'sīkdat', 'privātuma politika'
                ]):
                    continue
                paragraphs.append(clean_p)

        return '\n\n'.join(paragraphs)

    def _extract_title_from_html(self, html_content: str) -> str:
        """Extract title from HTML meta tags or title element."""
        # Try og:title first
        match = re.search(r'<title>([^<]+)</title>', html_content)
        if match:
            title = html.unescape(match.group(1))
            # Remove " - Latvijas Republikas Saeima" suffix
            title = re.sub(r'\s*-\s*Latvijas Republikas Saeima$', '', title)
            return title.strip()

        return ""

    def _extract_date_from_html(self, html_content: str) -> Optional[str]:
        """
        Extract session date from transcript HTML.

        Looks for date patterns like:
        - "2026. gada 15. janvārī" (Jan 15, 2026)
        - "15.01.2026"
        """
        # Look for Latvian date format in session header
        # Pattern: "YYYY. gada DD. month_name"
        latvian_months = {
            'janvārī': '01', 'janvāris': '01',
            'februārī': '02', 'februāris': '02',
            'martā': '03', 'marts': '03',
            'aprīlī': '04', 'aprīlis': '04',
            'maijā': '05', 'maijs': '05',
            'jūnijā': '06', 'jūnijs': '06',
            'jūlijā': '07', 'jūlijs': '07',
            'augustā': '08', 'augusts': '08',
            'septembrī': '09', 'septembris': '09',
            'oktobrī': '10', 'oktobris': '10',
            'novembrī': '11', 'novembris': '11',
            'decembrī': '12', 'decembris': '12',
        }

        # Pattern: "YYYY. gada DD. month"
        pattern = r'(\d{4})\.\s*gada\s+(\d{1,2})\.\s*(\w+)'
        match = re.search(pattern, html_content, re.IGNORECASE)
        if match:
            year = match.group(1)
            day = match.group(2).zfill(2)
            month_name = match.group(3).lower()
            if month_name in latvian_months:
                month = latvian_months[month_name]
                return f"{year}-{month}-{day}"

        # Try date format DD.MM.YYYY
        match = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', html_content)
        if match:
            return f"{match.group(3)}-{match.group(2)}-{match.group(1)}"

        return None

    def _extract_saeima_number(self, html_content: str) -> Optional[int]:
        """Extract Saeima number (e.g., 14) from transcript."""
        match = re.search(r'(\d{1,2})\.\s*Saeimas', html_content)
        if match:
            return int(match.group(1))
        return None

    def _extract_session_leader(self, html_content: str) -> Optional[str]:
        """Extract session leader name from transcript."""
        # Pattern: "Sēdi vada ... priekšsēdētāj... Name"
        match = re.search(r'Sēdi vada[^<]*?([A-ZĀČĒĢĪĶĻŅŠŪŽ][a-zāčēģīķļņšūž]+\s+[A-ZĀČĒĢĪĶĻŅŠŪŽ][a-zāčēģīķļņšūž\-]+)', html_content)
        if match:
            return match.group(1)
        return None

    def _discover_transcript_ids(self, category_id: int) -> List[int]:
        """
        Discover transcript IDs from a category page.

        Returns list of transcript IDs found on the category page.
        """
        url = f"{BASE_URL}/lv/transcripts/category/{category_id}"

        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()

            # Extract transcript IDs from links
            matches = re.findall(r'transcripts/view/(\d+)', resp.text)
            ids = list(set(int(m) for m in matches))

            logger.info(f"Found {len(ids)} transcripts in category {category_id}")
            return sorted(ids)

        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to fetch category {category_id}: {e}")
            return []

    def _fetch_transcript(self, transcript_id: int) -> Optional[dict]:
        """
        Fetch a single transcript by ID.

        Returns raw dict with HTML content and extracted fields.
        """
        url = f"{BASE_URL}/lv/transcripts/view/{transcript_id}"

        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()

            html_content = resp.text

            # Extract components
            title = self._extract_title_from_html(html_content)
            full_text = self._extract_text_from_html(html_content)
            date = self._extract_date_from_html(html_content)
            saeima_number = self._extract_saeima_number(html_content)
            session_leader = self._extract_session_leader(html_content)

            if not full_text or len(full_text) < 500:
                logger.warning(f"Transcript {transcript_id} has insufficient text ({len(full_text)} chars)")
                return None

            return {
                "transcript_id": transcript_id,
                "url": url,
                "title": title,
                "full_text": full_text,
                "date": date,
                "saeima_number": saeima_number,
                "session_leader": session_leader,
            }

        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to fetch transcript {transcript_id}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all transcripts from all Saeima categories.

        Iterates through category pages to discover transcript IDs,
        then fetches each transcript.
        """
        doc_count = 0
        seen_ids: Set[int] = set()

        for cat_id, cat_name in SAEIMA_CATEGORIES.items():
            logger.info(f"Processing category {cat_id}: {cat_name}")

            transcript_ids = self._discover_transcript_ids(cat_id)

            for tid in transcript_ids:
                if tid in seen_ids:
                    continue
                seen_ids.add(tid)

                raw = self._fetch_transcript(tid)
                if raw and raw.get("full_text"):
                    doc_count += 1
                    yield raw

                    # Log progress every 50 docs
                    if doc_count % 50 == 0:
                        logger.info(f"Fetched {doc_count} transcripts with full text")

            # Safety limit for full bootstrap
            if doc_count >= 2000:
                logger.warning("Reached document limit (2000), stopping")
                break

        logger.info(f"Total transcripts fetched with full text: {doc_count}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield recent transcripts (from current Saeima).

        Checks the 14th Saeima category for new transcripts.
        """
        # Focus on current Saeima (category 29 = 14th Saeima)
        current_category = 29

        logger.info(f"Fetching updates from category {current_category}")

        transcript_ids = self._discover_transcript_ids(current_category)

        # Get the most recent ones (highest IDs tend to be more recent)
        recent_ids = sorted(transcript_ids, reverse=True)[:50]

        for tid in recent_ids:
            raw = self._fetch_transcript(tid)
            if raw and raw.get("full_text"):
                yield raw

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw transcript data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        transcript_id = raw.get("transcript_id", "")
        title = raw.get("title", "")
        full_text = self._clean_text(raw.get("full_text", ""))
        url = raw.get("url", f"{BASE_URL}/lv/transcripts/view/{transcript_id}")
        date = raw.get("date", "")
        saeima_number = raw.get("saeima_number")

        # Build a better title if needed
        if not title and date and saeima_number:
            title = f"{saeima_number}. Saeimas sēde - {date}"

        return {
            # Required base fields
            "_id": str(transcript_id),
            "_source": "LV/Parliament",
            "_type": "parliamentary_proceedings",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date or "",
            "url": url,
            # Additional metadata
            "transcript_id": transcript_id,
            "saeima_number": saeima_number,
            "session_leader": raw.get("session_leader", ""),
            "language": "lv",
            "document_type": "stenogramma",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing LV/Parliament endpoints...")

        # Test category page
        print("\n1. Testing category page...")
        try:
            url = f"{BASE_URL}/lv/transcripts/category/29"
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            matches = re.findall(r'transcripts/view/(\d+)', resp.text)
            print(f"   Category page accessible, found {len(set(matches))} transcript links")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test transcript fetch
        print("\n2. Testing transcript fetch...")
        try:
            # Get a recent transcript
            transcript_ids = self._discover_transcript_ids(29)
            if transcript_ids:
                test_id = transcript_ids[-1]  # Most recent
                print(f"   Fetching transcript {test_id}...")

                doc = self._fetch_transcript(test_id)
                if doc:
                    print(f"   Title: {doc.get('title', 'N/A')[:60]}...")
                    print(f"   Date: {doc.get('date', 'N/A')}")
                    print(f"   Saeima: {doc.get('saeima_number', 'N/A')}")
                    print(f"   Full text length: {len(doc.get('full_text', ''))} characters")
                    if doc.get('full_text'):
                        print(f"   Text preview: {doc['full_text'][:200]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = ParliamentScraper()

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
