#!/usr/bin/env python3
"""
EE/SupremeCourt -- Estonian Supreme Court (Riigikohus) Case Law Fetcher

Fetches Supreme Court decisions with full text from the official sources.

Strategy:
  - Discovery: Year-by-year pagination through rikos.rik.ee search
  - Full text: HTML from rikos.rik.ee/?asjaNr={case_number}
  - Updates: RSS feed at riigikohus.ee/lahendid/rss.xml for recent judgments

Data access method:
  - rikos.rik.ee search API with ?aasta=YYYY&pageSize=100&lk=N
  - HTML scraping for full judgment text
  - ~350 decisions per year, ~12K total from 1991-present

The judgments include:
  - Administrative Chamber (Halduskolleegium)
  - Civil Chamber (Tsiviilkolleegium)
  - Criminal Chamber (Kriminaalkolleegium)
  - Constitutional Review Chamber (Pohiseaduslikkuse jarelevalve kolleegium)

Usage:
  python bootstrap.py bootstrap           # Full historical pull with checkpointing
  python bootstrap.py bootstrap --sample  # Fetch 10+ sample records
  python bootstrap.py update              # Incremental update via RSS
  python bootstrap.py test-api            # Quick connectivity test
  python bootstrap.py status              # Show checkpoint status
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, List
from xml.etree import ElementTree as ET
from bs4 import BeautifulSoup
import feedparser

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.EE.SupremeCourt")

# Base URLs
RIIGIKOHUS_URL = "https://www.riigikohus.ee"
RIKOS_URL = "https://rikos.rik.ee"

# RSS feeds for different chambers (used for updates)
RSS_FEEDS = {
    "all": "/lahendid/rss.xml",
    "admin": "/haldusasjad/rss.xml",
    "civil": "/tsiviilasjad/rss.xml",
    "criminal": "/kuriteo-ja-vaarteoasjad/rss.xml",
    "constitutional": "/pohiseaduslikkuse-jaralevalve-asjad/rss.xml",
}

# Pagination settings for full archive fetch
FIRST_YEAR = 1991  # Estonian Supreme Court established
CURRENT_YEAR = datetime.now().year + 1  # Include next year for recent decisions
PAGE_SIZE = 100  # Maximum allowed by the API
CHECKPOINT_INTERVAL = 50  # Save progress every N records

# Chamber code mapping
CHAMBER_CODES = {
    "1": "criminal",        # 1-XX-XXXX
    "2": "civil",           # 2-XX-XXXX
    "3": "administrative",  # 3-XX-XXXX
    "4": "misdemeanor",     # 4-XX-XXXX (Vaarteoasjad)
    "5": "constitutional",  # 5-XX-XXXX
}


class SupremeCourtScraper(BaseScraper):
    """
    Scraper for EE/SupremeCourt -- Estonian Supreme Court case law.
    Country: EE
    URL: https://www.riigikohus.ee

    Data types: case_law
    Auth: none (Open public access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=RIIGIKOHUS_URL,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "text/html,application/xml,application/xhtml+xml",
                "Accept-Language": "et,en;q=0.9",
            },
            timeout=60,
        )

        self.rikos_client = HttpClient(
            base_url=RIKOS_URL,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "et,en;q=0.9",
            },
            timeout=60,
        )

        self.checkpoint_file = self.source_dir / "checkpoint.json"

    # -- Checkpoint management -------------------------------------------------

    def _load_checkpoint(self) -> Dict:
        """Load checkpoint data from file."""
        if not self.checkpoint_file.exists():
            return {}
        try:
            with open(self.checkpoint_file) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}

    def _save_checkpoint(self, data: Dict) -> None:
        """Save checkpoint data to file."""
        with open(self.checkpoint_file, 'w') as f:
            json.dump(data, f, indent=2)

    # -- Year-based archive pagination -----------------------------------------

    def _fetch_year_page(self, year: int, page: int = 1) -> List[Dict]:
        """
        Fetch a page of decisions from rikos.rik.ee for a specific year.

        Returns list of dicts with case_number and basic metadata.
        """
        url = f"/?aasta={year}&pageSize={PAGE_SIZE}&lk={page}"

        try:
            self.rate_limiter.wait()
            resp = self.rikos_client.get(url)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch year {year} page {page}: {e}")
            return []

        entries = []
        try:
            soup = BeautifulSoup(resp.content, 'html.parser')

            # Extract total count from header
            header = soup.find('h2', class_='search-result__header')
            total_count = 0
            if header:
                match = re.search(r'\((\d+)\)', header.get_text())
                if match:
                    total_count = int(match.group(1))

            # Find all case links in search results
            for link in soup.find_all('a', class_='viitaja'):
                href = link.get('href', '')
                case_match = re.search(r'asjaNr=([^&]+)', href)
                if case_match:
                    case_number = case_match.group(1)
                    # URL decode the case number
                    case_number = case_number.replace('%2F', '/')

                    # Get the description from next row
                    row = link.find_parent('tr')
                    description = ""
                    if row:
                        desc_row = row.find_next_sibling('tr')
                        if desc_row:
                            desc_td = desc_row.find('td', colspan='4')
                            if desc_td:
                                description = desc_td.get_text(strip=True)

                    entries.append({
                        "case_number": case_number,
                        "description": description,
                        "year": year,
                        "_total_for_year": total_count,
                    })

        except Exception as e:
            logger.error(f"Error parsing year {year} page {page}: {e}")

        return entries

    def _fetch_all_for_year(self, year: int, checkpoint: Dict) -> Generator[Dict, None, None]:
        """
        Fetch all decisions for a specific year with pagination.
        """
        page = checkpoint.get(f"year_{year}_page", 1)
        processed_in_year = checkpoint.get(f"year_{year}_processed", 0)

        logger.info(f"Fetching year {year} from page {page}...")

        while True:
            entries = self._fetch_year_page(year, page)

            if not entries:
                break

            total_for_year = entries[0].get('_total_for_year', 0) if entries else 0

            for entry in entries:
                yield entry

            processed_in_year += len(entries)
            logger.info(f"  Year {year}: processed {processed_in_year}/{total_for_year}")

            # Check if there are more pages
            if len(entries) < PAGE_SIZE:
                break

            page += 1

        # Mark year as complete in checkpoint
        checkpoint[f"year_{year}_complete"] = True
        checkpoint[f"year_{year}_total"] = processed_in_year

    # -- RSS Feed parsing ------------------------------------------------------

    def _fetch_rss_feed(self, feed_type: str = "all") -> List[Dict]:
        """
        Fetch and parse RSS feed for judgments.

        Returns list of dicts with: case_number, title, description, link, pub_date
        """
        feed_path = RSS_FEEDS.get(feed_type, RSS_FEEDS["all"])
        url = f"{RIIGIKOHUS_URL}{feed_path}"

        logger.info(f"Fetching RSS feed: {url}")

        try:
            self.rate_limiter.wait()
            feed = feedparser.parse(url)

            if feed.bozo:
                logger.warning(f"RSS feed parsing issue: {feed.bozo_exception}")

            entries = []
            for entry in feed.entries:
                # Extract case number from title
                case_number = entry.get("title", "").strip()

                # Parse publication date
                pub_date = None
                if entry.get("published_parsed"):
                    pub_date = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)

                entries.append({
                    "case_number": case_number,
                    "title": entry.get("title", ""),
                    "description": entry.get("description", ""),
                    "link": entry.get("link", ""),
                    "guid": entry.get("id", ""),
                    "pub_date": pub_date.isoformat() if pub_date else None,
                })

            logger.info(f"Found {len(entries)} entries in RSS feed")
            return entries

        except Exception as e:
            logger.error(f"Failed to fetch RSS feed: {e}")
            return []

    # -- Full text fetching ----------------------------------------------------

    def _extract_case_number_from_link(self, link: str) -> Optional[str]:
        """Extract case number from riigikohus.ee link."""
        # Pattern: ?asjaNr=X-XX-XXXX or ?asjaNr=X-XX-XXXX/XX
        match = re.search(r'asjaNr=([^&\s]+)', link)
        if match:
            return match.group(1)
        return None

    def _fetch_full_text(self, case_number: str) -> Optional[Dict]:
        """
        Fetch full text of a judgment from rikos.rik.ee.

        Returns dict with full_text, metadata extracted from HTML.
        """
        url = f"/?asjaNr={case_number}"

        self.rate_limiter.wait()

        try:
            resp = self.rikos_client.get(url)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch full text for {case_number}: {e}")
            return None

        try:
            soup = BeautifulSoup(resp.content, 'html.parser')

            # Extract document metadata
            metadata = {}
            custom_data = soup.find('div', id='custom-data-container')
            if custom_data:
                metadata['file_object_id'] = custom_data.get('data-faili-objekt-id', '')
                metadata['document_nr'] = custom_data.get('data-toimingu-nr', '')

            # Find main content div
            word_section = soup.find('div', class_='WordSection1')
            if not word_section:
                # Try alternative patterns
                word_section = soup.find('body')

            if not word_section:
                logger.warning(f"No content section found for {case_number}")
                return None

            # Extract structured metadata from table
            case_metadata = {}
            tables = word_section.find_all('table')
            for table in tables[:1]:  # Usually first table has metadata
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        key = cells[0].get_text(strip=True)
                        value = cells[1].get_text(strip=True)
                        if key and value:
                            case_metadata[key] = value

            # Extract full text - get all paragraph text
            text_parts = []
            for p in word_section.find_all(['p', 'li']):
                text = p.get_text(strip=True)
                if text:
                    text_parts.append(text)

            full_text = "\n\n".join(text_parts)

            # Clean up the text
            full_text = html.unescape(full_text)
            full_text = re.sub(r'\s+', ' ', full_text)  # Normalize whitespace
            full_text = re.sub(r'(\w+)\s*\n\s*(\w+)', r'\1 \2', full_text)  # Join broken words

            # Extract title (usually the case subject)
            title = case_metadata.get('Kohtuasi', '')

            # Extract date
            date_str = case_metadata.get('Määruse kuupäev', '') or case_metadata.get('Otsuse kuupäev', '')
            # Parse Estonian date format: "Tartu, 11. veebruar 2026"
            date = None
            if date_str:
                # Remove location prefix
                date_str = re.sub(r'^[^,]+,\s*', '', date_str)
                # Try to parse
                month_map = {
                    'jaanuar': '01', 'veebruar': '02', 'märts': '03', 'aprill': '04',
                    'mai': '05', 'juuni': '06', 'juuli': '07', 'august': '08',
                    'september': '09', 'oktoober': '10', 'november': '11', 'detsember': '12'
                }
                for month_name, month_num in month_map.items():
                    if month_name in date_str.lower():
                        match = re.search(r'(\d+)\.\s*' + month_name + r'\s*(\d{4})', date_str, re.IGNORECASE)
                        if match:
                            day = match.group(1).zfill(2)
                            year = match.group(2)
                            date = f"{year}-{month_num}-{day}"
                            break

            # Extract chamber/court composition
            chamber = case_metadata.get('Kohtukoosseis', '')

            # Determine chamber type from case number
            chamber_type = "unknown"
            if case_number:
                first_digit = case_number.split('-')[0] if '-' in case_number else ''
                chamber_type = CHAMBER_CODES.get(first_digit, "unknown")

            return {
                "full_text": full_text,
                "title": title,
                "date": date,
                "chamber": chamber,
                "chamber_type": chamber_type,
                "case_metadata": case_metadata,
                "file_metadata": metadata,
            }

        except Exception as e:
            logger.error(f"Error parsing full text for {case_number}: {e}")
            return None

    # -- Abstract method implementations ---------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all Supreme Court decisions from 1991 to present.

        Uses year-by-year pagination through rikos.rik.ee search API.
        Supports checkpoint/resume for multi-session bootstraps.
        """
        checkpoint = self._load_checkpoint()
        start_year = checkpoint.get("current_year", FIRST_YEAR)
        processed_ids = set(checkpoint.get("processed_ids", []))
        total_processed = checkpoint.get("total_processed", 0)

        logger.info(f"Starting from year {start_year}, {total_processed} already processed")

        for year in range(start_year, CURRENT_YEAR + 1):
            # Skip completed years
            if checkpoint.get(f"year_{year}_complete"):
                logger.info(f"Year {year} already complete, skipping")
                continue

            logger.info(f"Processing year {year}...")

            year_count = 0
            for entry in self._fetch_all_for_year(year, checkpoint):
                case_number = entry.get("case_number")

                if not case_number:
                    continue

                # Skip duplicates
                if case_number in processed_ids:
                    continue

                processed_ids.add(case_number)
                total_processed += 1
                year_count += 1

                yield {
                    "_search_entry": entry,
                    "_case_number": case_number,
                }

                # Save checkpoint periodically
                if total_processed % CHECKPOINT_INTERVAL == 0:
                    checkpoint["current_year"] = year
                    checkpoint["total_processed"] = total_processed
                    checkpoint["processed_ids"] = list(processed_ids)[-5000:]  # Keep last 5K
                    self._save_checkpoint(checkpoint)
                    logger.info(f"Checkpoint saved: {total_processed} records")

            logger.info(f"Year {year} complete: {year_count} decisions")

            # Update checkpoint after each year
            checkpoint["current_year"] = year + 1
            checkpoint["total_processed"] = total_processed
            checkpoint["processed_ids"] = list(processed_ids)[-5000:]
            self._save_checkpoint(checkpoint)

        # Final checkpoint
        checkpoint["bootstrap_complete"] = True
        checkpoint["last_bootstrap"] = datetime.now(timezone.utc).isoformat()
        self._save_checkpoint(checkpoint)
        logger.info(f"Bootstrap complete: {total_processed} total decisions")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield decisions published since the given date.
        """
        entries = self._fetch_rss_feed("all")

        for entry in entries:
            pub_date_str = entry.get("pub_date")
            if pub_date_str:
                try:
                    pub_date = datetime.fromisoformat(pub_date_str.replace('Z', '+00:00'))
                    if pub_date < since:
                        continue
                except ValueError:
                    pass

            case_number = entry.get("case_number") or self._extract_case_number_from_link(entry.get("link", ""))

            if case_number:
                yield {
                    "_rss_entry": entry,
                    "_case_number": case_number,
                }

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw data into standard schema.

        CRITICAL: Fetches and includes FULL TEXT from rikos.rik.ee.
        """
        # Handle both RSS entry and search entry formats
        rss_entry = raw.get("_rss_entry", {})
        search_entry = raw.get("_search_entry", {})
        case_number = raw.get("_case_number", "")

        # Fetch full text
        full_text_data = self._fetch_full_text(case_number)

        if not full_text_data:
            logger.warning(f"Could not fetch full text for {case_number}")
            return None

        full_text = full_text_data.get("full_text", "")

        # Skip if no substantial text
        if len(full_text) < 100:
            logger.warning(f"Full text too short for {case_number}: {len(full_text)} chars")
            return None

        # Get description from either RSS or search entry
        description = rss_entry.get("description", "") or search_entry.get("description", "")

        # Build normalized record
        return {
            # Required base fields
            "_id": case_number,
            "_source": "EE/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": full_text_data.get("title") or description[:200] if description else case_number,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": full_text_data.get("date") or rss_entry.get("pub_date", "")[:10] if rss_entry.get("pub_date") else None,
            "url": rss_entry.get("link") or f"https://www.riigikohus.ee/et/lahendid/?asjaNr={case_number}",
            # Case-specific metadata
            "case_number": case_number,
            "chamber": full_text_data.get("chamber", ""),
            "chamber_type": full_text_data.get("chamber_type", ""),
            "description": description,
            "pub_date": rss_entry.get("pub_date"),
            "document_id": full_text_data.get("file_metadata", {}).get("file_object_id", ""),
            # ECLI (construct from case number)
            "ecli": self._construct_ecli(case_number),
        }

    def _construct_ecli(self, case_number: str) -> str:
        """
        Construct ECLI from case number.

        Estonian ECLI format: ECLI:EE:RK:YYYY:case_number_with_dots
        Example: ECLI:EE:RK:2024:5.24.28.1 from 5-24-28/1
        """
        if not case_number:
            return ""

        # Extract year from case number (e.g., 2-24-1408 -> 2024)
        match = re.match(r'(\d+)-(\d+)-(\d+)(?:/(\d+))?', case_number)
        if not match:
            return ""

        prefix, year_short, case_id, doc_num = match.groups()

        # Determine full year (assume 20XX for years < 50, 19XX otherwise)
        year_int = int(year_short)
        if year_int < 50:
            year = f"20{year_short.zfill(2)}"
        else:
            year = f"19{year_short.zfill(2)}"

        # Build ECLI case number with dots
        ecli_case = f"{prefix}.{year_short}.{case_id}"
        if doc_num:
            ecli_case += f".{doc_num}"

        return f"ECLI:EE:RK:{year}:{ecli_case}"

    # -- Custom commands -------------------------------------------------------

    def test_api(self):
        """Quick connectivity test."""
        print("Testing Estonian Supreme Court connectivity...")

        # Test RSS feed
        try:
            self.rate_limiter.wait()
            resp = self.client.get("/lahendid/rss.xml")
            resp.raise_for_status()
            feed = feedparser.parse(resp.content)
            print(f"  RSS feed: OK ({len(feed.entries)} entries)")
        except Exception as e:
            print(f"  RSS feed: FAILED ({e})")
            return

        # Test full text fetch
        if feed.entries:
            first_entry = feed.entries[0]
            case_number = first_entry.get("title", "").strip()
            print(f"  Testing full text for: {case_number}")

            try:
                self.rate_limiter.wait()
                resp = self.rikos_client.get(f"/?asjaNr={case_number}")
                resp.raise_for_status()
                print(f"  Full text endpoint: OK ({len(resp.content)} bytes)")
            except Exception as e:
                print(f"  Full text endpoint: FAILED ({e})")
                return

        print("\nConnectivity test passed!")

    def run_sample(self, n: int = 12) -> dict:
        """
        Fetch a sample of decisions with full text from both RSS and archive.

        Tests both discovery methods:
        1. RSS feed (for recent decisions)
        2. Year-based archive pagination (for historical coverage)
        """
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        checked = 0
        errors = []
        text_lengths = []
        years_tested = []

        # First, test year-based archive pagination with a few years
        # This verifies the full bootstrap will work
        test_years = [2024, 2020, 2015, 2010, 2005, 2000, 1995]
        archive_entries = []
        total_archive_count = 0

        logger.info("Testing year-based archive pagination...")
        for year in test_years:
            entries_for_year = self._fetch_year_page(year, 1)
            count = entries_for_year[0].get('_total_for_year', 0) if entries_for_year else 0
            total_archive_count += count
            years_tested.append({"year": year, "count": count})
            logger.info(f"  Year {year}: {count} decisions")
            # Add a few entries from each year for sampling
            archive_entries.extend(entries_for_year[:2])

        logger.info(f"Archive test complete: {total_archive_count} decisions found across {len(test_years)} test years")
        logger.info(f"Estimated total archive: ~{total_archive_count * 34 // len(test_years)} decisions")

        # Also fetch RSS entries for recent coverage
        rss_entries = self._fetch_rss_feed("all")
        logger.info(f"Got {len(rss_entries)} RSS entries")

        # Combine entries for sampling, prioritizing archive entries to test that pathway
        all_entries = []
        for entry in archive_entries:
            all_entries.append({"_search_entry": entry, "_case_number": entry.get("case_number")})
        for entry in rss_entries:
            case_number = entry.get("case_number") or self._extract_case_number_from_link(entry.get("link", ""))
            if case_number:
                all_entries.append({"_rss_entry": entry, "_case_number": case_number})

        # Process entries (both archive and RSS)
        seen_case_numbers = set()
        for raw in all_entries[:n * 3]:  # Check more than needed in case some fail
            if saved >= n:
                break

            case_number = raw.get("_case_number")

            if not case_number:
                errors.append(f"Entry {checked}: No case number")
                continue

            # Skip duplicates
            if case_number in seen_case_numbers:
                continue
            seen_case_numbers.add(case_number)

            checked += 1
            logger.info(f"Processing {case_number}...")

            try:
                normalized = self.normalize(raw)

                if not normalized:
                    errors.append(f"{case_number}: Normalization returned None")
                    continue

                if not normalized.get("text"):
                    errors.append(f"{case_number}: No text content")
                    continue

                text_len = len(normalized.get("text", ""))
                if text_len < 500:
                    errors.append(f"{case_number}: Text too short ({text_len} chars)")
                    continue

                # Save to sample directory
                safe_name = re.sub(r'[^\w\-]', '_', case_number)
                sample_path = sample_dir / f"{safe_name}.json"
                with open(sample_path, "w", encoding="utf-8") as f:
                    json.dump(normalized, f, ensure_ascii=False, indent=2)

                saved += 1
                text_lengths.append(text_len)
                logger.info(f"  Saved {case_number}: {normalized.get('title', '')[:50]}... ({text_len} chars)")

            except Exception as e:
                errors.append(f"{case_number}: {str(e)}")
                logger.error(f"Error processing {case_number}: {e}")

        stats = {
            "sample_records_saved": saved,
            "documents_checked": checked,
            "errors": errors[:10],
            "avg_text_length": sum(text_lengths) / len(text_lengths) if text_lengths else 0,
            "min_text_length": min(text_lengths) if text_lengths else 0,
            "max_text_length": max(text_lengths) if text_lengths else 0,
            "years_tested": years_tested,
            "archive_total_sampled": total_archive_count,
            "estimated_full_archive": total_archive_count * 34 // len(test_years),
        }

        return stats


# -- CLI Entry Point -----------------------------------------------------------


def main():
    scraper = SupremeCourtScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api|status] "
            "[--sample] [--sample-size N] [--reset]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    reset_mode = "--reset" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "status":
        checkpoint = scraper._load_checkpoint()
        if checkpoint:
            print("Checkpoint status:")
            print(f"  Current year: {checkpoint.get('current_year', FIRST_YEAR)}")
            print(f"  Total processed: {checkpoint.get('total_processed', 0)}")
            print(f"  Bootstrap complete: {checkpoint.get('bootstrap_complete', False)}")
            print(f"  Last bootstrap: {checkpoint.get('last_bootstrap', 'N/A')}")
            # Count completed years
            completed_years = sum(1 for y in range(FIRST_YEAR, CURRENT_YEAR + 1)
                                  if checkpoint.get(f"year_{y}_complete"))
            print(f"  Completed years: {completed_years}/{CURRENT_YEAR - FIRST_YEAR + 1}")
        else:
            print("No checkpoint found. Run 'bootstrap' to start.")
        return

    if command == "test-api":
        scraper.test_api()

    elif command == "bootstrap":
        if reset_mode:
            if scraper.checkpoint_file.exists():
                scraper.checkpoint_file.unlink()
                print("Checkpoint cleared.")
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
            print(json.dumps(stats, indent=2))
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
