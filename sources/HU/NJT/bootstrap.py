#!/usr/bin/env python3
"""
HU/NJT -- Hungarian National Legislation Database (Nemzeti Jogszabálytár)

Fetches Hungarian legislation from njt.hu, the official Hungarian legislation database.

Strategy:
  - Uses search endpoint to discover documents by year
  - Fetches full HTML pages which contain server-rendered law text
  - Parses HTML to extract clean text content from jogszabaly div

Endpoints:
  - Search: https://njt.hu/search/-:-:{year}:-:-:-:-:-:-:-:1/{page}/{size}
  - Document: https://njt.hu/jogszabaly/{year}-{num}-{mod1}-{mod2}
  - ELI: https://njt.hu/eli/TV/{year}/{num}

Data:
  - Legislation types: TV (laws), TVR (statutory decrees), KR (govt decrees), MR (ministerial)
  - Languages: Hungarian (primary), English translations available for some
  - License: CC BY 4.0 (Open Government Data)

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
import time
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
logger = logging.getLogger("legal-data-hunter.HU.njt")

# Base URL for Hungarian NJT
BASE_URL = "https://njt.hu"

# Years to scrape (most recent first for sample mode)
YEARS_TO_SCRAPE = list(range(2025, 1989, -1))  # 2025 down to 1990

# Search results per page
PAGE_SIZE = 50


class NJTScraper(BaseScraper):
    """
    Scraper for HU/NJT -- Hungarian National Legislation Database.
    Country: HU
    URL: https://njt.hu

    Data types: legislation
    Auth: none (Open Government Data, CC BY 4.0)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept-Language": "hu,en",
                "Accept": "text/html,application/xhtml+xml",
            },
            timeout=60,
        )

    def _extract_doc_ids_from_search(self, html_content: str) -> List[str]:
        """
        Extract document IDs from search results HTML.

        Format: jogszabaly/{year}-{num}-{mod1}-{mod2}
        Example: jogszabaly/2024-45-00-00
        """
        # Pattern matches: jogszabaly/YYYY-NNN-NN-NN
        pattern = re.compile(r'jogszabaly/(\d{4}-\d+-\d+-\d+)')

        matches = pattern.findall(html_content)

        # Deduplicate while preserving order
        seen = set()
        unique = []
        for m in matches:
            if m not in seen:
                seen.add(m)
                unique.append(m)

        return unique

    def _extract_text_from_html(self, html_content: str) -> str:
        """
        Extract clean law text from the jogszabaly HTML page.

        The text is in a div with class 'jogszabaly' containing structured
        elements like preambulum, szakasz, bekezdesNyito, etc.
        """
        text_parts = []

        # Extract main title
        title_match = re.search(r'<h1[^>]*class="[^"]*jogszabalyMainTitle[^"]*"[^>]*>([^<]+)', html_content)
        if title_match:
            text_parts.append(html.unescape(title_match.group(1).strip()))

        # Extract subtitle
        subtitle_match = re.search(r'<h2[^>]*class="[^"]*jogszabalySubtitle[^"]*"[^>]*>([^<]+)', html_content)
        if subtitle_match:
            text_parts.append(html.unescape(subtitle_match.group(1).strip()))

        # Extract preambulum text
        for match in re.finditer(r'<div[^>]*class="preambulum"[^>]*>(.*?)</div>', html_content, re.DOTALL):
            text = self._clean_html_text(match.group(1))
            if text:
                text_parts.append(text)

        # Extract all bekezdes (paragraph) content - these contain the law text
        # Pattern: <div class="bekezdesNyito"> or <div class="bekezdes">
        for match in re.finditer(r'<div[^>]*class="[^"]*bekezdes[^"]*"[^>]*>(.*?)</div><!--', html_content, re.DOTALL):
            div_content = match.group(1)
            # Extract text from <p> tags within
            for p_match in re.finditer(r'<p[^>]*>(.*?)</p>', div_content, re.DOTALL):
                text = self._clean_html_text(p_match.group(1))
                if text and len(text) > 10:
                    text_parts.append(text)

        # Extract melléklet (appendix/annex) content - often has important text
        for match in re.finditer(r'<p[^>]*class="[^"]*mhk-[^"]*"[^>]*>(.*?)</p>', html_content, re.DOTALL):
            text = self._clean_html_text(match.group(1))
            if text and len(text) > 10:
                text_parts.append(text)

        # Also extract plain paragraphs with AC/AJ/UJ classes (common in legislation)
        for match in re.finditer(r'<p[^>]*class="[^"]*(?:AC|AJ|UJ)[^"]*"[^>]*>(.*?)</p>', html_content, re.DOTALL):
            text = self._clean_html_text(match.group(1))
            if text and len(text) > 10:
                text_parts.append(text)

        # Extract footnotes
        for match in re.finditer(r'<div class="footnote"[^>]*><sup>\d+</sup><p>(.*?)</p></div>', html_content, re.DOTALL):
            text = self._clean_html_text(match.group(1))
            if text:
                text_parts.append(f"[Note: {text}]")

        full_text = '\n\n'.join(text_parts)
        return full_text.strip()

    def _clean_html_text(self, html_text: str) -> str:
        """Remove HTML tags and clean up text."""
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', html_text)
        # Decode HTML entities
        text = html.unescape(text)
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _extract_metadata(self, html_content: str, doc_id: str) -> Dict[str, Any]:
        """
        Extract metadata (title, date, type) from the HTML page.
        """
        metadata = {
            "doc_id": doc_id,
            "title": "",
            "subtitle": "",
            "date": "",
            "doc_type": "",
            "year": "",
            "eli_uri": "",
        }

        # Extract title from meta tag or h1
        title_match = re.search(r'<meta name="title"[^>]*content="([^"]+)"', html_content)
        if title_match:
            metadata["title"] = html.unescape(title_match.group(1))
        else:
            title_match = re.search(r'<h1[^>]*class="[^"]*jogszabalyMainTitle[^"]*"[^>]*>([^<]+)', html_content)
            if title_match:
                metadata["title"] = html.unescape(title_match.group(1).strip())

        # Extract subtitle
        subtitle_match = re.search(r'<h2[^>]*class="[^"]*jogszabalySubtitle[^"]*"[^>]*>([^<]+)', html_content)
        if subtitle_match:
            metadata["subtitle"] = html.unescape(subtitle_match.group(1).strip())

        # Parse doc_id for year and type info
        # Format: YYYY-NUM-MOD-MOD (e.g., 2024-45-00-00)
        parts = doc_id.split('-')
        if len(parts) >= 2:
            metadata["year"] = parts[0]
            metadata["number"] = parts[1]

        # Extract effective date from hataly div
        date_match = re.search(r'<div class="hataly">(\d{4}\.\d{2}\.\d{2}\.?)</div>', html_content)
        if date_match:
            # Convert from Hungarian date format YYYY.MM.DD. to ISO
            date_str = date_match.group(1).rstrip('.')
            try:
                metadata["date"] = date_str.replace('.', '-')
            except:
                metadata["date"] = date_str

        # Try to determine document type from title
        title_lower = metadata["title"].lower()
        if "törvény" in title_lower:
            metadata["doc_type"] = "TV"  # Law
        elif "kormányrendelet" in title_lower or "korm. rendelet" in title_lower:
            metadata["doc_type"] = "KR"  # Government decree
        elif "miniszteri rendelet" in title_lower:
            metadata["doc_type"] = "MR"  # Ministerial decree
        elif "rendelet" in title_lower:
            metadata["doc_type"] = "R"  # Decree (generic)
        else:
            metadata["doc_type"] = "JS"  # Jogszabály (generic legislation)

        # Extract ELI URI if present
        eli_match = re.search(r'https://njt\.hu/eli/[A-Z]+/\d+/\d+', html_content)
        if eli_match:
            metadata["eli_uri"] = eli_match.group(0)

        return metadata

    def _fetch_document(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a single document by its ID.

        Returns dict with metadata and full_text, or None if failed.
        """
        url = f"/jogszabaly/{doc_id}"

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()

            content = resp.text

            # Extract text and metadata
            full_text = self._extract_text_from_html(content)
            metadata = self._extract_metadata(content, doc_id)

            if not full_text or len(full_text) < 100:
                logger.warning(f"Insufficient text content for {doc_id}: {len(full_text)} chars")
                return None

            metadata["full_text"] = full_text
            metadata["url"] = f"{BASE_URL}{url}"

            return metadata

        except Exception as e:
            logger.warning(f"Failed to fetch document {doc_id}: {e}")
            return None

    def _search_year(self, year: int, page: int = 1) -> List[str]:
        """
        Search for documents from a specific year.

        Returns list of document IDs found.
        """
        # URL pattern: /search/{params}/page/size
        # Params: type:subtype:year:number:text:title:effectiveDate:???:???:???:sortOrder
        url = f"/search/-:-:{year}:-:-:-:-:-:-:-:1/{page}/{PAGE_SIZE}"

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()

            doc_ids = self._extract_doc_ids_from_search(resp.text)
            logger.info(f"Found {len(doc_ids)} documents for year {year}, page {page}")
            return doc_ids

        except Exception as e:
            logger.warning(f"Search failed for year {year}, page {page}: {e}")
            return []

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from NJT.

        Iterates through years, fetching document listings and then
        full content for each document.
        """
        for year in YEARS_TO_SCRAPE:
            logger.info(f"Fetching documents from {year}...")

            page = 1
            while True:
                doc_ids = self._search_year(year, page)

                if not doc_ids:
                    break

                for doc_id in doc_ids:
                    doc = self._fetch_document(doc_id)
                    if doc:
                        yield doc

                # Check if we got a full page (might have more)
                if len(doc_ids) < PAGE_SIZE:
                    break

                page += 1

                # Safety limit on pages per year
                if page > 100:
                    logger.warning(f"Hit page limit for year {year}")
                    break

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents modified since the given date.

        Since NJT doesn't have a direct "modified since" filter, we
        fetch recent years and filter by effective date.
        """
        since_year = since.year
        current_year = datetime.now().year

        years_to_check = list(range(current_year, since_year - 1, -1))

        for year in years_to_check:
            logger.info(f"Checking year {year} for updates...")

            page = 1
            while True:
                doc_ids = self._search_year(year, page)

                if not doc_ids:
                    break

                for doc_id in doc_ids:
                    doc = self._fetch_document(doc_id)
                    if doc:
                        # Try to filter by date
                        doc_date_str = doc.get("date", "")
                        if doc_date_str:
                            try:
                                doc_date = datetime.strptime(doc_date_str, "%Y-%m-%d")
                                if doc_date.replace(tzinfo=timezone.utc) < since:
                                    continue
                            except:
                                pass
                        yield doc

                if len(doc_ids) < PAGE_SIZE:
                    break

                page += 1
                if page > 50:
                    break

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        doc_id = raw.get("doc_id", "")
        title = raw.get("title", "")
        subtitle = raw.get("subtitle", "")
        full_text = raw.get("full_text", "")

        # Combine title and subtitle for full title
        full_title = title
        if subtitle:
            full_title = f"{title} - {subtitle}"

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "HU/NJT",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": full_title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": raw.get("date", ""),
            "url": raw.get("url", f"{BASE_URL}/jogszabaly/{doc_id}"),
            # Additional metadata
            "doc_id": doc_id,
            "doc_type": raw.get("doc_type", ""),
            "year": raw.get("year", ""),
            "number": raw.get("number", ""),
            "eli_uri": raw.get("eli_uri", ""),
            "language": "hu",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Hungarian NJT endpoints...")

        # Test search
        print("\n1. Testing search (year 2024)...")
        try:
            doc_ids = self._search_year(2024, 1)
            print(f"   Found {len(doc_ids)} documents")
            if doc_ids:
                print(f"   Sample IDs: {doc_ids[:3]}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test document fetch
        print("\n2. Testing document fetch...")
        try:
            if doc_ids:
                test_id = doc_ids[0]
                print(f"   Fetching: {test_id}")
                doc = self._fetch_document(test_id)
                if doc:
                    print(f"   Title: {doc.get('title', 'N/A')[:60]}...")
                    print(f"   Text length: {len(doc.get('full_text', ''))} characters")
                    print(f"   Date: {doc.get('date', 'N/A')}")
                    print(f"   Type: {doc.get('doc_type', 'N/A')}")
                    if doc.get('full_text'):
                        print(f"   Text preview: {doc['full_text'][:200]}...")
                else:
                    print("   ERROR: No document returned")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = NJTScraper()

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
