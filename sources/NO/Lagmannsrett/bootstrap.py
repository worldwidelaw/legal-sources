#!/usr/bin/env python3
"""
NO/Lagmannsrett -- Norwegian Courts of Appeal (Lagmannsretter)

Fetches court of appeal decisions from Lovdata's public registry.
Covers 6 lagmannsretter from 2008+. Full text HTML extraction.

Strategy:
  - Enumerate decisions per court via registry pagination (?verdict=XXA&year=YYYY&offset=N)
  - Fetch individual decision pages and extract full text from #documentBody
  - Rate limited to ~1 req/2s to be respectful to government server

API:
  - Registry: https://lovdata.no/register/avgjørelser?verdict={CODE}A&year={YEAR}&offset={N}
  - Document: https://lovdata.no/dokument/{path} (linked from registry)
  - No auth required. NLOD 2.0 license.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch ~15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NO.Lagmannsrett")

BASE_URL = "https://lovdata.no"
REGISTRY_URL = "/register/avgjørelser"
START_YEAR = 2008
PAGE_SIZE = 20  # Lovdata returns 20 per page

# Courts of Appeal with their verdict codes
COURTS = [
    {"code": "LB", "name": "Borgarting lagmannsrett"},
    {"code": "LG", "name": "Gulating lagmannsrett"},
    {"code": "LH", "name": "Hålogaland lagmannsrett"},
    {"code": "LA", "name": "Agder lagmannsrett"},
    {"code": "LE", "name": "Eidsivating lagmannsrett"},
    {"code": "LF", "name": "Frostating lagmannsrett"},
]


class LagmannsrettScraper(BaseScraper):
    """Scraper for NO/Lagmannsrett -- Norwegian Courts of Appeal."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/worldwidelaw/legal-sources)",
                "Accept": "text/html,application/xhtml+xml,*/*",
                "Accept-Language": "nb-NO,nb;q=0.9,no;q=0.8,en;q=0.5",
            },
            timeout=60,
        )

    def _fetch_registry_page(self, court_code: str, year: int, offset: int = 0) -> tuple:
        """Fetch a page of decisions from the registry.
        Returns (list of {case_id, url}, total_count).
        """
        self.rate_limiter.wait()
        url = f"{REGISTRY_URL}?verdict={court_code}A&year={year}&offset={offset}"
        try:
            resp = self.client.get(url)
            if not resp or resp.status_code != 200:
                return [], 0

            html = resp.text

            # Extract total count: "Viser 1 - 20 av 886 treff"
            total = 0
            match = re.search(r'av\s+([\d\s,.]+)\s*treff', html)
            if match:
                total = int(re.sub(r'[\s,.]', '', match.group(1)))

            # Extract decision links
            items = []
            # Pattern: links to /dokument/...avgjorelse/CASE-ID
            for m in re.finditer(
                r'<a[^>]*href="(/dokument/[^"]*?/avgjorelse/([^"]+?))"[^>]*>',
                html
            ):
                doc_url = m.group(1)
                case_id = m.group(2).upper().replace('%20', '-')
                # Only include lagmannsrett cases (start with L)
                if case_id.startswith(court_code + '-'):
                    items.append({
                        "case_id": case_id,
                        "url": doc_url,
                    })

            # Deduplicate (same case can appear in multiple links)
            seen = set()
            unique = []
            for item in items:
                if item["case_id"] not in seen:
                    seen.add(item["case_id"])
                    unique.append(item)

            return unique, total

        except Exception as e:
            logger.warning(f"Error fetching registry {court_code} {year} offset={offset}: {e}")
            return [], 0

    def _fetch_decision(self, doc_url: str) -> Optional[Dict]:
        """Fetch full text and metadata from a decision page."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(doc_url)
            if not resp or resp.status_code != 200:
                return None

            html = resp.text

            # Check for "full text not available"
            if 'Full tekst til avgjørelsen er ikke tilgjengelig' in html:
                return None

            # Extract text from documentBody
            text = self._extract_text(html)
            if not text or len(text) < 100:
                return None

            # Extract metadata
            metadata = self._extract_metadata(html)

            return {
                "text": text,
                "metadata": metadata,
            }

        except Exception as e:
            logger.warning(f"Error fetching decision {doc_url}: {e}")
            return None

    def _extract_text(self, html: str) -> str:
        """Extract clean text from document body."""
        # Find documentBody div
        match = re.search(
            r'<div[^>]*id="documentBody"[^>]*>(.*?)</div>\s*(?:</div>|<div[^>]*id=")',
            html, re.DOTALL
        )
        if not match:
            # Try documentContent
            match = re.search(
                r'<div[^>]*class="documentContent"[^>]*>(.*?)</div>\s*</div>',
                html, re.DOTALL
            )
        if not match:
            # Try lovdataDocument
            match = re.search(
                r'<div[^>]*id="lovdataDocument"[^>]*>(.*?)</div>\s*</div>',
                html, re.DOTALL
            )
        if not match:
            return ""

        content = match.group(1)

        # Remove scripts, styles, nav
        content = re.sub(r'<(?:script|style|nav|header|footer)[^>]*>.*?</(?:script|style|nav|header|footer)>', '', content, flags=re.DOTALL | re.IGNORECASE)

        # Convert block elements to newlines
        content = re.sub(r'<(?:p|div|br|h[1-6]|li|tr|blockquote)[^>]*/?>', '\n', content, flags=re.IGNORECASE)
        content = re.sub(r'</(?:p|div|li|tr|blockquote|ol|ul|table)>', '\n', content, flags=re.IGNORECASE)

        # Remove remaining tags
        content = re.sub(r'<[^>]+>', ' ', content)

        # Decode entities
        import html as htmlmod
        content = htmlmod.unescape(content)

        # Clean whitespace
        lines = [line.strip() for line in content.split('\n')]
        lines = [line for line in lines if line]
        text = '\n'.join(lines).strip()

        # Collapse excessive whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)

        return text

    def _extract_metadata(self, html: str) -> Dict:
        """Extract metadata from meta table."""
        metadata = {}

        # Extract from meta table rows
        for m in re.finditer(
            r'<th[^>]*class="metafieldLabel"[^>]*>(.*?)</th>\s*<td[^>]*class="metavalue"[^>]*>(.*?)</td>',
            html, re.DOTALL
        ):
            label = re.sub(r'<[^>]+>', '', m.group(1)).strip().lower()
            value = re.sub(r'<[^>]+>', '', m.group(2)).strip()

            if 'instans' in label:
                metadata['court'] = value
            elif 'dato' in label:
                metadata['date'] = value
            elif 'publisert' in label:
                metadata['published_id'] = value
            elif 'stikkord' in label:
                metadata['keywords'] = value
            elif 'sammendrag' in label:
                metadata['summary'] = value
            elif 'saksgang' in label:
                metadata['case_history'] = value

        # Get title from h1
        h1_match = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
        if h1_match:
            metadata['title'] = re.sub(r'<[^>]+>', '', h1_match.group(1)).strip()

        return metadata

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all decisions from all courts and years."""
        current_year = datetime.now().year

        for court in COURTS:
            code = court["code"]
            name = court["name"]
            logger.info(f"Processing {name} ({code})")

            for year in range(current_year, START_YEAR - 1, -1):
                items, total = self._fetch_registry_page(code, year, 0)
                if total == 0:
                    continue

                logger.info(f"  {code} {year}: {total} decisions")

                # Yield first page items
                for item in items:
                    yield {
                        "_court_code": code,
                        "_court_name": name,
                        "_year": year,
                        "_case_id": item["case_id"],
                        "_doc_url": item["url"],
                    }

                # Paginate through remaining pages
                offset = PAGE_SIZE
                while offset < total:
                    page_items, _ = self._fetch_registry_page(code, year, offset)
                    if not page_items:
                        break
                    for item in page_items:
                        yield {
                            "_court_code": code,
                            "_court_name": name,
                            "_year": year,
                            "_case_id": item["case_id"],
                            "_doc_url": item["url"],
                        }
                    offset += PAGE_SIZE

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent decisions (last 2 years)."""
        current_year = datetime.now().year
        for court in COURTS:
            code = court["code"]
            name = court["name"]
            for year in [current_year, current_year - 1]:
                items, total = self._fetch_registry_page(code, year, 0)
                for item in items:
                    yield {
                        "_court_code": code,
                        "_court_name": name,
                        "_year": year,
                        "_case_id": item["case_id"],
                        "_doc_url": item["url"],
                    }
                offset = PAGE_SIZE
                while offset < total:
                    page_items, _ = self._fetch_registry_page(code, year, offset)
                    if not page_items:
                        break
                    for item in page_items:
                        yield {
                            "_court_code": code,
                            "_court_name": name,
                            "_year": year,
                            "_case_id": item["case_id"],
                            "_doc_url": item["url"],
                        }
                    offset += PAGE_SIZE

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw entry into standard schema, fetching full text."""
        case_id = raw.get("_case_id", "")
        court_code = raw.get("_court_code", "")
        court_name = raw.get("_court_name", "")
        doc_url = raw.get("_doc_url", "")

        if not case_id or not doc_url:
            return None

        result = self._fetch_decision(doc_url)
        if not result or not result.get("text"):
            return None

        metadata = result.get("metadata", {})
        date_str = metadata.get("date", "")

        # Build title
        title = metadata.get("title", case_id)
        if metadata.get("keywords"):
            title = f"{case_id} - {metadata['keywords'][:100]}"

        court = metadata.get("court", court_name)

        return {
            "_id": case_id,
            "_source": "NO/Lagmannsrett",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": result["text"],
            "date": date_str,
            "url": f"{BASE_URL}{doc_url}",
            "court": court,
            "case_id": case_id,
            "court_code": court_code,
            "keywords": metadata.get("keywords"),
            "summary": metadata.get("summary"),
            "jurisdiction": "NO",
            "language": "nob",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing NO/Lagmannsrett...")

        for court in COURTS[:2]:
            code = court["code"]
            name = court["name"]
            print(f"\n--- {name} ({code}) ---")

            items, total = self._fetch_registry_page(code, 2024, 0)
            print(f"  2024: {total} decisions, {len(items)} on page 1")

            if items:
                item = items[0]
                print(f"  First: {item['case_id']}")
                result = self._fetch_decision(item["url"])
                if result:
                    print(f"  Full text: {len(result['text'])} chars")
                    print(f"  Court: {result['metadata'].get('court', 'N/A')}")
                    print(f"  Sample: {result['text'][:150]}...")
                else:
                    print("  FAILED: No text extracted")

        print("\nTest complete!")


def main():
    scraper = LagmannsrettScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, {stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
