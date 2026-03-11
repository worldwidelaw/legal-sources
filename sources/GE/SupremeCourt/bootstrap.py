#!/usr/bin/env python3
"""
GE/SupremeCourt -- Georgian Supreme Court Data Fetcher

Fetches Georgian Supreme Court case law from the official website.

Strategy:
  - Search endpoint returns paginated case listings by chamber (palata)
  - Three chambers: 0=Administrative, 1=Civil, 2=Criminal
  - Full text available via /fullcase/{id}/{palata} endpoint
  - HTML content with Georgian text

Endpoints:
  - Search: GET https://www.supremecourt.ge/ka/getCases?palata={0|1|2}&page={n}
  - Full case: GET https://www.supremecourt.ge/ka/fullcase/{id}/{palata}

Data:
  - ~85,000+ decisions across all chambers
  - Language: Georgian (KA)
  - Rate limit: 1-2 requests/second

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent pages only)
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
logger = logging.getLogger("legal-data-hunter.GE.supremecourt")

# Base URL for Georgian Supreme Court
BASE_URL = "https://www.supremecourt.ge"

# Chambers: 0=Administrative, 1=Civil, 2=Criminal
CHAMBERS = {
    0: "administrative",
    1: "civil",
    2: "criminal",
}


class GeorgianSupremeCourtScraper(BaseScraper):
    """
    Scraper for GE/SupremeCourt -- Georgian Supreme Court.
    Country: GE
    URL: https://www.supremecourt.ge

    Data types: case_law
    Auth: none (Open public access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ka,en",
            },
            timeout=60,
        )

    def _get_cases_page(self, palata: int, page: int = 1) -> List[Dict[str, Any]]:
        """
        Fetch a page of cases from the search endpoint.

        Returns list of dicts with: id, palata, case_number, date, subject, result, type
        """
        cases = []

        try:
            params = {"palata": palata, "page": page}

            self.rate_limiter.wait()
            resp = self.client.get("/ka/getCases", params=params)
            resp.raise_for_status()

            content = resp.text

            # Check if empty/no results
            if "მოიძებნა 0" in content or len(content) < 200:
                return []

            # Parse total count from first page
            total_match = re.search(r'მოიძებნა (\d+) გადაწყვეტილება', content)
            if total_match and page == 1:
                total = int(total_match.group(1))
                logger.info(f"Chamber {palata} ({CHAMBERS.get(palata, 'unknown')}): {total} total decisions")

            # Extract case entries using pattern from onclick and href
            # Pattern: seeMore(ID, PALATA) and href="/ka/fullcase/ID/PALATA"
            pattern = re.compile(
                r'seeMore\((\d+),(\d+)\).*?'
                r'საქმის ნომერი:</span>\s*([^<]+)</div>.*?'
                r'თარიღი:</span>\s*([^<]+)</div>.*?'
                r'(?:დავის საგანი:</span>\s*([^<]*?)</span>)?.*?'
                r'შედეგი:</span>\s*([^<]+)</div>.*?'
                r'საჩივრის სახე:</span>\s*([^<]+?)\s*</div>',
                re.DOTALL | re.IGNORECASE
            )

            for match in pattern.finditer(content):
                case_id = match.group(1)
                case_palata = int(match.group(2))
                case_number = match.group(3).strip()
                date = match.group(4).strip()
                subject = (match.group(5) or "").strip()
                result = match.group(6).strip()
                case_type = match.group(7).strip()

                # Clean up subject - remove extra whitespace and newlines
                subject = re.sub(r'\s+', ' ', subject)

                cases.append({
                    "id": case_id,
                    "palata": case_palata,
                    "case_number": case_number,
                    "date": date,
                    "subject": subject,
                    "result": result,
                    "type": case_type,
                })

            logger.debug(f"Page {page} of chamber {palata}: found {len(cases)} cases")
            return cases

        except Exception as e:
            logger.error(f"Failed to fetch cases page {page} for chamber {palata}: {e}")
            return []

    def _fetch_full_case(self, case_id: str, palata: int) -> Optional[Dict[str, Any]]:
        """
        Fetch the full text of a case.

        Returns dict with: title, text, or None on failure.
        """
        try:
            url = f"/ka/fullcase/{case_id}/{palata}"

            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()

            content = resp.text

            # Extract title from <title> tag
            title_match = re.search(r'<title>([^<]+)</title>', content)
            title = ""
            if title_match:
                title = html.unescape(title_match.group(1)).strip()

            # Extract full text from modalBody div
            # The content is between <div class="case-single mt-5" id="modalBody"> and its closing tag
            modal_start = content.find('id="modalBody">')
            if modal_start == -1:
                logger.warning(f"No modalBody found for case {case_id}")
                return None

            modal_start = content.find('>', modal_start) + 1

            # Find end of case content - typically ends with </div> before the script
            modal_end = content.find('</div>', modal_start)

            # Actually get a larger chunk and clean it
            # Look for closing </div> that matches, accounting for nesting
            depth = 1
            pos = modal_start
            while depth > 0 and pos < len(content):
                next_open = content.find('<div', pos)
                next_close = content.find('</div>', pos)

                if next_close == -1:
                    break

                if next_open != -1 and next_open < next_close:
                    depth += 1
                    pos = next_open + 4
                else:
                    depth -= 1
                    if depth == 0:
                        modal_end = next_close
                    else:
                        pos = next_close + 6

            if modal_end == -1 or modal_end <= modal_start:
                # Fallback: just take a large chunk
                modal_end = min(modal_start + 500000, len(content))

            raw_html = content[modal_start:modal_end]

            # Clean HTML to extract text
            text = self._extract_text(raw_html)

            if not text or len(text) < 50:
                logger.warning(f"Very short text for case {case_id}: {len(text)} chars")

            return {
                "title": title,
                "text": text,
            }

        except Exception as e:
            logger.warning(f"Failed to fetch full case {case_id}/{palata}: {e}")
            return None

    def _extract_text(self, raw_html: str) -> str:
        """Extract clean text from case HTML."""
        # Remove script and style tags
        text = re.sub(r'<script[^>]*>.*?</script>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<style[^>]*>.*?</style>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)

        # Remove HTML comments
        text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)

        # Convert breaks and paragraphs to newlines
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)

        # Remove all remaining HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)

        # Decode HTML entities (including numeric entities like &#4321;)
        text = html.unescape(text)

        # Clean up whitespace while preserving paragraph structure
        lines = []
        for line in text.split('\n'):
            line = re.sub(r'\s+', ' ', line).strip()
            if line:
                lines.append(line)

        return '\n'.join(lines)

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from the Georgian Supreme Court.

        Iterates through all chambers and all pages.
        """
        documents_yielded = 0

        for palata in CHAMBERS.keys():
            logger.info(f"Processing chamber {palata} ({CHAMBERS[palata]})...")

            page = 1
            consecutive_empty = 0
            max_consecutive_empty = 3

            while True:
                cases = self._get_cases_page(palata, page)

                if not cases:
                    consecutive_empty += 1
                    if consecutive_empty >= max_consecutive_empty:
                        logger.info(f"Chamber {palata}: no more pages after page {page}")
                        break
                    page += 1
                    continue

                consecutive_empty = 0

                for case_info in cases:
                    case_id = case_info["id"]
                    case_palata = case_info["palata"]

                    # Fetch full case text
                    full_case = self._fetch_full_case(case_id, case_palata)

                    if not full_case:
                        continue

                    if not full_case.get("text") or len(full_case.get("text", "")) < 50:
                        logger.warning(f"Skipping case {case_id}: no/short text")
                        continue

                    yield {
                        "id": case_id,
                        "palata": case_palata,
                        "chamber": CHAMBERS.get(case_palata, "unknown"),
                        "case_number": case_info.get("case_number", ""),
                        "date": case_info.get("date", ""),
                        "subject": case_info.get("subject", ""),
                        "result": case_info.get("result", ""),
                        "case_type": case_info.get("type", ""),
                        "title": full_case.get("title", ""),
                        "text": full_case.get("text", ""),
                    }

                    documents_yielded += 1

                page += 1

                # Safety limit for full bootstrap
                if page > 2000:  # ~60,000 cases per chamber max
                    logger.warning(f"Chamber {palata}: reached page limit 2000")
                    break

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents from recent pages (first 10 pages of each chamber).

        Since the API doesn't support date filtering, we fetch the most recent pages.
        """
        max_pages = 10  # Recent updates only

        for palata in CHAMBERS.keys():
            logger.info(f"Checking chamber {palata} ({CHAMBERS[palata]}) for updates...")

            for page in range(1, max_pages + 1):
                cases = self._get_cases_page(palata, page)

                if not cases:
                    break

                for case_info in cases:
                    case_id = case_info["id"]
                    case_palata = case_info["palata"]

                    full_case = self._fetch_full_case(case_id, case_palata)

                    if not full_case:
                        continue

                    if not full_case.get("text") or len(full_case.get("text", "")) < 50:
                        continue

                    yield {
                        "id": case_id,
                        "palata": case_palata,
                        "chamber": CHAMBERS.get(case_palata, "unknown"),
                        "case_number": case_info.get("case_number", ""),
                        "date": case_info.get("date", ""),
                        "subject": case_info.get("subject", ""),
                        "result": case_info.get("result", ""),
                        "case_type": case_info.get("type", ""),
                        "title": full_case.get("title", ""),
                        "text": full_case.get("text", ""),
                    }

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        case_id = raw.get("id", "")
        palata = raw.get("palata", 0)
        chamber = raw.get("chamber", CHAMBERS.get(palata, "unknown"))

        # Create unique document ID
        doc_id = f"GE-SC/{case_id}/{palata}"

        case_number = raw.get("case_number", "")
        title = raw.get("title", "") or f"საქმე {case_number}"
        text = raw.get("text", "")
        date_str = raw.get("date", "")
        subject = raw.get("subject", "")
        result = raw.get("result", "")
        case_type = raw.get("case_type", "")

        # Parse date if in Georgian format (YYYY-MM-DD from the HTML)
        # The date appears to already be in YYYY-MM-DD format from the search results

        # Build URL
        url = f"{BASE_URL}/ka/fullcase/{case_id}/{palata}"

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "GE/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": text,  # MANDATORY FULL TEXT
            "date": date_str,
            "url": url,
            # Additional metadata
            "case_number": case_number,
            "chamber": chamber,
            "chamber_id": palata,
            "subject": subject,
            "result": result,
            "case_type": case_type,
            "language": "ka",
            "court": "საქართველოს უზენაესი სასამართლო",
            "court_en": "Supreme Court of Georgia",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Georgian Supreme Court endpoints...")

        # Test homepage
        print("\n1. Testing homepage...")
        try:
            resp = self.client.get("/ka/cases")
            print(f"   Status: {resp.status_code}")
            print(f"   Page length: {len(resp.text)} chars")
            if "გადაწყვეტილებები" in resp.text:
                print("   Decisions page found: YES")
            else:
                print("   Decisions page found: NO")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test search for each chamber
        print("\n2. Testing case search...")
        for palata, name in CHAMBERS.items():
            try:
                cases = self._get_cases_page(palata, 1)
                print(f"   Chamber {palata} ({name}): {len(cases)} cases on page 1")
                if cases:
                    print(f"      Sample: {cases[0]['case_number']} ({cases[0]['date']})")
            except Exception as e:
                print(f"   Chamber {palata}: ERROR - {e}")

        # Test full case fetch
        print("\n3. Testing full case fetch...")
        try:
            cases = self._get_cases_page(0, 1)  # Administrative chamber
            if cases:
                case = cases[0]
                result = self._fetch_full_case(case["id"], case["palata"])
                if result:
                    print(f"   Title: {result['title'][:60]}...")
                    print(f"   Text length: {len(result.get('text', ''))} chars")
                    if result.get('text'):
                        # Show first 150 chars of Georgian text
                        preview = result['text'][:150].replace('\n', ' ')
                        print(f"   Sample text: {preview}...")
                else:
                    print("   ERROR: No result returned")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = GeorgianSupremeCourtScraper()

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
