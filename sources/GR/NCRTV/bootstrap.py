#!/usr/bin/env python3
"""
GR/NCRTV -- Greek National Council for Radio and Television (ESR) Data Fetcher

Fetches decisions from the ESR DSpace repository hosted by EKT.

Strategy:
  - Paginate through DSpace simple-search filtered by year
  - Extract item handles and reference numbers from listing pages
  - Fetch each item page for metadata (date, subject, station, medium)
  - Download PDF bitstream for full text extraction

Endpoints:
  - Repository: http://repository-esr.ekt.gr/esr/handle/20.500.12039/20
  - Search: simple-search?filterquery={year}&filtername=search_ekt.year&filtertype=equals
  - Bitstream: /esr/bitstream/20.500.12039/{item_id}/1/{ref}.pdf

Data:
  - ~10,000+ broadcasting regulator decisions
  - Categories: sanctions, licensing, content regulation, advertising
  - Language: Greek
  - Rate limit: 1 request/second

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
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GR.NCRTV")

BASE_URL = "http://repository-esr.ekt.gr"
COLLECTION_HANDLE = "20.500.12039/20"
ITEMS_PER_PAGE = 100
# Years to scan (ESR repository has decisions from ~1990 to present)
YEARS = list(range(2026, 1989, -1))


class GreekNCRTVScraper(BaseScraper):
    """
    Scraper for GR/NCRTV -- Greek National Council for Radio and Television.
    Country: GR
    URL: http://repository-esr.ekt.gr

    Data types: doctrine
    Auth: none (Open public access, Public Domain license)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,application/xml",
                "Accept-Language": "el,en",
            },
            timeout=60,
        )

    def _search_url(self, year: int, start: int = 0) -> str:
        """Build DSpace simple-search URL for a given year."""
        return (
            f"/esr/handle/{COLLECTION_HANDLE}/simple-search"
            f"?filterquery={year}"
            f"&filtername=search_ekt.year"
            f"&filtertype=equals"
            f"&rpp={ITEMS_PER_PAGE}"
            f"&sort_by=dc.date.issued_dt"
            f"&order=DESC"
            f"&start={start}"
        )

    def _get_total_for_year(self, year: int) -> int:
        """Get total number of results for a year from the search page."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(self._search_url(year, 0))
            resp.raise_for_status()
            # DSpace shows "Results X-Y of Z" or similar
            total_match = re.search(
                r'(\d+)\s*-\s*(\d+)\s+(?:of|από)\s+(\d+)',
                resp.text
            )
            if total_match:
                return int(total_match.group(3))
            # Also try Greek pattern "Αποτελέσματα X-Y από Z"
            total_match2 = re.search(r'από\s+(\d+)', resp.text)
            if total_match2:
                return int(total_match2.group(1))
            # If only one page of results, count items on page
            items = self._parse_list_page(resp.text)
            return len(items)
        except Exception as e:
            logger.error(f"Failed to get total for year {year}: {e}")
            return 0

    def _parse_list_page(self, html: str) -> List[Dict[str, Any]]:
        """Parse a DSpace search results page to extract item handles."""
        items = []
        seen = set()

        # DSpace list items typically have links to /esr/handle/20.500.12039/{id}
        # with title text
        pattern = re.compile(
            r'<a\s+href="(/esr/handle/20\.500\.12039/(\d+))"[^>]*>\s*(.*?)\s*</a>',
            re.DOTALL | re.IGNORECASE
        )

        for match in pattern.finditer(html):
            href = match.group(1)
            item_id = match.group(2)
            title_html = match.group(3)
            title = re.sub(r'<[^>]+>', '', title_html).strip()
            title = html_module.unescape(title)

            # Skip collection handles (the parent collection)
            if item_id == "20":
                continue
            if item_id in seen:
                continue
            # Skip very short titles (likely navigation)
            if len(title) < 3:
                continue
            seen.add(item_id)

            items.append({
                "item_id": item_id,
                "handle_url": href,
                "title": title,
            })

        return items

    def _scrape_list_page(self, year: int, start: int = 0) -> List[Dict[str, Any]]:
        """Fetch and parse a search results page for a year."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(self._search_url(year, start))
            resp.raise_for_status()
            items = self._parse_list_page(resp.text)
            logger.info(f"Year {year}, offset {start}: found {len(items)} items")
            return items
        except Exception as e:
            logger.error(f"Failed to scrape year {year} offset {start}: {e}")
            return []

    def _fetch_item_metadata(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch an individual item page and extract metadata."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(item["handle_url"])
            resp.raise_for_status()
            content = resp.text

            result = {
                "item_id": item["item_id"],
                "title": item["title"],
                "handle_url": item["handle_url"],
            }

            # Extract decision reference number (e.g., "230-2024")
            ref_match = re.search(
                r'(?:Αριθμός\s*(?:Απόφασης|απόφασης)|dc\.identifier\.other)[^<]*?'
                r'[\s:>]+\s*(\d+[-/]\d{4})',
                content, re.IGNORECASE
            )
            if ref_match:
                result["decision_ref"] = ref_match.group(1)
            else:
                # Try to extract from title (often "Apofasi NUMBER-YEAR")
                ref_from_title = re.search(r'(\d+[-/]\d{4})', item["title"])
                if ref_from_title:
                    result["decision_ref"] = ref_from_title.group(1)

            # Extract metadata fields from DSpace metadata table
            # DSpace uses <td class="metadataFieldLabel">Label</td><td class="metadataFieldValue">Value</td>
            meta_patterns = {
                "date": r'(?:Ημερομηνία|dc\.date\.issued|Ημ/νία)[^<]*</t[dh]>\s*<t[dh][^>]*>\s*([\d/.-]+)',
                "subject": r'(?:Θέμα|dc\.subject|Θεματική)[^<]*</t[dh]>\s*<t[dh][^>]*>\s*([^<]+)',
                "station": r'(?:Σταθμός|Τηλεοπτικός|Ραδιοφωνικός)[^<]*</t[dh]>\s*<t[dh][^>]*>\s*([^<]+)',
                "medium": r'(?:Μέσο|Κατηγορία\s*Μέσου)[^<]*</t[dh]>\s*<t[dh][^>]*>\s*([^<]+)',
                "case_number": r'(?:Φάκελος|Αριθμός\s*Φακέλου)[^<]*</t[dh]>\s*<t[dh][^>]*>\s*([^<]+)',
            }

            for field, pattern in meta_patterns.items():
                match = re.search(pattern, content, re.IGNORECASE | re.DOTALL)
                if match:
                    value = match.group(1).strip()
                    value = re.sub(r'<[^>]+>', '', value).strip()
                    value = html_module.unescape(value)
                    if value:
                        result[field] = value

            # Parse date to ISO format
            if "date" in result:
                raw_date = result["date"]
                for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d.%m.%Y"):
                    try:
                        dt = datetime.strptime(raw_date, fmt)
                        result["date"] = dt.strftime("%Y-%m-%d")
                        break
                    except ValueError:
                        continue

            # Find PDF bitstream URL
            pdf_match = re.search(
                r'href="(/esr/bitstream/20\.500\.12039/[^"]+\.pdf)"',
                content, re.IGNORECASE
            )
            if pdf_match:
                result["pdf_url"] = pdf_match.group(1)
            else:
                # Try alternative bitstream pattern
                pdf_match2 = re.search(
                    r'href="([^"]*bitstream[^"]*\.pdf)"',
                    content, re.IGNORECASE
                )
                if pdf_match2:
                    result["pdf_url"] = pdf_match2.group(1)

            # Extract decision ref from PDF filename (e.g., "21-2026.pdf" -> "21-2026")
            if result.get("pdf_url") and not result.get("decision_ref"):
                pdf_name_match = re.search(r'/(\d+[-]\d{4})\.pdf', result["pdf_url"])
                if pdf_name_match:
                    result["decision_ref"] = pdf_name_match.group(1)

            # Unescape any HTML entities in subject
            if "subject" in result:
                result["subject"] = html_module.unescape(result["subject"])

            return result

        except Exception as e:
            logger.error(f"Failed to fetch item {item['item_id']}: {e}")
            return None

    def _extract_pdf_text(self, pdf_url: str) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="GR/NCRTV",
            source_id="",
            pdf_url=pdf_url,
            table="doctrine",
        ) or ""

    def _fetch_detail(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch item metadata and full text from PDF."""
        result = self._fetch_item_metadata(item)
        if not result:
            return None

        # Extract full text from PDF
        pdf_url = result.get("pdf_url")
        if pdf_url:
            text = self._extract_pdf_text(pdf_url)
            result["text"] = text
        else:
            result["text"] = ""
            logger.warning(f"No PDF found for item {item['item_id']}")

        if not result.get("text"):
            logger.warning(f"No text extracted for item {item['item_id']}")

        return result

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw data into standard schema."""
        item_id = raw.get("item_id", "unknown")
        decision_ref = raw.get("decision_ref") or item_id

        return {
            "_id": f"GR/NCRTV/{decision_ref}",
            "_source": "GR/NCRTV",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": f"{BASE_URL}{raw.get('handle_url', '/esr/handle/20.500.12039/' + item_id)}",
            "decision_ref": decision_ref,
            "subject": raw.get("subject", ""),
            "station": raw.get("station", ""),
            "medium": raw.get("medium", ""),
            "case_number": raw.get("case_number", ""),
            "pdf_url": f"{BASE_URL}{raw['pdf_url']}" if raw.get("pdf_url") else None,
            "language": "el",
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all decisions (raw dicts), iterating by year."""
        for year in YEARS:
            total = self._get_total_for_year(year)
            if total == 0:
                logger.info(f"Year {year}: no results, skipping")
                continue

            logger.info(f"Year {year}: {total} results")

            for start in range(0, total, ITEMS_PER_PAGE):
                items = self._scrape_list_page(year, start)
                if not items:
                    break
                for item in items:
                    detail = self._fetch_detail(item)
                    if detail:
                        yield detail

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Yield recently added decisions (current year + previous year)."""
        current_year = datetime.now().year
        for year in [current_year, current_year - 1]:
            items = self._scrape_list_page(year, 0)
            for item in items:
                detail = self._fetch_detail(item)
                if detail:
                    if since and detail.get("date") and detail["date"] < since:
                        return
                    yield detail


def main():
    scraper = GreekNCRTVScraper()

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
