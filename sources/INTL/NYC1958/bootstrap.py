#!/usr/bin/env python3
"""
INTL/NYC1958 -- New York Convention 1958 Case Law Database

Fetches international arbitration case law from the NYC 1958 Guide.

Strategy:
  - Paginated search results at opac_view=2 (case law view), 200/page
  - Scrape case detail pages for structured metadata (dt/dd pairs)
  - Download PDFs via doc_num.php?explnum_id=XXXX
  - Extract text via common/pdf_extract
  - ~3,957 case law documents from 60+ jurisdictions

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.NYC1958")

BASE_URL = "https://newyorkconvention1958.org"
SEARCH_URL = (
    f"{BASE_URL}/index.php?lvl=more_results&mode=keyword"
    "&user_query=*&opac_view=2&nb_per_page=200"
)
DETAIL_URL = f"{BASE_URL}/index.php?lvl=notice_display&id={{id}}&opac_view=2"
PDF_URL = f"{BASE_URL}/doc_num.php?explnum_id={{explnum_id}}"


class NYC1958Scraper(BaseScraper):
    """
    Scraper for INTL/NYC1958 -- New York Convention 1958 Case Law.
    Country: INTL
    URL: https://newyorkconvention1958.org/

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    def _get_case_ids_from_search(self) -> list[dict]:
        """Paginate through search results and collect case IDs + basic info."""
        all_cases = []
        page = 1
        while True:
            url = f"{SEARCH_URL}&page={page}"
            logger.info(f"Fetching search page {page}...")
            try:
                r = self.session.get(url, timeout=60)
                r.raise_for_status()
            except requests.RequestException as e:
                logger.error(f"Failed to fetch search page {page}: {e}")
                break

            soup = BeautifulSoup(r.text, "html.parser")

            # Find all case links (notice_display with opac_view=2)
            case_links = soup.find_all(
                "a", href=re.compile(r"lvl=notice_display&id=\d+")
            )
            page_ids = set()
            for link in case_links:
                href = link.get("href", "")
                m = re.search(r"id=(\d+)", href)
                if m:
                    case_id = m.group(1)
                    if case_id not in page_ids:
                        page_ids.add(case_id)
                        all_cases.append({"id": case_id})

            if not page_ids:
                logger.info(f"No cases found on page {page}, stopping.")
                break

            logger.info(f"Page {page}: found {len(page_ids)} cases (total: {len(all_cases)})")
            page += 1
            time.sleep(1)

        return all_cases

    def _scrape_case_detail(self, case_id: str) -> Optional[dict]:
        """Scrape a case detail page for metadata and PDF explnum_id."""
        url = DETAIL_URL.format(id=case_id)
        try:
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch case {case_id}: {e}")
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        # Extract metadata from table rows (key-value pairs in 2-cell rows)
        metadata = {}
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) >= 2:
                    key = cells[0].get_text(strip=True).rstrip(":").lower()
                    val = cells[1].get_text(strip=True)
                    if key and val:
                        metadata[key] = val

        if not metadata:
            logger.warning(f"No metadata found for case {case_id}")
            return None

        # Extract explnum_id for PDF
        explnum_id = None
        # Look for vig_num.php img src
        for img in soup.find_all("img", src=re.compile(r"explnum_id=\d+")):
            m = re.search(r"explnum_id=(\d+)", img.get("src", ""))
            if m:
                explnum_id = m.group(1)
                break
        if not explnum_id:
            # Check onclick with open_visionneuse(sendToVisionneuse, ID)
            for tag in soup.find_all(attrs={"onclick": re.compile(r"open_visionneuse")}):
                onclick = tag.get("onclick", "")
                m = re.search(r"open_visionneuse\(\w+,\s*(\d+)\)", onclick)
                if m:
                    explnum_id = m.group(1)
                    break

        # Parse date
        date_str = metadata.get("date", "")
        iso_date = self._parse_date(date_str)

        return {
            "case_id": case_id,
            "country": metadata.get("country", ""),
            "court": metadata.get("court", ""),
            "date_raw": date_str,
            "date": iso_date,
            "parties": metadata.get("parties", ""),
            "case_number": metadata.get("case number", ""),
            "provisions": metadata.get("applicable nyc provisions", ""),
            "source_ref": metadata.get("source", ""),
            "language": metadata.get("languages", metadata.get("language", "")),
            "explnum_id": explnum_id,
        }

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string like '10 January 2023' to ISO 8601."""
        if not date_str:
            return None
        # Try common formats
        for fmt in ("%d %B %Y", "%B %d, %Y", "%d %b %Y", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        # Try extracting year at least
        m = re.search(r"(\d{4})", date_str)
        if m:
            return f"{m.group(1)}-01-01"
        return None

    def _download_and_extract_pdf(self, explnum_id: str, case_id: str) -> Optional[str]:
        """Download PDF and extract text."""
        pdf_url = PDF_URL.format(explnum_id=explnum_id)
        return extract_pdf_markdown(
            source="INTL/NYC1958",
            source_id=f"NYC1958-{case_id}",
            pdf_url=pdf_url,
            table="case_law",
        ) or ""

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw record into standard schema."""
        text = raw.get("text", "").strip()
        if not text:
            return None

        case_id = raw.get("case_id", "")
        parties = raw.get("parties", "")
        court = raw.get("court", "")

        title = parties if parties else f"Case {case_id}"
        if court and parties:
            title = f"{parties} ({court})"

        return {
            "_id": f"NYC1958-{case_id}",
            "_source": "INTL/NYC1958",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": DETAIL_URL.format(id=case_id),
            "case_number": raw.get("case_number", ""),
            "jurisdiction": raw.get("country", ""),
            "court": court,
            "parties": parties,
            "provisions": raw.get("provisions", ""),
            "language": raw.get("language", ""),
            "source_ref": raw.get("source_ref", ""),
            "pdf_url": PDF_URL.format(explnum_id=raw.get("explnum_id", "")),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all NYC 1958 case law documents."""
        cases = self._get_case_ids_from_search()
        logger.info(f"Total cases found: {len(cases)}")

        total_docs = 0
        skipped_no_pdf = 0
        skipped_no_text = 0

        for i, case in enumerate(cases):
            case_id = case["id"]
            logger.info(f"[{i+1}/{len(cases)}] Fetching case {case_id}")

            detail = self._scrape_case_detail(case_id)
            if not detail:
                continue

            if not detail.get("explnum_id"):
                skipped_no_pdf += 1
                logger.info(f"  No PDF for case {case_id}, skipping")
                continue

            text = self._download_and_extract_pdf(detail["explnum_id"], case_id)
            if text:
                detail["text"] = text
                yield detail
                total_docs += 1
            else:
                skipped_no_text += 1
                logger.warning(f"  No text extracted for case {case_id}")

            time.sleep(1)

        logger.info(
            f"Total: {total_docs} docs, {skipped_no_pdf} no PDF, "
            f"{skipped_no_text} no text extracted"
        )

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent cases (re-fetches all and filters by date)."""
        since_str = since.strftime("%Y-%m-%d") if isinstance(since, datetime) else str(since)
        logger.info(f"Fetching updates since {since_str}")

        for record in self.fetch_all():
            if record.get("date") and record["date"] >= since_str:
                yield record


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="INTL/NYC1958 data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = NYC1958Scraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            # Test search
            r = scraper.session.get(f"{SEARCH_URL}&page=1", timeout=30)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            links = soup.find_all("a", href=re.compile(r"lvl=notice_display&id=\d+"))
            ids = set()
            for link in links:
                m = re.search(r"id=(\d+)", link.get("href", ""))
                if m:
                    ids.add(m.group(1))
            logger.info(f"Search page 1: {len(ids)} case IDs found")

            # Test detail page
            test_id = next(iter(ids)) if ids else "6781"
            detail = scraper._scrape_case_detail(test_id)
            if detail:
                logger.info(f"Case {test_id}: {detail.get('parties', 'N/A')[:80]}")
                logger.info(f"  Court: {detail.get('court', 'N/A')}")
                logger.info(f"  Date: {detail.get('date', 'N/A')}")
                logger.info(f"  PDF explnum_id: {detail.get('explnum_id', 'None')}")

                if detail.get("explnum_id"):
                    text = scraper._download_and_extract_pdf(detail["explnum_id"], test_id)
                    if text:
                        logger.info(f"  PDF text: {len(text)} chars")
                        logger.info(f"  Preview: {text[:200]}")
                    else:
                        logger.warning("  PDF text extraction failed")

            logger.info("Connectivity test passed!")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
