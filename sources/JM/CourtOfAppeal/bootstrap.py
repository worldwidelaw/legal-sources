#!/usr/bin/env python3
"""
JM/CourtOfAppeal -- Jamaica Court of Appeal Judgments Fetcher

Fetches Jamaica Court of Appeal judgments from courtofappeal.gov.jm.

Strategy:
  - Iterate through year taxonomy IDs for each category page
  - Extract node IDs from listing tables (citation, title, date, node link)
  - Visit each node page to get PDF URL
  - Download PDFs and extract full text using pdfplumber

Endpoints:
  - Civil: https://www.courtofappeal.gov.jm/civil-judgments?field_year_target_id={tid}
  - Criminal: https://www.courtofappeal.gov.jm/criminal-judgments?field_year_target_id={tid}
  - Civil Endorsements: https://www.courtofappeal.gov.jm/civil-judgments-endorsement-and-memoranda?field_year_target_id={tid}
  - Criminal Endorsements: https://www.courtofappeal.gov.jm/criminal-judgments-endorsement-and-memoranda?field_year_target_id={tid}

Data:
  - Court of Appeal civil and criminal judgments 1990-2026
  - Full text extracted from PDF attachments
  - Rate limit: 1 request/second

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.JM.CourtOfAppeal")

BASE_URL = "https://www.courtofappeal.gov.jm"

# Mapping of year to Drupal taxonomy term ID
YEAR_TIDS = {
    2026: 157, 2025: 155, 2024: 148, 2023: 147, 2022: 114, 2021: 105,
    2020: 90, 2019: 66, 2018: 64, 2017: 62, 2016: 28, 2015: 27,
    2014: 26, 2013: 25, 2012: 24, 2011: 23, 2010: 22, 2009: 21,
    2008: 20, 2007: 19, 2006: 18, 2005: 17, 2004: 16, 2003: 15,
    2002: 14, 2001: 13, 2000: 12, 1999: 79, 1998: 80, 1997: 81,
    1996: 82, 1995: 83, 1994: 84, 1993: 85, 1992: 86, 1991: 87,
    1990: 88,
}

# Listing paths and their category labels
LISTING_PATHS = [
    ("/civil-judgments", "civil"),
    ("/criminal-judgments", "criminal"),
    ("/civil-judgments-endorsement-and-memoranda", "civil_endorsement"),
    ("/criminal-judgments-endorsement-and-memoranda", "criminal_endorsement"),
]


class JamaicaCourtOfAppealScraper(BaseScraper):
    """
    Scraper for JM/CourtOfAppeal -- Jamaica Court of Appeal Judgments.
    Country: JM
    URL: https://www.courtofappeal.gov.jm

    Data types: case_law
    Auth: none (Public Access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-JM,en;q=0.9",
            },
            timeout=120,
        )

    def _get_listings(self, path: str, category: str, year_tids: Dict[int, int] = None) -> List[Dict]:
        """
        Fetch all judgment listings from a given listing path across all years.
        Returns list of dicts with node_id, citation, title, date_iso, category.
        """
        if year_tids is None:
            year_tids = YEAR_TIDS

        all_judgments = []
        seen_nodes = set()

        for year, tid in sorted(year_tids.items(), reverse=True):
            try:
                self.rate_limiter.wait()
                url = f"{path}?field_year_target_id={tid}"
                resp = self.client.get(url)
                resp.raise_for_status()
                content = resp.text

                # Parse table rows
                entries = self._parse_listing_table(content, category)

                for entry in entries:
                    if entry["node_id"] not in seen_nodes:
                        seen_nodes.add(entry["node_id"])
                        all_judgments.append(entry)

                if entries:
                    logger.info(f"{path} year={year}: {len(entries)} judgments")

            except Exception as e:
                logger.warning(f"Failed to fetch {path}?field_year_target_id={tid}: {e}")
                continue

        return all_judgments

    def _parse_listing_table(self, html_content: str, category: str) -> List[Dict]:
        """Parse the listing table HTML and extract judgment entries."""
        entries = []

        # Match table rows with citation, title+link, date
        # Pattern: <td>citation</td> <td><a href="/node/ID">title</a></td> <td><time datetime="...">date</time></td>
        row_pattern = re.compile(
            r'<td[^>]*class="views-field views-field-nothing"[^>]*>\s*(.*?)\s*</td>\s*'
            r'<td[^>]*class="views-field views-field-title"[^>]*>\s*<a\s+href="/node/(\d+)"[^>]*>\s*(.*?)\s*</a>\s*</td>\s*'
            r'<td[^>]*class="views-field views-field-field-date"[^>]*>\s*<time\s+datetime="([^"]*)"[^>]*>[^<]*</time>',
            re.DOTALL | re.IGNORECASE
        )

        for match in row_pattern.finditer(html_content):
            citation = html.unescape(match.group(1).strip())
            node_id = match.group(2).strip()
            title = html.unescape(match.group(3).strip())
            date_iso = match.group(4).strip()

            # Clean citation of any remaining HTML
            citation = re.sub(r'<[^>]+>', '', citation).strip()

            # Normalize date to YYYY-MM-DD
            if 'T' in date_iso:
                date_iso = date_iso.split('T')[0]

            entries.append({
                "node_id": node_id,
                "citation": citation,
                "title": title,
                "date_iso": date_iso,
                "category": category,
            })

        # Fallback: simpler extraction if structured pattern fails
        if not entries:
            node_links = re.findall(
                r'<a\s+href="/node/(\d+)"[^>]*>\s*([^<]+)',
                html_content, re.IGNORECASE
            )
            for node_id, title in node_links:
                title = html.unescape(title.strip())
                # Filter out nav links
                if title and not title.lower().startswith(('home', 'about', 'contact', 'log', 'more')):
                    entries.append({
                        "node_id": node_id,
                        "citation": "",
                        "title": title,
                        "date_iso": "",
                        "category": category,
                    })

        return entries

    def _get_pdf_url(self, node_id: str) -> Optional[str]:
        """Fetch a node page and extract the PDF URL."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(f"/node/{node_id}")
            resp.raise_for_status()
            content = resp.text

            # Look for PDF link in /sites/default/files/judgments/
            pdf_match = re.search(
                r'href="(/sites/default/files/judgments/[^"]+\.pdf)"',
                content, re.IGNORECASE
            )
            if pdf_match:
                return pdf_match.group(1)

            # Broader: any PDF in /sites/default/files/
            pdf_match = re.search(
                r'href="(/sites/default/files/[^"]+\.pdf)"',
                content, re.IGNORECASE
            )
            if pdf_match:
                return pdf_match.group(1)

            return None
        except Exception as e:
            logger.warning(f"Failed to get PDF URL for node {node_id}: {e}")
            return None

    def _download_and_extract_pdf(self, pdf_path: str, node_id: str) -> Optional[str]:
        """Download a PDF and extract text using centralized PDF extractor."""
        full_url = f"{BASE_URL}{pdf_path}"
        text = extract_pdf_markdown(
            source="JM/CourtOfAppeal",
            source_id=f"JM-COA-{node_id}",
            pdf_url=full_url,
            table="case_law",
        )
        if text and len(text.strip()) > 50:
            return text.strip()
        return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw judgment record into the standard schema."""
        year = ""
        if raw.get("date_iso"):
            try:
                year = raw["date_iso"][:4]
            except (IndexError, TypeError):
                pass

        return {
            "_id": f"JM-COA-{raw['node_id']}",
            "_source": "JM/CourtOfAppeal",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date_iso", ""),
            "year": year,
            "citation": raw.get("citation", ""),
            "category": raw.get("category", ""),
            "node_id": raw.get("node_id", ""),
            "pdf_url": raw.get("pdf_url", ""),
            "url": f"{BASE_URL}/node/{raw['node_id']}",
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all judgments from all categories and years."""
        all_entries = []
        for path, category in LISTING_PATHS:
            entries = self._get_listings(path, category)
            all_entries.extend(entries)
            logger.info(f"Category '{category}': {len(entries)} entries found")

        logger.info(f"Total entries to process: {len(all_entries)}")

        for entry in all_entries:
            pdf_url = self._get_pdf_url(entry["node_id"])
            if not pdf_url:
                logger.warning(f"No PDF found for node {entry['node_id']} ({entry['title']})")
                continue

            text = self._download_and_extract_pdf(pdf_url, entry["node_id"])
            if not text:
                logger.warning(f"No text extracted from PDF for node {entry['node_id']}")
                continue

            entry["text"] = text
            entry["pdf_url"] = f"{BASE_URL}{pdf_url}"
            yield self.normalize(entry)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch judgments updated since a given date."""
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        since_date = datetime.fromisoformat(since).date()
        current_year = datetime.now().year

        # Only check recent years for updates
        recent_tids = {y: tid for y, tid in YEAR_TIDS.items() if y >= since_date.year}

        for path, category in LISTING_PATHS:
            entries = self._get_listings(path, category, year_tids=recent_tids)
            for entry in entries:
                if entry.get("date_iso") and entry["date_iso"] >= since:
                    pdf_url = self._get_pdf_url(entry["node_id"])
                    if not pdf_url:
                        continue
                    text = self._download_and_extract_pdf(pdf_url, entry["node_id"])
                    if not text:
                        continue
                    entry["text"] = text
                    entry["pdf_url"] = f"{BASE_URL}{pdf_url}"
                    yield self.normalize(entry)

    def test_connection(self) -> bool:
        """Quick test to verify the site is reachable."""
        try:
            resp = self.client.get("/civil-judgments")
            return resp.status_code == 200
        except Exception:
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="JM/CourtOfAppeal bootstrapper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to execute")
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch a sample of records")
    parser.add_argument("--since", type=str, default=None,
                        help="ISO date for incremental updates")
    args = parser.parse_args()

    scraper = JamaicaCourtOfAppealScraper()

    if args.command == "test":
        if scraper.test_connection():
            print("Connection OK")
            sys.exit(0)
        else:
            print("Connection FAILED")
            sys.exit(1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        max_records = 15 if args.sample else 99999

        if args.sample:
            # For sample mode, only fetch from the most recent years
            sample_tids = {y: tid for y, tid in YEAR_TIDS.items() if y >= 2024}
            all_entries = []
            for path, category in LISTING_PATHS:
                entries = scraper._get_listings(path, category, year_tids=sample_tids)
                all_entries.extend(entries)

            logger.info(f"Sample mode: {len(all_entries)} entries from recent years")

            for entry in all_entries:
                if count >= max_records:
                    break

                pdf_url = scraper._get_pdf_url(entry["node_id"])
                if not pdf_url:
                    logger.warning(f"No PDF for node {entry['node_id']}")
                    continue

                text = scraper._download_and_extract_pdf(pdf_url, entry["node_id"])
                if not text:
                    logger.warning(f"No text from PDF for node {entry['node_id']}")
                    continue

                entry["text"] = text
                entry["pdf_url"] = f"{BASE_URL}{pdf_url}"
                record = scraper.normalize(entry)

                out_file = sample_dir / f"{record['_id']}.json"
                with open(out_file, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

                count += 1
                logger.info(f"[{count}/{max_records}] Saved {record['_id']} ({len(text)} chars)")

        else:
            for record in scraper.fetch_all():
                out_file = sample_dir / f"{record['_id']}.json"
                with open(out_file, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
                logger.info(f"[{count}] Saved {record['_id']}")

        print(f"\nBootstrap complete: {count} records saved to {sample_dir}")

    elif args.command == "update":
        since = args.since or "2024-01-01"
        count = 0
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        for record in scraper.fetch_updates(since):
            out_file = sample_dir / f"{record['_id']}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info(f"[{count}] Updated {record['_id']}")

        print(f"\nUpdate complete: {count} records updated since {since}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
