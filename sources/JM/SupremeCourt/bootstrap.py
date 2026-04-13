#!/usr/bin/env python3
"""
JM/SupremeCourt -- Jamaica Supreme Court Judgments Fetcher

Fetches Jamaica Supreme Court judgments from supremecourt.gov.jm.

Strategy:
  - Paginate through Drupal listing at /content/judgments?page=N
  - Extract node IDs from listing, visit each node page for metadata + PDF URL
  - Download PDFs and extract full text using pdfplumber

Endpoints:
  - Listing: https://supremecourt.gov.jm/content/judgments?page={N}
  - Node: https://supremecourt.gov.jm/node/{id}
  - PDFs: https://supremecourt.gov.jm/sites/default/files/judgments/{title}.pdf

Data:
  - Supreme Court civil and criminal judgments
  - Recent years (listings show ~24 judgments across 3 pages)
  - Rate limit: 2 seconds between requests

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
import tempfile
import os
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.parse import quote, urljoin

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
logger = logging.getLogger("legal-data-hunter.JM.SupremeCourt")

BASE_URL = "https://supremecourt.gov.jm"


class JamaicaSupremeCourtScraper(BaseScraper):
    """
    Scraper for JM/SupremeCourt -- Jamaica Supreme Court Judgments.
    Country: JM
    URL: https://supremecourt.gov.jm/content/judgments

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

    def _get_listings_page(self, page: int = 0) -> List[Dict]:
        """
        Fetch a page of judgment listings.
        Returns list of dicts with node_id, case_number, title, judge, date.
        """
        judgments = []

        try:
            self.rate_limiter.wait()
            url = f"/content/judgments?page={page}"
            resp = self.client.get(url)
            resp.raise_for_status()
            content = resp.text

            # Extract table rows: each row has case number, title (linked to /node/ID), judge, date
            # Pattern: <td>case_number</td> <td><a href="/node/ID">Title</a></td> <td>judge</td> <td>date</td>
            row_pattern = re.compile(
                r'<td[^>]*>\s*([^<]+?)\s*</td>\s*'          # Case number
                r'<td[^>]*>\s*<a\s+href="(/node/(\d+))"[^>]*>\s*([^<]+?)\s*</a>\s*</td>\s*'  # Title + node ID
                r'<td[^>]*>\s*(?:<a[^>]*>)?\s*([^<]+?)\s*(?:</a>)?\s*</td>\s*'  # Judge (may be linked)
                r'<td[^>]*>\s*([^<]*?)\s*</td>',            # Date
                re.IGNORECASE | re.DOTALL
            )

            for match in row_pattern.finditer(content):
                case_number = html.unescape(match.group(1).strip())
                node_url = match.group(2).strip()
                node_id = match.group(3).strip()
                title = html.unescape(match.group(4).strip())
                judge = html.unescape(match.group(5).strip())
                date_str = match.group(6).strip()

                # Skip header rows or empty entries
                if case_number.lower() in ('case number', '') or not node_id:
                    continue

                judgments.append({
                    "node_id": node_id,
                    "node_url": node_url,
                    "case_number": case_number,
                    "title": title,
                    "judge": judge,
                    "date_raw": date_str,
                })

            # Fallback: simpler extraction if complex pattern fails
            if not judgments:
                logger.debug("Complex pattern failed, trying simple node extraction")
                node_links = re.findall(
                    r'href="(/node/(\d+))"[^>]*>\s*([^<]+)',
                    content, re.IGNORECASE
                )
                for node_url, node_id, title in node_links:
                    title = html.unescape(title.strip())
                    if title and not title.lower().startswith(('home', 'about', 'contact', 'log')):
                        judgments.append({
                            "node_id": node_id,
                            "node_url": node_url,
                            "case_number": "",
                            "title": title,
                            "judge": "",
                            "date_raw": "",
                        })

            # Deduplicate by node_id
            seen = set()
            unique = []
            for j in judgments:
                if j["node_id"] not in seen:
                    seen.add(j["node_id"])
                    unique.append(j)
            judgments = unique

            logger.info(f"Page {page}: Found {len(judgments)} judgments")
            return judgments

        except Exception as e:
            logger.warning(f"Failed to get listings page {page}: {e}")
            return []

    def _get_node_details(self, node_id: str) -> Dict[str, Any]:
        """
        Fetch a judgment node page and extract metadata + PDF URL.
        """
        details = {
            "neutral_citation": "",
            "case_number": "",
            "title": "",
            "judge": "",
            "date_delivered": "",
            "year": "",
            "topics": [],
            "pdf_url": "",
        }

        try:
            self.rate_limiter.wait()
            resp = self.client.get(f"/node/{node_id}")
            resp.raise_for_status()
            content = resp.text

            # Extract neutral citation: [YYYY] JMSC Civ/Crim N
            citation_match = re.search(
                r'\[(\d{4})\]\s*JMSC\s+(?:Civ|Crim)\s+\d+',
                content
            )
            if citation_match:
                details["neutral_citation"] = citation_match.group(0).strip()

            # Extract case number - Drupal field label followed by value
            case_match = re.search(
                r'Case\s*Number\s*</div>\s*<div[^>]*>\s*([^<]+)',
                content, re.IGNORECASE
            )
            if not case_match:
                case_match = re.search(
                    r'Case\s*Number\s*:?\s*</?\w[^>]*>\s*([^<]+)',
                    content, re.IGNORECASE
                )
            if case_match:
                details["case_number"] = case_match.group(1).strip()

            # Extract PDF URL - look for /sites/default/files/judgments/*.pdf
            pdf_match = re.search(
                r'href="([^"]*?/sites/default/files/judgments/[^"]+\.pdf)"',
                content, re.IGNORECASE
            )
            if pdf_match:
                pdf_path = pdf_match.group(1)
                if pdf_path.startswith('/'):
                    details["pdf_url"] = pdf_path
                elif pdf_path.startswith('http'):
                    details["pdf_url"] = pdf_path
                else:
                    details["pdf_url"] = '/' + pdf_path

            # Also try broader file pattern
            if not details["pdf_url"]:
                pdf_match = re.search(
                    r'href="([^"]*?\.pdf)"',
                    content, re.IGNORECASE
                )
                if pdf_match:
                    pdf_path = pdf_match.group(1)
                    if '/files/' in pdf_path or '/judgments/' in pdf_path:
                        if pdf_path.startswith('/'):
                            details["pdf_url"] = pdf_path
                        elif pdf_path.startswith('http'):
                            details["pdf_url"] = pdf_path
                        else:
                            details["pdf_url"] = '/' + pdf_path

            # Extract date of delivery - Drupal field
            date_match = re.search(
                r'Date\s+of\s+Delivery\s*</div>\s*<div[^>]*>\s*<span[^>]*>\s*([^<]+)',
                content, re.IGNORECASE
            )
            if not date_match:
                date_match = re.search(
                    r'Date\s+of\s+Delivery\s*:?\s*</?\w[^>]*>\s*(?:<[^>]*>)*\s*([^<]+)',
                    content, re.IGNORECASE
                )
            if not date_match:
                date_match = re.search(
                    r'Date\s+of\s+Delivery[^<]*?(\d{1,2}/\d{1,2}/\d{4})',
                    content, re.IGNORECASE
                )
            if date_match:
                details["date_delivered"] = date_match.group(1).strip()

            # Extract judge - Drupal field: "Presiding Judge" label then link with name
            judge_match = re.search(
                r'Presiding\s+Judge\s*</div>\s*<div[^>]*>\s*(?:<a[^>]*>)?\s*([^<]+)',
                content, re.IGNORECASE
            )
            if not judge_match:
                judge_match = re.search(
                    r'Presiding\s+Judge\s*:?\s*</?\w[^>]*>\s*(?:<[^>]*>)*\s*([^<]+)',
                    content, re.IGNORECASE
                )
            if judge_match:
                judge_val = judge_match.group(1).strip()
                # Sanity check: judge name should be short
                if len(judge_val) < 100 and not judge_val.startswith('http'):
                    details["judge"] = html.unescape(judge_val)

            # Extract year
            year_match = re.search(r'Year\s*:?\s*(\d{4})', content, re.IGNORECASE)
            if year_match:
                details["year"] = year_match.group(1)

            # Extract topics/keywords - often in quoted strings after field label
            topic_matches = re.findall(
                r'(?:Legal\s+Topics?|Keywords?|Subject)\s*</div>\s*<div[^>]*>\s*"([^"]+)"',
                content, re.IGNORECASE
            )
            if not topic_matches:
                topic_matches = re.findall(
                    r'"(CPR\s+rule[^"]+|[A-Z][^"]{5,80})"',
                    content
                )
            if topic_matches:
                details["topics"] = topic_matches

            # Extract title from page
            title_match = re.search(r'<h1[^>]*>([^<]+)</h1>', content, re.IGNORECASE)
            if title_match:
                details["title"] = html.unescape(title_match.group(1).strip())

            logger.info(f"Node {node_id}: citation={details['neutral_citation']}, pdf={'YES' if details['pdf_url'] else 'NO'}")
            return details

        except Exception as e:
            logger.warning(f"Failed to get node {node_id}: {e}")
            return details

    def _download_and_extract_pdf(self, pdf_url: str) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="JM/SupremeCourt",
            source_id="",
            pdf_url=pdf_url,
            table="case_law",
        ) or ""

    def _parse_date(self, date_str: str) -> str:
        """Parse date string to ISO 8601 format."""
        if not date_str:
            return ""

        # Remove time portion
        date_str = re.sub(r'\s*-\s*\d{1,2}:\d{2}.*', '', date_str).strip()
        # Remove ordinal suffixes
        date_str = re.sub(r'(\d)(st|nd|rd|th)', r'\1', date_str)

        formats = [
            "%m/%d/%Y",
            "%d/%m/%Y",
            "%Y-%m-%d",
            "%d %B %Y",
            "%d %b %Y",
            "%B %d, %Y",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                # Sanity check: year should be reasonable
                if 1960 <= dt.year <= 2030:
                    return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all judgment documents from the Jamaica Supreme Court.
        Paginates through listing pages, visits each node, downloads PDFs.
        """
        page = 0
        seen_nodes = set()
        empty_pages = 0

        while empty_pages < 2:
            judgments = self._get_listings_page(page)

            if not judgments:
                empty_pages += 1
                page += 1
                continue
            empty_pages = 0

            for j in judgments:
                node_id = j["node_id"]
                if node_id in seen_nodes:
                    continue
                seen_nodes.add(node_id)

                # Get detailed metadata from node page
                details = self._get_node_details(node_id)

                # Use listing data as fallback
                if not details["case_number"]:
                    details["case_number"] = j.get("case_number", "")
                if not details["title"]:
                    details["title"] = j.get("title", "")
                if not details["judge"]:
                    details["judge"] = j.get("judge", "")
                if not details["date_delivered"]:
                    details["date_delivered"] = j.get("date_raw", "")

                # Must have a PDF URL
                if not details["pdf_url"]:
                    logger.warning(f"No PDF URL for node {node_id}, skipping")
                    continue

                # Download and extract PDF text
                full_text = self._download_and_extract_pdf(details["pdf_url"])

                if not full_text:
                    logger.warning(f"No text extracted for node {node_id}, skipping")
                    continue

                yield {
                    "node_id": node_id,
                    "neutral_citation": details["neutral_citation"],
                    "case_number": details["case_number"],
                    "title": details["title"],
                    "judge": details["judge"],
                    "date_delivered": details["date_delivered"],
                    "year": details.get("year", ""),
                    "topics": details.get("topics", []),
                    "pdf_url": details["pdf_url"],
                    "full_text": full_text,
                }

            page += 1

            # Safety limit
            if page > 100:
                logger.warning("Reached page limit, stopping")
                break

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield judgments since the given date (uses fetch_all with date filter)."""
        for doc in self.fetch_all():
            date_str = self._parse_date(doc.get("date_delivered", ""))
            if date_str:
                try:
                    doc_date = datetime.strptime(date_str, "%Y-%m-%d")
                    doc_date = doc_date.replace(tzinfo=timezone.utc)
                    if doc_date < since:
                        continue
                except ValueError:
                    pass
            yield doc

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw judgment data into standard schema.
        CRITICAL: Includes full text in the 'text' field.
        """
        citation = raw.get("neutral_citation", "")
        case_number = raw.get("case_number", "")
        node_id = raw.get("node_id", "")

        # Build document ID
        if citation:
            doc_id = citation.replace(" ", "_").replace("[", "").replace("]", "")
        elif case_number:
            doc_id = case_number.replace(" ", "_")
        else:
            doc_id = f"node_{node_id}"

        # Parse date
        date_delivered = self._parse_date(raw.get("date_delivered", ""))

        # Build URL
        pdf_url = raw.get("pdf_url", "")
        if pdf_url.startswith('/'):
            full_url = f"{BASE_URL}{pdf_url}"
        elif pdf_url.startswith('http'):
            full_url = pdf_url
        else:
            full_url = f"{BASE_URL}/node/{node_id}"

        return {
            "_id": doc_id,
            "_source": "JM/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("full_text", ""),
            "date": date_delivered,
            "url": full_url,
            "case_number": case_number,
            "neutral_citation": citation,
            "court": "Supreme Court of Jamaica",
            "judge": raw.get("judge", ""),
            "year": raw.get("year", ""),
            "topics": raw.get("topics", []),
            "jurisdiction": "JM",
            "language": "en",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Jamaica Supreme Court endpoints...")
        print(f"pdfplumber available: {HAS_PDFPLUMBER}")

        print("\n1. Testing judgment listing page...")
        try:
            judgments = self._get_listings_page(0)
            print(f"   Found {len(judgments)} judgments on page 0")
            if judgments:
                j = judgments[0]
                print(f"   Sample: node={j['node_id']} - {j['title'][:60]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        if not judgments:
            print("   No judgments found. Cannot continue testing.")
            return

        print("\n2. Testing node detail page...")
        try:
            node_id = judgments[0]["node_id"]
            details = self._get_node_details(node_id)
            print(f"   Citation: {details['neutral_citation']}")
            print(f"   PDF URL: {details['pdf_url']}")
            print(f"   Judge: {details['judge']}")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\n3. Testing PDF download and extraction...")
        if details.get("pdf_url") and HAS_PDFPLUMBER:
            try:
                text = self._download_and_extract_pdf(details["pdf_url"])
                print(f"   Extracted {len(text)} characters")
                if text:
                    print(f"   Sample: {text[:200]}...")
            except Exception as e:
                print(f"   ERROR: {e}")
        else:
            print("   SKIPPED: No PDF URL or pdfplumber not available")

        print("\nTest complete!")


def main():
    scraper = JamaicaSupremeCourtScraper()

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
