#!/usr/bin/env python3
"""
German Federal Cartel Office (Bundeskartellamt) Decision Fetcher

Extracts competition law decisions from bundeskartellamt.de:
- Kartellverbot (cartel prohibition)
- Missbrauchsaufsicht (abuse of dominance)
- Fusionskontrolle (merger control)

Coverage: 2000-present
PDF-based full text extraction using pypdf.

Data source: https://www.bundeskartellamt.de
License: Official German government publication (public domain under § 5 UrhG)
"""

import argparse
import io
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, List, Optional, Dict, Any

import requests
from bs4 import BeautifulSoup

# Try to import pypdf for PDF text extraction
try:
    from pypdf import PdfReader
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False
    print("Warning: pypdf not installed. Install with: pip install pypdf")

SOURCE_ID = "DE/Bundeskartellamt"
BASE_URL = "https://www.bundeskartellamt.de"

# Categories of decisions
CATEGORIES = [
    "Kartellverbot",        # Cartel prohibition
    "Missbrauchsaufsicht",  # Abuse of dominance
    "Fusionskontrolle",     # Merger control
]

# Years to scan (Bundeskartellamt has decisions from early 2000s)
YEARS = list(range(2000, datetime.now().year + 1))

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter; EU Legal Research)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/pdf,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
}


def extract_pdf_text(pdf_content: bytes) -> str:
    """Extract text from PDF content using pypdf."""
    if not HAS_PYPDF:
        return ""

    text_parts = []

    try:
        reader = PdfReader(io.BytesIO(pdf_content))
        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                # Clean up the text
                page_text = page_text.strip()
                text_parts.append(page_text)
    except Exception as e:
        print(f"Error extracting PDF text: {e}")
        return ""

    full_text = "\n\n".join(text_parts)

    # Clean up common PDF extraction artifacts
    full_text = re.sub(r'\n{3,}', '\n\n', full_text)
    full_text = re.sub(r' {2,}', ' ', full_text)
    full_text = re.sub(r'-\n', '', full_text)  # Join hyphenated words

    return full_text


class BundeskartellamtFetcher:
    """Fetcher for Bundeskartellamt competition decisions."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _build_pdf_url(self, category: str, year: int, case_number: str) -> str:
        """Build PDF URL for a decision.

        Pattern: /SharedDocs/Entscheidung/DE/Entscheidungen/{category}/{year}/{case_number}.pdf?__blob=publicationFile
        """
        return (
            f"{BASE_URL}/SharedDocs/Entscheidung/DE/Entscheidungen/"
            f"{category}/{year}/{case_number}.pdf?__blob=publicationFile"
        )

    def _build_html_url(self, category: str, year: int, case_number: str) -> str:
        """Build HTML page URL for a decision."""
        return (
            f"{BASE_URL}/SharedDocs/Entscheidung/DE/Entscheidungen/"
            f"{category}/{year}/{case_number}.html"
        )

    def _scan_category_year(self, category: str, year: int) -> List[Dict[str, Any]]:
        """Scan for decisions in a specific category and year.

        Uses a search-based approach via web scraping the decision database.
        """
        decisions = []

        # The search URL pattern
        search_url = (
            f"{BASE_URL}/SiteGlobals/Forms/Suche/Entscheidungsdatenbanksuche_Formular.html"
        )

        params = {
            "resultsPerPage": 100,
            "sortOrder": "dateOfIssue_dt desc",
        }

        # We'll scan known case number patterns instead
        # Bundeskartellamt uses pattern like B1-123/20, B2-45/21, etc.
        # Decision divisions are B1-B12 + special divisions

        return decisions

    def _discover_decisions_via_search(self, limit: int = 500) -> List[Dict[str, Any]]:
        """Discover decisions by searching known case patterns.

        Bundeskartellamt case numbers follow pattern: B{division}-{number}/{year}
        where division is 1-12 and year is 2-digit.
        """
        decisions = []
        checked_count = 0

        divisions = [f"B{i}" for i in range(1, 13)]  # B1 to B12

        for category in CATEGORIES:
            for year in sorted(YEARS, reverse=True):  # Start with recent years
                year_suffix = str(year)[-2:]

                for div in divisions:
                    # Try common case number ranges
                    for case_num in range(1, 200):
                        case_number = f"{div}-{case_num}-{year_suffix}"

                        # Try to access PDF directly
                        pdf_url = self._build_pdf_url(category, year, case_number)

                        try:
                            response = self.session.head(pdf_url, timeout=10, allow_redirects=True)

                            if response.status_code == 200:
                                decisions.append({
                                    "category": category,
                                    "year": year,
                                    "case_number": case_number,
                                    "pdf_url": pdf_url,
                                    "html_url": self._build_html_url(category, year, case_number)
                                })
                                print(f"Found: {category}/{year}/{case_number}")

                                if len(decisions) >= limit:
                                    return decisions
                        except Exception:
                            pass

                        checked_count += 1

                        # Rate limiting
                        if checked_count % 10 == 0:
                            time.sleep(0.5)

                        if checked_count % 100 == 0:
                            print(f"Checked {checked_count} URLs, found {len(decisions)} decisions...")

        return decisions

    def _search_decisions_google(self) -> List[Dict[str, Any]]:
        """Get list of known decisions from existing web index.

        Uses known patterns based on web search results.
        """
        # Known decisions from web search results - we'll use these as seeds
        known_decisions = [
            # From search results
            ("Missbrauchsaufsicht", 2024, "B6-26-23"),
            ("Fusionskontrolle", 2010, "B4-45-10"),
            ("Missbrauchsaufsicht", 2022, "B6-27-21"),
            ("Missbrauchsaufsicht", 2021, "B7-61-21"),
            ("Kartellverbot", 2015, "B9-121-13"),
            ("Kartellverbot", 2020, "B6-28-19"),
            ("Missbrauchsaufsicht", 2023, "B9-144-19"),
            ("Fusionskontrolle", 2008, "B6-52-08"),
            ("Fusionskontrolle", 2021, "B6-37-21"),
            ("Kartellverbot", 2015, "B2-98-11"),
            ("Kartellverbot", 2009, "B3-123-08"),
            ("Kartellverbot", 2001, "B9-194-00"),
            ("Kartellverbot", 2009, "B2-90-01-1"),
        ]

        decisions = []
        for category, year, case_number in known_decisions:
            decisions.append({
                "category": category,
                "year": year,
                "case_number": case_number,
                "pdf_url": self._build_pdf_url(category, year, case_number),
                "html_url": self._build_html_url(category, year, case_number)
            })

        return decisions

    def _scrape_decision_list(self, category: str, year: int) -> List[Dict[str, Any]]:
        """Scrape list of decisions by trying common case number patterns."""
        decisions = []
        divisions = [f"B{i}" for i in range(1, 13)]

        year_suffix = str(year)[-2:]

        for div in divisions:
            # Try case numbers 1-50 for each division
            for case_num in range(1, 51):
                case_number = f"{div}-{case_num}-{year_suffix}"
                pdf_url = self._build_pdf_url(category, year, case_number)

                try:
                    response = self.session.head(pdf_url, timeout=10, allow_redirects=True)

                    if response.status_code == 200:
                        content_type = response.headers.get("Content-Type", "")
                        if "pdf" in content_type.lower() or "application/octet" in content_type.lower():
                            decisions.append({
                                "category": category,
                                "year": year,
                                "case_number": case_number,
                                "pdf_url": pdf_url,
                                "html_url": self._build_html_url(category, year, case_number)
                            })
                            print(f"  Found: {case_number}")
                except Exception as e:
                    continue

                time.sleep(0.2)  # Rate limiting

        return decisions

    def _fetch_html_metadata(self, html_url: str) -> Optional[Dict[str, Any]]:
        """Fetch metadata from the HTML decision page."""
        try:
            response = self.session.get(html_url, timeout=30)
            if response.status_code != 200:
                return None

            soup = BeautifulSoup(response.content, 'html.parser')

            metadata = {}

            # Extract title
            title_elem = soup.find('h1')
            if title_elem:
                metadata['title'] = title_elem.get_text(strip=True)

            # Extract date
            date_dl = soup.find('dt', string=re.compile(r'Datum der Entscheidung|Entscheidungsdatum', re.I))
            if date_dl:
                date_dd = date_dl.find_next_sibling('dd')
                if date_dd:
                    date_text = date_dd.get_text(strip=True)
                    # Parse German date format: DD.MM.YYYY
                    date_match = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', date_text)
                    if date_match:
                        metadata['decision_date'] = f"{date_match.group(3)}-{date_match.group(2)}-{date_match.group(1)}"

            # Extract decision type
            type_dl = soup.find('dt', string=re.compile(r'Entscheidungsart|Art der Entscheidung', re.I))
            if type_dl:
                type_dd = type_dl.find_next_sibling('dd')
                if type_dd:
                    metadata['decision_type'] = type_dd.get_text(strip=True)

            # Extract product market
            market_dl = soup.find('dt', string=re.compile(r'Sachmarkt|Produktmarkt', re.I))
            if market_dl:
                market_dd = market_dl.find_next_sibling('dd')
                if market_dd:
                    metadata['product_market'] = market_dd.get_text(strip=True)

            return metadata

        except Exception as e:
            print(f"Error fetching HTML metadata from {html_url}: {e}")
            return None

    def _fetch_pdf_content(self, pdf_url: str) -> Optional[bytes]:
        """Fetch PDF content."""
        try:
            response = self.session.get(pdf_url, timeout=120)
            if response.status_code == 200:
                # Verify it's a PDF
                if response.content[:4] == b'%PDF':
                    return response.content
            return None
        except Exception as e:
            print(f"Error fetching PDF from {pdf_url}: {e}")
            return None

    def fetch_decision(self, decision_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch a single decision with full text."""
        pdf_url = decision_info['pdf_url']
        html_url = decision_info['html_url']
        case_number = decision_info['case_number']
        category = decision_info['category']
        year = decision_info['year']

        # Fetch PDF content
        pdf_content = self._fetch_pdf_content(pdf_url)
        if not pdf_content:
            return None

        # Extract text
        full_text = extract_pdf_text(pdf_content)
        if not full_text or len(full_text.strip()) < 500:
            print(f"  Insufficient text extracted from {case_number}: {len(full_text)} chars")
            return None

        # Try to get metadata from HTML page
        metadata = self._fetch_html_metadata(html_url) or {}

        # Build document
        doc_id = f"DE_BKartA_{category}_{year}_{case_number}".replace("-", "_").replace("/", "_")

        title = metadata.get('title') or f"Bundeskartellamt Decision {case_number}"
        decision_date = metadata.get('decision_date') or f"{year}-01-01"

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "case_number": case_number,
            "title": title,
            "text": full_text,
            "date": decision_date,
            "decision_date": decision_date,
            "decision_type": metadata.get('decision_type', ''),
            "product_market": metadata.get('product_market', ''),
            "procedure_type": category,
            "year": year,
            "url": pdf_url,
            "html_url": html_url,
            "authority": "Bundeskartellamt",
            "country": "DE",
            "language": "de",
            "text_length": len(full_text),
        }

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """Fetch all Bundeskartellamt decisions."""
        count = 0

        # Start with known decisions
        print("Loading known decisions from web search...")
        known = self._search_decisions_google()
        print(f"Found {len(known)} known decisions to process")

        for decision_info in known:
            if limit and count >= limit:
                break

            print(f"Fetching {decision_info['case_number']}...")
            doc = self.fetch_decision(decision_info)

            if doc:
                yield doc
                count += 1
                print(f"  [{count}] Success: {doc['title'][:60]}... ({doc['text_length']:,} chars)")
            else:
                print(f"  Failed to fetch {decision_info['case_number']}")

            time.sleep(1.5)  # Rate limiting

        # Then scan for more decisions year by year
        if not limit or count < limit:
            print("\nScanning for additional decisions...")

            for category in CATEGORIES:
                for year in sorted(YEARS, reverse=True)[:5]:  # Last 5 years
                    if limit and count >= limit:
                        break

                    print(f"Scanning {category}/{year}...")
                    found = self._scrape_decision_list(category, year)

                    for decision_info in found:
                        if limit and count >= limit:
                            break

                        # Skip if already processed
                        doc_id = f"DE_BKartA_{category}_{year}_{decision_info['case_number']}".replace("-", "_")

                        print(f"  Fetching {decision_info['case_number']}...")
                        doc = self.fetch_decision(decision_info)

                        if doc:
                            yield doc
                            count += 1
                            print(f"  [{count}] Success: {len(doc['text']):,} chars")

                        time.sleep(1.5)

        print(f"\nTotal decisions fetched: {count}")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch recent decisions (last 2 years)."""
        current_year = datetime.now().year
        count = 0

        for category in CATEGORIES:
            for year in [current_year, current_year - 1]:
                print(f"Scanning {category}/{year} for updates...")
                found = self._scrape_decision_list(category, year)

                for decision_info in found:
                    doc = self.fetch_decision(decision_info)
                    if doc:
                        yield doc
                        count += 1

                    time.sleep(1.5)

        print(f"Total updates fetched: {count}")

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document (already normalized in fetch)."""
        return raw_doc


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Bundeskartellamt Decision Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (15 docs)")
    parser.add_argument("--limit", type=int, default=100, help="Maximum documents to fetch")

    args = parser.parse_args()

    fetcher = BundeskartellamtFetcher()
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command == "bootstrap":
        target_count = 15 if args.sample else args.limit

        print(f"Starting bootstrap (target: {target_count} documents)...")
        print("=" * 60)

        saved_count = 0

        for doc in fetcher.fetch_all(limit=target_count + 10):
            if saved_count >= target_count:
                break

            # Validate document
            text_len = len(doc.get('text', ''))
            if text_len < 500:
                print(f"Skipping {doc['_id']} - text too short ({text_len} chars)")
                continue

            # Save to sample directory
            filename = f"{doc['_id']}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(doc, f, indent=2, ensure_ascii=False)

            saved_count += 1
            print(f"Saved [{saved_count}/{target_count}]: {doc['case_number']} ({text_len:,} chars)")

        # Print summary
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)

        files = list(sample_dir.glob("*.json"))
        total_chars = 0
        for f in files:
            with open(f, 'r', encoding='utf-8') as fp:
                data = json.load(fp)
                total_chars += len(data.get('text', ''))

        print(f"Sample files: {len(files)}")
        print(f"Total text chars: {total_chars:,}")
        print(f"Average chars/doc: {total_chars // max(len(files), 1):,}")

        # Validate all have text
        missing_text = 0
        for f in files:
            with open(f, 'r', encoding='utf-8') as fp:
                data = json.load(fp)
                if len(data.get('text', '')) < 500:
                    missing_text += 1

        if missing_text > 0:
            print(f"WARNING: {missing_text} files have insufficient text!")
        else:
            print("All files have full text content.")

    elif args.command == "test":
        print("Testing fetcher with 3 documents...")

        count = 0
        for doc in fetcher.fetch_all(limit=3):
            count += 1
            print(f"\n--- Document {count} ---")
            print(f"ID: {doc['_id']}")
            print(f"Case: {doc['case_number']}")
            print(f"Title: {doc['title'][:80]}...")
            print(f"Date: {doc['date']}")
            print(f"Type: {doc['procedure_type']}")
            print(f"Text length: {len(doc['text']):,} chars")
            print(f"Text preview: {doc['text'][:300]}...")


if __name__ == "__main__":
    main()
