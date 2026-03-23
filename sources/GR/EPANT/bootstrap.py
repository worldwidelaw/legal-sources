#!/usr/bin/env python3
"""
GR/EPANT -- Hellenic Competition Commission (EPANT) Data Fetcher

Fetches competition law decisions from the Hellenic Competition Commission
(Επιτροπή Ανταγωνισμού / EPANT).

Strategy:
  - Scrape the decisions list at /en/decisions.html (paginated with ?start=N)
  - Extract decision metadata from list pages
  - For each decision, fetch the detail page and extract full text from JSON-LD
  - Also available: PDF documents with full Greek text

Endpoints:
  - List: https://www.epant.gr/en/decisions.html?start=N
  - Detail: https://www.epant.gr/en/decisions/item/XXXX-decision-NNN-YYYY.html
  - PDFs: https://www.epant.gr/files/YYYY/apofaseis/NNN_YYYY.pdf

Data:
  - ~400+ decisions from 1990s to present
  - 23 items per page
  - Language: English summaries available, Greek full text in PDFs
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
import html
import io
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

# PDF extraction (optional, for Greek full text)
try:
    import pypdf
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GR.epant")

# Base URL for EPANT
BASE_URL = "https://www.epant.gr"
LIST_URL = "/en/decisions.html"
ITEMS_PER_PAGE = 23


class GreekCompetitionScraper(BaseScraper):
    """
    Scraper for GR/EPANT -- Hellenic Competition Commission.
    Country: GR
    URL: https://www.epant.gr

    Data types: case_law
    Auth: none (Open public access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,application/xml",
                "Accept-Language": "en,el",
            },
            timeout=60,
        )

    def _get_max_start(self) -> int:
        """Get the maximum start parameter from pagination."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(LIST_URL)
            resp.raise_for_status()
            content = resp.text

            # Find all start= parameters in pagination links
            matches = re.findall(r'start=(\d+)', content)
            if matches:
                max_start = max(int(m) for m in matches)
                logger.info(f"Found max pagination start={max_start}")
                return max_start
            return 0
        except Exception as e:
            logger.error(f"Failed to get pagination: {e}")
            return 0

    def _scrape_list_page(self, start: int) -> List[Dict[str, Any]]:
        """
        Scrape a single list page and extract decision entries.

        Returns list of dicts with: url, title, decision_type, number, year
        """
        decisions = []

        try:
            url = f"{LIST_URL}?start={start}"
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()
            content = resp.text

            # Extract decision URLs and basic info
            # Pattern: /en/decisions/item/3441-decision-893-2025.html
            # Also handles: /en/decisions/item/3439-act-5-2025.html
            pattern = re.compile(
                r'href="(/en/decisions/item/(\d+)-(decision|act)-(\d+)-(\d{4})\.html)"',
                re.IGNORECASE
            )

            seen = set()
            for match in pattern.finditer(content):
                rel_url = match.group(1)
                item_id = match.group(2)
                decision_type = match.group(3).capitalize()
                number = int(match.group(4))
                year = int(match.group(5))

                # Avoid duplicates on same page
                key = f"{number}/{year}"
                if key in seen:
                    continue
                seen.add(key)

                decisions.append({
                    "url": rel_url,
                    "item_id": item_id,
                    "decision_type": decision_type,
                    "number": number,
                    "year": year,
                })

            logger.info(f"Page start={start}: found {len(decisions)} decisions")
            return decisions

        except Exception as e:
            logger.error(f"Failed to scrape page start={start}: {e}")
            return []

    def _fetch_decision_page(self, rel_url: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a single decision page and extract metadata + full text.

        Returns dict with: title, text, date, relevant_market, companies, pdf_url, etc.
        """
        try:
            self.rate_limiter.wait()
            resp = self.client.get(rel_url)
            resp.raise_for_status()
            content = resp.text

            result = {}

            # Extract JSON-LD data which contains articleBody (full text summary)
            json_ld_match = re.search(
                r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
                content,
                re.DOTALL | re.IGNORECASE
            )
            if json_ld_match:
                try:
                    json_data = json.loads(json_ld_match.group(1))
                    if isinstance(json_data, dict):
                        result["title"] = json_data.get("name", "")
                        result["text"] = json_data.get("articleBody", "")
                        result["date_published"] = json_data.get("datePublished", "")
                except json.JSONDecodeError:
                    pass

            # Extract title from page if not in JSON-LD
            if not result.get("title"):
                title_match = re.search(
                    r'<h2[^>]*class="itemTitle"[^>]*>([^<]+)</h2>',
                    content,
                    re.IGNORECASE
                )
                if title_match:
                    result["title"] = html.unescape(title_match.group(1).strip())

            # Extract PDF link
            pdf_match = re.search(
                r'href="(/files/\d{4}/apofaseis/[^"]+\.pdf)"',
                content,
                re.IGNORECASE
            )
            if pdf_match:
                result["pdf_url"] = pdf_match.group(1)

            # Extract FEK (Government Gazette) link
            fek_match = re.search(
                r'href="(/files/\d{4}/apofaseis/[^"]+_FEK\.pdf)"',
                content,
                re.IGNORECASE
            )
            if fek_match:
                result["fek_pdf_url"] = fek_match.group(1)

            # Extract date from the articleBody text
            # Pattern: "Date of Issuance of Decision Month DDth, YYYY"
            article_body = result.get("text", "")
            date_match = re.search(
                r'Date\s+of\s+Issuance\s+of\s+Decision\s+'
                r'(January|February|March|April|May|June|July|August|September|October|November|December)'
                r'\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})',
                article_body,
                re.IGNORECASE
            )
            if date_match:
                month_name = date_match.group(1)
                day = date_match.group(2)
                year = date_match.group(3)
                months = {
                    'january': '01', 'february': '02', 'march': '03', 'april': '04',
                    'may': '05', 'june': '06', 'july': '07', 'august': '08',
                    'september': '09', 'october': '10', 'november': '11', 'december': '12'
                }
                month_num = months.get(month_name.lower(), '01')
                result["date"] = f"{year}-{month_num}-{day.zfill(2)}"

            # Extract relevant market
            market_match = re.search(
                r'Relevant\s+Market</td>\s*<td[^>]*>([^<]+)</td>',
                content,
                re.IGNORECASE | re.DOTALL
            )
            if market_match:
                result["relevant_market"] = html.unescape(market_match.group(1).strip())

            # Extract companies concerned
            company_match = re.search(
                r'Company\(?i?e?s?\)?\s+concerned</td>\s*<td[^>]*>([^<]+)</td>',
                content,
                re.IGNORECASE | re.DOTALL
            )
            if company_match:
                result["companies"] = html.unescape(company_match.group(1).strip())

            # Extract legal framework
            legal_match = re.search(
                r'Legal\s+Framework</td>\s*<td[^>]*>([^<]+)</td>',
                content,
                re.IGNORECASE | re.DOTALL
            )
            if legal_match:
                result["legal_framework"] = html.unescape(legal_match.group(1).strip())

            # Extract Government Gazette reference
            gazette_match = re.search(
                r'Government\s+Gazette</td>\s*<td[^>]*>([^<]+)</td>',
                content,
                re.IGNORECASE | re.DOTALL
            )
            if gazette_match:
                result["gazette_reference"] = html.unescape(gazette_match.group(1).strip())

            return result

        except Exception as e:
            logger.warning(f"Failed to fetch decision page {rel_url}: {e}")
            return None

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse various date formats to ISO format."""
        if not date_str:
            return None

        # Try common formats
        formats = [
            "%B %d, %Y",      # November 4th, 2025
            "%B %dth, %Y",
            "%B %dst, %Y",
            "%B %dnd, %Y",
            "%B %drd, %Y",
            "%d/%m/%Y",       # 04/11/2025
            "%Y-%m-%d",       # 2025-11-04
        ]

        # Clean ordinal suffixes
        clean_date = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', date_str)

        for fmt in formats:
            try:
                dt = datetime.strptime(clean_date.strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        return None

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """
        Download and extract text from a PDF.

        Uses pypdf for text extraction with memory bounds.
        """
        if not HAS_PYPDF:
            logger.debug("pypdf not installed, skipping PDF extraction")
            return None

        try:
            full_url = urljoin(BASE_URL, pdf_url)
            self.rate_limiter.wait()
            resp = self.client.get(pdf_url)
            resp.raise_for_status()

            # Memory bound: skip very large PDFs (>20MB)
            content_length = len(resp.content)
            if content_length > 20 * 1024 * 1024:
                logger.warning(f"PDF too large ({content_length} bytes): {pdf_url}")
                return None

            # Extract text using pypdf
            pdf_file = io.BytesIO(resp.content)
            reader = pypdf.PdfReader(pdf_file)

            text_parts = []
            for page_num, page in enumerate(reader.pages):
                try:
                    page_text = page.extract_text() or ""
                    text_parts.append(page_text)
                except Exception as e:
                    logger.debug(f"Error extracting page {page_num}: {e}")
                    continue

            text = "\n\n".join(text_parts)

            # Clean up text
            text = re.sub(r'\s+', ' ', text)
            text = text.strip()

            if len(text) < 100:
                logger.warning(f"Very short PDF text ({len(text)} chars): {pdf_url}")
                return None

            return text

        except Exception as e:
            logger.warning(f"Failed to extract PDF text from {pdf_url}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from the Hellenic Competition Commission.

        Iterates through all list pages, then fetches each decision.
        """
        max_start = self._get_max_start()
        documents_yielded = 0

        start = 0
        while start <= max_start:
            logger.info(f"Processing page start={start} (max={max_start})...")

            entries = self._scrape_list_page(start)

            for entry in entries:
                # Fetch decision page for metadata and full text
                page_data = self._fetch_decision_page(entry["url"])

                if not page_data:
                    logger.warning(f"No page data for decision {entry['number']}/{entry['year']}")
                    continue

                # Get text: prefer articleBody from JSON-LD, fallback to PDF
                text = page_data.get("text", "")

                # If no text from HTML, try PDF extraction
                if not text and page_data.get("pdf_url"):
                    text = self._extract_pdf_text(page_data["pdf_url"]) or ""

                if not text:
                    logger.warning(f"No text for decision {entry['number']}/{entry['year']}")
                    continue

                yield {
                    "url": entry["url"],
                    "item_id": entry["item_id"],
                    "decision_type": entry["decision_type"],
                    "number": entry["number"],
                    "year": entry["year"],
                    "title": page_data.get("title", ""),
                    "text": text,
                    "date": page_data.get("date", f"{entry['year']}-01-01"),
                    "relevant_market": page_data.get("relevant_market", ""),
                    "companies": page_data.get("companies", ""),
                    "legal_framework": page_data.get("legal_framework", ""),
                    "gazette_reference": page_data.get("gazette_reference", ""),
                    "pdf_url": page_data.get("pdf_url", ""),
                }

                documents_yielded += 1

            start += ITEMS_PER_PAGE

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents from recent pages.

        Since the list is sorted by newest first, we fetch pages until
        we hit decisions older than 'since'.
        """
        since_str = since.strftime("%Y-%m-%d")
        since_year = since.year

        start = 0
        max_pages = 10  # Cap at ~230 decisions for updates

        for _ in range(max_pages):
            entries = self._scrape_list_page(start)

            if not entries:
                break

            all_old = True
            for entry in entries:
                # Check if this entry is newer than 'since'
                if entry["year"] >= since_year:
                    all_old = False

                    page_data = self._fetch_decision_page(entry["url"])
                    if not page_data:
                        continue

                    text = page_data.get("text", "")
                    if not text and page_data.get("pdf_url"):
                        text = self._extract_pdf_text(page_data["pdf_url"]) or ""

                    if not text:
                        continue

                    entry_date = page_data.get("date", f"{entry['year']}-01-01")
                    if entry_date >= since_str:
                        yield {
                            "url": entry["url"],
                            "item_id": entry["item_id"],
                            "decision_type": entry["decision_type"],
                            "number": entry["number"],
                            "year": entry["year"],
                            "title": page_data.get("title", ""),
                            "text": text,
                            "date": entry_date,
                            "relevant_market": page_data.get("relevant_market", ""),
                            "companies": page_data.get("companies", ""),
                            "legal_framework": page_data.get("legal_framework", ""),
                            "gazette_reference": page_data.get("gazette_reference", ""),
                            "pdf_url": page_data.get("pdf_url", ""),
                        }

            # If all entries on this page are older than 'since', stop
            if all_old:
                logger.info(f"Reached entries older than {since_str}, stopping")
                break

            start += ITEMS_PER_PAGE

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        number = raw.get("number", 0)
        year = raw.get("year", 0)
        decision_type = raw.get("decision_type", "Decision")

        # Create unique document ID
        doc_id = f"EPANT/{decision_type}/{number}/{year}"

        title = raw.get("title", f"{decision_type} {number}/{year}")
        text = raw.get("text", "")
        date_str = raw.get("date", f"{year}-01-01")
        rel_url = raw.get("url", "")
        pdf_url = raw.get("pdf_url", "")

        # Build full URLs
        page_url = urljoin(BASE_URL, rel_url) if rel_url else ""
        full_pdf_url = urljoin(BASE_URL, pdf_url) if pdf_url else ""

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "GR/EPANT",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": text,  # MANDATORY FULL TEXT
            "date": date_str,
            "url": page_url,
            # Additional metadata
            "decision_number": number,
            "year": year,
            "decision_type": decision_type,
            "relevant_market": raw.get("relevant_market", ""),
            "companies": raw.get("companies", ""),
            "legal_framework": raw.get("legal_framework", ""),
            "gazette_reference": raw.get("gazette_reference", ""),
            "pdf_url": full_pdf_url,
            "language": "en",  # English summaries
            "authority": "Επιτροπή Ανταγωνισμού",
            "authority_en": "Hellenic Competition Commission",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Hellenic Competition Commission endpoints...")

        # Test list page
        print("\n1. Testing list page...")
        try:
            resp = self.client.get(LIST_URL)
            print(f"   Status: {resp.status_code}")
            print(f"   Page length: {len(resp.text)} chars")
            max_start = self._get_max_start()
            print(f"   Max pagination start: {max_start}")
            total_approx = max_start + ITEMS_PER_PAGE
            print(f"   Approx total decisions: {total_approx}")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        # Test page scraping
        print("\n2. Testing page scraping...")
        try:
            entries = self._scrape_list_page(0)
            print(f"   Found {len(entries)} entries on first page")
            if entries:
                entry = entries[0]
                print(f"   Sample: {entry['decision_type']} {entry['number']}/{entry['year']}")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        # Test decision page
        print("\n3. Testing decision page fetch...")
        entry = None
        page_data = None
        try:
            if entries:
                entry = entries[0]
                page_data = self._fetch_decision_page(entry["url"])
                if page_data:
                    print(f"   Title: {page_data.get('title', 'N/A')[:60]}...")
                    print(f"   Text length: {len(page_data.get('text', ''))} chars")
                    print(f"   PDF URL: {page_data.get('pdf_url', 'NOT FOUND')}")
                    print(f"   Date: {page_data.get('date', 'N/A')}")
                else:
                    print("   ERROR: No page data returned")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        # Test PDF extraction (optional)
        if HAS_PYPDF:
            print("\n4. Testing PDF extraction (optional)...")
            try:
                if page_data and page_data.get("pdf_url"):
                    text = self._extract_pdf_text(page_data["pdf_url"])
                    if text:
                        print(f"   PDF text length: {len(text)} chars")
                        print(f"   Sample: {text[:150]}...")
                    else:
                        print("   No text extracted from PDF")
                else:
                    print("   SKIP: No PDF URL available")
            except Exception as e:
                print(f"   ERROR: {e}")
        else:
            print("\n4. PDF extraction: SKIP (pypdf not installed)")

        print("\nTest complete!")


def main():
    scraper = GreekCompetitionScraper()

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
