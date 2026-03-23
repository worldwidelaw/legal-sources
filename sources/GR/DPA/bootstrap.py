#!/usr/bin/env python3
"""
GR/DPA -- Greek Data Protection Authority (HDPA) Data Fetcher

Fetches GDPR enforcement decisions, opinions, and guidelines from the
Hellenic Data Protection Authority (Αρχή Προστασίας Δεδομένων).

Strategy:
  - Scrape the decisions list at /el/enimerwtiko/prakseisArxis (paginated)
  - Extract decision metadata (number, date, category, title, PDF URL)
  - Download PDF files and extract full text using pypdf

Endpoints:
  - List: https://www.dpa.gr/el/enimerwtiko/prakseisArxis?page=N
  - PDFs: https://www.dpa.gr/sites/default/files/YYYY-MM/N_YYYY%20anonym.pdf

Data:
  - ~2260 decisions from 1997 to present
  - Language: Greek (EL)
  - Categories: Decisions (Αποφάσεις), Opinions (Γνωμοδοτήσεις), Guidelines
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
from urllib.parse import urljoin, urlparse, parse_qs

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

# PDF extraction
try:
    import pypdf
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GR.dpa")

# Base URL for Greek DPA
BASE_URL = "https://www.dpa.gr"
LIST_URL = "/el/enimerwtiko/prakseisArxis"


class GreekDPAScraper(BaseScraper):
    """
    Scraper for GR/DPA -- Greek Data Protection Authority.
    Country: GR
    URL: https://www.dpa.gr

    Data types: doctrine
    Auth: none (Open public access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,application/xml",
                "Accept-Language": "el,en",
            },
            timeout=60,
        )

    def _get_total_pages(self) -> int:
        """Get total number of pages from the list page."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(LIST_URL)
            resp.raise_for_status()
            content = resp.text

            # Look for "Βρέθηκαν X αποτελέσματα" (Found X results)
            match = re.search(r'Βρέθηκαν\s+(\d+)\s+αποτελέσματα', content)
            if match:
                total = int(match.group(1))
                # 10 items per page
                pages = (total + 9) // 10
                logger.info(f"Found {total} total decisions ({pages} pages)")
                return pages

            # Fallback: look for pagination links
            page_match = re.findall(r'page=(\d+)', content)
            if page_match:
                return max(int(p) for p in page_match) + 1

            return 1
        except Exception as e:
            logger.error(f"Failed to get total pages: {e}")
            return 1

    def _scrape_list_page(self, page: int) -> List[Dict[str, Any]]:
        """
        Scrape a single list page and extract decision entries.

        Returns list of dicts with: url, title, category, number, date
        """
        decisions = []

        try:
            url = f"{LIST_URL}?page={page}"
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()
            content = resp.text

            # The HTML structure is:
            # <tr>
            #   <td class="views-field-nothing-1">Απόφαση</td>
            #   <td class="views-field-field-arithmos-protokolloy">2</td>
            #   <td>...<time datetime="...">11/02/2026</time>...</td>
            #   <td>...<a href="/el/enimerwtiko/prakseisArxis/...">Title</a>...</td>
            # </tr>

            # Pattern to match table rows with decision data
            row_pattern = re.compile(
                r'<tr>\s*'
                r'<td[^>]*class="[^"]*views-field-nothing-1[^"]*"[^>]*>\s*'
                r'(Απόφαση|Γνωμοδότηση|Οδηγία|Σύσταση|Κατευθυντήρια γραμμή)\s*</td>\s*'  # Category
                r'<td[^>]*class="[^"]*views-field-field-arithmos-protokolloy[^"]*"[^>]*>\s*'
                r'(\d+)\s*</td>\s*'  # Number
                r'<td[^>]*>.*?<time[^>]*>(\d{2}/\d{2}/\d{4})</time>.*?</td>\s*'  # Date
                r'<td[^>]*>.*?<a\s+href="(/el/enimerwtiko/prakseisArxis/[^"]+)"[^>]*>([^<]+)</a>.*?</td>\s*'  # URL + Title
                r'</tr>',
                re.DOTALL | re.IGNORECASE
            )

            for match in row_pattern.finditer(content):
                category = html.unescape(match.group(1).strip())
                number = int(match.group(2))
                date_str = match.group(3)  # DD/MM/YYYY
                rel_url = match.group(4)
                title = html.unescape(match.group(5).strip())

                # Convert date to ISO format
                try:
                    day, month, year = date_str.split('/')
                    iso_date = f"{year}-{month}-{day}"
                except:
                    iso_date = ""
                    year = None

                decisions.append({
                    "url": rel_url,
                    "title": title,
                    "category": category,
                    "number": number,
                    "date": iso_date,
                    "year": int(year) if iso_date else None,
                })

            logger.info(f"Page {page}: found {len(decisions)} decisions")
            return decisions

        except Exception as e:
            logger.error(f"Failed to scrape page {page}: {e}")
            return []

    def _fetch_decision_page(self, rel_url: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a single decision page and extract metadata + PDF URL.

        Returns dict with: pdf_url, subject_area, provisions, summary, or None.
        """
        try:
            self.rate_limiter.wait()
            resp = self.client.get(rel_url)
            resp.raise_for_status()
            content = resp.text

            result = {}

            # Extract PDF link
            # Pattern can be either absolute URL or relative path:
            # http://www.dpa.gr/sites/default/files/2026-02/2_2026%20anonym_0.pdf
            # /sites/default/files/2026-02/2_2026%20anonym_0.pdf
            pdf_match = re.search(
                r'href="((?:https?://[^/]+)?/sites/default/files/[^"]+\.pdf)"',
                content,
                re.IGNORECASE
            )
            if pdf_match:
                pdf_url = pdf_match.group(1)
                # Convert to relative URL if it's absolute
                if pdf_url.startswith('http'):
                    parsed = urlparse(pdf_url)
                    pdf_url = parsed.path
                result["pdf_url"] = pdf_url

            # Extract subject area (Θεματική Ενότητα)
            subject_match = re.search(
                r'Θεματική\s+Ενότητα.*?<[^>]*>([^<]+)<',
                content,
                re.DOTALL | re.IGNORECASE
            )
            if subject_match:
                result["subject_area"] = html.unescape(subject_match.group(1).strip())

            # Extract applicable provisions
            provisions = []
            prov_pattern = re.compile(r'Άρθρο\s+(\d+)', re.IGNORECASE)
            for match in prov_pattern.finditer(content):
                provisions.append(f"Article {match.group(1)}")
            if provisions:
                result["provisions"] = list(set(provisions))

            # Extract summary from page body
            # Look for text after the title and before the PDF link
            body_match = re.search(
                r'<div[^>]*class="[^"]*content[^"]*"[^>]*>(.*?)</div>',
                content,
                re.DOTALL | re.IGNORECASE
            )
            if body_match:
                summary_html = body_match.group(1)
                # Clean HTML
                summary = re.sub(r'<[^>]+>', ' ', summary_html)
                summary = html.unescape(summary)
                summary = re.sub(r'\s+', ' ', summary).strip()
                if len(summary) > 100:
                    result["summary"] = summary[:2000]  # Cap summary length

            return result

        except Exception as e:
            logger.warning(f"Failed to fetch decision page {rel_url}: {e}")
            return None

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """
        Download and extract text from a PDF.

        Uses pypdf for text extraction with memory bounds.
        """
        if not HAS_PYPDF:
            logger.warning("pypdf not installed, skipping PDF extraction")
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
        Yield all documents from the Greek DPA.

        Iterates through all list pages, then fetches each decision.
        """
        total_pages = self._get_total_pages()
        documents_yielded = 0

        for page in range(total_pages):
            logger.info(f"Processing page {page + 1}/{total_pages}...")

            entries = self._scrape_list_page(page)

            for entry in entries:
                # Fetch decision page for metadata and PDF URL
                page_data = self._fetch_decision_page(entry["url"])

                if not page_data or not page_data.get("pdf_url"):
                    logger.warning(f"No PDF found for {entry['title'][:50]}")
                    continue

                # Extract text from PDF
                text = self._extract_pdf_text(page_data["pdf_url"])

                if not text:
                    logger.warning(f"No text extracted for {entry['title'][:50]}")
                    continue

                yield {
                    "url": entry["url"],
                    "title": entry["title"],
                    "category": entry["category"],
                    "number": entry["number"],
                    "date": entry["date"],
                    "year": entry["year"],
                    "pdf_url": page_data.get("pdf_url"),
                    "subject_area": page_data.get("subject_area", ""),
                    "provisions": page_data.get("provisions", []),
                    "summary": page_data.get("summary", ""),
                    "text": text,
                }

                documents_yielded += 1

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents from recent pages.

        Since the list is sorted by newest first, we fetch pages until
        we hit decisions older than 'since'.
        """
        since_str = since.strftime("%Y-%m-%d")
        total_pages = min(self._get_total_pages(), 20)  # Cap at 20 pages for updates

        for page in range(total_pages):
            entries = self._scrape_list_page(page)

            all_old = True
            for entry in entries:
                # Check if this entry is newer than 'since'
                if entry.get("date") and entry["date"] >= since_str:
                    all_old = False

                    page_data = self._fetch_decision_page(entry["url"])
                    if not page_data or not page_data.get("pdf_url"):
                        continue

                    text = self._extract_pdf_text(page_data["pdf_url"])
                    if not text:
                        continue

                    yield {
                        "url": entry["url"],
                        "title": entry["title"],
                        "category": entry["category"],
                        "number": entry["number"],
                        "date": entry["date"],
                        "year": entry["year"],
                        "pdf_url": page_data.get("pdf_url"),
                        "subject_area": page_data.get("subject_area", ""),
                        "provisions": page_data.get("provisions", []),
                        "summary": page_data.get("summary", ""),
                        "text": text,
                    }

            # If all entries on this page are older than 'since', stop
            if all_old:
                logger.info(f"Reached entries older than {since_str}, stopping")
                break

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        number = raw.get("number", 0)
        year = raw.get("year", 0)
        category = raw.get("category", "Απόφαση")

        # Map Greek category to English
        category_map = {
            "Απόφαση": "Decision",
            "Γνωμοδότηση": "Opinion",
            "Οδηγία": "Guideline",
            "Σύσταση": "Recommendation",
            "Κατευθυντήρια γραμμή": "Guideline",
        }
        category_en = category_map.get(category, "Decision")

        # Create unique document ID
        doc_id = f"HDPA/{number}/{year}"

        title = raw.get("title", "")
        text = raw.get("text", "")
        date_str = raw.get("date", "")
        rel_url = raw.get("url", "")
        pdf_url = raw.get("pdf_url", "")

        # Build full URLs
        page_url = urljoin(BASE_URL, rel_url) if rel_url else ""
        full_pdf_url = urljoin(BASE_URL, pdf_url) if pdf_url else ""

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "GR/DPA",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": text,  # MANDATORY FULL TEXT
            "date": date_str,
            "url": page_url,
            # Additional metadata
            "decision_number": number,
            "year": year,
            "category": category,
            "category_en": category_en,
            "subject_area": raw.get("subject_area", ""),
            "provisions": raw.get("provisions", []),
            "pdf_url": full_pdf_url,
            "language": "el",
            "authority": "Αρχή Προστασίας Δεδομένων Προσωπικού Χαρακτήρα",
            "authority_en": "Hellenic Data Protection Authority",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Greek DPA endpoints...")

        # Test list page
        print("\n1. Testing list page...")
        try:
            resp = self.client.get(LIST_URL)
            print(f"   Status: {resp.status_code}")
            print(f"   Page length: {len(resp.text)} chars")
            total_pages = self._get_total_pages()
            print(f"   Total pages: {total_pages}")
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
                print(f"   Sample: {entry['category']} {entry['number']} - {entry['title'][:40]}...")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        # Test decision page
        print("\n3. Testing decision page fetch...")
        try:
            if entries:
                entry = entries[0]
                page_data = self._fetch_decision_page(entry["url"])
                if page_data:
                    print(f"   PDF URL: {page_data.get('pdf_url', 'NOT FOUND')}")
                    print(f"   Subject area: {page_data.get('subject_area', 'N/A')}")
                else:
                    print("   ERROR: No page data returned")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        # Test PDF extraction
        print("\n4. Testing PDF extraction...")
        try:
            if page_data and page_data.get("pdf_url"):
                text = self._extract_pdf_text(page_data["pdf_url"])
                if text:
                    print(f"   Text length: {len(text)} chars")
                    print(f"   Sample: {text[:150]}...")
                else:
                    print("   ERROR: No text extracted")
            else:
                print("   SKIP: No PDF URL available")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = GreekDPAScraper()

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
