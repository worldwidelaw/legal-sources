#!/usr/bin/env python3
"""
PT/STA -- Portuguese Supreme Administrative Court Case Law Fetcher

Fetches Supreme Administrative Court (Supremo Tribunal Administrativo - STA) decisions
from the DGSI jurisprudence database at dgsi.pt/jsta.nsf.

Strategy:
  - Database listing: /jsta.nsf?OpenDatabase&Start=N gives paginated list of decisions
  - Document detail: /jsta.nsf/{view_id}/{doc_id}?OpenDocument&ExpandSection=1 gives full text
  - The view_id is constant: 35fbbbf22e1bb1e680256f8e003ea931
  - ~121 records per page, ~89,300 total decisions

Data:
  - Case types: Administrative contentious, Tax/customs contentious
  - Coverage: Administrative since 1950, Tax since 1963, full text from 2002
  - Sections: Secção do Contencioso Administrativo, Secção do Contencioso Tributário
  - License: Public (open government data)
  - Full text: HTML content with legal arguments, facts, and decision

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
  python bootstrap.py update             # Incremental update (recent decisions)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.parse import urljoin, quote

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PT.STA")

# Base URL for the DGSI STA database
BASE_URL = "http://www.dgsi.pt"

# The STA database path
JSTA_DB = "/jsta.nsf"

# Constant view ID for document links
VIEW_ID = "35fbbbf22e1bb1e680256f8e003ea931"

# Number of records per page in the listing
PAGE_SIZE = 121


class STAScraper(BaseScraper):
    """
    Scraper for PT/STA -- Portuguese Supreme Administrative Court.
    Country: PT
    URL: https://www.dgsi.pt/jsta.nsf

    Data types: case_law
    Auth: none (Public government data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
                "Accept-Charset": "utf-8,ISO-8859-1;q=0.7,*;q=0.3",
            },
            timeout=60,
        )

    def _fetch_listing_page(self, start: int = 1) -> str:
        """
        Fetch a listing page from the database.

        Args:
            start: The pagination offset (1-based)

        Returns:
            HTML content of the listing page
        """
        url = f"{JSTA_DB}?OpenDatabase&Start={start}"

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)

            if resp.status_code != 200:
                logger.warning(f"Listing fetch failed for start={start}: HTTP {resp.status_code}")
                return ""

            # Try to decode with ISO-8859-1 first (Portuguese encoding), fall back to utf-8
            try:
                return resp.content.decode("iso-8859-1")
            except:
                return resp.content.decode("utf-8", errors="replace")

        except Exception as e:
            logger.warning(f"Error fetching listing page start={start}: {e}")
            return ""

    def _parse_listing_page(self, html_content: str) -> List[Dict[str, str]]:
        """
        Parse a listing page to extract document links and basic metadata.

        Returns list of dicts with: doc_id, session_date, case_number, rapporteur, descriptors
        """
        results = []

        # Simple pattern to extract document links
        # Format: <a href="/jsta.nsf/{view_id}/{doc_id}?OpenDocument">case_number</a>
        link_pattern = re.compile(
            r'<a href="/jsta\.nsf/' + VIEW_ID + r'/([a-f0-9]+)\?OpenDocument">([^<]+)</a>',
            re.IGNORECASE
        )

        # Split content into table rows
        rows = re.split(r'<tr[^>]*>', html_content, flags=re.IGNORECASE)

        for row in rows:
            # Skip rows without document links
            link_match = link_pattern.search(row)
            if not link_match:
                continue

            doc_id = link_match.group(1).strip()
            case_number = link_match.group(2).strip()

            # Extract date (format: DD/MM/YYYY)
            date_match = re.search(r'>(\d{2}/\d{2}/\d{4})<', row)
            session_date = date_match.group(1) if date_match else ""

            # Extract cells for rapporteur and descriptors
            # After the link, the next cell has rapporteur, then descriptors
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL | re.IGNORECASE)

            rapporteur = ""
            descriptors = ""

            # Typically: cell 0 = date, cell 1 = link/case, cell 2 = rapporteur, cell 3 = descriptors
            if len(cells) >= 3:
                # Cell 2 contains rapporteur
                rapporteur = self._clean_html(cells[2]).strip()
            if len(cells) >= 4:
                # Cell 3 contains descriptors
                descriptors = self._clean_html(cells[3]).strip()

            results.append({
                "doc_id": doc_id,
                "session_date": session_date,
                "case_number": case_number,
                "rapporteur": rapporteur,
                "descriptors": descriptors,
            })

        return results

    def _fetch_document(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a full document with all metadata and full text.

        Args:
            doc_id: The Lotus Notes document ID (hex string)

        Returns:
            Dict with all parsed fields, or None if fetch failed
        """
        url = f"{JSTA_DB}/{VIEW_ID}/{doc_id}?OpenDocument&ExpandSection=1"

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)

            if resp.status_code != 200:
                logger.debug(f"Document fetch failed: {doc_id} -> HTTP {resp.status_code}")
                return None

            # Decode content
            try:
                html_content = resp.content.decode("iso-8859-1")
            except:
                html_content = resp.content.decode("utf-8", errors="replace")

            return self._parse_document(html_content, doc_id)

        except Exception as e:
            logger.warning(f"Error fetching document {doc_id}: {e}")
            return None

    def _parse_document(self, html_content: str, doc_id: str) -> Optional[Dict[str, Any]]:
        """
        Parse a document page to extract all fields.
        """
        result = {"doc_id": doc_id}

        # Helper function to extract a field value from the two-column table structure
        # Format: <td>...<font ...>Label:</font>...</td><td>...<font ...>VALUE</font>...</td>
        def extract_field(label_pattern: str) -> Optional[str]:
            # Pattern matches: label cell followed by value cell with font tag
            pattern = (
                label_pattern + r'</font></b></td>'
                r'<td[^>]*><b><font[^>]*>([^<]+)</font>'
            )
            match = re.search(pattern, html_content, re.DOTALL | re.IGNORECASE)
            if match:
                return match.group(1).strip()
            return None

        # Extract basic fields
        result["case_number"] = extract_field(r'Processo:')
        result["date"] = extract_field(r'Data do Acord[^<]*:')
        result["section"] = extract_field(r'Tribunal:')
        result["rapporteur"] = extract_field(r'Relator:')
        result["conventional_number"] = extract_field(r'N[^<]*Convencional:')
        result["document_number"] = extract_field(r'N[^<]*do Documento:')
        result["appellant"] = extract_field(r'Recorrente:')
        result["appellee"] = extract_field(r'Recorrido[^<]*:')
        result["voting"] = extract_field(r'Vota[^<]*o:')

        # Extract summary (may have more complex content)
        sumario_match = re.search(
            r'Sum[^<]*rio:</font></b></td>[^<]*<td[^>]*>(.*?)</td>',
            html_content,
            re.DOTALL | re.IGNORECASE
        )
        if sumario_match:
            result["summary"] = self._clean_html(sumario_match.group(1))

        # Extract descriptors (may have multiple lines with <br>)
        desc_match = re.search(
            r'Descritores:</font></b></td><td[^>]*><b><font[^>]*>([^<]*(?:<br>[^<]*)*)</font>',
            html_content,
            re.DOTALL | re.IGNORECASE
        )
        if desc_match:
            descriptors_html = desc_match.group(1)
            # Split by <br> and clean
            desc_lines = re.split(r'<br\s*/?>', descriptors_html, flags=re.IGNORECASE)
            result["descriptors"] = [self._clean_html(d).strip() for d in desc_lines if self._clean_html(d).strip()]

        # Extract full text (Texto Integral)
        # The full text is in a table row after the "Texto Integral" section
        texto_match = re.search(
            r'Texto Integral:</font></b></td>\s*<td[^>]*>(.*?)</td>\s*</tr>',
            html_content,
            re.DOTALL | re.IGNORECASE
        )
        if texto_match:
            texto_html = texto_match.group(1)
            result["full_text"] = self._clean_html(texto_html)
        else:
            # Try alternative pattern
            texto_match2 = re.search(
                r'<b><font[^>]*>Texto Integral</font></b>.*?<td[^>]*>(.*?)</td>\s*</tr>',
                html_content,
                re.DOTALL | re.IGNORECASE
            )
            if texto_match2:
                texto_html = texto_match2.group(1)
                result["full_text"] = self._clean_html(texto_html)

        return result

    def _clean_html(self, html_text: str) -> str:
        """Strip HTML tags and clean text."""
        if not html_text:
            return ""

        # Remove style and script tags
        text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)

        # Convert br/p/div to newlines
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)

        # Remove all remaining tags
        text = re.sub(r'<[^>]+>', '', text)

        # Decode HTML entities
        text = html.unescape(text)

        # Clean up whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' +', ' ', text)

        # Strip lines
        lines = [line.strip() for line in text.split('\n')]
        text = '\n'.join(lines)

        return text.strip()

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Convert DD/MM/YYYY to ISO format YYYY-MM-DD."""
        if not date_str:
            return None
        try:
            parts = date_str.split("/")
            if len(parts) == 3:
                return f"{parts[2]}-{parts[1]}-{parts[0]}"
        except:
            pass
        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all decisions from the Portuguese Supreme Administrative Court.

        Iterates through the database listing pages from newest to oldest.
        """
        start = 1
        page_num = 1
        empty_pages = 0
        max_empty_pages = 3  # Stop after 3 consecutive empty pages

        while True:
            logger.info(f"Processing listing page {page_num} (start={start})...")

            html_content = self._fetch_listing_page(start)
            if not html_content:
                empty_pages += 1
                if empty_pages >= max_empty_pages:
                    logger.info("Reached end of database (empty pages)")
                    break
                start += PAGE_SIZE
                page_num += 1
                continue

            entries = self._parse_listing_page(html_content)
            if not entries:
                empty_pages += 1
                if empty_pages >= max_empty_pages:
                    logger.info("Reached end of database (no entries)")
                    break
                start += PAGE_SIZE
                page_num += 1
                continue

            empty_pages = 0  # Reset counter on successful page
            logger.info(f"Found {len(entries)} entries on page {page_num}")

            for entry in entries:
                doc_id = entry["doc_id"]

                # Fetch full document
                doc = self._fetch_document(doc_id)
                if doc:
                    # Merge listing data with document data
                    doc["listing_data"] = entry
                    yield doc

            start += PAGE_SIZE
            page_num += 1

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield recent decisions.

        Fetches decisions from the first few pages (most recent).
        """
        pages_to_check = 5  # Check first 5 pages for recent updates
        start = 1

        for page_num in range(pages_to_check):
            logger.info(f"Checking page {page_num + 1} for updates...")

            html_content = self._fetch_listing_page(start)
            if not html_content:
                break

            entries = self._parse_listing_page(html_content)
            if not entries:
                break

            for entry in entries:
                doc_id = entry["doc_id"]

                # Check date
                date_str = entry.get("session_date", "")
                iso_date = self._parse_date(date_str)
                if iso_date:
                    doc_date = datetime.fromisoformat(iso_date)
                    if doc_date.replace(tzinfo=None) < since.replace(tzinfo=None):
                        # Past the since date, stop
                        return

                # Fetch full document
                doc = self._fetch_document(doc_id)
                if doc:
                    doc["listing_data"] = entry
                    yield doc

            start += PAGE_SIZE

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw decision data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        listing = raw.get("listing_data", {})

        # Get identifiers
        case_number = raw.get("case_number", listing.get("case_number", ""))
        doc_id = raw.get("doc_id", "")
        conv_num = raw.get("conventional_number", "")

        # Build document ID
        if conv_num:
            doc_id_str = f"STA-{conv_num}"
        elif case_number:
            doc_id_str = f"STA-{case_number.replace('/', '-').replace(' ', '')}"
        else:
            doc_id_str = f"STA-{doc_id[:16]}"

        # Get date
        date_str = raw.get("date", listing.get("session_date", ""))
        iso_date = self._parse_date(date_str)

        # Get full text
        full_text = raw.get("full_text", "")

        # Get summary
        summary = raw.get("summary", "")

        # Get section
        section = raw.get("section", "")

        # Build title
        title = f"Acórdão STA {case_number}"
        if section:
            # Abbreviate section name
            if "ADMINISTRATIVO" in section.upper():
                title = f"{title} (CA)"
            elif "TRIBUTÁRIO" in section.upper() or "TRIBUT" in section.upper():
                title = f"{title} (CT)"

        # Get descriptors
        descriptors = raw.get("descriptors", [])
        if isinstance(descriptors, str):
            descriptors = [d.strip() for d in descriptors.split("\n") if d.strip()]

        # Build URL
        url = f"{BASE_URL}{JSTA_DB}/{VIEW_ID}/{doc_id}?OpenDocument"

        return {
            # Required base fields
            "_id": doc_id_str,
            "_source": "PT/STA",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "summary": summary,
            "date": iso_date,
            "url": url,
            # Case law specific fields
            "case_number": case_number,
            "conventional_number": conv_num,
            "document_number": raw.get("document_number", ""),
            "rapporteur": raw.get("rapporteur", listing.get("rapporteur", "")),
            "section": section,
            "descriptors": descriptors,
            "appellant": raw.get("appellant", ""),
            "appellee": raw.get("appellee", ""),
            "voting": raw.get("voting", ""),
            # Source info
            "court": "Supremo Tribunal Administrativo",
            "jurisdiction": "PT",
            "language": "pt",
            "doc_id": doc_id,
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Portuguese Supreme Administrative Court (STA) endpoints...")

        # Test 1: Database listing
        print("\n1. Testing database listing...")
        html_content = self._fetch_listing_page(start=1)
        if html_content:
            print(f"   Listing page fetched: {len(html_content)} bytes")
            entries = self._parse_listing_page(html_content)
            print(f"   Found {len(entries)} entries")

            if entries:
                sample = entries[0]
                print(f"   Sample entry:")
                print(f"     Case: {sample.get('case_number')}")
                print(f"     Date: {sample.get('session_date')}")
                print(f"     Rapporteur: {sample.get('rapporteur')}")
                print(f"     Doc ID: {sample.get('doc_id')}")
        else:
            print("   Failed to fetch listing page")

        # Test 2: Document fetch with full text
        print("\n2. Testing document fetch (full text)...")
        if entries:
            doc_id = entries[0]["doc_id"]
            doc = self._fetch_document(doc_id)
            if doc:
                print(f"   Document fetched successfully")
                print(f"   Case number: {doc.get('case_number')}")
                print(f"   Date: {doc.get('date')}")
                print(f"   Section: {doc.get('section')}")
                print(f"   Rapporteur: {doc.get('rapporteur')}")
                print(f"   Conventional number: {doc.get('conventional_number')}")

                full_text = doc.get("full_text", "")
                print(f"   Full text length: {len(full_text)} characters")
                if full_text:
                    print(f"   Full text preview: {full_text[:400]}...")

                summary = doc.get("summary", "")
                print(f"   Summary length: {len(summary)} characters")

                descriptors = doc.get("descriptors", [])
                print(f"   Descriptors: {descriptors}")
            else:
                print("   Failed to fetch document")

        # Test 3: Second page
        print("\n3. Testing pagination (page 2)...")
        html_content2 = self._fetch_listing_page(start=122)
        if html_content2:
            entries2 = self._parse_listing_page(html_content2)
            print(f"   Found {len(entries2)} entries on page 2")
            if entries2:
                print(f"   First entry on page 2: {entries2[0].get('case_number')}")

        print("\nTest complete!")


def main():
    scraper = STAScraper()

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
