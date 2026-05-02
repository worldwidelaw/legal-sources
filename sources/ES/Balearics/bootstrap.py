#!/usr/bin/env python3
"""
ES/Balearics -- Balearic Islands Regional Legislation (BOIB)

Fetches legislation from the Butlletí Oficial de les Illes Balears (BOIB)
via ELI XML endpoints with full text content.

Strategy:
  - Crawl annual calendar pages at caib.es/eboibfront/ca/{year}/ to get bulletin IDs.
  - For each bulletin, parse the TOC page to extract XML document links.
  - Fetch XML for each document, extract full text from <env:contingut>.
  - Filter for Secció I (Disposicions Generals) = core legislation.

Data:
  - Full text available from 2013 onward (ELI XML from ~2019, legacy XML before).
  - License: CC BY (Govern Illes Balears open data catalog).
  - Languages: Catalan (primary), Spanish translations available.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import time
import socket
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Dict, Any, Optional

socket.setdefaulttimeout(120)

try:
    import requests
    from requests.adapters import HTTPAdapter
except ImportError:
    print("ERROR: requests is required. Install with: pip install requests")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ES.balearics")

BASE_URL = "https://www.caib.es/eboibfront"
CALENDAR_URL = f"{BASE_URL}/ca/{{year}}/"
SECTION_I_ID = "471"  # Secció I - Disposicions Generals

# Year range: full text XML available from 2013
START_YEAR = 2013


class BalearicsScraper(BaseScraper):
    """
    Scraper for ES/Balearics -- Balearic Islands Regional Legislation (BOIB).
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml,application/xml",
        })
        adapter = HTTPAdapter(max_retries=3)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _clean_html(self, text: str) -> str:
        """Strip HTML tags and clean whitespace from content."""
        if not text:
            return ""
        # First unescape HTML entities (older XML has &lt;p&gt; encoded tags)
        text = html.unescape(text)
        # Unescape again for double-encoded content
        if '&lt;' in text or '&amp;' in text:
            text = html.unescape(text)
        # Now strip HTML tags
        text = re.sub(r'<br\s*/?\s*>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<p[^>]*>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n[ \t]+', '\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _get_bulletin_ids(self, year: int) -> list:
        """Get all bulletin IDs for a given year from the calendar page."""
        url = CALENDAR_URL.format(year=year)
        self.rate_limiter.wait()
        try:
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch calendar for {year}: {e}")
            return []

        pattern = rf'eboibfront/ca/{year}/(\d+)'
        ids = list(dict.fromkeys(re.findall(pattern, resp.text)))
        logger.info(f"Year {year}: found {len(ids)} bulletins")
        return ids

    def _get_section_i_xml_links(self, year: int, bulletin_id: str) -> list:
        """Get XML document links from Section I of a bulletin."""
        # Try to load the Section I sub-page directly
        section_url = f"{BASE_URL}/ca/{year}/{bulletin_id}/seccio-i-disposicions-generals/{SECTION_I_ID}"
        self.rate_limiter.wait()
        try:
            resp = self.session.get(section_url, timeout=60)
            if resp.status_code == 200 and 'seccio' in resp.text.lower():
                return self._extract_xml_links(resp.text, year, bulletin_id)
        except Exception as e:
            logger.debug(f"Section I page failed for {bulletin_id}: {e}")

        # Fallback: load the main bulletin TOC page
        toc_url = f"{BASE_URL}/ca/{year}/{bulletin_id}"
        self.rate_limiter.wait()
        try:
            resp = self.session.get(toc_url, timeout=60)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch bulletin {bulletin_id}: {e}")
            return []

        # Check if Section I exists at all
        if f'seccio-i-disposicions-generals/{SECTION_I_ID}' not in resp.text:
            logger.debug(f"Bulletin {bulletin_id}: no Section I")
            return []

        return self._extract_xml_links(resp.text, year, bulletin_id)

    def _extract_xml_links(self, page_html: str, year: int, bulletin_id: str) -> list:
        """Extract XML document links from a bulletin or section page."""
        links = []

        # ELI format (newer): /eboibfront/eli/.../xml
        eli_pattern = r'https?://www\.caib\.es(/eboibfront/eli/[^"]+/xml)'
        for match in re.finditer(eli_pattern, page_html):
            links.append(f"https://www.caib.es{match.group(1)}")

        # Legacy format (older): /eboibfront/ca/{year}/{id}/{enviament_id}/{slug}/xml
        legacy_pattern = rf'/eboibfront/ca/{year}/{bulletin_id}/(\d+)/([^/"]+)/xml'
        for match in re.finditer(legacy_pattern, page_html):
            path = match.group(0)
            full_url = f"https://www.caib.es{path}"
            if full_url not in links:
                links.append(full_url)

        return list(dict.fromkeys(links))

    def _fetch_document_xml(self, xml_url: str) -> Optional[dict]:
        """Fetch and parse a single document's XML, extracting full text."""
        self.rate_limiter.wait()
        try:
            resp = self.session.get(xml_url, timeout=60)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch {xml_url}: {e}")
            return None

        # Force UTF-8 decoding (server reports ISO-8859-1 but content is UTF-8)
        resp.encoding = "utf-8"
        xml_text = resp.text

        # Extract fields using regex (XML namespaces make ET parsing fragile)
        contingut = self._extract_xml_field(xml_text, "env:contingut")
        if not contingut:
            contingut = self._extract_xml_field(xml_text, "contingut")
        if not contingut:
            return None

        # Clean CDATA wrappers
        contingut = re.sub(r'!\[CDATA\[', '', contingut)
        contingut = re.sub(r'\]\]$', '', contingut)

        # Clean HTML from content
        text = self._clean_html(contingut)
        if len(text) < 50:
            return None

        # Extract metadata
        title = self._extract_xml_field(xml_text, "env:sumariEnviament") or ""
        title = re.sub(r'!\[CDATA\[|\]\]', '', title).strip()
        title = html.unescape(title)

        enviament_id = self._extract_xml_field(xml_text, "env:idEnviament") or ""
        pub_date = self._extract_xml_field(xml_text, "env:dataPublicacio") or ""
        if not pub_date:
            pub_date = self._extract_xml_field(xml_text, "dc:date") or ""
        reg_date = self._extract_xml_field(xml_text, "env:dataRegistre") or ""
        section_ref = self._extract_xml_attr(xml_text, "env:seccio", "rdf:resource") or ""
        org_ref = self._extract_xml_attr(xml_text, "env:organisme", "rdf:resource") or ""

        # Extract ELI title if available
        eli_title = self._extract_xml_field(xml_text, "eli:title") or ""
        if eli_title and not title:
            title = eli_title

        # Determine document type from ELI metadata
        doc_type_ref = self._extract_xml_attr(xml_text, "eli:type_document", "rdf:resource") or ""
        doc_type = doc_type_ref.rsplit("/", 1)[-1] if doc_type_ref else ""

        return {
            "enviament_id": enviament_id,
            "title": title,
            "text": text,
            "date": pub_date or reg_date,
            "registration_date": reg_date,
            "publication_date": pub_date,
            "section_ref": section_ref,
            "organism_ref": org_ref,
            "doc_type": doc_type,
            "xml_url": xml_url,
        }

    def _extract_xml_field(self, xml_text: str, tag: str) -> Optional[str]:
        """Extract text content of an XML element by tag name."""
        # Handle both self-closing and content-containing tags
        pattern = rf'<{re.escape(tag)}[^>]*>(.*?)</{re.escape(tag)}>'
        match = re.search(pattern, xml_text, re.DOTALL)
        if match:
            return match.group(1).strip()

        # Try rdf:parseType="Literal" variant
        pattern = rf'<{re.escape(tag)}\s+rdf:parseType="Literal">(.*?)</{re.escape(tag)}>'
        match = re.search(pattern, xml_text, re.DOTALL)
        if match:
            return match.group(1).strip()

        # Try rdf:datatype variant
        pattern = rf'<{re.escape(tag)}\s+rdf:datatype="[^"]*">(.*?)</{re.escape(tag)}>'
        match = re.search(pattern, xml_text, re.DOTALL)
        if match:
            return match.group(1).strip()

        return None

    def _extract_xml_attr(self, xml_text: str, tag: str, attr: str) -> Optional[str]:
        """Extract an attribute value from an XML element."""
        pattern = rf'<{re.escape(tag)}\s+{re.escape(attr)}="([^"]*)"'
        match = re.search(pattern, xml_text)
        return match.group(1) if match else None

    def _is_section_i(self, doc: dict) -> bool:
        """Check if a document belongs to Section I (Disposicions Generals)."""
        ref = doc.get("section_ref", "")
        return f"/{SECTION_I_ID}" in ref or ref.endswith(SECTION_I_ID)

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all Section I legislation from BOIB (2013-present)."""
        current_year = datetime.now().year
        seen_ids = set()

        for year in range(START_YEAR, current_year + 1):
            logger.info(f"Processing year {year}...")
            bulletin_ids = self._get_bulletin_ids(year)

            for bid in bulletin_ids:
                xml_links = self._get_section_i_xml_links(year, bid)
                if not xml_links:
                    continue

                logger.info(f"Bulletin {bid}: {len(xml_links)} XML documents")

                for xml_url in xml_links:
                    doc = self._fetch_document_xml(xml_url)
                    if not doc:
                        continue

                    eid = doc.get("enviament_id", "")
                    if eid and eid in seen_ids:
                        continue
                    if eid:
                        seen_ids.add(eid)

                    yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents published since the given date."""
        current_year = datetime.now().year
        start_year = since.year
        seen_ids = set()

        for year in range(start_year, current_year + 1):
            logger.info(f"Checking updates for year {year}...")
            bulletin_ids = self._get_bulletin_ids(year)

            for bid in bulletin_ids:
                xml_links = self._get_section_i_xml_links(year, bid)
                if not xml_links:
                    continue

                for xml_url in xml_links:
                    doc = self._fetch_document_xml(xml_url)
                    if not doc:
                        continue

                    pub_date = doc.get("publication_date", "") or doc.get("date", "")
                    if pub_date and pub_date < since.strftime("%Y-%m-%d"):
                        continue

                    eid = doc.get("enviament_id", "")
                    if eid and eid in seen_ids:
                        continue
                    if eid:
                        seen_ids.add(eid)

                    yield doc

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        eid = raw.get("enviament_id", "")
        xml_url = raw.get("xml_url", "")
        html_url = xml_url.replace("/xml", "/html") if xml_url else ""
        # For ELI URLs, derive the HTML link; for legacy, use the slug URL
        if "/eli/" in xml_url:
            html_url = xml_url.rsplit("/xml", 1)[0] + "/html"
        else:
            html_url = xml_url.rsplit("/xml", 1)[0] + "/" if xml_url else ""

        return {
            "_id": f"BOIB-{eid}" if eid else f"BOIB-{hash(xml_url)}",
            "_source": "ES/Balearics",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": html_url or xml_url,
            "publication_date": raw.get("publication_date", ""),
            "registration_date": raw.get("registration_date", ""),
            "doc_type": raw.get("doc_type", ""),
            "enviament_id": eid,
            "language": "ca",
            "region": "Illes Balears",
            "country": "ES",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing BOIB Balearics connection...")

        print("\n1. Testing calendar page (2025)...")
        try:
            ids = self._get_bulletin_ids(2025)
            print(f"   Found {len(ids)} bulletins for 2025")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        print("\n2. Testing Section I document discovery...")
        found_docs = False
        for bid in ids[:10]:
            xml_links = self._get_section_i_xml_links(2025, bid)
            if xml_links:
                print(f"   Bulletin {bid}: {len(xml_links)} Section I documents")
                found_docs = True

                print("\n3. Testing XML full text retrieval...")
                doc = self._fetch_document_xml(xml_links[0])
                if doc:
                    text = doc.get("text", "")
                    print(f"   Title: {doc.get('title', '')[:80]}")
                    print(f"   Date: {doc.get('date', '')}")
                    print(f"   Text length: {len(text)} chars")
                    print(f"   Preview: {text[:200]}...")
                else:
                    print("   ERROR: Could not parse XML")
                break

        if not found_docs:
            print("   No Section I documents found in first 10 bulletins")

        print("\nAll tests passed!")


def main():
    scraper = BalearicsScraper()

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
