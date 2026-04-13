#!/usr/bin/env python3
"""
AT/E-Control -- Austrian Energy Regulator (E-Control) Data Fetcher

Fetches regulatory doctrine documents (market rules, technical operating rules,
general conditions, ordinance explanations) from E-Control Austria's Liferay CMS.

Strategy:
  - Crawl known document section pages on e-control.at
  - Extract PDF links from the Liferay document library pattern
  - Download PDFs and extract full text via common/pdf_extract

Endpoints:
  - Market rules (Strom): /marktteilnehmer/strom/marktregeln/...
  - Market rules (Gas): /marktteilnehmer/gas/marktregeln/...
  - TOR (Technical Operating Rules): /marktteilnehmer/strom/marktregeln/tor
  - Certificates download: /stromnachweis/download/...
  - System usage tariffs: /marktteilnehmer/strom/systemnutzungsentgelte
  - Gas system usage tariffs: /marktteilnehmer/gas/systemnutzungsentgelte

Data:
  - ~130+ regulatory PDF documents
  - Language: German (DE)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
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
from urllib.parse import urljoin, unquote

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
logger = logging.getLogger("legal-data-hunter.AT.e-control")

BASE_URL = "https://www.e-control.at"

# Pages known to contain PDF document links
DOCUMENT_PAGES = [
    # Strom (Electricity) market rules - general conditions
    {
        "path": "/marktteilnehmer/strom/marktregeln/allgemeine-bedingungen/bko",
        "category": "market_rules_strom",
        "subcategory": "general_conditions_bko",
    },
    {
        "path": "/marktteilnehmer/strom/marktregeln/allgemeine-bedingungen/bgv",
        "category": "market_rules_strom",
        "subcategory": "general_conditions_bgv",
    },
    {
        "path": "/marktteilnehmer/strom/marktregeln/allgemeine-bedingungen/vnb",
        "category": "market_rules_strom",
        "subcategory": "general_conditions_vnb",
    },
    {
        "path": "/marktteilnehmer/strom/marktregeln/allgemeine-bedingungen/oeko-bgv",
        "category": "market_rules_strom",
        "subcategory": "general_conditions_oeko_bgv",
    },
    {
        "path": "/marktteilnehmer/strom/marktregeln/allgemeine-bedingungen/uenb",
        "category": "market_rules_strom",
        "subcategory": "general_conditions_uenb",
    },
    # TOR (Technical Operating Rules)
    {
        "path": "/marktteilnehmer/strom/marktregeln/tor",
        "category": "technical_operating_rules",
        "subcategory": "tor",
    },
    # Gas market rules
    {
        "path": "/marktteilnehmer/gas/marktregeln/sonstige-marktregeln",
        "category": "market_rules_gas",
        "subcategory": "sonstige_marktregeln",
    },
    # Electricity system usage tariffs
    {
        "path": "/marktteilnehmer/strom/systemnutzungsentgelte",
        "category": "system_usage_tariffs",
        "subcategory": "strom",
    },
    # Gas system usage tariffs
    {
        "path": "/marktteilnehmer/gas/systemnutzungsentgelte",
        "category": "system_usage_tariffs",
        "subcategory": "gas",
    },
    # Certificate downloads (legal, handbooks, AGBs)
    {
        "path": "/stromnachweis/download/recht",
        "category": "certificates",
        "subcategory": "recht",
    },
    {
        "path": "/stromnachweis/download/agbs",
        "category": "certificates",
        "subcategory": "agbs",
    },
    {
        "path": "/stromnachweis/download/handbucher",
        "category": "certificates",
        "subcategory": "handbucher",
    },
    # Gas general conditions
    {
        "path": "/marktteilnehmer/gas/marktregeln/allgemeine-bedingungen",
        "category": "market_rules_gas",
        "subcategory": "general_conditions",
    },
    # Strom other market rules
    {
        "path": "/marktteilnehmer/strom/marktregeln/sonstige_marktregeln",
        "category": "market_rules_strom",
        "subcategory": "sonstige_marktregeln",
    },
]

# Liferay document URL pattern: /documents/{groupId}/{folderId}/{filename}.pdf/{uuid}?t={timestamp}
PDF_LINK_PATTERN = re.compile(
    r'href="(/documents/\d+/[^"]*\.pdf[^"]*)"', re.IGNORECASE
)


class EControlScraper(BaseScraper):
    """
    Scraper for AT/E-Control -- Austrian Energy Regulator.
    Country: AT
    URL: https://www.e-control.at

    Data types: doctrine
    Auth: none (Open public access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "de,en;q=0.5",
            },
            timeout=60,
        )

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="AT/E-Control",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="doctrine",
        ) or ""

    def _title_from_filename(self, filename: str) -> str:
        """Derive a human-readable title from a PDF filename."""
        # Remove .pdf extension
        name = re.sub(r'\.pdf$', '', filename, flags=re.IGNORECASE)
        # URL-decode
        name = unquote(name)
        # Remove version suffixes like (1), (2)
        name = re.sub(r'\+?\(\d+\)$', '', name).strip()
        # Replace + and _ with spaces
        name = name.replace('+', ' ').replace('_', ' ')
        # Clean up multiple spaces
        name = re.sub(r'\s+', ' ', name).strip()
        return name

    def _extract_pdf_links(self, page_path: str) -> List[Dict[str, str]]:
        """Extract unique PDF document links from a page."""
        results = []
        seen_uuids = set()

        try:
            self.rate_limiter.wait()
            resp = self.client.get(page_path)
            resp.raise_for_status()

            matches = PDF_LINK_PATTERN.findall(resp.text)
            for href in matches:
                # Extract UUID from URL to deduplicate
                uuid_match = re.search(r'/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', href)
                if uuid_match:
                    uuid = uuid_match.group(1)
                    if uuid in seen_uuids:
                        continue
                    seen_uuids.add(uuid)

                # Extract filename from URL
                filename_match = re.search(r'/([^/]+\.pdf)', href, re.IGNORECASE)
                filename = unquote(filename_match.group(1)) if filename_match else "unknown.pdf"
                title = self._title_from_filename(filename)

                results.append({
                    "href": href,
                    "filename": filename,
                    "title": title,
                })

            logger.info(f"Found {len(results)} unique PDFs on {page_path}")
        except Exception as e:
            logger.warning(f"Failed to fetch {page_path}: {e}")

        return results

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all doctrine documents from E-Control."""
        logger.info("Starting full bootstrap of AT/E-Control documents")

        global_seen = set()
        doc_count = 0

        for page_info in DOCUMENT_PAGES:
            page_path = page_info["path"]
            category = page_info["category"]
            subcategory = page_info["subcategory"]

            logger.info(f"Crawling: {page_path}")
            pdf_links = self._extract_pdf_links(page_path)

            for link in pdf_links:
                href = link["href"]

                # Deduplicate across pages by UUID
                uuid_match = re.search(
                    r'/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', href
                )
                dedup_key = uuid_match.group(1) if uuid_match else href
                if dedup_key in global_seen:
                    continue
                global_seen.add(dedup_key)

                doc_count += 1
                logger.info(f"  [{doc_count}] Downloading: {link['title'][:60]}...")

                try:
                    self.rate_limiter.wait()
                    resp = self.client.get(href)
                    resp.raise_for_status()
                    pdf_bytes = resp.content

                    if len(pdf_bytes) < 500:
                        logger.warning(f"  Skipping (too small: {len(pdf_bytes)} bytes)")
                        continue

                    text = self._extract_pdf_text(pdf_bytes)
                    if not text or len(text) < 50:
                        logger.warning(f"  Skipping (no text extracted)")
                        continue

                    doc_id = dedup_key if uuid_match else f"econtrol-{doc_count:04d}"

                    yield {
                        "doc_id": doc_id,
                        "title": link["title"],
                        "text": text,
                        "date": "",
                        "url": f"{BASE_URL}{page_path}",
                        "file_url": f"{BASE_URL}{href}",
                        "filename": link["filename"],
                        "category": category,
                        "subcategory": subcategory,
                        "language": "de",
                    }

                except Exception as e:
                    logger.warning(f"  Failed to process {link['title'][:60]}: {e}")
                    continue

        logger.info(f"Bootstrap complete: {doc_count} documents processed")

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Fetch updates since a given date. Re-fetches all (moderate corpus)."""
        logger.info(f"Fetching updates since {since} (re-scanning all)")
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        return {
            "_id": raw.get("doc_id", ""),
            "_source": "AT/E-Control",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date") or None,
            "url": raw.get("url", ""),
            "doc_id": raw.get("doc_id", ""),
            "file_url": raw.get("file_url", ""),
            "filename": raw.get("filename", ""),
            "category": raw.get("category", ""),
            "subcategory": raw.get("subcategory", ""),
            "language": raw.get("language", "de"),
            "authority": "Energie-Control Austria",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing E-Control endpoints...")

        print("\n1. Testing main site...")
        try:
            resp = self.client.get("/")
            print(f"   Status: {resp.status_code}")
            print(f"   Page length: {len(resp.text)} chars")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        print("\n2. Testing BKO page for PDF links...")
        try:
            links = self._extract_pdf_links(
                "/marktteilnehmer/strom/marktregeln/allgemeine-bedingungen/bko"
            )
            print(f"   Found {len(links)} PDF links")
            for link in links[:3]:
                print(f"   - {link['title']}")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\n3. Testing sample PDF download...")
        try:
            links = self._extract_pdf_links(
                "/marktteilnehmer/strom/marktregeln/allgemeine-bedingungen/bko"
            )
            if links:
                self.rate_limiter.wait()
                resp = self.client.get(links[0]["href"])
                print(f"   Status: {resp.status_code}")
                print(f"   Content-Type: {resp.headers.get('Content-Type', 'unknown')}")
                print(f"   Size: {len(resp.content)} bytes")
                if resp.status_code == 200:
                    text = self._extract_pdf_text(resp.content)
                    print(f"   Extracted text: {len(text)} chars")
                    if text:
                        print(f"   First 200 chars: {text[:200]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete.")


if __name__ == "__main__":
    scraper = EControlScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
