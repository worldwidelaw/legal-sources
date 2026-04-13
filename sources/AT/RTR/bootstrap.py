#!/usr/bin/env python3
"""
AT/RTR -- Austrian Regulatory Authority (RTR) Data Fetcher

Fetches regulatory decisions and publications from RTR (Rundfunk und Telekom
Regulierungs-GmbH) — Austria's regulator for telecom, media, broadcasting,
and postal services.

Strategy:
  - Paginate through the decisions listing at /TKP/aktuelles/entscheidungen/
  - Paginate through the publications listing at /TKP/aktuelles/publikationen/
  - Visit each detail page to extract PDF download links
  - Download PDFs and extract full text via common/pdf_extract

Endpoints:
  - Decisions: /TKP/aktuelles/entscheidungen/Uebersichtseite.de.html?p={page}
  - Publications: /TKP/aktuelles/publikationen/Uebersichtseite.de.html?p={page}
  - Detail pages: /TKP/aktuelles/entscheidungen/entscheidungen/{id}.de.html
  - PDF downloads: /TKP/aktuelles/entscheidungen/entscheidungen/{filename}.pdf

Data:
  - ~500+ decisions (50 pages), ~240+ publications (24 pages)
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
import hashlib
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
logger = logging.getLogger("legal-data-hunter.AT.RTR")

BASE_URL = "https://www.rtr.at"

# Listing pages with pagination
LISTINGS = [
    {
        "name": "decisions",
        "path": "/TKP/aktuelles/entscheidungen/Uebersichtseite.de.html",
        "category": "decision",
        "max_pages": 55,
    },
    {
        "name": "publications",
        "path": "/TKP/aktuelles/publikationen/Uebersichtseite.de.html",
        "category": "publication",
        "max_pages": 30,
    },
]

# Pattern to match detail page links from listing pages (absolute or relative URLs)
DECISION_LINK_RE = re.compile(
    r'href="(?:https://www\.rtr\.at)?(/TKP/aktuelles/entscheidungen/entscheidungen/[^"#]+\.de\.html)'
)
PUBLICATION_LINK_RE = re.compile(
    r'href="(?:https://www\.rtr\.at)?(/TKP/aktuelles/publikationen/(?:publikationen|newsletter)/[^"#]+\.de\.html)'
)

# Pattern to match PDF download links on detail pages (absolute or relative)
PDF_LINK_RE = re.compile(
    r'href="(?:https://www\.rtr\.at)?(/TKP/[^"]+\.pdf)"', re.IGNORECASE
)

# Pattern to extract date from listing entries (DD.MM.YYYY)
DATE_RE = re.compile(r'(\d{2}\.\d{2}\.\d{4})')


class RTRScraper(BaseScraper):
    """
    Scraper for AT/RTR -- Austrian Regulatory Authority.
    Country: AT
    URL: https://www.rtr.at

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
            source="AT/RTR",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="doctrine",
        ) or ""

    def _get_detail_links(self, listing: dict, max_pages: Optional[int] = None) -> List[Dict[str, str]]:
        """Paginate through a listing page and collect detail-page links."""
        results = []
        seen = set()
        pages = max_pages or listing["max_pages"]

        link_re = DECISION_LINK_RE if listing["category"] == "decision" else PUBLICATION_LINK_RE

        for page_num in range(pages):
            url = f"{listing['path']}?l=de&q=&t=&p={page_num}"
            logger.info(f"  Listing page {page_num + 1}/{pages}: {url}")

            try:
                self.rate_limiter.wait()
                resp = self.client.get(url)
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"  Failed to fetch listing page {page_num}: {e}")
                break

            links = link_re.findall(resp.text)
            if not links:
                logger.info(f"  No more links found on page {page_num + 1}, stopping")
                break

            new_count = 0
            for href in links:
                if href not in seen:
                    seen.add(href)
                    results.append({
                        "href": href,
                        "category": listing["category"],
                    })
                    new_count += 1

            logger.info(f"  Found {new_count} new detail links (total: {len(results)})")

        return results

    def _extract_detail_page(self, href: str, category: str) -> Optional[Dict[str, Any]]:
        """Visit a detail page and extract metadata + PDF link."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(href)
            resp.raise_for_status()
            html_text = resp.text
        except Exception as e:
            logger.warning(f"  Failed to fetch detail page {href}: {e}")
            return None

        # Extract PDF links
        pdf_links = PDF_LINK_RE.findall(html_text)
        if not pdf_links:
            logger.debug(f"  No PDF found on {href}")
            return None

        # Use first PDF link
        pdf_href = pdf_links[0]

        # Extract title from <title> tag or <h1>
        title_match = re.search(r'<title>([^<]+)</title>', html_text)
        title = title_match.group(1).strip() if title_match else ""
        # Clean title — remove site suffix
        title = re.sub(r'\s*[|–-]\s*RTR.*$', '', title).strip()

        # Extract date — look for date in content panel divs
        date_str = ""
        # The date typically appears in <div class="el-content uk-panel">DD.MM.YYYY</div>
        panel_date = re.search(
            r'<div[^>]*class="el-content[^"]*"[^>]*>(\d{2}\.\d{2}\.\d{4})</div>',
            html_text,
        )
        date_match = panel_date or DATE_RE.search(html_text)
        if date_match:
            try:
                dt = datetime.strptime(date_match.group(1), "%d.%m.%Y")
                date_str = dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Extract case number for decisions (e.g., "R 4/24", "F 3/25")
        case_number = ""
        if category == "decision":
            case_match = re.search(
                r'(?:Geschäftszahl|GZ|Zahl)[\s:]*([A-Z]+\s*\d+/\d+(?:-\d+)?)',
                html_text,
            )
            if not case_match:
                # Try to extract from URL
                url_match = re.search(r'/([a-zA-Z]+\d*[-_]\d+(?:[-_]\d+)*)\.de\.html', href)
                if url_match:
                    case_number = url_match.group(1).replace('-', ' ').replace('_', '/')
            else:
                case_number = case_match.group(1)

        # Determine authority from page content
        authority = "RTR"
        if "TKK" in html_text[:3000] or "Telekom-Control-Kommission" in html_text:
            authority = "Telekom-Control-Kommission (TKK)"
        elif "PCK" in html_text[:3000] or "Post-Control-Kommission" in html_text:
            authority = "Post-Control-Kommission (PCK)"
        elif "KommAustria" in html_text[:3000]:
            authority = "KommAustria"

        return {
            "href": href,
            "pdf_href": pdf_href,
            "title": title,
            "date": date_str,
            "case_number": case_number,
            "category": category,
            "authority": authority,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all doctrine documents from RTR."""
        logger.info("Starting full bootstrap of AT/RTR documents")

        doc_count = 0
        global_seen_pdfs = set()

        for listing in LISTINGS:
            logger.info(f"\n=== Processing {listing['name']} ===")
            detail_links = self._get_detail_links(listing)
            logger.info(f"Found {len(detail_links)} detail pages for {listing['name']}")

            for i, link_info in enumerate(detail_links):
                href = link_info["href"]
                category = link_info["category"]

                detail = self._extract_detail_page(href, category)
                if not detail:
                    continue

                pdf_href = detail["pdf_href"]
                if pdf_href in global_seen_pdfs:
                    continue
                global_seen_pdfs.add(pdf_href)

                doc_count += 1
                logger.info(
                    f"  [{doc_count}] Downloading PDF: {detail['title'][:60]}..."
                )

                try:
                    self.rate_limiter.wait()
                    resp = self.client.get(pdf_href)
                    resp.raise_for_status()
                    pdf_bytes = resp.content

                    if len(pdf_bytes) < 500:
                        logger.warning(f"  Skipping (too small: {len(pdf_bytes)} bytes)")
                        continue

                    text = self._extract_pdf_text(pdf_bytes)
                    if not text or len(text) < 50:
                        logger.warning(f"  Skipping (no text extracted)")
                        continue

                    # Generate stable ID from PDF URL
                    doc_id = hashlib.sha256(pdf_href.encode()).hexdigest()[:16]

                    yield {
                        "doc_id": doc_id,
                        "title": detail["title"],
                        "text": text,
                        "date": detail["date"],
                        "url": f"{BASE_URL}{href}",
                        "file_url": f"{BASE_URL}{pdf_href}",
                        "case_number": detail["case_number"],
                        "category": category,
                        "authority": detail["authority"],
                        "language": "de",
                    }

                except Exception as e:
                    logger.warning(
                        f"  Failed to process PDF for {detail['title'][:60]}: {e}"
                    )
                    continue

        logger.info(f"Bootstrap complete: {doc_count} documents processed")

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Fetch updates since a given date. Re-scans first few pages."""
        logger.info(f"Fetching updates since {since} (scanning recent pages)")
        # Only scan first 3 pages of each listing for updates
        for listing in LISTINGS:
            detail_links = self._get_detail_links(listing, max_pages=3)
            for link_info in detail_links:
                detail = self._extract_detail_page(
                    link_info["href"], link_info["category"]
                )
                if not detail:
                    continue

                pdf_href = detail["pdf_href"]
                try:
                    self.rate_limiter.wait()
                    resp = self.client.get(pdf_href)
                    resp.raise_for_status()
                    pdf_bytes = resp.content
                    if len(pdf_bytes) < 500:
                        continue
                    text = self._extract_pdf_text(pdf_bytes)
                    if not text or len(text) < 50:
                        continue
                    doc_id = hashlib.sha256(pdf_href.encode()).hexdigest()[:16]
                    yield {
                        "doc_id": doc_id,
                        "title": detail["title"],
                        "text": text,
                        "date": detail["date"],
                        "url": f"{BASE_URL}{link_info['href']}",
                        "file_url": f"{BASE_URL}{pdf_href}",
                        "case_number": detail["case_number"],
                        "category": detail["category"],
                        "authority": detail["authority"],
                        "language": "de",
                    }
                except Exception:
                    continue

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        return {
            "_id": raw.get("doc_id", ""),
            "_source": "AT/RTR",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date") or None,
            "url": raw.get("url", ""),
            "doc_id": raw.get("doc_id", ""),
            "file_url": raw.get("file_url", ""),
            "case_number": raw.get("case_number", ""),
            "category": raw.get("category", ""),
            "authority": raw.get("authority", "RTR"),
            "language": raw.get("language", "de"),
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing RTR endpoints...")

        print("\n1. Testing decisions listing...")
        try:
            resp = self.client.get(
                "/TKP/aktuelles/entscheidungen/Uebersichtseite.de.html?l=de&q=&t=&p=0"
            )
            print(f"   Status: {resp.status_code}")
            links = DECISION_LINK_RE.findall(resp.text)
            print(f"   Found {len(links)} decision links on page 1")
            for link in links[:3]:
                print(f"   - {link}")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\n2. Testing publications listing...")
        try:
            resp = self.client.get(
                "/TKP/aktuelles/publikationen/Uebersichtseite.de.html?l=de&q=&t=&p=0"
            )
            print(f"   Status: {resp.status_code}")
            links = PUBLICATION_LINK_RE.findall(resp.text)
            print(f"   Found {len(links)} publication links on page 1")
            for link in links[:3]:
                print(f"   - {link}")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\n3. Testing detail page + PDF download...")
        try:
            links = DECISION_LINK_RE.findall(resp.text) if links else []
            # Get a decision link
            resp2 = self.client.get(
                "/TKP/aktuelles/entscheidungen/Uebersichtseite.de.html?l=de&q=&t=&p=0"
            )
            dec_links = DECISION_LINK_RE.findall(resp2.text)
            if dec_links:
                detail = self._extract_detail_page(dec_links[0], "decision")
                if detail:
                    print(f"   Title: {detail['title']}")
                    print(f"   PDF: {detail['pdf_href']}")
                    self.rate_limiter.wait()
                    pdf_resp = self.client.get(detail["pdf_href"])
                    print(f"   PDF Status: {pdf_resp.status_code}")
                    print(f"   PDF Size: {len(pdf_resp.content)} bytes")
                    text = self._extract_pdf_text(pdf_resp.content)
                    print(f"   Extracted text: {len(text)} chars")
                    if text:
                        print(f"   First 200 chars: {text[:200]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete.")


if __name__ == "__main__":
    scraper = RTRScraper()

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
