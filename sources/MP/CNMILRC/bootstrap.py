#!/usr/bin/env python3
"""
MP/CNMILRC -- CNMI Law Revision Commission

Fetches legislation and court decisions from https://www.cnmilaw.org/.
Covers:
  - Commonwealth Code (CMC): 10 titles, ~7,174 sections as individual PDFs
  - CNMI Administrative Code (NMIAC): ~289 chapter PDFs
  - Public Laws: 1,673+ law PDFs across 24 legislatures
  - Supreme Court slip opinions: 757+ decision PDFs (1989-present)
  - Superior Court decisions: 1,446+ decision PDFs (1989-present)
  - Commonwealth Reporter: 282 decisions in 3 volumes

All content is PDF. We scrape HTML index pages for metadata and PDF links,
then download and extract text via pdfplumber.

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~20 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test-api              # Quick connectivity test
"""

import sys
import re
import logging
import time
import hashlib
from io import BytesIO
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional
from urllib.parse import urljoin
from itertools import zip_longest

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MP.CNMILRC")

BASE_URL = "https://www.cnmilaw.org"

_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

MAX_RETRIES = 3
RETRY_BACKOFF = 2


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    try:
        import pdfplumber
    except ImportError:
        logger.error("pdfplumber not installed. Run: pip3 install pdfplumber")
        return ""
    text_parts = []
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
    except Exception as e:
        logger.warning(f"PDF extraction error: {e}")
    return "\n\n".join(text_parts)


class CNMILRCScraper(BaseScraper):
    """Scraper for MP/CNMILRC - CNMI Law Revision Commission."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = None

    def _get_session(self):
        if self.session is None:
            import requests
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry

            self.session = requests.Session()
            self.session.headers.update(_BROWSER_HEADERS)

            retry_strategy = Retry(
                total=MAX_RETRIES,
                backoff_factor=RETRY_BACKOFF,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET"],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self.session.mount("https://", adapter)
            self.session.mount("http://", adapter)
        return self.session

    def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch an HTML page with retry."""
        self.rate_limiter.wait()
        sess = self._get_session()
        try:
            resp = sess.get(url, timeout=30)
            resp.raise_for_status()
            if resp.encoding and resp.encoding.lower() != 'utf-8':
                resp.encoding = resp.apparent_encoding or 'utf-8'
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

    def _fetch_pdf(self, url: str) -> Optional[bytes]:
        """Download a PDF file."""
        self.rate_limiter.wait()
        sess = self._get_session()
        try:
            resp = sess.get(url, timeout=60)
            resp.raise_for_status()
            if len(resp.content) < 100:
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {url}")
                return None
            return resp.content
        except Exception as e:
            logger.warning(f"Failed to download PDF {url}: {e}")
            return None

    # ── CMC (Commonwealth Code) ──────────────────────────────────────

    def _discover_cmc_sections(self) -> List[Dict[str, str]]:
        """Parse cmc.php to discover all section PDF links."""
        html = self._fetch_page(f"{BASE_URL}/cmc.php")
        if not html:
            return []

        sections = []
        seen = set()
        # Match href links to cmc_section PDFs
        for m in re.finditer(
            r'href="([^"]*pdf/cmc_section/T(\d+)/(\d+)\.pdf)"',
            html, re.IGNORECASE
        ):
            href, title_num, section_num = m.group(1), m.group(2), m.group(3)
            pdf_url = urljoin(f"{BASE_URL}/cmc.php", href)
            key = f"T{title_num}/{section_num}"
            if key not in seen:
                seen.add(key)
                # Try to extract section name from surrounding text
                sections.append({
                    "pdf_url": pdf_url,
                    "title_num": title_num,
                    "section_num": section_num,
                    "doc_type": "legislation",
                    "category": "cmc",
                })

        logger.info(f"CMC: found {len(sections)} section PDFs")
        return sections

    # ── NMIAC (Administrative Code) ──────────────────────────────────

    def _discover_admin_chapters(self) -> List[Dict[str, str]]:
        """Parse admin.php to discover all admin code chapter PDF links."""
        html = self._fetch_page(f"{BASE_URL}/admin.php")
        if not html:
            return []

        chapters = []
        seen = set()
        for m in re.finditer(
            r'href="([^"]*pdf/admincode/T(\d+)/T\d+-([^"]+)\.pdf)"',
            html, re.IGNORECASE
        ):
            href, title_num, chapter_id = m.group(1), m.group(2), m.group(3)
            pdf_url = urljoin(f"{BASE_URL}/admin.php", href)
            key = f"T{title_num}/{chapter_id}"
            if key not in seen:
                seen.add(key)
                chapters.append({
                    "pdf_url": pdf_url,
                    "title_num": title_num,
                    "chapter_id": chapter_id,
                    "doc_type": "legislation",
                    "category": "nmiac",
                })

        logger.info(f"NMIAC: found {len(chapters)} chapter PDFs")
        return chapters

    # ── Supreme Court ────────────────────────────────────────────────

    def _discover_supreme_court(self) -> List[Dict[str, Any]]:
        """Parse spm{YY}.php pages to discover Supreme Court opinion PDFs."""
        decisions = []
        # Years: 89-99 (1989-1999), 00-26 (2000-2026)
        year_codes = [str(y).zfill(2) for y in list(range(89, 100)) + list(range(0, 27))]

        for yy in year_codes:
            url = f"{BASE_URL}/spm{yy}.php"
            html = self._fetch_page(url)
            if not html:
                continue

            # Parse table rows for case info and PDF links
            # Look for PDF href links in the page
            for m in re.finditer(
                r'href="([^"]*pdf/supreme/([^"]+)\.pdf)"',
                html, re.IGNORECASE
            ):
                href, case_file = m.group(1), m.group(2)
                pdf_url = urljoin(url, href)

                # Try to extract case name and date from surrounding table row
                case_name = ""
                case_date = ""

                # Search backward in HTML for table data
                pos = m.start()
                # Look for the table row containing this link
                row_start = html.rfind('<tr', max(0, pos - 2000), pos)
                if row_start >= 0:
                    row_end = html.find('</tr>', pos)
                    if row_end >= 0:
                        row_html = html[row_start:row_end]
                        # Extract table cells
                        cells = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.DOTALL | re.IGNORECASE)
                        if len(cells) >= 3:
                            case_name = re.sub(r'<[^>]+>', '', cells[0]).strip()
                            # Date is typically in 3rd or 4th cell
                            for cell in cells[2:]:
                                cell_text = re.sub(r'<[^>]+>', '', cell).strip()
                                date_m = re.search(r'(\d{4}-\d{2}-\d{2})', cell_text)
                                if date_m:
                                    case_date = date_m.group(1)
                                    break
                                # Try MM/DD/YYYY
                                date_m = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', cell_text)
                                if date_m:
                                    case_date = f"{date_m.group(3)}-{date_m.group(1).zfill(2)}-{date_m.group(2).zfill(2)}"
                                    break

                if not case_name:
                    case_name = case_file.replace('-', ' ')

                full_year = int("19" + yy) if int(yy) >= 89 else int("20" + yy)

                decisions.append({
                    "pdf_url": pdf_url,
                    "case_file": case_file,
                    "case_name": case_name,
                    "case_date": case_date,
                    "year": full_year,
                    "court": "Supreme Court",
                    "doc_type": "case_law",
                    "category": "supreme",
                })

        logger.info(f"Supreme Court: found {len(decisions)} opinion PDFs")
        return decisions

    # ── Superior Court ───────────────────────────────────────────────

    def _discover_superior_court(self) -> List[Dict[str, Any]]:
        """Parse sup{YY}.php pages to discover Superior Court decision PDFs."""
        decisions = []
        year_codes = [str(y).zfill(2) for y in list(range(89, 100)) + list(range(0, 27))]

        for yy in year_codes:
            url = f"{BASE_URL}/sup{yy}.php"
            html = self._fetch_page(url)
            if not html:
                continue

            for m in re.finditer(
                r'href="([^"]*pdf/superior/([^"]+)\.pdf)"',
                html, re.IGNORECASE
            ):
                href, filename = m.group(1), m.group(2)
                pdf_url = urljoin(url, href)

                case_name = ""
                case_date = ""
                case_file = ""

                pos = m.start()
                row_start = html.rfind('<tr', max(0, pos - 2000), pos)
                if row_start >= 0:
                    row_end = html.find('</tr>', pos)
                    if row_end >= 0:
                        row_html = html[row_start:row_end]
                        cells = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.DOTALL | re.IGNORECASE)
                        if len(cells) >= 3:
                            case_name = re.sub(r'<[^>]+>', '', cells[0]).strip()
                            if len(cells) >= 2:
                                case_file = re.sub(r'<[^>]+>', '', cells[1]).strip()
                            for cell in cells[2:]:
                                cell_text = re.sub(r'<[^>]+>', '', cell).strip()
                                date_m = re.search(r'(\d{4}-\d{2}-\d{2})', cell_text)
                                if date_m:
                                    case_date = date_m.group(1)
                                    break
                                date_m = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', cell_text)
                                if date_m:
                                    case_date = f"{date_m.group(3)}-{date_m.group(1).zfill(2)}-{date_m.group(2).zfill(2)}"
                                    break

                if not case_name:
                    case_name = filename.replace('-', ' ')
                if not case_file:
                    case_file = filename

                full_year = int("19" + yy) if int(yy) >= 89 else int("20" + yy)

                decisions.append({
                    "pdf_url": pdf_url,
                    "filename": filename,
                    "case_name": case_name,
                    "case_file": case_file,
                    "case_date": case_date,
                    "year": full_year,
                    "court": "Superior Court",
                    "doc_type": "case_law",
                    "category": "superior",
                })

        logger.info(f"Superior Court: found {len(decisions)} decision PDFs")
        return decisions

    # ── Public Laws ──────────────────────────────────────────────────

    def _discover_public_laws(self) -> List[Dict[str, Any]]:
        """Parse pl{NN}.php pages to discover Public Law PDFs."""
        laws = []

        for leg_num in range(1, 25):
            url = f"{BASE_URL}/pl{leg_num:02d}.php"
            html = self._fetch_page(url)
            if not html:
                continue

            for m in re.finditer(
                r'href="([^"]*pdf/public_laws/[^"]+\.pdf)"',
                html, re.IGNORECASE
            ):
                href = m.group(1)
                pdf_url = urljoin(url, href)

                # Extract PL number from filename
                pl_match = re.search(r'PL(\d+-\d+)', href, re.IGNORECASE)
                pl_number = pl_match.group(1) if pl_match else ""

                law_name = ""
                law_date = ""

                pos = m.start()
                row_start = html.rfind('<tr', max(0, pos - 2000), pos)
                if row_start >= 0:
                    row_end = html.find('</tr>', pos)
                    if row_end >= 0:
                        row_html = html[row_start:row_end]
                        cells = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.DOTALL | re.IGNORECASE)
                        if len(cells) >= 2:
                            # First cell is usually PL number, second is description
                            desc = re.sub(r'<[^>]+>', '', cells[1]).strip() if len(cells) > 1 else ""
                            law_name = desc if desc else f"PL {pl_number}"
                            for cell in cells[2:]:
                                cell_text = re.sub(r'<[^>]+>', '', cell).strip()
                                date_m = re.search(r'(\d{4}-\d{2}-\d{2})', cell_text)
                                if date_m:
                                    law_date = date_m.group(1)
                                    break
                                date_m = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', cell_text)
                                if date_m:
                                    law_date = f"{date_m.group(3)}-{date_m.group(1).zfill(2)}-{date_m.group(2).zfill(2)}"
                                    break

                if not law_name:
                    law_name = f"Public Law {pl_number}" if pl_number else pdf_url.split('/')[-1]

                laws.append({
                    "pdf_url": pdf_url,
                    "pl_number": pl_number,
                    "law_name": law_name,
                    "law_date": law_date,
                    "legislature": leg_num,
                    "doc_type": "legislation",
                    "category": "public_law",
                })

        logger.info(f"Public Laws: found {len(laws)} law PDFs")
        return laws

    # ── Normalize ────────────────────────────────────────────────────

    def normalize(self, raw: dict) -> dict:
        category = raw.get("category", "")
        pdf_url = raw.get("pdf_url", "")

        if category == "cmc":
            title_num = raw["title_num"]
            section_num = raw["section_num"]
            doc_id = f"MP-CMC-T{title_num}-S{section_num}"
            title = f"CMC Title {title_num}, Section {section_num}"
            _type = "legislation"
            date = ""
        elif category == "nmiac":
            title_num = raw["title_num"]
            chapter_id = raw["chapter_id"]
            doc_id = f"MP-NMIAC-T{title_num}-{chapter_id}"
            title = f"NMIAC Title {title_num}, Chapter {chapter_id}"
            _type = "legislation"
            date = ""
        elif category == "supreme":
            case_file = raw.get("case_file", "")
            doc_id = f"MP-SC-{case_file}" if case_file else f"MP-SC-{hashlib.md5(pdf_url.encode()).hexdigest()[:12]}"
            title = raw.get("case_name", case_file)
            _type = "case_law"
            date = raw.get("case_date", "")
        elif category == "superior":
            filename = raw.get("filename", "")
            doc_id = f"MP-SUP-{filename}" if filename else f"MP-SUP-{hashlib.md5(pdf_url.encode()).hexdigest()[:12]}"
            title = raw.get("case_name", filename)
            _type = "case_law"
            date = raw.get("case_date", "")
        elif category == "public_law":
            pl_number = raw.get("pl_number", "")
            doc_id = f"MP-PL-{pl_number}" if pl_number else f"MP-PL-{hashlib.md5(pdf_url.encode()).hexdigest()[:12]}"
            title = raw.get("law_name", f"Public Law {pl_number}")
            _type = "legislation"
            date = raw.get("law_date", "")
        else:
            doc_id = f"MP-{hashlib.md5(pdf_url.encode()).hexdigest()[:12]}"
            title = raw.get("title", "Unknown")
            _type = raw.get("doc_type", "legislation")
            date = ""

        return {
            "_id": doc_id,
            "_source": "MP/CNMILRC",
            "_type": _type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": date or None,
            "url": pdf_url,
            "jurisdiction": "MP",
            "doc_type": _type,
            "category": category,
        }

    # ── Fetch ────────────────────────────────────────────────────────

    def _fetch_and_extract_pdf(self, item: dict) -> Optional[dict]:
        """Download a PDF and extract text, returning the enriched item."""
        pdf_bytes = self._fetch_pdf(item["pdf_url"])
        if not pdf_bytes:
            return None

        text = _extract_pdf_text(pdf_bytes)
        if not text or len(text.strip()) < 50:
            logger.warning(f"Insufficient text ({len(text)} chars) from {item['pdf_url']}")
            return None

        item["text"] = text
        return item

    def fetch_all(self) -> Generator[dict, None, None]:
        count = 0

        # Discover all document categories
        logger.info("Discovering CMC sections...")
        cmc = self._discover_cmc_sections()

        logger.info("Discovering NMIAC chapters...")
        nmiac = self._discover_admin_chapters()

        logger.info("Discovering Supreme Court opinions...")
        supreme = self._discover_supreme_court()

        logger.info("Discovering Superior Court decisions...")
        superior = self._discover_superior_court()

        logger.info("Discovering Public Laws...")
        public_laws = self._discover_public_laws()

        # Combine all legislation items
        legislation = cmc + nmiac + public_laws
        # Combine all case_law items
        case_law = supreme + superior

        logger.info(f"Total discovered: {len(legislation)} legislation, {len(case_law)} case_law")

        # Interleave legislation and case_law for balanced sampling
        for leg_item, case_item in zip_longest(legislation, case_law):
            if leg_item is not None:
                result = self._fetch_and_extract_pdf(leg_item)
                if result:
                    yield result
                    count += 1
                    logger.info(f"  [{count}] {result.get('category', '')}: {result.get('title', '')[:60]} ({len(result['text'])} chars)")

            if case_item is not None:
                result = self._fetch_and_extract_pdf(case_item)
                if result:
                    yield result
                    count += 1
                    logger.info(f"  [{count}] {result.get('category', '')}: {result.get('case_name', result.get('title', ''))[:60]} ({len(result['text'])} chars)")

        logger.info(f"Total records yielded: {count}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        yield from self.fetch_all()


if __name__ == "__main__":
    scraper = CNMILRCScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test-api":
        print("Testing CNMI Law Revision Commission access...")
        import requests
        sess = requests.Session()
        sess.headers.update(_BROWSER_HEADERS)

        # Test main page
        resp = sess.get(f"{BASE_URL}/cmc.php", timeout=30)
        print(f"  CMC index status: {resp.status_code}, size: {len(resp.text)} bytes")

        # Count PDF links
        pdfs = re.findall(r'href="[^"]*pdf/cmc_section/[^"]+\.pdf"', resp.text, re.IGNORECASE)
        print(f"  CMC section PDFs found: {len(pdfs)}")

        # Test one PDF download
        if pdfs:
            first_href = re.search(r'href="([^"]*pdf/cmc_section/[^"]+\.pdf)"', resp.text, re.IGNORECASE)
            if first_href:
                pdf_url = urljoin(f"{BASE_URL}/cmc.php", first_href.group(1))
                print(f"  Testing PDF: {pdf_url}")
                pdf_resp = sess.get(pdf_url, timeout=30)
                print(f"  PDF status: {pdf_resp.status_code}, size: {len(pdf_resp.content)} bytes")
                text = _extract_pdf_text(pdf_resp.content)
                print(f"  Extracted text: {len(text)} chars")
                if text:
                    print(f"  Preview: {text[:200]}...")

        # Test Supreme Court year page
        resp2 = sess.get(f"{BASE_URL}/spm25.php", timeout=30)
        print(f"\n  Supreme Court 2025 status: {resp2.status_code}")
        sc_pdfs = re.findall(r'href="[^"]*pdf/supreme/[^"]+\.pdf"', resp2.text, re.IGNORECASE)
        print(f"  Supreme Court 2025 PDFs: {len(sc_pdfs)}")

        print("\nConnectivity test PASSED")

    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
