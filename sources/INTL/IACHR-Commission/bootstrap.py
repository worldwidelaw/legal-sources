#!/usr/bin/env python3
"""
INTL/IACHR-Commission -- Inter-American Commission on Human Rights Reports

Fetches IACHR reports, admissibility/inadmissibility decisions, friendly
settlements, and resolutions with full text extracted from DOCX files.

Strategy:
  - Scrape year-based listing pages at oas.org/en/iachr/decisions/
  - Parse HTML for document links and metadata (report number, case, country)
  - Download DOCX files and extract full text via stdlib zipfile + xml
  - Fall back to PDF link if no DOCX available (text will be empty, PDF URL saved)

Data:
  - ~2,600+ documents (2000-present)
  - Types: merits reports, admissibility, inadmissibility, friendly settlements,
    resolutions, thematic reports, country reports
  - No authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent documents
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import logging
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List, Tuple
from html import unescape

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.IACHR-Commission")

BASE_URL = "https://www.oas.org"

# Document type configurations: (slug, label, year_range, language_prefix)
DOC_TYPES = [
    ("merits", "Merits Report", range(2014, 2027), "/en/iachr/decisions/pc/merits.asp"),
    ("admissibility", "Admissibility Decision", range(2006, 2027), "/en/iachr/decisions/pc/admissibilities.asp"),
    ("inadmissibility", "Inadmissibility Decision", range(2014, 2027), "/en/iachr/decisions/pc/inadmissibilities.asp"),
    ("friendly", "Friendly Settlement", range(2000, 2027), "/en/iachr/decisions/pc/friendly.asp"),
]

# Non-year-based pages
STATIC_PAGES = [
    ("resolution", "Resolution", "/en/iachr/decisions/resolutions.asp"),
    ("thematic_report", "Thematic Report", "/en/iachr/reports/thematic.asp"),
    ("country_report", "Country Report", "/en/iachr/reports/country.asp"),
]


class IACHRCommissionScraper(BaseScraper):
    """Scraper for INTL/IACHR-Commission."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        })

    def _get_page(self, url: str) -> str:
        """Fetch a page with rate limiting and retries."""
        self.rate_limiter.wait()
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=60)
                resp.raise_for_status()
                # Handle iso-8859-1 encoding
                if 'charset=iso-8859-1' in resp.headers.get('content-type', '').lower():
                    resp.encoding = 'iso-8859-1'
                elif resp.encoding is None or resp.encoding == 'ISO-8859-1':
                    resp.encoding = 'utf-8'
                return resp.text
            except requests.RequestException as e:
                if attempt < 2:
                    logger.warning("Retry %d for %s: %s", attempt + 1, url, e)
                    import time
                    time.sleep(3)
                else:
                    raise
        return ""

    def _extract_text_from_docx(self, content: bytes) -> str:
        """Extract text from DOCX file bytes using stdlib only."""
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                if 'word/document.xml' not in zf.namelist():
                    return ""
                xml_data = zf.read('word/document.xml')
                root = ET.fromstring(xml_data)
                # Namespace for Word XML
                ns = {'w': 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}
                paragraphs = []
                for p in root.iter('{http://schemas.openxmlformats.org/wordprocessingml/2006/main}p'):
                    texts = []
                    for t in p.iter('{http://schemas.openxmlformats.org/wordprocessingml/2006/main}t'):
                        if t.text:
                            texts.append(t.text)
                    if texts:
                        paragraphs.append(''.join(texts))
                return '\n\n'.join(paragraphs)
        except (zipfile.BadZipFile, ET.ParseError, KeyError) as e:
            logger.warning("DOCX extraction failed: %s", e)
            return ""

    def _download_docx_text(self, url: str) -> str:
        """Download a DOCX file and extract text."""
        self.rate_limiter.wait()
        try:
            resp = self.session.get(url, timeout=90)
            if resp.status_code == 200:
                text = self._extract_text_from_docx(resp.content)
                if text and len(text) > 100:
                    return text
        except requests.RequestException as e:
            logger.warning("Failed to download DOCX %s: %s", url, e)
        return ""

    def _parse_year_listing(self, html: str, doc_type: str, year: int) -> List[Dict[str, Any]]:
        """Parse a year-based listing page and extract document entries.

        HTML structure (observed):
          <p style='...'>CountryName</p>
          <ul><li style='float:none; ...'>
            <a href='...docx' ...><i class='far fa-file-word'></i></a>
            <a href='...PDF' ...>Report No. X/YY</a>, Case/Petition NNN, Title text
          </li></ul>
        """
        docs = []
        current_country = ""

        # Track country headings: <p style='background-color:#f2f2f2...'>CountryName</p>
        # Split HTML into segments by country headers
        # Process line by line through the HTML

        # First, find all country headers and their positions
        country_positions = []
        for m in re.finditer(
            r"<p[^>]*background-color:\s*#f2f2f2[^>]*>\s*(.+?)\s*</p>",
            html, re.IGNORECASE | re.DOTALL
        ):
            name = re.sub(r'<[^>]+>', '', m.group(1)).strip()
            country_positions.append((m.start(), name))

        # Find all <li> entries with document links
        li_pattern = re.compile(
            r"<li[^>]*style='float:none[^']*'[^>]*>(.*?)</li>",
            re.DOTALL | re.IGNORECASE
        )

        for li_match in li_pattern.finditer(html):
            entry = li_match.group(1)
            pos = li_match.start()

            # Determine country from nearest preceding country header
            country = ""
            for cp, cname in reversed(country_positions):
                if cp < pos:
                    country = cname
                    break

            # Extract DOCX and PDF links
            docx_url = None
            pdf_url = None
            link_text = ""

            for href_match in re.finditer(r"href='([^']+)'", entry):
                href = href_match.group(1)
                if href.lower().endswith('.docx'):
                    docx_url = BASE_URL + href if not href.startswith('http') else href
                elif href.lower().endswith('.pdf'):
                    pdf_url = BASE_URL + href if not href.startswith('http') else href

            if not docx_url and not pdf_url:
                continue

            # Extract report number from PDF link text: <a href='...PDF'>Report No. X/YY</a>
            report_match = re.search(
                r"<a[^>]+\.PDF'[^>]*>([^<]+)</a>",
                entry, re.IGNORECASE
            )
            report_number = report_match.group(1).strip() if report_match else ""

            # Extract case/petition number and title from remaining text
            clean_text = re.sub(r'<[^>]+>', '', entry).strip()
            clean_text = unescape(clean_text)
            clean_text = re.sub(r'\s+', ' ', clean_text)

            case_number = ""
            case_match = re.search(r'(?:Case|Caso)\s+(\d+[\.\-]\d+)', clean_text)
            petition_match = re.search(r'(?:Petition|Petici[oó]n)\s+(\d+[\-]\d+)', clean_text)
            if case_match:
                case_number = case_match.group(0)
            elif petition_match:
                case_number = petition_match.group(0)

            # Title: everything after report number and case/petition
            # Format: "Report No. X/YY, Case NNN, Actual Title"
            title_text = clean_text
            # Remove report number prefix
            if report_number:
                title_text = title_text.replace(report_number, '', 1)
            title_text = re.sub(r'^[\s,]+', '', title_text)

            # Build title
            title_parts = []
            if report_number:
                title_parts.append(report_number)
            if case_number:
                title_parts.append(case_number)
            # Add remaining title text (party names etc)
            remaining = title_text
            if case_number:
                remaining = remaining.replace(case_number, '', 1)
            remaining = re.sub(r'^[\s,]+', '', remaining).strip()
            if remaining:
                title_parts.append(remaining)

            title = ", ".join(title_parts) if title_parts else f"IACHR {doc_type} {year}"

            doc = {
                "doc_type": doc_type,
                "report_number": report_number,
                "case_number": case_number,
                "country": country,
                "year": year,
                "title": title,
                "docx_url": docx_url,
                "pdf_url": pdf_url,
                "source_url": "",
            }
            docs.append(doc)

        return docs

    def _parse_static_listing(self, html: str, doc_type: str) -> List[Dict[str, Any]]:
        """Parse a non-year-based listing page (resolutions, thematic/country reports)."""
        docs = []

        # Find <li> entries with document links
        li_pattern = re.compile(r"<li[^>]*>(.*?)</li>", re.DOTALL | re.IGNORECASE)

        for li_match in li_pattern.finditer(html):
            entry = li_match.group(1)

            docx_url = None
            pdf_url = None

            for href_match in re.finditer(r"""href=['"]([^'"]+)['"]""", entry):
                href = href_match.group(1)
                if href.lower().endswith('.docx'):
                    docx_url = BASE_URL + href if not href.startswith('http') else href
                elif href.lower().endswith('.pdf'):
                    pdf_url = BASE_URL + href if not href.startswith('http') else href

            if not docx_url and not pdf_url:
                continue

            year_match = re.search(r'/(\d{4})/', (docx_url or pdf_url or ""))
            year = int(year_match.group(1)) if year_match else 0

            # Get title from link text
            link_texts = re.findall(r'<a[^>]+>([^<]+)</a>', entry)
            title = ""
            for lt in link_texts:
                lt = lt.strip()
                if lt and len(lt) > 5:
                    title = lt
                    break
            if not title:
                clean = re.sub(r'<[^>]+>', ' ', entry).strip()
                clean = unescape(clean)
                title = re.sub(r'\s+', ' ', clean)[:200]
            if not title or len(title) < 5:
                continue

            doc = {
                "doc_type": doc_type,
                "report_number": "",
                "case_number": "",
                "country": "",
                "year": year,
                "title": title,
                "docx_url": docx_url,
                "pdf_url": pdf_url,
                "source_url": "",
            }
            docs.append(doc)

        return docs

    def _fetch_listing_docs(self, sample_mode: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Iterate all listing pages and yield raw document dicts."""
        count = 0
        sample_limit = 15 if sample_mode else float('inf')

        # Year-based pages
        for doc_type_slug, doc_type_label, years, path in DOC_TYPES:
            for year in years:
                if count >= sample_limit:
                    return
                url = f"{BASE_URL}{path}?Year={year}"
                logger.info("Fetching %s year=%d: %s", doc_type_slug, year, url)
                try:
                    html = self._get_page(url)
                except Exception as e:
                    logger.error("Failed to fetch %s: %s", url, e)
                    continue

                docs = self._parse_year_listing(html, doc_type_slug, year)
                logger.info("  Found %d entries for %s/%d", len(docs), doc_type_slug, year)

                for doc in docs:
                    if count >= sample_limit:
                        return
                    doc["source_url"] = url
                    yield doc
                    count += 1

        # Static pages (only if not in sample mode or still under limit)
        if count < sample_limit:
            for doc_type_slug, doc_type_label, path in STATIC_PAGES:
                if count >= sample_limit:
                    return
                url = f"{BASE_URL}{path}"
                logger.info("Fetching %s: %s", doc_type_slug, url)
                try:
                    html = self._get_page(url)
                except Exception as e:
                    logger.error("Failed to fetch %s: %s", url, e)
                    continue

                docs = self._parse_static_listing(html, doc_type_slug)
                logger.info("  Found %d entries for %s", len(docs), doc_type_slug)

                for doc in docs:
                    if count >= sample_limit:
                        return
                    doc["source_url"] = url
                    yield doc
                    count += 1

    def _get_full_text(self, raw_doc: dict) -> str:
        """Get full text from DOCX (preferred) or PDF fallback."""
        text = ""
        if raw_doc.get("docx_url"):
            logger.info("Downloading DOCX: %s", raw_doc["docx_url"])
            text = self._download_docx_text(raw_doc["docx_url"])

        if not text and raw_doc.get("pdf_url"):
            logger.info("DOCX empty/missing, trying PDF: %s", raw_doc["pdf_url"])
            doc_id = raw_doc.get("title", "")[:80].replace("/", "-")
            text = extract_pdf_markdown(
                source="INTL/IACHR-Commission",
                source_id=doc_id,
                pdf_url=raw_doc["pdf_url"],
                table="doctrine",
                force=True,
            ) or ""
        return text

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all IACHR documents with full text."""
        for raw_doc in self._fetch_listing_docs(sample_mode=False):
            raw_doc["text"] = self._get_full_text(raw_doc)
            yield raw_doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield recently added documents (current year only)."""
        current_year = datetime.now().year
        count = 0

        for doc_type_slug, doc_type_label, years, path in DOC_TYPES:
            if current_year not in years:
                continue
            url = f"{BASE_URL}{path}?Year={current_year}"
            try:
                html = self._get_page(url)
            except Exception:
                continue

            docs = self._parse_year_listing(html, doc_type_slug, current_year)
            for doc in docs:
                doc["source_url"] = url
                doc["text"] = self._get_full_text(doc)
                yield doc
                count += 1

        logger.info("Update yielded %d documents for %d", count, current_year)

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        # Build a unique ID
        id_parts = [raw.get("doc_type", "unknown")]
        if raw.get("report_number"):
            id_parts.append(raw["report_number"].replace(" ", "").replace("/", "-"))
        elif raw.get("case_number"):
            id_parts.append(raw["case_number"].replace(" ", "").replace("/", "-"))
        else:
            # Use hash of title as fallback
            import hashlib
            id_parts.append(hashlib.md5(raw.get("title", "").encode()).hexdigest()[:12])

        _id = "IACHR-" + "-".join(id_parts)

        # Date: use year if available
        date_str = None
        if raw.get("year") and raw["year"] > 0:
            date_str = f"{raw['year']}-01-01"

        # URL: prefer DOCX, then PDF, then source page
        url = raw.get("docx_url") or raw.get("pdf_url") or raw.get("source_url", "")

        return {
            "_id": _id,
            "_source": "INTL/IACHR-Commission",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", "").strip(),
            "text": raw.get("text", ""),
            "date": date_str,
            "url": url,
            "report_number": raw.get("report_number", ""),
            "case_number": raw.get("case_number", ""),
            "country": raw.get("country", ""),
            "doc_type": raw.get("doc_type", ""),
            "pdf_url": raw.get("pdf_url", ""),
            "docx_url": raw.get("docx_url", ""),
        }


    def test_connection(self) -> bool:
        """Quick connectivity test."""
        try:
            url = f"{BASE_URL}/en/iachr/decisions/pc/merits.asp?Year=2024"
            html = self._get_page(url)
            docs = self._parse_year_listing(html, "merits", 2024)
            logger.info("Connection OK: %d merits entries for 2024", len(docs))
            return len(docs) > 0
        except Exception as e:
            logger.error("Connection test failed: %s", e)
            return False

def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="INTL/IACHR-Commission Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = IACHRCommissionScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
