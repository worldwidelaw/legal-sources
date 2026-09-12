#!/usr/bin/env python3
"""
INTL/UNCITRAL-TransparencyRegistry -- Investor-State Arbitration Documents

Fetches arbitration documents from the UNCITRAL Transparency Registry.

Strategy:
  - Scrape search pages to get all case URLs (~33 cases)
  - Scrape each case detail page for metadata + PDF document links
  - Download PDFs and extract text via pdfplumber
  - One record per document (award, decision, pleading, etc.)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import html as html_mod
import re
import sys
import json
import time
import logging
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html.parser import HTMLParser

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.UNCITRAL-TransparencyRegistry")

BASE_URL = "https://www.uncitral.org"
SEARCH_URL = f"{BASE_URL}/transparency-registry/registry/search.jspx"


class SimpleHTMLExtractor(HTMLParser):
    """Minimal HTML parser to extract links and text from registry pages."""

    def __init__(self):
        super().__init__()
        self.links = []
        self.current_tag = None
        self.current_attrs = {}
        self.text_parts = []
        self.in_target = False

    def handle_starttag(self, tag, attrs):
        self.current_tag = tag
        self.current_attrs = dict(attrs)
        if tag == "a":
            href = self.current_attrs.get("href", "")
            self.links.append({"href": href, "text": ""})
            self.in_target = True

    def handle_endtag(self, tag):
        if tag == "a":
            self.in_target = False
        self.current_tag = None

    def handle_data(self, data):
        if self.in_target and self.links:
            self.links[-1]["text"] += data.strip()
        self.text_parts.append(data)


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber with pypdf fallback."""
    try:
        import pdfplumber
        import io
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            if pages:
                return "\n\n".join(pages)
    except Exception as e:
        logger.debug(f"pdfplumber failed: {e}")

    try:
        import PyPDF2
        import io
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        pages = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                pages.append(text)
            try:
                page.flush_cache(); page.get_textmap.cache_clear()
            except Exception:
                pass
        if pages:
            return "\n\n".join(pages)
    except Exception as e:
        logger.debug(f"PyPDF2 failed: {e}")

    return ""


class UNCITRALTransparencyRegistryScraper(BaseScraper):
    """
    Scraper for INTL/UNCITRAL-TransparencyRegistry.
    Country: INTL
    URL: https://www.uncitral.org/transparency-registry/registry/search.jspx

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    def _get_case_urls(self) -> list[str]:
        """Scrape search pages to collect all case detail page URLs."""
        all_urls = []
        start = 0

        while True:
            url = f"{SEARCH_URL}?start={start}" if start > 0 else SEARCH_URL
            logger.info(f"Fetching search page: start={start}")
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            html = resp.text

            # Case URLs appear in onClick and debug elements as /registry/data/{country}/{slug}.html
            case_pattern = r'/registry/data/[a-z]{2,3}/[^"<>\s]+\.html'
            matches = re.findall(case_pattern, html)

            new_urls = []
            for path in matches:
                full_url = BASE_URL + "/transparency-registry" + path
                if full_url not in all_urls and full_url not in new_urls:
                    new_urls.append(full_url)

            if not new_urls:
                break

            all_urls.extend(new_urls)
            start += 10
            time.sleep(1.5)

            # Safety: don't loop forever
            if start > 100:
                break

        logger.info(f"Found {len(all_urls)} case URLs")
        return all_urls

    def _parse_case_page(self, case_url: str) -> dict:
        """Parse a case detail page to extract metadata and document list."""
        logger.info(f"Fetching case: {case_url}")
        resp = self.session.get(case_url, timeout=30)
        resp.raise_for_status()
        html = resp.text

        # Extract case title from h1 (may contain <img> tags)
        title_match = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
        case_name = re.sub(r'<[^>]+>', '', title_match.group(1)).strip() if title_match else "Unknown Case"

        # Extract metadata from wdivlabel/wdivvalue pattern
        metadata = {}
        meta_items = re.findall(
            r'<span class="wdivlabel">([^<]+)</span><span class="wdivvalue">(.*?)</span></span></div>',
            html, re.DOTALL
        )
        for label, val in meta_items:
            val_text = re.sub(r'<[^>]+>', '', val).strip()
            label_lower = label.strip().lower()
            if "treaty" in label_lower:
                metadata["treaty"] = val_text
            elif "sector" in label_lower:
                metadata["sector"] = val_text
            elif "status" in label_lower:
                metadata["status"] = val_text
            elif "remark" in label_lower:
                metadata["remarks"] = val_text

        # Arbitration rules (appears as plain text after the metadata divs)
        rules_match = re.search(r'UNCITRAL\s+Arbitration\s+Rules[^<]*', html)
        if rules_match:
            metadata["arbitration_rules"] = rules_match.group(0).strip()

        # Respondent code from URL
        resp_match = re.search(r'/data/([a-z]{2,3})/', case_url)
        if resp_match:
            metadata["respondent_code"] = resp_match.group(1).upper()

        # Extract documents from the downloads table
        documents = []
        table_match = re.search(r'<table[^>]*id="downloads">(.*?)</table>', html, re.DOTALL)
        if table_match:
            rows = re.findall(r'<tr>(.*?)</tr>', table_match.group(1), re.DOTALL)
            for row in rows:
                pdf_match = re.search(r'href="([^"]+\.pdf)"', row)
                if not pdf_match:
                    continue
                pdf_href = pdf_match.group(1)
                if not pdf_href.startswith("http"):
                    pdf_href = BASE_URL + pdf_href

                cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
                cells_text = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]

                doc_info = {
                    "pdf_url": pdf_href,
                    "filename": cells_text[0] if len(cells_text) > 0 else "",
                    "title": cells_text[1] if len(cells_text) > 1 else "",
                    "doc_type": cells_text[2] if len(cells_text) > 2 else "",
                    "language": cells_text[3] if len(cells_text) > 3 else "",
                    "date": cells_text[4] if len(cells_text) > 4 else "",
                }
                documents.append(doc_info)

        # Fallback: find PDF links anywhere on page
        if not documents:
            pdf_links = re.findall(r'href="([^"]+\.pdf)"', html)
            for pdf_path in pdf_links:
                pdf_url = pdf_path if pdf_path.startswith("http") else BASE_URL + pdf_path
                fname = pdf_path.split("/")[-1]
                documents.append({
                    "pdf_url": pdf_url,
                    "filename": fname,
                    "title": fname.replace(".pdf", "").replace("_", " ").replace("-", " "),
                    "doc_type": "",
                    "language": "English",
                    "date": "",
                })

        return {
            "case_name": case_name,
            "case_url": case_url,
            "metadata": metadata,
            "documents": documents,
        }

    def _download_pdf_text(self, pdf_url: str) -> str:
        """Download a PDF and extract its text content."""
        try:
            logger.info(f"Downloading PDF: {pdf_url}")
            resp = self.session.get(pdf_url, timeout=60)
            resp.raise_for_status()

            if len(resp.content) < 100:
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {pdf_url}")
                return ""

            text = extract_pdf_text(resp.content)
            if text:
                logger.info(f"Extracted {len(text)} chars from {pdf_url}")
            else:
                logger.warning(f"No text extracted from {pdf_url}")
            return text

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download PDF {pdf_url}: {e}")
            return ""

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse various date formats to ISO 8601."""
        if not date_str:
            return None

        date_str = date_str.strip()

        # Try YYYY-MM-DD
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            return date_str

        # Try DD/MM/YYYY or DD-MM-YYYY
        m = re.match(r'^(\d{1,2})[/-](\d{1,2})[/-](\d{4})$', date_str)
        if m:
            day, month, year = m.groups()
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

        # Try "Month DD, YYYY" or "DD Month YYYY"
        months = {
            'january': '01', 'february': '02', 'march': '03', 'april': '04',
            'may': '05', 'june': '06', 'july': '07', 'august': '08',
            'september': '09', 'october': '10', 'november': '11', 'december': '12',
        }
        for mname, mnum in months.items():
            if mname in date_str.lower():
                # Try "DD Month YYYY"
                m2 = re.search(r'(\d{1,2})\s+' + mname + r'\s+(\d{4})', date_str, re.IGNORECASE)
                if m2:
                    return f"{m2.group(2)}-{mnum}-{m2.group(1).zfill(2)}"
                # Try "Month DD, YYYY"
                m3 = re.search(mname + r'\s+(\d{1,2}),?\s+(\d{4})', date_str, re.IGNORECASE)
                if m3:
                    return f"{m3.group(2)}-{mnum}-{m3.group(1).zfill(2)}"
                break

        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents from the Transparency Registry."""
        case_urls = self._get_case_urls()

        for case_url in case_urls:
            time.sleep(1.5)
            try:
                case_data = self._parse_case_page(case_url)
            except Exception as e:
                logger.error(f"Failed to parse case {case_url}: {e}")
                continue

            if not case_data["documents"]:
                logger.info(f"No documents for: {case_data['case_name']}")
                continue

            for doc in case_data["documents"]:
                time.sleep(1.5)
                text = self._download_pdf_text(doc["pdf_url"])
                if not text:
                    continue

                yield {
                    "case_name": case_data["case_name"],
                    "case_url": case_data["case_url"],
                    "metadata": case_data["metadata"],
                    "doc_title": doc["title"],
                    "doc_type": doc["doc_type"],
                    "doc_language": doc["language"],
                    "doc_date": doc["date"],
                    "pdf_url": doc["pdf_url"],
                    "text": text,
                }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch updates — re-runs full fetch for this small dataset."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        case_name = html_mod.unescape(raw.get("case_name", "Unknown"))
        doc_title = html_mod.unescape(raw.get("doc_title", ""))
        doc_type = raw.get("doc_type", "")
        metadata = raw.get("metadata", {})

        # Build a meaningful title
        if doc_title:
            title = f"{case_name} — {doc_title}"
        else:
            title = case_name

        # Parse date
        date = self._parse_date(raw.get("doc_date", ""))

        # Build unique ID from PDF URL
        pdf_url = raw.get("pdf_url", "")
        doc_id = pdf_url.split("/")[-1].replace(".pdf", "") if pdf_url else case_name

        return {
            "_id": f"uncitral-tr-{doc_id}",
            "_source": "INTL/UNCITRAL-TransparencyRegistry",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": date,
            "url": raw.get("case_url", ""),
            "pdf_url": pdf_url,
            "case_name": case_name,
            "doc_type": doc_type,
            "doc_language": raw.get("doc_language", ""),
            "treaty": metadata.get("treaty", ""),
            "sector": metadata.get("sector", ""),
            "status": metadata.get("status", ""),
            "arbitration_rules": metadata.get("arbitration_rules", ""),
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = UNCITRALTransparencyRegistryScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        print("Testing connectivity...")
        resp = scraper.session.get(SEARCH_URL, timeout=30)
        print(f"Status: {resp.status_code}")
        print(f"Content length: {len(resp.text)}")
        case_urls = scraper._get_case_urls()
        print(f"Found {len(case_urls)} cases")
        sys.exit(0)

    if command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=sample_mode)
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.bootstrap(sample_mode=False)
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
