#!/usr/bin/env python3
"""
US/NY-DTF -- New York Department of Taxation and Finance Advisory Opinions

Fetches advisory opinions (TSB-A) with full text from tax.ny.gov.
Organized by 17 tax type categories, spanning 1980-present.

Strategy:
  1. Parse the main advisory opinions index to discover tax type pages
  2. For each tax type, fetch the main page and year archive pages
  3. Extract individual opinion links from each page
  4. For HTML opinions (2020+): extract text from page content
  5. For PDF opinions (pre-2020): download and extract text via pdfplumber

Data: Public domain (NYS government tax guidance). No auth required.
Crawl-delay: 1 second between requests.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample opinions
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import io
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NY-DTF")

BASE_URL = "https://www.tax.ny.gov"

# Tax type slugs and their index pages
TAX_TYPES = [
    {"slug": "sales", "name": "Sales Tax", "page": "sales_ao.htm"},
    {"slug": "income", "name": "Income Tax", "page": "income_ao.htm"},
    {"slug": "corporation", "name": "Corporation Tax", "page": "corp_ao.htm"},
    {"slug": "estate", "name": "Estate Tax", "page": "estate_ao.htm"},
    {"slug": "mortgage", "name": "Mortgage Recording Tax", "page": "mortgage_ao.htm"},
    {"slug": "real_estate_transfer", "name": "Real Estate Transfer Tax", "page": "ret_ao.htm"},
    {"slug": "rptg", "name": "Real Property Transfer Gains Tax", "page": "rptg_ao.htm"},
    {"slug": "alcoholic_bev", "name": "Alcoholic Beverages Tax", "page": "alc_bev_ao.htm"},
    {"slug": "boxing", "name": "Boxing/Wrestling Tax", "page": "boxing_ao.htm"},
    {"slug": "cigarette", "name": "Cigarette Tax", "page": "cig_ao.htm"},
    {"slug": "fuel_use", "name": "Fuel Use Tax", "page": "fuel_ao.htm"},
    {"slug": "gasoline", "name": "Gasoline Tax", "page": "gas_ao.htm"},
    {"slug": "gift", "name": "Gift Tax", "page": "gift_ao.htm"},
    {"slug": "highway_use", "name": "Highway Use Tax", "page": "highway_ao.htm"},
    {"slug": "mctmt", "name": "MCTMT", "page": "mctmt_ao.htm"},
    {"slug": "petroleum", "name": "Petroleum Business Tax", "page": "pet_ao.htm"},
    {"slug": "stock_transfer", "name": "Stock Transfer Tax", "page": "stock_ao.htm"},
    {"slug": "multitax", "name": "Multiple Tax Types", "page": "multitax_ao.htm"},
]

# Known sample URLs for --sample mode (mix of HTML and PDF)
SAMPLE_OPINIONS = [
    # HTML opinions (recent)
    {"url": "/pubs_and_bulls/advisory_opinions/sales/24-3s.htm", "tax_type": "sales"},
    {"url": "/pubs_and_bulls/advisory_opinions/sales/24-2s.htm", "tax_type": "sales"},
    {"url": "/pubs_and_bulls/advisory_opinions/income/21-1i.htm", "tax_type": "income"},
    # PDF opinions (older — correct directory and separator patterns)
    {"url": "/pdf/advisory_opinions/sales/a18_1s.pdf", "tax_type": "sales"},
    {"url": "/pdf/advisory_opinions/income/a17_1i.pdf", "tax_type": "income"},
    {"url": "/pdf/advisory_opinions/sales/a10_1s.pdf", "tax_type": "sales"},
    {"url": "/pdf/advisory_opinions/sales/a05_1s.pdf", "tax_type": "sales"},
    {"url": "/pdf/advisory_opinions/corporation/a15_1c.pdf", "tax_type": "corporation"},
    {"url": "/pdf/advisory_opinions/income/a00_1i.pdf", "tax_type": "income"},
    {"url": "/pdf/advisory_opinions/sales/a95_1s.pdf", "tax_type": "sales"},
    {"url": "/pdf/advisory_opinions/sales/a90_1s.pdf", "tax_type": "sales"},
    {"url": "/pdf/advisory_opinions/sales/a19-1s.pdf", "tax_type": "sales"},
    {"url": "/pdf/advisory_opinions/sales/a20-1s.pdf", "tax_type": "sales"},
    {"url": "/pdf/advisory_opinions/sales/a15_1s.pdf", "tax_type": "sales"},
    {"url": "/pdf/advisory_opinions/mortgage/a10_1m.pdf", "tax_type": "mortgage"},
]


def strip_html(html_text: str) -> str:
    """Strip HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'</p>', '\n\n', text)
    text = re.sub(r'</div>', '\n\n', text)
    text = re.sub(r'</li>', '\n', text)
    text = re.sub(r'</tr>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="US/NY-DTF",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="doctrine",
    ) or ""

class NYDTFScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (open-data research project; +https://github.com/worldwidelaw/legal-sources)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=60,
        )
        self.delay = 1.0

    def _get(self, url: str, binary: bool = False, retries: int = 2):
        """Fetch URL with rate limiting."""
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp.content if binary else resp.text
                if resp.status_code == 404:
                    logger.debug(f"404: {url}")
                    return b"" if binary else ""
                if resp.status_code == 429:
                    wait = min(30, self.delay * (attempt + 2))
                    logger.warning(f"Rate limited, waiting {wait}s")
                    time.sleep(wait)
                    continue
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url}: {e}")
                if attempt < retries:
                    time.sleep(3)
        return b"" if binary else ""

    def test_api(self):
        """Test connectivity to tax.ny.gov."""
        logger.info("Testing NY DTF advisory opinions...")
        try:
            html = self._get(f"{BASE_URL}/pubs_and_bulls/advisory_opinions/ao_tax_types.htm")
            if "advisory" in html.lower():
                logger.info("  Advisory opinions index: OK")
            else:
                logger.error("  Advisory opinions index: unexpected content")
                return False

            # Test an HTML opinion
            html = self._get(f"{BASE_URL}/pubs_and_bulls/advisory_opinions/sales/24-3s.htm")
            if html and len(html) > 500:
                logger.info("  HTML opinion page: OK")
            else:
                logger.warning("  HTML opinion page: may not be accessible")

            # Test a PDF opinion
            pdf = self._get(f"{BASE_URL}/pdf/advisory_opinions/sales/a19_1s.pdf", binary=True)
            if pdf and len(pdf) > 1000:
                logger.info("  PDF opinion: OK")
            else:
                logger.warning("  PDF opinion: may not be accessible")

            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def discover_opinions_from_page(self, page_url: str, tax_type: dict) -> list:
        """Extract opinion links from an index/archive page."""
        html = self._get(page_url)
        if not html:
            return []

        opinions = []
        seen = set()

        # Match links to HTML opinions (e.g., 24-3s.htm, 23-1i.htm)
        for m in re.finditer(r'href="([^"]*?(\d{2}-\d{1,3}[a-zA-Z]?)\.htm)"', html):
            href = m.group(1)
            doc_id = m.group(2)
            if doc_id not in seen:
                seen.add(doc_id)
                full_url = urljoin(page_url, href)
                opinions.append({
                    "url": full_url,
                    "doc_id": doc_id,
                    "format": "html",
                    "tax_type": tax_type["slug"],
                    "tax_type_name": tax_type["name"],
                })

        # Match links to PDF opinions (e.g., a19_1s.pdf, a00_1i.pdf)
        for m in re.finditer(r'href="([^"]*?/a(\d{2}[_-]\d{1,3}[a-zA-Z]?)\.pdf)"', html):
            href = m.group(1)
            doc_id = m.group(2)
            if doc_id not in seen:
                seen.add(doc_id)
                full_url = urljoin(page_url, href)
                opinions.append({
                    "url": full_url,
                    "doc_id": doc_id,
                    "format": "pdf",
                    "tax_type": tax_type["slug"],
                    "tax_type_name": tax_type["name"],
                })

        return opinions

    def discover_all_opinions(self, tax_type: dict) -> list:
        """Discover all opinions for a tax type, including year archives."""
        base_path = "/pubs_and_bulls/advisory_opinions/"
        page_name = tax_type["page"]
        main_url = f"{BASE_URL}{base_path}{page_name}"

        # Fetch main page
        all_opinions = self.discover_opinions_from_page(main_url, tax_type)
        logger.info(f"  {tax_type['name']}: {len(all_opinions)} opinions from main page")

        # Discover year archive links from the main page
        html = self._get(main_url)
        if html:
            stem = page_name.replace(".htm", "")
            # Match year archive links like sales_ao_2019.htm or sales-ao-2020.htm
            year_pages = set()
            for m in re.finditer(
                r'href="([^"]*?' + re.escape(stem).replace(r'\_', '[_-]') + r'[_-](\d{4})\.htm)"',
                html
            ):
                year_pages.add(m.group(1))
            # Also try broader pattern
            for m in re.finditer(
                r'href="([^"]*?[_-]ao[_-](\d{4})\.htm)"', html
            ):
                year_pages.add(m.group(1))

            for year_href in sorted(year_pages):
                year_url = urljoin(main_url, year_href)
                year_opinions = self.discover_opinions_from_page(year_url, tax_type)
                # Merge, avoiding duplicates
                existing_ids = {o["doc_id"] for o in all_opinions}
                for op in year_opinions:
                    if op["doc_id"] not in existing_ids:
                        all_opinions.append(op)
                        existing_ids.add(op["doc_id"])

        return all_opinions

    def fetch_html_opinion(self, url: str) -> dict:
        """Fetch an HTML advisory opinion and extract content."""
        html = self._get(url)
        if not html or len(html) < 200:
            return None

        # Extract title
        title = ""
        title_match = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
        if title_match:
            title = strip_html(title_match.group(1))
        if not title:
            title_match = re.search(r'<title>(.*?)</title>', html, re.DOTALL)
            if title_match:
                title = strip_html(title_match.group(1)).split("|")[0].strip()

        # Extract date
        date = ""
        date_match = re.search(
            r'(?:Date\s+Issued|Issued|Date)[:\s]*([A-Z][a-z]+ \d{1,2},?\s*\d{4})',
            html, re.IGNORECASE
        )
        if date_match:
            try:
                raw_date = date_match.group(1).replace(",", "")
                dt = datetime.strptime(raw_date, "%B %d %Y")
                date = dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Extract main content
        text = ""
        # Try to find the main content area
        for marker in ['id="mainContent"', 'id="content"', 'class="field-item"',
                       'id="block-system-main"', '<main', 'role="main"']:
            idx = html.find(marker)
            if idx > 0:
                content = html[idx:]
                # Cut before footer/nav
                for end_marker in ['id="footer"', '</main>', 'class="footer"',
                                   'id="sidebar"', 'class="backToTop"']:
                    end_idx = content.find(end_marker)
                    if end_idx > 0:
                        content = content[:end_idx]
                        break
                text = strip_html(content)
                if len(text) > 100:
                    break

        # Fallback: everything between </header> and <footer
        if len(text) < 100:
            header_end = html.find('</header>')
            footer_start = html.find('<footer')
            if header_end > 0 and footer_start > header_end:
                text = strip_html(html[header_end:footer_start])

        if not text or len(text) < 50:
            return None

        # Clean boilerplate
        text = re.sub(r'Skip to content.*?\n', '', text)
        text = re.sub(r'NYS Tax Department.*?\n', '', text)
        text = re.sub(r'Updated:\s*\n', '', text)
        text = re.sub(r'\n{3,}', '\n\n', text).strip()

        return {
            "title": title,
            "text": text,
            "date": date,
            "url": url,
        }

    def fetch_pdf_opinion(self, url: str) -> dict:
        """Fetch a PDF advisory opinion and extract text."""
        pdf_bytes = self._get(url, binary=True)
        if not pdf_bytes or len(pdf_bytes) < 500:
            return None

        text = extract_pdf_text(pdf_bytes)
        if not text or len(text) < 50:
            return None

        # Try to extract title from first lines
        lines = text.split('\n')
        title = ""
        for line in lines[:10]:
            line = line.strip()
            if line and len(line) > 10 and not line.startswith('New York'):
                title = line
                break
        if not title:
            title = lines[0].strip() if lines else ""

        # Try to extract date
        date = ""
        date_match = re.search(
            r'(?:Date\s+Issued|Issued|Date)[:\s]*([A-Z][a-z]+ \d{1,2},?\s*\d{4})',
            text, re.IGNORECASE
        )
        if date_match:
            try:
                raw_date = date_match.group(1).replace(",", "")
                dt = datetime.strptime(raw_date, "%B %d %Y")
                date = dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        return {
            "title": title,
            "text": text,
            "date": date,
            "url": url,
        }

    def fetch_opinion(self, opinion_info: dict) -> dict:
        """Fetch a single opinion (HTML or PDF)."""
        url = opinion_info["url"]
        if opinion_info["format"] == "html":
            raw = self.fetch_html_opinion(url)
        else:
            raw = self.fetch_pdf_opinion(url)

        if not raw or not raw.get("text"):
            return None

        raw["doc_id"] = opinion_info["doc_id"]
        raw["tax_type"] = opinion_info["tax_type"]
        raw["tax_type_name"] = opinion_info["tax_type_name"]
        raw["format"] = opinion_info["format"]
        return raw

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into standard schema."""
        # Build a clean opinion ID
        doc_id = raw["doc_id"].replace("_", "-").upper()
        suffix = raw.get("tax_type", "")[0:1].upper() if raw.get("tax_type") else ""
        opinion_id = f"TSB-A-{doc_id}"

        return {
            "_id": opinion_id,
            "_source": "US/NY-DTF",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_id": opinion_id,
            "tax_type": raw.get("tax_type", ""),
            "tax_type_name": raw.get("tax_type_name", ""),
            "title": raw.get("title", ""),
            "text": raw["text"],
            "date": raw.get("date", ""),
            "format": raw.get("format", ""),
            "url": raw.get("url", ""),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all advisory opinions with full text."""
        total = 0
        for tax_type in TAX_TYPES:
            logger.info(f"Discovering opinions for {tax_type['name']}...")
            opinions = self.discover_all_opinions(tax_type)
            logger.info(f"  Found {len(opinions)} opinions for {tax_type['name']}")
            for op_info in opinions:
                raw = self.fetch_opinion(op_info)
                if raw and raw.get("text"):
                    yield raw
                    total += 1
                    if total % 50 == 0:
                        logger.info(f"  Progress: {total} opinions fetched")
        logger.info(f"Total opinions fetched: {total}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch updates since a date."""
        yield from self.fetch_all()

    def bootstrap(self, sample: bool = False):
        """Run the bootstrap process."""
        sample_dir = Path(self.source_dir) / "sample"
        sample_dir.mkdir(exist_ok=True)

        if sample:
            logger.info(f"Running in SAMPLE mode — fetching {len(SAMPLE_OPINIONS)} opinions")
            count = 0
            for op_info in SAMPLE_OPINIONS:
                url = f"{BASE_URL}{op_info['url']}"
                fmt = "pdf" if op_info["url"].endswith(".pdf") else "html"
                # Extract doc_id from URL
                fname = op_info["url"].rsplit("/", 1)[-1]
                doc_id = fname.replace(".htm", "").replace(".pdf", "").lstrip("a")

                info = {
                    "url": url,
                    "doc_id": doc_id,
                    "format": fmt,
                    "tax_type": op_info["tax_type"],
                    "tax_type_name": next(
                        (t["name"] for t in TAX_TYPES if t["slug"] == op_info["tax_type"]),
                        op_info["tax_type"]
                    ),
                }
                raw = self.fetch_opinion(info)
                if raw and raw.get("text"):
                    record = self.normalize(raw)
                    safe_id = record["_id"].replace("/", "_")
                    out_file = sample_dir / f"{safe_id}.json"
                    out_file.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                    count += 1
                    logger.info(f"  Saved: {record['_id']} ({len(record['text'])} chars)")
                else:
                    logger.warning(f"  No text for {url}")
            logger.info(f"Sample complete: {count} records saved to {sample_dir}")
        else:
            logger.info("Running FULL bootstrap")
            count = 0
            for record in self.fetch_all():
                safe_id = record["_id"].replace("/", "_")
                out_file = sample_dir / f"{safe_id}.json"
                out_file.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                count += 1
                if count % 100 == 0:
                    logger.info(f"  Progress: {count} records saved")
            logger.info(f"Full bootstrap complete: {count} records")


def main():
    scraper = NYDTFScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [test-api|bootstrap] [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)
    elif cmd == "bootstrap":
        sample = "--sample" in sys.argv
        scraper.bootstrap(sample=sample)
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
