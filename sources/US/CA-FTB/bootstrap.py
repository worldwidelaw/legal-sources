#!/usr/bin/env python3
"""
US/CA-FTB -- California Franchise Tax Board Legal Rulings

Fetches legal rulings, chief counsel rulings, and FTB notices with full text
from ftb.ca.gov. Older documents are HTML, newer ones are PDF.

Strategy:
  1. Parse index pages for each collection to discover all document links
  2. For HTML documents, extract text from the main content area
  3. For PDF documents, download and extract text via pdfplumber
  4. Normalize all records into standard schema

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample documents
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
import tempfile
import io
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CA-FTB")

BASE_URL = "https://www.ftb.ca.gov"

COLLECTIONS = [
    {
        "name": "legal_ruling",
        "index_path": "/tax-pros/law/legal-rulings/index.html",
        "id_prefix": "LR",
    },
    {
        "name": "chief_counsel",
        "index_path": "/tax-pros/law/chief-counsel-rulings/index.html",
        "id_prefix": "CCR",
    },
    {
        "name": "ftb_notice",
        "index_path": "/tax-pros/law/ftb-notices/index.html",
        "id_prefix": "FN",
    },
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
        source="US/CA-FTB",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="doctrine",
    ) or ""

class CAFTBScraper(BaseScraper):

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
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url}: {e}")
                if attempt < retries:
                    time.sleep(3)
        return b"" if binary else ""

    def test_api(self):
        """Test connectivity to FTB."""
        logger.info("Testing FTB legal rulings...")
        try:
            html = self._get(f"{BASE_URL}/tax-pros/law/legal-rulings/index.html")
            if "legal ruling" in html.lower() or "Legal Ruling" in html:
                logger.info("  Legal rulings index: OK")
            else:
                logger.error("  Legal rulings index: unexpected content")
                return False

            # Test an HTML ruling
            html = self._get(f"{BASE_URL}/tax-pros/law/legal-rulings/1998-2.html")
            if len(html) > 500:
                logger.info("  HTML ruling page: OK")
            else:
                logger.error("  HTML ruling page: too short or missing")
                return False

            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def discover_documents(self, collection: dict) -> list:
        """Discover all document links from an index page."""
        html = self._get(f"{BASE_URL}{collection['index_path']}")
        if not html:
            return []

        docs = []
        seen = set()
        base_dir = collection["index_path"].rsplit("/", 1)[0]

        # Only match links that look like ruling documents: YYYY-NN.html/pdf
        # or historic numbered format like 1958-13.html
        doc_pattern = re.compile(
            r'href="([^"]*?/(\d{4}-\d{1,4}(?:[a-zA-Z])?)\.(html|pdf))"[^>]*>([^<]*)'
        )

        for m in doc_pattern.finditer(html):
            href = m.group(1)
            doc_num = m.group(2)
            ext = m.group(3)
            link_text = m.group(4).strip()

            # Resolve relative URLs
            if href.startswith("http"):
                url = href
            elif href.startswith("/"):
                url = f"{BASE_URL}{href}"
            else:
                url = f"{BASE_URL}{base_dir}/{href}"

            if url not in seen:
                seen.add(url)
                docs.append({
                    "url": url,
                    "filename": f"{doc_num}.{ext}",
                    "doc_num": doc_num,
                    "link_text": link_text,
                    "is_pdf": ext == "pdf",
                    "collection": collection["name"],
                    "id_prefix": collection["id_prefix"],
                })

        logger.info(f"  {collection['name']}: {len(docs)} documents")
        return docs

    def fetch_document(self, doc: dict) -> dict:
        """Fetch a single document and extract full text."""
        url = doc["url"]

        if doc["is_pdf"]:
            pdf_bytes = self._get(url, binary=True)
            if not pdf_bytes:
                return None
            text = extract_pdf_text(pdf_bytes)
        else:
            html = self._get(url)
            if not html:
                return None
            text = self._extract_html_text(html)

        if not text or len(text) < 20:
            logger.warning(f"No text extracted from {url}")
            return None

        # Build title from link text or document number
        title = doc["link_text"] if doc["link_text"] else doc["doc_num"]

        # Try to extract year from doc_num
        year_match = re.match(r'(\d{4})', doc["doc_num"])
        date = f"{year_match.group(1)}-01-01" if year_match else None

        return {
            "ruling_id": f"{doc['id_prefix']}-{doc['doc_num']}",
            "title": title,
            "text": text,
            "ruling_type": doc["collection"],
            "date": date,
            "url": url,
        }

    def _extract_html_text(self, html: str) -> str:
        """Extract main content text from an FTB HTML page."""
        # Try to find main content area
        text = ""

        # Strategy 1: Look for InstanceBeginEditable Main content
        match = re.search(
            r'InstanceBeginEditable\s+name="Main content".*?-->(.*?)<!--\s*InstanceEndEditable',
            html, re.DOTALL
        )
        if match:
            text = strip_html(match.group(1))

        # Strategy 2: Look for main content div
        if not text or len(text) < 50:
            for marker in ['id="main-content"', 'class="main-content"',
                          'role="main"', 'id="content"']:
                idx = html.find(marker)
                if idx > 0:
                    content = html[idx:]
                    # Cut at footer or sidebar
                    for end in ['id="footer"', 'class="footer"',
                               'id="sidebar"', '</main>']:
                        end_idx = content.find(end)
                        if end_idx > 0:
                            content = content[:end_idx]
                            break
                    candidate = strip_html(content)
                    if len(candidate) > len(text):
                        text = candidate

        # Strategy 3: body fallback
        if not text or len(text) < 50:
            body_match = re.search(r'<body[^>]*>(.*?)</body>', html, re.DOTALL)
            if body_match:
                text = strip_html(body_match.group(1))

        # Clean up
        if text:
            # Remove FTB navigation/boilerplate
            text = re.sub(r'^.*?(?=(?:LEGAL RULING|Legal Ruling|FTB Notice|FRANCHISE TAX BOARD))', '', text, flags=re.DOTALL)
            text = re.sub(r'\n{3,}', '\n\n', text).strip()

        return text

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into standard schema."""
        return {
            "_id": raw["ruling_id"],
            "_source": "US/CA-FTB",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "ruling_id": raw["ruling_id"],
            "title": raw["title"],
            "text": raw["text"],
            "ruling_type": raw.get("ruling_type", ""),
            "url": raw.get("url", ""),
            "date": raw.get("date") or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all FTB rulings and notices."""
        total = 0
        for collection in COLLECTIONS:
            logger.info(f"Processing {collection['name']}...")
            docs = self.discover_documents(collection)
            for doc in docs:
                raw = self.fetch_document(doc)
                if raw and raw.get("text"):
                    yield self.normalize(raw)
                    total += 1
        logger.info(f"Total documents fetched: {total}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch updates since a date."""
        yield from self.fetch_all()

    def bootstrap(self, sample: bool = False):
        """Run the bootstrap process."""
        sample_dir = Path(self.source_dir) / "sample"
        sample_dir.mkdir(exist_ok=True)

        if sample:
            logger.info("Running in SAMPLE mode — fetching ~5 docs per collection")
            count = 0
            for collection in COLLECTIONS:
                logger.info(f"Processing {collection['name']}...")
                docs = self.discover_documents(collection)
                # Take up to 5 per collection, mix of HTML and PDF
                selected = docs[:5]
                for doc in selected:
                    raw = self.fetch_document(doc)
                    if raw and raw.get("text"):
                        record = self.normalize(raw)
                        safe_id = record['_id'].replace('/', '_')
                        out_file = sample_dir / f"{safe_id}.json"
                        out_file.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                        count += 1
                        logger.info(f"  Saved: {record['_id']} ({len(record['text'])} chars)")
                    else:
                        logger.warning(f"  No text for {doc['url']}")
            logger.info(f"Sample complete: {count} records saved to {sample_dir}")
        else:
            logger.info("Running FULL bootstrap")
            count = 0
            for record in self.fetch_all():
                safe_id = record['_id'].replace('/', '_')
                out_file = Path(self.source_dir) / "sample" / f"{safe_id}.json"
                out_file.parent.mkdir(exist_ok=True)
                out_file.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                count += 1
                if count % 20 == 0:
                    logger.info(f"  Progress: {count} records saved")
            logger.info(f"Full bootstrap complete: {count} records")


def main():
    scraper = CAFTBScraper()

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
