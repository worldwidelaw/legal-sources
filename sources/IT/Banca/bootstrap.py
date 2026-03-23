#!/usr/bin/env python3
"""
IT/Banca -- Bank of Italy (Banca d'Italia) Supervision Data Fetcher

Fetches banking supervision documents from bancaditalia.it:
  - Archivio norme: circolari, disposizioni, comunicazioni (21 pages, ~210 docs)
  - Provvedimenti sanzionatori: sanction decisions (18 pages of PDFs)

Strategy:
  - Scrapes paginated archive listing at /compiti/vigilanza/normativa/archivio-norme/
  - Follows detail page links to find PDFs
  - For sanctions, directly downloads PDFs from listing pages
  - Extracts full text from PDFs using pdfplumber
  - Falls back to HTML text extraction for comunicazioni without PDFs

License: Italian Open Data (public domain)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records
  python bootstrap.py test-api           # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
import subprocess
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, List
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.Banca")

BASE_URL = "https://www.bancaditalia.it"
ARCHIVE_BASE = "/compiti/vigilanza/normativa/archivio-norme/"
SANCTIONS_BASE = "/compiti/vigilanza/provvedimenti-sanzionatori/"


class BancaScraper(BaseScraper):
    """Scraper for Bank of Italy supervision documents."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self._seen_urls = set()

    def _curl_get(self, url: str, timeout: int = 60) -> Optional[bytes]:
        try:
            result = subprocess.run(
                ["curl", "-sL", "--max-time", str(timeout), url],
                capture_output=True,
                timeout=timeout + 10,
            )
            if result.returncode == 0 and result.stdout:
                return result.stdout
            return None
        except Exception as e:
            logger.warning(f"curl failed for {url}: {e}")
            return None

    def _extract_pdf_text(self, pdf_bytes: bytes, max_pages: int = 50) -> Optional[str]:
        """Extract text from PDF, limiting to max_pages to avoid huge circulars."""
        if not HAS_PDFPLUMBER:
            return None
        # Verify it's actually a PDF
        if not pdf_bytes[:4] == b'%PDF':
            return None
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                parts = []
                for i, page in enumerate(pdf.pages):
                    if i >= max_pages:
                        parts.append(f"[... truncated at {max_pages} pages, {len(pdf.pages)} total ...]")
                        break
                    text = page.extract_text()
                    if text:
                        parts.append(text)
                return "\n\n".join(parts) if parts else None
        except Exception as e:
            logger.warning(f"PDF extraction failed: {e}")
            return None

    def _extract_html_text(self, html: str) -> str:
        """Extract main content text from HTML page."""
        # Find main column
        main = re.search(r'class="bdi-main-column">(.*?)(?=class="bdi-side|<footer)', html, re.S)
        if not main:
            return ""
        content = main.group(1)
        # Remove scripts, styles, nav
        content = re.sub(r'<(script|style|nav)[^>]*>.*?</\1>', '', content, flags=re.S)
        # Strip tags
        text = re.sub(r'<[^>]+>', ' ', content)
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'&[a-z]+;', '', text)
        text = ' '.join(text.split())
        # Only return if substantial content (> 200 chars after cleanup)
        return text if len(text) > 200 else ""

    def _parse_date(self, text: str) -> Optional[str]:
        """Extract date from text."""
        # DD/MM/YYYY
        m = re.search(r'(\d{2})/(\d{2})/(\d{4})', text)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        # DD.MM.YYYY
        m = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', text)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        # Italian date in URL: com-YYYYMMDD
        m = re.search(r'com-(\d{4})(\d{2})(\d{2})', text)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        # Date in URL: YYYY.MM.DD
        m = re.search(r'(\d{4})\.(\d{2})\.(\d{2})', text)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        # Date from filename: del_DDMMYYYY or del_DD_MM_YYYY
        m = re.search(r'del[_ ](\d{2})(\d{2})(\d{4})', text)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        m = re.search(r'del[_ ](\d{2})[_ ](\d{2})[_ ](\d{4})', text)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        # Written Italian dates
        months = {
            'gennaio': '01', 'febbraio': '02', 'marzo': '03', 'aprile': '04',
            'maggio': '05', 'giugno': '06', 'luglio': '07', 'agosto': '08',
            'settembre': '09', 'ottobre': '10', 'novembre': '11', 'dicembre': '12'
        }
        for month_name, month_num in months.items():
            m = re.search(rf'(\d{{1,2}})\s+{month_name}\s+(\d{{4}})', text, re.I)
            if m:
                return f"{m.group(2)}-{month_num}-{m.group(1).zfill(2)}"
        return None

    def _make_id(self, url: str) -> str:
        clean_url = re.sub(r'\?.*$', '', url)
        return hashlib.md5(clean_url.encode()).hexdigest()[:16]

    def _classify_doc(self, url: str, title: str) -> str:
        lower = (url + " " + title).lower()
        if "circolar" in lower:
            return "circolare"
        elif "disposizion" in lower:
            return "disposizione"
        elif "comunicazion" in lower:
            return "comunicazione"
        elif "sanzion" in lower:
            return "sanzione"
        elif "determin" in lower:
            return "determinazione"
        elif "usura" in lower:
            return "contrasto_usura"
        return "documento"

    def normalize(self, raw: Dict) -> Dict:
        title = raw.get("title", "")
        text = raw.get("text", "")
        url = raw.get("url", "")
        date = self._parse_date(title + " " + url)

        return {
            "_id": self._make_id(url),
            "_source": "IT/Banca",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "doc_type": self._classify_doc(url, title),
        }

    def _fetch_archive_docs(self, sample_limit: int) -> Generator[Dict, None, None]:
        """Fetch documents from the archivio norme."""
        total = 0
        max_pages = 3 if sample_limit <= 15 else 22

        for page_num in range(max_pages):
            if total >= sample_limit:
                break

            url = f"{BASE_URL}{ARCHIVE_BASE}index.html?page={page_num}"
            logger.info(f"Fetching archive page {page_num}...")
            data = self._curl_get(url)
            if not data:
                continue

            html = data.decode("utf-8", errors="ignore")

            # Find detail page links
            detail_links = re.findall(
                r'href="(/compiti/vigilanza/normativa/archivio-norme/'
                r'(?:circolari|disposizioni|comunicazioni|contrasto-usura)/[^"]+index\.html)"',
                html
            )
            unique_links = list(dict.fromkeys(detail_links))
            logger.info(f"Found {len(unique_links)} documents on page {page_num}")

            for detail_path in unique_links:
                if total >= sample_limit:
                    break

                detail_url = BASE_URL + detail_path
                if detail_url in self._seen_urls:
                    continue
                self._seen_urls.add(detail_url)

                logger.info(f"Fetching detail: {detail_path}")
                detail_data = self._curl_get(detail_url)
                time.sleep(1)
                if not detail_data:
                    continue

                detail_html = detail_data.decode("utf-8", errors="ignore")

                # Extract title
                title_match = re.search(r'<h1[^>]*>([^<]+)</h1>', detail_html)
                title = title_match.group(1).strip() if title_match else detail_path

                # Find PDFs on detail page
                pdf_links = re.findall(r'href="([^"]*\.pdf[^"]*)"', detail_html)

                text = ""
                pdf_url = None

                if pdf_links:
                    # Try each PDF link until one works
                    for pdf_path in pdf_links:
                        pdf_full = pdf_path if pdf_path.startswith("http") else BASE_URL + pdf_path
                        logger.info(f"Trying PDF: {pdf_full[-60:]}...")
                        pdf_bytes = self._curl_get(pdf_full, timeout=120)
                        time.sleep(1)

                        if pdf_bytes and pdf_bytes[:4] == b'%PDF':
                            pdf_url = pdf_full
                            if len(pdf_bytes) > 15_000_000:
                                logger.warning(f"PDF too large ({len(pdf_bytes)} bytes), extracting first 50 pages")
                            text = self._extract_pdf_text(pdf_bytes) or ""
                            if text:
                                logger.info(f"Extracted {len(text)} chars from PDF")
                                break

                if not text:
                    # Fall back to HTML text extraction
                    text = self._extract_html_text(detail_html)
                    if text:
                        logger.info(f"Extracted {len(text)} chars from HTML")

                if not text:
                    logger.warning(f"No text for {title[:60]}")
                    continue

                raw = {
                    "title": title,
                    "text": text,
                    "url": pdf_url or detail_url,
                }
                record = self.normalize(raw)
                yield record
                total += 1
                logger.info(f"[{total}] {title[:60]}")

            time.sleep(2)

    def _fetch_sanctions(self, sample_limit: int) -> Generator[Dict, None, None]:
        """Fetch sanction decision PDFs."""
        total = 0
        max_pages = 2 if sample_limit <= 15 else 19

        for section in ["index.html", "archivio/index.html"]:
            if total >= sample_limit:
                break

            for page_num in range(max_pages):
                if total >= sample_limit:
                    break

                url = f"{BASE_URL}{SANCTIONS_BASE}{section}?page={page_num}"
                logger.info(f"Fetching sanctions page {page_num} ({section})...")
                data = self._curl_get(url)
                if not data:
                    continue

                html = data.decode("utf-8", errors="ignore")

                # Find PDF links (note: URLs may have trailing whitespace/newline)
                pdf_links = re.findall(
                    r'href="(/compiti/vigilanza/provvedimenti-sanzionatori/documenti/[^"]+\.pdf)',
                    html
                )

                if not pdf_links:
                    break

                for pdf_path in pdf_links:
                    if total >= sample_limit:
                        break

                    pdf_url = BASE_URL + pdf_path
                    if pdf_url in self._seen_urls:
                        continue
                    self._seen_urls.add(pdf_url)

                    # Extract title from filename
                    filename = unquote(pdf_path.split("/")[-1])
                    title = re.sub(r'\.pdf$', '', filename).replace('_', ' ')

                    logger.info(f"Downloading sanction: {title[:60]}...")
                    pdf_bytes = self._curl_get(pdf_url, timeout=120)
                    time.sleep(1.5)

                    if not pdf_bytes or len(pdf_bytes) < 500:
                        continue

                    text = self._extract_pdf_text(pdf_bytes) or ""
                    if not text:
                        continue

                    raw = {
                        "title": f"Provvedimento sanzionatorio: {title}",
                        "text": text,
                        "url": pdf_url,
                    }
                    record = self.normalize(raw)
                    yield record
                    total += 1
                    logger.info(f"[S{total}] {title[:60]} ({len(text)} chars)")

                time.sleep(2)

    def fetch_all(self, sample: bool = False) -> Generator[Dict, None, None]:
        """Fetch all Bank of Italy supervision documents."""
        sample_limit = 15 if sample else 10000

        # First fetch archive norms
        count = 0
        for record in self._fetch_archive_docs(sample_limit):
            yield record
            count += 1

        # Then fetch sanctions (use remaining budget)
        remaining = sample_limit - count
        if remaining > 0:
            for record in self._fetch_sanctions(remaining):
                yield record

    def fetch_updates(self, since: str) -> Generator[Dict, None, None]:
        """Fetch recent documents."""
        # Just fetch first few pages of archive + sanctions
        for record in self._fetch_archive_docs(100):
            yield record
        for record in self._fetch_sanctions(50):
            yield record

    def test_api(self) -> bool:
        """Test connectivity."""
        logger.info("Testing Bank of Italy connectivity...")
        data = self._curl_get(f"{BASE_URL}{ARCHIVE_BASE}")
        if not data:
            logger.error("Failed to fetch archive page")
            return False

        html = data.decode("utf-8", errors="ignore")
        links = re.findall(
            r'href="(/compiti/vigilanza/normativa/archivio-norme/(?:circolari|disposizioni|comunicazioni)/[^"]+)"',
            html
        )
        logger.info(f"Archive page returned {len(set(links))} document links")
        return len(links) > 0


def main():
    scraper = BancaScraper()

    if len(sys.argv) < 2:
        print("Usage: bootstrap.py [bootstrap|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    elif command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_all(sample=sample):
            out_path = sample_dir / f"{record['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1

        logger.info(f"Saved {count} records to {sample_dir}")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
