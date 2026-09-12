#!/usr/bin/env python3
"""
CO/FiscaliaGeneral -- Fiscalía General de la Nación - Normatividad

Scrapes normative documents (decrees, resolutions, directives) from the
Fiscalía's normatividad page. Downloads PDFs and extracts full text.

Usage:
  python bootstrap.py bootstrap --sample    # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Quick connectivity test
"""

import io
import json
import re
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, List, Optional
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CO.FiscaliaGeneral")

BASE_URL = "https://www.fiscalia.gov.co"
NORMATIVIDAD_URL = f"{BASE_URL}/colombia/la-entidad/normatividad/"
DELAY = 2.0
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

SOURCE_DIR = Path(__file__).resolve().parent
DATA_DIR = SOURCE_DIR / "data"
SAMPLE_DIR = SOURCE_DIR / "sample"
SOURCE_ID = "CO/FiscaliaGeneral"


def _clean_html(html: str) -> str:
    """Strip HTML tags and clean whitespace."""
    text = re.sub(r"<[^>]+>", "", html)
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&quot;", '"').replace("&#039;", "'").replace("&nbsp;", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _guess_doc_type(filename: str, title: str) -> str:
    """Guess document type from filename or title."""
    combined = (filename + " " + title).lower()
    if "decreto" in combined:
        return "decreto"
    elif "resolucion" in combined or "resolución" in combined:
        return "resolucion"
    elif "directiva" in combined:
        return "directiva"
    elif "acuerdo" in combined:
        return "acuerdo"
    elif "ley" in combined:
        return "ley"
    elif "manual" in combined:
        return "manual"
    elif "circular" in combined:
        return "circular"
    return "otro"


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    try:
        import pdfplumber
    except ImportError:
        logger.error("pdfplumber not installed. Run: pip install pdfplumber")
        return ""

    text_parts = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
    except Exception as e:
        logger.warning(f"PDF extraction error: {e}")
        return ""

    return "\n\n".join(text_parts)


def _parse_normatividad_page(html: str) -> List[Dict[str, str]]:
    """Parse the normatividad page to extract document links with metadata."""
    items = []
    seen_urls = set()

    # Pattern: <a href="URL">Title</a>. Description
    # The content sections are in vc_toggle_content divs
    sections = re.findall(
        r'<div class="vc_toggle_content">(.*?)</div>',
        html,
        re.DOTALL,
    )

    # Also capture menu items from the nav section
    all_html = "\n".join(sections) if sections else html

    # Find all PDF links with surrounding text
    pattern = re.compile(
        r'<a[^>]*href="([^"]*\.pdf)"[^>]*>(.*?)</a>'
        r'(\.?\s*([^<]{0,500}))?',
        re.DOTALL,
    )

    for m in pattern.finditer(all_html):
        url = m.group(1).strip()
        link_text = _clean_html(m.group(2)).strip()
        description = _clean_html(m.group(3) or "").strip().rstrip(".")

        if url in seen_urls:
            continue
        seen_urls.add(url)

        # Make URL absolute
        if url.startswith("/"):
            url = BASE_URL + url

        # Extract filename
        filename = unquote(url.split("/")[-1])

        items.append({
            "url": url,
            "title": link_text or filename.replace(".pdf", "").replace("-", " "),
            "description": description,
            "filename": filename,
        })

    return items


class FiscaliaGeneralScraper(BaseScraper):
    """Scraper for Fiscalía General normative documents."""

    def __init__(self):
        super().__init__()
        self._session = None

    def _get_session(self):
        if self._session is None:
            import requests
            self._session = requests.Session()
            self._session.headers.update({"User-Agent": UA})
        return self._session

    def _download_pdf(self, url: str) -> str:
        """Download PDF and return extracted text."""
        session = self._get_session()
        try:
            resp = session.get(url, timeout=120)
            resp.raise_for_status()
            return _extract_pdf_text(resp.content)
        except Exception as e:
            logger.warning(f"Failed to download {url}: {e}")
            return ""

    def normalize(self, item: Dict, text: str) -> Dict:
        """Normalize a document into standard record format."""
        filename = item["filename"]
        doc_type = _guess_doc_type(filename, item["title"])
        date = self._extract_date(item, filename)

        return {
            "_id": f"CO-FiscaliaGeneral-{filename.replace('.pdf', '')}",
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": item["title"],
            "text": text,
            "date": date,
            "description": item.get("description", ""),
            "url": item["url"],
            "doc_type": doc_type,
            "pdf_filename": filename,
            "language": "es",
        }

    @staticmethod
    def _extract_date(item: Dict, filename: str) -> Optional[str]:
        """Extract date from description ('Publicado YYYY-MM-DD') or filename."""
        desc = item.get("description", "")
        m = re.search(r"Publicado\s+(\d{4}-\d{2}-\d{2})", desc)
        if m:
            return m.group(1)
        # Try filename patterns like "DEL-3-DE-JUNIO-DE-2025"
        months = {
            "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
            "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
            "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
        }
        m = re.search(r"DEL-(\d{1,2})-DE-(\w+)-DE-(\d{4})", filename, re.IGNORECASE)
        if m:
            day, month_name, year = m.group(1), m.group(2).lower(), m.group(3)
            if month_name in months:
                return f"{year}-{months[month_name]}-{int(day):02d}"
        # Fall back to year from filename
        m = re.search(r"(?:^|-)(\d{4})(?:-|$)", filename)
        if m:
            return f"{m.group(1)}-01-01"
        return None

    def fetch_all(self, sample: bool = False) -> Generator[Dict, None, None]:
        """Fetch all normative documents."""
        session = self._get_session()

        logger.info(f"Fetching normatividad page: {NORMATIVIDAD_URL}")
        resp = session.get(NORMATIVIDAD_URL, timeout=30)
        resp.raise_for_status()

        items = _parse_normatividad_page(resp.text)
        logger.info(f"Found {len(items)} documents on normatividad page")

        count = 0
        for item in items:
            logger.info(f"Downloading: {item['title']} ({item['filename']})")
            text = self._download_pdf(item["url"])
            if not text or len(text) < 100:
                logger.warning(f"Insufficient text from {item['filename']}, skipping")
                continue

            record = self.normalize(item, text)
            yield record
            count += 1

            if sample and count >= 15:
                break

            time.sleep(DELAY)

        logger.info(f"Total records fetched: {count}")

    def fetch_updates(self, since: str) -> Generator[Dict, None, None]:
        """Fetch updates (re-fetches all as page has no date filter)."""
        yield from self.fetch_all(sample=False)

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            session = self._get_session()
            resp = session.get(NORMATIVIDAD_URL, timeout=15)
            resp.raise_for_status()
            items = _parse_normatividad_page(resp.text)
            logger.info(f"Test OK: found {len(items)} documents")
            return len(items) > 0
        except Exception as e:
            logger.error(f"Test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CO/FiscaliaGeneral scraper")
    parser.add_argument("command", choices=["bootstrap", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Full fetch")
    args = parser.parse_args()

    scraper = FiscaliaGeneralScraper()

    if args.command == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)

    elif args.command == "bootstrap":
        sample = args.sample and not args.full
        out_dir = SAMPLE_DIR if sample else DATA_DIR
        out_dir.mkdir(parents=True, exist_ok=True)

        records_file = out_dir / "records.jsonl"
        count = 0

        with open(records_file, "w", encoding="utf-8") as f:
            for record in scraper.fetch_all(sample=sample):
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                count += 1
                logger.info(
                    f"[{count}] {record.get('title', 'N/A')} "
                    f"({len(record.get('text', ''))} chars)"
                )

                if sample:
                    sample_file = out_dir / f"{record['_id']}.json"
                    with open(sample_file, "w", encoding="utf-8") as sf:
                        json.dump(record, sf, indent=2, ensure_ascii=False)

        logger.info(f"Done. {count} records written to {records_file}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
