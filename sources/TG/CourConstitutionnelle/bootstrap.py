#!/usr/bin/env python3
"""
TG/CourConstitutionnelle -- Togo Constitutional Court Decisions

Fetches ~157 decisions from courconstitutionnelle.tg (2005-2025).
Files are hosted via WordPress Download Manager plugin in PDF, DOC, and DOCX formats.

Strategy:
  - Paginate through /download-category/decisions/ to collect all decision page URLs
  - Fetch each decision page; extract title, date, and download URL
  - Download URL comes from either direct PDF links or WPDM data-downloadurl attribute
  - Extract text based on content type: pypdf for PDF, python-docx for DOCX,
    subprocess textutil for DOC (macOS), or raw binary extraction as fallback

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap --full
  python bootstrap.py test
"""

import argparse
import io
import json
import logging
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TG.CourConstitutionnelle")

BASE_URL = "https://courconstitutionnelle.tg"
CATEGORY_URL = f"{BASE_URL}/download-category/decisions/"

SIDEBAR_PDF = "R-I-DE-LA-COUR-MODIFIE-2020.pdf"

try:
    from pypdf import PdfReader
except ImportError:
    PdfReader = None

try:
    from docx import Document as DocxDocument
except ImportError:
    DocxDocument = None


class CourConstitutionnelleScraper(BaseScraper):
    """Scraper for TG/CourConstitutionnelle -- Togo Constitutional Court."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                time.sleep(1.5)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _get_all_decision_urls(self) -> List[str]:
        """Paginate through the decisions category to collect all decision page URLs."""
        all_urls = set()
        for page in range(1, 30):
            url = f"{CATEGORY_URL}page/{page}/" if page > 1 else CATEGORY_URL
            resp = self._request(url)
            if resp is None:
                break
            links = set(re.findall(
                rf'href="(https?://courconstitutionnelle\.tg/download/[^"]+)"',
                resp.text,
            ))
            new = links - all_urls
            if not new:
                break
            all_urls.update(links)
            logger.info(f"Category page {page}: {len(new)} new links (total: {len(all_urls)})")
        return sorted(all_urls)

    def _extract_decision_info(self, page_url: str) -> Optional[Dict]:
        """Fetch a decision page and extract title, date, and download URL."""
        resp = self._request(page_url)
        if resp is None:
            return None

        html = resp.text

        # Extract title from h1
        title_m = re.search(r'<h1[^>]*>([^<]+)</h1>', html)
        title = title_m.group(1).strip() if title_m else ""

        # Strategy 1: Direct PDF link (excluding sidebar)
        pdf_urls = re.findall(r'href="([^"]*\.pdf[^"]*)"', html)
        download_url = None
        for p in pdf_urls:
            if SIDEBAR_PDF not in p:
                download_url = p.replace("http://", "https://")
                break

        # Strategy 2: WPDM data-downloadurl attribute
        if not download_url:
            wpdm_m = re.search(r'data-downloadurl=([^\s>]+)', html)
            if wpdm_m:
                download_url = wpdm_m.group(1).strip('"').strip("'")
                download_url = download_url.replace("http://", "https://")

        if not download_url:
            return None

        date = self._extract_date(title)

        return {
            "title": title,
            "download_url": download_url,
            "page_url": page_url,
            "date": date,
        }

    def _extract_date(self, text: str) -> str:
        """Extract date from French text like 'du 13 août 2025'."""
        months = {
            "janvier": "01", "février": "02", "mars": "03", "avril": "04",
            "mai": "05", "juin": "06", "juillet": "07", "août": "08",
            "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12",
            "fevrier": "02", "aout": "08", "decembre": "12",
        }
        pattern = r'(\d{1,2})\s+(' + '|'.join(months.keys()) + r')\s+(\d{4})'
        m = re.search(pattern, text.lower())
        if m:
            day = int(m.group(1))
            month = months[m.group(2)]
            year = m.group(3)
            return f"{year}-{month}-{day:02d}"
        year_m = re.search(r'(\d{4})', text)
        if year_m:
            y = int(year_m.group(1))
            if 2000 <= y <= 2030:
                return f"{y}-01-01"
        return ""

    def _extract_text(self, content: bytes, content_type: str) -> str:
        """Extract text from downloaded content based on its type."""
        ct = content_type.lower()

        if "pdf" in ct or content[:4] == b"%PDF":
            return self._extract_pdf_text(content)
        elif "openxmlformats" in ct or content[:2] == b"PK":
            return self._extract_docx_text(content)
        elif "msword" in ct or content[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1":
            return self._extract_doc_text(content)
        elif "html" in ct:
            return self._extract_html_text(content.decode("utf-8", errors="replace"))
        else:
            logger.warning(f"Unknown content type: {content_type}")
            return ""

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        if PdfReader is None:
            return ""
        try:
            reader = PdfReader(io.BytesIO(pdf_bytes))
            pages_text = []
            for page in reader.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(text)
            full_text = "\n\n".join(pages_text).strip()
            return re.sub(r"\n{3,}", "\n\n", full_text)
        except Exception as e:
            logger.warning(f"PDF extraction error: {e}")
            return ""

    def _extract_docx_text(self, docx_bytes: bytes) -> str:
        if DocxDocument is None:
            return ""
        try:
            doc = DocxDocument(io.BytesIO(docx_bytes))
            paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
            return "\n\n".join(paragraphs).strip()
        except Exception as e:
            logger.warning(f"DOCX extraction error: {e}")
            return ""

    def _extract_doc_text(self, doc_bytes: bytes) -> str:
        """Extract text from old .doc format using textutil (macOS) or binary fallback."""
        # Try textutil (macOS)
        try:
            with tempfile.NamedTemporaryFile(suffix=".doc", delete=False) as tmp:
                tmp.write(doc_bytes)
                tmp_path = tmp.name
            out_path = tmp_path + ".txt"
            result = subprocess.run(
                ["textutil", "-convert", "txt", "-output", out_path, tmp_path],
                capture_output=True, timeout=30,
            )
            if result.returncode == 0:
                text = Path(out_path).read_text(encoding="utf-8", errors="replace").strip()
                Path(tmp_path).unlink(missing_ok=True)
                Path(out_path).unlink(missing_ok=True)
                return re.sub(r"\n{3,}", "\n\n", text)
            Path(tmp_path).unlink(missing_ok=True)
            Path(out_path).unlink(missing_ok=True)
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass

        # Binary fallback: extract readable text sequences from OLE document
        try:
            text_parts = re.findall(
                rb'[\x20-\x7e\xc0-\xff]{20,}',
                doc_bytes,
            )
            if text_parts:
                raw = b" ".join(text_parts).decode("latin-1", errors="replace")
                return raw.strip()
        except Exception as e:
            logger.warning(f"DOC binary extraction error: {e}")
        return ""

    def _extract_html_text(self, html: str) -> str:
        """Extract text from HTML content (some WPDM downloads return HTML)."""
        from html.parser import HTMLParser

        class TextExtractor(HTMLParser):
            def __init__(self):
                super().__init__()
                self.texts = []
                self.skip = False

            def handle_starttag(self, tag, attrs):
                if tag in ("script", "style", "nav", "header", "footer"):
                    self.skip = True

            def handle_endtag(self, tag):
                if tag in ("script", "style", "nav", "header", "footer"):
                    self.skip = False

            def handle_data(self, data):
                if not self.skip and data.strip():
                    self.texts.append(data.strip())

        parser = TextExtractor()
        parser.feed(html)
        return "\n".join(parser.texts).strip()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("decision_id", ""),
            "_source": "TG/CourConstitutionnelle",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("page_url", ""),
            "pdf_url": raw.get("download_url", ""),
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        decision_urls = self._get_all_decision_urls()
        logger.info(f"Found {len(decision_urls)} decision pages to process")

        count = 0
        for page_url in decision_urls:
            if max_records and count >= max_records:
                return

            info = self._extract_decision_info(page_url)
            if not info:
                logger.warning(f"No download URL on: {page_url}")
                continue

            resp = self._request(info["download_url"], timeout=120)
            if resp is None:
                logger.warning(f"Failed to download: {info['download_url']}")
                continue

            if len(resp.content) > 50 * 1024 * 1024:
                logger.warning(f"File too large ({len(resp.content)} bytes): {info['title']}")
                continue

            content_type = resp.headers.get("Content-Type", "")
            text = self._extract_text(resp.content, content_type)
            if not text or len(text) < 50:
                logger.warning(
                    f"Insufficient text ({len(text)} chars, type={content_type}): {info['title']}"
                )
                continue

            slug = page_url.rstrip("/").split("/")[-1]
            decision_id = f"TG-CC-{slug}"

            raw = {
                "decision_id": decision_id,
                "title": info["title"],
                "text": text,
                "date": info["date"],
                "page_url": page_url,
                "download_url": info["download_url"],
            }
            count += 1
            yield raw

        logger.info(f"Completed: {count} decisions fetched with full text")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all(max_records=20)

    def test(self) -> bool:
        decision_urls = self._get_all_decision_urls()
        if not decision_urls:
            logger.error("Cannot fetch decision list from courconstitutionnelle.tg")
            return False
        logger.info(f"Index OK: {len(decision_urls)} decision pages found")
        return True


def main():
    parser = argparse.ArgumentParser(description="TG/CourConstitutionnelle data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CourConstitutionnelleScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        max_records = 15 if args.sample else None

        for record in scraper.fetch_all(max_records=max_records):
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"record_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            text_len = len(normalized.get("text", ""))
            logger.info(
                f"[{count + 1}] {normalized.get('title', '?')[:80]} "
                f"({text_len:,} chars)"
            )
            count += 1

        logger.info(f"Bootstrap complete: {count} records saved to sample/")

    elif args.command == "update":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_updates():
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"update_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
