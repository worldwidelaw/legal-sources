#!/usr/bin/env python3
"""
KH/CCF-Prakas -- Cambodia Competition Commission Prakas

Fetches ministerial orders (prakas) from the Cambodia Consumer Protection,
Competition, and Fraud Repression Directorate-General (CCF/DG). Documents are
PDFs covering food safety, halal standards, consumer protection, and competition law.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
import hashlib
import io
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin, unquote

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KH.CCF-Prakas")

BASE_URL = "https://www.ccfdg.gov.kh"
PRAKAS_URL = f"{BASE_URL}/en/laws-regulations/prakas/"


class CCFPrakasScraper(BaseScraper):
    """
    Scraper for KH/CCF-Prakas -- Cambodia Competition Commission Prakas.
    Country: KH
    URL: https://www.ccfdg.gov.kh/en/laws-regulations/prakas/

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (open-data research project)",
        })

    def _parse_date_from_filename(self, filename: str) -> Optional[str]:
        """Extract date from PDF filename patterns like 20220303 or 19990729."""
        # Pattern: YYYYMMDD at start of filename
        match = re.search(r'(\d{4})(\d{2})(\d{2})', filename)
        if match:
            year, month, day = match.group(1), match.group(2), match.group(3)
            y = int(year)
            m = int(month)
            d = int(day)
            if 1990 <= y <= 2030 and 1 <= m <= 12 and 1 <= d <= 31:
                return f"{year}-{month}-{day}"
        return None

    def _title_from_filename(self, filename: str) -> str:
        """Derive a readable title from the PDF filename."""
        # Remove extension
        name = re.sub(r'\.pdf$', '', filename, flags=re.IGNORECASE)
        # Remove date prefix patterns
        name = re.sub(r'^\d{8}_', '', name)
        # Remove common prefixes
        name = re.sub(r'^(Khmer_\d{8}_Prk-\d+_MoC_|ENG_|Unoffcial_EN_|Unofficial_translation_|CLN_)', '', name)
        # Replace underscores/hyphens with spaces
        name = re.sub(r'[-_]+', ' ', name)
        # Remove trailing -1, -2 suffixes
        name = re.sub(r'\s*-?\d+\s*$', '', name)
        return name.strip() or filename

    def _get_document_list(self) -> list[dict]:
        """Scrape all PDF links from the CCF prakas page."""
        from bs4 import BeautifulSoup

        try:
            resp = self.session.get(PRAKAS_URL, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch prakas page: {e}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        # Collect all PDF links, dedup by filename (View vs Download same PDF)
        by_filename: dict[str, dict] = {}

        for link in soup.find_all("a", href=True):
            href = link["href"]
            if ".pdf" not in href.lower():
                continue

            pdf_url = urljoin(PRAKAS_URL, href)

            # Extract the actual wp-content PDF URL from pdf-view wrapper
            if "pdf-view" in pdf_url and "filename=" in pdf_url:
                actual_url = pdf_url.split("filename=", 1)[1]
                filename = unquote(actual_url.split("/")[-1])
            else:
                actual_url = pdf_url
                filename = unquote(pdf_url.split("/")[-1])

            if filename in by_filename:
                # Prefer the direct wp-content URL over pdf-view wrapper
                if "/wp-content/" in actual_url:
                    by_filename[filename]["pdf_url"] = actual_url
                continue

            link_text = link.get_text(strip=True)
            date = self._parse_date_from_filename(filename)

            if link_text and len(link_text) > 10 and link_text.lower() not in ("view", "download", "pdf"):
                title = link_text
            else:
                title = self._title_from_filename(filename)

            by_filename[filename] = {
                "title": title,
                "date": date,
                "pdf_url": actual_url,
                "filename": filename,
            }

        documents = list(by_filename.values())

        logger.info(f"Found {len(documents)} prakas PDFs")
        return documents

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text via pdfplumber."""
        try:
            resp = self.session.get(pdf_url, timeout=60)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to download PDF {pdf_url}: {e}")
            return None

        if len(resp.content) < 500:
            return None

        try:
            pdf = pdfplumber.open(io.BytesIO(resp.content))
            pages_text = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            pdf.close()
            full_text = "\n\n".join(pages_text)
            return full_text if len(full_text) >= 50 else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url}: {e}")
            return None

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw document into standard schema."""
        text = raw.get("text", "").strip()
        if not text or len(text) < 50:
            return None

        title = raw.get("title", "").strip()
        if not title:
            return None

        url_hash = hashlib.md5(raw["pdf_url"].encode()).hexdigest()[:12]
        doc_id = f"KH-CCF-{url_hash}"

        return {
            "_id": doc_id,
            "_source": "KH/CCF-Prakas",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": raw["pdf_url"],
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all CCF prakas documents with full PDF text."""
        documents = self._get_document_list()
        logger.info(f"Processing {len(documents)} prakas documents")

        yielded = 0
        skipped = 0

        for i, doc in enumerate(documents):
            if (i + 1) % 10 == 0:
                logger.info(f"[{i+1}/{len(documents)}] Yielded: {yielded}, Skipped: {skipped}")

            text = self._extract_pdf_text(doc["pdf_url"])
            if not text:
                skipped += 1
                logger.warning(f"Skipped (no text): {doc['title'][:60]}")
                continue

            doc["text"] = text
            normalized = self.normalize(doc)
            if normalized:
                yielded += 1
                yield normalized

            time.sleep(1.5)

        logger.info(f"Done. Yielded: {yielded}, Skipped: {skipped}")

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Fetch recently added documents."""
        yield from self.fetch_all()

    def test(self) -> dict:
        """Quick connectivity test."""
        resp = self.session.get(PRAKAS_URL, timeout=30)
        return {
            "status": "ok" if resp.status_code == 200 else "error",
            "http_code": resp.status_code,
            "url": PRAKAS_URL,
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = CCFPrakasScraper()

    if len(sys.argv) < 2:
        print("Usage: bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        result = scraper.test()
        print(json.dumps(result, indent=2))
    elif command in ("bootstrap", "update"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        limit = 15 if sample_mode else 99999

        gen = scraper.fetch_all() if command == "bootstrap" else scraper.fetch_updates()

        for record in gen:
            count += 1
            if sample_mode:
                outpath = sample_dir / f"{count:04d}.json"
                outpath.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                print(f"[{count}] {record['title'][:60]} ({len(record['text'])} chars)")
            else:
                print(json.dumps(record, ensure_ascii=False))

            if count >= limit:
                break

        print(f"\nTotal records: {count}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
