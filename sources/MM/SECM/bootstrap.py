#!/usr/bin/env python3
"""
MM/SECM -- Myanmar Securities and Exchange Commission Instructions

Fetches securities regulatory instructions (prakas/directives) from the
Myanmar Securities and Exchange Commission. Documents are PDFs covering
insider trading, prospectus requirements, AML/CFT, and market regulation.

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
logger = logging.getLogger("legal-data-hunter.MM.SECM")

BASE_URL = "https://secm.gov.mm"
INSTRUCTIONS_URL = f"{BASE_URL}/en/instructions/"


class SECMScraper(BaseScraper):
    """
    Scraper for MM/SECM -- Myanmar Securities and Exchange Commission Instructions.
    Country: MM
    URL: https://secm.gov.mm/en/instructions/

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

    def _parse_instruction_number_and_date(self, title: str, filename: str) -> tuple[Optional[str], Optional[str]]:
        """Extract instruction number and approximate date from title or filename.

        Instruction numbers like '3/2020' or '1/2016' encode the year.
        """
        # Try to find pattern like N/YYYY in the title
        match = re.search(r'(\d{1,2})/(\d{4})', title)
        if match:
            num = match.group(1)
            year = match.group(2)
            return f"{num}/{year}", f"{year}-01-01"

        # Try filename
        match = re.search(r'(\d{1,2})[._-](\d{4})', filename)
        if match:
            num = match.group(1)
            year = match.group(2)
            y = int(year)
            if 2010 <= y <= 2030:
                return f"{num}/{year}", f"{year}-01-01"

        return None, None

    def _get_document_list(self) -> list[dict]:
        """Scrape all PDF links from the SECM instructions page."""
        from bs4 import BeautifulSoup

        try:
            resp = self.session.get(INSTRUCTIONS_URL, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch instructions page: {e}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        by_filename: dict[str, dict] = {}

        for link in soup.find_all("a", href=True):
            href = link["href"]
            if ".pdf" not in href.lower():
                continue

            pdf_url = urljoin(INSTRUCTIONS_URL, href)
            filename = unquote(pdf_url.split("/")[-1])

            if filename in by_filename:
                if "/wp-content/" in pdf_url:
                    by_filename[filename]["pdf_url"] = pdf_url
                continue

            link_text = link.get_text(strip=True)

            # Walk up to find the row/context with the full title and instruction number
            number_cell_text = ""
            parent_row = link.find_parent("tr")
            if parent_row:
                cells = parent_row.find_all("td")
                if len(cells) >= 2:
                    number_cell_text = cells[0].get_text(strip=True)
                    title_cell = cells[1].get_text(strip=True)
                    if len(title_cell) > 15:
                        link_text = title_cell

            if not link_text or len(link_text) < 10 or link_text.lower() in ("view", "download", "pdf"):
                link_text = re.sub(r'\.pdf$', '', filename, flags=re.IGNORECASE)
                link_text = re.sub(r'[-_]+', ' ', link_text).strip()

            # Check number cell first (e.g. "(3/2020)"), then title, then filename
            inst_num, date = self._parse_instruction_number_and_date(number_cell_text, filename)
            if not inst_num:
                inst_num, date = self._parse_instruction_number_and_date(link_text, filename)

            by_filename[filename] = {
                "title": link_text,
                "date": date,
                "instruction_number": inst_num,
                "pdf_url": pdf_url,
                "filename": filename,
            }

        documents = list(by_filename.values())
        logger.info(f"Found {len(documents)} instruction PDFs")
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
        doc_id = f"MM-SECM-{url_hash}"

        return {
            "_id": doc_id,
            "_source": "MM/SECM",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": raw["pdf_url"],
            "instruction_number": raw.get("instruction_number"),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all SECM instruction documents with full PDF text."""
        documents = self._get_document_list()
        logger.info(f"Processing {len(documents)} instruction documents")

        yielded = 0
        skipped = 0

        for i, doc in enumerate(documents):
            logger.info(f"[{i+1}/{len(documents)}] Downloading: {doc['title'][:60]}")

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
        resp = self.session.get(INSTRUCTIONS_URL, timeout=30)
        return {
            "status": "ok" if resp.status_code == 200 else "error",
            "http_code": resp.status_code,
            "url": INSTRUCTIONS_URL,
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = SECMScraper()

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
