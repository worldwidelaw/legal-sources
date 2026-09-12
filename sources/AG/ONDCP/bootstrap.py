#!/usr/bin/env python3
"""
AG/ONDCP -- Antigua & Barbuda ONDCP AML/CFT Regulatory Library

The Office of National Drug and Money Laundering Control Policy (ONDCP) is
Antigua & Barbuda's AML/CFT supervisory authority. It publishes the country's
money-laundering, proceeds-of-crime and terrorism-financing legislation,
regulations, guidelines, directives and conventions as PDFs at ondcp.gov.ag.

Strategy:
  - Crawl the five law sections (/laws/{statutes,regulations,guidelines,
    directives,conventions})
  - Parse anchors linking to /files/{section}/*.pdf, capturing the link text
    as the document title
  - Download each PDF and extract text via pdfplumber

Many older statutes/directives are scanned image PDFs with no text layer; those
yield no extractable text and are skipped. Born-digital documents (most
guidelines, recent statutes, conventions) extract full text cleanly.

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
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import pdfplumber
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AG.ONDCP")

BASE_URL = "https://ondcp.gov.ag"
SECTIONS = ["statutes", "regulations", "guidelines", "directives", "conventions"]
MIN_TEXT_CHARS = 200
MAX_PDF_SIZE = 60 * 1024 * 1024  # 60MB

YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")


class AGONDCPScraper(BaseScraper):
    """Scraper for AG/ONDCP -- Antigua & Barbuda AML/CFT Regulatory Library."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Legal-Data-Hunter/1.0; +https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                time.sleep(2)
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

    def _parse_section(self, section: str) -> List[Dict[str, str]]:
        """Parse a section page, returning docs that genuinely belong to it.

        Only anchors pointing at /files/{section}/*.pdf are kept, which excludes
        the cross-linked advisory/form PDFs shown in page sidebars.
        """
        url = f"{BASE_URL}/laws/{section}"
        resp = self._request(url)
        if resp is None:
            return []

        documents = []
        seen = set()
        anchor_re = re.compile(
            r'<a[^>]+href="([^"]*?/files/' + re.escape(section) + r'/[^"]+\.pdf)"[^>]*>(.*?)</a>',
            re.IGNORECASE | re.DOTALL,
        )
        for match in anchor_re.finditer(resp.text):
            href = match.group(1)
            if href.startswith("/"):
                href = BASE_URL + href
            elif not href.startswith("http"):
                href = f"{BASE_URL}/{href.lstrip('/')}"
            if href in seen:
                continue
            seen.add(href)

            title = re.sub(r"<[^>]+>", " ", match.group(2))
            title = re.sub(r"\s+", " ", title).strip()
            if not title:
                # Fall back to a humanised filename
                title = re.sub(r"[-_]+", " ", href.rsplit("/", 1)[-1].rsplit(".", 1)[0]).strip()

            documents.append({
                "title": title,
                "section": section,
                "download_url": href,
            })

        return documents

    def _extract_pdf_text(self, download_url: str) -> str:
        resp = self._request(download_url, timeout=120)
        if resp is None:
            return ""

        pdf_bytes = resp.content
        if len(pdf_bytes) > MAX_PDF_SIZE:
            logger.warning(f"PDF too large ({len(pdf_bytes)} bytes): {download_url}")
            return ""
        if len(pdf_bytes) < 200:
            return ""

        try:
            pdf = pdfplumber.open(io.BytesIO(pdf_bytes))
            parts = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    parts.append(text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            pdf.close()
            return "\n\n".join(parts).strip()
        except Exception as e:
            logger.warning(f"PDF extraction failed for {download_url}: {e}")
            return ""

    @staticmethod
    def _slug(url: str) -> str:
        name = url.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        return re.sub(r"[^A-Za-z0-9]+", "-", name).strip("-")

    @staticmethod
    def _guess_date(title: str) -> str:
        m = YEAR_RE.search(title)
        if m:
            return f"{m.group(0)}-01-01"
        return ""

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("doc_id", ""),
            "_source": "AG/ONDCP",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("download_url", ""),
            "category": raw.get("section", ""),
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        count = 0
        for section in SECTIONS:
            if max_records and count >= max_records:
                return

            docs = self._parse_section(section)
            logger.info(f"Section '{section}': {len(docs)} PDFs listed")

            for doc in docs:
                if max_records and count >= max_records:
                    return

                text = self._extract_pdf_text(doc["download_url"])
                if not text or len(text) < MIN_TEXT_CHARS:
                    logger.warning(
                        f"Skipping (scanned/no text, {len(text)} chars): {doc['title'][:60]}"
                    )
                    continue

                doc_id = f"AG-ONDCP-{section}-{self._slug(doc['download_url'])}"
                raw = {
                    "doc_id": doc_id,
                    "title": doc["title"],
                    "text": text,
                    "date": self._guess_date(doc["title"]),
                    "download_url": doc["download_url"],
                    "section": section,
                }
                count += 1
                yield raw

        logger.info(f"Completed: {count} documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all(max_records=30)

    def test(self) -> bool:
        docs = self._parse_section("statutes")
        if not docs:
            logger.error("Cannot parse statutes section")
            return False
        logger.info(f"Statutes section OK: {len(docs)} PDFs")

        for doc in docs:
            text = self._extract_pdf_text(doc["download_url"])
            if len(text) >= MIN_TEXT_CHARS:
                logger.info(f"Extracted {len(text)} chars from: {doc['title'][:60]}")
                return True
        logger.error("No statute yielded extractable text")
        return False


def main():
    parser = argparse.ArgumentParser(description="AG/ONDCP data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = AGONDCPScraper()

    if args.command == "test":
        sys.exit(0 if scraper.test() else 1)

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
            logger.info(
                f"[{count + 1}] {normalized.get('title', '?')[:80]} "
                f"({len(normalized.get('text', '')):,} chars)"
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
