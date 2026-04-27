#!/usr/bin/env python3
"""
TC/RevisedLaws -- Turks and Caicos Revised Laws (Attorney General)

Fetches consolidated legislation from the TCI Attorney General's site.
PDFs are served via Joomla eDocman component at gov.tc/agc/.

Strategy:
  1. Scrape listing pages for edocman viewdocument links
  2. Download each PDF
  3. Extract full text via pdfplumber/pypdf

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Re-fetch all
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from html.parser import HTMLParser
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TC.RevisedLaws")

BASE_URL = "https://gov.tc"

# Listing pages to scrape
LISTING_PAGES = [
    ("/agc/2021-revised-laws", "2021", "revised"),
    ("/agc/2023-ordinances", "2023", "ordinance"),
    ("/agc/2022-ordinances", "2022", "ordinance"),
    ("/agc/2023-subsidiary-legislation", "2023", "subsidiary"),
    ("/agc/2022-subsidiary-legislation", "2022", "subsidiary"),
    ("/agc/2021-ordinances", "2021", "ordinance"),
    ("/agc/2020-ordinances", "2020", "ordinance"),
    ("/agc/2019-ordinances", "2019", "ordinance"),
    ("/agc/2018-ordinances", "2018", "ordinance"),
    ("/agc/2017-ordinances", "2017", "ordinance"),
    ("/agc/2016-ordinances", "2016", "ordinance"),
    ("/agc/2015-ordinances", "2015", "ordinance"),
    ("/agc/2021-subsidiary-legislation", "2021", "subsidiary"),
    ("/agc/2020-subsidiary-legislation", "2020", "subsidiary"),
    ("/agc/2019-subsidiary-legislation", "2019", "subsidiary"),
    ("/agc/2018-subsidiary-legislation", "2018", "subsidiary"),
    ("/agc/2017-subsidiary-legislation", "2017", "subsidiary"),
    ("/agc/2016-subsidiary-legislation", "2016", "subsidiary"),
    ("/agc/2015-subsidiary-legislation", "2015", "subsidiary"),
]


class _EdocmanLinkParser(HTMLParser):
    """Extract eDocman viewdocument links and their text from HTML."""

    def __init__(self):
        super().__init__()
        self.links: List[Tuple[str, str]] = []  # (href, text)
        self._current_href: Optional[str] = None
        self._current_text: List[str] = []
        self._in_a = False

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            href = dict(attrs).get("href", "")
            if "viewdocument" in href:
                self._in_a = True
                self._current_href = href
                self._current_text = []

    def handle_data(self, data):
        if self._in_a:
            self._current_text.append(data)

    def handle_endtag(self, tag):
        if tag == "a" and self._in_a:
            self._in_a = False
            if self._current_href:
                text = " ".join(self._current_text).strip()
                if text:
                    self.links.append((self._current_href, text))
            self._current_href = None
            self._current_text = []


def _parse_edocman_links(html: str) -> List[Tuple[str, str]]:
    """Parse HTML and return list of (href, title) for eDocman documents."""
    parser = _EdocmanLinkParser()
    parser.feed(html)
    return parser.links


def _extract_doc_id(href: str) -> str:
    """Extract numeric document ID from eDocman URL."""
    # Pattern: /agc/component/edocman/{slug}/viewdocument/{ID}
    m = re.search(r"/viewdocument/(\d+)", href)
    return m.group(1) if m else ""


def _clean_title(text: str) -> str:
    """Clean up link text to a proper title."""
    text = re.sub(r"\s+", " ", text).strip()
    # Remove leading number patterns like "01.01 " or "1 of 2023 - "
    # Keep them actually — they're useful identifiers
    return text


class TCRevisedLawsScraper(BaseScraper):
    """Scraper for TC/RevisedLaws -- Turks and Caicos Legislation."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
            },
            timeout=120,
        )

    def _fetch_page(self, path: str) -> str:
        """Fetch an HTML page and return its content."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(path)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch page {path}: {e}")
            return ""

    def _download_pdf(self, path: str) -> Optional[bytes]:
        """Download a PDF and return raw bytes."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(path)
            resp.raise_for_status()
            content = resp.content
            if content and content[:5] == b"%PDF-":
                return content
            # Some eDocman links redirect or serve HTML — check
            if content and b"<html" in content[:500].lower():
                logger.warning(f"Got HTML instead of PDF for {path}")
                return None
            # Accept even if not starting with %PDF (some have BOM)
            if content and len(content) > 100:
                return content
            logger.warning(f"Empty or invalid response for {path}")
            return None
        except Exception as e:
            logger.warning(f"Failed to download PDF {path}: {e}")
            return None

    def _collect_documents(self) -> List[Dict[str, Any]]:
        """Scrape all listing pages and collect document metadata."""
        seen_ids = set()
        documents = []

        for page_path, year, leg_type in LISTING_PAGES:
            html = self._fetch_page(page_path)
            if not html:
                logger.warning(f"Empty page: {page_path}")
                continue

            links = _parse_edocman_links(html)
            page_count = 0

            for href, title in links:
                doc_id = _extract_doc_id(href)
                if not doc_id:
                    continue

                # Skip index/reference documents
                lower_title = title.lower()
                if any(skip in lower_title for skip in [
                    "list of titles", "general index", "preliminary booklet",
                    "table of contents", "chronological table"
                ]):
                    continue

                if doc_id in seen_ids:
                    continue
                seen_ids.add(doc_id)

                # Build download URL
                if href.startswith("http"):
                    download_url = href
                elif href.startswith("/"):
                    download_url = href
                else:
                    download_url = f"/agc/{href}"

                # Strip ?Itemid= suffix for cleaner URLs
                download_url = re.sub(r"\?Itemid=.*$", "", download_url)

                documents.append({
                    "doc_id": doc_id,
                    "title": _clean_title(title),
                    "year": year,
                    "type": leg_type,
                    "download_path": download_url,
                    "page_url": f"{BASE_URL}{page_path}",
                })
                page_count += 1

            logger.info(f"Page {page_path}: {page_count} documents found")

        logger.info(f"Total unique documents collected: {len(documents)}")
        return documents

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        doc_id = raw.get("doc_id", "")
        title = raw.get("title", "Untitled")
        year = raw.get("year", "")
        leg_type = raw.get("type", "revised")

        return {
            "_id": f"TC/RevisedLaws/{doc_id}",
            "_source": "TC/RevisedLaws",
            "_type": "legislation",
            "_fetched_at": now,
            "title": title,
            "text": raw.get("text", ""),
            "date": year,
            "url": f"{BASE_URL}{raw.get('download_path', '')}",
            "legislation_id": doc_id,
            "legislation_type": leg_type,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        documents = self._collect_documents()
        count = 0
        errors = 0

        for doc in documents:
            pdf_bytes = self._download_pdf(doc["download_path"])
            if not pdf_bytes:
                errors += 1
                continue

            text = extract_pdf_markdown(
                source="TC/RevisedLaws",
                source_id=doc["doc_id"],
                pdf_bytes=pdf_bytes,
                table="legislation",
            ) or ""

            if not text or len(text.strip()) < 50:
                logger.warning(
                    f"Insufficient text for {doc['doc_id']} ({doc['title']}): "
                    f"{len(text)} chars"
                )
                errors += 1
                continue

            doc["text"] = text
            yield doc
            count += 1

            if count % 25 == 0:
                logger.info(f"Progress: {count} records, {errors} errors")

        logger.info(f"Completed: {count} records, {errors} errors")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()


# ── CLI ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = TCRevisedLawsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing page fetch...")
        html = scraper._fetch_page("/agc/2021-revised-laws")
        if not html:
            logger.error("FAILED — could not fetch 2021 revised laws page")
            sys.exit(1)

        links = _parse_edocman_links(html)
        if not links:
            logger.error("FAILED — no edocman links found")
            sys.exit(1)
        logger.info(f"OK — found {len(links)} document links")

        # Test one PDF download
        href, title = links[2]  # Skip index documents
        doc_id = _extract_doc_id(href)
        download_path = re.sub(r"\?Itemid=.*$", "", href)

        logger.info(f"Testing PDF download: {title} (ID {doc_id})")
        pdf_bytes = scraper._download_pdf(download_path)
        if not pdf_bytes:
            logger.error("FAILED — PDF download failed")
            sys.exit(1)

        text = extract_pdf_markdown(
            source="TC/RevisedLaws",
            source_id=doc_id,
            pdf_bytes=pdf_bytes,
            table="legislation",
        ) or ""
        logger.info(f"OK — PDF extracted, {len(text)} chars from '{title}'")

        if len(text) > 100:
            logger.info(f"Text preview: {text[:200]}...")
        else:
            logger.warning(f"Text too short: {len(text)} chars")

    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
