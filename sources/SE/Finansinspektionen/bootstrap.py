#!/usr/bin/env python3
"""
SE/Finansinspektionen — Swedish Financial Supervisory Authority Sanctions

Fetches sanctions/enforcement decisions from Finansinspektionen (FI).

Strategy:
  - Crawl listing pages for 5 sanctions categories via cumulative pagination
  - Fetch each detail page for summary text + PDF link
  - Download PDFs and extract full decision text via pdfplumber
  - Fall back to HTML summary if PDF unavailable

Categories:
  - finansiella-foretag: Sanctions against financial firms (~206)
  - marknadsmissbruk: Market abuse sanctions (~249)
  - marknadsinformation: Market information/insider reporting (~876)
  - foretag-utan-tillstand: Firms operating without license (~35)
  - redovisningstillsyn: Financial reporting supervision (~5)

Data:
  - ~1,370 decisions from 2002-2026
  - Language: Swedish
  - License: PSI Directive / Swedish open government data

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
"""

import argparse
import html
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests

try:
    import pdfplumber
    HAS_PDF = True
except ImportError:
    HAS_PDF = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://www.fi.se"
SOURCE_ID = "SE/Finansinspektionen"
SAMPLE_DIR = Path(__file__).parent / "sample"
REQUEST_DELAY = 1.5

CATEGORIES = [
    ("finansiella-foretag", "Financial firms"),
    ("marknadsmissbruk", "Market abuse"),
    ("marknadsinformation", "Market information"),
    ("foretag-utan-tillstand", "Firms without license"),
    ("redovisningstillsyn", "Financial reporting supervision"),
]


class _HTMLTextExtractor(HTMLParser):
    """Strip HTML tags and extract plain text."""

    def __init__(self):
        super().__init__()
        self._pieces: List[str] = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "noscript"):
            self._skip = True
        elif tag in ("br", "p", "div", "h1", "h2", "h3", "h4", "h5", "h6", "li", "tr"):
            self._pieces.append("\n")

    def handle_endtag(self, tag):
        if tag in ("script", "style", "noscript"):
            self._skip = False
        elif tag in ("p", "div", "h1", "h2", "h3", "h4", "h5", "h6", "li", "tr", "table"):
            self._pieces.append("\n")

    def handle_data(self, data):
        if not self._skip:
            self._pieces.append(data)

    def get_text(self) -> str:
        raw = "".join(self._pieces)
        lines = [line.strip() for line in raw.splitlines()]
        lines = [line for line in lines if line]
        return "\n".join(lines)


def strip_html(html_content: str) -> str:
    decoded = html.unescape(html_content)
    extractor = _HTMLTextExtractor()
    extractor.feed(decoded)
    return extractor.get_text()


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    if not HAS_PDF:
        return ""
    try:
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
            return "\n\n".join(pages)
    except Exception as e:
        logger.warning("PDF extraction failed: %s", e)
        return ""


class FinansinspektionenFetcher:
    """Fetcher for FI sanctions decisions via HTML scraping + PDF extraction."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml",
        })

    def _get(self, url: str) -> Optional[str]:
        """Fetch a URL with retry logic."""
        time.sleep(REQUEST_DELAY)
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                return resp.text
            except requests.RequestException as e:
                if attempt < 2:
                    logger.warning("Retry %d for %s: %s", attempt + 1, url, e)
                    time.sleep(5 * (attempt + 1))
                else:
                    logger.error("Failed to fetch %s: %s", url, e)
                    return None

    def _get_bytes(self, url: str) -> Optional[bytes]:
        """Fetch binary content (PDF)."""
        time.sleep(REQUEST_DELAY)
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=60)
                resp.raise_for_status()
                return resp.content
            except requests.RequestException as e:
                if attempt < 2:
                    logger.warning("Retry %d for PDF %s: %s", attempt + 1, url, e)
                    time.sleep(5 * (attempt + 1))
                else:
                    logger.error("Failed to fetch PDF %s: %s", url, e)
                    return None

    def _parse_listing(self, html_text: str) -> List[Dict[str, str]]:
        """Parse listing page HTML to extract decision entries."""
        items = []
        pattern = re.compile(
            r'<div class="list-item extended-click-area">\s*'
            r'<h2><a href="([^"]+)">(.+?)</a></h2>\s*'
            r'<div class="date-and-categories">\s*'
            r'<span class="date">(\d{4}-\d{2}-\d{2})</span>',
            re.DOTALL,
        )
        for m in pattern.finditer(html_text):
            href, title_html, date = m.group(1), m.group(2), m.group(3)
            title = html.unescape(title_html).strip()
            url = href if href.startswith("http") else BASE_URL + href
            items.append({"url": url, "title": title, "date": date})
        return items

    def _parse_detail(self, html_text: str) -> Tuple[str, Optional[str]]:
        """Extract summary text and PDF URL from detail page."""
        # Extract lead paragraph
        lead = ""
        m = re.search(r'<p class="lead">(.*?)</p>', html_text, re.DOTALL)
        if m:
            lead = strip_html(m.group(1))

        # Extract editor-content body
        body = ""
        m = re.search(
            r'<div class="editor-content">(.*?)</div>\s*(?:<div class="|</article|<div class="link-list")',
            html_text,
            re.DOTALL,
        )
        if m:
            body = strip_html(m.group(1))

        summary = (lead + "\n\n" + body).strip() if lead else body.strip()

        # Extract PDF link (unquoted href)
        pdf_url = None
        m = re.search(r'<div class="link-list">(.*?)</div>', html_text, re.DOTALL)
        if m:
            link_html = m.group(1)
            pm = re.search(r'href=([^\s>]+\.pdf)', link_html, re.I)
            if pm:
                pdf_path = pm.group(1)
                pdf_url = pdf_path if pdf_path.startswith("http") else BASE_URL + pdf_path

        return summary, pdf_url

    def _fetch_category(self, cat_slug: str, cat_name: str) -> List[Dict[str, str]]:
        """Fetch all items from a category listing."""
        url = f"{BASE_URL}/sv/publicerat/sanktioner/{cat_slug}/?page=2000"
        logger.info("Fetching category: %s (%s)", cat_name, cat_slug)
        page_html = self._get(url)
        if not page_html:
            return []
        items = self._parse_listing(page_html)
        logger.info("  Found %d items in %s", len(items), cat_name)
        for item in items:
            item["category"] = cat_name
        return items

    def normalize(self, item: dict, summary: str, pdf_text: str) -> Optional[Dict[str, Any]]:
        """Normalize a decision into a standard record."""
        text = pdf_text if pdf_text and len(pdf_text) > len(summary) else summary

        if not text or len(text) < 50:
            return None

        slug = item["url"].rstrip("/").split("/")[-1]
        doc_id = f"SE-FI-{item['date']}-{slug[:60]}"

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": item["title"],
            "text": text,
            "date": item["date"],
            "url": item["url"],
            "category": item.get("category", ""),
        }

    def fetch_all(self, sample: bool = False, max_docs: int = 15) -> Iterator[Dict[str, Any]]:
        """Yield all decisions across all categories."""
        count = 0
        for cat_slug, cat_name in CATEGORIES:
            items = self._fetch_category(cat_slug, cat_name)
            for item in items:
                detail_html = self._get(item["url"])
                if not detail_html:
                    continue

                summary, pdf_url = self._parse_detail(detail_html)

                pdf_text = ""
                if pdf_url and HAS_PDF:
                    pdf_bytes = self._get_bytes(pdf_url)
                    if pdf_bytes:
                        pdf_text = extract_pdf_text(pdf_bytes)

                doc = self.normalize(item, summary, pdf_text)
                if doc:
                    count += 1
                    yield doc

                if sample and count >= max_docs:
                    return

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        """Yield decisions published after a given date."""
        since_date = since[:10]
        for cat_slug, cat_name in CATEGORIES:
            items = self._fetch_category(cat_slug, cat_name)
            for item in items:
                if item["date"] < since_date:
                    continue
                detail_html = self._get(item["url"])
                if not detail_html:
                    continue
                summary, pdf_url = self._parse_detail(detail_html)
                pdf_text = ""
                if pdf_url and HAS_PDF:
                    pdf_bytes = self._get_bytes(pdf_url)
                    if pdf_bytes:
                        pdf_text = extract_pdf_text(pdf_bytes)
                doc = self.normalize(item, summary, pdf_text)
                if doc:
                    yield doc


def bootstrap(sample: bool = False, full: bool = False, since: Optional[str] = None):
    """Main entry point."""
    fetcher = FinansinspektionenFetcher()
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    if since:
        docs = fetcher.fetch_updates(since)
    else:
        docs = fetcher.fetch_all(sample=sample)

    count = 0
    for doc in docs:
        count += 1
        text_len = len(doc.get("text", ""))
        logger.info("  → %s | text=%d chars | date=%s", doc["title"][:70], text_len, doc.get("date"))

        if sample:
            sample_path = SAMPLE_DIR / f"{doc['_id']}.json"
            with open(sample_path, "w", encoding="utf-8") as f:
                json.dump(doc, f, ensure_ascii=False, indent=2)

    logger.info("Done: %d decisions fetched", count)
    return count


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    parser = argparse.ArgumentParser(description="SE/Finansinspektionen bootstrap")
    parser.add_argument("command", choices=["bootstrap"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Save sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--since", type=str, help="Fetch updates since date (ISO 8601)")
    args = parser.parse_args()

    if args.command == "bootstrap":
        count = bootstrap(sample=args.sample, full=args.full, since=args.since)
        if count == 0:
            logger.error("No records fetched!")
            sys.exit(1)
