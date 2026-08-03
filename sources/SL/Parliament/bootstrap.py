#!/usr/bin/env python3
"""
SL/Parliament -- Sierra Leone Parliament: Acts of Parliament

Fetches the full statute corpus of Sierra Leone published by Parliament on
parliament.gov.sl. The index (acts.html) is static HTML listing per-year pages
(acts-{YEAR}-{NN}.html, 1920..present); each year page links full-text Act PDFs
at /uploads/acts/{Title}.pdf (born-digital).

Strategy:
  - GET acts.html -> collect year-page links (acts-YYYY-NN.html)
  - GET each year page -> collect /uploads/acts/*.pdf links + anchor text
  - Download each PDF (URL-encode spaces) and extract full text

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap --full
  python bootstrap.py test
"""

import argparse
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Generator, List, Tuple
from urllib.parse import urljoin, quote, unquote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SL.Parliament")

BASE_URL = "https://www.parliament.gov.sl"
INDEX_URL = "https://www.parliament.gov.sl/acts.html"


class _PdfLinkParser(HTMLParser):
    """Collect (href, anchor_text) for .pdf links."""

    def __init__(self):
        super().__init__()
        self.in_a = False
        self.href = ""
        self.parts: List[str] = []
        self.links: List[Tuple[str, str]] = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            d = dict(attrs)
            href = d.get("href", "")
            if href.lower().endswith(".pdf") or "/uploads/acts/" in href.lower():
                self.in_a = True
                self.href = href
                self.parts = []

    def handle_data(self, data):
        if self.in_a:
            self.parts.append(data.strip())

    def handle_endtag(self, tag):
        if tag == "a" and self.in_a:
            self.links.append((self.href, " ".join(p for p in self.parts if p).strip()))
            self.in_a = False


def _encode_pdf_url(url: str) -> str:
    """Percent-encode the filename part of an /uploads/acts/ URL."""
    marker = "/uploads/acts/"
    if marker in url:
        head, tail = url.split(marker, 1)
        return head + marker + quote(unquote(tail))
    return url


def _slug(text: str, maxlen: int = 70) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return s[:maxlen] or "act"


def _title_from_url(url: str) -> str:
    name = unquote(url.rsplit("/", 1)[-1])
    name = re.sub(r"\.pdf$", "", name, flags=re.I)
    name = name.replace("_", " ")
    return re.sub(r"\s+", " ", name).strip()


class ParliamentScraper(BaseScraper):
    """Scraper for SL/Parliament -- Sierra Leone Acts of Parliament PDFs."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/605.1.15 (KHTML, like Gecko) Safari/605.1.15",
        })
        self.session.verify = False

    def _get(self, url: str, timeout: int = 60):
        for attempt in range(3):
            try:
                time.sleep(1.0)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    time.sleep(30)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning("Attempt %d failed for %s: %s", attempt + 1, url, e)
                time.sleep(5)
        return None

    def _year_pages(self) -> List[Tuple[str, str]]:
        """Return (year, absolute_url) for each acts-YYYY-NN.html page."""
        resp = self._get(INDEX_URL)
        if resp is None:
            return []
        out, seen = [], set()
        for href in re.findall(r'href="([^"]*acts-(\d{4})-\d+\.html)"', resp.text):
            page, year = href
            full = urljoin(INDEX_URL, unescape(page))
            if full not in seen:
                seen.add(full)
                out.append((year, full))
        out.sort()
        logger.info("Found %d year pages", len(out))
        return out

    def _discover_docs(self) -> List[Dict[str, str]]:
        """Return list of {url, title, year} for every Act PDF."""
        docs, seen = [], set()
        for year, page_url in self._year_pages():
            resp = self._get(page_url)
            if resp is None:
                continue
            parser = _PdfLinkParser()
            parser.feed(resp.text)
            for href, text in parser.links:
                full = urljoin(page_url, unescape(href))
                if "/uploads/acts/" not in full.lower():
                    continue
                enc = _encode_pdf_url(full)
                if enc in seen:
                    continue
                seen.add(enc)
                title = text or _title_from_url(full)
                docs.append({"url": enc, "title": title, "year": year})
        logger.info("Discovered %d Act PDF(s)", len(docs))
        return docs

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("doc_id", ""),
            "_source": "SL/Parliament",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", None),
            "url": raw.get("url", ""),
            "jurisdiction": "SL",
            "language": "en",
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        docs = self._discover_docs()
        if not docs:
            logger.error("No Act PDFs discovered")
            return
        count = 0
        for d in docs:
            url = d["url"]
            doc_id = _slug(_title_from_url(url))
            logger.info("Downloading: %s", d["title"])
            text = extract_pdf_markdown(
                source="SL/Parliament",
                source_id=doc_id,
                pdf_url=url,
                table="legislation",
            )
            if not text or len(text) < 300:
                logger.warning("Insufficient text (%d chars): %s",
                               len(text or ""), d["title"])
                continue
            # Prefer a year found in the title, else the year-page year.
            ym = re.search(r"\b(19\d{2}|20\d{2})\b", d["title"])
            year = ym.group(1) if ym else d["year"]
            yield {
                "doc_id": doc_id,
                "title": d["title"],
                "text": text,
                "date": f"{year}-01-01",
                "url": url,
            }
            count += 1
        logger.info("Completed: %d Acts fetched", count)

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def test(self) -> bool:
        docs = self._discover_docs()
        if not docs:
            logger.error("Cannot discover Act PDFs")
            return False
        d = docs[-1]
        logger.info("Testing download: %s", d["title"])
        text = extract_pdf_markdown(
            source="SL/Parliament",
            source_id="test",
            pdf_url=d["url"],
            table="legislation",
            force=True,
        )
        logger.info("PDF extraction: %d chars", len(text or ""))
        return bool(text)


def main():
    parser = argparse.ArgumentParser(description="SL/Parliament data fetcher")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast",
                                            "update", "test"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = ParliamentScraper()
    if args.command == "test":
        sys.exit(0 if scraper.test() else 1)
    elif args.command == "bootstrap":
        scraper.bootstrap(sample_mode=args.sample, sample_size=15)
    elif args.command == "bootstrap-fast":
        scraper.bootstrap_fast()
    elif args.command == "update":
        scraper.bootstrap(sample_mode=False)


if __name__ == "__main__":
    main()
