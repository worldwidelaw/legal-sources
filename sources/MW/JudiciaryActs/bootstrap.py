#!/usr/bin/env python3
"""
MW/JudiciaryActs -- Malawi Judiciary: Acts of Parliament

Fetches core consolidated Acts of Parliament published by the Malawi Judiciary
on judiciary.mw. The "Acts of Parliament" resource page is a paginated Drupal
document table; each Act has a full-text PDF attachment under
/sites/default/files/{YYYY-MM}/{Name}.pdf (mostly born-digital).

This is the legislation companion to MW/Judiciary (case_law) on the same
official domain, distinct from MW/MalawiLII (the LII aggregator).

Strategy:
  - Scrape both listing pages (?page=0, ?page=1) for PDF hrefs
  - Download each PDF and extract full text via common.pdf_extract
    (OCR fallback handles the occasional scanned Act)

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
from pathlib import Path
from typing import Any, Dict, Generator, List, Tuple
from urllib.parse import urljoin, unquote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MW.JudiciaryActs")

BASE_URL = "https://www.judiciary.mw"
LISTING_URL = ("https://www.judiciary.mw/resources/legal-instruments/"
               "acts-of-parliament")
# The table paginates; a couple of extra pages are probed harmlessly.
LISTING_PAGES = [f"{LISTING_URL}?page={i}" for i in range(0, 4)]


def _slug(text: str, maxlen: int = 60) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return s[:maxlen] or "act"


def _title_from_url(url: str) -> str:
    name = unquote(url.rsplit("/", 1)[-1])
    name = re.sub(r"\.pdf$", "", name, flags=re.I)
    return re.sub(r"\s+", " ", name).strip()


def _date_from_url(url: str) -> str:
    m = re.search(r"/(\d{4})-(\d{2})/", url)
    if m:
        return f"{m.group(1)}-{m.group(2)}-01"
    return None


class JudiciaryActsScraper(BaseScraper):
    """Scraper for MW/JudiciaryActs -- Malawi Acts of Parliament PDFs."""

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
                time.sleep(1.5)
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
                time.sleep(6)
        return None

    def _discover_pdfs(self) -> List[Tuple[str, str]]:
        """Return unique (title, url) for each Act PDF."""
        seen, out = set(), []
        for page in LISTING_PAGES:
            resp = self._get(page)
            if resp is None:
                continue
            hrefs = re.findall(r'href="([^"]+\.pdf)"', resp.text, re.I)
            new_here = 0
            for href in hrefs:
                full = urljoin(BASE_URL, href)
                if full in seen:
                    continue
                seen.add(full)
                out.append((_title_from_url(full), full))
                new_here += 1
            # Stop paginating once a page yields no new PDFs.
            if new_here == 0 and out:
                break
        logger.info("Discovered %d Act PDF(s)", len(out))
        return out

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("doc_id", ""),
            "_source": "MW/JudiciaryActs",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", None),
            "url": raw.get("url", ""),
            "jurisdiction": "MW",
            "language": "en",
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        docs = self._discover_pdfs()
        if not docs:
            logger.error("No Act PDFs discovered")
            return
        count = 0
        for title, url in docs:
            doc_id = _slug(_title_from_url(url), 60)
            logger.info("Downloading: %s", title)
            text = extract_pdf_markdown(
                source="MW/JudiciaryActs",
                source_id=doc_id,
                pdf_url=url,
                table="legislation",
            )
            if not text or len(text) < 300:
                logger.warning("Insufficient text (%d chars): %s",
                               len(text or ""), title)
                continue
            yield {
                "doc_id": doc_id,
                "title": title,
                "text": text,
                "date": _date_from_url(url),
                "url": url,
            }
            count += 1
        logger.info("Completed: %d Acts fetched", count)

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def test(self) -> bool:
        docs = self._discover_pdfs()
        if not docs:
            logger.error("Cannot discover Act PDFs")
            return False
        title, url = docs[0]
        logger.info("Testing download: %s", title)
        text = extract_pdf_markdown(
            source="MW/JudiciaryActs",
            source_id="test",
            pdf_url=url,
            table="legislation",
            force=True,
        )
        logger.info("PDF extraction: %d chars", len(text or ""))
        return bool(text)


def main():
    parser = argparse.ArgumentParser(description="MW/JudiciaryActs data fetcher")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast",
                                            "update", "test"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = JudiciaryActsScraper()
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
