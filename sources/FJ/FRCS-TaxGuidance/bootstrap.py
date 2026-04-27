#!/usr/bin/env python3
"""
FJ/FRCS-TaxGuidance -- Fiji Revenue & Customs Service Tax Guidance

Fetches Standard Interpretation Guidelines (SIGs), Practice Statements, and
Draft SIGs from frcs.org.fj.  All documents are PDFs linked from dedicated
index pages.

Content: income tax, VAT, fringe benefits, incentives, stamp duty, ECAL,
         telecoms levy, capital gains tax, and more.  ~185 PDFs.

Strategy:
  1. Scrape SIG / Practice Statement / Draft SIG index pages for PDF links
  2. Download each PDF and extract text via common/pdf_extract

Usage:
  python bootstrap.py bootstrap          # Full pull (~185 PDFs)
  python bootstrap.py bootstrap --sample # Fetch ~12 sample records
  python bootstrap.py update             # (same as bootstrap -- static docs)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
import html as html_mod
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Tuple
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FJ.FRCS-TaxGuidance")

USER_AGENT = (
    "LegalDataHunter/1.0 (open-data research; "
    "https://github.com/worldwidelaw/legal-sources)"
)
BASE_URL = "https://www.frcs.org.fj"
REQUEST_DELAY = 1.5

# Index pages containing PDF links
INDEX_PAGES: List[Tuple[str, str]] = [
    (
        f"{BASE_URL}/our-services/legislation/sigs/standard-interpretation-guidelines/",
        "sig",
    ),
    (
        f"{BASE_URL}/our-services/legislation/sigs/practice-statements/",
        "practice_statement",
    ),
    (
        f"{BASE_URL}/our-services/legislation/sigs/draft-standard-interpretation-guidelines/",
        "draft_sig",
    ),
]

TAG_RE = re.compile(r"<[^>]+>")
PDF_LINK_RE = re.compile(
    r'href=["\']([^"\']+\.pdf)["\']', re.IGNORECASE
)
# Extract link text: <a href="...pdf">LINK TEXT</a>
LINK_WITH_TEXT_RE = re.compile(
    r'<a\s[^>]*href=["\']([^"\']+\.pdf)["\'][^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)


def _clean_text(text: str) -> str:
    """Strip HTML tags and decode entities."""
    text = TAG_RE.sub(" ", text)
    text = html_mod.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _http_get(url: str, timeout: int = 30, accept: str = "text/html") -> Optional[bytes]:
    req = Request(url, headers={"User-Agent": USER_AGENT, "Accept": accept})
    try:
        resp = urlopen(req, timeout=timeout)
        return resp.read()
    except (HTTPError, URLError) as e:
        logger.warning(f"HTTP error for {url}: {e}")
        return None


def _download_pdf(url: str, timeout: int = 60) -> Optional[bytes]:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        resp = urlopen(req, timeout=timeout)
        data = resp.read()
        if data and b"%PDF" in data[:20]:
            return data
    except (HTTPError, URLError) as e:
        logger.debug(f"PDF download failed for {url}: {e}")
    return None


def _extract_pdf_links(page_url: str) -> List[Tuple[str, str]]:
    """Fetch an index page and return list of (pdf_url, title) tuples."""
    data = _http_get(page_url)
    if not data:
        return []
    html_str = data.decode("utf-8", errors="replace")

    results = []
    seen = set()
    for match in LINK_WITH_TEXT_RE.finditer(html_str):
        raw_url = match.group(1).strip()
        raw_title = _clean_text(match.group(2))

        # Resolve relative URLs
        pdf_url = urljoin(page_url, raw_url)
        if pdf_url in seen:
            continue
        seen.add(pdf_url)

        # Derive title from link text or filename
        if not raw_title or len(raw_title) < 3:
            fname = pdf_url.rsplit("/", 1)[-1]
            raw_title = fname.replace(".pdf", "").replace("-", " ").replace("_", " ")

        results.append((pdf_url, raw_title))

    return results


class FRCSTaxGuidanceScraper(BaseScraper):
    """
    Scraper for FJ/FRCS-TaxGuidance.
    Country: FJ
    URL: https://www.frcs.org.fj

    Data types: doctrine
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _fetch_docs(self, max_records: int = 999999) -> Generator[dict, None, None]:
        count = 0
        for page_url, doc_type in INDEX_PAGES:
            if count >= max_records:
                return

            logger.info(f"Fetching index: {page_url.split('/')[-2]} ({doc_type})")
            time.sleep(REQUEST_DELAY)
            links = _extract_pdf_links(page_url)
            logger.info(f"  Found {len(links)} PDF links")

            for pdf_url, title in links:
                if count >= max_records:
                    return

                time.sleep(REQUEST_DELAY)
                pdf_bytes = _download_pdf(pdf_url)
                if not pdf_bytes:
                    logger.warning(f"  PDF download failed: {title[:60]} ({pdf_url})")
                    continue

                # Derive a stable doc_id from the filename
                fname = pdf_url.rsplit("/", 1)[-1]
                doc_id = re.sub(r"\.pdf$", "", fname, flags=re.IGNORECASE)
                doc_id = re.sub(r"[^a-zA-Z0-9_-]", "_", doc_id)[:120]
                source_id = f"{doc_type}-{doc_id}"

                text = extract_pdf_markdown(
                    source="FJ/FRCS-TaxGuidance",
                    source_id=source_id,
                    pdf_bytes=pdf_bytes,
                    table="doctrine",
                ) or ""

                if not text or len(text) < 100:
                    logger.warning(f"  Insufficient text for {title[:60]}: {len(text)} chars")
                    continue

                yield {
                    "doc_id": doc_id,
                    "doc_type": doc_type,
                    "title": title,
                    "text": text,
                    "url": pdf_url,
                }
                count += 1
                logger.info(f"  [{count}] {title[:60]} ({len(text)} chars)")

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._fetch_docs()

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        yield from self._fetch_docs()

    def normalize(self, raw: dict) -> dict:
        doc_type = raw.get("doc_type", "sig")
        doc_id = raw.get("doc_id", "unknown")

        return {
            "_id": f"{doc_type}-{doc_id}",
            "_source": "FJ/FRCS-TaxGuidance",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw["text"],
            "date": None,
            "url": raw.get("url", ""),
            "doc_id": doc_id,
            "doc_type": doc_type,
        }


if __name__ == "__main__":
    scraper = FRCSTaxGuidanceScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        data = _http_get(
            f"{BASE_URL}/our-services/legislation/sigs/standard-interpretation-guidelines/"
        )
        if data and b".pdf" in data:
            print("OK: SIG index page reachable and contains PDF links")
        else:
            print("FAIL: Cannot reach SIG index or no PDF links found")
            sys.exit(1)

    elif command in ("bootstrap", "update"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        limit = 12 if sample else 999999

        if sample:
            logger.info("=== SAMPLE MODE: fetching ~12 records ===")

        for raw in scraper._fetch_docs(max_records=limit):
            record = scraper.normalize(raw)
            out_file = sample_dir / f"{record['_id']}.json"
            out_file.write_text(json.dumps(record, indent=2, ensure_ascii=False))
            count += 1
            logger.info(f"Saved [{count}]: {record['title'][:70]}")

        logger.info(f"Done. Total records: {count}")
        if count == 0:
            logger.error("No records fetched — check connectivity")
            sys.exit(1)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
