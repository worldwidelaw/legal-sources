#!/usr/bin/env python3
"""
BB/SubsidiaryLegislation -- Barbados Subsidiary Legislation (Attorney General)

Fetches subsidiary legislation (regulations, orders, rules) from the Office of
the Attorney General & Legal Affairs at oag.gov.bb. Documents are listed on a
single page with direct PDF download links. Full text extracted via pdf_extract.

Endpoint:
  - Listing: https://oag.gov.bb/Laws/Consolidated-Laws/Subsidiary-Legislation-of-Barbados/
  - PDFs: https://oag.gov.bb/attachments/{Instrument-Name}.pdf

Data:
  - 400+ subsidiary legislation instruments
  - Full text extracted from PDFs
  - Language: English

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import html as html_mod
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Tuple
from urllib.parse import unquote, quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BB.SubsidiaryLegislation")

BASE_URL = "https://oag.gov.bb"
LISTING_URL = f"{BASE_URL}/Laws/Consolidated-Laws/Subsidiary-Legislation-of-Barbados/"

LINK_RE = re.compile(
    r'<a[^>]+href=["\'](/attachments/[^"\']+\.[pP][dD][fF])["\'][^>]*>(.*?)</a>',
    re.DOTALL | re.IGNORECASE,
)
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
CAP_RE = re.compile(r"Cap\s*(\d+[A-Z]?(?:'[A-Z])?)", re.IGNORECASE)
YEAR_RE = re.compile(r",?\s*(\d{4})\s+Cap", re.IGNORECASE)
FILEINFO_RE = re.compile(r"\s*Adobe PDF Document,\s*[\d.,]+\s*[KMG]B\s*$", re.IGNORECASE)


def strip_html(s: str) -> str:
    text = TAG_RE.sub(" ", s)
    text = html_mod.unescape(text)
    text = WS_RE.sub(" ", text).strip()
    text = FILEINFO_RE.sub("", text).strip()
    return text


def doc_id_from_url(pdf_path: str) -> str:
    fname = unquote(pdf_path.rsplit("/", 1)[-1])
    fname = fname.replace(".pdf", "").replace(".PDF", "")
    return fname


def extract_cap_number(title: str) -> str:
    m = CAP_RE.search(title)
    return m.group(1) if m else ""


def extract_year(title: str) -> str:
    m = YEAR_RE.search(title)
    return m.group(1) if m else ""


class BBSubsidiaryLegislationScraper(BaseScraper):
    """Scraper for BB/SubsidiaryLegislation."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        import requests
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
        })

    def _get(self, url: str, **kwargs) -> "requests.Response":
        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=120, **kwargs)
        resp.raise_for_status()
        return resp

    def _list_instruments(self) -> List[Tuple[str, str]]:
        """Scrape all (title, pdf_path) pairs from the listing page."""
        logger.info(f"Fetching subsidiary legislation listing from {LISTING_URL}")
        resp = self._get(LISTING_URL)
        results = []
        seen = set()

        for match in LINK_RE.finditer(resp.text):
            pdf_path = html_mod.unescape(match.group(1))
            title = strip_html(match.group(2))
            if pdf_path not in seen and title:
                seen.add(pdf_path)
                results.append((title, pdf_path))

        logger.info(f"Found {len(results)} subsidiary legislation instruments")
        return results

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        doc_id = raw.get("doc_id", "")
        cap = extract_cap_number(raw.get("title", ""))
        year = extract_year(raw.get("title", ""))
        return {
            "_id": f"BB/SubsidiaryLegislation/{doc_id}",
            "_source": "BB/SubsidiaryLegislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": f"{year}-01-01" if year else None,
            "url": raw.get("pdf_url", ""),
            "doc_id": doc_id,
            "cap_number": cap,
            "year": year,
            "pdf_url": raw.get("pdf_url", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        limit = 15 if sample else None
        count = 0

        instruments = self._list_instruments()
        if sample:
            instruments = instruments[:25]  # extra buffer in case some PDFs fail

        for title, pdf_path in instruments:
            if limit and count >= limit:
                break

            doc_id = doc_id_from_url(pdf_path)
            pdf_url = BASE_URL + quote(pdf_path, safe="/'")
            logger.info(f"  [{count+1}] {title} -> {doc_id}")

            try:
                text = extract_pdf_markdown(
                    source="BB/SubsidiaryLegislation",
                    source_id=doc_id,
                    pdf_url=pdf_url,
                    table="legislation",
                )
            except Exception as e:
                logger.warning(f"    PDF extraction failed for {doc_id}: {e}")
                text = None

            if not text or len(text.strip()) < 50:
                logger.warning(f"    Skipping {doc_id} - no/short text")
                continue

            record = self.normalize({
                "title": title,
                "text": text,
                "pdf_url": pdf_url,
                "doc_id": doc_id,
            })
            yield record
            count += 1
            logger.info(f"    OK ({len(text)} chars)")

        logger.info(f"Total records yielded: {count}")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """No update mechanism — static PDF collection."""
        logger.info("No incremental update support; use full refresh.")
        return
        yield


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = BBSubsidiaryLegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
