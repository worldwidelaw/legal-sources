#!/usr/bin/env python3
"""
HN/CNBS-Regulations -- Comisión Nacional de Bancos y Seguros, circulares y normas.

Fetches the full text of the regulatory framework published by the Comisión
Nacional de Bancos y Seguros (CNBS), the Honduran financial-sector supervisor,
via its public "Resoluciones y circulares" portal (circulares.cnbs.gob.hn).

CNBS circulars carry the binding prudential regulation of Honduras: the
*normas* and *reglamentos* governing banks, insurers, pension fund managers
(AFP), credit-card issuers and the wider financial system (loan-portfolio
classification, reserves, investments, AML/CFT, market conduct, etc.), as well
as the resoluciones that enact them. Classified as legislation (regulations).

Strategy:
  The portal is a DevExpress ASP.NET MVC app whose listing is served as an
  HTML partial by:
    /Home/Circulares?filterType=CircularesYear&valueFilter=<year>&page=<n>&lastPageShow=0
  Paging is 0-indexed, 20 cards per page. Each card links to the circular PDF
    /Archivo/Viewer/<id>/<filename>.pdf
  We enumerate every year (1996..current), parse the card metadata (title,
  resolution reference, date, summary), download each PDF and extract its full
  text with pdfplumber.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Sample mode (default 15)
  python bootstrap.py update             # Incremental (current + previous year)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import re
import json
import time
import html
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Dict

import requests
import pdfplumber
import urllib3

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.HN.CNBS-Regulations")

BASE_URL = "https://circulares.cnbs.gob.hn"
LIST_URL = BASE_URL + "/Home/Circulares"
SOURCE_ID = "HN/CNBS-Regulations"
ISSUER = "Comisión Nacional de Bancos y Seguros"
MIN_TEXT_CHARS = 180          # short administrative circulars are still complete
FIRST_YEAR = 1996
PAGE_SIZE = 20

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# One card: the Viewer anchor wraps an <h4> title and (optionally) an <h5>
# resolution reference; a calendar <span> with the dd/mm/yyyy date follows.
CARD_RE = re.compile(
    r'<a\s+href="(?P<href>/Archivo/Viewer/[^"]+)"[^>]*>\s*'
    r'<h4[^>]*>(?P<title>.*?)</h4>\s*'
    r'(?:<h5[^>]*>(?P<sub>.*?)</h5>\s*)?'
    r'</a>(?P<after>.*?)(?=<a\s+href="/Archivo/Viewer/|$)',
    re.S | re.I,
)
DATE_SPAN_RE = re.compile(r"<span[^>]*>\s*(\d{1,2}/\d{1,2}/\d{4})\s*</span>")
DESC_RE = re.compile(r'card-body.*?<p[^>]*>(.*?)</p>', re.S | re.I)
TAG_RE = re.compile(r"<[^>]+>")
NUM_RE = re.compile(r"No\.?\s*([\dA-Z./\-]+/\d{4}|[\dA-Z./\-]+-\d{4}|[\d.]+/\d{4})", re.I)


def strip_tags(s: str) -> str:
    return html.unescape(re.sub(r"\s+", " ", TAG_RE.sub(" ", s))).strip()


def extract_pdf_text(content: bytes) -> str:
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            pages = [p.extract_text() or "" for p in pdf.pages]
        return "\n\n".join(t for t in pages if t)
    except Exception as e:
        logger.warning(f"PDF extraction failed: {e}")
        return ""


def clean_text(text: str) -> str:
    text = text.replace("\x00", " ")
    text = re.sub(
        r"(?:\b[A-ZÁÉÍÓÚÑ]\s){3,}[A-ZÁÉÍÓÚÑ]\b",
        lambda m: m.group(0).replace(" ", ""),
        text,
    )
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def iso_date(ddmmyyyy: Optional[str]) -> Optional[str]:
    if not ddmmyyyy:
        return None
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", ddmmyyyy)
    if not m:
        return None
    d, mo, y = (int(x) for x in m.groups())
    try:
        return f"{y:04d}-{mo:02d}-{d:02d}"
    except ValueError:
        return None


def doc_number(title: str, sub: str) -> Optional[str]:
    for src in (title, sub):
        m = NUM_RE.search(src or "")
        if m:
            return m.group(1).upper().rstrip(".")
    return None


def doc_kind(title: str) -> str:
    t = (title or "").upper()
    if "CIRCULAR" in t:
        return "circular"
    if "RESOLUCI" in t:
        return "resolucion"
    if "NORMA" in t or "REGLAMENTO" in t:
        return "norma"
    return "circular"


class CNBSRegulationsScraper(BaseScraper):
    """
    Scraper for HN/CNBS-Regulations -- Comisión Nacional de Bancos y Seguros
    circulars, resolutions and prudential norms. Full text via pdfplumber over
    the public "Resoluciones y circulares" portal.
    Country: HN
    URL: https://circulares.cnbs.gob.hn/
    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,*/*",
            "X-Requested-With": "XMLHttpRequest",
        })
        urllib3.disable_warnings()

    def _list_year_page(self, year: int, page: int) -> List[Dict]:
        params = {
            "filterType": "CircularesYear",
            "valueFilter": str(year),
            "page": str(page),
            "lastPageShow": "0",
        }
        try:
            r = self.session.get(LIST_URL, params=params, timeout=60)
            if r.status_code != 200:
                logger.warning(f"List {year} p{page} HTTP {r.status_code}")
                return []
            text = r.text
        except Exception as e:
            logger.warning(f"List {year} p{page} failed: {e}")
            return []

        items = []
        for m in CARD_RE.finditer(text):
            href = m.group("href")
            title = strip_tags(m.group("title") or "")
            sub = strip_tags(m.group("sub") or "")
            after = m.group("after") or ""
            dm = DATE_SPAN_RE.search(after)
            descm = DESC_RE.search(after)
            items.append({
                "url": BASE_URL + html.unescape(href),
                "title": title,
                "sub": sub,
                "date": iso_date(dm.group(1) if dm else None),
                "summary": strip_tags(descm.group(1)) if descm else "",
                "year": year,
            })
        return items

    def _discover(self) -> List[Dict]:
        """Enumerate every circular across all years via paginated listing."""
        current_year = datetime.now(timezone.utc).year
        files, seen = [], set()
        for year in range(current_year, FIRST_YEAR - 1, -1):
            page = 0
            while True:
                batch = self._list_year_page(year, page)
                if not batch:
                    break
                new = 0
                for it in batch:
                    if it["url"] in seen:
                        continue
                    seen.add(it["url"])
                    files.append(it)
                    new += 1
                time.sleep(0.5)
                if len(batch) < PAGE_SIZE:
                    break
                page += 1
                if page > 50:  # safety guard
                    break
            logger.info(f"Year {year}: cumulative {len(files)} circulars")
        logger.info(f"Discovered {len(files)} circulars total")
        return files

    def _download_and_extract(self, item: Dict) -> Optional[dict]:
        try:
            r = self.session.get(
                item["url"], timeout=120,
                headers={"Accept": "application/pdf,*/*"},
            )
            if r.status_code != 200 or r.content[:4] != b"%PDF":
                logger.debug(f"Not a live PDF ({r.status_code}): {item['url']}")
                return None
        except Exception as e:
            logger.warning(f"Download failed for {item['url']}: {e}")
            return None

        text = clean_text(extract_pdf_text(r.content))
        if len(text) < MIN_TEXT_CHARS:
            logger.debug(f"Insufficient text ({len(text)} chars): {item['title']}")
            return None

        title = item["title"] or "Circular CNBS"
        return {
            **item,
            "text": text,
            "title": title,
            "doc_number": doc_number(title, item.get("sub", "")),
            "doc_kind": doc_kind(title),
            "pdf_size": len(r.content),
        }

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": raw["url"],
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "doc_number": raw.get("doc_number"),
            "doc_kind": raw.get("doc_kind"),
            "resolution_ref": raw.get("sub") or None,
            "summary": raw.get("summary") or None,
            "issuer": ISSUER,
            "jurisdiction": "HN",
            "language": "es",
            "year": raw.get("year"),
            "pdf_size": raw.get("pdf_size", 0),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        files = self._discover()
        yielded = 0
        for item in files:
            result = self._download_and_extract(item)
            time.sleep(1.0)
            if result:
                yield result
                yielded += 1
        logger.info(f"fetch_all complete: {yielded} documents with full text")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Re-fetch the current and previous year; upsert dedups by URL."""
        current_year = datetime.now(timezone.utc).year
        for year in (current_year, current_year - 1):
            page = 0
            while True:
                batch = self._list_year_page(year, page)
                if not batch:
                    break
                for item in batch:
                    result = self._download_and_extract(item)
                    time.sleep(1.0)
                    if result:
                        yield result
                if len(batch) < PAGE_SIZE:
                    break
                page += 1


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="HN/CNBS-Regulations — CNBS circulars, resolutions and norms"
    )
    subparsers = parser.add_subparsers(dest="command")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Sample mode")
    bp.add_argument("--sample-size", type=int, default=15, help="Sample size")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = CNBSRegulationsScraper()

    if args.command == "test":
        logger.info("Testing CNBS circulars portal connectivity...")
        items = scraper._list_year_page(datetime.now(timezone.utc).year, 0) \
            or scraper._list_year_page(datetime.now(timezone.utc).year - 1, 0)
        if not items:
            logger.error("No circulars discovered")
            sys.exit(1)
        logger.info(f"Listed {len(items)} circulars on first page")
        result = None
        for it in items:
            result = scraper._download_and_extract(it)
            if result:
                break
        if result:
            logger.info(f"Title: {result['title'][:120]}")
            logger.info(f"Kind: {result['doc_kind']} | Ref: {result['doc_number']} "
                        f"| Date: {result['date']} | Text: {len(result['text'])} chars")
            logger.info(f"Preview: {result['text'][:200]}")
            logger.info("Connectivity test passed!")
        else:
            logger.error("Failed to extract text from any listed document")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
