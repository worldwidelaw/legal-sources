#!/usr/bin/env python3
"""
DO/SIB-Regulations -- Superintendencia de Bancos de la República Dominicana

Fetches the full text of the regulatory normative output of the Dominican
banking supervisor (Superintendencia de Bancos, SB): Circulares, Cartas
Circulares, Circulares e Instructivos, and Resoluciones SB. These are the
prudential and supervisory provisions the Superintendent of Banks issues to
the regulated financial system under the authority of Article 21 of the
Monetary and Financial Law (Ley núm. 183-02 Monetaria y Financiera). Classified
as doctrine (official regulatory/supervisory guidance issued by a public
administrative authority).

Strategy:
  sb.gob.do is a custom CMS. The "Normativas SB" archive
    /regulacion/normativas-sb/?page=1&size=<N>
  renders every normative document as a card carrying its category, publication
  date, title, descriptive subject, and a link to the document detail page.
  A single request with a large page size returns the entire corpus (~229
  documents). Each detail page links to one /media/<id>/<file>.pdf containing
  the full text, which is downloaded and extracted with pdfplumber.

  A handful of the oldest documents (e.g. Resoluciones from the 1980s–90s) are
  scanned images with no extractable text; these are skipped (no full text).

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Incremental update (by date)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import re
import io
import time
import html as ht
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Dict

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.DO.SIB-Regulations")

BASE_URL = "https://sb.gob.do"
# A single large page returns the whole archive (~229 docs as of 2026).
LISTING_URL = BASE_URL + "/regulacion/normativas-sb/?page=1&size=1000"
SOURCE_ID = "DO/SIB-Regulations"
MIN_TEXT_CHARS = 200
ISSUER = "Superintendencia de Bancos de la República Dominicana"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Listing card fields.
CARD_SPLIT_RE = re.compile(r'<div class="document_card')
CARD_LABEL_RE = re.compile(r'<div class="label">([^<]*)</div>')
CARD_DATE_RE = re.compile(r'<div class="date">([^<]*)</div>')
CARD_LINK_RE = re.compile(
    r'<a href="(/regulacion/normativas-sb/[^"]+/)">(.*?)</a>', re.S
)
CARD_DESC_RE = re.compile(
    r'<div class="description_container">(.*?)</div>', re.S
)

# Detail page: the document PDF lives under /media/.
PDF_RE = re.compile(r'href="(/media/[^"]+?\.pdf)"', re.I)

TAG_RE = re.compile(r"<[^>]+>")
DATE_RE = re.compile(r"(\d{1,2})\s*/\s*(\d{1,2})\s*/\s*(\d{4})")
# Reference codes, e.g. "CSB-REG-2026000009", "CCI-REG-2026000004", "No. 13-1994".
REF_CODE_RE = re.compile(
    r"((?:CSB|CCI|SB)[-\s]?(?:REG)?[-\s]?\d{3,}(?:-\d{2,4})?|No\.?\s*\d+[-/]\d{2,4})",
    re.I,
)


def strip_tags(s: str) -> str:
    return ht.unescape(TAG_RE.sub("", s)).strip()


def parse_card_date(raw: Optional[str]) -> Optional[str]:
    """'08 / 05 / 2026' -> '2026-05-08' (ISO 8601)."""
    if not raw:
        return None
    m = DATE_RE.search(raw)
    if not m:
        return None
    d, mth, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return datetime(y, mth, d).strftime("%Y-%m-%d")
    except ValueError:
        return None


def extract_pdf_text(content: bytes) -> str:
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            pages = []
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    pages.append(t)
                # Release per-page cache to avoid pdfplumber OOM (exit 137, #987)
                try:
                    page.flush_cache()
                    page.get_textmap.cache_clear()
                except Exception:
                    pass
        return "\n\n".join(pages)
    except Exception as e:
        logger.warning(f"PDF extraction failed: {e}")
        return ""


def clean_text(text: str) -> str:
    text = text.replace("\x00", " ")
    # Collapse letter-spaced ALL-CAPS headings (e.g. "C I R C U L A R").
    text = re.sub(
        r"(?:\b[A-ZÁÉÍÓÚÑ]\s){3,}[A-ZÁÉÍÓÚÑ]\b",
        lambda m: m.group(0).replace(" ", ""),
        text,
    )
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def doc_number(title: str) -> Optional[str]:
    if not title:
        return None
    m = REF_CODE_RE.search(title)
    if m:
        return re.sub(r"\s+", " ", m.group(1)).strip().upper()
    return None


class SIBRegulationsScraper(BaseScraper):
    """
    Scraper for DO/SIB-Regulations -- Superintendencia de Bancos de la
    República Dominicana. Banking supervisor circulars, instructivos, cartas
    circulares, and resoluciones (prudential/supervisory regulation).
    Country: DO
    URL: https://sb.gob.do/

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
        })
        import urllib3
        urllib3.disable_warnings()

    def _discover(self) -> List[Dict]:
        """Parse the Normativas SB archive into per-document card dicts."""
        try:
            r = self.session.get(LISTING_URL, timeout=120)
            if r.status_code != 200:
                logger.warning(f"Listing HTTP {r.status_code}")
                return []
        except Exception as e:
            logger.warning(f"Listing fetch failed: {e}")
            return []

        cards = []
        seen = set()
        for chunk in CARD_SPLIT_RE.split(r.text)[1:]:
            a = CARD_LINK_RE.search(chunk)
            if not a:
                continue
            url = a.group(1)
            if url in seen:
                continue
            seen.add(url)
            lab = CARD_LABEL_RE.search(chunk)
            dat = CARD_DATE_RE.search(chunk)
            desc = CARD_DESC_RE.search(chunk)
            cards.append({
                "category": (ht.unescape(lab.group(1)).strip() if lab else None),
                "pub_date": parse_card_date(dat.group(1) if dat else None),
                "url": BASE_URL + url,
                "title": strip_tags(a.group(2)),
                "summary": (strip_tags(desc.group(1)) if desc else None),
            })
        logger.info(f"Discovered {len(cards)} normative documents")
        return cards

    def _find_pdf(self, detail_url: str) -> Optional[str]:
        try:
            r = self.session.get(detail_url, timeout=60)
            if r.status_code != 200:
                logger.debug(f"Detail HTTP {r.status_code}: {detail_url}")
                return None
        except Exception as e:
            logger.warning(f"Detail fetch failed for {detail_url}: {e}")
            return None
        m = PDF_RE.search(r.text)
        if not m:
            logger.debug(f"No PDF on {detail_url}")
            return None
        # The href carries HTML-escaped non-ASCII chars (e.g. &#xF3;); requests
        # percent-encodes the resulting unicode path automatically.
        return BASE_URL + ht.unescape(m.group(1))

    def _download_and_extract(self, item: Dict) -> Optional[dict]:
        pdf_url = self._find_pdf(item["url"])
        if not pdf_url:
            return None
        try:
            r = self.session.get(pdf_url, timeout=120)
            if r.status_code != 200 or r.content[:4] != b"%PDF":
                logger.debug(f"Not a live PDF ({r.status_code}): {pdf_url}")
                return None
        except Exception as e:
            logger.warning(f"Download failed for {pdf_url}: {e}")
            return None

        text = clean_text(extract_pdf_text(r.content))
        if len(text) < MIN_TEXT_CHARS:
            # Oldest documents are scanned images with no extractable text.
            logger.debug(f"Insufficient text ({len(text)} chars): {pdf_url}")
            return None

        return {
            **item,
            "pdf_url": pdf_url,
            "text": text,
            "doc_number": doc_number(item.get("title", "")),
            "pdf_size": len(r.content),
        }

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": raw["url"],
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("pub_date"),
            "url": raw["url"],
            "pdf_url": raw["pdf_url"],
            "summary": raw.get("summary"),
            "doc_number": raw.get("doc_number"),
            "category": raw.get("category"),
            "issuer": ISSUER,
            "jurisdiction": "DO",
            "language": "es",
            "pdf_size": raw.get("pdf_size", 0),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        cards = self._discover()
        yielded = 0
        for card in cards:
            result = self._download_and_extract(card)
            time.sleep(1.0)
            if result:
                yield result
                yielded += 1
                if yielded % 25 == 0:
                    logger.info(f"Extracted {yielded} documents...")
        logger.info(f"fetch_all complete: {yielded} documents with full text")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        # `update()` passes a datetime, but the comparison below is against a
        # record's ISO date string, which raises TypeError (#1512).
        since = as_date_str(since)
        cards = self._discover()
        yielded = 0
        for card in cards:
            if card.get("pub_date") and card["pub_date"] < since:
                continue
            result = self._download_and_extract(card)
            time.sleep(1.0)
            if result:
                yield result
                yielded += 1
        logger.info(f"fetch_updates complete: {yielded} documents")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="DO/SIB-Regulations — Superintendencia de Bancos (RD) regulations"
    )
    subparsers = parser.add_subparsers(dest="command")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    bp.add_argument("--sample-size", type=int, default=15, help="Sample size")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = SIBRegulationsScraper()

    if args.command == "test":
        logger.info("Testing Superintendencia de Bancos connectivity...")
        cards = scraper._discover()
        if not cards:
            logger.error("No documents discovered")
            sys.exit(1)
        logger.info(f"First doc: {cards[0]['title']} ({cards[0]['category']})")
        result = scraper._download_and_extract(cards[0])
        if result:
            logger.info(f"Title: {result['title'][:120]}")
            logger.info(f"Doc number: {result['doc_number']} | Date: {result['date']}")
            logger.info(f"Text: {len(result['text'])} chars")
            logger.info(f"Preview: {result['text'][:200]}")
            logger.info("Connectivity test passed!")
        else:
            logger.error("Failed to extract text from first document")
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
