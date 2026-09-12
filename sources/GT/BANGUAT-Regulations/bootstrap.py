#!/usr/bin/env python3
"""
GT/BANGUAT-Regulations -- Banco de Guatemala, marco legal del sistema financiero.

Fetches the full text of the banking and financial legal framework published
by the Banco de Guatemala (BANGUAT), the central bank of Guatemala. This is the
body of monetary, banking, and financial-system law that BANGUAT publishes as
the legal basis for its operation and for the supervision of the Guatemalan
financial system: the Ley Orgánica del Banco de Guatemala, Ley Monetaria, Ley
de Bancos y Grupos Financieros, Ley de Supervisión Financiera, AML/CFT laws,
the Ley de la Actividad Aseguradora, and related decrees and acuerdos.

Classified as legislation (statutes/decrees that constitute the financial-sector
legal framework).

Strategy:
  BANGUAT's "Leyes Bancarias y Financieras" page
    /page/leyes-bancarias-y-financieras
  lists each law as an <a> link to a text PDF under /sites/.../banguat/leyes/.
  Each PDF is downloaded and its full text extracted with pdfplumber.

  NOTE on the Junta Monetaria / Gerencia General "resoluciones": those archives
  (/page/ano-YYYY, /page/resoluciones-de-gerencia-general-*) publish their
  documents only as scanned-image PDFs with no embedded text layer; pdfplumber
  extracts 0–6 characters from them. They are therefore NOT full-text accessible
  and are excluded. This source captures the full-text legal framework only.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Sample mode (default 15)
  python bootstrap.py update             # Incremental (re-fetch; laws change rarely)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import re
import json
import time
import html as ht
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
logger = logging.getLogger("legal-data-hunter.GT.BANGUAT-Regulations")

BASE_URL = "https://banguat.gob.gt"
LISTING_URL = BASE_URL + "/page/leyes-bancarias-y-financieras"
SOURCE_ID = "GT/BANGUAT-Regulations"
ISSUER = "Banco de Guatemala"
MIN_TEXT_CHARS = 500

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Law PDFs live under /sites/default/files/banguat/leyes/<year>/<name>.pdf.
LAW_LINK_RE = re.compile(
    r'<a[^>]+href="(/sites/default/files/banguat/leyes/[^"]+?\.pdf)"[^>]*>(.*?)</a>',
    re.I | re.S,
)
TAG_RE = re.compile(r"<[^>]+>")
# Decree/acuerdo reference, e.g. "DECRETO NUMERO 16-2002", "Acuerdo Número 32-2008".
REF_RE = re.compile(
    r"(?:DECRETO|ACUERDO)\s+N[ÚU]MERO\s+([\dA-Z]+-\d{2,4})", re.I
)
YEAR_RE = re.compile(r"/leyes/(\d{4})/")


def strip_tags(s: str) -> str:
    s = ht.unescape(TAG_RE.sub(" ", s))
    # Strip the zero-width non-joiner BANGUAT prefixes its anchor text with.
    s = s.replace("‌", " ")
    return re.sub(r"\s+", " ", s).strip()


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
    # Collapse letter-spaced ALL-CAPS headings (e.g. "L E Y  O R G A N I C A").
    text = re.sub(
        r"(?:\b[A-ZÁÉÍÓÚÑ]\s){3,}[A-ZÁÉÍÓÚÑ]\b",
        lambda m: m.group(0).replace(" ", ""),
        text,
    )
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def doc_number(text: str, title: str) -> Optional[str]:
    for src in (title, text[:2000]):
        m = REF_RE.search(src or "")
        if m:
            return m.group(1).upper()
    return None


class BanguatRegulationsScraper(BaseScraper):
    """
    Scraper for GT/BANGUAT-Regulations -- Banco de Guatemala financial/banking
    legal framework (Leyes Bancarias y Financieras). Full text via pdfplumber.
    Country: GT
    URL: https://banguat.gob.gt/
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
            "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
        })
        urllib3.disable_warnings()

    def _discover(self) -> List[Dict]:
        """Parse the Leyes Bancarias y Financieras page into law dicts."""
        try:
            r = self.session.get(LISTING_URL, timeout=120)
            if r.status_code != 200:
                logger.warning(f"Listing HTTP {r.status_code}")
                return []
        except Exception as e:
            logger.warning(f"Listing fetch failed: {e}")
            return []

        laws, seen = [], set()
        for href, anchor in LAW_LINK_RE.findall(r.text):
            if href in seen:
                continue
            seen.add(href)
            ym = YEAR_RE.search(href)
            laws.append({
                "url": BASE_URL + href,
                "title": strip_tags(anchor),
                "edition_year": ym.group(1) if ym else None,
            })
        logger.info(f"Discovered {len(laws)} laws")
        return laws

    def _download_and_extract(self, item: Dict) -> Optional[dict]:
        try:
            r = self.session.get(item["url"], timeout=120)
            if r.status_code != 200 or r.content[:4] != b"%PDF":
                logger.debug(f"Not a live PDF ({r.status_code}): {item['url']}")
                return None
        except Exception as e:
            logger.warning(f"Download failed for {item['url']}: {e}")
            return None

        text = clean_text(extract_pdf_text(r.content))
        if len(text) < MIN_TEXT_CHARS:
            logger.debug(f"Insufficient text ({len(text)} chars): {item['url']}")
            return None

        return {
            **item,
            "text": text,
            "doc_number": doc_number(text, item.get("title", "")),
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
            # Laws are undated on the listing; edition_year is the publication
            # year of the BANGUAT edition (folder), used as a coarse date.
            "date": (f"{raw['edition_year']}-01-01" if raw.get("edition_year") else None),
            "url": raw["url"],
            "doc_number": raw.get("doc_number"),
            "issuer": ISSUER,
            "jurisdiction": "GT",
            "language": "es",
            "edition_year": raw.get("edition_year"),
            "pdf_size": raw.get("pdf_size", 0),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        laws = self._discover()
        yielded = 0
        for law in laws:
            result = self._download_and_extract(law)
            time.sleep(1.0)
            if result:
                yield result
                yielded += 1
        logger.info(f"fetch_all complete: {yielded} laws with full text")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        # The framework changes rarely and laws carry no per-document timestamp,
        # so an update re-fetches the full set (upsert by URL downstream).
        yield from self.fetch_all()


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="GT/BANGUAT-Regulations — Banco de Guatemala financial legal framework"
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

    scraper = BanguatRegulationsScraper()

    if args.command == "test":
        logger.info("Testing Banco de Guatemala connectivity...")
        laws = scraper._discover()
        if not laws:
            logger.error("No laws discovered")
            sys.exit(1)
        result = scraper._download_and_extract(laws[0])
        if result:
            logger.info(f"Title: {result['title'][:120]}")
            logger.info(f"Doc number: {result['doc_number']} | Text: {len(result['text'])} chars")
            logger.info(f"Preview: {result['text'][:200]}")
            logger.info("Connectivity test passed!")
        else:
            logger.error("Failed to extract text from first law")
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
