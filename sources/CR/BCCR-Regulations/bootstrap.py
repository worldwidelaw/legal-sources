#!/usr/bin/env python3
"""
CR/BCCR-Regulations -- Banco Central de Costa Rica, Marco Legal

Fetches the full text of the Central Bank of Costa Rica's regulatory
framework ("Marco Legal"): reglamentos, normativa, acuerdos de la Junta
Directiva (board agreements/resolutions), and control gacetario.

Strategy:
  The BCCR site (bccr.fi.cr) is a SharePoint site. Each "Marco Legal"
  section page embeds its document catalogue as a JSON blob in the HTML,
  with the PDF paths under /marco-legal/DocReglamento/, DocNormativa/,
  DocAcuerdosJuntaDirectiva/, DocControlGacetario/ (slashes escaped as
  \\u002f). We:
    1. Fetch each section page and extract every /marco-legal/Doc*/*.pdf
       path from the embedded JSON.
    2. Download each PDF and extract full text with pdfplumber.
    3. Best-effort parse an issue date from the document text.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import re
import io
import time
import logging
import urllib.parse
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
logger = logging.getLogger("legal-data-hunter.CR.BCCR-Regulations")

BASE_URL = "https://www.bccr.fi.cr"
MIN_TEXT_CHARS = 200

SECTION_PATHS = [
    "/marco-legal/reglamentos",
    "/marco-legal/normativa",
    "/marco-legal/acuerdos-de-junta-directiva",
    "/marco-legal/acuerdos-de-junta-directiva/histórico-2010-2019",
    "/marco-legal/acuerdos-de-junta-directiva/histórico-anterior-a-2010",
    "/marco-legal/control-gacetario",
]

PDF_RE = re.compile(r"/marco-legal/Doc[A-Za-z]+/[A-Za-z0-9_.%()-]+?\.pdf", re.I)

MONTHS_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}
DATE_TEXT_RE = re.compile(
    r"(\d{1,2})\s+de\s+(" + "|".join(MONTHS_ES) + r")\s+de\s+(\d{4})", re.I
)


def extract_pdf_text(content: bytes) -> str:
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            pages = []
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    pages.append(t)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
        return "\n\n".join(pages)
    except Exception as e:
        logger.warning(f"PDF extraction failed: {e}")
        return ""


def clean_text(text: str) -> str:
    text = text.replace("\x00", " ")
    # Fix letter-spaced headings ("R E G L A M E N T O" -> "REGLAMENTO")
    text = re.sub(
        r"(?:\b[A-ZÁÉÍÓÚÑ]\s){3,}[A-ZÁÉÍÓÚÑ]\b",
        lambda m: m.group(0).replace(" ", ""),
        text,
    )
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_date_from_text(text: str) -> Optional[str]:
    """Best-effort: first long-form Spanish date in the document body."""
    m = DATE_TEXT_RE.search(text)
    if not m:
        return None
    day = int(m.group(1))
    month = MONTHS_ES[m.group(2).lower()]
    year = int(m.group(3))
    try:
        return datetime(year, month, day).strftime("%Y-%m-%d")
    except ValueError:
        return None


def make_title(rel_path: str) -> str:
    base = rel_path.rsplit("/", 1)[-1]
    base = re.sub(r"\.pdf$", "", base, flags=re.I)
    base = base.replace("_", " ").replace("-", " ")
    base = re.sub(r"\s+", " ", base).strip()
    return base


def doc_category(rel_path: str) -> str:
    seg = rel_path.split("/")
    folder = seg[2] if len(seg) > 2 else ""
    return {
        "DocReglamento": "reglamento",
        "DocNormativa": "normativa",
        "DocAcuerdosJuntaDirectiva": "acuerdo_junta_directiva",
        "DocControlGacetario": "control_gacetario",
    }.get(folder, "otro")


class BCCRRegulationsScraper(BaseScraper):
    """
    Scraper for CR/BCCR-Regulations — Banco Central de Costa Rica.
    Country: CR
    URL: https://www.bccr.fi.cr/

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) LegalDataHunter/1.0 "
                          "(research; https://github.com/ZachLaik/LegalDataHunter)",
        })
        import urllib3
        urllib3.disable_warnings()

    def _discover(self) -> List[Dict]:
        """Enumerate all Marco Legal PDF paths across section pages."""
        seen = {}
        for sec in SECTION_PATHS:
            url = BASE_URL + urllib.parse.quote(sec)
            try:
                r = self.session.get(url, timeout=60)
                if r.status_code != 200:
                    logger.warning(f"Section {sec}: HTTP {r.status_code}")
                    continue
                html = r.text.replace("\\u002f", "/").replace("\\u002F", "/")
                found = PDF_RE.findall(html)
                new = 0
                for p in found:
                    key = p.lower()
                    if key not in seen:
                        seen[key] = {
                            "rel_path": p,
                            "url": BASE_URL + urllib.parse.quote(p),
                            "category": doc_category(p),
                        }
                        new += 1
                logger.info(f"Section {sec}: {len(found)} refs, {new} new")
            except Exception as e:
                logger.warning(f"Section {sec} failed: {e}")
            time.sleep(1.0)
        items = list(seen.values())
        logger.info(f"Discovered {len(items)} unique Marco Legal PDFs")
        return items

    def _download_and_extract(self, item: Dict) -> Optional[dict]:
        try:
            r = self.session.get(item["url"], timeout=90)
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
            "rel_path": item["rel_path"],
            "url": item["url"],
            "title": make_title(item["rel_path"]),
            "text": text,
            "date": parse_date_from_text(text),
            "category": item["category"],
            "pdf_size": len(r.content),
        }

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": raw["url"],
            "_source": "CR/BCCR-Regulations",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "category": raw.get("category"),
            "issuer": "Banco Central de Costa Rica",
            "jurisdiction": "CR",
            "language": "es",
            "pdf_size": raw.get("pdf_size", 0),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        items = self._discover()
        yielded = 0
        for item in items:
            result = self._download_and_extract(item)
            if result:
                yield result
                yielded += 1
                if yielded % 25 == 0:
                    logger.info(f"Extracted {yielded} documents...")
            time.sleep(1.0)
        logger.info(f"fetch_all complete: {yielded} documents with full text")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        # No reliable modified-date index; re-discover and let upsert dedup.
        # `update()` passes a datetime, but the comparison below is against a
        # record's ISO date string, which raises TypeError (#1512).
        since = as_date_str(since)
        items = self._discover()
        yielded = 0
        for item in items:
            result = self._download_and_extract(item)
            if result:
                if result.get("date") and result["date"] < since:
                    continue
                yield result
                yielded += 1
            time.sleep(1.0)
        logger.info(f"fetch_updates complete: {yielded} documents")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="CR/BCCR-Regulations — Banco Central de Costa Rica"
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

    scraper = BCCRRegulationsScraper()

    if args.command == "test":
        logger.info("Testing BCCR connectivity...")
        items = scraper._discover()
        if not items:
            logger.error("No PDFs discovered")
            sys.exit(1)
        logger.info(f"First candidate: {items[0]['rel_path']}")
        result = scraper._download_and_extract(items[0])
        if result:
            logger.info(f"Title: {result['title']}")
            logger.info(f"Category: {result['category']} | Date: {result['date']}")
            logger.info(f"Text: {len(result['text'])} chars")
            logger.info(f"Preview: {result['text'][:200]}")
            logger.info("Connectivity test passed!")
        else:
            logger.error("Failed to extract text from first candidate")
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
