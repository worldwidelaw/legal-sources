#!/usr/bin/env python3
"""
CR/SUGEF-Regulations -- Superintendencia General de Entidades Financieras

Fetches the full text of Costa Rica's financial-sector prudential
regulations: the "Acuerdos SUGEF" (e.g. SUGEF 3-06 suficiencia
patrimonial, SUGEF 24-00 grupos financieros) plus the cross-cutting
"Normativa Transversal" issued by CONASSIF (Consejo Nacional de
Supervisión del Sistema Financiero).

Strategy:
  Two listing pages on sugef.fi.cr embed direct links to the regulation
  PDFs:
    /normativa/normativa_vigente.aspx       -> /normativa/normativa_vigente/*.pdf
    /normativa/NormativaTransversal.aspx    -> /normativa/normativa_transversal/documentos/*.pdf
  We scrape both pages for .pdf hrefs, download each PDF, and extract the
  full text with pdfplumber. The site uses a self-signed/incomplete cert
  chain, so TLS verification is disabled (public, read-only data).

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
logger = logging.getLogger("legal-data-hunter.CR.SUGEF-Regulations")

BASE_URL = "https://www.sugef.fi.cr"
MIN_TEXT_CHARS = 200

# Listing pages -> the URL prefix where their PDFs live (for categorisation).
SECTION_PAGES = [
    "/normativa/normativa_vigente.aspx",
    "/normativa/NormativaTransversal.aspx",
]

PDF_RE = re.compile(r'href=["\']([^"\']+?\.pdf)["\']', re.I)

MONTHS_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}
DATE_TEXT_RE = re.compile(
    r"(\d{1,2})\s+de\s+(" + "|".join(MONTHS_ES) + r")\s+de\s+(\d{4})", re.I
)

# "SUGEF 3-06", "CONASSIF 1-10" style acuerdo identifiers in the filename.
DOC_NUM_RE = re.compile(r"\b(SUGEF|CONASSIF)\s+(\d{1,3}-\d{2})\b", re.I)


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


def filename_of(rel_path: str) -> str:
    name = urllib.parse.unquote(rel_path.rsplit("/", 1)[-1])
    return re.sub(r"\.pdf$", "", name, flags=re.I)


def doc_number(rel_path: str) -> Optional[str]:
    m = DOC_NUM_RE.search(filename_of(rel_path))
    if m:
        return f"{m.group(1).upper()} {m.group(2)}"
    return None


def make_title(rel_path: str) -> str:
    base = filename_of(rel_path)
    # Drop trailing version annotation like "(v18 1° de enero de 2024)".
    base = re.sub(r"\s*\([^)]*\)\s*$", "", base)
    base = base.replace("_", " ")
    base = re.sub(r"\s+", " ", base).strip()
    return base


TITLE_HEAD_RE = re.compile(
    r"\b(REGLAMENTO|C[ÓO]DIGO|LINEAMIENTOS?|MANUAL|"
    r"METODOLOG[ÍI]A|DIRECTR[IÍ]Z)\b[^\n]{0,150}",
    re.I,
)


def descriptive_title(text: str) -> Optional[str]:
    """Pull a human-readable heading (e.g. 'REGLAMENTO SOBRE ...') from the
    document body, used to enrich the bare acuerdo-number filenames."""
    m = TITLE_HEAD_RE.search(text)
    if not m:
        return None
    t = re.sub(r"\s+", " ", m.group(0)).strip(" .,:;-")
    # Need a real description beyond the keyword itself.
    if len(t.split()) < 3 or len(t) < 15:
        return None
    return t[:150]


def doc_category(rel_path: str) -> str:
    num = doc_number(rel_path)
    if num and num.startswith("CONASSIF"):
        return "acuerdo_conassif"
    if num and num.startswith("SUGEF"):
        return "acuerdo_sugef"
    return "reglamento"


def issuer_of(rel_path: str) -> str:
    if "transversal" in rel_path.lower() or (doc_number(rel_path) or "").startswith("CONASSIF"):
        return "Consejo Nacional de Supervisión del Sistema Financiero (CONASSIF)"
    return "Superintendencia General de Entidades Financieras (SUGEF)"


class SUGEFRegulationsScraper(BaseScraper):
    """
    Scraper for CR/SUGEF-Regulations — Superintendencia General de
    Entidades Financieras (Costa Rica).
    Country: CR
    URL: https://www.sugef.fi.cr/

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
        """Enumerate all regulation PDF paths across the listing pages."""
        seen: Dict[str, Dict] = {}
        for page in SECTION_PAGES:
            url = BASE_URL + urllib.parse.quote(page)
            try:
                r = self.session.get(url, timeout=60)
                if r.status_code != 200:
                    logger.warning(f"Page {page}: HTTP {r.status_code}")
                    continue
                found = PDF_RE.findall(r.text)
                new = 0
                for p in found:
                    # Normalise to an absolute path on this host.
                    if p.startswith("http"):
                        parsed = urllib.parse.urlparse(p)
                        if "sugef.fi.cr" not in parsed.netloc:
                            continue
                        p = parsed.path
                    if not p.startswith("/"):
                        p = "/" + p
                    # Hrefs are already percent-encoded; normalise by
                    # decoding then re-encoding cleanly so we never double-
                    # encode '%' -> '%25'.
                    p = urllib.parse.quote(urllib.parse.unquote(p), safe="/")
                    key = p.lower()
                    if key not in seen:
                        seen[key] = {
                            "rel_path": p,
                            "url": BASE_URL + p,
                            "category": doc_category(p),
                            "doc_number": doc_number(p),
                            "issuer": issuer_of(p),
                        }
                        new += 1
                logger.info(f"Page {page}: {len(found)} refs, {new} new")
            except Exception as e:
                logger.warning(f"Page {page} failed: {e}")
            time.sleep(1.0)
        items = list(seen.values())
        logger.info(f"Discovered {len(items)} unique regulation PDFs")
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

        # Enrich bare acuerdo-number filenames with a descriptive heading.
        num = item.get("doc_number")
        desc = descriptive_title(text)
        if num and desc:
            title = f"{num} — {desc}"
        elif desc and len(make_title(item["rel_path"])) < 12:
            title = desc
        else:
            title = make_title(item["rel_path"])

        return {
            "rel_path": item["rel_path"],
            "url": item["url"],
            "title": title,
            "text": text,
            "date": parse_date_from_text(text),
            "category": item["category"],
            "doc_number": item.get("doc_number"),
            "issuer": item.get("issuer"),
            "pdf_size": len(r.content),
        }

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": raw["url"],
            "_source": "CR/SUGEF-Regulations",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "category": raw.get("category"),
            "doc_number": raw.get("doc_number"),
            "issuer": raw.get("issuer", "Superintendencia General de Entidades Financieras (SUGEF)"),
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
        description="CR/SUGEF-Regulations — Superintendencia General de Entidades Financieras"
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

    scraper = SUGEFRegulationsScraper()

    if args.command == "test":
        logger.info("Testing SUGEF connectivity...")
        items = scraper._discover()
        if not items:
            logger.error("No PDFs discovered")
            sys.exit(1)
        logger.info(f"First candidate: {items[0]['rel_path']}")
        result = scraper._download_and_extract(items[0])
        if result:
            logger.info(f"Title: {result['title']}")
            logger.info(f"Doc number: {result['doc_number']} | Category: {result['category']}")
            logger.info(f"Date: {result['date']} | Text: {len(result['text'])} chars")
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
