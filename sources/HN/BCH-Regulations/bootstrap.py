#!/usr/bin/env python3
"""
HN/BCH-Regulations -- Banco Central de Honduras, Marco Legal (Leyes y Reglamentos).

Fetches the full text of the legal and regulatory framework published by the
Banco Central de Honduras (BCH), the central bank of Honduras, on its
"Marco Legal / Leyes y Reglamentos" page. This is the body of monetary,
banking and financial-sector law plus the BCH Directorio's own normative
instruments — the laws (Ley del BCH, Ley del Sistema Financiero, Ley de
Equidad Tributaria, the Constitution, etc.) and the central bank's binding
regulatory acts: circulares, resoluciones and acuerdos that set monetary
policy, exchange policy, payment-system and financial-market rules.

Classified as legislation (statutes/decrees + central-bank regulations that
constitute the financial-sector legal framework — "legislation includes
regulations").

Strategy:
  The page is backed by a public SharePoint document library
    /administrativas/JUR/Marco Legal OM 2/
  whose contents are exposed without authentication via the SharePoint REST
  API:
    /administrativas/JUR/_api/web/GetFolderByServerRelativeUrl('<folder>')/Files
  We list every PDF in that library, download each one, and extract its full
  text with pdfplumber. Titles and dates are parsed from the document header
  text where possible, with the file name as a fallback.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Sample mode (default 15)
  python bootstrap.py update             # Incremental (re-fetch; upsert by URL)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import re
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Dict
from urllib.parse import quote

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
logger = logging.getLogger("legal-data-hunter.HN.BCH-Regulations")

BASE_URL = "https://www.bch.hn"
SUBSITE = "/administrativas/JUR"
FOLDER = "/administrativas/JUR/Marco Legal OM 2"
LIST_API = (
    f"{BASE_URL}{SUBSITE}/_api/web/GetFolderByServerRelativeUrl('{quote(FOLDER)}')"
    f"/Files?$select=Name,ServerRelativeUrl,TimeLastModified&$top=2000"
)
SOURCE_ID = "HN/BCH-Regulations"
ISSUER = "Banco Central de Honduras"
MIN_TEXT_CHARS = 500

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Lines that open with one of these keywords are treated as the document title.
TITLE_KW_RE = re.compile(
    r"^(RESOLUCI[ÓO]N|CIRCULAR|ACUERDO|DECRETO|LEY|REGLAMENTO|NORMATIVA|"
    r"CONSTITUCI[ÓO]N|CONVENIO|INSTRUCTIVO|DISPOSICI[ÓO]N|REFORMA)\b",
    re.I,
)
# Document reference, e.g. "No.D-03/2022", "No.237-7/2020", "No. 456-10-2024".
REF_RE = re.compile(r"No\.?\s*([A-Z]{0,2}-?\d{1,4}[-/]\d{1,2}[-/]\d{2,4})", re.I)
# Decree number, e.g. "Decreto No. 129-2004", "Decreto No. 53".
DECRETO_RE = re.compile(r"DECRETO\s+No\.?\s*([\d-]+)", re.I)
YEAR_RE = re.compile(r"(19|20)\d{2}")

SP_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}
# Spanish date: "del 30 de julio de 2020", "3 de febrero de 1950".
DATE_RE = re.compile(
    r"(\d{1,2})\s+de\s+([a-záéíóú]+)\s+de\s+((?:19|20)\d{2})", re.I
)
TAG_RE = re.compile(r"<[^>]+>")


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
    # Collapse letter-spaced ALL-CAPS headings (e.g. "L E Y").
    text = re.sub(
        r"(?:\b[A-ZÁÉÍÓÚÑ]\s){3,}[A-ZÁÉÍÓÚÑ]\b",
        lambda m: m.group(0).replace(" ", ""),
        text,
    )
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def filename_title(name: str) -> str:
    """Human-readable title derived from the file name (fallback)."""
    base = re.sub(r"\.(pdf|docx?|doc\.pdf)$", "", name, flags=re.I)
    base = base.replace("_", " ").replace("%20", " ")
    base = re.sub(r"\s+", " ", base).strip()
    # Title-case names that are all lowercase or look like a slug.
    if base.islower():
        base = base.title()
    return base


def make_title(text: str, name: str) -> str:
    """Prefer the first document-header line; fall back to the file name."""
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for ln in lines[:8]:
        if TITLE_KW_RE.match(ln):
            title = ln
            # Cut off the session/date tail that follows the reference number.
            title = re.split(r"\.-|\bSesi[óo]n\b|\bEmitid", title)[0].strip(" .-")
            # If the header was truncated to a stub like "LEY DEL", append the
            # next line to complete it.
            if len(title) < 15 and lines.index(ln) + 1 < len(lines):
                title = (title + " " + lines[lines.index(ln) + 1]).strip()
            title = re.sub(r"\s+", " ", title)[:300].strip()
            if len(title) >= 6:
                return title
    return filename_title(name)


def parse_date(text: str, name: str) -> Optional[str]:
    """ISO date from the document text (Spanish long form); year-only fallback."""
    m = DATE_RE.search(text[:4000])
    if m:
        day, mon, year = m.group(1), m.group(2).lower(), m.group(3)
        if mon in SP_MONTHS:
            try:
                return f"{int(year):04d}-{SP_MONTHS[mon]:02d}-{int(day):02d}"
            except ValueError:
                pass
    # Year from file name (e.g. "resolucion_237_7_2020", "enero_2012").
    ym = YEAR_RE.search(name)
    if ym:
        return f"{ym.group(0)}-01-01"
    # Year anywhere in the header.
    ym = YEAR_RE.search(text[:1000])
    if ym:
        return f"{ym.group(0)}-01-01"
    return None


def doc_number(text: str, title: str) -> Optional[str]:
    for src in (title, text[:1500]):
        m = REF_RE.search(src or "")
        if m:
            return m.group(1).upper()
    m = DECRETO_RE.search(text[:1500])
    if m:
        return "DECRETO " + m.group(1)
    return None


def doc_kind(title: str) -> str:
    t = title.upper()
    for kw in ("RESOLUCI", "CIRCULAR", "ACUERDO", "DECRETO", "REGLAMENTO",
               "NORMATIVA", "CONSTITUCI", "CONVENIO", "INSTRUCTIVO", "LEY"):
        if kw in t:
            return {
                "RESOLUCI": "resolucion", "CIRCULAR": "circular",
                "ACUERDO": "acuerdo", "DECRETO": "decreto",
                "REGLAMENTO": "reglamento", "NORMATIVA": "normativa",
                "CONSTITUCI": "constitucion", "CONVENIO": "convenio",
                "INSTRUCTIVO": "instructivo", "LEY": "ley",
            }[kw]
    return "documento"


class BCHRegulationsScraper(BaseScraper):
    """
    Scraper for HN/BCH-Regulations -- Banco Central de Honduras legal and
    regulatory framework (Marco Legal / Leyes y Reglamentos). Full text via
    pdfplumber over the public SharePoint document library.
    Country: HN
    URL: https://www.bch.hn/
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
            "Accept": "application/json;odata=verbose",
        })
        urllib3.disable_warnings()

    def _discover(self) -> List[Dict]:
        """List every PDF in the Marco Legal document library via SharePoint API."""
        try:
            r = self.session.get(LIST_API, timeout=120)
            if r.status_code != 200:
                logger.warning(f"List API HTTP {r.status_code}")
                return []
            results = r.json().get("d", {}).get("results", [])
        except Exception as e:
            logger.warning(f"List API failed: {e}")
            return []

        files, seen = [], set()
        for f in results:
            name = f.get("Name", "")
            sru = f.get("ServerRelativeUrl", "")
            if not name.lower().endswith(".pdf") or not sru:
                continue
            if sru in seen:
                continue
            seen.add(sru)
            files.append({
                "name": name,
                "url": BASE_URL + quote(sru),
                "modified": f.get("TimeLastModified"),
            })
        logger.info(f"Discovered {len(files)} PDF documents")
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
            logger.debug(f"Insufficient text ({len(text)} chars): {item['name']}")
            return None

        title = make_title(text, item["name"])
        return {
            **item,
            "text": text,
            "title": title,
            "doc_number": doc_number(text, title),
            "doc_kind": doc_kind(title),
            "date": parse_date(text, item["name"]),
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
            "issuer": ISSUER,
            "jurisdiction": "HN",
            "language": "es",
            "source_file": raw.get("name"),
            "modified": raw.get("modified"),
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
        # Documents carry a TimeLastModified; downstream upsert dedups by URL.
        # The library is small enough to re-list and re-fetch on each update.
        yield from self.fetch_all()


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="HN/BCH-Regulations — Banco Central de Honduras legal framework"
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

    scraper = BCHRegulationsScraper()

    if args.command == "test":
        logger.info("Testing Banco Central de Honduras connectivity...")
        files = scraper._discover()
        if not files:
            logger.error("No documents discovered")
            sys.exit(1)
        result = scraper._download_and_extract(files[0])
        if result:
            logger.info(f"Title: {result['title'][:120]}")
            logger.info(f"Kind: {result['doc_kind']} | Ref: {result['doc_number']} "
                        f"| Date: {result['date']} | Text: {len(result['text'])} chars")
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
