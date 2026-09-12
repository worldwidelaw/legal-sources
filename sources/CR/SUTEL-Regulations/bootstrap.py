#!/usr/bin/env python3
"""
CR/SUTEL-Regulations -- Superintendencia de Telecomunicaciones (Costa Rica)

Fetches the full text of the Costa Rican telecom regulator's regulatory
output:
  * "Principales Resoluciones y Acuerdos del Consejo" — the RCS-numbered
    resolutions issued by the SUTEL Council since 2009 (regulatory
    decisions on interconnection, spectrum, universal-access funding,
    parafiscal contributions, shared use, etc.).
  * SUTEL's own "Normativas" — reglamentos and lineamientos it issues
    (laws authored by other bodies, e.g. the Ley General de
    Telecomunicaciones, are excluded; they belong to legislation sources).

Strategy:
  sutel.go.cr is a Drupal site. The resoluciones view is paginated:
    /sutel/resoluciones?field_tipo_documento_tid=All&page=N
  Each page links directly to /sites/default/files/*.pdf documents. We
  page through the view collecting PDF links, add the reglamentos from
  /normativas, then download each PDF and extract full text with
  pdfplumber.

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
logger = logging.getLogger("legal-data-hunter.CR.SUTEL-Regulations")

BASE_URL = "https://www.sutel.go.cr"
MIN_TEXT_CHARS = 200
RESOLUCIONES_VIEW = "/sutel/resoluciones?field_tipo_documento_tid=All"
NORMATIVAS_PAGE = "/normativas"
MAX_PAGES = 60

PDF_RE = re.compile(r'href=["\']([^"\']+?\.pdf)["\']', re.I)

# Filenames to skip: external laws (belong to legislation sources), forms,
# FAQs, and presentation/decks.
SKIP_RE = re.compile(
    r"(^|/)(ley_|preguntas_frecuentes|formulario|presentacion_)", re.I
)

MONTHS_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}
DATE_TEXT_RE = re.compile(
    r"(\d{1,2})\s+de\s+(" + "|".join(MONTHS_ES) + r")\s+de\s+(\d{4})", re.I
)

# RCS-NNN-YYYY resolution identifier in the filename.
RCS_RE = re.compile(r"\brcs[-_]?(\d{1,4})[-_](\d{4})\b", re.I)


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
    text = re.sub(
        r"(?:\b[A-ZÁÉÍÓÚÑ]\s){3,}[A-ZÁÉÍÓÚÑ]\b",
        lambda m: m.group(0).replace(" ", ""),
        text,
    )
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_date_from_text(text: str) -> Optional[str]:
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
    m = RCS_RE.search(filename_of(rel_path))
    if m:
        return f"RCS-{m.group(1)}-{m.group(2)}"
    return None


def make_title(rel_path: str) -> str:
    base = filename_of(rel_path)
    base = base.replace("_", " ").replace("-", " ")
    base = re.sub(r"\s+", " ", base).strip()
    # Capitalise first letter for readability.
    return base[:1].upper() + base[1:] if base else base


def doc_category(rel_path: str) -> str:
    name = filename_of(rel_path).lower()
    if RCS_RE.search(name) or "resolucion" in name:
        return "resolucion"
    if "reglamento" in name:
        return "reglamento"
    if "lineamiento" in name:
        return "lineamientos"
    if "acuerdo" in name:
        return "acuerdo"
    if "politica" in name:
        return "politica"
    return "normativa"


class SUTELRegulationsScraper(BaseScraper):
    """
    Scraper for CR/SUTEL-Regulations — Superintendencia de
    Telecomunicaciones (Costa Rica).
    Country: CR
    URL: https://www.sutel.go.cr/

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

    def _links_on(self, html: str) -> List[str]:
        out = []
        for p in PDF_RE.findall(html):
            p = p.replace("&amp;", "&")
            if p.startswith("http"):
                parsed = urllib.parse.urlparse(p)
                if "sutel.go.cr" not in parsed.netloc:
                    continue
                p = parsed.path
            if not p.startswith("/"):
                p = "/" + p
            if "/sites/default/files/" not in p:
                continue
            if SKIP_RE.search(p):
                continue
            out.append(urllib.parse.quote(urllib.parse.unquote(p), safe="/"))
        return out

    def _add(self, seen: Dict[str, Dict], path: str) -> bool:
        key = path.lower()
        if key in seen:
            return False
        seen[key] = {
            "rel_path": path,
            "url": BASE_URL + path,
            "category": doc_category(path),
            "doc_number": doc_number(path),
        }
        return True

    def _discover(self) -> List[Dict]:
        seen: Dict[str, Dict] = {}

        # 1. SUTEL's own reglamentos / lineamientos.
        try:
            r = self.session.get(BASE_URL + NORMATIVAS_PAGE, timeout=60)
            if r.status_code == 200:
                new = sum(self._add(seen, p) for p in self._links_on(r.text))
                logger.info(f"{NORMATIVAS_PAGE}: {new} reglamentos/normativas")
        except Exception as e:
            logger.warning(f"{NORMATIVAS_PAGE} failed: {e}")
        time.sleep(1.0)

        # 2. Paginated Council resolutions view.
        empty_streak = 0
        for page in range(MAX_PAGES):
            url = f"{BASE_URL}{RESOLUCIONES_VIEW}&page={page}"
            try:
                r = self.session.get(url, timeout=60)
                if r.status_code != 200:
                    logger.warning(f"resoluciones page {page}: HTTP {r.status_code}")
                    break
                links = self._links_on(r.text)
                new = sum(self._add(seen, p) for p in links)
                logger.info(f"resoluciones page {page}: {len(links)} refs, {new} new")
                if new == 0:
                    empty_streak += 1
                    if empty_streak >= 2:
                        logger.info("No new docs for 2 pages; stopping pagination.")
                        break
                else:
                    empty_streak = 0
            except Exception as e:
                logger.warning(f"resoluciones page {page} failed: {e}")
                break
            time.sleep(1.0)

        items = list(seen.values())
        logger.info(f"Discovered {len(items)} unique SUTEL documents")
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
            "doc_number": item.get("doc_number"),
            "pdf_size": len(r.content),
        }

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": raw["url"],
            "_source": "CR/SUTEL-Regulations",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "category": raw.get("category"),
            "doc_number": raw.get("doc_number"),
            "issuer": "Superintendencia de Telecomunicaciones (SUTEL)",
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
        description="CR/SUTEL-Regulations — Superintendencia de Telecomunicaciones"
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

    scraper = SUTELRegulationsScraper()

    if args.command == "test":
        logger.info("Testing SUTEL connectivity...")
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
