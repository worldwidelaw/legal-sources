#!/usr/bin/env python3
"""
DO/DGII-Normativa -- Dirección General de Impuestos Internos (Dominican Republic)

Fetches the full text of the Dominican tax authority's regulatory corpus:
  * "Normas Generales" — general norms issued by the DGII under arts. 34-35
    of the Tax Code (Ley 11-92) on ISR, ITBIS, ISC, fiscal vouchers,
    incentive laws, casinos, motor vehicles, sectoral norms, etc.
  * "Resoluciones" — DGII resolutions (indexing/multiplier resolutions for
    fiscal closure, ISC amount indexation, and other binding resolutions).

Strategy:
  dgii.gov.do is an ASP.NET/SharePoint site. Two listing pages enumerate the
  corpus as static HTML with direct links to /Documents/*.pdf files:
    /legislacion/normasGenerales/Paginas/default.aspx
    /legislacion/resoluciones/Paginas/default.aspx
  Each <a> tag's text carries a rich descriptive title plus "Año:" /
  "Modificado:" metadata. We parse those, download each PDF, and extract the
  full text with pdfplumber.

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
logger = logging.getLogger("legal-data-hunter.DO.DGII-Normativa")

BASE_URL = "https://dgii.gov.do"
MIN_TEXT_CHARS = 200

LISTING_PAGES = [
    ("/legislacion/normasGenerales/Paginas/default.aspx", "norma_general"),
    ("/legislacion/resoluciones/Paginas/default.aspx", "resolucion"),
]

# Anchor tags pointing at a /Documents/*.pdf within /legislacion/.
ANCHOR_RE = re.compile(
    r'<a[^>]+href=["\']([^"\']*?/legislacion/[^"\']*?/Documents/[^"\']+?\.pdf)["\'][^>]*>(.*?)</a>',
    re.I | re.S,
)
TAG_RE = re.compile(r"<[^>]+>")

# "Modificado: DD/MM/YYYY" and "Año: YYYY" carried in the anchor text.
MOD_RE = re.compile(r"Modificado:\s*(\d{1,2})/(\d{1,2})/(\d{4})")
YEAR_RE = re.compile(r"Año:\s*(\d{4})")
# Boilerplate suffix begins at the first of these tokens.
BOILER_RE = re.compile(r"\s*(?:Año:|Modificado:|Tamaño:|Descargar).*$", re.I | re.S)

MONTHS_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}
DATE_TEXT_RE = re.compile(
    r"(\d{1,2})\s+de\s+(" + "|".join(MONTHS_ES) + r")\s+de[l]?\s+(\d{4})", re.I
)
# DGII norms/resolutions close with the formula:
#   "Dada en Santo Domingo ... a los (17) días del mes de julio
#    del año dos mil diecinueve (2019)."
# Anchoring on "Dada/Dado en" disambiguates this promulgation date from
# other datelines the body cites (e.g. the Tax Code's 1992 date). The day
# may be parenthesised ("(17)") or bare ("04"); the year is parenthesised.
PROMULGATION_RE = re.compile(
    r"Dad[oa]\s+en\b.{0,220}?\(?(\d{1,2})\)?\s*d[ií]as?\s+del\s+mes\s+de\s+(" +
    "|".join(MONTHS_ES) + r")\s+del?\s+a[ñn]o\s+[^()]{0,80}?\((\d{4})\)",
    re.I | re.S,
)


def extract_pdf_text(content: bytes) -> str:
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            pages = []
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    pages.append(t)
                # Release per-page layout + cached textmap to cap peak RSS
                # on large PDFs (prevents OOM exit 137 on the fleet).
                page.flush_cache()
                try:
                    page.get_textmap.cache_clear()
                except AttributeError:
                    pass
        return "\n\n".join(pages)
    except Exception as e:
        logger.warning(f"PDF extraction failed: {e}")
        return ""


def clean_text(text: str) -> str:
    text = text.replace("\x00", " ")
    # Collapse letter-spaced ALL-CAPS headings (e.g. "R E S O L U C I Ó N").
    text = re.sub(
        r"(?:\b[A-ZÁÉÍÓÚÑ]\s){3,}[A-ZÁÉÍÓÚÑ]\b",
        lambda m: m.group(0).replace(" ", ""),
        text,
    )
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def clean_inner(html_fragment: str) -> str:
    txt = TAG_RE.sub(" ", html_fragment)
    txt = txt.replace("&amp;", "&").replace("&nbsp;", " ")
    txt = txt.replace("&aacute;", "á").replace("&eacute;", "é")
    txt = txt.replace("&iacute;", "í").replace("&oacute;", "ó").replace("&uacute;", "ú")
    txt = txt.replace("&ntilde;", "ñ").replace("&ndash;", "–")
    return re.sub(r"\s+", " ", txt).strip()


def parse_dates_from_text(text: str) -> List[str]:
    """All 'DD de mes de YYYY' datelines in the body, in document order."""
    out = []
    for m in DATE_TEXT_RE.finditer(text):
        try:
            out.append(datetime(int(m.group(3)), MONTHS_ES[m.group(2).lower()],
                                int(m.group(1))).strftime("%Y-%m-%d"))
        except (ValueError, KeyError):
            continue
    return out


def parse_promulgation_date(text: str) -> Optional[str]:
    """The 'Dada en ... a los (DD) días del mes de MES del año ... (YYYY)'
    closing formula — the document's own promulgation date."""
    m = PROMULGATION_RE.search(text)
    if not m:
        return None
    try:
        return datetime(int(m.group(3)), MONTHS_ES[m.group(2).lower()],
                        int(m.group(1))).strftime("%Y-%m-%d")
    except (ValueError, KeyError):
        return None


def pick_date(text: str, expected_year: Optional[str],
              mod_date: Optional[str]) -> Optional[str]:
    """
    Prefer the explicit promulgation formula. Otherwise DGII bodies cite
    other laws' dates (e.g. the Tax Code's 1992 date), so a raw dateline is
    unreliable — only trust one whose year matches the document's own year.
    Last resort: the listing's modification date.
    """
    promulgated = parse_promulgation_date(text)
    if promulgated:
        return promulgated
    if expected_year:
        matches = [d for d in parse_dates_from_text(text)
                   if d.startswith(expected_year)]
        if matches:
            return matches[-1]
    return mod_date


def expected_year(doc_num: Optional[str], year: Optional[str]) -> Optional[str]:
    if year:
        return year
    if doc_num:
        # Trailing 4-digit year (e.g. DDG-AR1-2026-00003, 72-2025).
        m = re.search(r"(\d{4})(?:-\d+)?$", doc_num)
        if m and 1990 <= int(m.group(1)) <= 2099:
            return m.group(1)
        # 2-digit year suffix (e.g. 07-19 -> 2019, 02-96 -> 1996).
        m = re.search(r"-(\d{2})$", doc_num)
        if m:
            yy = int(m.group(1))
            return f"19{yy:02d}" if yy >= 80 else f"20{yy:02d}"
    return None


def filename_of(rel_path: str) -> str:
    name = urllib.parse.unquote(rel_path.rsplit("/", 1)[-1])
    return re.sub(r"\.pdf$", "", name, flags=re.I).strip()


def doc_number(rel_path: str, title: str) -> Optional[str]:
    """Best-effort identifier, e.g. '07-19', 'DDG-AR1-2026-00003', '72-2025'."""
    # Resolution identifiers like "DDG- AR1-2026-00003" (spaces collapsed).
    for source in (title, filename_of(rel_path)):
        m = re.search(r"DDG-?\s*AR\d-\d{4}-\d{3,6}", source, re.I)
        if m:
            return re.sub(r"\s+", "", m.group(0)).upper()
    base = filename_of(rel_path)
    # Strip a leading "norma"/"resolucion" word.
    base = re.sub(r"^(norma|resoluci[óo]n)\s*", "", base, flags=re.I)
    m = re.search(r"\b(\d{1,3}-\d{2,4})\b", base)
    if m:
        return m.group(1)
    # Fall back to a number found in the title.
    m = re.search(r"\b(\d{1,3}-\d{2,4})\b", title)
    return m.group(1) if m else None


def subcategory_of(rel_path: str) -> Optional[str]:
    """The folder name under .../Documents/ (e.g. the tax it concerns)."""
    m = re.search(r"/Documents/([^/]+)/", urllib.parse.unquote(rel_path))
    return m.group(1).strip() if m else None


class DGIINormativaScraper(BaseScraper):
    """
    Scraper for DO/DGII-Normativa — Dirección General de Impuestos Internos
    (Dominican Republic).
    Country: DO
    URL: https://dgii.gov.do/

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

    def _normalize_path(self, raw: str) -> Optional[str]:
        p = raw.replace("&amp;", "&")
        if p.startswith("http"):
            parsed = urllib.parse.urlparse(p)
            if "dgii.gov.do" not in parsed.netloc:
                return None
            p = parsed.path
        if not p.startswith("/"):
            p = "/" + p
        # Re-encode (handles spaces and accents in file paths).
        return urllib.parse.quote(urllib.parse.unquote(p), safe="/")

    def _discover(self) -> List[Dict]:
        seen: Dict[str, Dict] = {}
        for page_path, default_cat in LISTING_PAGES:
            try:
                r = self.session.get(BASE_URL + page_path, timeout=60)
                if r.status_code != 200:
                    logger.warning(f"{page_path}: HTTP {r.status_code}")
                    continue
            except Exception as e:
                logger.warning(f"{page_path} failed: {e}")
                continue

            new = 0
            for raw_href, inner in ANCHOR_RE.findall(r.text):
                path = self._normalize_path(raw_href)
                if not path:
                    continue
                key = path.lower()
                if key in seen:
                    continue
                text = clean_inner(inner)
                mod = MOD_RE.search(text)
                mod_date = None
                if mod:
                    try:
                        mod_date = datetime(int(mod.group(3)), int(mod.group(2)),
                                            int(mod.group(1))).strftime("%Y-%m-%d")
                    except ValueError:
                        mod_date = None
                year_m = YEAR_RE.search(text)
                title = BOILER_RE.sub("", text).strip(" .-")
                if not title:
                    title = filename_of(path)
                seen[key] = {
                    "rel_path": path,
                    "url": BASE_URL + path,
                    "title": title,
                    "category": default_cat,
                    "subcategory": subcategory_of(path),
                    "doc_number": doc_number(path, title),
                    "year": year_m.group(1) if year_m else None,
                    "mod_date": mod_date,
                }
                new += 1
            logger.info(f"{page_path}: {new} documents")
            time.sleep(1.0)

        items = list(seen.values())
        logger.info(f"Discovered {len(items)} unique DGII documents")
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

        # Date preference: body dateline matching the document's own year,
        # else last body dateline, else listing "Modificado" date.
        ey = expected_year(item.get("doc_number"), item.get("year"))
        date = pick_date(text, ey, item.get("mod_date"))
        return {**item, "text": text, "date": date, "pdf_size": len(r.content)}

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": raw["url"],
            "_source": "DO/DGII-Normativa",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "category": raw.get("category"),
            "subcategory": raw.get("subcategory"),
            "doc_number": raw.get("doc_number"),
            "year": raw.get("year"),
            "issuer": "Dirección General de Impuestos Internos (DGII)",
            "jurisdiction": "DO",
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
        description="DO/DGII-Normativa — Dirección General de Impuestos Internos"
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

    scraper = DGIINormativaScraper()

    if args.command == "test":
        logger.info("Testing DGII connectivity...")
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
