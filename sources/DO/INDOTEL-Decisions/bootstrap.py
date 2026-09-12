#!/usr/bin/env python3
"""
DO/INDOTEL-Decisions -- Instituto Dominicano de las Telecomunicaciones (INDOTEL)

Fetches the full text of the resolutions of INDOTEL's Board of Directors
(Consejo Directivo) -- the binding regulatory acts of the Dominican Republic's
telecommunications regulator, issued under the General Telecommunications Law
(Ley General de Telecomunicaciones, núm. 153-98). They cover spectrum
assignments, licensing, sanctioning procedures, public consultations,
interconnection, numbering, broadcasting concessions, tariffs, and more.

Strategy:
  indotel.gob.do is a WordPress site. The transparency listing page
    /transparencia/documentos/resoluciones-del-consejo-directivo/
  enumerates the entire corpus (~3,300 resolutions) as a single static HTML
  page. Each entry is a <li class="el-archivo-N"> block carrying:
    * <span class="name">  -> "Resolución No. 032-2026"
    * a direct link to a /wp-content/uploads/YYYY/MM/*.pdf file
    * a descriptive subject after the <hr/>
    * file size and "Fecha de subida: <strong>21 mayo, 2026</strong>"
  We parse those, download each PDF, and extract the full text with pdfplumber.
  The resolution's own date is recovered from the body's "de fecha DD de MES de
  YYYY" formula, falling back to the listing upload date.

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
from collections import Counter
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
logger = logging.getLogger("legal-data-hunter.DO.INDOTEL-Decisions")

BASE_URL = "https://indotel.gob.do"
LISTING_URL = (
    BASE_URL + "/transparencia/documentos/resoluciones-del-consejo-directivo/"
)
MIN_TEXT_CHARS = 200
ISSUER = "Consejo Directivo del Instituto Dominicano de las Telecomunicaciones (INDOTEL)"

# A realistic browser UA -- the site returns HTTP 473 to non-browser agents.
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Each resolution is one <li class="el-archivo-N"> ... </li> block.
ITEM_RE = re.compile(r'<li class="el-archivo[^"]*">(.*?)</li>', re.I | re.S)
NAME_RE = re.compile(r'<span class="name">(.*?)</span>', re.I | re.S)
PDF_RE = re.compile(r'href="([^"]+?\.pdf)"', re.I)
SIZE_RE = re.compile(r'fa-th-large[^>]*>\s*([\d.]+\s*[KMG]B)', re.I)
UPLOAD_RE = re.compile(r'Fecha de subida:.*?<strong>(.*?)</strong>', re.I | re.S)
TAG_RE = re.compile(r"<[^>]+>")

MONTHS_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}
# Listing upload date: "21 mayo, 2026"
UPLOAD_DATE_RE = re.compile(
    r"(\d{1,2})\s+(" + "|".join(MONTHS_ES) + r")\s*,?\s+(\d{4})", re.I
)
# The page-footer formula carries the resolution's OWN date, e.g.
# "...del Consejo Directivo del INDOTEL de fecha 16 de abril de 2026".
FOOTER_DATE_RE = re.compile(
    r"INDOTEL\s+de\s+fecha\s+(\d{1,2})\s+de\s+(" + "|".join(MONTHS_ES) +
    r")\s+de[l]?\s+(\d{4})", re.I,
)
# Any "de fecha DD de MES de YYYY" -- but bodies also cite OTHER documents'
# dates this way (e.g. the Ley 153-98 of 27 May 1998), so a raw match is only
# trusted when its year matches the resolution's own year.
BODY_DATE_RE = re.compile(
    r"de\s+fecha\s+(\d{1,2})\s+de\s+(" + "|".join(MONTHS_ES) +
    r")\s+de[l]?\s+(\d{4})", re.I,
)
DOC_NUM_RE = re.compile(r"(\d{1,4})[-/.](\d{4})")


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
    txt = (txt.replace("&amp;", "&").replace("&nbsp;", " ")
              .replace("&aacute;", "á").replace("&eacute;", "é")
              .replace("&iacute;", "í").replace("&oacute;", "ó")
              .replace("&uacute;", "ú").replace("&ntilde;", "ñ")
              .replace("&ndash;", "–").replace("&uuml;", "ü")
              .replace("&Aacute;", "Á").replace("&Eacute;", "É")
              .replace("&Iacute;", "Í").replace("&Oacute;", "Ó")
              .replace("&Uacute;", "Ú").replace("&Ntilde;", "Ñ"))
    return re.sub(r"\s+", " ", txt).strip()


def parse_upload_date(block: str) -> Optional[str]:
    m = UPLOAD_RE.search(block)
    if not m:
        return None
    dm = UPLOAD_DATE_RE.search(m.group(1))
    if not dm:
        return None
    try:
        return datetime(int(dm.group(3)), MONTHS_ES[dm.group(2).lower()],
                        int(dm.group(1))).strftime("%Y-%m-%d")
    except (ValueError, KeyError):
        return None


def doc_number(name: str, url: str) -> Optional[str]:
    """e.g. '032-2026' from 'Resolución No. 032-2026'."""
    for src in (name, url):
        m = DOC_NUM_RE.search(src or "")
        if m:
            return f"{m.group(1)}-{m.group(2)}"
    return None


def expected_year(doc_num: Optional[str], upload_date: Optional[str]) -> Optional[str]:
    if doc_num:
        m = re.search(r"-(\d{4})$", doc_num)
        if m:
            return m.group(1)
    if upload_date:
        return upload_date[:4]
    return None


def _most_common_date(regex, text: str) -> Optional[str]:
    found = []
    for m in regex.finditer(text):
        try:
            found.append(datetime(int(m.group(3)), MONTHS_ES[m.group(2).lower()],
                                  int(m.group(1))).strftime("%Y-%m-%d"))
        except (ValueError, KeyError):
            continue
    return Counter(found).most_common(1)[0][0] if found else None


def pick_body_date(text: str, exp_year: Optional[str]) -> Optional[str]:
    """The resolution's own date. The page-footer formula ('...INDOTEL de
    fecha DD de MES de YYYY'), repeated on every page, is authoritative. Else
    fall back to a plain 'de fecha' dateline -- but only if its year matches
    the resolution's own year, since bodies also cite other documents' dates
    (e.g. the Ley 153-98 of 1998). If neither is reliable, return None so the
    caller falls back to the listing's upload date."""
    footer = _most_common_date(FOOTER_DATE_RE, text)
    if footer and (not exp_year or footer.startswith(exp_year)):
        return footer
    if exp_year:
        in_year = []
        for m in BODY_DATE_RE.finditer(text):
            try:
                d = datetime(int(m.group(3)), MONTHS_ES[m.group(2).lower()],
                             int(m.group(1))).strftime("%Y-%m-%d")
                if d.startswith(exp_year):
                    in_year.append(d)
            except (ValueError, KeyError):
                continue
        if in_year:
            return Counter(in_year).most_common(1)[0][0]
    return footer  # year-mismatched footer is still better than upload guess; else None


class INDOTELDecisionsScraper(BaseScraper):
    """
    Scraper for DO/INDOTEL-Decisions -- Instituto Dominicano de las
    Telecomunicaciones (Dominican Republic), Board of Directors resolutions.
    Country: DO
    URL: https://indotel.gob.do/

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
        try:
            r = self.session.get(LISTING_URL, timeout=180)
            if r.status_code != 200:
                logger.warning(f"Listing HTTP {r.status_code}")
                return []
        except Exception as e:
            logger.warning(f"Listing fetch failed: {e}")
            return []

        seen: Dict[str, Dict] = {}
        for block in ITEM_RE.findall(r.text):
            pdf_m = PDF_RE.search(block)
            if not pdf_m:
                continue
            url = pdf_m.group(1).replace("&amp;", "&")
            if not url.startswith("http"):
                url = BASE_URL + ("" if url.startswith("/") else "/") + url
            key = url.lower()
            if key in seen:
                continue

            name_m = NAME_RE.search(block)
            name = clean_inner(name_m.group(1)) if name_m else ""

            # Description: text between the <hr/> and the <div class="datos">.
            desc = ""
            hr = re.search(r"<hr\s*/?>", block, re.I)
            if hr:
                tail = block[hr.end():]
                tail = re.split(r'<div class="datos"', tail, maxsplit=1)[0]
                desc = clean_inner(tail)

            size_m = SIZE_RE.search(block)
            num = doc_number(name, url)
            title = f"{name} — {desc}".strip(" —") if desc else (name or num or url)

            seen[key] = {
                "url": url,
                "title": title,
                "name": name,
                "summary": desc or None,
                "doc_number": num,
                "file_size_label": size_m.group(1).strip() if size_m else None,
                "upload_date": parse_upload_date(block),
            }

        items = list(seen.values())
        logger.info(f"Discovered {len(items)} unique INDOTEL resolutions")
        return items

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

        ey = expected_year(item.get("doc_number"), item.get("upload_date"))
        date = pick_body_date(text, ey) or item.get("upload_date")
        return {**item, "text": text, "date": date, "pdf_size": len(r.content)}

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": raw["url"],
            "_source": "DO/INDOTEL-Decisions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "summary": raw.get("summary"),
            "doc_number": raw.get("doc_number"),
            "issuer": ISSUER,
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
                    logger.info(f"Extracted {yielded} resolutions...")
            time.sleep(1.0)
        logger.info(f"fetch_all complete: {yielded} resolutions with full text")

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
        logger.info(f"fetch_updates complete: {yielded} resolutions")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="DO/INDOTEL-Decisions — INDOTEL Board of Directors resolutions"
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

    scraper = INDOTELDecisionsScraper()

    if args.command == "test":
        logger.info("Testing INDOTEL connectivity...")
        items = scraper._discover()
        if not items:
            logger.error("No PDFs discovered")
            sys.exit(1)
        logger.info(f"First candidate: {items[0]['url']}")
        result = scraper._download_and_extract(items[0])
        if result:
            logger.info(f"Title: {result['title'][:120]}")
            logger.info(f"Doc number: {result['doc_number']} | Date: {result['date']}")
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
