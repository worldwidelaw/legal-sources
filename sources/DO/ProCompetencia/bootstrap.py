#!/usr/bin/env python3
"""
DO/ProCompetencia -- Comisión Nacional de Defensa de la Competencia (PRO-COMPETENCIA)

Fetches the full text of the resolutions ("Resoluciones del Consejo Directivo")
of the Dominican Republic's national competition authority, the Comisión
Nacional de Defensa de la Competencia (PRO-COMPETENCIA). These are the binding
adjudicatory and regulatory acts the Board of Directors (Consejo Directivo)
issues under the General Competition Law (Ley General de Defensa de la
Competencia, núm. 42-08): merger-control decisions, abuse-of-dominance and
anticompetitive-practice rulings, sanctioning procedures, hierarchical appeals
(recursos jerárquicos), advocacy opinions, market studies, and procedural
resolutions deciding specific cases.

Strategy:
  procompetencia.gob.do is a WordPress site. Each resolution is a post under the
  custom post type `resoluciones-procompetencia`. The Yoast SEO sitemap
    /resoluciones-pc-sitemap.xml
  enumerates the entire corpus (~490 posts). Each post page carries:
    * <meta property="og:title">  -> the resolution title (e.g. "RESOLUCIÓN
      NÚM. 002-2026" or "Resolución núm. FT-01-2015")
    * a direct link to a /wp-content/uploads/YYYY/MM/*.pdf file (the full text)
    * a descriptive subject (og:description) and, on newer posts, a
      "Fecha Publicación: DD/MM/YYYY" field.
  We parse those, download each PDF, and extract the full text with pdfplumber.
  The resolution's own adoption date is recovered from the closing formula
  ("...el día NUMBER (NN) de MES de AÑO (YYYY)"), falling back to a numeric
  dateline matching the resolution's year, then to the listing publication date.

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
logger = logging.getLogger("legal-data-hunter.DO.ProCompetencia")

BASE_URL = "https://procompetencia.gob.do"
SITEMAP_URL = BASE_URL + "/resoluciones-pc-sitemap.xml"
POST_PREFIX = BASE_URL + "/resoluciones-procompetencia/"
MIN_TEXT_CHARS = 200
ISSUER = (
    "Consejo Directivo de la Comisión Nacional de Defensa de la Competencia "
    "(PRO-COMPETENCIA)"
)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

LOC_RE = re.compile(r"<loc>(.*?)</loc>", re.I | re.S)
OG_TITLE_RE = re.compile(r'property="og:title"\s+content="([^"]+)"', re.I)
OG_DESC_RE = re.compile(r'property="og:description"\s+content="([^"]*)"', re.I)
PDF_RE = re.compile(
    r'(https://procompetencia\.gob\.do/wp-content/uploads/[^"\'\s]+?\.pdf)', re.I
)
FECHA_PUB_RE = re.compile(
    r"Fecha\s+Publicaci[oó]n:\s*(\d{1,2})/(\d{1,2})/(\d{4})", re.I
)
TAG_RE = re.compile(r"<[^>]+>")

MONTHS_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}

# The closing adoption formula, e.g. "...el día once (11) de febrero de dos mil
# veintiséis (2026)". The spelled-out day/year words are skipped; the
# authoritative numerals live in the parentheses.
CLOSING_DATE_RE = re.compile(
    r"d[ií]a\s+[a-záéíóúñ]+(?:\s+[a-záéíóúñ]+){0,2}\s*\((\d{1,2})\)\s+de\s+("
    + "|".join(MONTHS_ES) + r")[^()]*?\((\d{4})\)",
    re.I,
)
# Plain numeric dateline, e.g. "3 de diciembre de 2019" / "1º diciembre de 2025".
BODY_DATE_RE = re.compile(
    r"(\d{1,2})[ºo]?\s+(?:de\s+)?(" + "|".join(MONTHS_ES)
    + r")\s+(?:de[l]?\s+)?(\d{4})",
    re.I,
)
# Resolution number/year, e.g. "002-2026", "FT-01-2015", "RR-DE-001-2025".
DOC_NUM_RE = re.compile(r"([A-Z]{0,3}(?:-[A-Z]{1,3})*-?\d{1,4})[-/](\d{4})", re.I)
YEAR_RE = re.compile(r"(20\d{2}|19\d{2})")


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


def unescape_html(txt: str) -> str:
    return (txt.replace("&amp;", "&").replace("&nbsp;", " ")
               .replace("&#038;", "&").replace("&#8217;", "’")
               .replace("&#8220;", "“").replace("&#8221;", "”")
               .replace("&aacute;", "á").replace("&eacute;", "é")
               .replace("&iacute;", "í").replace("&oacute;", "ó")
               .replace("&uacute;", "ú").replace("&ntilde;", "ñ")
               .replace("&Aacute;", "Á").replace("&Eacute;", "É")
               .replace("&Iacute;", "Í").replace("&Oacute;", "Ó")
               .replace("&Uacute;", "Ú").replace("&Ntilde;", "Ñ")
               .replace("&ndash;", "–").replace("&uuml;", "ü")).strip()


def clean_title(raw: str) -> str:
    # og:title is "<title> - Comisión Nacional de Defensa de la Competencia"
    txt = unescape_html(raw)
    txt = re.sub(r"\s*-\s*Comisión Nacional de Defensa de la Competencia\s*$",
                 "", txt, flags=re.I)
    return txt.strip()


def doc_number(title: str, url: str) -> Optional[str]:
    """e.g. '002-2026' from 'RESOLUCIÓN NÚM. 002-2026'."""
    for src in (title, url):
        if not src:
            continue
        m = DOC_NUM_RE.search(src)
        if m:
            return f"{m.group(1)}-{m.group(2)}".upper()
    return None


def number_year(doc_num: Optional[str], title: str) -> Optional[str]:
    for src in (doc_num, title):
        if not src:
            continue
        m = YEAR_RE.search(src)
        if m:
            return m.group(1)
    return None


def pick_date(text: str, exp_year: Optional[str], fallback: Optional[str]) -> Optional[str]:
    """The resolution's own adoption date. The closing formula ('...el día NN de
    MES de YYYY') is authoritative; prefer the last such match (the act's own
    sign-off, after any quoted prior decisions). Else fall back to a numeric
    dateline whose year matches the resolution year. Else the listing date."""
    closing = []
    for m in CLOSING_DATE_RE.finditer(text):
        try:
            d = datetime(int(m.group(3)), MONTHS_ES[m.group(2).lower()],
                         int(m.group(1))).strftime("%Y-%m-%d")
            closing.append(d)
        except (ValueError, KeyError):
            continue
    if closing:
        if exp_year:
            in_year = [d for d in closing if d.startswith(exp_year)]
            if in_year:
                return in_year[-1]
        return closing[-1]

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

    return fallback


class ProCompetenciaScraper(BaseScraper):
    """
    Scraper for DO/ProCompetencia -- Comisión Nacional de Defensa de la
    Competencia (PRO-COMPETENCIA), Dominican Republic. Board of Directors
    resolutions (competition case decisions and regulatory acts).
    Country: DO
    URL: https://procompetencia.gob.do/

    Data types: case_law
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

    def _discover(self) -> List[str]:
        """Enumerate resolution post URLs from the Yoast sitemap."""
        try:
            r = self.session.get(SITEMAP_URL, timeout=120)
            if r.status_code != 200:
                logger.warning(f"Sitemap HTTP {r.status_code}")
                return []
        except Exception as e:
            logger.warning(f"Sitemap fetch failed: {e}")
            return []

        urls = []
        for loc in LOC_RE.findall(r.text):
            loc = loc.strip()
            # Skip the archive root itself (no specific resolution slug).
            if loc.rstrip("/") == POST_PREFIX.rstrip("/"):
                continue
            if loc.startswith(POST_PREFIX) and loc.lower().endswith("/"):
                urls.append(loc)
        seen, ordered = set(), []
        for u in urls:
            if u not in seen:
                seen.add(u)
                ordered.append(u)
        logger.info(f"Discovered {len(ordered)} resolution posts")
        return ordered

    def _parse_post(self, post_url: str) -> Optional[Dict]:
        try:
            r = self.session.get(post_url, timeout=60)
            if r.status_code != 200:
                logger.debug(f"Post HTTP {r.status_code}: {post_url}")
                return None
        except Exception as e:
            logger.warning(f"Post fetch failed for {post_url}: {e}")
            return None

        html = r.text
        tm = OG_TITLE_RE.search(html)
        title = clean_title(tm.group(1)) if tm else None

        pdfs = list(dict.fromkeys(PDF_RE.findall(html)))
        if not pdfs:
            logger.debug(f"No PDF on {post_url}")
            return None
        pdf_url = pdfs[0].replace("&amp;", "&")

        dm = OG_DESC_RE.search(html)
        summary = unescape_html(dm.group(1)) if dm and dm.group(1).strip() else None

        fp = FECHA_PUB_RE.search(html)
        pub_date = None
        if fp:
            try:
                pub_date = datetime(int(fp.group(3)), int(fp.group(2)),
                                    int(fp.group(1))).strftime("%Y-%m-%d")
            except ValueError:
                pub_date = None

        num = doc_number(title or "", pdf_url)
        if not title:
            title = num or post_url

        return {
            "post_url": post_url,
            "pdf_url": pdf_url,
            "title": title,
            "summary": summary,
            "doc_number": num,
            "pub_date": pub_date,
        }

    def _download_and_extract(self, item: Dict) -> Optional[dict]:
        try:
            r = self.session.get(item["pdf_url"], timeout=120)
            if r.status_code != 200 or r.content[:4] != b"%PDF":
                logger.debug(f"Not a live PDF ({r.status_code}): {item['pdf_url']}")
                return None
        except Exception as e:
            logger.warning(f"Download failed for {item['pdf_url']}: {e}")
            return None

        text = clean_text(extract_pdf_text(r.content))
        if len(text) < MIN_TEXT_CHARS:
            logger.debug(f"Insufficient text ({len(text)} chars): {item['pdf_url']}")
            return None

        exp_year = number_year(item.get("doc_number"), item.get("title", ""))
        date = pick_date(text, exp_year, item.get("pub_date"))
        return {**item, "text": text, "date": date, "pdf_size": len(r.content)}

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": raw["post_url"],
            "_source": "DO/ProCompetencia",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["post_url"],
            "pdf_url": raw["pdf_url"],
            "summary": raw.get("summary"),
            "doc_number": raw.get("doc_number"),
            "issuer": ISSUER,
            "jurisdiction": "DO",
            "language": "es",
            "pdf_size": raw.get("pdf_size", 0),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        urls = self._discover()
        yielded = 0
        for url in urls:
            item = self._parse_post(url)
            time.sleep(1.0)
            if not item:
                continue
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
        urls = self._discover()
        yielded = 0
        for url in urls:
            item = self._parse_post(url)
            time.sleep(1.0)
            if not item:
                continue
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
        description="DO/ProCompetencia — PRO-COMPETENCIA Board of Directors resolutions"
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

    scraper = ProCompetenciaScraper()

    if args.command == "test":
        logger.info("Testing PRO-COMPETENCIA connectivity...")
        urls = scraper._discover()
        if not urls:
            logger.error("No resolution posts discovered")
            sys.exit(1)
        logger.info(f"First post: {urls[0]}")
        item = scraper._parse_post(urls[1] if len(urls) > 1 else urls[0])
        if not item:
            logger.error("Failed to parse first post")
            sys.exit(1)
        result = scraper._download_and_extract(item)
        if result:
            logger.info(f"Title: {result['title'][:120]}")
            logger.info(f"Doc number: {result['doc_number']} | Date: {result['date']}")
            logger.info(f"Text: {len(result['text'])} chars")
            logger.info(f"Preview: {result['text'][:200]}")
            logger.info("Connectivity test passed!")
        else:
            logger.error("Failed to extract text from first post")
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
