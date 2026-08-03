#!/usr/bin/env python3
"""
FR/CCAM -- CCAM en ligne (Classification Commune des Actes Médicaux)

Official billing rules and nomenclature of the French Classification Commune
des Actes Médicaux, published by l'Assurance Maladie (CNAM) on the
"CCAM en ligne" portal at https://www.ameli.fr/accueil-de-la-ccam/.

The CCAM is the binding classification used to code, price and bill medical
procedures under French statutory health insurance. The site exposes:

  - The full-text *billing rules* ("règles de facturation"): the consolidated
    "Liste des actes et des prestations" (dispositions générales / diverses),
    the "Définitions, contextes et principes", the CAMNOTE release notes, and
    a set of methodological / topic fiches — all distributed as PDFs linked
    from thin nav stubs.
  - The per-version *nomenclature* PDFs (CCAM_V*.pdf): the full list of act
    codes, libellés and tariffs (large, partly tabular).

Both the règles-de-facturation pages and the /telechargement pages link their
real content as PDFs under /fileadmin/user_upload/documents/*.pdf. This scraper
crawls every CCAM sub-page, collects the distinct PDF documents, and extracts
the full text via common/pdf_extract (OOM-hardened pdfplumber).

Classification:
  - Nomenclature PDFs (CCAM_V*.pdf) and the consolidated Liste des actes et des
    prestations -> ``legislation`` (the binding coded list of acts + tariffs).
  - Everything else (definitions, release notes, methodology, topic fiches)
    -> ``doctrine`` (official explanatory / operational material of a public
    body).

Covers public-repo source request #1034 (FR/CCAM-en-ligne).

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py bootstrap-fast     # High-throughput full pull (VPS)
  python bootstrap.py update             # Re-scan (idempotent via Neon)
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FR.CCAM")

BASE = "https://www.ameli.fr"
ROOT = "/accueil-de-la-ccam"

# Seed pages to crawl for sub-pages + PDF links. The site is small and stable.
SEED_PAGES = [
    f"{ROOT}/index.php",
    f"{ROOT}/telechargement/index.php",
    f"{ROOT}/telechargement/version-actuelle/index.php",
    f"{ROOT}/telechargement/historique/index.php",
    f"{ROOT}/telechargement/fichiers-informatiques-nouvelle-structure/index.php",
    f"{ROOT}/regles-de-facturation/index.php",
]

# Documents whose body is the binding coded list of acts + tariffs.
# Anchored to the filename start so a leaflet like 'depliant_ccam_v2' (a
# doctrine aide-mémoire) is NOT mistaken for the CCAM_V* nomenclature.
LEGISLATION_PATTERNS = (
    re.compile(r"^CCAM_V\d", re.I),
    re.compile(r"^LISTE_DES_ACTES_ET_DES_PRESTATIONS", re.I),
)

MIN_TEXT_CHARS = 300  # below this we treat the PDF as scanned / empty

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.7",
}

# Map French month names to numbers for date heuristics.
FR_MONTHS = {
    "janvier": "01", "fevrier": "02", "février": "02", "mars": "03",
    "avril": "04", "mai": "05", "juin": "06", "juillet": "07", "aout": "08",
    "août": "08", "septembre": "09", "octobre": "10", "novembre": "11",
    "decembre": "12", "décembre": "12",
}


class CCAMScraper(BaseScraper):
    """
    Scraper for FR/CCAM.
    Country: FR
    URL: https://www.ameli.fr/accueil-de-la-ccam/
    Data types: doctrine, legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # ── crawling ────────────────────────────────────────────────────
    def _get(self, path: str) -> str:
        """Fetch a CCAM page. The TYPO3 site soft-errors (HTTP 500) on some
        sub-pages but still returns the full rendered body, so we read the body
        regardless of status code. Charset is ISO-8859-1."""
        url = path if path.startswith("http") else BASE + path
        try:
            r = self.session.get(url, timeout=60)
        except requests.RequestException as e:
            logger.warning(f"GET {path} failed: {e}")
            return ""
        return r.content.decode("iso-8859-1", "replace")

    def _discover_pages(self) -> list[str]:
        """Walk the seed pages to collect every règles-de-facturation /
        telechargement sub-page URL."""
        pages: set[str] = set(SEED_PAGES)
        for seed in SEED_PAGES:
            html = self._get(seed)
            for href in re.findall(
                r'href=["\']([^"\']*(?:regles-de-facturation|telechargement)[^"\']*\.php)["\']',
                html,
            ):
                if href.startswith("/"):
                    pages.add(href)
            time.sleep(0.2)
        return sorted(pages)

    def _discover_pdfs(self) -> list[dict]:
        """Crawl all CCAM pages and collect distinct PDF documents with the
        page title that links them (used as context for the record title)."""
        docs: dict[str, dict] = {}
        for page in self._discover_pages():
            html = self._get(page)
            soup = BeautifulSoup(html, "html.parser")
            title_tag = soup.find("title")
            page_title = (
                title_tag.get_text(strip=True).replace("\xa0", " ")
                if title_tag else page
            )
            for a in soup.find_all(
                "a", href=re.compile(r"/fileadmin/user_upload/documents/.*\.pdf", re.I)
            ):
                href = a["href"]
                if not href.startswith("http"):
                    href = BASE + href
                if href in docs:
                    continue
                link_text = a.get_text(" ", strip=True)
                docs[href] = {
                    "pdf_url": href,
                    "page": BASE + page if page.startswith("/") else page,
                    "page_title": page_title,
                    "link_text": link_text,
                }
            time.sleep(0.2)
        logger.info(f"Discovered {len(docs)} distinct PDF documents")
        return list(docs.values())

    # ── schema helpers ──────────────────────────────────────────────
    @staticmethod
    def _filename(pdf_url: str) -> str:
        return pdf_url.rsplit("/", 1)[-1]

    @classmethod
    def _doc_id(cls, pdf_url: str) -> str:
        stem = cls._filename(pdf_url).rsplit(".", 1)[0]
        return "CCAM-" + re.sub(r"[^0-9A-Za-z]+", "-", stem).strip("-")

    @classmethod
    def _doc_type(cls, pdf_url: str) -> str:
        fn = cls._filename(pdf_url)
        for pat in LEGISLATION_PATTERNS:
            if pat.search(fn):
                return "legislation"
        return "doctrine"

    @classmethod
    def _guess_date(cls, pdf_url: str) -> Optional[str]:
        """Best-effort publication date from the filename (e.g.
        '28_mai_2026', 'mars2011', '_01.07.2025'). Returns None if unknown."""
        fn = cls._filename(pdf_url)
        # dd_month_yyyy  or  dd month yyyy
        m = re.search(r"(\d{1,2})[ _-]([a-zéèûâ]+)[ _-](\d{4})", fn, re.I)
        if m:
            mo = FR_MONTHS.get(m.group(2).lower())
            if mo:
                return f"{m.group(3)}-{mo}-{int(m.group(1)):02d}"
        # dd.mm.yyyy
        m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", fn)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        # month+year glued, e.g. mars2011 / juin2010
        m = re.search(r"([a-zéèûâ]+)(\d{4})", fn, re.I)
        if m:
            mo = FR_MONTHS.get(m.group(1).lower())
            if mo:
                return f"{m.group(2)}-{mo}-01"
        # bare year
        m = re.search(r"(19|20)\d{2}", fn)
        if m:
            return f"{m.group(0)}-01-01"
        return None

    @classmethod
    def _humanise_filename(cls, pdf_url: str) -> str:
        """Turn a document filename into a readable title. The CCAM filenames
        are descriptive (e.g. 'LISTE_DES_ACTES_ET_DES_PRESTATIONS_-28_mai_2026'
        -> 'Liste des actes et des prestations - 28 mai 2026')."""
        stem = cls._filename(pdf_url).rsplit(".", 1)[0]
        s = stem.replace("_", " ").replace("-", " - ")
        s = re.sub(r"\s*-\s*-\s*", " - ", s)       # collapse '- -'
        s = re.sub(r"\s+", " ", s).strip(" -")
        # Title-case ALL-CAPS words but keep CCAM / CAMNOTE / version codes,
        # and lowercase French connector words (de, des, et, la, le, du…).
        keep = re.compile(r"^(CCAM|CAMNOTE|DBF|ACP|V\d|VF|CIR)", re.I)
        connectors = {"DE", "DES", "DU", "ET", "LA", "LE", "LES", "EN", "AUX",
                      "A", "AU", "D", "L", "SUR"}
        words = []
        for w in s.split(" "):
            if w in connectors:
                words.append(w.lower())
            elif w.isupper() and len(w) > 1 and not keep.match(w):
                words.append(w.capitalize())
            else:
                words.append(w)
        return " ".join(words)

    @classmethod
    def _title(cls, raw: dict) -> str:
        # Filenames are the most distinctive label for each document; the page
        # <title> tags are generic section names ("Facturer en CCAM"), and the
        # link text is usually "Télécharger le document".
        base = cls._humanise_filename(raw["pdf_url"])
        base = re.sub(r"\s+", " ", base).strip()[:300]
        return base if base.lower().startswith(("ccam", "camnote")) else f"CCAM — {base}"

    # ── normalize ───────────────────────────────────────────────────
    def normalize(self, raw: dict) -> Optional[dict]:
        pdf_url = raw["pdf_url"]
        doc_id = self._doc_id(pdf_url)
        doc_type = self._doc_type(pdf_url)
        text = extract_pdf_markdown(
            source="FR/CCAM",
            source_id=doc_id,
            pdf_url=pdf_url,
            table=doc_type,
        )
        if not text or len(text.strip()) < MIN_TEXT_CHARS:
            logger.info(f"Skipping {doc_id}: no extractable text")
            return None
        text = text.strip()
        time.sleep(0.8)  # politeness between downloads

        return {
            "_id": doc_id,
            "_source": "FR/CCAM",
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": self._title(raw),
            "text": text,
            "date": self._guess_date(pdf_url),
            "url": raw.get("page") or pdf_url,
            "pdf_url": pdf_url,
            "issuer": "Caisse nationale de l'Assurance Maladie (CNAM)",
            "collection": "Classification Commune des Actes Médicaux (CCAM)",
            "jurisdiction": "FR",
            "language": "fr",
        }

    # ── fetch ───────────────────────────────────────────────────────
    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW PDF references; normalize() downloads + extracts text."""
        for doc in self._discover_pdfs():
            yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """No incremental feed; re-scan (idempotent via Neon)."""
        yield from self.fetch_all()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="FR/CCAM fetcher")
    sub = parser.add_subparsers(dest="command")

    bp = sub.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of samples")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    bf = sub.add_parser("bootstrap-fast", help="High-throughput full fetch (VPS)")
    bf.add_argument("--full", action="store_true", default=True)

    sub.add_parser("update", help="Incremental update")
    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = CCAMScraper()

    if args.command == "test":
        docs = scraper._discover_pdfs()
        logger.info(f"OK: discovered {len(docs)} PDF documents")
        if docs:
            rec = scraper.normalize(docs[0])
            if rec:
                logger.info(f"First: {rec['title'][:110]!r} "
                            f"[{rec['_type']}] ({len(rec['text'])} chars, {rec['date']})")
            else:
                logger.info(f"First doc had no extractable text: {docs[0]['pdf_url']}")
    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=args.sample_size)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")
    elif args.command in ("bootstrap-fast", "bootstrap_fast"):
        stats = scraper.bootstrap_fast()
        logger.info(f"Fast bootstrap complete: {json.dumps(stats, indent=2)}")
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
