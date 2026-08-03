#!/usr/bin/env python3
"""
FR/AssuranceMaladie -- Circulaires réseau de l'Assurance Maladie (CNAM)

Official regulatory circulars ("directives réseau" / "directives extranet") of the
French statutory health-insurance fund (Caisse nationale de l'Assurance Maladie,
CNAM), published at https://circulaires.ameli.fr/.

These circulaires instruct the CPAM / CGSS / CARSAT / CRAMIF network on how to
apply social-security and health-insurance law: gestion du risque, prestations,
prévention des risques professionnels (AT/MP), relations avec les professionnels
de santé, etc. They are binding administrative directives — classified here as
``doctrine`` (administrative interpretation/instruction), consistent with how the
project classes circulaires elsewhere.

Site structure (Drupal):
  - The listing lives at ``/directives`` with a numeric pager (?page=N), ~153
    pages × 20 = ~3,060 circulaires spanning 1999–present.
  - Each circulaire is a detail page ``/circulaire/{slug}`` carrying structured
    metadata (date, objet, résumé, domaine, plan de classement, émetteur, mots
    clés) AND a link to the full-text PDF under
    ``/sites/default/files/directives/...``.

Strategy:
  - Walk ``/directives?page=N`` to enumerate every ``/circulaire/{slug}`` link.
  - For each, fetch the detail HTML, parse the metadata table, find the PDF URL,
    download the PDF and extract its full text (pdfplumber, OOM-hardened shared
    helper). The binding text is in the PDF; the HTML résumé is kept as a field.

The Drupal ``?_format=json`` endpoint is WAF-rejected ("Request Rejected"), so
the HTML detail page + PDF extraction is the supported path.

Covers public-repo source requests #1033 and #1035 (FR/Assurance-maladie).

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py bootstrap-fast     # High-throughput full pull (VPS)
  python bootstrap.py update             # Re-scan listing (idempotent via Neon)
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import html
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FR.AssuranceMaladie")

SOURCE_ID = "FR/AssuranceMaladie"
BASE = "https://circulaires.ameli.fr"
LISTING = BASE + "/directives"
MAX_PAGES = 200  # safety cap; site currently has ~153 pages
MIN_TEXT_CHARS = 200

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.7",
}


class AssuranceMaladieScraper(BaseScraper):
    """
    Scraper for FR/AssuranceMaladie.
    Country: FR
    URL: https://circulaires.ameli.fr/
    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # ── listing enumeration ─────────────────────────────────────────
    def _list_slugs(self) -> Generator[str, None, None]:
        """Yield every /circulaire/{slug} (slug only) across the pager, in order."""
        seen: set[str] = set()
        for page in range(MAX_PAGES):
            url = f"{LISTING}?page={page}"
            try:
                r = self.session.get(url, timeout=60)
                r.raise_for_status()
            except requests.RequestException as e:
                logger.warning(f"Listing page {page} failed: {e}")
                break
            slugs = re.findall(r'href="/circulaire/([^"#?]+)"', r.text)
            new = [s for s in slugs if s not in seen]
            if not new:
                # No fresh links: either empty page or pager ran out.
                logger.info(f"Listing exhausted at page {page}")
                break
            for s in new:
                seen.add(s)
                yield s
            time.sleep(0.5)

    # ── helpers ─────────────────────────────────────────────────────
    @staticmethod
    def _main_text(h: str) -> str:
        m = re.search(r"<article.*?</article>", h, re.S) or re.search(
            r"<main.*?</main>", h, re.S
        )
        seg = m.group(0) if m else h
        txt = re.sub(r"<[^>]+>", " ", seg)
        txt = html.unescape(txt)
        txt = re.sub(r"[ \t ]+", " ", txt)
        return txt.strip()

    @staticmethod
    def _field(text: str, label: str, until: tuple) -> Optional[str]:
        """Grab the value following 'Label :' up to the next known label."""
        stop = "|".join(re.escape(u) for u in until)
        m = re.search(
            rf"{re.escape(label)}\s*:?\s*(.*?)(?:{stop}|$)", text, re.S
        )
        if not m:
            return None
        v = re.sub(r"\s+", " ", m.group(1)).strip(" :;-")
        return v or None

    @staticmethod
    def _to_iso(fr_date: Optional[str]) -> Optional[str]:
        if not fr_date:
            return None
        m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", fr_date)
        if not m:
            return None
        d, mo, y = m.groups()
        return f"{y}-{int(mo):02d}-{int(d):02d}"

    # ── schema ──────────────────────────────────────────────────────
    def normalize(self, raw: dict) -> Optional[dict]:
        slug = raw["slug"]
        detail_url = f"{BASE}/circulaire/{slug}"
        try:
            r = self.session.get(detail_url, timeout=60)
            if r.status_code != 200:
                return None
            h = r.text
        except requests.RequestException:
            return None

        # PDF link (full text). Accept .pdf or .PDF.
        pdf_m = re.search(r'href="([^"]+\.(?:pdf|PDF))"', h)
        if not pdf_m:
            return None
        pdf_url = pdf_m.group(1)
        if pdf_url.startswith("/"):
            pdf_url = BASE + pdf_url

        text = extract_pdf_markdown(
            SOURCE_ID, slug, pdf_url=pdf_url, table="doctrine"
        )
        if not text or len(text) < MIN_TEXT_CHARS:
            return None

        meta = self._main_text(h)

        # Title: header <title> "CIR-9/2026 | Directives extranet" + objet.
        tt = re.findall(r"<title>([^<]+)</title>", h)
        head = (tt[0].split("|")[0].strip() if tt else "") or slug
        objet = self._field(
            meta, "Objet",
            ("Liens:", "Liens externes", "Plan de classement", "Emetteur",
             "Résumé", "Mots clés"),
        )
        title = f"{head} — {objet}" if objet else head
        title = html.unescape(re.sub(r"\s+", " ", title)).strip()[:500]

        date = self._to_iso(self._field(meta, "Date", ("Domaine", "Objet", "Emetteur")))

        domaine = self._field(meta, "Domaine(s)", ("Nouveau", "Modificatif", "Objet"))
        resume = self._field(meta, "Résumé", ("Mots clés", "Pour mise en",))
        plan = self._field(
            meta, "Plan de classement", ("Emetteur", "Résumé", "Pièces jointes")
        )
        mots = self._field(meta, "Mots clés", ("La Direct", "Le Direct"))

        return {
            "_id": slug,
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "objet": objet,
            "resume": resume,
            "domaine": domaine,
            "plan_classement": plan,
            "mots_cles": mots,
            "pdf_url": pdf_url,
            "url": detail_url,
            "jurisdiction": "FR",
            "language": "fr",
        }

    # ── fetch ───────────────────────────────────────────────────────
    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW slug refs; normalize() fetches detail + PDF full text."""
        for slug in self._list_slugs():
            yield {"slug": slug}

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """No incremental feed; re-scan listing (idempotent via Neon)."""
        yield from self.fetch_all()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="FR/AssuranceMaladie fetcher")
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

    scraper = AssuranceMaladieScraper()

    if args.command == "test":
        n = 0
        for slug in scraper._list_slugs():
            rec = scraper.normalize({"slug": slug})
            if rec:
                logger.info(
                    f"First doc: {rec['title'][:110]!r} "
                    f"({len(rec['text'])} chars, {rec['date']})"
                )
                n += 1
                if n >= 1:
                    break
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
