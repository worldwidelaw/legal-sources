#!/usr/bin/env python3
"""
FR/BulletinsOfficielsSociaux -- Bulletins officiels des ministères chargés des
affaires sociales (Santé / Protection sociale / Solidarités  &  Travail /
Emploi / Formation professionnelle)

Official bulletins of the French social-affairs ministries, published at
https://bulletins-officiels.social.gouv.fr/. The site carries two bulletins:

  - BO Santé - Protection sociale - Solidarités
  - BO Travail - Emploi - Formation professionnelle

Each published act (circulaire, instruction, arrêté, décision, note
d'information, avis, convention, délibération, etc.) is a Drupal node whose
*full text* is exposed in the ``field_body_text`` field. The Drupal site serves
the complete node as JSON via the ``?_format=json`` query parameter, so no HTML
scraping or PDF extraction is needed for the core text — the binding body of the
text is in ``field_body_text`` (annexes are linked PDFs).

Strategy:
  - Read the Simple-XML-Sitemap (``/sitemap.xml``) to enumerate every node URL.
  - For each, fetch ``<url>?_format=json``.
  - Keep only real legal documents: nodes that carry a ``field_institutional``
    taxonomy term (the document-type, e.g. /circulaire, /arrete). Utility and
    section pages (accessibilité, cookies, mentions légales, BO landing pages)
    have no ``field_institutional`` and are skipped.
  - Strip the body HTML to clean text; derive date, document type, NOR
    reference and issuer from the structured fields.

Covers public-repo source requests #1036 (Ministères chargés des affaires
sociales) and #1037 (Ministère de la Santé — BO Santé) — both are served by
this single site.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py bootstrap-fast     # High-throughput full pull (VPS)
  python bootstrap.py update             # Re-scan sitemap (idempotent via Neon)
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FR.BulletinsOfficielsSociaux")

BASE = "https://bulletins-officiels.social.gouv.fr"
SITEMAP_URL = BASE + "/sitemap.xml"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.7",
}

MIN_TEXT_CHARS = 200  # below this the node carries no real body (e.g. PDF-only)

# Document-type taxonomy slug -> normalized data type.
# Binding administrative/regulatory acts -> legislation; explanatory /
# instructional texts (circulaires, instructions, notes, guides) -> doctrine.
LEGISLATION_TYPES = {
    "arrete", "decret", "decision", "deliberation", "convention",
    "avenant", "rectificatif", "loi", "ordonnance",
}
DOCTRINE_TYPES = {
    "circulaire", "instruction", "note", "note-dinformation", "avis",
    "lettre", "guide", "manuel", "catalogue", "liste", "communique",
}


class BulletinsOfficielsSociauxScraper(BaseScraper):
    """
    Scraper for FR/BulletinsOfficielsSociaux.
    Country: FR
    URL: https://bulletins-officiels.social.gouv.fr/
    Data types: legislation, doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # ── sitemap enumeration ─────────────────────────────────────────
    def _list_paths(self) -> list[str]:
        """Return every node path from the sitemap (public-host-relative)."""
        try:
            r = self.session.get(SITEMAP_URL, timeout=60)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Sitemap fetch failed: {e}")
            return []

        locs = re.findall(r"<loc>([^<]+)</loc>", r.text)
        paths: list[str] = []
        seen: set[str] = set()
        # Utility / section pages to drop outright (also filtered later by the
        # absence of field_institutional, but cheap to skip here).
        skip_exact = {
            "", "accessibilite", "cookies", "mentions-legales", "plan-du-site",
            "contact", "glossaire", "aide", "recherche", "flux-rss",
            "donnees-personnelles", "sitemap",
        }
        for loc in locs:
            # Sitemap emits the internal cegedim host; keep only the path.
            path = re.sub(r"^https?://[^/]+/?", "", loc).strip("/")
            if path in skip_exact or path in seen:
                continue
            if path.startswith(("bo-", "node/", "user/", "taxonomy/", "media/")):
                continue
            seen.add(path)
            paths.append(path)
        logger.info(f"Collected {len(paths)} candidate node paths from sitemap")
        return paths

    # ── helpers ─────────────────────────────────────────────────────
    @staticmethod
    def _first(field) -> Optional[dict]:
        return field[0] if isinstance(field, list) and field else None

    @staticmethod
    def _strip_html(raw_html: str) -> str:
        # Drop drupal-media placeholders (PDF/annex embeds) and tags, then
        # collapse whitespace and decode entities.
        txt = re.sub(r"<drupal-media[^>]*>.*?</drupal-media>", " ", raw_html,
                     flags=re.DOTALL | re.IGNORECASE)
        txt = re.sub(r"<[^>]+>", " ", txt)
        txt = html.unescape(txt)
        txt = re.sub(r"[ \t ]+", " ", txt)
        txt = re.sub(r"\s*\n\s*", "\n", txt)
        txt = re.sub(r"\n{3,}", "\n\n", txt)
        return txt.strip()

    @classmethod
    def _doc_type_slug(cls, data: dict) -> Optional[str]:
        inst = cls._first(data.get("field_institutional"))
        if not inst:
            return None
        url = inst.get("url") or ""
        return url.strip("/").split("/")[-1] or None

    @classmethod
    def _data_type(cls, slug: Optional[str]) -> str:
        if slug:
            base = slug.split("-")[0]
            if slug in LEGISLATION_TYPES or base in LEGISLATION_TYPES:
                return "legislation"
        return "doctrine"

    # ── schema ──────────────────────────────────────────────────────
    def normalize(self, raw: dict) -> Optional[dict]:
        url = raw["url"]
        try:
            r = self.session.get(url + "?_format=json", timeout=60)
            if r.status_code != 200:
                return None
            data = r.json()
        except (requests.RequestException, ValueError):
            return None

        # Must be a real legal document (carries a document-type taxonomy).
        slug = self._doc_type_slug(data)
        if not slug:
            return None

        body = self._first(data.get("field_body_text"))
        if not body or not body.get("value"):
            return None
        text = self._strip_html(body["value"])
        if len(text) < MIN_TEXT_CHARS:
            return None

        title_f = self._first(data.get("title"))
        title = (title_f.get("value") if title_f else "") or ""
        title = html.unescape(re.sub(r"\s+", " ", title)).strip()
        if not title:
            return None

        # Date: prefer the signature date, then the publication date.
        date = None
        for fld in ("field_additional_date", "field_publication_date", "created"):
            f = self._first(data.get(fld))
            if f and f.get("value"):
                date = f["value"][:10]
                break

        nor_f = self._first(data.get("field_toptitle"))
        nor = (nor_f.get("value").strip() if nor_f and nor_f.get("value") else None)

        issuer_f = self._first(data.get("field_issuer"))
        issuer = None
        if issuer_f and issuer_f.get("url"):
            issuer = issuer_f["url"].strip("/").replace("-", " ")

        nid_f = self._first(data.get("nid"))
        nid = nid_f.get("value") if nid_f else None
        doc_id = f"BO-{nid}" if nid else "BO-" + re.sub(r"[^0-9A-Za-z]+", "-", url.rsplit("/", 1)[-1])[:80]

        return {
            "_id": doc_id,
            "_source": "FR/BulletinsOfficielsSociaux",
            "_type": self._data_type(slug),
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "doc_type": slug,
            "nor": nor,
            "issuer": issuer,
            "url": url,
            "jurisdiction": "FR",
            "language": "fr",
        }

    # ── fetch ───────────────────────────────────────────────────────
    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW node refs; normalize() fetches JSON + extracts full text."""
        for path in self._list_paths():
            yield {"url": f"{BASE}/{path}"}
            time.sleep(0.5)  # politeness between node fetches

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """No incremental feed; re-scan sitemap (idempotent via Neon)."""
        yield from self.fetch_all()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="FR/BulletinsOfficielsSociaux fetcher")
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

    scraper = BulletinsOfficielsSociauxScraper()

    if args.command == "test":
        paths = scraper._list_paths()
        logger.info(f"OK: {len(paths)} node paths listed")
        found = 0
        for p in paths:
            rec = scraper.normalize({"url": f"{BASE}/{p}"})
            if rec:
                logger.info(f"First doc: {rec['title'][:110]!r} "
                            f"[{rec['_type']}/{rec['doc_type']}] "
                            f"({len(rec['text'])} chars, {rec['date']})")
                found += 1
                if found >= 1:
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
