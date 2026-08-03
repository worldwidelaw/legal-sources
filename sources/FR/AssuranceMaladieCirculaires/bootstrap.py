#!/usr/bin/env python3
"""
FR/AssuranceMaladieCirculaires -- Circulaires & directives de l'Assurance Maladie

Official directives ("circulaires", "lettres-réseau", etc.) of the French
national health-insurance fund (CNAM / l'Assurance Maladie), published on the
"Directives extranet" portal at https://circulaires.ameli.fr/.

Each directive is a Drupal node listed in a paginated table (date, reference,
subject) and links to a PDF that holds the full directive text. The portal's
BIG-IP WAF rejects the Drupal ``?_format=json`` endpoint, so this scraper:

  - Walks the paginated listing (``/?page=N``) to collect every directive's
    node URL plus its date, reference number and subject.
  - Fetches each node's HTML to find the linked PDF under
    ``/sites/default/files/directives/…``.
  - Downloads + extracts the full text from the PDF via common/pdf_extract.

These are operational/regulatory directives of l'Assurance Maladie addressed to
the regional funds (CPAM/CARSAT/CGSS…), classified here as ``doctrine`` (binding
operational instructions of a public body, not statutory law).

Covers public-repo source request #1035.

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
logger = logging.getLogger("legal-data-hunter.FR.AssuranceMaladieCirculaires")

BASE = "https://circulaires.ameli.fr"
LISTING_URL = BASE + "/?page={page}"
MAX_PAGES = 200  # safety cap; listing currently has ~153 pages
MIN_TEXT_CHARS = 400  # below this we treat the PDF as scanned / empty

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.7",
}


class AssuranceMaladieCirculairesScraper(BaseScraper):
    """
    Scraper for FR/AssuranceMaladieCirculaires.
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

    # ── listing ─────────────────────────────────────────────────────
    @staticmethod
    def _iso_date(fr_date: str) -> Optional[str]:
        m = re.match(r"(\d{2})/(\d{2})/(\d{4})", fr_date.strip())
        if not m:
            return None
        d, mo, y = m.groups()
        return f"{y}-{mo}-{d}"

    def _list_page(self, page: int) -> list[dict]:
        """Parse one listing page into directive metadata dicts."""
        try:
            r = self.session.get(LISTING_URL.format(page=page), timeout=60)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Listing page {page} failed: {e}")
            return []

        soup = BeautifulSoup(r.text, "html.parser")
        items: list[dict] = []
        for tr in soup.find_all("tr"):
            a = tr.find("a", href=re.compile(r"^/circulaire/"))
            if not a:
                continue
            cells = [c.get_text(" ", strip=True) for c in tr.find_all("td")]
            date = ref = title = None
            for c in cells:
                if date is None and re.match(r"\d{2}/\d{2}/\d{4}", c):
                    date = c
                elif ref is None and re.match(r"[A-Z]", c) and len(c) < 30 and "/" in c:
                    ref = c
            # subject = longest cell
            if cells:
                title = max(cells, key=len)
            items.append({
                "node_url": BASE + a["href"],
                "ref": ref or a["href"].rsplit("/", 1)[-1],
                "date": self._iso_date(date) if date else None,
                "title": title or (ref or a["href"].rsplit("/", 1)[-1]),
            })
        return items

    def _list_all(self) -> list[dict]:
        out: list[dict] = []
        seen: set[str] = set()
        for page in range(MAX_PAGES):
            items = self._list_page(page)
            if not items:
                break
            for it in items:
                if it["node_url"] in seen:
                    continue
                seen.add(it["node_url"])
                out.append(it)
            time.sleep(0.5)
        logger.info(f"Collected {len(out)} directives from listing")
        return out

    def _pdf_url(self, node_url: str) -> Optional[str]:
        """Fetch a node's HTML and return its directive PDF URL."""
        try:
            r = self.session.get(node_url, timeout=60)
            r.raise_for_status()
        except requests.RequestException:
            return None
        m = re.search(r'href="(/sites/default/files/[^"]+\.pdf[^"]*)"', r.text)
        if m:
            return BASE + m.group(1)
        m = re.search(r'href="(https?://[^"]+\.pdf[^"]*)"', r.text)
        return m.group(1) if m else None

    # ── schema ──────────────────────────────────────────────────────
    @staticmethod
    def _doc_id(ref: str, node_url: str) -> str:
        base = ref or node_url.rsplit("/", 1)[-1]
        return "AM-" + re.sub(r"[^0-9A-Za-z]+", "-", base).strip("-")

    def normalize(self, raw: dict) -> Optional[dict]:
        node_url = raw["node_url"]
        pdf_url = self._pdf_url(node_url)
        if not pdf_url:
            return None

        doc_id = self._doc_id(raw.get("ref", ""), node_url)
        text = extract_pdf_markdown(
            source="FR/AssuranceMaladieCirculaires",
            source_id=doc_id,
            pdf_url=pdf_url,
            table="legislation",
        )
        if not text or len(text.strip()) < MIN_TEXT_CHARS:
            return None
        text = text.strip()
        time.sleep(0.8)  # politeness between downloads

        title = raw.get("title") or raw.get("ref") or doc_id
        title = re.sub(r"\s+", " ", title).strip()[:300]
        ref = raw.get("ref")
        if ref and ref not in title:
            title = f"{ref} — {title}"[:300]

        return {
            "_id": doc_id,
            "_source": "FR/AssuranceMaladieCirculaires",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "reference": ref,
            "url": node_url,
            "pdf_url": pdf_url,
            "issuer": "Caisse nationale de l'Assurance Maladie (CNAM)",
            "jurisdiction": "FR",
            "language": "fr",
        }

    # ── fetch ───────────────────────────────────────────────────────
    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW directive refs; normalize() fetches PDF + extracts text."""
        for it in self._list_all():
            yield it

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """No incremental feed; re-scan listing (idempotent via Neon)."""
        yield from self.fetch_all()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="FR/AssuranceMaladieCirculaires fetcher")
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

    scraper = AssuranceMaladieCirculairesScraper()

    if args.command == "test":
        items = scraper._list_page(0)
        logger.info(f"OK: page 0 has {len(items)} directives")
        if items:
            rec = scraper.normalize(items[0])
            if rec:
                logger.info(f"First: {rec['title'][:110]!r} "
                            f"({len(rec['text'])} chars, {rec['date']})")
            else:
                logger.info(f"First directive had no extractable PDF: {items[0]['node_url']}")
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
