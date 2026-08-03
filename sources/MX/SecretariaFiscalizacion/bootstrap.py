#!/usr/bin/env python3
"""
MX/SecretariaFiscalizacion -- Secretaría Anticorrupción y Buen Gobierno
                              (ex Secretaría de la Función Pública, SFP)
                              Internal normative provisions (Normateca Interna)

Mexico's federal comptroller / anti-corruption secretariat (the body the source
issue calls the "Office of the Comptroller — audit/accountability") publishes
its binding internal administrative norms — ACUERDOs, manuales, lineamientos,
disposiciones — through the SANI system (Sistema de Administración de Normas
Internas de la Administración Pública Federal).

The "Normas Internas Vigentes de la SFP" register exposes a per-document
download endpoint that returns the full-text PDF of each norm:

    https://normasapf.buengobierno.gob.mx/NORMASAPF/SFP.jsf      (listing)
    https://normasapf.buengobierno.gob.mx/NORMASAPF/Descarga?id= (per-norm PDF)

Strategy:
  - Fetch the SFP.jsf register page; its PrimeFaces datatable embeds every
    norm row (id, name, issuer, emission date) and its Descarga?id= URL
    directly in the rendered HTML — no JSF postback needed.
  - Download + extract full text from each norm PDF via common/pdf_extract.py.
  - Skip any norm whose PDF has no text layer (scanned image, < MIN_TEXT_CHARS).

Classified as `doctrine`: these are official internal administrative norms /
guidance of a federal secretariat (good-conduct codes, organizational acuerdos,
operating lineamientos), not laws of general application.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py bootstrap-fast     # High-throughput full pull (VPS)
  python bootstrap.py update             # Re-scan register (idempotent via Neon)
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
logger = logging.getLogger("legal-data-hunter.MX.SecretariaFiscalizacion")

BASE = "https://normasapf.buengobierno.gob.mx"
LISTING_URL = BASE + "/NORMASAPF/SFP.jsf"
DOWNLOAD_FMT = BASE + "/NORMASAPF/Descarga?id={id}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.7",
}

MIN_TEXT_CHARS = 500  # below this we treat the PDF as scanned / no text layer


class SecretariaFiscalizacionScraper(BaseScraper):
    """
    Scraper for MX/SecretariaFiscalizacion.
    Country: MX
    URL: https://normasapf.buengobierno.gob.mx/NORMASAPF/SFP.jsf
    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # ── listing ────────────────────────────────────────────────────
    def _list_norms(self) -> list[dict]:
        """Scrape the SANI register page; return one dict per norm."""
        try:
            r = self.session.get(LISTING_URL, timeout=90)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Register page failed: {e}")
            return []

        soup = BeautifulSoup(r.text, "html.parser")
        items: list[dict] = []
        seen: set[str] = set()

        for tr in soup.find_all("tr"):
            row_html = str(tr)
            m = re.search(r"Descarga\?id=(\d+)", row_html)
            if not m:
                continue
            norm_id = m.group(1)
            if norm_id in seen:
                continue
            seen.add(norm_id)

            cells = [c.get_text(" ", strip=True) for c in tr.find_all("td")]
            # Column layout: 0=id 1=name 2=issuer 3=emission 4=vigente
            #                5=inventory 6=historic 7=button 8=download
            name = cells[1] if len(cells) > 1 else ""
            issuer = cells[2] if len(cells) > 2 else ""
            emission = cells[3] if len(cells) > 3 else ""

            items.append({
                "id": norm_id,
                "name": re.sub(r"\s+", " ", name).strip(),
                "issuer": re.sub(r"\s+", " ", issuer).strip(),
                "emission": emission.strip(),
                "pdf_url": DOWNLOAD_FMT.format(id=norm_id),
            })

        logger.info(f"Collected {len(items)} SANI norms")
        return items

    # ── parsing helpers ────────────────────────────────────────────
    @staticmethod
    def _iso_date(raw: str) -> Optional[str]:
        """SANI dates render as YYYY/MM/DD -> ISO 8601."""
        m = re.match(r"(\d{4})[/-](\d{2})[/-](\d{2})", raw or "")
        if m:
            y, mo, d = (int(x) for x in m.groups())
            if 1900 <= y <= 2100 and 1 <= mo <= 12 and 1 <= d <= 31:
                return f"{y:04d}-{mo:02d}-{d:02d}"
        return None

    @staticmethod
    def _title(name: str, text: str, norm_id: str) -> str:
        if name and len(name) > 5:
            return name[:300]
        # Fall back to the first heading line of the PDF.
        for ln in text[:1500].splitlines():
            ln = re.sub(r"\s+", " ", ln).strip()
            if len(ln) > 10:
                return ln[:300]
        return f"Norma interna SANI {norm_id}"

    # ── schema ─────────────────────────────────────────────────────
    def normalize(self, raw: dict) -> Optional[dict]:
        norm_id = raw["id"]
        pdf_url = raw["pdf_url"]
        doc_id = f"SFP-NORM-{norm_id}"

        text = extract_pdf_markdown(
            source="MX/SecretariaFiscalizacion",
            source_id=doc_id,
            pdf_url=pdf_url,
            table="doctrine",
        )
        if not text or len(text.strip()) < MIN_TEXT_CHARS:
            # Scanned image / no text layer, or already in Neon — skip.
            return None
        text = text.strip()
        time.sleep(1.0)  # politeness between downloads

        date = self._iso_date(raw.get("emission", ""))
        title = self._title(raw.get("name", ""), text, norm_id)

        return {
            "_id": doc_id,
            "_source": "MX/SecretariaFiscalizacion",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "year": int(date[:4]) if date else None,
            "url": pdf_url,
            "source_page": LISTING_URL,
            "issuer": raw.get("issuer") or "Secretaría Anticorrupción y Buen Gobierno (SFP)",
            "jurisdiction": "MX",
            "language": "es",
        }

    # ── fetch ──────────────────────────────────────────────────────
    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW norm metadata; normalize() downloads + extracts full text."""
        for it in self._list_norms():
            yield it

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """No incremental feed; re-scan register (idempotent via Neon)."""
        yield from self.fetch_all()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="MX/SecretariaFiscalizacion fetcher")
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

    scraper = SecretariaFiscalizacionScraper()

    if args.command == "test":
        items = scraper._list_norms()
        logger.info(f"OK: {len(items)} norms listed")
        if items:
            rec = scraper.normalize(items[0])
            if rec:
                logger.info(f"First: {rec['title'][:120]!r} ({len(rec['text'])} chars)")
            else:
                logger.info(f"First norm had no extractable text: id={items[0]['id']}")
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
