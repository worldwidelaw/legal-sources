#!/usr/bin/env python3
"""
MX/PoderJudicialTlaxcala -- Tlaxcala State Court Decisions (Versiones Públicas)

Fetches full-text court decisions (sentencias en versión pública) from the
Poder Judicial del Estado de Tlaxcala "Sentencias (Publicación en versión
pública)" portal.

Portal:  https://tsjtlaxcala.gob.mx/transparencia/sps/sps1.html
Listing: https://tsjtlaxcala.gob.mx/transparencia/sps/tabla1.html
         (a single static HTML table of ALL ~6,560 public sentencias, each row
          carries expediente, tipo de juicio, año, juzgado and a direct link to
          the full-text PDF under sentencias/<file>.pdf).

Strategy:
  - GET tabla1.html once -> parse every <tr>: columns are
    [expediente, tipo, año, juzgado, <a href="sentencias/FILE.pdf">].
  - GET sentencias/<file>.pdf -> extract the full text layer.

Usage:
  python bootstrap.py test
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap-fast --full
"""

import sys
import re
import html
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import quote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import _extract as extract_pdf_text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MX.PoderJudicialTlaxcala")

BASE = "https://tsjtlaxcala.gob.mx/transparencia/sps"
LIST_URL = f"{BASE}/tabla1.html"
PORTAL = f"{BASE}/sps1.html"

_ROW_RE = re.compile(r"<tr>(.*?)</tr>", re.S | re.I)
_TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.S | re.I)
_HREF_RE = re.compile(r'href="(sentencias/[^"]+\.pdf)"', re.I)
_TAG_RE = re.compile(r"<[^>]+>")


class TlaxcalaCourtScraper(BaseScraper):
    """Scraper for MX/PoderJudicialTlaxcala full-text sentencias (versiones públicas)."""

    def __init__(self, source_dir=None):
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Legal-Data-Hunter/1.0",
            "Referer": PORTAL,
        })

    # ── helpers ───────────────────────────────────────────────────────
    @staticmethod
    def _clean(cell: str) -> str:
        return html.unescape(_TAG_RE.sub("", cell)).strip()

    def _list_rows(self) -> List[Dict[str, Any]]:
        for attempt in range(3):
            try:
                time.sleep(0.5)
                resp = self.session.get(LIST_URL, timeout=120)
                resp.raise_for_status()
                resp.encoding = resp.apparent_encoding or "utf-8"
                htmltext = resp.text
                break
            except requests.exceptions.RequestException as e:
                logger.warning(f"GET listing attempt {attempt+1} failed: {e}")
                time.sleep(4)
        else:
            logger.error("Could not fetch listing")
            return []

        rows: List[Dict[str, Any]] = []
        seen = set()
        for rawrow in _ROW_RE.findall(htmltext):
            href = _HREF_RE.search(rawrow)
            if not href:
                continue
            cells = _TD_RE.findall(rawrow)
            if len(cells) < 4:
                continue
            expediente = self._clean(cells[0])
            tipo = self._clean(cells[1])
            anio = self._clean(cells[2])
            juzgado = self._clean(cells[3])
            relpath = html.unescape(href.group(1))
            if relpath in seen:
                continue
            seen.add(relpath)
            rows.append({
                "expediente": expediente,
                "tipo": tipo,
                "anio": anio,
                "juzgado": juzgado,
                "relpath": relpath,
            })
        logger.info(f"Listing: {len(rows)} sentencias")
        return rows

    def _pdf_url(self, relpath: str) -> str:
        # Encode the filename (it may contain accents / spaces) but keep the path.
        prefix, _, fname = relpath.rpartition("/")
        return f"{BASE}/{prefix}/{quote(fname)}"

    def _fetch_pdf(self, relpath: str) -> Optional[bytes]:
        url = self._pdf_url(relpath)
        for attempt in range(3):
            try:
                time.sleep(0.5)
                resp = self.session.get(url, timeout=90)
                if 400 <= resp.status_code < 500:
                    return None
                resp.raise_for_status()
                if resp.content[:4] != b"%PDF":
                    return None
                return resp.content
            except requests.exceptions.RequestException as e:
                logger.warning(f"PDF {relpath} attempt {attempt+1} failed: {e}")
                time.sleep(4)
        return None

    def _iter_rows(self) -> Generator[Dict[str, Any], None, None]:
        rows = self._list_rows()
        for row in rows:
            pdf = self._fetch_pdf(row["relpath"])
            if not pdf:
                continue
            text = extract_pdf_text(pdf)
            if not text or len(text) < 100:
                continue
            row["_text"] = text
            yield row

    # ── BaseScraper interface ─────────────────────────────────────────
    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        yield from self._iter_rows()

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        # No reliable per-record timestamp; re-scan everything.
        yield from self._iter_rows()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        relpath = raw.get("relpath", "")
        fname = relpath.rpartition("/")[2].rsplit(".", 1)[0]
        expediente = (raw.get("expediente") or "").strip()
        tipo = (raw.get("tipo") or "").strip()
        anio = (raw.get("anio") or "").strip()
        juzgado = re.sub(r"\s+", " ", (raw.get("juzgado") or "").strip())

        slug = re.sub(r"[^A-Za-z0-9]+", "-", fname).strip("-") or \
            re.sub(r"[^A-Za-z0-9]+", "-", expediente)
        doc_id = f"MX-TLA-{slug}"

        date_str = None
        m = re.match(r"^(19|20)\d{2}$", anio)
        if m:
            date_str = f"{anio}-01-01"

        title_parts = [p for p in [tipo, expediente] if p]
        title = " — ".join(title_parts) if title_parts else f"Sentencia {fname}"

        return {
            "_id": doc_id,
            "_source": "MX/PoderJudicialTlaxcala",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("_text", ""),
            "date": date_str,
            "url": self._pdf_url(relpath),
            "court": juzgado,
            "subject_area": tipo,
            "case_number": expediente,
            "year": anio,
            "jurisdiction": "MX-TLA",
        }

    # ── Connectivity test ─────────────────────────────────────────────
    def test(self) -> bool:
        rows = self._list_rows()
        if not rows:
            logger.error("Listing returned no rows")
            return False
        for row in rows[:8]:
            pdf = self._fetch_pdf(row["relpath"])
            if not pdf:
                continue
            text = extract_pdf_text(pdf)
            logger.info(f"Sample {row['relpath']}: {len(text or '')} chars")
            if text and len(text) > 100:
                return True
        logger.error("No sample PDF with extractable text found")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="MX/PoderJudicialTlaxcala data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Only fetch a small sample")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = TlaxcalaCourtScraper()

    if args.command == "test":
        sys.exit(0 if scraper.test() else 1)
    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"Bootstrap-fast complete: {stats}")
        if stats.get("records_new", 0) == 0 and stats.get("records_fetched", 0) == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
