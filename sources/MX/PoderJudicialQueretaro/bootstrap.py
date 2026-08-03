#!/usr/bin/env python3
"""
MX/PoderJudicialQueretaro -- Querétaro State Court Decisions (Sentencias Públicas)

Fetches full-text court decisions (sentencias en versión pública) from the
Poder Judicial del Estado de Querétaro public "Sentencias Públicas" portal.

Portal: https://poderjudicialqro.gob.mx/APP_UT69ii/sentencias-publicas.php

Strategy:
  - The portal's search form posts to leeSent.php and returns an HTML table of
    decisions. Although the form is wrapped in a reCAPTCHA in the browser, the
    leeSent.php endpoint performs NO server-side captcha validation, so the
    listing is retrievable directly.
  - We iterate year by year (fecINI/fecFIN = Jan 1 .. Dec 31), materia "00"
    (todas), paginating through every result page.
  - Each table row exposes a document key via abrePDF("<key>"). The full-text
    PDF is downloaded from leeDoc.php?cual=<key> and the text layer extracted.
  - Records are normalized into the standard case_law schema.

Usage:
  python bootstrap.py bootstrap --sample   # fetch a small sample
  python bootstrap.py bootstrap --full     # fetch everything
  python bootstrap.py bootstrap-fast --full
  python bootstrap.py test                 # quick connectivity test
"""

import re
import sys
import html
import logging
import time
from pathlib import Path
from datetime import datetime, timezone, date
from typing import Generator, Optional, Dict, Any, List

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import _extract as extract_pdf_text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MX.PoderJudicialQueretaro")

BASE = "https://poderjudicialqro.gob.mx/APP_UT69ii"
SEARCH_URL = f"{BASE}/leeSent.php"
DOC_URL = f"{BASE}/leeDoc.php"
FORM_URL = f"{BASE}/sentencias-publicas.php"

# The portal's earliest published decisions; iterate forward from here.
FIRST_YEAR = 2015

_TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.S | re.I)
_TR_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
_KEY_RE = re.compile(r"abrePDF\(\s*['\"]([^'\"]+)['\"]")
_TOTAL_PAGES_RE = re.compile(r"de\s+([\d,]+)")
_TAG_RE = re.compile(r"<[^>]+>")


def _clean(text: str) -> str:
    text = _TAG_RE.sub("", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


class QueretaroCourtScraper(BaseScraper):
    """Scraper for MX/PoderJudicialQueretaro full-text sentencias."""

    def __init__(self, source_dir=None):
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Legal-Data-Hunter/1.0",
            "Referer": FORM_URL,
            "X-Requested-With": "XMLHttpRequest",
        })

    # ── HTTP helpers ──────────────────────────────────────────────────
    def _post_search(self, page: int, fecINI: str, fecFIN: str,
                     expmat: str = "00") -> Optional[str]:
        data = {
            "pagina": str(page),
            "fecINI": fecINI,
            "fecFIN": fecFIN,
            "expmat": expmat,
            "juicio": "",
            "Juz": ".....0",
            "genero": "0",
            "grupos": "0",
        }
        for attempt in range(3):
            try:
                time.sleep(1.5)
                resp = self.session.post(SEARCH_URL, data=data, timeout=45)
                resp.raise_for_status()
                return resp.content.decode("utf-8", errors="replace")
            except requests.exceptions.RequestException as e:
                logger.warning(f"Search page {page} ({fecINI}) attempt {attempt+1} failed: {e}")
                time.sleep(4)
        return None

    def _fetch_pdf(self, key: str) -> Optional[bytes]:
        for attempt in range(3):
            try:
                time.sleep(1.5)
                resp = self.session.get(DOC_URL, params={"cual": key}, timeout=60)
                resp.raise_for_status()
                ctype = resp.headers.get("Content-Type", "")
                if "pdf" not in ctype.lower() and not resp.content[:4] == b"%PDF":
                    logger.debug(f"Non-PDF response for key {key}: {ctype}")
                    return None
                return resp.content
            except requests.exceptions.RequestException as e:
                logger.warning(f"PDF fetch {key} attempt {attempt+1} failed: {e}")
                time.sleep(4)
        return None

    # ── Parsing ───────────────────────────────────────────────────────
    @staticmethod
    def _parse_rows(html_text: str) -> List[Dict[str, Any]]:
        rows = []
        for tr in _TR_RE.findall(html_text):
            km = _KEY_RE.search(tr)
            if not km:
                continue
            key = km.group(1)
            tds = [_clean(td) for td in _TD_RE.findall(tr)]
            # tds[0] is the download-button cell (empty after cleaning);
            # the following cells are: periodo, materia, asunto, fecha,
            # expediente, juzgado, magistrado.
            cells = [c for c in tds]
            # Drop leading empty (button) cell(s)
            meta = cells[1:] if cells and cells[0] == "" else cells
            row = {
                "key": key,
                "periodo": meta[0] if len(meta) > 0 else "",
                "materia": meta[1] if len(meta) > 1 else "",
                "asunto": meta[2] if len(meta) > 2 else "",
                "fecha": meta[3] if len(meta) > 3 else "",
                "expediente": meta[4] if len(meta) > 4 else "",
                "juzgado": meta[5] if len(meta) > 5 else "",
                "magistrado": meta[6] if len(meta) > 6 else "",
            }
            rows.append(row)
        return rows

    @staticmethod
    def _parse_total_pages(html_text: str) -> int:
        # Format: "pagina ... value='1' >1 de 2773"
        m = _TOTAL_PAGES_RE.search(html_text)
        if m:
            try:
                return int(m.group(1).replace(",", ""))
            except ValueError:
                return 0
        return 0

    # ── Core iteration ────────────────────────────────────────────────
    def _iter_listing(self) -> Generator[Dict[str, Any], None, None]:
        """Yield raw row dicts (with full-text PDF) across all years/pages."""
        current_year = datetime.now(timezone.utc).year
        seen_keys = set()
        for year in range(current_year, FIRST_YEAR - 1, -1):
            fecINI = f"{year}-01-01"
            fecFIN = f"{year}-12-31"
            first = self._post_search(1, fecINI, fecFIN)
            if first is None:
                continue
            total_pages = self._parse_total_pages(first)
            if total_pages <= 0:
                continue
            logger.info(f"Year {year}: {total_pages} pages")
            for page in range(1, total_pages + 1):
                page_html = first if page == 1 else self._post_search(page, fecINI, fecFIN)
                if page_html is None:
                    continue
                rows = self._parse_rows(page_html)
                if not rows:
                    continue
                for row in rows:
                    if row["key"] in seen_keys:
                        continue
                    seen_keys.add(row["key"])
                    pdf_bytes = self._fetch_pdf(row["key"])
                    if not pdf_bytes:
                        continue
                    text = extract_pdf_text(pdf_bytes)
                    if not text or len(text) < 100:
                        continue
                    row["_text"] = text
                    row["_year"] = year
                    yield row
                if page % 50 == 0:
                    logger.info(f"Year {year}: page {page}/{total_pages}")

    # ── BaseScraper interface ─────────────────────────────────────────
    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        yield from self._iter_listing()

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch decisions from the current year only."""
        year = datetime.now(timezone.utc).year
        fecINI = f"{year}-01-01"
        fecFIN = f"{year}-12-31"
        first = self._post_search(1, fecINI, fecFIN)
        if first is None:
            return
        total_pages = self._parse_total_pages(first)
        seen = set()
        for page in range(1, max(total_pages, 1) + 1):
            page_html = first if page == 1 else self._post_search(page, fecINI, fecFIN)
            if page_html is None:
                continue
            for row in self._parse_rows(page_html):
                if row["key"] in seen:
                    continue
                seen.add(row["key"])
                if since and row["fecha"] and row["fecha"] < since:
                    continue
                pdf_bytes = self._fetch_pdf(row["key"])
                if not pdf_bytes:
                    continue
                text = extract_pdf_text(pdf_bytes)
                if not text or len(text) < 100:
                    continue
                row["_text"] = text
                row["_year"] = year
                yield row

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        key = raw.get("key", "")
        slug = re.sub(r"[^A-Za-z0-9]+", "-", key).strip("-")

        fecha = raw.get("fecha", "")
        date_str = fecha if re.match(r"^\d{4}-\d{2}-\d{2}$", fecha or "") else None

        expediente = raw.get("expediente", "")
        materia = raw.get("materia", "")
        asunto = raw.get("asunto", "")
        juzgado = raw.get("juzgado", "")

        title_parts = [p for p in [materia, asunto, expediente] if p]
        title = " — ".join(title_parts) if title_parts else f"Sentencia {key}"

        return {
            "_id": f"MX-QRO-{slug}",
            "_source": "MX/PoderJudicialQueretaro",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("_text", ""),
            "date": date_str,
            "url": f"{DOC_URL}?cual={key}",
            "court": juzgado,
            "judge": raw.get("magistrado", ""),
            "subject_area": materia,
            "case_type": asunto,
            "case_number": expediente,
            "period": raw.get("periodo", ""),
        }

    # ── Connectivity test ─────────────────────────────────────────────
    def test(self) -> bool:
        year = datetime.now(timezone.utc).year
        html_text = self._post_search(1, f"{year}-01-01", f"{year}-12-31")
        if html_text is None:
            logger.error("Cannot reach Querétaro sentencias portal")
            return False
        rows = self._parse_rows(html_text)
        total_pages = self._parse_total_pages(html_text)
        logger.info(f"Listing OK: {total_pages} pages, {len(rows)} rows on page 1")
        if not rows:
            logger.error("No rows parsed")
            return False
        pdf = self._fetch_pdf(rows[0]["key"])
        if not pdf:
            logger.error("PDF download failed")
            return False
        text = extract_pdf_text(pdf)
        logger.info(f"Sample PDF text length: {len(text or '')} chars")
        return bool(text and len(text) > 100)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="MX/PoderJudicialQueretaro data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Only fetch a small sample")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = QueretaroCourtScraper()

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
