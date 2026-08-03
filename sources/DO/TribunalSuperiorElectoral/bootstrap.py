#!/usr/bin/env python3
"""
DO/TribunalSuperiorElectoral -- Dominican Republic Tribunal Superior Electoral

Fetches the full-text "Sentencias Contenciosas" of the Tribunal Superior
Electoral (TSE), the Dominican Republic's highest electoral court (distinct
from the Suprema Corte de Justicia and the Tribunal Constitucional).

Data access (no auth, public open data):
  - The TSE site (tse.do) embeds a standalone PDF viewer at visorpdf.tse.do.
  - The viewer lists, per year, all contentious sentencias paginated by `pos`:
        https://visorpdf.tse.do/?pos={n}&y={year}&s=
    Each list page links to detail pages:
        https://visorpdf.tse.do/documento/contenciosas/{token}
  - Each detail page exposes the sentencia number, expediente, síntesis,
    publication date, and an <iframe> pointing at the actual PDF:
        https://visorpdf.tse.do/file-upload/{ts}.pdf
  - PDFs are machine-readable (not scanned); full text is extracted with the
    shared OOM-hardened common.pdf_extract backend.

NOTE: visorpdf.tse.do / tse.do return HTTP 403 to non-browser User-Agents; a
desktop browser UA is required (set below). Coverage: 2012–present.

Usage:
  python bootstrap.py bootstrap --sample     # 12+ sample records
  python bootstrap.py bootstrap-fast --full  # full pull -> data/records.jsonl
  python bootstrap.py test                   # connectivity / parse check
"""

import sys
import re
import html
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.DO.TribunalSuperiorElectoral")

VISOR_BASE = "https://visorpdf.tse.do"
SOURCE_ID = "DO/TribunalSuperiorElectoral"

# visorpdf.tse.do 403s non-browser UAs.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-DO,es;q=0.9,en;q=0.8",
}

# Contentious sentencias exist from 2012 onward (see tse.do category posts).
FIRST_YEAR = 2012

_TOKEN_RE = re.compile(r"documento/contenciosas/([A-Za-z0-9]+)")
_H4_RE = re.compile(r'<p class="h4 fw-bold">(.*?)</p>', re.S)
_SINTESIS_RE = re.compile(r'Síntesis:.*?<p class="text-start">(.*?)</p>', re.S)
_FECHA_RE = re.compile(r"Fecha de publicación:.*?<br>(.*?)</small>", re.S)
_PDF_RE = re.compile(r'iframe src="([^"]+\.pdf)"')

_ES_MONTHS = {
    "ene": "01", "feb": "02", "mar": "03", "abr": "04", "may": "05",
    "jun": "06", "jul": "07", "ago": "08", "sep": "09", "set": "09",
    "oct": "10", "nov": "11", "dic": "12",
}


def _strip_tags(s: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", " ", s)).strip()


def _parse_spanish_date(raw: str) -> Optional[str]:
    """'Jue 03 Oct 2024' / '03 Oct 2024' -> '2024-10-03'."""
    if not raw:
        return None
    m = re.search(r"(\d{1,2})\s+([A-Za-zÁÉÍÓÚáéíóú]{3,})\.?\s+(\d{4})", raw)
    if not m:
        return None
    day, mon, year = m.group(1), m.group(2)[:3].lower(), m.group(3)
    month = _ES_MONTHS.get(mon)
    if not month:
        return None
    return f"{year}-{month}-{int(day):02d}"


class TribunalSuperiorElectoralScraper(BaseScraper):
    """
    Scraper for DO/TribunalSuperiorElectoral — TSE contentious sentencias.
    Country: DO | Data type: case_law | Auth: none
    """

    def __init__(self, source_dir: Optional[str] = None):
        super().__init__(source_dir)
        self.client = HttpClient(base_url=VISOR_BASE, headers=HEADERS, timeout=60)

    # -- discovery ----------------------------------------------------------

    def _list_year_tokens(self, year: int) -> list:
        """Return ordered unique documento tokens for a given year."""
        tokens: list = []
        seen = set()
        pos = 1
        empty_streak = 0
        while pos <= 60:  # hard ceiling; years have <=8 pages
            self.rate_limiter.wait()
            try:
                resp = self.client.get(f"/?pos={pos}&y={year}&s=")
                if resp.status_code != 200:
                    break
                page_tokens = _TOKEN_RE.findall(resp.text)
            except Exception as e:
                logger.debug(f"List {year} pos {pos} failed: {e}")
                break

            new_on_page = [t for t in page_tokens if t not in seen]
            if not new_on_page:
                empty_streak += 1
                if empty_streak >= 2:
                    break
            else:
                empty_streak = 0
                for t in new_on_page:
                    seen.add(t)
                    tokens.append(t)
            pos += 1
        return tokens

    def _fetch_detail(self, token: str, year: int) -> Optional[dict]:
        """Fetch a documento detail page; return raw metadata + pdf url."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(f"/documento/contenciosas/{token}")
            if resp.status_code != 200:
                return None
            h = resp.text
        except Exception as e:
            logger.debug(f"Detail {token} failed: {e}")
            return None

        h4 = _H4_RE.findall(h)
        pdf_m = _PDF_RE.search(h)
        if not pdf_m:
            return None
        pdf_url = pdf_m.group(1)
        if pdf_url.startswith("/"):
            pdf_url = VISOR_BASE + pdf_url

        sint_m = _SINTESIS_RE.search(h)
        fecha_m = _FECHA_RE.search(h)

        return {
            "token": token,
            "year": year,
            "sentencia": _strip_tags(h4[0]) if h4 else "",
            "expediente": _strip_tags(h4[1]) if len(h4) > 1 else "",
            "sintesis": _strip_tags(sint_m.group(1)) if sint_m else "",
            "fecha_raw": _strip_tags(fecha_m.group(1)) if fecha_m else "",
            "pdf_url": pdf_url,
        }

    # -- BaseScraper contract ----------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW detail dicts (framework calls normalize())."""
        current_year = datetime.now(timezone.utc).year
        for year in range(FIRST_YEAR, current_year + 1):
            tokens = self._list_year_tokens(year)
            if not tokens:
                continue
            logger.info(f"Year {year}: {len(tokens)} sentencias")
            for token in tokens:
                detail = self._fetch_detail(token, year)
                if not detail:
                    continue

                # Stable id: prefer expediente, fall back to token.
                doc_key = detail["expediente"] or detail["token"]
                text = extract_pdf_markdown(
                    SOURCE_ID, doc_key, pdf_url=detail["pdf_url"], table="case_law"
                )
                if not text or len(text) < 200:
                    continue
                detail["_text"] = text
                yield detail

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a raw detail dict into the standard schema."""
        if not raw.get("_text"):
            return None

        sentencia = raw.get("sentencia", "").strip()
        expediente = raw.get("expediente", "").strip()
        doc_key = expediente or raw.get("token", "")
        date_iso = _parse_spanish_date(raw.get("fecha_raw", ""))

        title_parts = []
        if sentencia:
            title_parts.append(f"Sentencia {sentencia}")
        if expediente:
            title_parts.append(f"Exp. {expediente}")
        title = " — ".join(title_parts) or f"TSE {doc_key}"

        return {
            "_id": f"DO-TSE-{doc_key}".replace("/", "-").replace(" ", ""),
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "sentencia": sentencia,
            "expediente": expediente,
            "tribunal": "Tribunal Superior Electoral",
            "sintesis": raw.get("sintesis", ""),
            "text": raw["_text"],
            "date": date_iso,
            "year": raw.get("year"),
            "url": raw.get("pdf_url", ""),
        }

    def fetch_updates(self, since: str = None) -> Generator[dict, None, None]:
        """Recent sentencias: scan the current and previous year only."""
        current_year = datetime.now(timezone.utc).year
        for year in (current_year, current_year - 1):
            tokens = self._list_year_tokens(year)
            for token in tokens:
                detail = self._fetch_detail(token, year)
                if not detail:
                    continue
                doc_key = detail["expediente"] or detail["token"]
                text = extract_pdf_markdown(
                    SOURCE_ID, doc_key, pdf_url=detail["pdf_url"], table="case_law"
                )
                if not text or len(text) < 200:
                    continue
                detail["_text"] = text
                yield detail


def main():
    import argparse

    parser = argparse.ArgumentParser(description="DO/TribunalSuperiorElectoral scraper")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "update", "test"]
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (12 records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap")
    args = parser.parse_args()

    scraper = TribunalSuperiorElectoralScraper(str(Path(__file__).resolve().parent))

    if args.command == "test":
        year = datetime.now(timezone.utc).year
        tokens = scraper._list_year_tokens(year)
        logger.info(f"Year {year}: {len(tokens)} tokens")
        if not tokens:
            tokens = scraper._list_year_tokens(year - 1)
            logger.info(f"Year {year-1}: {len(tokens)} tokens")
        if tokens:
            d = scraper._fetch_detail(tokens[0], year)
            logger.info(f"Detail: {json.dumps({k: v for k, v in d.items()}, ensure_ascii=False)[:300]}")
            text = extract_pdf_markdown(
                SOURCE_ID, d["expediente"] or d["token"], pdf_url=d["pdf_url"]
            )
            logger.info(f"PDF text: {len(text or '')} chars — {(text or '')[:200]}")
        logger.info("Test complete.")
        return

    if args.command in ("bootstrap", "bootstrap-fast"):
        sample = args.sample and not args.full
        result = scraper.bootstrap(sample_mode=sample, sample_size=12)
        logger.info(f"Bootstrap result: {json.dumps(result, indent=2, default=str)}")
    elif args.command == "update":
        count = 0
        sample_dir = scraper.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)
        for raw in scraper.fetch_updates():
            rec = scraper.normalize(raw)
            if not rec:
                continue
            count += 1
            with open(sample_dir / f"{rec['_id']}.json", "w", encoding="utf-8") as f:
                json.dump(rec, f, ensure_ascii=False, indent=2)
            if count >= 50:
                break
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    main()
