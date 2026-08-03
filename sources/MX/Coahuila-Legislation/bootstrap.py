#!/usr/bin/env python3
"""
MX/Coahuila-Legislation -- Coahuila State Legislation Fetcher

Fetches the consolidated, currently-in-force laws, codes and constitution of
the State of Coahuila de Zaragoza from the Congreso del Estado catalog page
("Leyes Estatales Vigentes"). ~247 documents published as PDFs with full text.

Strategy:
  - Scrape the "Leyes Estatales Vigentes" listing page. Each <tr> holds the
    law title in its first <td> and a PDF link (.../Leyes_Coahuila/coa*.pdf).
  - fetch_all() yields RAW metadata dicts (BaseScraper contract).
  - normalize() downloads the PDF and extracts full text via the shared
    pdfplumber-based extractor, and derives the publication / last-reform date
    from the document header.

Usage:
  python bootstrap.py bootstrap          # Full pull -> data/records.jsonl
  python bootstrap.py bootstrap --sample # 12 sample records -> sample/
  python bootstrap.py bootstrap-fast     # Concurrent full pull (VPS alias)
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import html as htmlmod
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Dict, Any
from urllib.parse import urljoin

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MX.Coahuila-Legislation")

SOURCE_ID = "MX/Coahuila-Legislation"
CATALOG_URL = "https://www.congresocoahuila.gob.mx/portal/leyes-estatales-vigentes/"
BASE_URL = "https://www.congresocoahuila.gob.mx/"

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.5",
}

SPANISH_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5,
    "junio": 6, "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9,
    "octubre": 10, "noviembre": 11, "diciembre": 12,
}


class CoahuilaLegislationScraper(BaseScraper):
    """Scraper for MX/Coahuila-Legislation -- Coahuila state laws and codes."""

    def __init__(self, source_dir: Optional[str] = None):
        super().__init__(source_dir or str(Path(__file__).parent))
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # ── Catalog parsing ────────────────────────────────────────────────

    def _get_law_list(self) -> List[Dict[str, str]]:
        """Scrape the catalog page for law titles and PDF URLs."""
        logger.info(f"Fetching catalog from {CATALOG_URL}")
        resp = self.session.get(CATALOG_URL, timeout=45)
        resp.raise_for_status()
        html = resp.text

        laws: List[Dict[str, str]] = []
        seen: set = set()

        for row in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S | re.I):
            m = re.search(
                r'href="([^"]*Leyes_Coahuila[^"]*\.pdf)"', row, re.I
            )
            if not m:
                continue
            url = urljoin(BASE_URL, m.group(1))
            if url in seen:
                continue

            # Title = first non-empty cell that is not the PDF/WORD label.
            title = ""
            for cell in re.findall(r"<td[^>]*>(.*?)</td>", row, re.S | re.I):
                text = re.sub(r"<[^>]+>", " ", cell)
                text = htmlmod.unescape(re.sub(r"\s+", " ", text)).strip()
                if text and text.upper() not in ("PDF", "WORD", "DOC", "DOCX"):
                    title = text
                    break
            if not title:
                # Fall back to filename-derived title.
                title = url.split("/")[-1].rsplit(".", 1)[0]

            seen.add(url)
            laws.append({"title": title, "url": url})

        logger.info(f"Found {len(laws)} laws in catalog")
        return laws

    # ── Date parsing ───────────────────────────────────────────────────

    @staticmethod
    def _parse_date(text: str) -> str:
        """Extract an ISO date from the PDF header.

        Prefers the 'ULTIMA REFORMA PUBLICADA ... : DD DE MES DE YYYY' line,
        falling back to the original 'Ley publicada ... el DD de mes de YYYY'.
        """
        head = text[:1500]
        patterns = [
            r"ULTIMA REFORMA[^:]*:\s*(\d{1,2})\s+DE\s+([A-Za-zÁÉÍÓÚáéíóú]+)\s+DE\s+(\d{4})",
            r"publicad[ao][^.]*?(\d{1,2})\s+de\s+([A-Za-zÁÉÍÓÚáéíóú]+)\s+de\s+(\d{4})",
        ]
        for pat in patterns:
            m = re.search(pat, head, re.I)
            if not m:
                continue
            day = int(m.group(1))
            month = SPANISH_MONTHS.get(m.group(2).lower())
            year = int(m.group(3))
            if month and 1 <= day <= 31 and 1900 <= year <= 2100:
                return f"{year:04d}-{month:02d}-{day:02d}"
        return ""

    # ── Normalize (downloads + extracts full text) ─────────────────────

    def normalize(self, raw: dict) -> Optional[dict]:
        url = raw["url"]
        slug = url.split("/")[-1].rsplit(".", 1)[0]
        doc_id = f"MX-COA-{slug}"

        # The server 403s the default requests UA, so download the bytes with
        # our browser-like session and hand them to the extractor directly.
        try:
            resp = self.session.get(url, timeout=90)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"PDF download failed for {raw['title'][:50]}: {e}")
            return None

        text = extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=doc_id,
            pdf_bytes=resp.content,
            table="legislation",
        )
        if not text or len(text.strip()) < 100:
            logger.warning(f"Insufficient text for {raw['title'][:50]}: "
                           f"{len(text or '')} chars")
            return None

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": text,
            "date": self._parse_date(text),
            "url": url,
            "jurisdiction": "MX-COA",
        }

    # ── Fetch ──────────────────────────────────────────────────────────

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Yield RAW law metadata dicts. The framework calls normalize()."""
        laws = self._get_law_list()
        if sample:
            step = max(1, len(laws) // 15)
            laws = laws[::step][:15]
        for law in laws:
            yield law

    def fetch_updates(self, since: str = None) -> Generator[dict, None, None]:
        """Small, slowly-changing dataset — always full refresh."""
        yield from self.fetch_all(sample=False)

    # ── Local sample runner ────────────────────────────────────────────

    def run_bootstrap(self, sample: bool = False) -> int:
        sample_dir = self.source_dir / "sample"
        data_dir = self.source_dir / "data"
        sample_dir.mkdir(exist_ok=True)
        data_dir.mkdir(exist_ok=True)

        target = 12 if sample else 100000
        count = 0
        jsonl = None
        if not sample:
            jsonl = open(data_dir / "records.jsonl", "w", encoding="utf-8")

        try:
            for raw in self.fetch_all(sample=sample):
                record = self.normalize(raw)
                if not record or not record.get("text"):
                    continue
                count += 1
                if sample:
                    with open(sample_dir / f"{record['_id']}.json", "w",
                              encoding="utf-8") as f:
                        json.dump(record, f, ensure_ascii=False, indent=2)
                else:
                    jsonl.write(json.dumps(record, ensure_ascii=False) + "\n")
                logger.info(f"[{count}] {record['title'][:55]} | "
                            f"{record['date'] or 'no-date'} | "
                            f"{len(record['text'])} chars")
                if count >= target:
                    break
        finally:
            if jsonl:
                jsonl.close()

        logger.info(f"Bootstrap complete: {count} records")
        return count


def main():
    scraper = CoahuilaLegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        laws = scraper._get_law_list()
        if not laws:
            logger.error("FAILED: no laws found in catalog")
            sys.exit(1)
        logger.info(f"Found {len(laws)} laws. Testing first PDF...")
        rec = scraper.normalize(laws[0])
        if rec and rec.get("text"):
            logger.info(f"SUCCESS: {rec['title'][:55]} | {rec['date']} | "
                        f"{len(rec['text'])} chars")
        else:
            logger.error("FAILED: could not extract text from first PDF")
            sys.exit(1)

    elif command in ("bootstrap", "bootstrap-fast"):
        scraper.run_bootstrap(sample=sample)

    elif command == "update":
        scraper.run_bootstrap(sample=False)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
