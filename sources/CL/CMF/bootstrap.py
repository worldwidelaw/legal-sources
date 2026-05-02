#!/usr/bin/env python3
"""
CL/CMF -- Chilean Financial Market Commission (Comisión para el Mercado Financiero)

Fetches regulations (NCG, Circulares, Oficios Circulares) and sanction resolutions.

Strategy:
  - Regulations: search normativa2.php by year/type, parse HTML table, download PDFs
  - Sanctions: scrape sanciones_mercados_entidad.php table, download PDFs
  - Extract full text from PDFs using common/pdf_extract

Data:
  - ~600 normativa documents (1981-present): NCGs, Circulares, Oficios Circulares
  - ~1,450 sanction resolutions (2002-present)
  - Language: Spanish

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Fetch current year only
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CL.CMF")

BASE_URL = "https://www.cmfchile.cl"
NORMATIVA_URL = f"{BASE_URL}/institucional/legislacion_normativa/normativa2.php"
SANCTIONS_URL = f"{BASE_URL}/institucional/sanciones/sanciones_mercados_entidad.php"
USER_AGENT = "LegalDataHunter/1.0 (open-data research; https://github.com/worldwidelaw/legal-sources)"

NORM_TYPES = ["NCG", "CIR", "OFC"]
START_YEAR = 1981
CURRENT_YEAR = datetime.now().year


def _get(url: str, timeout: int = 60) -> str:
    """GET a URL and return decoded text."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    resp = urlopen(req, timeout=timeout)
    return resp.read().decode("utf-8", errors="replace")


def _get_bytes(url: str, timeout: int = 120) -> bytes:
    """GET a URL and return raw bytes."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    resp = urlopen(req, timeout=timeout)
    return resp.read()


def _clean_html(text: str) -> str:
    """Strip HTML tags and decode entities."""
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    return text.strip()


def _parse_date(date_str: str) -> Optional[str]:
    """Parse DD/MM/YYYY or DD.MM.YYYY to ISO format."""
    date_str = date_str.strip().replace(".", "/")
    for fmt in ("%d/%m/%Y",):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _parse_normativa_table(html: str) -> List[Dict[str, Any]]:
    """Parse the normativa results table from normativa2.php."""
    results = []

    # Find the data table (second table, contains ver_sgd links)
    table_match = re.search(
        r'<table[^>]*>\s*<tr[^>]*>\s*<th[^>]*>Tipo de Norma',
        html, re.DOTALL
    )
    if not table_match:
        return results

    table_start = table_match.start()
    table_end = html.find("</table>", table_start)
    if table_end == -1:
        return results

    table_html = html[table_start:table_end]

    # Extract rows — each normativa entry starts with a row containing the norm type
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, re.DOTALL)

    for row in rows:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
        if len(cells) < 4:
            continue

        # First cells: tipo, numero, fecha, titulo
        tipo = _clean_html(cells[0])
        if tipo not in NORM_TYPES:
            continue

        numero = _clean_html(cells[1])
        fecha = _clean_html(cells[2])
        titulo = _clean_html(cells[3])

        if not numero or not fecha:
            continue

        # Extract PDF link (href may be unquoted, single-quoted, or double-quoted)
        links = re.findall(r'href=["\']?([^\s"\'<>]*ver_sgd[^\s"\'<>]*)', row)
        if not links:
            continue

        pdf_path = links[0]
        if pdf_path.startswith("/"):
            pdf_url = BASE_URL + pdf_path
        else:
            pdf_url = pdf_path

        iso_date = _parse_date(fecha)

        results.append({
            "doc_type": tipo,
            "number": numero,
            "date": iso_date or fecha,
            "title": titulo,
            "pdf_url": pdf_url,
            "doc_id": f"{tipo}-{numero}",
        })

    return results


def _parse_sanctions_table(html: str) -> List[Dict[str, Any]]:
    """Parse the sanctions results table."""
    results = []

    # Find table rows with resolution data
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL)

    for row in rows:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
        if len(cells) < 4:
            continue

        num = _clean_html(cells[0])
        fecha = _clean_html(cells[1])
        materia = _clean_html(cells[2])

        # Sanction numbers are numeric
        if not num or not num.strip().isdigit():
            continue

        links = re.findall(r'href=["\']?([^\s"\'<>]*ver_sgd[^\s"\'<>]*)', row)
        if not links:
            continue

        pdf_path = links[0]
        if pdf_path.startswith("/"):
            pdf_url = BASE_URL + pdf_path
        else:
            pdf_url = pdf_path

        iso_date = _parse_date(fecha)

        results.append({
            "doc_type": "SANCION",
            "number": num.strip(),
            "date": iso_date or fecha,
            "title": materia,
            "pdf_url": pdf_url,
            "doc_id": f"SANCION-{num.strip()}",
        })

    return results


def _fetch_normativa_by_year(year: int, tipo: str = "TODOS") -> List[Dict[str, Any]]:
    """Fetch normativa for a given year."""
    url = (
        f"{NORMATIVA_URL}?tiponorma={tipo}"
        f"&dd=01&mm=01&aa={year}"
        f"&dd2=31&mm2=12&aa2={year}"
        f"&enviado=1"
    )
    logger.info(f"Fetching normativa for {year} (type={tipo})")
    html = _get(url)
    return _parse_normativa_table(html)


class CMFScraper(BaseScraper):
    SOURCE_ID = "CL/CMF"

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all normativa + sanction documents."""
        seen = set()

        # 1. Fetch all normativa by year
        for year in range(START_YEAR, CURRENT_YEAR + 1):
            try:
                norms = _fetch_normativa_by_year(year)
                logger.info(f"Year {year}: {len(norms)} normativa docs")
                for doc in norms:
                    if doc["doc_id"] in seen:
                        continue
                    seen.add(doc["doc_id"])
                    record = self._process_document(doc, "legislation")
                    if record:
                        yield record
                time.sleep(1)
            except Exception as e:
                logger.warning(f"Error fetching normativa for {year}: {e}")
                continue

        # 2. Fetch all sanctions
        try:
            logger.info("Fetching sanctions listing")
            html = _get(SANCTIONS_URL)
            sanctions = _parse_sanctions_table(html)
            logger.info(f"Found {len(sanctions)} sanction resolutions")
            for doc in sanctions:
                if doc["doc_id"] in seen:
                    continue
                seen.add(doc["doc_id"])
                record = self._process_document(doc, "case_law")
                if record:
                    yield record
        except Exception as e:
            logger.error(f"Error fetching sanctions: {e}")

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch documents from current year only."""
        seen = set()

        norms = _fetch_normativa_by_year(CURRENT_YEAR)
        for doc in norms:
            if doc["doc_id"] in seen:
                continue
            seen.add(doc["doc_id"])
            record = self._process_document(doc, "legislation")
            if record:
                yield record

        try:
            html = _get(SANCTIONS_URL)
            sanctions = _parse_sanctions_table(html)
            for doc in sanctions:
                if doc["doc_id"] in seen:
                    continue
                seen.add(doc["doc_id"])
                if since and doc.get("date", "") < since:
                    continue
                record = self._process_document(doc, "case_law")
                if record:
                    yield record
        except Exception as e:
            logger.error(f"Error fetching sanctions: {e}")

    def _process_document(self, doc: Dict[str, Any], data_type: str) -> Optional[Dict[str, Any]]:
        """Download PDF and build normalized record."""
        doc_id = doc["doc_id"]
        pdf_url = doc["pdf_url"]

        logger.info(f"Processing {doc_id}: {doc['title'][:60]}")
        time.sleep(1)

        try:
            pdf_data = _get_bytes(pdf_url)
        except Exception as e:
            logger.warning(f"PDF download failed for {doc_id}: {e}")
            return None

        text = extract_pdf_markdown(
            source=self.SOURCE_ID,
            source_id=doc_id,
            pdf_bytes=pdf_data,
            table=data_type,
        )

        if not text:
            logger.warning(f"No text extracted for {doc_id}")
            return None

        return self.normalize({**doc, "text": text, "data_type": data_type})

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw["doc_id"],
            "_source": self.SOURCE_ID,
            "_type": raw.get("data_type", "legislation"),
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw.get("pdf_url", ""),
            "doc_type": raw["doc_type"],
            "number": raw.get("number", ""),
        }


# ── CLI entry point ─────────────────────────────────────────────
def main():
    import argparse

    parser = argparse.ArgumentParser(description="CL/CMF data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 10+ sample records")
    args = parser.parse_args()

    source_dir = Path(__file__).resolve().parent
    scraper = CMFScraper(str(source_dir))

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            norms = _fetch_normativa_by_year(CURRENT_YEAR)
            logger.info(f"Normativa test: {len(norms)} docs for {CURRENT_YEAR}")
            html = _get(SANCTIONS_URL)
            sanctions = _parse_sanctions_table(html)
            logger.info(f"Sanctions test: {len(sanctions)} total resolutions")
            logger.info("Connectivity OK")
        except Exception as e:
            logger.error(f"Test failed: {e}")
            sys.exit(1)
        return

    sample_dir = source_dir / "sample"
    sample_dir.mkdir(exist_ok=True)
    count = 0
    limit = 15 if args.sample else 999999

    gen = scraper.fetch_all() if args.command == "bootstrap" else scraper.fetch_updates()

    for record in gen:
        if count >= limit:
            break
        text_len = len(record.get("text", ""))
        if text_len < 100:
            logger.warning(f"Skipping {record['_id']}: text too short ({text_len} chars)")
            continue

        fname = re.sub(r"[^a-zA-Z0-9_-]", "_", record["_id"]) + ".json"
        with open(sample_dir / fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info(f"[{count}/{limit}] Saved {record['_id']} ({text_len} chars)")

    logger.info(f"Done: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
