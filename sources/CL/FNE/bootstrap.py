#!/usr/bin/env python3
"""
CL/FNE -- Fiscalía Nacional Económica (Chilean Competition Authority)

Fetches competition case law: TDLC decisions, Supreme Court competition
sentencias, and historical Antimonopoly Commission decisions.

Strategy:
  - PHP search endpoints return all records in one HTML table when palabra= (empty)
  - Parse HTML tables for metadata + PDF URLs
  - Download PDFs and extract full text using common/pdf_extract

Data:
  - ~238 TDLC decisions (sentencias, resoluciones, informes) 2004-present
  - ~105 Corte Suprema competition sentencias
  - ~2038 historical Antimonopoly Commission decisions (1974-2004)
  - Language: Spanish

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Fetch recent records
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
from typing import Generator, Optional, Dict, Any, List
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
logger = logging.getLogger("legal-data-hunter.CL.FNE")

BASE_URL = "https://www.fne.gob.cl"
USER_AGENT = "LegalDataHunter/1.0 (open-data research; https://github.com/worldwidelaw/legal-sources)"

# PHP search endpoints — passing palabra= (empty) returns all records
ENDPOINTS = {
    "tdlc": {
        "url": f"{BASE_URL}/search/decisiones_resultados_single.php?palabra=",
        "label": "Decisiones TDLC",
        "data_type": "case_law",
    },
    "corte_suprema": {
        "url": f"{BASE_URL}/search/sentencias_resultados_single.php?palabra=",
        "label": "Sentencias Corte Suprema",
        "data_type": "case_law",
    },
    "comisiones": {
        "url": f"{BASE_URL}/search/deccomisiones_resultados_single.php?palabra=",
        "label": "Decisiones Comisiones Antimonopolio",
        "data_type": "case_law",
    },
}


def _get(url: str, timeout: int = 120) -> str:
    """GET a URL and return decoded text."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    resp = urlopen(req, timeout=timeout)
    raw = resp.read()
    # Try utf-8 first, fall back to latin-1
    for enc in ("utf-8", "latin-1"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def _get_bytes(url: str, timeout: int = 120) -> bytes:
    """GET a URL and return raw bytes."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    resp = urlopen(req, timeout=timeout)
    return resp.read()


def _clean_html(text: str) -> str:
    """Strip HTML tags and decode entities."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


SPANISH_MONTHS = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
    "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
    "ene": "01", "feb": "02", "mar": "03", "abr": "04",
    "may": "05", "jun": "06", "jul": "07", "ago": "08",
    "sep": "09", "oct": "10", "nov": "11", "dic": "12",
}


def _parse_date(date_str: str) -> Optional[str]:
    """Parse various date formats to ISO format."""
    if not date_str:
        return None
    date_str = date_str.strip()

    # DD/MM/YYYY or DD-MM-YYYY
    m = re.match(r"(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{4})", date_str)
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"

    # "DD de Month de YYYY"
    m = re.match(r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})", date_str, re.IGNORECASE)
    if m:
        month = SPANISH_MONTHS.get(m.group(2).lower())
        if month:
            return f"{m.group(3)}-{month}-{m.group(1).zfill(2)}"

    # YYYY-MM-DD already
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", date_str)
    if m:
        return date_str[:10]

    return None


def _parse_search_table(html: str, endpoint_key: str) -> List[Dict[str, Any]]:
    """Parse an FNE search results HTML table into records."""
    results = []

    # Extract table rows
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL | re.IGNORECASE)

    for row in rows:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL | re.IGNORECASE)
        if len(cells) < 3:
            continue

        # Extract PDF link from the row
        pdf_match = re.search(
            r'href=["\']?(https?://[^\s"\'<>]+\.pdf[^\s"\'<>]*|/[^\s"\'<>]+\.pdf[^\s"\'<>]*)',
            row, re.IGNORECASE
        )
        if not pdf_match:
            # Also check for wp-content/uploads links without .pdf extension
            pdf_match = re.search(
                r'href=["\']?(https?://[^\s"\'<>]*wp-content/uploads/[^\s"\'<>]+)',
                row, re.IGNORECASE
            )
        if not pdf_match:
            continue

        pdf_url = pdf_match.group(1)
        if pdf_url.startswith("/"):
            pdf_url = BASE_URL + pdf_url

        # First meaningful cell with text is usually the title (may contain link)
        title = ""
        for cell in cells:
            cleaned = _clean_html(cell)
            if len(cleaned) > 5 and not cleaned.isdigit():
                title = cleaned
                break

        if not title:
            continue

        # Extract date — usually in last cell
        date_str = None
        for cell in reversed(cells):
            cleaned = _clean_html(cell)
            parsed = _parse_date(cleaned)
            if parsed:
                date_str = parsed
                break

        # Extract document number/rol
        doc_number = ""
        conducta = ""

        if endpoint_key == "tdlc":
            # TDLC: cells typically = [sort, titulo, conducta-subconducta, mercado, numero, rol, fecha]
            if len(cells) >= 5:
                doc_number = _clean_html(cells[-3]) if len(cells) >= 6 else ""
                conducta = _clean_html(cells[2]) if len(cells) >= 3 else ""
                # Try to get Rol TDLC
                rol = _clean_html(cells[-2]) if len(cells) >= 6 else ""
                if rol and not _parse_date(rol):
                    doc_number = rol if not doc_number else doc_number

        elif endpoint_key == "corte_suprema":
            # CS: cells = [sort, titulo, conducta, mercado, fecha]
            if len(cells) >= 3:
                conducta = _clean_html(cells[2]) if len(cells) >= 3 else ""

        elif endpoint_key == "comisiones":
            # Comisiones: cells = [sort, titulo, conducta, mercado, doc_rel, numero, fecha]
            if len(cells) >= 5:
                doc_number = _clean_html(cells[-2]) if len(cells) >= 6 else ""
                conducta = _clean_html(cells[2]) if len(cells) >= 3 else ""

        # Build unique ID from PDF filename
        pdf_basename = pdf_url.rsplit("/", 1)[-1].split("?")[0]
        doc_id = re.sub(r"\.pdf$", "", pdf_basename, flags=re.IGNORECASE)
        doc_id = re.sub(r"[^a-zA-Z0-9_-]", "_", doc_id)

        if not doc_id:
            continue

        results.append({
            "doc_id": f"{endpoint_key}-{doc_id}",
            "title": title,
            "date": date_str,
            "pdf_url": pdf_url,
            "doc_number": doc_number,
            "conducta": conducta,
            "section": endpoint_key,
        })

    return results


class FNEScraper(BaseScraper):
    SOURCE_ID = "CL/FNE"

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        """Yield all competition case law documents."""
        seen = set()
        count = 0

        for key, ep in ENDPOINTS.items():
            logger.info(f"Fetching {ep['label']} from {ep['url']}")
            try:
                html = _get(ep["url"])
                records = _parse_search_table(html, key)
                logger.info(f"  Found {len(records)} records in {ep['label']}")
            except Exception as e:
                logger.error(f"  Error fetching {ep['label']}: {e}")
                continue

            for doc in records:
                if max_records and count >= max_records:
                    return
                if doc["doc_id"] in seen:
                    continue
                seen.add(doc["doc_id"])

                record = self._process_document(doc)
                if record:
                    yield record
                    count += 1

            time.sleep(2)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent documents (TDLC and Corte Suprema only)."""
        seen = set()

        for key in ("tdlc", "corte_suprema"):
            ep = ENDPOINTS[key]
            try:
                html = _get(ep["url"])
                records = _parse_search_table(html, key)
            except Exception as e:
                logger.error(f"Error fetching {ep['label']}: {e}")
                continue

            for doc in records:
                if doc["doc_id"] in seen:
                    continue
                seen.add(doc["doc_id"])
                if since and doc.get("date") and doc["date"] < since:
                    continue
                record = self._process_document(doc)
                if record:
                    yield record

    def _process_document(self, doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
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
            table="case_law",
        )

        if not text or len(text) < 50:
            logger.warning(f"No/insufficient text extracted for {doc_id} ({len(text) if text else 0} chars)")
            return None

        return self.normalize({**doc, "text": text})

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        section_labels = {
            "tdlc": "TDLC",
            "corte_suprema": "Corte Suprema",
            "comisiones": "Comisiones Antimonopolio",
        }
        return {
            "_id": raw["doc_id"],
            "_source": self.SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw.get("pdf_url", ""),
            "doc_number": raw.get("doc_number", ""),
            "conducta": raw.get("conducta", ""),
            "section": section_labels.get(raw.get("section", ""), raw.get("section", "")),
        }


# ── CLI entry point ─────────────────────────────────────────────
def main():
    import argparse

    parser = argparse.ArgumentParser(description="CL/FNE data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    args = parser.parse_args()

    source_dir = Path(__file__).resolve().parent
    scraper = FNEScraper(str(source_dir))

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            for key, ep in ENDPOINTS.items():
                html = _get(ep["url"], timeout=30)
                records = _parse_search_table(html, key)
                logger.info(f"  {ep['label']}: {len(records)} records")
            logger.info("Connectivity OK")
        except Exception as e:
            logger.error(f"Test failed: {e}")
            sys.exit(1)
        return

    sample_dir = source_dir / "sample"
    sample_dir.mkdir(exist_ok=True)
    count = 0
    limit = 15 if args.sample else 999999

    gen = scraper.fetch_all(max_records=limit) if args.command == "bootstrap" else scraper.fetch_updates()

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
