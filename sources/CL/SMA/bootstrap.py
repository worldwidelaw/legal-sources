#!/usr/bin/env python3
"""
CL/SMA -- Superintendencia del Medio Ambiente (Environmental Enforcement)

Fetches sanction procedure resolutions from SNIFA (Sistema Nacional de
Información de Fiscalización Ambiental).

Strategy:
  - Iterate through Sancionatorio Ficha pages by ID (1..~4500)
  - Parse HTML for case metadata (expedition, entity, unit, infractions)
  - Find the resolution sancionatoria PDF link (/General/Descargar/{id})
  - Download PDF and extract full text via common/pdf_extract
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
logger = logging.getLogger("legal-data-hunter.CL.SMA")

BASE_URL = "https://snifa.sma.gob.cl"
FICHA_URL = BASE_URL + "/Sancionatorio/Ficha/{id}"
DOWNLOAD_URL = BASE_URL + "/General/Descargar/{doc_id}"
USER_AGENT = "LegalDataHunter/1.0 (open-data research; https://github.com/worldwidelaw/legal-sources)"

# Max Ficha ID to probe (as of 2026, ~4500 exist)
MAX_FICHA_ID = 5000
# Consecutive 500 errors before stopping
MAX_CONSECUTIVE_ERRORS = 50


def _get(url: str, timeout: int = 60) -> str:
    """GET a URL and return decoded text."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    resp = urlopen(req, timeout=timeout)
    raw = resp.read()
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
    return " ".join(text.split()).strip()


def _parse_date(date_str: str) -> Optional[str]:
    """Parse DD-MM-YYYY to ISO YYYY-MM-DD."""
    if not date_str:
        return None
    m = re.match(r"(\d{1,2})-(\d{1,2})-(\d{4})", date_str.strip())
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
    return None


def _parse_ficha(html: str, ficha_id: int) -> Optional[Dict[str, Any]]:
    """Parse a Sancionatorio Ficha page into a metadata dict."""
    # Expedition number
    exp_match = re.search(r"Expediente:\s*([A-Z]-\d+-\d+)", html)
    if not exp_match:
        return None
    expediente = exp_match.group(1)

    # Dates
    start_match = re.search(r"Fecha\s+Inicio\s*:\s*</b>\s*<i>([^<]+)</i>", html)
    end_match = re.search(r"Fecha\s+T.rmino\s*:\s*</b>\s*<i>([^<]+)</i>", html)
    start_date = _parse_date(start_match.group(1)) if start_match else None
    end_date = _parse_date(end_match.group(1)) if end_match else None

    # Status
    status_match = re.search(r"Estado:\s*</b>\s*<i>([^<]+)</i>", html)
    status = _clean_html(status_match.group(1)) if status_match else ""

    # Entity (titular) - in the panel after "Titular(es)"
    entity = ""
    titular_match = re.search(
        r"Titular(?:es)?\s*</h4>\s*<ul>(.*?)</ul>",
        html, re.DOTALL
    )
    if titular_match:
        names = re.findall(r"<li[^>]*>([^<]+)</li>", titular_match.group(1))
        if not names:
            names = re.findall(r"<a[^>]*>([^<]+)</a>", titular_match.group(1))
        if not names:
            # Try to get text content
            entity_text = _clean_html(titular_match.group(1))
            if entity_text:
                entity = entity_text
        else:
            entity = "; ".join(n.strip() for n in names if n.strip())
    if not entity:
        # Alternative: look for text after Titular header
        tit2 = re.search(r"Titular.*?<a[^>]*>([^<]+)</a>", html, re.DOTALL)
        if tit2:
            entity = tit2.group(1).strip()

    # Unit name and location
    unit_name = ""
    location = ""
    unit_match = re.search(
        r'box-unidad-fiscalizable">\s*<h4>.*?Unidad\s+fiscalizable.*?</h4>\s*<ul>(.*?)</ul>',
        html, re.DOTALL
    )
    if unit_match:
        unit_block = unit_match.group(1)
        name_match = re.search(r'<a[^>]*>([^<]+)</a>', unit_block)
        if name_match:
            unit_name = name_match.group(1).strip()
        loc_match = re.search(r'<br\s*/?>([^<]+)', unit_block)
        if loc_match:
            location = unescape(loc_match.group(1).strip())

    # Find resolution sancionatoria PDF
    # Look for document rows that mention "Resolución Sancionatoria" or "Resolucion Exenta"
    # These are in table rows with /General/Descargar/{id} links
    resolution_doc_id = None
    resolution_date = None

    # Strategy: find all doc entries and pick the resolution sancionatoria
    doc_entries = re.findall(
        r'<tr[^>]*>(.*?)</tr>',
        html, re.DOTALL
    )
    for row in doc_entries:
        if '/General/Descargar/' not in row:
            continue
        row_text = _clean_html(row).lower()
        # Look for resolution sancionatoria
        if 'resoluci' in row_text and ('sancionatoria' in row_text or 'sancion' in row_text):
            doc_match = re.search(r'/General/Descargar/(\d+)', row)
            if doc_match:
                resolution_doc_id = doc_match.group(1)
                # Try to find date in this row
                date_m = re.search(r'(\d{2}-\d{2}-\d{4})', row)
                if date_m:
                    resolution_date = _parse_date(date_m.group(1))
                break

    # If no explicit resolution sancionatoria found, look for "Resolución Exenta" with sanction context
    if not resolution_doc_id:
        for row in doc_entries:
            if '/General/Descargar/' not in row:
                continue
            row_text = _clean_html(row).lower()
            if 'resoluci' in row_text and ('exenta' in row_text or 'absolut' in row_text or 'termin' in row_text):
                doc_match = re.search(r'/General/Descargar/(\d+)', row)
                if doc_match:
                    resolution_doc_id = doc_match.group(1)
                    date_m = re.search(r'(\d{2}-\d{2}-\d{4})', row)
                    if date_m:
                        resolution_date = _parse_date(date_m.group(1))
                    break

    # Last resort: take the last document (usually the final resolution)
    if not resolution_doc_id:
        all_docs = re.findall(r'/General/Descargar/(\d+)', html)
        if all_docs:
            resolution_doc_id = all_docs[-1]

    if not resolution_doc_id:
        return None

    return {
        "ficha_id": ficha_id,
        "expediente": expediente,
        "start_date": start_date,
        "end_date": end_date,
        "resolution_date": resolution_date,
        "status": status,
        "entity": entity,
        "unit_name": unit_name,
        "location": location,
        "resolution_doc_id": resolution_doc_id,
    }


class SMAScraper(BaseScraper):
    SOURCE_ID = "CL/SMA"

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        """Yield all sanction procedure records."""
        count = 0
        consecutive_errors = 0

        for ficha_id in range(1, MAX_FICHA_ID + 1):
            if max_records and count >= max_records:
                return

            url = FICHA_URL.format(id=ficha_id)
            time.sleep(2)  # Rate limit

            try:
                html = _get(url, timeout=60)
                consecutive_errors = 0
            except HTTPError as e:
                if e.code in (404, 500):
                    consecutive_errors += 1
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                        logger.info(f"Stopping: {MAX_CONSECUTIVE_ERRORS} consecutive errors after ID {ficha_id}")
                        return
                    continue
                logger.warning(f"HTTP {e.code} for Ficha/{ficha_id}")
                consecutive_errors += 1
                continue
            except (URLError, OSError) as e:
                logger.warning(f"Network error for Ficha/{ficha_id}: {e}")
                consecutive_errors += 1
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    return
                continue

            meta = _parse_ficha(html, ficha_id)
            if not meta:
                logger.debug(f"Ficha/{ficha_id}: no parseable data")
                continue

            record = self._process_document(meta)
            if record:
                yield record
                count += 1
                logger.info(f"[{count}] {meta['expediente']} ({len(record.get('text', ''))} chars)")

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent sanction procedures (scan from high IDs down)."""
        consecutive_empty = 0
        for ficha_id in range(MAX_FICHA_ID, 0, -1):
            url = FICHA_URL.format(id=ficha_id)
            time.sleep(2)

            try:
                html = _get(url, timeout=60)
            except Exception:
                consecutive_empty += 1
                if consecutive_empty >= MAX_CONSECUTIVE_ERRORS:
                    return
                continue

            meta = _parse_ficha(html, ficha_id)
            if not meta:
                consecutive_empty += 1
                if consecutive_empty >= MAX_CONSECUTIVE_ERRORS:
                    return
                continue
            consecutive_empty = 0

            # Check if the case is recent enough
            case_date = meta.get("resolution_date") or meta.get("end_date") or meta.get("start_date")
            if since and case_date and case_date < since:
                return

            record = self._process_document(meta)
            if record:
                yield record

    def _process_document(self, meta: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Download resolution PDF and build normalized record."""
        doc_id = meta["resolution_doc_id"]
        expediente = meta["expediente"]
        url = DOWNLOAD_URL.format(doc_id=doc_id)

        logger.info(f"Downloading PDF for {expediente} (doc {doc_id})")
        time.sleep(1)

        try:
            pdf_data = _get_bytes(url, timeout=120)
        except Exception as e:
            logger.warning(f"PDF download failed for {expediente}: {e}")
            return None

        if len(pdf_data) < 100:
            logger.warning(f"PDF too small for {expediente} ({len(pdf_data)} bytes)")
            return None

        text = extract_pdf_markdown(
            source=self.SOURCE_ID,
            source_id=expediente,
            pdf_bytes=pdf_data,
            table="case_law",
        )

        if not text or len(text) < 50:
            logger.warning(f"No/insufficient text for {expediente} ({len(text) if text else 0} chars)")
            return None

        return self.normalize({**meta, "text": text})

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        date = raw.get("resolution_date") or raw.get("end_date") or raw.get("start_date")

        title_parts = []
        if raw.get("expediente"):
            title_parts.append(f"Procedimiento Sancionatorio {raw['expediente']}")
        if raw.get("entity"):
            title_parts.append(raw["entity"])
        if raw.get("unit_name"):
            title_parts.append(raw["unit_name"])
        title = " - ".join(title_parts) or f"SMA Sanción {raw.get('ficha_id', '')}"

        return {
            "_id": raw["expediente"],
            "_source": self.SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": date,
            "url": f"{BASE_URL}/Sancionatorio/Ficha/{raw['ficha_id']}",
            "expediente": raw["expediente"],
            "status": raw.get("status", ""),
            "entity": raw.get("entity", ""),
            "unit_name": raw.get("unit_name", ""),
            "location": raw.get("location", ""),
        }


# ── CLI entry point ─────────────────────────────────────────────
def main():
    import argparse

    parser = argparse.ArgumentParser(description="CL/SMA data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    args = parser.parse_args()

    source_dir = Path(__file__).resolve().parent
    scraper = SMAScraper(str(source_dir))

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            html = _get(FICHA_URL.format(id=1), timeout=30)
            meta = _parse_ficha(html, 1)
            if meta:
                logger.info(f"  Ficha/1: {meta['expediente']} - {meta.get('entity', 'N/A')}")
            else:
                logger.info("  Ficha/1: page loaded but no data parsed")
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
