#!/usr/bin/env python3
"""
ES/Asturias -- Asturias Regional Legislation Data Fetcher

Fetches regional legislation from the Boletín Oficial del Principado de Asturias
(BOPA) via the Asturias Open Data JSON index and the Liferay detail endpoint.

Strategy:
  - Download annual JSON index files from descargas.asturias.es (2019-2024).
  - Filter for DISPOSICIONES GENERALES (core legislation).
  - For each disposition, fetch full text from miprincipado.asturias.es detail page.
  - Extract text from <div id="bopa-articulo"> element.
  - For 2025+, use the daily sumario endpoint to discover new dispositions.

Data:
  - ~100-160 legislative dispositions per year (DISPOSICIONES GENERALES).
  - License: CC BY 4.0
  - Language: Spanish (es)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import time
import socket
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Dict, Any, Optional
from urllib.parse import quote

socket.setdefaulttimeout(120)

try:
    import requests
    from requests.adapters import HTTPAdapter
except ImportError:
    print("ERROR: requests is required. Install with: pip install requests")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ES.asturias")

OPEN_DATA_BASE = "https://descargas.asturias.es/asturias/opendata/LegislacionyJusticia"
DETAIL_URL = (
    "https://miprincipado.asturias.es/bopa/disposiciones"
    "?p_p_id=pa_sede_bopa_web_portlet_SedeBopaDispositionWeb"
    "&p_p_lifecycle=0"
    "&_pa_sede_bopa_web_portlet_SedeBopaDispositionWeb_mvcRenderCommandName=/disposition/detail"
    "&p_r_p_dispositionReference={codigo}"
    "&p_r_p_dispositionDate={date}"
)
SUMARIO_URL = (
    "https://miprincipado.asturias.es/bopa-sumario"
    "?p_p_id=pa_sede_bopa_web_portlet_SedeBopaSummaryWeb"
    "&p_p_lifecycle=0"
    "&p_r_p_summaryDate={date}"
    "&p_r_p_summaryIsSearch=false"
)

# Legislation subsection patterns (case-insensitive matching)
LEGISLATION_SUBSECTIONS = [
    "disposiciones generales",
]


class AsturiasScraper(BaseScraper):
    """
    Scraper for ES/Asturias -- Asturias Regional Legislation (BOPA).
    Country: ES
    URL: https://sede.asturias.es/bopa

    Data types: legislation
    Auth: none (CC BY 4.0)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml,application/json",
        })
        adapter = HTTPAdapter(max_retries=3)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _clean_html(self, text: str) -> str:
        """Strip HTML tags and clean whitespace."""
        if not text:
            return ""
        text = re.sub(r'<br\s*/?\s*>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = html.unescape(text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n[ \t]+', '\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _fetch_index_for_year(self, year: int) -> list:
        """Download and parse the JSON index for a given year."""
        if year == 2024:
            url = f"{OPEN_DATA_BASE}/BOPA2019-28/dataset-sum-bopa-2024.json"
        elif 2019 <= year <= 2023:
            url = f"{OPEN_DATA_BASE}/BOPA2019-28/dataset-sum_bopa_{year}.json"
        else:
            return []

        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=60)
        if resp.status_code != 200:
            logger.warning(f"Could not fetch index for {year}: HTTP {resp.status_code}")
            return []

        data = resp.json()

        # 2024 wraps in {"DATOS ABIERTOS BOPA": [...]}
        if isinstance(data, dict):
            key = list(data.keys())[0]
            records = data[key]
        else:
            records = data

        # Validate the format - 2023 file is malformed
        if records and isinstance(records[0], dict):
            keys = list(records[0].keys())
            if len(keys) <= 4 and any("FECHA" in k and "CODIGO" in k for k in keys):
                logger.warning(f"Year {year}: malformed JSON (delimiter issue), skipping")
                return []

        return records

    def _filter_legislation(self, records: list) -> list:
        """Filter records to only legislation (DISPOSICIONES GENERALES)."""
        results = []
        for r in records:
            subseccion = r.get("SUBSECCION", "").strip().lower()
            if any(pat in subseccion for pat in LEGISLATION_SUBSECTIONS):
                results.append(r)
        return results

    def _parse_date_from_record(self, record: dict) -> str:
        """Extract and normalize date from a record (various formats)."""
        date_str = record.get("FECHA-BOLETIN", "")
        if not date_str:
            return ""

        # YYYY-MM-DD (2024 format)
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            return date_str

        # YYYY/MM/DD (2019-2022 format)
        if re.match(r'^\d{4}/\d{2}/\d{2}$', date_str):
            parts = date_str.split("/")
            return f"{parts[0]}-{parts[1]}-{parts[2]}"

        # MM/DD/YYYY (2009 format)
        if re.match(r'^\d{2}/\d{2}/\d{4}$', date_str):
            parts = date_str.split("/")
            return f"{parts[2]}-{parts[0]}-{parts[1]}"

        return date_str

    def _date_to_ddmmyyyy(self, iso_date: str) -> str:
        """Convert ISO date (YYYY-MM-DD) to DD/MM/YYYY for the detail endpoint."""
        if not iso_date:
            return ""
        parts = iso_date.split("-")
        if len(parts) == 3:
            return f"{parts[2]}/{parts[1]}/{parts[0]}"
        return iso_date

    def _fetch_full_text(self, codigo: str, date_ddmmyyyy: str) -> str:
        """Fetch full text for a disposition from the detail endpoint."""
        url = DETAIL_URL.format(codigo=codigo, date=date_ddmmyyyy)

        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=60)
        resp.raise_for_status()
        page = resp.text

        # Extract content from <div ... id="bopa-articulo">
        start = page.find('id="bopa-articulo"')
        if start < 0:
            return ""

        # Find the end: look for the navigation section (Regresar al sumario)
        end = page.find("Regresar al sumario", start)
        if end < 0:
            end = page.find("bulletin-disposition", start + 100)
        if end < 0:
            end = start + 100000  # fallback: take a large chunk

        content_html = page[start:end]

        # Clean HTML to plain text
        text = self._clean_html(content_html)

        # Remove leading artifacts from the id attribute
        text = re.sub(r'^id="bopa-articulo"\s*>?\s*', '', text)

        return text

    def _fetch_sumario_refs(self, date_ddmmyyyy: str) -> list:
        """Fetch disposition references from a daily sumario page."""
        url = SUMARIO_URL.format(date=quote(date_ddmmyyyy, safe=''))

        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=60)
        if resp.status_code != 200:
            return []

        refs = re.findall(r'dispositionReference=([^&"]+)', resp.text)
        # Deduplicate while preserving order
        seen = set()
        unique_refs = []
        for r in refs:
            r_clean = r.strip().replace("+", "").replace("%20", "")
            if r_clean and r_clean not in seen:
                seen.add(r_clean)
                unique_refs.append(r_clean)

        return unique_refs

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legislative documents from BOPA (2019-2024 via JSON index)."""
        for year in range(2019, 2025):
            logger.info(f"Fetching index for year {year}...")
            records = self._fetch_index_for_year(year)
            if not records:
                logger.warning(f"No records for {year}, trying sumario fallback...")
                yield from self._fetch_year_via_sumario(year)
                continue

            legislation = self._filter_legislation(records)
            logger.info(f"Year {year}: {len(legislation)} legislation records (of {len(records)} total)")

            for record in legislation:
                codigo = record.get("CODIGO", "").strip()
                if not codigo:
                    continue

                iso_date = self._parse_date_from_record(record)
                date_ddmmyyyy = self._date_to_ddmmyyyy(iso_date)

                if not date_ddmmyyyy:
                    continue

                try:
                    text = self._fetch_full_text(codigo, date_ddmmyyyy)
                except Exception as e:
                    logger.warning(f"Error fetching {codigo}: {e}")
                    continue

                if not text or len(text) < 100:
                    logger.debug(f"Skipping {codigo}: text too short ({len(text) if text else 0} chars)")
                    continue

                yield {
                    "codigo": codigo,
                    "title": record.get("TITULO", "").strip(),
                    "date": iso_date,
                    "text": text,
                    "section": record.get("SECCION", "").strip(),
                    "subsection": record.get("SUBSECCION", "").strip(),
                    "organism": record.get("NOMBRE-ORGANISMO-PADRE", "").strip(),
                    "bulletin_number": record.get("NUMERO-BOLETIN", ""),
                }

    def _fetch_year_via_sumario(self, year: int) -> Generator[dict, None, None]:
        """Fallback: iterate through all days of a year via sumario endpoint."""
        start_date = datetime(year, 1, 1)
        end_date = datetime(year, 12, 31)
        current = start_date

        while current <= end_date:
            date_str = current.strftime("%d/%m/%Y")
            # Skip weekends (BOPA doesn't publish on weekends)
            if current.weekday() < 5:
                refs = self._fetch_sumario_refs(date_str)
                for codigo in refs:
                    try:
                        text = self._fetch_full_text(codigo, date_str)
                    except Exception as e:
                        logger.warning(f"Error fetching {codigo} on {date_str}: {e}")
                        continue

                    if not text or len(text) < 100:
                        continue

                    iso_date = current.strftime("%Y-%m-%d")
                    yield {
                        "codigo": codigo,
                        "title": "",
                        "date": iso_date,
                        "text": text,
                        "section": "",
                        "subsection": "",
                        "organism": "",
                        "bulletin_number": "",
                    }

            current += timedelta(days=1)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents published since the given date via sumario."""
        current = since
        today = datetime.now()

        while current <= today:
            if current.weekday() < 5:
                date_str = current.strftime("%d/%m/%Y")
                refs = self._fetch_sumario_refs(date_str)
                iso_date = current.strftime("%Y-%m-%d")

                for codigo in refs:
                    try:
                        text = self._fetch_full_text(codigo, date_str)
                    except Exception as e:
                        logger.warning(f"Error fetching {codigo}: {e}")
                        continue

                    if not text or len(text) < 100:
                        continue

                    yield {
                        "codigo": codigo,
                        "title": "",
                        "date": iso_date,
                        "text": text,
                        "section": "",
                        "subsection": "",
                        "organism": "",
                        "bulletin_number": "",
                    }

            current += timedelta(days=1)

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        codigo = raw.get("codigo", "")
        title = raw.get("title", "")
        text = raw.get("text", "")
        date = raw.get("date", "")
        date_ddmmyyyy = self._date_to_ddmmyyyy(date)

        url = DETAIL_URL.format(codigo=codigo, date=date_ddmmyyyy)

        return {
            "_id": f"BOPA-{codigo}",
            "_source": "ES/Asturias",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "section": raw.get("section", ""),
            "subsection": raw.get("subsection", ""),
            "organism": raw.get("organism", ""),
            "bulletin_number": raw.get("bulletin_number", ""),
            "language": "es",
            "region": "Asturias",
            "country": "ES",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing BOPA Asturias connection...")

        print("\n1. Testing Open Data JSON index (2024)...")
        try:
            records = self._fetch_index_for_year(2024)
            legislation = self._filter_legislation(records)
            print(f"   Total records: {len(records)}, Legislation: {len(legislation)}")
            if legislation:
                print(f"   Sample: {legislation[0].get('CODIGO')} - {legislation[0].get('TITULO', '')[:80]}")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        print("\n2. Testing full text retrieval...")
        if legislation:
            rec = legislation[0]
            codigo = rec.get("CODIGO", "").strip()
            iso_date = self._parse_date_from_record(rec)
            date_ddmmyyyy = self._date_to_ddmmyyyy(iso_date)
            try:
                text = self._fetch_full_text(codigo, date_ddmmyyyy)
                print(f"   {codigo}: {len(text)} chars")
                print(f"   Preview: {text[:200]}...")
            except Exception as e:
                print(f"   ERROR: {e}")

        print("\n3. Testing sumario endpoint...")
        try:
            refs = self._fetch_sumario_refs("15/03/2023")
            print(f"   Sumario 2023-03-15: {len(refs)} dispositions")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nAll tests passed!")


def main():
    scraper = AsturiasScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
