#!/usr/bin/env python3
"""
CO/DatosGovCo -- Colombia Constitutional Court Rulings Fetcher

Fetches Colombian Constitutional Court rulings via Socrata SODA API
for metadata + Corte Constitucional relatoria for full text.

Strategy:
  - Query Socrata API for ruling metadata (case ID, date, magistrate, type)
  - Build relatoria URL from sentencia ID and year
  - Download full text HTML from relatoria, strip tags
  - Normalize into standard schema

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import time
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from urllib.parse import urlencode

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CO.DatosGovCo")

SOCRATA_URL = "https://www.datos.gov.co/resource/v2k4-2t8s.json"
RELATORIA_BASE = "https://www.corteconstitucional.gov.co/relatoria"


class DatosGovCoScraper(BaseScraper):
    """Scraper for CO/DatosGovCo -- Colombian Constitutional Court rulings."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        try:
            from common.http_client import HttpClient
            self.client = HttpClient(timeout=60)
        except ImportError:
            self.client = None

    def _http_get(self, url: str, params: dict = None, headers: dict = None) -> Optional[str]:
        """HTTP GET returning response text."""
        full_url = url
        if params:
            full_url = f"{url}?{urlencode(params, doseq=True)}"
        for attempt in range(3):
            try:
                if self.client:
                    resp = self.client.get(full_url, headers=headers or {})
                    if resp.status_code == 200:
                        return resp.text
                    logger.warning(f"HTTP {resp.status_code} for {full_url[:100]}")
                else:
                    import urllib.request
                    req = urllib.request.Request(full_url, headers=headers or {})
                    with urllib.request.urlopen(req, timeout=60) as resp:
                        return resp.read().decode("utf-8", errors="replace")
            except Exception as e:
                logger.warning(f"Attempt {attempt+1} failed for {full_url[:100]}: {e}")
                time.sleep(2 * (attempt + 1))
        return None

    def _fetch_metadata_page(self, offset: int = 0, limit: int = 50) -> Optional[list]:
        """Fetch a page of T-type rulings metadata from Socrata.

        Only T-type (tutela) rulings are fetched because C-type and SU-type
        are on the new Angular SPA site which requires browser automation.
        """
        params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "fecha_sentencia DESC",
            "$where": "sentencia_tipo='T'",
        }
        text = self._http_get(SOCRATA_URL, params=params)
        if not text:
            return None
        try:
            return json.loads(text, strict=False)
        except json.JSONDecodeError:
            return None

    def _sentencia_to_url(self, sentencia: str, fecha: str) -> str:
        """Convert sentencia ID to relatoria URL.

        E.g., "T-012/92" + "1992-..." -> .../relatoria/1992/T-012-92.htm
              "C-141/10" + "2010-..." -> .../relatoria/2010/C-141-10.htm
        """
        # Extract year from fecha
        year = ""
        if fecha and len(fecha) >= 4:
            year = fecha[:4]

        # Convert "T-012/92" to "T-012-92"
        slug = sentencia.replace("/", "-")

        if year:
            return f"{RELATORIA_BASE}/{year}/{slug}.htm"
        return f"{RELATORIA_BASE}/{slug}.htm"

    def _fetch_full_text(self, url: str) -> Optional[str]:
        """Fetch and clean full text from relatoria HTML page."""
        html_content = self._http_get(url)
        if not html_content:
            return None
        return self._clean_html(html_content)

    def _clean_html(self, raw_html: str) -> Optional[str]:
        """Strip HTML tags and clean up text."""
        # Remove script/style blocks
        text = re.sub(r'<script[^>]*>.*?</script>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<head[^>]*>.*?</head>', '', text, flags=re.DOTALL | re.IGNORECASE)
        # Remove nav/header/footer
        text = re.sub(r'<nav[^>]*>.*?</nav>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<header[^>]*>.*?</header>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<footer[^>]*>.*?</footer>', '', text, flags=re.DOTALL | re.IGNORECASE)
        # Replace <br>, <p>, <div> with newlines
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
        # Strip remaining tags
        text = re.sub(r'<[^>]+>', '', text)
        # Decode HTML entities
        text = html.unescape(text)
        # Clean whitespace
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = text.strip()
        return text if len(text) > 200 else None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        sentencia = raw.get("sentencia", "")
        fecha = raw.get("fecha_sentencia", "")
        date = fecha[:10] if fecha and len(fecha) >= 10 else ""
        text = raw.get("text", "")
        magistrado = raw.get("magistrado_a", "")
        sala = raw.get("sala", "")
        proceso = raw.get("proceso", "")
        sentencia_tipo = raw.get("sentencia_tipo", "")

        title = f"Sentencia {sentencia}"
        if proceso:
            title += f" ({proceso})"

        url = raw.get("url", "")

        return {
            "_id": f"CO-CC-{sentencia.replace('/', '-')}",
            "_source": "CO/DatosGovCo",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "sentencia": sentencia,
            "sentencia_tipo": sentencia_tipo,
            "proceso": proceso,
            "magistrado": magistrado,
            "sala": sala,
            "expediente_numero": raw.get("expediente_numero", ""),
        }

    def fetch_all(self, page_size: int = 50, max_pages: int = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch all rulings via Socrata + relatoria."""
        offset = 0
        count = 0
        page = 0
        failures = 0

        while True:
            records = self._fetch_metadata_page(offset, page_size)
            if not records:
                break

            for rec in records:
                sentencia = rec.get("sentencia", "")
                fecha = rec.get("fecha_sentencia", "")
                if not sentencia or not fecha:
                    continue

                url = self._sentencia_to_url(sentencia, fecha)
                time.sleep(1.5)
                full_text = self._fetch_full_text(url)

                if not full_text:
                    failures += 1
                    logger.warning(f"No text for {sentencia} at {url}")
                    if failures > 5 and count == 0:
                        logger.error("Too many failures with no successes, stopping")
                        return
                    continue

                rec["text"] = full_text
                rec["url"] = url
                count += 1
                failures = 0
                yield rec

            offset += page_size
            page += 1
            if max_pages and page >= max_pages:
                break
            if len(records) < page_size:
                break
            time.sleep(1)

        logger.info(f"Completed: {count} rulings fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent rulings."""
        where_clause = ""
        if since:
            where_clause = f"fecha_sentencia >= '{since}'"

        params = {
            "$limit": 200,
            "$order": "fecha_sentencia DESC",
        }
        if where_clause:
            params["$where"] = where_clause

        text = self._http_get(SOCRATA_URL, params=params)
        if not text:
            return
        try:
            records = json.loads(text, strict=False)
        except json.JSONDecodeError:
            return

        count = 0
        for rec in records:
            sentencia = rec.get("sentencia", "")
            fecha = rec.get("fecha_sentencia", "")
            if not sentencia or not fecha:
                continue

            url = self._sentencia_to_url(sentencia, fecha)
            time.sleep(1.5)
            full_text = self._fetch_full_text(url)
            if not full_text:
                continue

            rec["text"] = full_text
            rec["url"] = url
            count += 1
            yield rec

        logger.info(f"Updates: {count} rulings")

    def test(self) -> bool:
        """Quick connectivity test."""
        records = self._fetch_metadata_page(0, 2)
        if not records:
            logger.error("Socrata API returned no results")
            return False

        rec = records[0]
        sentencia = rec.get("sentencia", "")
        fecha = rec.get("fecha_sentencia", "")
        logger.info(f"Socrata OK: {sentencia} from {fecha[:10]}")

        url = self._sentencia_to_url(sentencia, fecha)
        text = self._fetch_full_text(url)
        if text:
            logger.info(f"Relatoria OK: {len(text)} chars from {url}")
            return True
        else:
            logger.warning(f"Relatoria failed for {url}, trying older ruling")
            # Try a known older ruling
            url2 = f"{RELATORIA_BASE}/2023/C-141-23.htm"
            text2 = self._fetch_full_text(url2)
            if text2:
                logger.info(f"Relatoria OK (fallback): {len(text2)} chars")
                return True
            logger.error("Could not fetch full text from relatoria")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CO/DatosGovCo data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = DatosGovCoScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
if __name__ == "__main__":
    main()
