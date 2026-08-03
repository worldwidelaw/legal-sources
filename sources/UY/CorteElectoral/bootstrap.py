#!/usr/bin/env python3
"""
UY/CorteElectoral -- Uruguayan Corte Electoral jurisprudence (Sentencias)

Fetches the FULL TEXT of decisions ("Sentencias") of Uruguay's Corte Electoral
(constitutional-rank electoral authority) from IMPO's official "Base de Datos de
la Corte Electoral".

Access model:
  - IMPO's *search* UI is behind a free login, BUT every individual sentencia
    document page is PUBLIC at a stable URL:
        https://www.impo.com.uy/bases/sentencias-corte-electoral/{NUM}-{YEAR}/1
    e.g. .../sentencias-corte-electoral/27220-2014/1
  - The full text is served as latin-1 (ISO-8859-1) HTML on page /1.
  - A non-existent {NUM}-{YEAR} pair returns a ~219-char placeholder
    ("Todos los derechos reservados"); a real one returns the decision body.

Enumeration strategy (no login required):
  Sentencia numbers are a single dense, sequential, per-base counter; each number
  belongs to exactly one (monotonically non-decreasing) year. We anchor on a known
  valid pair (27220 / 2014) and walk the counter UP and DOWN, trying a small window
  of candidate years for each number, stopping after a long run of consecutive
  misses (corpus boundary). The dense 2014 band makes sampling instant.

Data: Public (IMPO official document pages; no auth to read a document).
Rate limit: polite delay between requests (robots.txt Crawl-Delay: 10).

Usage:
  python bootstrap.py bootstrap --sample            # 12 sample sentencias
  python bootstrap.py bootstrap                      # full crawl (idempotent)
  python bootstrap.py bootstrap-fast                 # streaming full crawl
  python bootstrap.py test-api                        # connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UY.CorteElectoral")

BASE = "https://www.impo.com.uy"
DOC_URL = BASE + "/bases/sentencias-corte-electoral/{num}-{year}/1"

# Known-good anchor (verified): sentencia 27220 was issued in 2014.
ANCHOR_NUM = 27220
ANCHOR_YEAR = 2014

# Plausible corpus year span (used only to bound the year-probe window).
MIN_YEAR = 1972
MAX_YEAR = datetime.now(timezone.utc).year

# A real document is well over this; the "not found" placeholder is ~219 chars.
MIN_VALID_TEXT = 400
# Stop a directional walk after this many consecutive numbers yield nothing.
MAX_CONSECUTIVE_MISSES = 40
# How many candidate years to try per number when resyncing across a boundary.
YEAR_WINDOW = 3

SPANISH_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "setiembre": 9, "septiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}


def html_to_text(html_content: str) -> str:
    """Extract clean plain text from an IMPO sentencia document page."""
    if not html_content:
        return ""
    content = html_content

    # Drop non-content blocks (scripts, styles, the top nav bar, the footer).
    content = re.sub(r'<script[^>]*>.*?</script>', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'<nav\b.*?</nav>', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'<header\b.*?</header>', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'<footer\b.*?</footer>', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'<noscript\b.*?</noscript>', '', content, flags=re.DOTALL | re.IGNORECASE)

    # Preserve block structure as newlines.
    content = re.sub(r'<h[1-6][^>]*>(.*?)</h[1-6]>', r'\n\n\1\n', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'<p[^>]*>', '\n\n', content, flags=re.IGNORECASE)
    content = re.sub(r'<br\s*/?>', '\n', content, flags=re.IGNORECASE)
    content = re.sub(r'<(div|tr|li)[^>]*>', '\n', content, flags=re.IGNORECASE)

    content = re.sub(r'<[^>]+>', '', content)
    content = html_module.unescape(content)

    content = re.sub(r'[ \t]+', ' ', content)
    content = re.sub(r'\n[ \t]+', '\n', content)
    content = re.sub(r'\n{3,}', '\n\n', content)
    content = content.strip()

    # Trim leading UI chrome ("Ingresar", menu labels) up to the decision marker.
    m = re.search(r'(VISTO|RESULTANDO|CONSIDERANDO|Montevideo,|SENTENCIA N|ACORDADA|ATENTO)', content)
    if m and m.start() < 1500:
        content = content[m.start():].strip()

    # Drop a trailing boilerplate footer if present.
    content = re.split(r'Todos los derechos reservados', content)[0].strip()
    # Strip trailing standalone UI labels (Ayuda / Imprimir / Compartir / Ver Original).
    content = re.sub(
        r'(?:\s*\n\s*(?:Ayuda|Imprimir|Compartir|Ver Original|Volver|Inicio))+\s*$',
        '', content, flags=re.IGNORECASE,
    ).strip()
    return content


def parse_date(text: str, year: int) -> Optional[str]:
    """Parse 'Montevideo, 7 de mayo de 2014' -> ISO date; fallback to Jan 1 of year."""
    m = re.search(r'(\d{1,2})\s+de\s+([A-Za-zñ]+)\s+de\s+(\d{4})', text, re.IGNORECASE)
    if m:
        day = int(m.group(1))
        mon = SPANISH_MONTHS.get(m.group(2).lower())
        yr = int(m.group(3))
        if mon and 1 <= day <= 31 and MIN_YEAR <= yr <= MAX_YEAR + 1:
            try:
                return f"{yr:04d}-{mon:02d}-{day:02d}"
            except Exception:
                pass
    return f"{year:04d}-01-01"


class CorteElectoralScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; open data research)",
                "Accept": "text/html",
            },
            timeout=60,
        )
        self.delay = float(self.config.get("fetch", {}).get("rate_limit_delay", 2))

    # ── low-level fetch ────────────────────────────────────────────
    def _fetch_doc(self, num: int, year: int) -> Optional[dict]:
        """Return raw doc dict if (num, year) is a real sentencia, else None."""
        url = DOC_URL.format(num=num, year=year)
        try:
            resp = self.http.get(url)
        except Exception as e:
            logger.debug(f"  fetch error {num}-{year}: {e}")
            return None
        if resp.status_code != 200:
            return None
        raw_html = resp.content.decode("latin-1", errors="replace")
        text = html_to_text(raw_html)
        if len(text) < MIN_VALID_TEXT:
            return None
        return {
            "number": num,
            "year": year,
            "text": text,
            "url": url,
        }

    def test_api(self):
        logger.info("Testing IMPO Corte Electoral document access...")
        doc = self._fetch_doc(ANCHOR_NUM, ANCHOR_YEAR)
        if doc and len(doc["text"]) > MIN_VALID_TEXT:
            logger.info(f"  OK: {ANCHOR_NUM}-{ANCHOR_YEAR} -> {len(doc['text'])} chars")
            logger.info("Connectivity test PASSED")
            return True
        logger.error("Connectivity test FAILED")
        return False

    # ── enumeration ────────────────────────────────────────────────
    def _walk(self, start_num: int, start_year: int, direction: int) -> Generator[dict, None, None]:
        """Walk the sentencia counter in `direction` (+1 up / -1 down)."""
        num = start_num
        year = start_year
        misses = 0
        while misses < MAX_CONSECUTIVE_MISSES and MIN_YEAR <= year <= MAX_YEAR + 1:
            doc = None
            # Try the current year first, then nearby years in walk direction.
            if direction > 0:
                candidate_years = [year + d for d in range(0, YEAR_WINDOW + 1)]
            else:
                candidate_years = [year - d for d in range(0, YEAR_WINDOW + 1)]
            for cy in candidate_years:
                if cy < MIN_YEAR or cy > MAX_YEAR + 1:
                    continue
                time.sleep(self.delay)
                doc = self._fetch_doc(num, cy)
                if doc:
                    year = cy
                    break
            if doc:
                misses = 0
                yield doc
            else:
                misses += 1
            num += direction

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all sentencias (raw). Walks up from the anchor first (so samples
        come from the dense recent band), then down toward the oldest decisions."""
        seen = set()
        # Up from anchor (inclusive).
        for doc in self._walk(ANCHOR_NUM, ANCHOR_YEAR, +1):
            key = (doc["number"], doc["year"])
            if key in seen:
                continue
            seen.add(key)
            yield doc
        # Down from just below the anchor.
        for doc in self._walk(ANCHOR_NUM - 1, ANCHOR_YEAR, -1):
            key = (doc["number"], doc["year"])
            if key in seen:
                continue
            seen.add(key)
            yield doc

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """Re-walk the most recent band (cheap; full enumeration is idempotent)."""
        for doc in self._walk(ANCHOR_NUM, ANCHOR_YEAR, +1):
            yield doc

    # ── normalization ──────────────────────────────────────────────
    def normalize(self, raw: dict) -> Optional[dict]:
        if not raw or len(raw.get("text", "")) < MIN_VALID_TEXT:
            return None
        num = raw["number"]
        year = raw["year"]
        text = raw["text"]
        date = parse_date(text, year)
        return {
            "_id": f"UY-CorteElectoral-Sentencia-{num}-{year}",
            "_source": "UY/CorteElectoral",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": f"Sentencia N° {num} (Corte Electoral, {year})",
            "text": text,
            "date": date,
            "url": raw["url"],
            "sentencia_number": str(num),
            "year": str(year),
            "court": "Corte Electoral",
            "jurisdiction": "UY",
        }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="UY/CorteElectoral bootstrapper")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "update", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--sample-size", type=int, default=12)
    args = parser.parse_args()

    scraper = CorteElectoralScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)
    elif args.command == "bootstrap":
        if args.sample:
            stats = scraper.run_sample(n=args.sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats.get('records_new', 0)} new, "
                  f"{stats.get('records_updated', 0)} updated, {stats.get('records_skipped', 0)} skipped")
        print(json.dumps(stats, indent=2))
    elif args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        print(json.dumps(stats, indent=2))
    elif args.command == "update":
        stats = scraper.update()
        print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
