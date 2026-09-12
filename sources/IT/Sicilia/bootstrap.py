#!/usr/bin/env python3
"""
IT/Sicilia -- Leggi della Regione Siciliana

Fetches regional laws (leggi regionali) of the Sicilian Region from the Banche
Dati of the Assemblea Regionale Siciliana (ARS).

Strategy:
  - The ARS search front-end (/home/cerca/201.jsp) drives a stateful ICA/Tomcat
    engine under /icaro/ whose results require browser JS. However, every law is
    ALSO published as a single consolidated HTML document under a stable,
    enumerable URL scheme:
        https://w3.ars.sicilia.it/lex/L_{YEAR}_{NUMBER:03d}.htm
    e.g. /lex/L_1947_001.htm, /lex/L_2025_001.htm
  - We enumerate year-by-year (1947 -> current). For each year we increment the
    law number from 1 until we hit several consecutive HTTP 404s (laws are
    numbered consecutively from 1 each year).
  - Each document is FrontPage-generated HTML (windows-1252). We decode it,
    extract the full text of all articles, plus title/date/G.U.R.S. metadata.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch laws from recent years
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import html as html_mod
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.Sicilia")

BASE_URL = "https://w3.ars.sicilia.it"
LEX_URL = BASE_URL + "/lex/L_{year}_{number:03d}.htm"

FIRST_YEAR = 1947
CURRENT_YEAR = datetime.now().year

# Stop scanning a year after this many consecutive missing law numbers.
MAX_CONSECUTIVE_MISSING = 3
# Safety cap on law numbers per year (Sicily has never approached this).
MAX_NUMBER_PER_YEAR = 120


class SiciliaScraper(BaseScraper):
    SOURCE_ID = "IT/Sicilia"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
            "Accept": "text/html,application/xhtml+xml",
        })

    # ── HTTP ──────────────────────────────────────────────────────────
    def _get(self, url: str) -> Optional[requests.Response]:
        """GET with retries. Returns None on a definitive 404, raises on other
        persistent failures."""
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=60)
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                # ARS pages are windows-1252 (FrontPage). Force it.
                resp.encoding = "windows-1252"
                return resp
            except requests.RequestException as e:
                if attempt == 2:
                    raise
                logger.warning("GET attempt %d failed for %s: %s", attempt + 1, url, e)
                time.sleep(2 ** attempt)
        raise RuntimeError("unreachable")

    # ── Parsing ───────────────────────────────────────────────────────
    @staticmethod
    def _lines(soup: BeautifulSoup) -> List[str]:
        """Return cleaned, non-empty text lines (one per <p>)."""
        out: List[str] = []
        for p in soup.find_all("p"):
            txt = p.get_text(" ", strip=True)
            txt = html_mod.unescape(txt).replace("\xa0", " ")
            txt = re.sub(r"\s+", " ", txt).strip()
            if txt and txt not in {"-°-", "-°-"}:
                out.append(txt)
        return out

    def _parse_law(self, html: str, year: int, number: int, url: str) -> Optional[Dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        lines = self._lines(soup)
        if not lines:
            return None

        legislatura = ""
        law_date = None
        gurs = ""
        title = ""

        # full text from <p> lines (header + all articles), joined by newlines
        full_text = "\n".join(lines).strip()

        for i, ln in enumerate(lines):
            m = re.search(r"\(([^()]*?)Legislatura\)", ln)
            if m and not legislatura:
                legislatura = m.group(1).strip()

            m = re.search(
                r"Legge Regionale n\.\s*\d+\s+del\s+(\d{1,2})\s+(\d{1,2})\s+(\d{4})", ln
            )
            if m and not law_date:
                d, mo, y = m.groups()
                law_date = f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
                # The descriptive subject is the next non-citation line.
                for nxt in lines[i + 1:]:
                    if nxt.startswith("(Gazzetta"):
                        continue
                    if re.match(r"Legge Regionale n\.", nxt):
                        continue
                    title = nxt
                    break

            m = re.search(
                r"Gazzetta Ufficiale Regione Siciliana\s+(\d{1,2})\s+(\d{1,2})\s+(\d{4})\s+n\.\s*(\d+)",
                ln,
            )
            if m and not gurs:
                d, mo, y, n = m.groups()
                gurs = f"G.U.R.S. {int(d):02d}.{int(mo):02d}.{y} n. {n}"

        if not title:
            title = f"Legge Regionale n. {number} del {year}"

        return {
            "year": year,
            "number": number,
            "legislatura": legislatura,
            "title": title,
            "date": law_date,
            "gurs": gurs,
            "text": full_text,
            "url": url,
        }

    def _fetch_law(self, year: int, number: int) -> Optional[Dict[str, Any]]:
        url = LEX_URL.format(year=year, number=number)
        resp = self._get(url)
        if resp is None:
            return None
        raw = self._parse_law(resp.text, year, number, url)
        if raw and len(raw.get("text", "")) >= 50:
            return raw
        return None

    # ── Normalize ─────────────────────────────────────────────────────
    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        year = raw.get("year")
        number = raw.get("number")
        subject = raw.get("title", "")
        law_number = f"L.R. {number}/{year}"
        return {
            "_id": f"IT/Sicilia/LR-{year}-{number}",
            "_source": "IT/Sicilia",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": f"{law_number} — {subject}" if subject else law_number,
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url"),
            "law_number": law_number,
            "legislatura": raw.get("legislatura", ""),
            "gurs": raw.get("gurs", ""),
        }

    # ── Enumeration ───────────────────────────────────────────────────
    def _iter_year(self, year: int) -> Generator[Dict[str, Any], None, None]:
        missing = 0
        for number in range(1, MAX_NUMBER_PER_YEAR + 1):
            try:
                raw = self._fetch_law(year, number)
            except Exception as e:
                logger.warning("Error fetching L %d/%d: %s", number, year, e)
                missing += 1
                if missing >= MAX_CONSECUTIVE_MISSING:
                    break
                continue
            if raw is None:
                missing += 1
                if missing >= MAX_CONSECUTIVE_MISSING:
                    break
                time.sleep(0.5)
                continue
            missing = 0
            yield self.normalize(raw)
            time.sleep(1.0)

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        for year in range(FIRST_YEAR, CURRENT_YEAR + 1):
            logger.info("Scanning year %d...", year)
            count = 0
            for record in self._iter_year(year):
                count += 1
                yield record
            logger.info("Year %d: %d laws", year, count)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        try:
            since_year = int(str(since)[:4])
        except (ValueError, IndexError):
            since_year = CURRENT_YEAR - 1
        for year in range(since_year, CURRENT_YEAR + 1):
            logger.info("Updating year %d...", year)
            for record in self._iter_year(year):
                yield record

    # ── Connectivity test ─────────────────────────────────────────────
    def test(self) -> bool:
        try:
            raw = self._fetch_law(1947, 1)
            return bool(raw and "ARTICOLO" in raw["text"].upper())
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = SiciliaScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        ok = scraper.test()
        print("OK" if ok else "FAIL")
        sys.exit(0 if ok else 1)

    elif command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        max_records = 15 if sample_mode else 999999

        for record in scraper.fetch_all():
            out_path = sample_dir / f"{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info(
                "[%d] %s — %d chars",
                count,
                record.get("law_number", record["_id"]),
                len(record.get("text", "")),
            )
            if count >= max_records:
                break

        logger.info("Done: %d records saved to %s", count, sample_dir)

    elif command == "update":
        since = sys.argv[2] if len(sys.argv) > 2 else str(CURRENT_YEAR - 1)
        count = 0
        for record in scraper.fetch_updates(since):
            count += 1
            logger.info("[%d] %s", count, record.get("law_number", record["_id"]))
        logger.info("Update done: %d records", count)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
