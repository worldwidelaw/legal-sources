#!/usr/bin/env python3
"""
IT/Basilicata -- Legislazione Regionale Basilicata

Fetches regional laws (leggi regionali) of the Basilicata Region from the
Consiglio Regionale della Basilicata's legislative database.

Strategy:
  - Each law is served as a full HTML page at a stable URL:
        https://atticonsiglio.consiglio.basilicata.it/AD_Elenco_Leggi?Codice=N
    where N is an internal database ID (1 to ~2389).
  - We enumerate all Codice values. Pages without a detail panel
    (id="MainContent_PnlDettaglio") are skipped.
  - Full consolidated text is extracted from the WordSection1 div.
  - Metadata (law number, date, title, Bollettino reference) is parsed
    from the text header.
  - Coverage: 1971-present, ~2350 regional laws.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent laws (Codice > 2200)
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
from typing import Generator, Optional, Dict, Any

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.Basilicata")

BASE_URL = "https://atticonsiglio.consiglio.basilicata.it"
LAW_URL = BASE_URL + "/AD_Elenco_Leggi?Codice={codice}"

# Codice range: 1 to ~2389 (as of 2026). Add buffer for new laws.
MIN_CODICE = 1
MAX_CODICE = 2500

# Empty page size threshold — pages without law content are ~36KB.
EMPTY_THRESHOLD = 37000

ITALIAN_MONTHS = {
    "gennaio": "01", "febbraio": "02", "marzo": "03", "aprile": "04",
    "maggio": "05", "giugno": "06", "luglio": "07", "agosto": "08",
    "settembre": "09", "ottobre": "10", "novembre": "11", "dicembre": "12",
}


class BasilicataScraper(BaseScraper):
    SOURCE_ID = "IT/Basilicata"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
            "Accept": "text/html,application/xhtml+xml",
        })

    def _get(self, url: str) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=60)
                resp.raise_for_status()
                resp.encoding = "utf-8"
                return resp
            except requests.RequestException as e:
                if attempt == 2:
                    raise
                logger.warning("GET attempt %d failed for %s: %s", attempt + 1, url, e)
                time.sleep(2 ** attempt)
        raise RuntimeError("unreachable")

    @staticmethod
    def _parse_date(text: str) -> Optional[str]:
        """Parse Italian date like '28 giugno 1994' to ISO format."""
        m = re.match(r"(\d{1,2})\s+(\w+)\s+(\d{4})", text.strip())
        if not m:
            return None
        day, month_name, year = m.groups()
        month = ITALIAN_MONTHS.get(month_name.lower())
        if not month:
            return None
        return f"{year}-{month}-{int(day):02d}"

    @staticmethod
    def _clean_text(raw_text: str) -> str:
        """Clean extracted text: normalize whitespace, remove artifacts."""
        text = html_mod.unescape(raw_text)
        text = text.replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _parse_law(self, html_content: str, codice: int) -> Optional[Dict[str, Any]]:
        """Parse a law page and extract metadata + full text."""
        soup = BeautifulSoup(html_content, "html.parser")
        detail = soup.find(id="MainContent_PnlDettaglio")
        if not detail:
            return None

        # Extract text from WordSection1 div (main content)
        ws = detail.find("div", class_="WordSection1")
        if ws:
            full_text = ws.get_text(separator="\n", strip=False)
        else:
            full_text = detail.get_text(separator="\n", strip=False)

        full_text = self._clean_text(full_text)
        if len(full_text) < 50:
            return None

        # Extract law reference: "Legge Regionale DD mese YYYY, n. NN"
        law_number = None
        law_date = None
        m = re.search(
            r"Legge\s+Regionale\s+(\d{1,2}\s+\w+\s+\d{4}),?\s*n\.\s*(\d+)",
            full_text,
        )
        if m:
            law_date = self._parse_date(m.group(1))
            law_number = m.group(2)

        # Extract title from H3
        h3 = detail.find("h3")
        title = h3.get_text(" ", strip=True) if h3 else ""
        title = html_mod.unescape(title).strip()

        # Extract Bollettino Ufficiale reference
        bollettino = ""
        boll_match = re.search(
            r"Bollettino\s+Ufficiale\s+n\.\s*(\d+)\s+del\s+(\d{1,2}\s+\w+\s+\d{4})",
            full_text,
        )
        if boll_match:
            bollettino = f"B.U. n. {boll_match.group(1)} del {boll_match.group(2)}"

        # Extract year from date or text
        year = None
        if law_date:
            year = int(law_date[:4])
        elif law_number:
            year_m = re.search(r"\b(19\d{2}|20\d{2})\b", full_text[:300])
            if year_m:
                year = int(year_m.group(1))

        if not title:
            if law_number and year:
                title = f"Legge Regionale n. {law_number} del {year}"
            else:
                title = f"Legge Regionale (Codice {codice})"

        url = LAW_URL.format(codice=codice)

        return {
            "codice": codice,
            "law_number": law_number,
            "year": year,
            "title": title,
            "date": law_date,
            "bollettino": bollettino,
            "text": full_text,
            "url": url,
        }

    def _fetch_law(self, codice: int) -> Optional[Dict[str, Any]]:
        url = LAW_URL.format(codice=codice)
        resp = self._get(url)
        if resp is None or len(resp.content) < EMPTY_THRESHOLD:
            return None
        return self._parse_law(resp.text, codice)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        codice = raw.get("codice")
        law_number = raw.get("law_number")
        year = raw.get("year")

        if law_number and year:
            _id = f"IT/Basilicata/LR-{year}-{law_number}"
            law_ref = f"L.R. {law_number}/{year}"
        else:
            _id = f"IT/Basilicata/codice-{codice}"
            law_ref = ""

        title = raw.get("title", "")
        if law_ref and title and not title.startswith("L.R."):
            title = f"{law_ref} — {title}"
        elif law_ref and not title:
            title = law_ref

        return {
            "_id": _id,
            "_source": "IT/Basilicata",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url"),
            "law_number": law_ref,
            "bollettino": raw.get("bollettino", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        consecutive_empty = 0
        for codice in range(MIN_CODICE, MAX_CODICE + 1):
            try:
                raw = self._fetch_law(codice)
            except Exception as e:
                logger.warning("Error fetching Codice %d: %s", codice, e)
                consecutive_empty += 1
                if consecutive_empty >= 20:
                    logger.info("20 consecutive empty codes at %d, stopping.", codice)
                    break
                continue

            if raw is None:
                consecutive_empty += 1
                if consecutive_empty >= 20:
                    logger.info("20 consecutive empty codes at %d, stopping.", codice)
                    break
                continue

            consecutive_empty = 0
            yield self.normalize(raw)
            time.sleep(1.0)

            if codice % 100 == 0:
                logger.info("Progress: Codice %d", codice)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent laws — scan from Codice 2200+ for new entries."""
        start_codice = 2200
        try:
            if since.isdigit() and int(since) > 1000:
                start_codice = int(since)
        except (ValueError, AttributeError):
            pass

        consecutive_empty = 0
        for codice in range(start_codice, MAX_CODICE + 1):
            try:
                raw = self._fetch_law(codice)
            except Exception as e:
                logger.warning("Error fetching Codice %d: %s", codice, e)
                consecutive_empty += 1
                if consecutive_empty >= 20:
                    break
                continue

            if raw is None:
                consecutive_empty += 1
                if consecutive_empty >= 20:
                    break
                continue

            consecutive_empty = 0
            yield self.normalize(raw)
            time.sleep(1.0)

    def test(self) -> bool:
        try:
            raw = self._fetch_law(1)
            if not raw:
                return False
            return len(raw.get("text", "")) > 100 and raw.get("law_number") is not None
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = BasilicataScraper()

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
        since = sys.argv[2] if len(sys.argv) > 2 else "2200"
        count = 0
        for record in scraper.fetch_updates(since):
            count += 1
            logger.info("[%d] %s", count, record.get("law_number", record["_id"]))
        logger.info("Update done: %d records", count)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
