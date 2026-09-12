#!/usr/bin/env python3
"""
IT/Umbria -- Legislazione Regionale Umbria

Fetches regional laws (leggi regionali) of the Umbria Region from the
Assemblea Legislativa dell'Umbria legislative database (leggi.crumbria.it).

Strategy:
  - Each law is published as a NIR (Norme in Rete) XML document at a stable
    URL:
        http://leggi.crumbria.it/xml/lr{YYYY}-{NN}.xml
    where YYYY is the year and NN the sequential law number within the year.
  - The full consolidated ("multivigente") text lives in the <articolato>
    element; the <intestazione> carries the header. We strip the <meta> and
    <modifichepassive> metadata blocks and extract clean flowing text.
  - Metadata (title, number, date, URN, status) is read from the NIR header
    elements (titoloDoc, numDoc, dataDoc[@norm], urn[@valore]).
  - We enumerate year x number; a run of consecutive 404s within a year ends
    that year. Coverage: 1971-present, ~1400+ regional laws.

The XML is declared iso-8859-15; we decode accordingly.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch laws for the last 2 years
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

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.Umbria")

BASE_URL = "http://leggi.crumbria.it"
XML_URL = BASE_URL + "/xml/lr{year}-{num:02d}.xml"
VIEW_URL = BASE_URL + "/mostra_atto.php?file=lr{year}-{num:02d}.xml"

FIRST_YEAR = 1971
LAST_YEAR = datetime.now(timezone.utc).year
# Max law number to probe within a year (Umbria rarely exceeds ~60/year).
MAX_NUM = 99
# Consecutive 404s within a year before moving to the next year.
YEAR_MISS_LIMIT = 6


class UmbriaScraper(BaseScraper):
    SOURCE_ID = "IT/Umbria"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
            "Accept": "application/xml,text/xml,*/*",
        })

    def _get(self, url: str) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=60, allow_redirects=True, verify=False)
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                if attempt == 2:
                    logger.warning("GET failed for %s: %s", url, e)
                    return None
                time.sleep(2 ** attempt)
        return None

    @staticmethod
    def _decode(resp: requests.Response) -> str:
        """NIR files are iso-8859-15; honour the XML declaration if present."""
        raw = resp.content
        enc = "iso-8859-15"
        m = re.search(rb'encoding="([^"]+)"', raw[:120])
        if m:
            enc = m.group(1).decode("ascii", "ignore")
        try:
            return raw.decode(enc, errors="replace")
        except LookupError:
            return raw.decode("iso-8859-15", errors="replace")

    @staticmethod
    def _clean(text: str) -> str:
        text = html_mod.unescape(text)
        text = text.replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @staticmethod
    def _meta_text(xml: str, tag: str) -> Optional[str]:
        m = re.search(r"<%s\b[^>]*>(.*?)</%s>" % (tag, tag), xml, re.S)
        if not m:
            return None
        val = re.sub(r"<[^>]+>", " ", m.group(1))
        val = html_mod.unescape(re.sub(r"\s+", " ", val)).strip()
        return val or None

    ITALIAN_MONTHS = {
        "gennaio": "01", "febbraio": "02", "marzo": "03", "aprile": "04",
        "maggio": "05", "giugno": "06", "luglio": "07", "agosto": "08",
        "settembre": "09", "ottobre": "10", "novembre": "11", "dicembre": "12",
    }

    def _parse(self, xml: str, year: int, num: int) -> Optional[Dict[str, Any]]:
        # Full body: drop metadata blocks, keep intestazione + articolato.
        body_src = re.sub(r"<meta>.*?</meta>", " ", xml, flags=re.S)
        body_src = re.sub(r"<modifichepassive>.*?</modifichepassive>", " ", body_src, flags=re.S)
        soup = BeautifulSoup(body_src, "html.parser")
        full_text = self._clean(soup.get_text(" "))
        if len(full_text) < 80:
            return None

        title = self._meta_text(xml, "titoloDoc") or ""
        tipo = (self._meta_text(xml, "tipoDoc") or "LEGGE REGIONALE").title()
        numdoc = self._meta_text(xml, "numDoc") or str(num)

        # Date: prefer dataDoc @norm (YYYYMMDD); fall back to its text.
        date_iso = None
        dm = re.search(r"<dataDoc\b[^>]*\bnorm=\"(\d{8})\"", xml)
        if dm:
            d = dm.group(1)
            date_iso = f"{d[0:4]}-{d[4:6]}-{d[6:8]}"
        else:
            dtxt = self._meta_text(xml, "dataDoc")
            if dtxt:
                mm = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})", dtxt)
                if mm:
                    mon = self.ITALIAN_MONTHS.get(mm.group(2).lower())
                    if mon:
                        date_iso = f"{mm.group(3)}-{mon}-{int(mm.group(1)):02d}"

        urn = None
        um = re.search(r"<urn\b[^>]*\bvalore=\"([^\"]+)\"", xml)
        if um:
            urn = um.group(1)

        status = None
        sm = re.search(r"<LeggeRegionale\b[^>]*\bstatus=\"([^\"]+)\"", xml)
        if sm:
            status = sm.group(1)

        return {
            "year": year,
            "num": numdoc,
            "tipo": tipo,
            "title": title,
            "date": date_iso,
            "urn": urn,
            "status": status,
            "text": full_text,
            "url": VIEW_URL.format(year=year, num=num),
        }

    def _fetch_law(self, year: int, num: int) -> Optional[Dict[str, Any]]:
        resp = self._get(XML_URL.format(year=year, num=num))
        if resp is None or len(resp.content) < 300:
            return None
        return self._parse(self._decode(resp), year, num)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        year = raw["year"]
        num = raw["num"]
        _id = f"IT/Umbria/LR-{year}-{num}"
        law_ref = f"L.R. Umbria {num}/{year}"

        title = raw.get("title", "").strip()
        full_title = f"{law_ref} — {title}" if title else law_ref

        return {
            "_id": _id,
            "_source": "IT/Umbria",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": full_title,
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url"),
            "law_number": law_ref,
            "urn": raw.get("urn"),
            "status": raw.get("status"),
        }

    def _iter_years(self, years) -> Generator[Dict[str, Any], None, None]:
        for year in years:
            misses = 0
            found_any = False
            for num in range(1, MAX_NUM + 1):
                try:
                    raw = self._fetch_law(year, num)
                except Exception as e:
                    logger.warning("Error %d-%d: %s", year, num, e)
                    raw = None

                if raw is None:
                    misses += 1
                    if misses >= YEAR_MISS_LIMIT and found_any:
                        break
                    if misses >= YEAR_MISS_LIMIT and not found_any and num >= YEAR_MISS_LIMIT:
                        break
                    continue

                misses = 0
                found_any = True
                yield raw
                time.sleep(1.0)
            logger.info("Year %d done", year)

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        yield from self._iter_years(range(FIRST_YEAR, LAST_YEAR + 1))

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        start = LAST_YEAR - 1
        try:
            if since and since[:4].isdigit():
                start = int(since[:4])
        except (ValueError, AttributeError):
            pass
        yield from self._iter_years(range(start, LAST_YEAR + 1))

    def test(self) -> bool:
        try:
            raw = self._fetch_law(2023, 1)
            return bool(raw and len(raw.get("text", "")) > 1000 and raw.get("date"))
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import warnings
    warnings.filterwarnings("ignore")
    try:
        import urllib3
        urllib3.disable_warnings()
    except Exception:
        pass

    scraper = UmbriaScraper()

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

        # In sample mode, start from a recent year so we capture rich,
        # in-force consolidated texts rather than short abrogated stubs.
        years = range(2023, LAST_YEAR + 1) if sample_mode else range(FIRST_YEAR, LAST_YEAR + 1)
        for raw in scraper._iter_years(years):
            record = scraper.normalize(raw)
            out_path = sample_dir / f"{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info("[%d] %s — %d chars", count, record["law_number"], len(record.get("text", "")))
            if count >= max_records:
                break

        logger.info("Done: %d records saved to %s", count, sample_dir)

    elif command == "update":
        since = sys.argv[2] if len(sys.argv) > 2 else str(LAST_YEAR - 1)
        count = 0
        for raw in scraper.fetch_updates(since):
            record = scraper.normalize(raw)
            count += 1
            logger.info("[%d] %s", count, record["law_number"])
        logger.info("Update done: %d records", count)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
