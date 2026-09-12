#!/usr/bin/env python3
"""
IT/Calabria -- Legislazione Regionale Calabria

Fetches regional laws from the Banche Dati of the Consiglio Regionale della
Calabria.

Strategy:
  - Year-by-year POST search to /BancheDati/Leggi/Leggi
  - Parse HTML response for law metadata and PDF links
  - Download PDF for each law from /bdf/api/BDF?numero=N&anno=YEAR
  - Extract full text using pdfplumber
  - Coverage: 1971-present

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch laws from recent years
  python bootstrap.py test               # Quick connectivity test
"""

import io
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
import pdfplumber
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.Calabria")

BASE_URL = "https://www.consiglioregionale.calabria.it"
SEARCH_URL = f"{BASE_URL}/portale/BancheDati/Leggi/Leggi"
PDF_BASE = f"{BASE_URL}/bdf/api/BDF"

FIRST_YEAR = 1971
CURRENT_YEAR = datetime.now().year


class CalabriaScraper(BaseScraper):
    SOURCE_ID = "IT/Calabria"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
            "Accept": "text/html,application/xhtml+xml",
        })

    def _post(self, url: str, data: Dict) -> requests.Response:
        for attempt in range(3):
            try:
                resp = self.session.post(url, data=data, timeout=60)
                resp.encoding = "utf-8"
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                if attempt == 2:
                    raise
                logger.warning("POST attempt %d failed: %s", attempt + 1, e)
                time.sleep(2 ** attempt)
        raise RuntimeError("unreachable")

    def _get(self, url: str, params: Optional[Dict] = None) -> requests.Response:
        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params, timeout=60)
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                if attempt == 2:
                    raise
                logger.warning("GET attempt %d failed for %s: %s", attempt + 1, url, e)
                time.sleep(2 ** attempt)
        raise RuntimeError("unreachable")

    def _download_pdf(self, number: str, year: int) -> Optional[bytes]:
        url = f"{PDF_BASE}?numero={number}&anno={year}"
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=120)
                resp.raise_for_status()
                ct = resp.headers.get("content-type", "")
                if "pdf" in ct or "application/octet" in ct or len(resp.content) > 1000:
                    # Skip PDFs larger than 10MB to avoid memory/time issues
                    if len(resp.content) > 10 * 1024 * 1024:
                        logger.warning("PDF too large for %s/%s: %d MB, skipping",
                                       year, number, len(resp.content) // (1024 * 1024))
                        return None
                    return resp.content
                logger.warning("Unexpected content type for %s/%s: %s", year, number, ct)
                return None
            except requests.RequestException as e:
                if attempt == 2:
                    logger.error("Failed to download PDF %s/%s: %s", year, number, e)
                    return None
                logger.warning("PDF attempt %d failed for %s/%s: %s", attempt + 1, year, number, e)
                time.sleep(2 ** attempt)
        return None

    def _extract_text_from_pdf(self, pdf_bytes: bytes, max_pages: int = 500) -> str:
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pages = []
                for i, page in enumerate(pdf.pages):
                    if i >= max_pages:
                        logger.warning("PDF truncated at %d pages", max_pages)
                        break
                    text = page.extract_text()
                    if text:
                        pages.append(text)
                    # Release per-page cache to avoid pdfplumber OOM (exit 137, #968)
                    try:
                        page.flush_cache()
                        page.get_textmap.cache_clear()
                    except Exception:
                        pass
                return "\n\n".join(pages)
        except Exception as e:
            logger.error("PDF text extraction failed: %s", e)
            return ""

    def _search_year(self, year: int) -> List[Dict[str, Any]]:
        """Search for all laws in a given year, returning all results at once."""
        # First, POST the search form to set the year
        data = {
            "anno": str(year),
            "numero": "",
            "oggetto": "",
            "modalitaTestoIncluso": "AlmenoUnaParola",
            "modalitaTestoEscluso": "AlmenoUnaParola",
        }
        resp = self._post(SEARCH_URL, data)

        # Then GET with pagerOff to get all results
        resp = self._get(f"{SEARCH_URL}?pagerOff=True")
        return self._parse_search_results(resp.text)

    def _parse_search_results(self, html_content: str) -> List[Dict[str, Any]]:
        """Parse the search results HTML and extract law entries."""
        results = []
        soup = BeautifulSoup(html_content, "html.parser")

        for div in soup.find_all("div", class_="row"):
            strong = div.find("strong")
            if not strong:
                continue
            header = strong.get_text(strip=True)

            # Parse header like "Legge Regionale 23/12/2024, n. 43"
            match = re.match(
                r"Legge\s+Regionale\s+(\d{2}/\d{2}/\d{4}),\s*n\.\s*(\d+)",
                header,
            )
            if not match:
                continue

            date_raw = match.group(1)
            number = match.group(2)

            # Extract title from the BDF link
            bdf_link = div.find("a", href=lambda h: h and "BDF" in h)
            title = bdf_link.get_text(strip=True) if bdf_link else ""

            # Extract BURC reference
            burc = ""
            text_content = div.get_text()
            burc_match = re.search(r"\(BURC[^)]*\)", text_content)
            if burc_match:
                burc = burc_match.group(0)

            # Parse date
            try:
                dt = datetime.strptime(date_raw, "%d/%m/%Y")
                date_iso = dt.strftime("%Y-%m-%d")
                year = dt.year
            except ValueError:
                date_iso = None
                year = None

            results.append({
                "number": number,
                "date_raw": date_raw,
                "date": date_iso,
                "year": year,
                "title": html_mod.unescape(title),
                "burc": burc,
            })

        return results

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        number = raw.get("number", "")
        year = raw.get("year", "")
        date_str = raw.get("date")

        return {
            "_id": f"IT/Calabria/LR-{year}-{number}" if year and number else f"IT/Calabria/{number}",
            "_source": "IT/Calabria",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date_str,
            "url": f"https://www.consiglioregionale.calabria.it/bdf/api/BDF?numero={number}&anno={year}",
            "law_number": f"LR {number}/{year}" if year and number else "",
            "burc": raw.get("burc", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        for year in range(CURRENT_YEAR, FIRST_YEAR - 1, -1):
            logger.info("Searching year %d...", year)
            try:
                entries = self._search_year(year)
            except Exception as e:
                logger.error("Failed to search year %d: %s", year, e)
                continue

            logger.info("Found %d laws for %d", len(entries), year)
            for entry in entries:
                time.sleep(1.5)
                pdf_bytes = self._download_pdf(entry["number"], year)
                if not pdf_bytes:
                    continue
                text = self._extract_text_from_pdf(pdf_bytes)
                if len(text) < 50:
                    logger.warning("Short text for LR %s/%d (%d chars)", entry["number"], year, len(text))
                    continue
                entry["text"] = text
                yield self.normalize(entry)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        try:
            since_year = int(since[:4])
        except (ValueError, IndexError):
            since_year = CURRENT_YEAR - 1

        for year in range(CURRENT_YEAR, since_year - 1, -1):
            logger.info("Updating year %d...", year)
            try:
                entries = self._search_year(year)
            except Exception as e:
                logger.error("Failed to search year %d: %s", year, e)
                continue

            for entry in entries:
                time.sleep(1.5)
                pdf_bytes = self._download_pdf(entry["number"], year)
                if not pdf_bytes:
                    continue
                text = self._extract_text_from_pdf(pdf_bytes)
                if len(text) >= 50:
                    entry["text"] = text
                    yield self.normalize(entry)

    def test(self) -> bool:
        try:
            data = {"anno": str(CURRENT_YEAR), "numero": "", "oggetto": "",
                    "modalitaTestoIncluso": "AlmenoUnaParola",
                    "modalitaTestoEscluso": "AlmenoUnaParola"}
            resp = self._post(SEARCH_URL, data)
            return "Leggi regionali trovate" in resp.text or "Legge Regionale" in resp.text
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = CalabriaScraper()

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
