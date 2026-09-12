#!/usr/bin/env python3
"""
IT/Sardegna -- Legislazione Regionale Sardegna

Fetches regional laws from the official Banca dati giuridica of the
Regione Autonoma della Sardegna via its REST API.

Strategy:
  - Paginate through /regional-laws/front-office/search for metadata
  - Download PDF for each law via /front-office/{id}/files/pdf
  - Extract full text using pdfplumber
  - Coverage: 1949-present (~2600 laws)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch laws from recent years
  python bootstrap.py test               # Quick connectivity test
"""

import io
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.Sardegna")

API_BASE = "https://leggiregionali.regione.sardegna.it/regional-laws"
SEARCH_URL = f"{API_BASE}/front-office/search"
DETAIL_URL = f"{API_BASE}/front-office"
PAGE_SIZE = 20


class SardegnaScraper(BaseScraper):
    SOURCE_ID = "IT/Sardegna"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
            "Accept": "application/json",
        })

    def _get_json(self, url: str, params: Optional[Dict] = None) -> Dict:
        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params, timeout=60)
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as e:
                if attempt == 2:
                    raise
                logger.warning("Attempt %d failed for %s: %s", attempt + 1, url, e)
                time.sleep(2 ** attempt)
        raise RuntimeError("unreachable")

    def _download_pdf(self, law_id: str) -> Optional[bytes]:
        url = f"{DETAIL_URL}/{law_id}/files/pdf"
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=120, stream=True)
                resp.raise_for_status()
                return resp.content
            except requests.RequestException as e:
                if attempt == 2:
                    logger.error("Failed to download PDF for %s: %s", law_id, e)
                    return None
                logger.warning("PDF download attempt %d failed for %s: %s", attempt + 1, law_id, e)
                time.sleep(2 ** attempt)
        return None

    def _extract_text_from_pdf(self, pdf_bytes: bytes) -> str:
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pages = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages.append(text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass
                return "\n\n".join(pages)
        except Exception as e:
            logger.error("PDF text extraction failed: %s", e)
            return ""

    def _search_page(self, page: int, size: int = PAGE_SIZE) -> Dict:
        return self._get_json(SEARCH_URL, params={"page": page, "size": size})

    def _format_date(self, date_array: List[int]) -> Optional[str]:
        if not date_array or len(date_array) < 3:
            return None
        try:
            return f"{date_array[0]:04d}-{date_array[1]:02d}-{date_array[2]:02d}"
        except (ValueError, IndexError):
            return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        law_id = raw.get("id", "")
        number = raw.get("number", "")
        date_array = raw.get("date", [])
        date_str = self._format_date(date_array)
        year = date_array[0] if date_array else ""

        return {
            "_id": f"IT/Sardegna/LR-{year}-{number}" if year and number else f"IT/Sardegna/{law_id}",
            "_source": "IT/Sardegna",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date_str,
            "url": f"https://leggiregionali.regione.sardegna.it/legge-regionale?data={date_array[2]}-{date_array[1]}-{date_array[0]}&numero={number}" if date_array and len(date_array) >= 3 and number else "",
            "law_number": f"LR {number}/{year}" if year and number else "",
        }

    def _fetch_law_with_text(self, item: Dict) -> Optional[Dict]:
        law_id = item.get("id", "")
        pdf_bytes = self._download_pdf(law_id)
        if not pdf_bytes:
            return None
        text = self._extract_text_from_pdf(pdf_bytes)
        if len(text) < 50:
            logger.warning("Short/empty text for %s (%d chars)", law_id, len(text))
            return None
        item["text"] = text
        return self.normalize(item)

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        page = 0
        total_pages = None
        while total_pages is None or page < total_pages:
            logger.info("Fetching search page %d/%s...", page, total_pages or "?")
            data = self._search_page(page)
            total_pages = data.get("totalPages", 0)
            items = data.get("content", [])
            if not items:
                break
            for item in items:
                time.sleep(1.5)
                record = self._fetch_law_with_text(item)
                if record:
                    yield record
            page += 1

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        try:
            since_year = int(since[:4])
        except (ValueError, IndexError):
            since_year = datetime.now().year - 1

        page = 0
        while True:
            data = self._search_page(page)
            items = data.get("content", [])
            if not items:
                break
            for item in items:
                date_array = item.get("date", [])
                if date_array and date_array[0] < since_year:
                    return
                time.sleep(1.5)
                record = self._fetch_law_with_text(item)
                if record:
                    yield record
            page += 1

    def test(self) -> bool:
        try:
            data = self._get_json(SEARCH_URL, params={"page": 0, "size": 1})
            total = data.get("totalElements", 0)
            logger.info("API accessible: %d total laws", total)
            return total > 0
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = SardegnaScraper()

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
        since = sys.argv[2] if len(sys.argv) > 2 else str(datetime.now().year - 1)
        count = 0
        for record in scraper.fetch_updates(since):
            count += 1
            logger.info("[%d] %s", count, record.get("law_number", record["_id"]))
        logger.info("Update done: %d records", count)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
