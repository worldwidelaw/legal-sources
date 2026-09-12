#!/usr/bin/env python3
"""
IT/Campania -- Legislazione Regionale Campania

Fetches regional laws from the Normativa section of the Regione Campania website.

Strategy:
  - Paginate through listing pages at items.php?pgCode=G19I231&id_doc_type=1
  - Extract item links for each law
  - Fetch individual law pages and extract full text from HTML
  - Coverage: 2001-present (~1,844 laws)

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
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.parse import urljoin, parse_qs, urlparse

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.Campania")

BASE_URL = "https://www.regione.campania.it/normativa"
ITEMS_URL = f"{BASE_URL}/items.php"
ITEM_URL = f"{BASE_URL}/item.php"

# Italian month names for date parsing
ITALIAN_MONTHS = {
    "gennaio": 1, "febbraio": 2, "marzo": 3, "aprile": 4,
    "maggio": 5, "giugno": 6, "luglio": 7, "agosto": 8,
    "settembre": 9, "ottobre": 10, "novembre": 11, "dicembre": 12,
}


class CampaniaScraper(BaseScraper):
    SOURCE_ID = "IT/Campania"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
            "Accept": "text/html,application/xhtml+xml",
        })

    def _get(self, url: str, params: Optional[Dict] = None) -> requests.Response:
        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params, timeout=30)
                resp.encoding = "iso-8859-1"
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                if attempt == 2:
                    raise
                logger.warning("GET attempt %d failed for %s: %s", attempt + 1, url, e)
                time.sleep(2 ** attempt)
        raise RuntimeError("unreachable")

    def _parse_date(self, text: str) -> Optional[str]:
        """Parse Italian date like '27 marzo 2026' to ISO format."""
        match = re.search(
            r"(\d{1,2})\s+(\w+)\s+(\d{4})",
            text,
        )
        if not match:
            return None
        day = int(match.group(1))
        month_name = match.group(2).lower()
        year = int(match.group(3))
        month = ITALIAN_MONTHS.get(month_name)
        if not month:
            return None
        try:
            return datetime(year, month, day).strftime("%Y-%m-%d")
        except ValueError:
            return None

    def _parse_law_header(self, text: str) -> Tuple[Optional[str], Optional[str], Optional[int]]:
        """Parse header like 'Legge Regionale 27 marzo 2026, n. 3.' into (date, number, year)."""
        date_iso = self._parse_date(text)
        num_match = re.search(r"n\.\s*(\d+)", text)
        number = num_match.group(1) if num_match else None
        year_match = re.search(r"(\d{4})", text)
        year = int(year_match.group(1)) if year_match else None
        return date_iso, number, year

    def _get_listing_page(self, page: int) -> List[Dict[str, Any]]:
        """Fetch a listing page and return item metadata."""
        params = {
            "n_pagina": str(page),
            "pgCode": "G19I231",
            "id_doc_type": "1",
            "refresh": "on",
        }
        resp = self._get(ITEMS_URL, params)
        soup = BeautifulSoup(resp.text, "html.parser")

        items = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "item.php" not in href or "pgCode=G19I231R" not in href:
                continue
            link_text = a.get_text(strip=True)
            if not link_text.startswith("Legge Regionale"):
                continue

            # Extract description from blockquote sibling
            description = ""
            li = a.find_parent("li")
            if li:
                bq = li.find("blockquote")
                if bq:
                    description = bq.get_text(strip=True)

            date_iso, number, year = self._parse_law_header(link_text)

            # Extract pgCode from URL
            parsed = urlparse(href)
            qs = parse_qs(parsed.query)
            pg_code = qs.get("pgCode", [None])[0]
            id_tema = qs.get("id_tema", [None])[0]

            items.append({
                "header": link_text,
                "description": description,
                "date": date_iso,
                "number": number,
                "year": year,
                "pg_code": pg_code,
                "id_tema": id_tema,
                "url": href if href.startswith("http") else urljoin(BASE_URL + "/", href),
            })

        return items

    def _get_total_pages(self) -> int:
        """Get the total number of listing pages."""
        params = {
            "n_pagina": "1",
            "pgCode": "G19I231",
            "id_doc_type": "1",
            "refresh": "on",
        }
        resp = self._get(ITEMS_URL, params)
        # Look for last page link (»»)
        soup = BeautifulSoup(resp.text, "html.parser")
        for a in soup.find_all("a", href=True):
            if a.get_text(strip=True) == "»»":
                qs = parse_qs(urlparse(a["href"]).query)
                pages = qs.get("n_pagina", [None])[0]
                if pages:
                    return int(pages)
        return 308  # fallback

    def _fetch_law_text(self, url: str) -> Tuple[str, str, str]:
        """Fetch individual law page, return (title, text, burc_ref)."""
        resp = self._get(url)
        soup = BeautifulSoup(resp.text, "html.parser")

        # Remove nav/script/style
        for tag in soup.find_all(["nav", "script", "style", "header", "footer"]):
            tag.decompose()

        # Get title from h2
        h2 = soup.find("h2")
        title = h2.get_text(strip=True) if h2 else ""

        # Get BURC reference
        burc = ""
        for p in soup.find_all("p"):
            t = p.get_text(strip=True)
            if "Bollettino Ufficiale" in t:
                burc = t
                break

        # Get full text from #document div, or fall back to #main-content
        doc_div = soup.find("div", id="document")
        if not doc_div:
            doc_div = soup.find("div", id="main-content")
        if not doc_div:
            doc_div = soup.find("div", id="content")

        if doc_div:
            text = doc_div.get_text(separator="\n", strip=True)
        else:
            text = ""

        # Clean up text: remove the title line from beginning if duplicated
        if title and text.startswith(title):
            text = text[len(title):].strip()

        return title, text, burc

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        number = raw.get("number", "")
        year = raw.get("year", "")

        law_id = f"IT/Campania/LR-{year}-{number}" if year and number else f"IT/Campania/{raw.get('pg_code', 'unknown')}"

        return {
            "_id": law_id,
            "_source": "IT/Campania",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "law_number": f"LR {number}/{year}" if year and number else "",
            "burc": raw.get("burc", ""),
            "description": raw.get("description", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        total_pages = self._get_total_pages()
        logger.info("Total listing pages: %d", total_pages)

        for page in range(1, total_pages + 1):
            logger.info("Fetching listing page %d/%d...", page, total_pages)
            try:
                items = self._get_listing_page(page)
            except Exception as e:
                logger.error("Failed to fetch listing page %d: %s", page, e)
                continue

            logger.info("Found %d items on page %d", len(items), page)

            for item in items:
                time.sleep(1.5)
                try:
                    title, text, burc = self._fetch_law_text(item["url"])
                except Exception as e:
                    logger.error("Failed to fetch law at %s: %s", item["url"], e)
                    continue

                if len(text) < 50:
                    logger.warning("Short text for %s (%d chars), skipping",
                                   item.get("header", ""), len(text))
                    continue

                item["title"] = title or item.get("header", "")
                item["text"] = text
                item["burc"] = burc
                yield self.normalize(item)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        try:
            since_year = int(since[:4])
        except (ValueError, IndexError):
            since_year = datetime.now().year - 1

        current_year = datetime.now().year
        logger.info("Fetching updates since %d", since_year)

        for page in range(1, 999):
            try:
                items = self._get_listing_page(page)
            except Exception as e:
                logger.error("Failed to fetch listing page %d: %s", page, e)
                break

            if not items:
                break

            # Check if we've gone past the since_year
            page_years = [i.get("year") for i in items if i.get("year")]
            if page_years and max(page_years) < since_year:
                break

            for item in items:
                if item.get("year") and item["year"] < since_year:
                    continue
                time.sleep(1.5)
                try:
                    title, text, burc = self._fetch_law_text(item["url"])
                except Exception as e:
                    logger.error("Failed to fetch law at %s: %s", item["url"], e)
                    continue

                if len(text) >= 50:
                    item["title"] = title or item.get("header", "")
                    item["text"] = text
                    item["burc"] = burc
                    yield self.normalize(item)

    def test(self) -> bool:
        try:
            params = {"n_pagina": "1", "pgCode": "G19I231", "id_doc_type": "1", "refresh": "on"}
            resp = self._get(ITEMS_URL, params)
            return "Legge Regionale" in resp.text
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = CampaniaScraper()

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
