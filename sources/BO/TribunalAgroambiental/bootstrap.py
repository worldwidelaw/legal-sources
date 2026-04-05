#!/usr/bin/env python3
"""
BO/TribunalAgroambiental -- Bolivia Tribunal Agroambiental

Fetches environmental/agrarian court decisions from Bolivia's Tribunal
Agroambiental via HTML scraping of the Yii2-based "Arbol de Jurisprudencia"
application.

Strategy:
  - Paginate listing at /index.php?r=viewresolucion%2Flistaresoluciones
  - Extract resolution IDs from data-key attributes on <tr> elements
  - Fetch full text from /index.php?r=ficha%2Fverresolucionta&id={ID}
  - Parse metadata from listing table cells

Data: ~6,910 resolutions (2011-2024). SAN, AAP, AID, SAP decision types.
License: Open data (public court decisions).
Rate limit: 1 req/sec (self-imposed).
SSL: Self-signed certificate (verify disabled).

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py update --since DATE  # Fetch recent decisions
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
import urllib3
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html.parser import HTMLParser

# Suppress SSL warnings for self-signed cert
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BO.TribunalAgroambiental")

BASE_URL = "https://arbol.tribunalagroambiental.bo"
LIST_URL = f"{BASE_URL}/index.php"
DETAIL_URL = f"{BASE_URL}/index.php"


class _HTMLTextExtractor(HTMLParser):
    """Strip HTML tags and extract plain text."""

    def __init__(self):
        super().__init__()
        self._parts = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "head"):
            self._skip = True
        if tag in ("br", "p", "div", "li", "tr", "h1", "h2", "h3", "h4", "h5", "h6"):
            self._parts.append("\n")

    def handle_endtag(self, tag):
        if tag in ("script", "style", "head"):
            self._skip = False
        if tag in ("p", "div", "li", "tr"):
            self._parts.append("\n")

    def handle_data(self, data):
        if not self._skip:
            self._parts.append(data)

    def get_text(self):
        return "".join(self._parts)


def strip_html(html_str: str) -> str:
    """Remove HTML tags and return clean plain text."""
    if not html_str:
        return ""
    extractor = _HTMLTextExtractor()
    try:
        extractor.feed(html_str)
        text = extractor.get_text()
    except Exception:
        text = re.sub(r"<[^>]+>", " ", html_str)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&quot;", '"', text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    lines = [line.strip() for line in text.split("\n")]
    text = "\n".join(lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class TribunalAgroambientalScraper(BaseScraper):
    """
    Scraper for BO/TribunalAgroambiental -- Bolivia Environmental/Agrarian Court.
    Country: BO
    URL: https://arbol.tribunalagroambiental.bo/

    Data types: case_law
    Auth: none (public data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html, application/xhtml+xml, */*",
            },
            timeout=30,
            verify=False,
        )

    # -- Listing page parsing --------------------------------------------------

    def _fetch_listing_page(self, page: int, per_page: int = 50) -> list[dict]:
        """Fetch one page of the resolution listing. Returns list of {id, resolucion, nro, gestion, tipo}."""
        params = {
            "r": "viewresolucion/listaresoluciones",
            "page": page,
            "per-page": per_page,
        }
        try:
            resp = self.client.get(LIST_URL, params=params, timeout=30)
            if resp is None or resp.status_code != 200:
                logger.warning(f"Listing page {page} returned status {resp.status_code if resp else 'None'}")
                return []
            return self._parse_listing_html(resp.text)
        except Exception as e:
            logger.warning(f"Listing page {page} failed: {e}")
            return []

    @staticmethod
    def _parse_listing_html(html: str) -> list[dict]:
        """Parse the listing HTML table to extract resolution metadata."""
        results = []

        # Extract data-key IDs from <tr> elements
        tr_pattern = re.compile(r'<tr[^>]*data-key="(\d+)"[^>]*>(.*?)</tr>', re.DOTALL)
        td_pattern = re.compile(r'<td[^>]*>(.*?)</td>', re.DOTALL)

        for tr_match in tr_pattern.finditer(html):
            res_id = int(tr_match.group(1))
            row_html = tr_match.group(2)

            cells = td_pattern.findall(row_html)
            if len(cells) >= 5:
                # Cells: [#, Resolución, Nro. Resolución, Gestión, Tipo, Liquidación, Actions]
                resolucion = re.sub(r'<[^>]+>', '', cells[1]).strip()
                nro = re.sub(r'<[^>]+>', '', cells[2]).strip()
                gestion = re.sub(r'<[^>]+>', '', cells[3]).strip()
                tipo = re.sub(r'<[^>]+>', '', cells[4]).strip()

                results.append({
                    "id": res_id,
                    "resolucion": resolucion,
                    "nro": nro,
                    "gestion": gestion,
                    "tipo": tipo,
                })

        return results

    @staticmethod
    def _count_total_pages(html: str) -> int:
        """Extract total page count from pagination links."""
        # Look for last page link: page=N in pagination
        pages = re.findall(r'[?&]page=(\d+)', html)
        if pages:
            return max(int(p) for p in pages)
        return 1

    # -- Full text fetching ----------------------------------------------------

    def _fetch_full_text(self, res_id: int) -> Optional[str]:
        """Fetch full text of a resolution by ID."""
        params = {
            "r": "ficha/verresolucionta",
            "id": res_id,
        }
        try:
            resp = self.client.get(DETAIL_URL, params=params, timeout=30)
            if resp is None or resp.status_code != 200:
                return None
            return self._extract_body_text(resp.text)
        except Exception as e:
            logger.debug(f"Full text fetch failed for ID {res_id}: {e}")
            return None

    @staticmethod
    def _extract_body_text(html: str) -> Optional[str]:
        """Extract the main text content from the detail page HTML."""
        # The full text is in the main content area. Try to find the main
        # content div or just strip all navigation/headers.

        # Remove header, nav, footer sections
        html = re.sub(r'<nav[^>]*>.*?</nav>', '', html, flags=re.DOTALL)
        html = re.sub(r'<header[^>]*>.*?</header>', '', html, flags=re.DOTALL)
        html = re.sub(r'<footer[^>]*>.*?</footer>', '', html, flags=re.DOTALL)

        # Try to find a main content container
        # Common Yii2 patterns: <div class="site-content">, <div class="container">
        content_match = re.search(
            r'<div[^>]*class="[^"]*(?:site-content|content-wrapper|panel-body)[^"]*"[^>]*>(.*)',
            html, re.DOTALL
        )
        if content_match:
            text_html = content_match.group(1)
        else:
            # Fallback: use everything in <body>
            body_match = re.search(r'<body[^>]*>(.*)</body>', html, re.DOTALL)
            text_html = body_match.group(1) if body_match else html

        text = strip_html(text_html)

        # Remove common UI elements that leak through
        text = re.sub(r'(?:Inicio|Volver|Imprimir|Descargar)\s*', '', text)

        if len(text) > 100:
            return text
        return None

    # -- Record building -------------------------------------------------------

    def _build_record(self, listing_item: dict, text: str) -> dict:
        """Build a raw record from listing metadata + full text."""
        # Try to extract date from the text
        date = self._extract_date(text, listing_item.get("gestion", ""))

        return {
            "id": listing_item["id"],
            "resolucion": listing_item.get("resolucion", ""),
            "nro": listing_item.get("nro", ""),
            "gestion": listing_item.get("gestion", ""),
            "tipo": listing_item.get("tipo", ""),
            "text": text,
            "date": date,
        }

    @staticmethod
    def _extract_date(text: str, gestion: str) -> Optional[str]:
        """Try to extract a date from the resolution text or year."""
        # Look for date patterns in Spanish: "21 de enero de 2024"
        months_es = {
            "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
            "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
            "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
        }
        pattern = r'(\d{1,2})\s+de\s+(' + '|'.join(months_es.keys()) + r')\s+de\s+(\d{4})'
        match = re.search(pattern, text.lower())
        if match:
            day = match.group(1).zfill(2)
            month = months_es[match.group(2)]
            year = match.group(3)
            return f"{year}-{month}-{day}"

        # Look for ISO-style dates
        iso_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', text)
        if iso_match:
            return iso_match.group(0)

        # Fallback to year from gestion
        if gestion and re.match(r'\d{4}$', gestion.strip()):
            return f"{gestion.strip()}-01-01"

        return None

    # -- Core scraper methods --------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all resolutions by paginating through the listing."""
        # First get page 1 to determine total pages
        logger.info("Fetching first listing page to determine pagination...")
        resp = self.client.get(LIST_URL, params={
            "r": "viewresolucion/listaresoluciones",
            "page": 1,
            "per-page": 50,
        }, timeout=30)

        if resp is None or resp.status_code != 200:
            logger.error("Cannot access listing endpoint")
            return

        total_pages = self._count_total_pages(resp.text)
        first_items = self._parse_listing_html(resp.text)
        logger.info(f"Total pages: {total_pages}, first page has {len(first_items)} items")

        total_found = 0

        # Process first page
        for item in first_items:
            self.rate_limiter.wait()
            text = self._fetch_full_text(item["id"])
            if text:
                record = self._build_record(item, text)
                total_found += 1
                if total_found % 50 == 0:
                    logger.info(f"Progress: {total_found} records (page 1/{total_pages})")
                yield record

        # Process remaining pages
        for page in range(2, total_pages + 1):
            self.rate_limiter.wait()
            items = self._fetch_listing_page(page)
            if not items:
                logger.warning(f"Page {page} returned no items, continuing...")
                continue

            for item in items:
                self.rate_limiter.wait()
                text = self._fetch_full_text(item["id"])
                if text:
                    record = self._build_record(item, text)
                    total_found += 1
                    if total_found % 50 == 0:
                        logger.info(f"Progress: {total_found} records (page {page}/{total_pages})")
                    yield record

        logger.info(f"Fetch complete: {total_found} records")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent decisions by scanning pages sorted by year descending."""
        since_year = since.year
        since_str = since.strftime("%Y-%m-%d")
        logger.info(f"Fetching updates since {since.date()}")

        found = 0
        for page in range(1, 200):
            self.rate_limiter.wait()
            items = self._fetch_listing_page(page)
            if not items:
                break

            all_too_old = True
            for item in items:
                gestion = item.get("gestion", "")
                if gestion and gestion.isdigit() and int(gestion) < since_year:
                    continue
                all_too_old = False

                self.rate_limiter.wait()
                text = self._fetch_full_text(item["id"])
                if text:
                    record = self._build_record(item, text)
                    if record.get("date") and record["date"] < since_str:
                        continue
                    found += 1
                    yield record

            if all_too_old:
                logger.info(f"All items on page {page} are before {since.date()}, stopping")
                break

        logger.info(f"Update complete: {found} records")

    def fetch_sample(self, count: int = 15) -> Generator[dict, None, None]:
        """Fetch sample records for validation."""
        found = 0

        # Fetch first page of listing
        logger.info("Fetching sample from listing...")
        items = self._fetch_listing_page(1, per_page=50)
        if not items:
            logger.error("Cannot fetch listing page")
            return

        for item in items:
            if found >= count:
                break
            self.rate_limiter.wait()
            text = self._fetch_full_text(item["id"])
            if text:
                record = self._build_record(item, text)
                found += 1
                logger.info(
                    f"Sample {found}/{count}: {record['resolucion']} "
                    f"({record['date']}) - {len(record['text'])} chars"
                )
                yield record

        logger.info(f"Sample complete: {found} records")

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record to standard schema."""
        title_parts = []
        if raw.get("tipo"):
            title_parts.append(raw["tipo"])
        if raw.get("resolucion"):
            title_parts.append(raw["resolucion"])
        if raw.get("nro"):
            title_parts.append(f"No {raw['nro']}")
        if raw.get("gestion"):
            title_parts.append(f"({raw['gestion']})")
        title = " ".join(title_parts) if title_parts else f"Resolución {raw['id']}"

        return {
            "_id": f"BO-TribunalAgroambiental-{raw['id']}",
            "_source": "BO/TribunalAgroambiental",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": raw.get("date"),
            "url": f"{BASE_URL}/index.php?r=ficha%2Fverresolucionta&id={raw['id']}",
            "resolucion": raw.get("resolucion"),
            "nro": raw.get("nro"),
            "gestion": raw.get("gestion"),
            "tipo": raw.get("tipo"),
        }

    def test_api(self) -> bool:
        """Test connectivity to the Tribunal Agroambiental website."""
        logger.info("Testing BO/TribunalAgroambiental connectivity...")

        # Test listing endpoint
        logger.info("Testing listing endpoint...")
        items = self._fetch_listing_page(1, per_page=5)
        if not items:
            logger.error("Listing endpoint not responding")
            return False
        logger.info(f"Listing OK: {len(items)} items on page 1")

        # Test detail endpoint
        logger.info("Testing detail endpoint...")
        test_id = items[0]["id"]
        text = self._fetch_full_text(test_id)
        if text:
            logger.info(f"Detail OK: ID {test_id} - {len(text)} chars of text")
            return True
        else:
            logger.error(f"Detail endpoint returned no text for ID {test_id}")
            return False


# -- CLI entry point ---------------------------------------------------------

if __name__ == "__main__":
    scraper = TribunalAgroambientalScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample] [--count N] [--since DATE]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        count = 15
        for i, arg in enumerate(sys.argv):
            if arg == "--count" and i + 1 < len(sys.argv):
                count = int(sys.argv[i + 1])

        if sample_mode:
            gen = scraper.fetch_sample(count=count)
        else:
            gen = scraper.fetch_all()

        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in gen:
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1
            logger.info(f"Saved: {out_path.name}")

        logger.info(f"Bootstrap complete: {saved} records saved to {sample_dir}")

    elif command == "update":
        since_str = None
        for i, arg in enumerate(sys.argv):
            if arg == "--since" and i + 1 < len(sys.argv):
                since_str = sys.argv[i + 1]

        if not since_str:
            print("Usage: python bootstrap.py update --since YYYY-MM-DD")
            sys.exit(1)

        since = datetime.strptime(since_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        gen = scraper.fetch_updates(since)

        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in gen:
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1

        logger.info(f"Update complete: {saved} records saved")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
