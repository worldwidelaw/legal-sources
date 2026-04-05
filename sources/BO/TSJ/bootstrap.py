#!/usr/bin/env python3
"""
BO/TSJ -- Bolivia Tribunal Supremo de Justicia - Jurisprudence

Fetches court decisions (Autos Supremos, Sentencias, Resoluciones) from
Bolivia's Supreme Court via the jurisprudencia.tsj.bo JSON API.

Strategy:
  - Enumerate resolution IDs via GET /resoluciones/{id}
  - Full text available in 'contenido' field (plain text)
  - Also 'contenido_html' field (HTML, may be null)
  - Fallback: paginated search via /resoluciones_simple?term=&page={n}

Data: Court decisions from all chambers (Civil, Penal, Social, Plena).
License: Open data (public court decisions).
Rate limit: 1 req/sec (self-imposed).

Usage:
  python bootstrap.py bootstrap            # Full pull (enumerates IDs)
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py update --since DATE  # Fetch decisions after DATE
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html.parser import HTMLParser

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BO.TSJ")

BASE_URL = "https://jurisprudencia.tsj.bo"
DETAIL_URL = f"{BASE_URL}/resoluciones"
SEARCH_URL = f"{BASE_URL}/resoluciones_simple"
JURIS_SEARCH_URL = f"{BASE_URL}/jurisprudencia_simple"
JURIS_DETAIL_URL = f"{BASE_URL}/jurisprudencia"


class _HTMLTextExtractor(HTMLParser):
    """Strip HTML tags and extract plain text."""

    def __init__(self):
        super().__init__()
        self._parts = []

    def handle_data(self, data):
        self._parts.append(data)

    def get_text(self):
        return " ".join(self._parts)


def strip_html(html_str: str) -> str:
    """Remove HTML tags and return plain text."""
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
    text = re.sub(r"\s+", " ", text).strip()
    return text


class TSJScraper(BaseScraper):
    """
    Scraper for BO/TSJ -- Bolivia Supreme Court Jurisprudence.
    Country: BO
    URL: https://jurisprudencia.tsj.bo/

    Data types: case_law
    Auth: none (public data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json, text/html, */*",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": f"{BASE_URL}/",
            },
            timeout=30,
        )

    # -- Detail fetching (by ID) -----------------------------------------------

    def _fetch_resolution(self, res_id: int) -> Optional[dict]:
        """Fetch a single resolution by ID."""
        try:
            resp = self.client.get(f"{DETAIL_URL}/{res_id}", timeout=15)
            if resp is None or resp.status_code != 200:
                return None
            data = resp.json()
            if not data or not isinstance(data, dict):
                return None
            # Must have contenido (full text)
            return data if data.get("id") else None
        except Exception as e:
            logger.debug(f"Resolution fetch failed for ID {res_id}: {e}")
            return None

    # -- Search-based fetching -------------------------------------------------

    def _search_resoluciones(self, term: str = "", page: int = 1) -> Optional[dict]:
        """Search resoluciones with pagination."""
        try:
            resp = self.client.get(
                SEARCH_URL,
                params={"term": term, "page": page},
                timeout=30,
            )
            if resp is None or resp.status_code != 200:
                return None
            data = resp.json()
            if not data:
                return None
            # Handle wrapper: {"status": true, "data": {...}}
            if isinstance(data, dict) and "data" in data:
                return data["data"]
            return data
        except Exception as e:
            logger.debug(f"Search failed for term='{term}' page={page}: {e}")
            return None

    # -- Text extraction -------------------------------------------------------

    @staticmethod
    def _extract_text(doc: dict) -> Optional[str]:
        """Extract full text from a resolution document."""
        # Prefer plain text contenido
        text = doc.get("contenido")
        if text and isinstance(text, str) and len(text.strip()) > 50:
            cleaned = strip_html(text) if "<" in text else text.strip()
            cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
            cleaned = re.sub(r" {2,}", " ", cleaned)
            return cleaned if len(cleaned) > 50 else None

        # Fallback to HTML version
        html = doc.get("contenido_html")
        if html and isinstance(html, str) and len(html.strip()) > 50:
            cleaned = strip_html(html)
            cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
            cleaned = re.sub(r" {2,}", " ", cleaned)
            return cleaned if len(cleaned) > 50 else None

        return None

    # -- Date parsing ----------------------------------------------------------

    @staticmethod
    def _parse_date(doc: dict) -> Optional[str]:
        """Parse fecha_emision to ISO 8601."""
        fecha = doc.get("fecha_emision")
        if not fecha:
            return None
        # Try common formats
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S", "%d-%m-%Y"):
            try:
                dt = datetime.strptime(fecha[:10], fmt)
                return dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                continue
        # If it's already YYYY-MM-DD format
        if re.match(r"\d{4}-\d{2}-\d{2}", str(fecha)):
            return str(fecha)[:10]
        return None

    # -- Build raw record ------------------------------------------------------

    def _build_record(self, doc: dict) -> Optional[dict]:
        """Build a raw record from API response."""
        res_id = doc.get("id")
        if not res_id:
            return None

        text = self._extract_text(doc)
        if not text:
            logger.debug(f"No text for resolution ID {res_id}")
            return None

        return {
            "id": res_id,
            "nro_resolucion": doc.get("nro_resolucion") or "",
            "nro_expediente": doc.get("nro_expediente") or "",
            "tipo_resolucion": doc.get("tipo_resolucion") or "",
            "sala": doc.get("sala") or "",
            "departamento": doc.get("departamento") or "",
            "magistrado": doc.get("magistrado") or "",
            "forma_resolucion": doc.get("forma_resolucion") or "",
            "demandante": doc.get("demandante") or "",
            "demandado": doc.get("demandado") or "",
            "proceso": doc.get("proceso") or "",
            "text": text,
            "date": self._parse_date(doc),
        }

    # -- Core scraper methods --------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all resolutions by enumerating IDs."""
        total_found = 0
        consecutive_empty = 0
        max_consecutive_empty = 500

        # Try to discover max ID via search first
        max_id = self._discover_max_id()
        logger.info(f"Starting ID enumeration from {max_id} down to 1")

        for res_id in range(max_id, 0, -1):
            self.rate_limiter.wait()
            doc = self._fetch_resolution(res_id)

            if not doc:
                consecutive_empty += 1
                if consecutive_empty > max_consecutive_empty:
                    logger.info(f"{max_consecutive_empty} consecutive empty IDs at ID {res_id}, stopping")
                    break
                continue

            consecutive_empty = 0
            record = self._build_record(doc)
            if record:
                total_found += 1
                if total_found % 100 == 0:
                    logger.info(f"Progress: {total_found} records found (at ID {res_id})")
                yield record

        logger.info(f"Enumeration complete: {total_found} records")

    def _discover_max_id(self) -> int:
        """Try to discover the highest resolution ID via search."""
        # Try searching and getting IDs from results
        page_data = self._search_resoluciones(term="", page=1)
        if page_data and isinstance(page_data, dict):
            records = page_data.get("data", [])
            if records and isinstance(records, list):
                max_id = max(r.get("id", 0) for r in records if isinstance(r, dict))
                if max_id > 0:
                    logger.info(f"Discovered max ID from search: {max_id}")
                    return max_id + 100  # Add buffer

        # Fallback: binary search for max ID
        logger.info("Search failed, using binary search for max ID")
        low, high = 1, 100000
        last_found = 1

        while low <= high:
            mid = (low + high) // 2
            self.rate_limiter.wait()
            doc = self._fetch_resolution(mid)
            if doc:
                last_found = mid
                low = mid + 1
            else:
                # Check if this is a gap or past the end
                # Try a few nearby IDs
                found_nearby = False
                for offset in [1, 2, 5, 10]:
                    self.rate_limiter.wait()
                    if self._fetch_resolution(mid + offset):
                        found_nearby = True
                        last_found = mid + offset
                        low = mid + offset + 1
                        break
                if not found_nearby:
                    high = mid - 1

        logger.info(f"Binary search found max ID near: {last_found}")
        return last_found + 100

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent decisions by scanning from high IDs downward."""
        since_str = since.strftime("%Y-%m-%d")
        logger.info(f"Fetching updates since {since.date()}")

        max_id = self._discover_max_id()
        consecutive_empty = 0
        found = 0

        for res_id in range(max_id, 0, -1):
            self.rate_limiter.wait()
            doc = self._fetch_resolution(res_id)

            if not doc:
                consecutive_empty += 1
                if consecutive_empty > 200:
                    logger.info("200 consecutive empty IDs, stopping")
                    break
                continue

            consecutive_empty = 0

            # Check date
            date_str = self._parse_date(doc)
            if date_str and date_str < since_str:
                logger.info(f"Reached decisions before {since.date()}, stopping")
                break

            record = self._build_record(doc)
            if record:
                found += 1
                yield record

        logger.info(f"Update complete: {found} new records")

    def fetch_sample(self, count: int = 15) -> Generator[dict, None, None]:
        """Fetch sample records for validation."""
        found = 0

        # Strategy 1: Try search endpoint
        logger.info("Trying search endpoint for sample...")
        page_data = self._search_resoluciones(term="", page=1)
        if page_data and isinstance(page_data, dict):
            records = page_data.get("data", [])
            if records and isinstance(records, list):
                for item in records:
                    if found >= count:
                        break
                    res_id = item.get("id")
                    if not res_id:
                        continue
                    self.rate_limiter.wait()
                    doc = self._fetch_resolution(res_id)
                    if doc:
                        record = self._build_record(doc)
                        if record:
                            found += 1
                            logger.info(
                                f"Sample {found}/{count}: {record['nro_resolucion']} "
                                f"({record['date']}) - {len(record['text'])} chars"
                            )
                            yield record

        if found >= count:
            logger.info(f"Sample complete: {found} records via search")
            return

        # Strategy 2: Enumerate recent IDs
        logger.info(f"Search yielded {found} records, trying ID enumeration...")
        # Try IDs from high to low
        for start in [50000, 40000, 30000, 20000, 10000, 5000, 1000, 500, 100]:
            if found >= count:
                break
            consecutive_empty = 0
            for res_id in range(start, start - 200, -1):
                if found >= count:
                    break
                if res_id < 1:
                    break
                self.rate_limiter.wait()
                doc = self._fetch_resolution(res_id)
                if not doc:
                    consecutive_empty += 1
                    if consecutive_empty > 30:
                        break
                    continue
                consecutive_empty = 0
                record = self._build_record(doc)
                if record:
                    found += 1
                    logger.info(
                        f"Sample {found}/{count}: ID {res_id} - "
                        f"{record['nro_resolucion']} ({record['date']}) - "
                        f"{len(record['text'])} chars"
                    )
                    yield record

        logger.info(f"Sample complete: {found} records")

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record to standard schema."""
        title_parts = []
        if raw.get("tipo_resolucion"):
            title_parts.append(raw["tipo_resolucion"])
        if raw.get("nro_resolucion"):
            title_parts.append(raw["nro_resolucion"])
        title = " ".join(title_parts) if title_parts else f"Resolución {raw['id']}"

        # Add parties if available
        parties = []
        if raw.get("demandante"):
            parties.append(raw["demandante"])
        if raw.get("demandado"):
            parties.append(raw["demandado"])
        if parties:
            title += f" - {' c/ '.join(parties)}"

        return {
            "_id": f"BO-TSJ-{raw['id']}",
            "_source": "BO/TSJ",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": raw.get("date"),
            "url": f"{DETAIL_URL}/{raw['id']}",
            "nro_resolucion": raw.get("nro_resolucion"),
            "nro_expediente": raw.get("nro_expediente"),
            "tipo_resolucion": raw.get("tipo_resolucion"),
            "sala": raw.get("sala"),
            "departamento": raw.get("departamento"),
            "magistrado": raw.get("magistrado"),
            "forma_resolucion": raw.get("forma_resolucion"),
            "proceso": raw.get("proceso"),
        }

    def test_api(self) -> bool:
        """Test API connectivity."""
        logger.info("Testing BO/TSJ jurisprudencia API...")

        # Test search endpoint
        logger.info("Testing search endpoint...")
        page_data = self._search_resoluciones(term="civil", page=1)
        if page_data and isinstance(page_data, dict):
            total = page_data.get("total", 0)
            records = page_data.get("data", [])
            logger.info(f"Search OK: {total} total results, {len(records)} on page 1")
        else:
            logger.warning("Search endpoint not responding (may be temporary server issue)")

        # Test detail endpoint with a few IDs
        logger.info("Testing detail endpoint...")
        for test_id in [1, 10, 100, 1000, 5000]:
            self.rate_limiter.wait()
            doc = self._fetch_resolution(test_id)
            if doc:
                text = self._extract_text(doc)
                logger.info(
                    f"Detail OK: ID {test_id} - {doc.get('nro_resolucion', 'N/A')} "
                    f"({len(text) if text else 0} chars text)"
                )
                return True

        logger.error("No detail endpoints responding")
        return False


# -- CLI entry point ---------------------------------------------------------

if __name__ == "__main__":
    scraper = TSJScraper()

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
