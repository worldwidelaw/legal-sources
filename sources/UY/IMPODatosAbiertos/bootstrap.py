#!/usr/bin/env python3
"""
UY/IMPODatosAbiertos -- Uruguayan Decrees, Resolutions & Constitution via IMPO Open Data

Fetches full text from IMPO's open data JSON API (?json=true on any document URL).
Laws are already covered by UY/IMPO (via Parlamento); this source covers:
  - Constitution (constitucion/1967-1967)
  - Decrees (decretos/{number}-{year}) — 1964-present
  - Decree-Laws (decretos-ley/{number}-{year})
  - Executive Resolutions (resoluciones/{number}-{year}) — 1975-present

Strategy: iterate by year and sequential document number. Invalid numbers return
an HTML "Acceso no válido" page (not JSON), which we detect and skip.

Data: Public open data (Licencia de Datos Abiertos Uruguay).
Rate limit: 2 sec between requests.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample documents
  python bootstrap.py test-api             # Connectivity test
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
logger = logging.getLogger("legal-data-hunter.UY.IMPODatosAbiertos")

IMPO_BASE = "https://www.impo.com.uy/bases"

# Document type configs: (path_segment, id_prefix, tipo_norma, start_year, end_year)
DOC_TYPES = [
    ("constitucion", "CONST", "constitution", 1967, 1967),
    ("decretos", "DEC", "decree", 1964, 2026),
    ("decretos-ley", "DECLEY", "decree-law", 1964, 1985),
    ("resoluciones", "RES", "resolution", 1975, 2026),
]

# Approximate max document numbers per year for decrees
# (conservative upper bounds to avoid wasting requests)
DECREE_MAX_PER_YEAR = 400
RESOLUTION_MAX_PER_YEAR = 200
DECREE_LEY_MAX_PER_YEAR = 100


def extract_text_from_articulos(articulos: list) -> str:
    """Concatenate all article texts into a single document."""
    parts = []
    for art in articulos:
        header = art.get("titulosArticulo", "")
        title = art.get("tituloArticulo", "")
        text = art.get("textoArticulo", "")
        notes = art.get("notasArticulo", "")

        section_parts = []
        if header:
            clean = strip_html(header)
            if clean:
                section_parts.append(clean)
        if title:
            clean = strip_html(title)
            if clean:
                section_parts.append(clean)
        if text:
            clean = strip_html(text)
            if clean:
                section_parts.append(clean)
        if notes:
            clean = strip_html(notes)
            if clean:
                section_parts.append(f"[Nota: {clean}]")

        if section_parts:
            parts.append("\n".join(section_parts))

    return "\n\n".join(parts)


def strip_html(html_content: str) -> str:
    """Remove HTML tags and decode entities."""
    if not html_content:
        return ""
    content = re.sub(r'<br\s*/?>', '\n', content := html_content, flags=re.IGNORECASE)
    content = re.sub(r'<p[^>]*>', '\n\n', content, flags=re.IGNORECASE)
    content = re.sub(r'</p>', '', content, flags=re.IGNORECASE)
    content = re.sub(r'<[^>]+>', '', content)
    content = html_module.unescape(content)
    content = re.sub(r'[ \t]+', ' ', content)
    content = re.sub(r'\n[ \t]+', '\n', content)
    content = re.sub(r'\n{3,}', '\n\n', content)
    return content.strip()


def parse_impo_date(date_str: str) -> Optional[str]:
    """Convert DD/MM/YYYY to ISO 8601."""
    if not date_str:
        return None
    try:
        parts = date_str.strip().split("/")
        if len(parts) == 3:
            return f"{parts[2]}-{parts[1]}-{parts[0]}"
    except Exception:
        pass
    return None


class IMPODatosAbiertosScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
                "Accept": "application/json, text/html",
            },
            timeout=60,
        )

    def test_api(self):
        """Test connectivity to IMPO JSON API."""
        logger.info("Testing IMPO datos abiertos JSON API...")
        test_url = f"{IMPO_BASE}/constitucion/1967-1967?json=true"
        try:
            resp = self.http.get(test_url)
            logger.info(f"  Status: {resp.status_code}")
            if resp.status_code == 200:
                data = resp.json()
                if "articulos" in data:
                    logger.info(f"  Constitution articles: {len(data['articulos'])}")
                    logger.info("Connectivity test PASSED")
                    return True
            logger.error("Connectivity test FAILED")
            return False
        except Exception as e:
            logger.error(f"Connectivity test FAILED: {e}")
            return False

    def fetch_document(self, path_segment: str, number: int, year: int) -> Optional[dict]:
        """Fetch a single document from IMPO JSON API."""
        url = f"{IMPO_BASE}/{path_segment}/{number}-{year}?json=true"
        try:
            resp = self.http.get(url)
            if resp.status_code != 200:
                return None

            # Check if response is JSON (invalid docs return HTML)
            content_type = resp.headers.get("Content-Type", "")
            if "json" not in content_type and "javascript" not in content_type:
                # Try parsing anyway in case content-type is wrong
                try:
                    return resp.json()
                except Exception:
                    return None

            return resp.json()
        except Exception:
            return None

    def is_valid_document(self, data: dict) -> bool:
        """Check if the JSON response is a valid document (not an error page)."""
        if not data:
            return False
        if "articulos" not in data:
            return False
        if not data["articulos"]:
            return False
        return True

    def normalize(self, raw: dict, path_segment: str, number: int, year: int) -> dict:
        """Normalize a raw IMPO document into standard schema."""
        tipo = raw.get("tipoNorma", path_segment).strip()
        nombre = raw.get("nombreNorma", "").strip()
        title = f"{tipo} {number}/{year}"
        if nombre:
            title = f"{tipo} {number}/{year} - {nombre}"

        text = extract_text_from_articulos(raw.get("articulos", []))

        # Add preamble sections if present
        preamble_parts = []
        if raw.get("vistos"):
            preamble_parts.append(f"VISTOS:\n{strip_html(raw['vistos'])}")
        if raw.get("considerando"):
            preamble_parts.append(f"CONSIDERANDO:\n{strip_html(raw['considerando'])}")
        if raw.get("firmantes"):
            preamble_parts.append(f"FIRMANTES:\n{strip_html(raw['firmantes'])}")

        if preamble_parts:
            text = "\n\n".join(preamble_parts) + "\n\n" + text

        date = parse_impo_date(raw.get("fechaPromulgacion") or raw.get("fechaPublicacion"))
        doc_url = f"https://www.impo.com.uy/bases/{path_segment}/{number}-{year}"

        return {
            "_id": f"UY-IMPO-{path_segment.upper()}-{number}-{year}",
            "_source": "UY/IMPODatosAbiertos",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": doc_url,
            "document_type": tipo,
            "document_number": str(number),
            "year": year,
            "path_segment": path_segment,
            "status": raw.get("leyenda", ""),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decrees, resolutions, decree-laws, and constitution."""
        # Constitution first
        yield from self._fetch_constitution()

        # Decrees
        for year in range(1964, 2027):
            max_num = DECREE_MAX_PER_YEAR
            consecutive_misses = 0
            for num in range(1, max_num + 1):
                time.sleep(2)
                data = self.fetch_document("decretos", num, year)
                if self.is_valid_document(data):
                    consecutive_misses = 0
                    rec = self.normalize(data, "decretos", num, year)
                    if rec["text"] and len(rec["text"]) > 20:
                        yield rec
                else:
                    consecutive_misses += 1
                    if consecutive_misses >= 20:
                        logger.info(f"  20 consecutive misses at decretos/{num}-{year}, moving to next year")
                        break

        # Decree-Laws
        for year in range(1964, 1986):
            consecutive_misses = 0
            for num in range(1, DECREE_LEY_MAX_PER_YEAR + 1):
                time.sleep(2)
                data = self.fetch_document("decretos-ley", num, year)
                if self.is_valid_document(data):
                    consecutive_misses = 0
                    rec = self.normalize(data, "decretos-ley", num, year)
                    if rec["text"] and len(rec["text"]) > 20:
                        yield rec
                else:
                    consecutive_misses += 1
                    if consecutive_misses >= 10:
                        break

        # Resolutions
        for year in range(1975, 2027):
            consecutive_misses = 0
            for num in range(1, RESOLUTION_MAX_PER_YEAR + 1):
                time.sleep(2)
                data = self.fetch_document("resoluciones", num, year)
                if self.is_valid_document(data):
                    consecutive_misses = 0
                    rec = self.normalize(data, "resoluciones", num, year)
                    if rec["text"] and len(rec["text"]) > 20:
                        yield rec
                else:
                    consecutive_misses += 1
                    if consecutive_misses >= 20:
                        break

    def _fetch_constitution(self) -> Generator[dict, None, None]:
        """Fetch the Uruguayan constitution."""
        logger.info("Fetching Constitution...")
        data = self.fetch_document("constitucion", 1967, 1967)
        if self.is_valid_document(data):
            rec = self.normalize(data, "constitucion", 1967, 1967)
            rec["_id"] = "UY-IMPO-CONSTITUCION-1967"
            rec["title"] = "Constitución de la República Oriental del Uruguay (1967)"
            if rec["text"] and len(rec["text"]) > 20:
                yield rec
                logger.info(f"  Constitution: {len(rec['text'])} chars")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch documents updated since a given date. Limited since no search API."""
        # Only check recent years for updates
        current_year = datetime.now().year
        for year in range(current_year - 1, current_year + 1):
            for path_seg, max_num in [("decretos", DECREE_MAX_PER_YEAR), ("resoluciones", RESOLUTION_MAX_PER_YEAR)]:
                consecutive_misses = 0
                for num in range(1, max_num + 1):
                    time.sleep(2)
                    data = self.fetch_document(path_seg, num, year)
                    if self.is_valid_document(data):
                        consecutive_misses = 0
                        rec = self.normalize(data, path_seg, num, year)
                        if rec["text"] and len(rec["text"]) > 20:
                            yield rec
                    else:
                        consecutive_misses += 1
                        if consecutive_misses >= 20:
                            break

    def bootstrap(self, sample: bool = False) -> int:
        """Run the bootstrap process."""
        if sample:
            return self._bootstrap_sample()
        return self._bootstrap_full()

    def _bootstrap_sample(self) -> int:
        """Fetch a sample of ~15 documents across different types."""
        logger.info("=== SAMPLE MODE: Fetching ~15 documents ===")
        records = []

        # 1. Constitution
        logger.info("Fetching Constitution...")
        time.sleep(2)
        data = self.fetch_document("constitucion", 1967, 1967)
        if self.is_valid_document(data):
            rec = self.normalize(data, "constitucion", 1967, 1967)
            rec["_id"] = "UY-IMPO-CONSTITUCION-1967"
            rec["title"] = "Constitución de la República Oriental del Uruguay (1967)"
            if rec["text"] and len(rec["text"]) > 20:
                records.append(rec)
                logger.info(f"  Constitution: {len(rec['text'])} chars")

        # 2. Sample decrees from different years
        sample_decrees = [
            (1, 2024), (2, 2024), (3, 2024),
            (1, 2020), (50, 2020),
            (1, 2015), (100, 2015),
            (1, 2010),
        ]
        logger.info("Fetching sample decrees...")
        for num, year in sample_decrees:
            if len(records) >= 12:
                break
            time.sleep(2)
            data = self.fetch_document("decretos", num, year)
            if self.is_valid_document(data):
                rec = self.normalize(data, "decretos", num, year)
                if rec["text"] and len(rec["text"]) > 20:
                    records.append(rec)
                    logger.info(f"  Decreto {num}/{year}: {len(rec['text'])} chars")

        # 3. Sample resolutions
        sample_resolutions = [
            (1, 2024), (2, 2024), (1, 2020), (1, 2015),
        ]
        logger.info("Fetching sample resolutions...")
        for num, year in sample_resolutions:
            if len(records) >= 15:
                break
            time.sleep(2)
            data = self.fetch_document("resoluciones", num, year)
            if self.is_valid_document(data):
                rec = self.normalize(data, "resoluciones", num, year)
                if rec["text"] and len(rec["text"]) > 20:
                    records.append(rec)
                    logger.info(f"  Resolución {num}/{year}: {len(rec['text'])} chars")

        # 4. Sample decree-law
        logger.info("Fetching sample decree-law...")
        time.sleep(2)
        data = self.fetch_document("decretos-ley", 14990, 1980)
        if self.is_valid_document(data):
            rec = self.normalize(data, "decretos-ley", 14990, 1980)
            if rec["text"] and len(rec["text"]) > 20:
                records.append(rec)
                logger.info(f"  Decreto-Ley 14990/1980: {len(rec['text'])} chars")

        # Save samples
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        for rec in records:
            fname = f"{rec['_id']}.json"
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(rec, f, ensure_ascii=False, indent=2)

        logger.info(f"=== Sample complete: {len(records)} records saved to sample/ ===")
        return len(records)

    def _bootstrap_full(self) -> int:
        """Full bootstrap - fetch all documents."""
        logger.info("=== FULL MODE ===")
        data_dir = Path(__file__).parent / "data"
        data_dir.mkdir(exist_ok=True)
        count = 0
        for rec in self.fetch_all():
            fname = f"{rec['_id']}.json"
            with open(data_dir / fname, "w", encoding="utf-8") as f:
                json.dump(rec, f, ensure_ascii=False, indent=2)
            count += 1
            if count % 100 == 0:
                logger.info(f"  Progress: {count} documents saved")
        logger.info(f"=== Full bootstrap complete: {count} records ===")
        return count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="UY/IMPODatosAbiertos bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 docs)")
    args = parser.parse_args()

    scraper = IMPODatosAbiertosScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        count = scraper.bootstrap(sample=args.sample)
        sys.exit(0 if count > 0 else 1)


if __name__ == "__main__":
    main()
