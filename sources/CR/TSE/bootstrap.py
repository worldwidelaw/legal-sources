#!/usr/bin/env python3
"""
CR/TSE -- Costa Rica Tribunal Supremo de Elecciones

Fetches electoral and municipal resolutions from Costa Rica's
Supreme Electoral Tribunal (TSE).

Strategy:
  - Download yearly ZIP archives from tse.go.cr/juris/zip/
  - Electoral resolutions: E-{YEAR}.zip (1946-2026)
  - Municipal resolutions: M-{YEAR}.zip (1975-2026)
  - Extract HTML files and parse full text
  - Browser User-Agent required to bypass Radware bot protection

Data:
  - ~10,000+ electoral resolutions (1946-present)
  - ~5,000+ municipal resolutions (1975-present)
  - Full text in HTML (Word-exported), cleaned to plain text
  - Language: Spanish

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
import zipfile
import time
from pathlib import Path
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Generator, Dict, Any, Optional, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CR.TSE")

BASE_URL = "https://www.tse.go.cr"
ELECTORAL_ZIP_URL = BASE_URL + "/juris/zip/E-{year}.zip"
MUNICIPAL_ZIP_URL = BASE_URL + "/juris/zip/M-{year}.zip"

# Years available (some gaps in early electoral years)
ELECTORAL_YEARS = list(range(2026, 1945, -1))  # 2026 down to 1946
MUNICIPAL_YEARS = list(range(2026, 1974, -1))  # 2026 down to 1975

# Spanish month names for date parsing
SPANISH_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}

# Spanish number words for date parsing
SPANISH_NUMBERS = {
    "uno": 1, "dos": 2, "tres": 3, "cuatro": 4, "cinco": 5,
    "seis": 6, "siete": 7, "ocho": 8, "nueve": 9, "diez": 10,
    "once": 11, "doce": 12, "trece": 13, "catorce": 14, "quince": 15,
    "dieciséis": 16, "diecisiete": 17, "dieciocho": 18, "diecinueve": 19,
    "veinte": 20, "veintiuno": 21, "veintidós": 22, "veintitrés": 23,
    "veinticuatro": 24, "veinticinco": 25, "veintiséis": 26,
    "veintisiete": 27, "veintiocho": 28, "veintinueve": 29,
    "treinta": 30, "treinta y uno": 31,
    "primero": 1, "primer": 1,
}


class _TextExtractor(HTMLParser):
    """Extract plain text from HTML, skipping style/script tags."""

    def __init__(self):
        super().__init__()
        self.parts: List[str] = []
        self.in_body = False
        self.skip = False

    def handle_starttag(self, tag, attrs):
        if tag == "body":
            self.in_body = True
        if tag in ("style", "script"):
            self.skip = True

    def handle_endtag(self, tag):
        if tag in ("style", "script"):
            self.skip = False
        if tag in ("p", "br", "div", "tr", "li"):
            self.parts.append("\n")

    def handle_data(self, data):
        if self.in_body and not self.skip:
            self.parts.append(data)


def _extract_text(html_content: str) -> str:
    """Extract clean plain text from HTML."""
    parser = _TextExtractor()
    parser.feed(html_content)
    text = "".join(parser.parts)
    # Clean up whitespace
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"(\n )+", "\n", text)
    return text.strip()


def _extract_title(html_content: str) -> str:
    """Extract title from HTML <title> tag."""
    m = re.search(r"<title>(.*?)</title>", html_content, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return ""


def _parse_date_from_text(text: str, year: int) -> Optional[str]:
    """Try to extract ISO date from resolution header text."""
    # Pattern: "a las X horas del D de MES de YEAR"
    m = re.search(
        r"del\s+([\w\s]+?)\s+de\s+([\w]+)\s+de\s+dos\s+mil",
        text[:1000],
        re.IGNORECASE,
    )
    if m:
        day_word = m.group(1).strip().lower()
        month_word = m.group(2).strip().lower()
        month = SPANISH_MONTHS.get(month_word)
        day = SPANISH_NUMBERS.get(day_word)
        if not day:
            # Try numeric
            dm = re.match(r"(\d{1,2})", day_word)
            if dm:
                day = int(dm.group(1))
        if month and day and 1 <= day <= 31:
            return f"{year:04d}-{month:02d}-{day:02d}"
    return f"{year:04d}-01-01"


def _parse_filename(filename: str):
    """Parse resolution info from filename like '0001-E1-2024.html'."""
    name = filename.replace(".html", "").replace(".htm", "")
    # Pattern: NUMBER-CATEGORY-YEAR or NUMBER-CATEGORY-SE-YEAR
    m = re.match(r"^(\d+)-([EM]\d+(?:-SE)?)-(\d{4})$", name, re.IGNORECASE)
    if m:
        return {
            "number": m.group(1),
            "category": m.group(2),
            "year": int(m.group(3)),
            "resolution_id": name,
        }
    # Fallback: try simpler pattern
    m = re.match(r"^(\d+)-([EM].*?)-(\d{4})$", name, re.IGNORECASE)
    if m:
        return {
            "number": m.group(1),
            "category": m.group(2),
            "year": int(m.group(3)),
            "resolution_id": name,
        }
    return None


class CostaRicaTSEScraper(BaseScraper):
    """
    Scraper for CR/TSE -- Costa Rica Supreme Electoral Tribunal.
    Country: CR
    URL: https://www.tse.go.cr/

    Data types: case_law
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/zip,application/octet-stream,*/*",
                "Accept-Language": "es-CR,es;q=0.9,en;q=0.5",
            },
            timeout=120,
        )

    def _download_zip(self, url: str) -> Optional[bytes]:
        """Download a ZIP file, returning bytes or None on error."""
        try:
            self.rate_limiter.wait()
            resp = self.client.session.get(url, timeout=120)
            if resp.status_code == 200 and len(resp.content) > 1000:
                # Verify it's actually a ZIP
                if resp.content[:2] == b"PK":
                    return resp.content
                else:
                    logger.debug(f"Response for {url} is not a ZIP file")
                    return None
            logger.debug(f"HTTP {resp.status_code} for {url}")
            return None
        except Exception as e:
            logger.debug(f"Failed to download {url}: {e}")
            return None

    def _process_zip(
        self, zip_data: bytes, res_type: str, year: int
    ) -> Generator[Dict[str, Any], None, None]:
        """Process a ZIP archive and yield normalized records."""
        try:
            zf = zipfile.ZipFile(io.BytesIO(zip_data))
        except zipfile.BadZipFile:
            logger.warning(f"Bad ZIP file for {res_type}-{year}")
            return

        for name in zf.namelist():
            if not name.lower().endswith((".html", ".htm")):
                continue

            try:
                raw = zf.read(name)
                # Try windows-1252 first (most common), fall back to utf-8
                try:
                    html_content = raw.decode("windows-1252")
                except UnicodeDecodeError:
                    html_content = raw.decode("utf-8", errors="replace")

                parsed = _parse_filename(name)
                if not parsed:
                    logger.debug(f"Cannot parse filename: {name}")
                    continue

                title = _extract_title(html_content)
                text = _extract_text(html_content)

                if not text or len(text) < 100:
                    logger.debug(f"Insufficient text in {name}: {len(text)} chars")
                    continue

                date = _parse_date_from_text(text, parsed["year"])
                resolution_id = parsed["resolution_id"]

                record = {
                    "_id": f"CR/TSE/{resolution_id}",
                    "_source": "CR/TSE",
                    "_type": "case_law",
                    "_fetched_at": datetime.now(timezone.utc).isoformat(),
                    "resolution_id": resolution_id,
                    "resolution_number": parsed["number"],
                    "resolution_category": parsed["category"],
                    "resolution_type": res_type,
                    "title": title or f"TSE {resolution_id}",
                    "text": text,
                    "date": date,
                    "year": parsed["year"],
                    "url": f"https://www.tse.go.cr/juris/zip/{res_type}-{year}.zip",
                    "language": "es",
                    "jurisdiction": "CR",
                    "court": "Tribunal Supremo de Elecciones",
                }
                yield record

            except Exception as e:
                logger.warning(f"Error processing {name}: {e}")
                continue

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all electoral and municipal resolutions."""
        total = 0

        # Electoral resolutions
        for year in ELECTORAL_YEARS:
            url = ELECTORAL_ZIP_URL.format(year=year)
            logger.info(f"Downloading electoral resolutions for {year}...")
            zip_data = self._download_zip(url)
            if not zip_data:
                logger.info(f"No electoral data for {year}, skipping")
                continue

            count = 0
            for record in self._process_zip(zip_data, "E", year):
                yield record
                count += 1
                total += 1

            logger.info(f"Electoral {year}: {count} resolutions")
            time.sleep(2)  # Rate limit between ZIP downloads

        # Municipal resolutions
        for year in MUNICIPAL_YEARS:
            url = MUNICIPAL_ZIP_URL.format(year=year)
            logger.info(f"Downloading municipal resolutions for {year}...")
            zip_data = self._download_zip(url)
            if not zip_data:
                logger.info(f"No municipal data for {year}, skipping")
                continue

            count = 0
            for record in self._process_zip(zip_data, "M", year):
                yield record
                count += 1
                total += 1

            logger.info(f"Municipal {year}: {count} resolutions")
            time.sleep(2)

        logger.info(f"Total: {total} resolutions fetched")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch resolutions from current year only."""
        current_year = datetime.now().year
        for res_type, url_template in [
            ("E", ELECTORAL_ZIP_URL),
            ("M", MUNICIPAL_ZIP_URL),
        ]:
            url = url_template.format(year=current_year)
            logger.info(f"Downloading {res_type}-{current_year} for updates...")
            zip_data = self._download_zip(url)
            if zip_data:
                yield from self._process_zip(zip_data, res_type, current_year)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Records are already normalized during fetch."""
        return raw


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = CostaRicaTSEScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap --sample|test]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        logger.info("Testing connectivity to tse.go.cr...")
        url = ELECTORAL_ZIP_URL.format(year=2024)
        zip_data = scraper._download_zip(url)
        if zip_data:
            zf = zipfile.ZipFile(io.BytesIO(zip_data))
            logger.info(
                f"OK: E-2024.zip downloaded ({len(zip_data)} bytes, "
                f"{len(zf.namelist())} files)"
            )
        else:
            logger.error("FAILED: Could not download E-2024.zip")
            sys.exit(1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        if sample_mode:
            # Download just 2024 electoral for sample
            logger.info("Sample mode: downloading E-2024 only...")
            url = ELECTORAL_ZIP_URL.format(year=2024)
            zip_data = scraper._download_zip(url)
            if not zip_data:
                logger.error("Failed to download E-2024.zip")
                sys.exit(1)

            count = 0
            for record in scraper._process_zip(zip_data, "E", 2024):
                out_path = sample_dir / f"{record['resolution_id']}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
                if count >= 15:
                    break

            logger.info(f"Sample: {count} records saved to {sample_dir}")
        else:
            # Full bootstrap
            count = 0
            for record in scraper.fetch_all():
                count += 1
                if count % 500 == 0:
                    logger.info(f"Progress: {count} records...")
            logger.info(f"Bootstrap complete: {count} records")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
