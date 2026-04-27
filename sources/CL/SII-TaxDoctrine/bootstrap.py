#!/usr/bin/env python3
"""
CL/SII-TaxDoctrine -- Chilean Internal Revenue Service Tax Circulars

Fetches tax circulars (Circulares) from Chile's Servicio de Impuestos Internos.

Strategy:
  - Scrape yearly index pages to discover circular numbers and dates
  - 2013+: Download PDFs, extract text with PyMuPDF
  - 2000-2012: Fetch HTML pages, extract text
  - Normalize into standard schema with full text

Data:
  - Circulares: official SII interpretations and instructions
  - ~50-70 per year, 2000-present
  - Language: Spanish

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Fetch current year only
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
import html as html_module
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CL.SII-TaxDoctrine")

BASE_URL = "https://www.sii.cl"
USER_AGENT = "LegalDataHunter/1.0 (open-data research; https://github.com/worldwidelaw/legal-sources)"

# Year ranges and URL patterns
# New layout (2013+): normativa_legislacion/circulares/{year}/
# Old layout (2000-2012): documentos/circulares/{year}/
NEW_LAYOUT_START = 2013
OLD_LAYOUT_START = 2000

SPANISH_MONTHS = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
    "septiembre": "09", "setiembre": "09", "octubre": "10",
    "noviembre": "11", "diciembre": "12",
}


def _fetch_url(url: str, timeout: int = 30) -> Optional[bytes]:
    """Fetch URL content as bytes. Return None on 404/error."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        resp = urlopen(req, timeout=timeout)
        return resp.read()
    except HTTPError as e:
        if e.code in (404, 410, 403):
            return None
        logger.warning("HTTP %d fetching %s", e.code, url)
        return None
    except (URLError, TimeoutError, OSError) as e:
        logger.warning("Error fetching %s: %s", url, e)
        return None


def _fetch_text(url: str, timeout: int = 30) -> Optional[str]:
    """Fetch URL content as text."""
    data = _fetch_url(url, timeout)
    if data is None:
        return None
    return data.decode("utf-8", errors="replace")


def _extract_text_from_pdf(pdf_bytes: bytes) -> Optional[str]:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="CL/SII-TaxDoctrine",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="doctrine",
    ) or ""

def _strip_html(raw_html: str) -> str:
    """Strip HTML tags and decode entities."""
    text = re.sub(r"<script[^>]*>.*?</script>", "", raw_html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_module.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _parse_date(date_str: str) -> Optional[str]:
    """Parse Spanish date string to ISO 8601."""
    if not date_str:
        return None
    date_str = date_str.strip().lower()
    # Try "DD de MES de YYYY" pattern
    m = re.search(r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})", date_str)
    if m:
        day, month_name, year = m.groups()
        month = SPANISH_MONTHS.get(month_name)
        if month:
            return f"{year}-{month}-{day.zfill(2)}"
    # Try "DD/MM/YYYY" or "DD-MM-YYYY"
    m = re.search(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})", date_str)
    if m:
        day, month, year = m.groups()
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    # Try to extract just year
    m = re.search(r"(\d{4})", date_str)
    if m:
        return f"{m.group(1)}-01-01"
    return None


def _parse_index_page(html_content: str, year: int) -> List[Dict[str, str]]:
    """Parse a circular index page to extract circular entries."""
    entries = []
    # Look for links to circular files
    # Pattern: <a href="circu{N}.pdf">...</a> or <a href="circu{N}.htm">
    pattern = re.compile(
        r'<a[^>]+href=["\']([^"\']*circu(\d+)\.(pdf|htm|html))["\'][^>]*>(.*?)</a>',
        re.IGNORECASE | re.DOTALL
    )
    seen = set()
    for match in pattern.finditer(html_content):
        href, num, ext, link_text = match.groups()
        num = int(num)
        if num in seen:
            continue
        seen.add(num)
        # Extract date from surrounding context
        # Look for date pattern near this link
        pos = match.start()
        context = html_content[max(0, pos - 300):pos + 500]
        date_match = re.search(
            r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})", context, re.IGNORECASE
        )
        date_str = None
        if date_match:
            day, month_name, yr = date_match.groups()
            month = SPANISH_MONTHS.get(month_name.lower())
            if month:
                date_str = f"{yr}-{month}-{day.zfill(2)}"
        if not date_str:
            date_str = f"{year}-01-01"

        entries.append({
            "number": num,
            "year": year,
            "ext": ext.lower(),
            "date": date_str,
            "href": href,
        })

    # Sort by number descending (newest first)
    entries.sort(key=lambda x: x["number"], reverse=True)
    return entries


class SIITaxDoctrineScraper(BaseScraper):
    SOURCE_ID = "CL/SII-TaxDoctrine"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _get_index_url(self, year: int) -> str:
        if year >= NEW_LAYOUT_START:
            return f"{BASE_URL}/normativa_legislacion/circulares/{year}/indcir{year}.htm"
        else:
            return f"{BASE_URL}/documentos/circulares/{year}/indcir{year}.htm"

    def _get_doc_base_url(self, year: int) -> str:
        if year >= NEW_LAYOUT_START:
            return f"{BASE_URL}/normativa_legislacion/circulares/{year}/"
        else:
            return f"{BASE_URL}/documentos/circulares/{year}/"

    def _fetch_circular(self, year: int, number: int, ext: str) -> Optional[Tuple[str, str]]:
        """Fetch a circular and return (text, url). Try PDF first, then HTML."""
        base = self._get_doc_base_url(year)

        # Try PDF first for newer circulars
        if year >= NEW_LAYOUT_START or ext == "pdf":
            pdf_url = f"{base}circu{number}.pdf"
            pdf_bytes = _fetch_url(pdf_url)
            if pdf_bytes:
                text = _extract_text_from_pdf(pdf_bytes)
                if text:
                    return text, pdf_url

        # Try HTML
        for html_ext in ["htm", "html"]:
            htm_url = f"{base}circu{number}.{html_ext}"
            html_content = _fetch_text(htm_url)
            if html_content:
                text = _strip_html(html_content)
                if len(text) > 50:
                    return text, htm_url

        return None

    def _extract_title_from_text(self, text: str) -> str:
        """Extract title/subject from circular text."""
        # Look for MATERIA line
        m = re.search(r"MATERIA\s*[:\.]?\s*(.+?)(?:\n|REF|REFERENCIA)", text, re.IGNORECASE | re.DOTALL)
        if m:
            title = m.group(1).strip()
            title = re.sub(r"\s+", " ", title)
            if len(title) > 20:
                return title[:500]

        # Look for first meaningful paragraph after header
        lines = text.split("\n")
        for line in lines[3:15]:
            line = line.strip()
            if len(line) > 30 and not re.match(r"^(DEPARTAMENTO|SUBDIRECCI|CIRCULAR|SISTEMA|FECHA|REF)", line, re.IGNORECASE):
                return line[:500]

        return f"Circular SII"

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw circular record."""
        year = raw["year"]
        number = raw["number"]
        return {
            "_id": f"CL-SII-CIR-{year}-{number}",
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "circular_id": f"CIR-{year}-{number}",
            "title": raw.get("title", f"Circular N°{number} de {year}"),
            "text": raw["text"],
            "date": raw.get("date", f"{year}-01-01"),
            "url": raw["url"],
            "number": number,
            "year": year,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all circulars from SII. Yields raw dicts."""
        current_year = datetime.now().year
        years = range(current_year, OLD_LAYOUT_START - 1, -1)
        total = 0

        for year in years:
            logger.info("Fetching index for year %d", year)
            index_url = self._get_index_url(year)
            html_content = _fetch_text(index_url)

            if not html_content:
                logger.info("No index page for year %d, skipping", year)
                continue

            entries = _parse_index_page(html_content, year)
            if not entries:
                # Fallback: try sequential numbers 1..80
                logger.info("No entries parsed from index, trying sequential for year %d", year)
                entries = [{"number": n, "year": year, "ext": "pdf" if year >= NEW_LAYOUT_START else "htm", "date": f"{year}-01-01", "href": ""} for n in range(1, 80)]

            logger.info("Year %d: %d entries found", year, len(entries))

            for entry in entries:
                number = entry["number"]
                self.rate_limiter.wait()
                result = self._fetch_circular(year, number, entry.get("ext", "pdf"))
                if result is None:
                    continue

                text, url = result
                title = self._extract_title_from_text(text)

                raw = {
                    "year": year,
                    "number": number,
                    "text": text,
                    "title": title,
                    "date": entry.get("date", f"{year}-01-01"),
                    "url": url,
                }

                total += 1
                logger.info("  [%d] Circular N°%d/%d: %s (%d chars)",
                            total, number, year, title[:60], len(text))
                yield raw

        logger.info("Total circulars fetched: %d", total)

    def fetch_updates(self, since: datetime = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch only current year circulars."""
        current_year = datetime.now().year
        logger.info("Fetching updates for year %d", current_year)

        index_url = self._get_index_url(current_year)
        html_content = _fetch_text(index_url)
        if not html_content:
            return

        entries = _parse_index_page(html_content, current_year)
        for entry in entries:
            self.rate_limiter.wait()
            result = self._fetch_circular(current_year, entry["number"], entry.get("ext", "pdf"))
            if result is None:
                continue
            text, url = result
            title = self._extract_title_from_text(text)
            yield {
                "year": current_year,
                "number": entry["number"],
                "text": text,
                "title": title,
                "date": entry.get("date"),
                "url": url,
            }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CL/SII-TaxDoctrine bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--sample-size", type=int, default=15, help="Sample size")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = SIITaxDoctrineScraper()

    if args.command == "test":
        logger.info("Testing connectivity to SII...")
        url = f"{BASE_URL}/normativa_legislacion/circulares/2024/circu50.pdf"
        data = _fetch_url(url)
        if data and len(data) > 1000:
            logger.info("SUCCESS: SII accessible, file size=%d bytes", len(data))
        else:
            logger.error("FAILED: Could not fetch test document")
            sys.exit(1)

    elif args.command == "bootstrap":
        result = scraper.bootstrap(sample_mode=args.sample, sample_size=args.sample_size)
        logger.info("Bootstrap result: %s", json.dumps(result, indent=2, default=str))

    elif args.command == "update":
        result = scraper.update()
        logger.info("Update result: %s", json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
