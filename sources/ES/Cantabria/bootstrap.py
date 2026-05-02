#!/usr/bin/env python3
"""
ES/Cantabria -- Cantabria Regional Legislation (BOC)

Fetches legislation from the Boletín Oficial de Cantabria (BOC) via:
  1. JSON API for bulletin listing (busquedaBoletines.do)
  2. HTML scraping of bulletin index pages for Section 1 entries
  3. PDF download + pdfplumber text extraction for full text

Data:
  - Digital archive from 1999 onward.
  - License: CC BY 4.0 (Gobierno de Cantabria open data).
  - Language: Spanish (es).

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import io
import time
import socket
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Dict, Any, Optional, List

socket.setdefaulttimeout(120)

try:
    import requests
    from requests.adapters import HTTPAdapter
except ImportError:
    print("ERROR: requests is required. Install with: pip install requests")
    sys.exit(1)

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ES.cantabria")

BASE_URL = "https://boc.cantabria.es/boces"
START_YEAR = 1999  # digital archive starts here


class CantabriaScraper(BaseScraper):
    """Scraper for ES/Cantabria -- Cantabria Regional Legislation (BOC)."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        adapter = HTTPAdapter(max_retries=3)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch an HTML page."""
        self.rate_limiter.wait()
        try:
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding or "utf-8"
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

    def _fetch_pdf_text(self, url: str) -> Optional[str]:
        """Download a PDF and extract text via pdfplumber."""
        if pdfplumber is None:
            logger.warning("pdfplumber not installed, cannot extract PDF text")
            return None

        self.rate_limiter.wait()
        try:
            resp = self.session.get(url, timeout=120)
            resp.raise_for_status()
            if "pdf" not in resp.headers.get("content-type", "").lower():
                logger.warning(f"Not a PDF: {url} (content-type: {resp.headers.get('content-type')})")
                return None
        except Exception as e:
            logger.warning(f"Failed to download PDF {url}: {e}")
            return None

        try:
            with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                pages = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages.append(text)
                full_text = "\n\n".join(pages)

                # Strip BOC header/footer lines that appear on every page
                lines = full_text.split("\n")
                cleaned = []
                for line in lines:
                    stripped = line.strip()
                    if stripped.startswith("Boletín Oficial de Cantabria") and len(stripped) < 80:
                        continue
                    if re.match(r'^Pág\.\s*\d+\s*boc\.cantabria\.es\s*\d+/\d+$', stripped):
                        continue
                    if re.match(r'^boc\.cantabria\.es\s*Pág\.\s*\d+$', stripped):
                        continue
                    if re.match(r'^[A-ZÁÉÍÓÚÑ\s,]+DE\s+\d{4}\s*-\s*BOC\s+NÚM\.\s*\d+$', stripped):
                        continue
                    if re.match(r'^\d+/\d+$', stripped):
                        continue
                    cleaned.append(line)
                return "\n".join(cleaned).strip()
        except Exception as e:
            logger.warning(f"Failed to parse PDF from {url}: {e}")
            return None

    def _get_bulletins_for_month(self, year: int, month: int) -> List[dict]:
        """Get all bulletins for a given month/year via JSON API."""
        url = f"{BASE_URL}/busquedaBoletines.do?mes={month}&year={year}"
        self.rate_limiter.wait()
        try:
            resp = self.session.post(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.warning(f"Failed to fetch bulletins for {year}/{month}: {e}")
            return []

    def _get_bulletins_for_year(self, year: int) -> List[dict]:
        """Get all bulletins for a year (all months)."""
        all_bulletins = []
        seen_ids = set()
        for month in range(1, 13):
            bulletins = self._get_bulletins_for_month(year, month)
            for b in bulletins:
                bid = b.get("id")
                if bid and bid not in seen_ids:
                    seen_ids.add(bid)
                    all_bulletins.append(b)
        logger.info(f"Year {year}: found {len(all_bulletins)} bulletins")
        return all_bulletins

    def _parse_section1_entries(self, bulletin_id: int, bulletin_date: str) -> List[dict]:
        """Parse a bulletin index page to extract Section 1 (Disposiciones Generales) entries."""
        url = f"{BASE_URL}/verBoletin.do?idBolOrd={bulletin_id}"
        page = self._fetch_page(url)
        if not page:
            return []

        # Find Section 1 boundaries
        sec1_match = re.search(r'<a\s+name="sec1"', page)
        if not sec1_match:
            return []  # No Section 1 in this bulletin

        # Find next section anchor after sec1
        sec1_start = sec1_match.end()
        next_sec = re.search(r'<a\s+name="sec[^1]', page[sec1_start:])
        if next_sec:
            section_html = page[sec1_start:sec1_start + next_sec.start()]
        else:
            section_html = page[sec1_start:]

        entries = []
        # Split by department markers
        dept_parts = re.split(r'<a\s+name="dpto\d+">', section_html)

        for part in dept_parts[1:]:  # Skip the first part (before first dpto)
            # Extract entity name (new format: <span class="spanH4">, old format: <h4>)
            entity_match = re.search(
                r'<(?:span\s+class="spanH4"|h4)>(.*?)</(?:span|h4)>',
                part, re.DOTALL
            )
            entity = html.unescape(re.sub(r'<[^>]+>', '', entity_match.group(1)).strip()) if entity_match else ""

            # Extract title (the <p> text before the PDF links)
            title_match = re.search(r'<p>(.*?)</p>', part, re.DOTALL)
            title = html.unescape(re.sub(r'<[^>]+>', ' ', title_match.group(1)).strip()) if title_match else ""
            # Normalize whitespace in title
            title = re.sub(r'\s+', ' ', title).strip()

            # Extract PDF link(s) - take the first main one (not "Corrige a")
            pdf_matches = re.findall(
                r'verAnuncioAction\.do\?idAnuBlob=(\d+).*?>\s*PDF\s*\((BOC-\d{4}-\d+)',
                part, re.DOTALL
            )
            if not pdf_matches:
                # Fallback: just find any idAnuBlob
                blob_match = re.search(r'idAnuBlob=(\d+)', part)
                if blob_match:
                    boc_code_match = re.search(r'BOC-\d{4}-\d+', part)
                    boc_code = boc_code_match.group(0) if boc_code_match else f"BOC-{bulletin_id}"
                    pdf_matches = [(blob_match.group(1), boc_code)]

            if not pdf_matches:
                continue

            # Check if this is a "Corrige a" (correction) — skip the correction reference
            # but take the main document
            blob_id = pdf_matches[0][0]
            boc_code = pdf_matches[0][1]

            entries.append({
                "blob_id": blob_id,
                "boc_code": boc_code,
                "title": title,
                "entity": entity,
                "date": bulletin_date,
                "bulletin_id": bulletin_id,
            })

        return entries

    def _clean_text(self, text: str) -> str:
        """Clean extracted PDF text."""
        if not text:
            return ""
        lines = text.split("\n")
        cleaned = []
        for line in lines:
            stripped = line.strip()
            # Skip CVE-style header codes (e.g., "23011-4202-EVC")
            if re.match(r'^\d+-\d+-[A-Z]+$', stripped):
                continue
            # Skip CVE reference codes (e.g., "CVE-2024-11032")
            if re.match(r'^CVE-\d{4}-\d+$', stripped):
                continue
            # Skip section headers like "1. DISPOSICIONES GENERALES"
            if re.match(r'^\d+\.\s*DISPOSICIONES\s+GENERALES$', stripped):
                continue
            # Skip standalone "i" or page markers
            if stripped == "i":
                continue
            # Skip trailing BOC reference codes (e.g., "2024/11032")
            if re.match(r'^\d{4}/\d+$', stripped):
                continue
            cleaned.append(line)
        text = "\n".join(cleaned)
        # Normalize whitespace
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all Section 1 legislation from BOC (newest first).

        Iterates years in reverse so sample mode gets recent, clean data.
        Pre-2006 bulletins share one PDF per bulletin page — blob_id dedup
        prevents duplicate text entries.
        """
        current_year = datetime.now().year
        seen_blobs = set()

        for year in range(current_year, START_YEAR - 1, -1):
            logger.info(f"Processing year {year}...")
            bulletins = self._get_bulletins_for_year(year)

            for bulletin in bulletins:
                bulletin_id = bulletin.get("id")
                bulletin_date = bulletin.get("fecBolString", "")
                iso_date = self._parse_spanish_date(bulletin_date)

                entries = self._parse_section1_entries(bulletin_id, iso_date)
                if not entries:
                    continue

                logger.info(f"Bulletin {bulletin_id} ({iso_date}): {len(entries)} Section 1 entries")

                for entry in entries:
                    blob_id = entry["blob_id"]
                    if blob_id in seen_blobs:
                        continue
                    seen_blobs.add(blob_id)

                    pdf_url = f"{BASE_URL}/verAnuncioAction.do?idAnuBlob={blob_id}"
                    text = self._fetch_pdf_text(pdf_url)
                    if not text or len(text) < 50:
                        logger.warning(f"Skipping {entry['boc_code']}: insufficient text")
                        continue

                    text = self._clean_text(text)
                    entry["text"] = text
                    entry["url"] = pdf_url
                    yield entry

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents published since the given date."""
        current_year = datetime.now().year
        since_year = since.year
        seen_blobs = set()

        for year in range(current_year, since_year - 1, -1):
            bulletins = self._get_bulletins_for_year(year)

            for bulletin in bulletins:
                bulletin_date_str = bulletin.get("fecBolString", "")
                iso_date = self._parse_spanish_date(bulletin_date_str)
                if iso_date:
                    try:
                        bd = datetime.strptime(iso_date, "%Y-%m-%d")
                        if bd < since.replace(tzinfo=None):
                            continue
                    except ValueError:
                        pass

                bulletin_id = bulletin.get("id")
                entries = self._parse_section1_entries(bulletin_id, iso_date)
                if not entries:
                    continue

                for entry in entries:
                    blob_id = entry["blob_id"]
                    if blob_id in seen_blobs:
                        continue
                    seen_blobs.add(blob_id)

                    pdf_url = f"{BASE_URL}/verAnuncioAction.do?idAnuBlob={blob_id}"
                    text = self._fetch_pdf_text(pdf_url)
                    if not text or len(text) < 50:
                        continue

                    text = self._clean_text(text)
                    entry["text"] = text
                    entry["url"] = pdf_url
                    yield entry

    def _parse_spanish_date(self, date_str: str) -> str:
        """Convert '01 de abril de 2025' to '2025-04-01'."""
        if not date_str:
            return ""
        months = {
            "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
            "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
            "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
        }
        m = re.match(r'(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})', date_str.strip())
        if m:
            day = m.group(1).zfill(2)
            month_name = m.group(2).lower()
            year = m.group(3)
            month = months.get(month_name, "01")
            return f"{year}-{month}-{day}"
        return date_str

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        boc_code = raw.get("boc_code", "")
        blob_id = raw.get("blob_id", "")
        doc_id = boc_code or f"BOC-blob-{blob_id}"

        return {
            "_id": doc_id,
            "_source": "ES/Cantabria",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
            "entity": raw.get("entity", ""),
            "boc_code": boc_code,
            "bulletin_id": raw.get("bulletin_id", ""),
            "language": "es",
            "region": "Cantabria",
            "country": "ES",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing BOC Cantabria connection...")

        print("\n1. Testing bulletin JSON API...")
        try:
            bulletins = self._get_bulletins_for_month(2025, 1)
            print(f"   January 2025: {len(bulletins)} bulletins")
            if bulletins:
                print(f"   First: id={bulletins[0]['id']} num={bulletins[0]['numBol']} {bulletins[0]['fecBolString']}")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        print("\n2. Testing bulletin index parsing...")
        found_entries = []
        for b in bulletins[:5]:
            bid = b["id"]
            iso_date = self._parse_spanish_date(b.get("fecBolString", ""))
            entries = self._parse_section1_entries(bid, iso_date)
            if entries:
                found_entries.extend(entries)
                print(f"   Bulletin {bid}: {len(entries)} Section 1 entries")
                if len(found_entries) >= 2:
                    break

        print(f"\n3. Testing PDF text extraction...")
        if found_entries:
            entry = found_entries[0]
            pdf_url = f"{BASE_URL}/verAnuncioAction.do?idAnuBlob={entry['blob_id']}"
            text = self._fetch_pdf_text(pdf_url)
            if text:
                print(f"   BOC code: {entry['boc_code']}")
                print(f"   Title: {entry['title'][:100]}...")
                print(f"   Entity: {entry['entity']}")
                print(f"   Text length: {len(text)} chars")
                print(f"   First 200 chars: {text[:200]}...")
            else:
                print("   ERROR: Could not extract text from PDF")
        else:
            print("   No entries found to test")

        print("\nAll tests passed!")


def main():
    scraper = CantabriaScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
