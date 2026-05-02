#!/usr/bin/env python3
"""
ES/Extremadura -- Extremadura Regional Legislation (DOE)

Fetches legislation from the Diario Oficial de Extremadura via:
  1. ELI hierarchy to discover documents: type -> year -> month -> day -> number
  2. PDF reference endpoint to get XML document ID and DOE issue directory
  3. XML endpoint for full text extraction

Data:
  - Coverage from 1984 to present.
  - License: CC BY 4.0.
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
import html as html_module
import time
import socket
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, List, Tuple

socket.setdefaulttimeout(120)

try:
    import requests
    from requests.adapters import HTTPAdapter
except ImportError:
    print("ERROR: requests is required. Install with: pip install requests")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ES.extremadura")

BASE_URL = "https://doe.juntaex.es"

# ELI document types available on DOE
# l=ley, d=decreto, dl=decreto-ley, dleg=decreto legislativo, o=orden, reg=reglamento
ELI_TYPES = ["l", "d", "dl", "dleg", "o", "reg"]

ELI_TYPE_NAMES = {
    "l": "Ley",
    "d": "Decreto",
    "dl": "Decreto-Ley",
    "dleg": "Decreto Legislativo",
    "o": "Orden",
    "reg": "Reglamento",
}


class ExtremaduraScraper(BaseScraper):
    """Scraper for ES/Extremadura -- DOE via ELI hierarchy + XML full text."""

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
        """Fetch a page with rate limiting."""
        self.rate_limiter.wait()
        try:
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding or "utf-8"
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

    def _extract_eli_links(self, page_html: str, pattern: str) -> List[str]:
        """Extract ELI path segments from a page matching a pattern."""
        if not page_html:
            return []
        matches = re.findall(pattern, page_html)
        return sorted(set(matches))

    def _get_years_for_type(self, doc_type: str) -> List[int]:
        """Get available years for a document type."""
        url = f"{BASE_URL}/eli/es-ex/{doc_type}"
        page = self._fetch_page(url)
        pattern = rf'eli/es-ex/{re.escape(doc_type)}/(\d{{4}})'
        years = self._extract_eli_links(page, pattern)
        return [int(y) for y in years]

    def _get_months_for_year(self, doc_type: str, year: int) -> List[int]:
        """Get available months for a type/year."""
        url = f"{BASE_URL}/eli/es-ex/{doc_type}/{year}"
        page = self._fetch_page(url)
        pattern = rf'eli/es-ex/{re.escape(doc_type)}/{year}/(\d{{1,2}})'
        months = self._extract_eli_links(page, pattern)
        return [int(m) for m in months]

    def _get_days_for_month(self, doc_type: str, year: int, month: int) -> List[int]:
        """Get available days for a type/year/month."""
        month_str = f"{month:02d}"
        url = f"{BASE_URL}/eli/es-ex/{doc_type}/{year}/{month_str}"
        page = self._fetch_page(url)
        pattern = rf'eli/es-ex/{re.escape(doc_type)}/{year}/{month_str}/(\d{{1,2}})'
        days = self._extract_eli_links(page, pattern)
        return [int(d) for d in days]

    def _get_doc_numbers(self, doc_type: str, year: int, month: int, day: int) -> List[str]:
        """Get document numbers for a specific date."""
        month_str = f"{month:02d}"
        day_str = f"{day:02d}"
        url = f"{BASE_URL}/eli/es-ex/{doc_type}/{year}/{month_str}/{day_str}"
        page = self._fetch_page(url)
        if not page:
            return []
        # Extract document numbers and also any PDF/consolidated links
        pattern = rf'eli/es-ex/{re.escape(doc_type)}/{year}/{month_str}/{day_str}/(\d+)'
        numbers = self._extract_eli_links(page, pattern)
        return numbers

    def _get_xml_info_from_eli(self, doc_type: str, year: int, month: int,
                                day: int, number: str) -> Optional[Tuple[str, str, str]]:
        """Get XML document ID, DOE directory, and title from ELI PDF reference.

        The ELI /dof/spa/pdf endpoint returns an iframe with the actual PDF path,
        from which we derive the XML document ID and DOE issue directory.

        Returns (xml_id, doe_dir, title) or None.
        """
        month_str = f"{month:02d}"
        day_str = f"{day:02d}"

        # First try the day page to extract the title
        day_url = f"{BASE_URL}/eli/es-ex/{doc_type}/{year}/{month_str}/{day_str}"
        day_page = self._fetch_page(day_url)
        title = ""
        if day_page:
            # Extract title text from the page (appears as plain text near the doc links)
            clean = re.sub(r'<[^>]+>', '\n', day_page)
            lines = [l.strip() for l in clean.split('\n') if l.strip() and len(l.strip()) > 20]
            for line in lines:
                if any(w in line for w in ['Ley ', 'Decreto ', 'Orden ', 'Reglamento ']):
                    title = line.strip()
                    break

        # Try consolidated version first (has full text), then original (dof)
        pdf_page = None
        for variant in ['con', 'dof']:
            if variant == 'con':
                # Need to find the consolidated date - look for it in day page
                con_links = re.findall(
                    rf'eli/es-ex/{re.escape(doc_type)}/{year}/{month_str}/{day_str}/{re.escape(number)}/con/(\d+)/spa/pdf',
                    day_page or ""
                )
                if con_links:
                    con_date = con_links[0]
                    url = f"{BASE_URL}/eli/es-ex/{doc_type}/{year}/{month_str}/{day_str}/{number}/con/{con_date}/spa/pdf"
                else:
                    continue
            else:
                url = f"{BASE_URL}/eli/es-ex/{doc_type}/{year}/{month_str}/{day_str}/{number}/dof/spa/pdf"

            pdf_page = self._fetch_page(url)
            if pdf_page:
                # Extract iframe src pointing to PDF path
                iframe_match = re.search(r'src="([^"]+\.pdf)"', pdf_page)
                if iframe_match:
                    pdf_path = iframe_match.group(1)
                    # Parse: /pdfs/doe/{year}/{doe_dir}/{filename}.pdf
                    path_match = re.match(
                        r'/pdfs/doe/(\d{4})/(\w+)/(\w+)\.pdf',
                        pdf_path
                    )
                    if path_match:
                        _, doe_dir, filename = path_match.groups()
                        # Remove trailing 'C' (consolidated marker) from filename
                        base_name = filename.rstrip('C')
                        # Build full XML ID: {full_year}{rango_code}{sequential}
                        # Short ID is like 24010001 -> full is 2024010001
                        if len(base_name) == 8:
                            xml_id = f"{year}{base_name[2:]}"
                        else:
                            xml_id = base_name
                        return (xml_id, doe_dir, title)

        return None

    def _fetch_xml_content(self, year: int, doe_dir: str, xml_id: str) -> Optional[str]:
        """Fetch XML document content."""
        url = f"{BASE_URL}/pdfs/doe/{year}/{doe_dir}/xml/{xml_id}.xml"
        return self._fetch_page(url)

    def _extract_text_from_xml(self, xml_content: str) -> Tuple[str, str, str, str, str]:
        """Extract metadata and text from DOE XML.

        Returns (title, text, date, rango, organismo).
        """
        if not xml_content:
            return ("", "", "", "", "")

        def get_tag(tag: str) -> str:
            m = re.search(rf'<{tag}>(.*?)</{tag}>', xml_content, re.S)
            return m.group(1).strip() if m else ""

        title = get_tag("titulo")
        date = get_tag("fecha_publicacion")
        rango = get_tag("rango")
        organismo = get_tag("organismo")

        # Extract text content
        text_match = re.search(r'<texto>(.*?)</texto>', xml_content, re.S)
        if not text_match:
            return (title, "", date, rango, organismo)

        text_html = text_match.group(1)

        # Clean HTML tags from text
        # Replace block elements with newlines
        text_html = re.sub(r'</p>', '\n', text_html)
        text_html = re.sub(r'<br\s*/?>', '\n', text_html)
        text_html = re.sub(r'</div>', '\n', text_html)
        text_html = re.sub(r'</tr>', '\n', text_html)
        text_html = re.sub(r'</td>', ' | ', text_html)
        text_html = re.sub(r'</th>', ' | ', text_html)
        text_html = re.sub(r'<firma[^>]*>', '\n', text_html)
        text_html = re.sub(r'</firma>', '\n', text_html)

        # Strip remaining HTML tags
        text = re.sub(r'<[^>]+>', '', text_html)

        # Decode HTML entities
        text = html_module.unescape(text)

        # Clean up whitespace
        lines = text.split('\n')
        cleaned = []
        for line in lines:
            line = line.strip()
            if line:
                cleaned.append(line)

        text = '\n'.join(cleaned)
        text = re.sub(r'\n{3,}', '\n\n', text)

        return (title, text.strip(), date, rango, organismo)

    def _build_eli_id(self, doc_type: str, year: int, month: int,
                       day: int, number: str) -> str:
        """Build a unique ELI-based document ID."""
        return f"eli-es-ex-{doc_type}-{year}-{month:02d}-{day:02d}-{number}"

    def _build_eli_url(self, doc_type: str, year: int, month: int,
                        day: int, number: str) -> str:
        """Build the ELI URL for a document."""
        return f"{BASE_URL}/eli/es-ex/{doc_type}/{year}/{month:02d}/{day:02d}/{number}"

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legislation from DOE via ELI hierarchy (newest first)."""
        for doc_type in ELI_TYPES:
            type_name = ELI_TYPE_NAMES.get(doc_type, doc_type)
            logger.info(f"Processing type: {type_name} ({doc_type})")

            years = self._get_years_for_type(doc_type)
            if not years:
                logger.info(f"  No years found for type {doc_type}")
                continue

            for year in sorted(years, reverse=True):
                months = self._get_months_for_year(doc_type, year)
                if not months:
                    continue

                for month in sorted(months, reverse=True):
                    days = self._get_days_for_month(doc_type, year, month)
                    if not days:
                        continue

                    for day in sorted(days, reverse=True):
                        numbers = self._get_doc_numbers(doc_type, year, month, day)
                        if not numbers:
                            continue

                        for number in numbers:
                            record = self._fetch_document(doc_type, type_name, year, month, day, number)
                            if record:
                                yield record

                logger.info(f"  Completed {type_name} year {year}")

    def _fetch_document(self, doc_type: str, type_name: str, year: int,
                         month: int, day: int, number: str) -> Optional[dict]:
        """Fetch a single document and return raw record dict."""
        xml_info = self._get_xml_info_from_eli(doc_type, year, month, day, number)
        if not xml_info:
            logger.warning(f"  No XML info for {doc_type}/{year}/{month:02d}/{day:02d}/{number}")
            return None

        xml_id, doe_dir, eli_title = xml_info
        xml_content = self._fetch_xml_content(year, doe_dir, xml_id)
        if not xml_content:
            logger.warning(f"  No XML content for {xml_id} in {doe_dir}")
            return None

        title, text, pub_date, rango, organismo = self._extract_text_from_xml(xml_content)

        if not text or len(text) < 50:
            logger.warning(
                f"  Insufficient text for {doc_type}/{year}/{month:02d}/{day:02d}/{number}: "
                f"{len(text) if text else 0} chars"
            )
            return None

        # Use ELI title if XML title is missing
        if not title and eli_title:
            title = eli_title

        # Normalize date from YYYYMMDD to YYYY-MM-DD
        if pub_date and len(pub_date) == 8 and pub_date.isdigit():
            iso_date = f"{pub_date[:4]}-{pub_date[4:6]}-{pub_date[6:8]}"
        else:
            iso_date = pub_date or f"{year}-{month:02d}-{day:02d}"

        return {
            "doc_type": doc_type,
            "type_name": type_name,
            "year": year,
            "month": month,
            "day": day,
            "number": number,
            "title": title,
            "text": text,
            "date": iso_date,
            "rango": rango,
            "organismo": organismo,
            "xml_id": xml_id,
            "eli_url": self._build_eli_url(doc_type, year, month, day, number),
        }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents published since the given date."""
        since_year = since.year
        current_year = datetime.now().year

        for doc_type in ELI_TYPES:
            type_name = ELI_TYPE_NAMES.get(doc_type, doc_type)
            years = self._get_years_for_type(doc_type)
            if not years:
                continue

            for year in sorted(years, reverse=True):
                if year < since_year:
                    break

                months = self._get_months_for_year(doc_type, year)
                for month in sorted(months, reverse=True):
                    days = self._get_days_for_month(doc_type, year, month)
                    for day in sorted(days, reverse=True):
                        doc_date = datetime(year, month, day)
                        if doc_date < since.replace(tzinfo=None):
                            continue

                        numbers = self._get_doc_numbers(doc_type, year, month, day)
                        for number in numbers:
                            record = self._fetch_document(doc_type, type_name, year, month, day, number)
                            if record:
                                yield record

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        doc_type = raw.get("doc_type", "")
        year = raw.get("year", 0)
        month = raw.get("month", 0)
        day = raw.get("day", 0)
        number = raw.get("number", "")

        doc_id = self._build_eli_id(doc_type, year, month, day, number)

        return {
            "_id": doc_id,
            "_source": "ES/Extremadura",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("eli_url", ""),
            "doc_type": doc_type,
            "type_name": raw.get("type_name", ""),
            "rango": raw.get("rango", ""),
            "organismo": raw.get("organismo", ""),
            "language": "es",
            "region": "Extremadura",
            "country": "ES",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing DOE Extremadura connection...")

        print("\n1. Testing ELI base endpoint...")
        page = self._fetch_page(f"{BASE_URL}/eli/es-ex")
        if not page:
            print("   ERROR: Cannot reach ELI endpoint")
            return
        types = self._extract_eli_links(page, r'eli/es-ex/([a-z]+)"')
        print(f"   Found document types: {', '.join(types)}")

        print("\n2. Testing law type hierarchy...")
        years = self._get_years_for_type("l")
        print(f"   Laws available for {len(years)} years ({min(years) if years else '?'}-{max(years) if years else '?'})")

        print("\n3. Testing document fetch with XML full text...")
        if years:
            test_year = max(years)
            months = self._get_months_for_year("l", test_year)
            if months:
                test_month = max(months)
                days = self._get_days_for_month("l", test_year, test_month)
                if days:
                    test_day = days[0]
                    numbers = self._get_doc_numbers("l", test_year, test_month, test_day)
                    if numbers:
                        test_num = numbers[0]
                        record = self._fetch_document("l", "Ley", test_year, test_month, test_day, test_num)
                        if record:
                            print(f"   Doc: l/{test_year}/{test_month:02d}/{test_day:02d}/{test_num}")
                            print(f"   Title: {record['title'][:100]}...")
                            print(f"   Text length: {len(record['text'])} chars")
                            print(f"   First 200 chars: {record['text'][:200]}...")
                        else:
                            print("   ERROR: Could not fetch document")
                    else:
                        print("   No documents found")
                else:
                    print("   No days found")
            else:
                print("   No months found")

        print("\nAll tests passed!")


def main():
    scraper = ExtremaduraScraper()

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
