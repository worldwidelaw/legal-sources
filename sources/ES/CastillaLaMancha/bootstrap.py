#!/usr/bin/env python3
"""
ES/CastillaLaMancha -- Castilla-La Mancha Regional Legislation (DOCM)

Fetches legislation from the Diario Oficial de Castilla-La Mancha via
ELI (European Legislation Identifier) hierarchy:
  1. Navigate type -> year -> month -> day -> number
  2. Fetch HTML full text at /eli/es-cm/{type}/{year}/{month}/{day}/{number}/dof/spa/html
  3. Extract clean text from structured HTML

Data:
  - Coverage from 1983 to present.
  - License: CC BY-SA 3.0 ES (per datos.gob.es).
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
from typing import Generator, Dict, Any, Optional, List

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
logger = logging.getLogger("legal-data-hunter.ES.castillalamancha")

BASE_URL = "https://docm.jccm.es/docm"

# ELI document types: legislation-relevant ones first
# l=ley, d=decreto, dlg=decreto legislativo, o=orden, res=resolucion,
# a=acuerdo, i=instruccion, reg=reglamento, not=notificacion, alia=otros
ELI_TYPES = ["l", "d", "dlg", "o", "res", "a", "i", "reg"]
# Skip 'not' (notificaciones) and 'alia' (otros) — not legislation

ELI_TYPE_NAMES = {
    "l": "Ley",
    "d": "Decreto",
    "dlg": "Decreto Legislativo",
    "o": "Orden",
    "res": "Resolución",
    "a": "Acuerdo",
    "i": "Instrucción",
    "reg": "Reglamento",
}


class CastillaLaManchaScraper(BaseScraper):
    """Scraper for ES/CastillaLaMancha -- DOCM via ELI hierarchy."""

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
        """Fetch an HTML page with rate limiting."""
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
        url = f"{BASE_URL}/eli/es-cm/{doc_type}"
        page = self._fetch_page(url)
        pattern = rf'eli/es-cm/{re.escape(doc_type)}/(\d{{4}})'
        years = self._extract_eli_links(page, pattern)
        return [int(y) for y in years]

    def _get_months_for_year(self, doc_type: str, year: int) -> List[int]:
        """Get available months for a type/year."""
        url = f"{BASE_URL}/eli/es-cm/{doc_type}/{year}"
        page = self._fetch_page(url)
        pattern = rf'eli/es-cm/{re.escape(doc_type)}/{year}/(\d{{1,2}})'
        months = self._extract_eli_links(page, pattern)
        return [int(m) for m in months]

    def _get_days_for_month(self, doc_type: str, year: int, month: int) -> List[int]:
        """Get available days for a type/year/month."""
        month_str = f"{month:02d}"
        url = f"{BASE_URL}/eli/es-cm/{doc_type}/{year}/{month_str}"
        page = self._fetch_page(url)
        pattern = rf'eli/es-cm/{re.escape(doc_type)}/{year}/{month_str}/(\d{{1,2}})'
        days = self._extract_eli_links(page, pattern)
        return [int(d) for d in days]

    def _get_doc_numbers(self, doc_type: str, year: int, month: int, day: int) -> List[str]:
        """Get document numbers for a specific date."""
        month_str = f"{month:02d}"
        day_str = f"{day:02d}"
        url = f"{BASE_URL}/eli/es-cm/{doc_type}/{year}/{month_str}/{day_str}"
        page = self._fetch_page(url)
        pattern = rf'eli/es-cm/{re.escape(doc_type)}/{year}/{month_str}/{day_str}/(\d+)'
        return self._extract_eli_links(page, pattern)

    def _fetch_full_text_html(self, doc_type: str, year: int, month: int,
                               day: int, number: str) -> Optional[str]:
        """Fetch the full text HTML for a document via ELI."""
        month_str = f"{month:02d}"
        day_str = f"{day:02d}"
        url = f"{BASE_URL}/eli/es-cm/{doc_type}/{year}/{month_str}/{day_str}/{number}/dof/spa/html"
        return self._fetch_page(url)

    def _extract_text_from_html(self, html_content: str) -> tuple:
        """Extract title and clean text from DOCM HTML full text page.

        Returns (title, text) tuple.
        """
        if not html_content:
            return ("", "")

        # Extract title from Sumario-disposición class
        title = ""
        title_match = re.search(
            r'class="Sumario-disposici[oó]n[^"]*"[^>]*>(.*?)</p>',
            html_content, re.DOTALL
        )
        if title_match:
            title = re.sub(r'<[^>]+>', '', title_match.group(1))
            title = html_module.unescape(title).strip()
            # Remove trailing NID code like [2025/3179]
            title = re.sub(r'\s*\[\d{4}/\d+\]\s*$', '', title).strip()

        # Extract body text from the contenido_html div
        body_match = re.search(
            r'<div id="contenido_html">(.*?)(?:</div>\s*</body>|$)',
            html_content, re.DOTALL
        )
        if not body_match:
            body_match = re.search(r'<body[^>]*>(.*?)</body>', html_content, re.DOTALL)

        if not body_match:
            return (title, "")

        body = body_match.group(1)

        # Remove header/cabecera section
        body = re.sub(r'<div class="cabecera">.*?</div>\s*</div>', '', body, flags=re.DOTALL)

        # Remove image tags (some annexes are images)
        body = re.sub(r'<img[^>]*>', '', body)

        # Replace block elements with newlines
        body = re.sub(r'</p>', '\n', body)
        body = re.sub(r'</div>', '\n', body)
        body = re.sub(r'<br\s*/?>', '\n', body)
        body = re.sub(r'</tr>', '\n', body)
        body = re.sub(r'</td>', ' | ', body)
        body = re.sub(r'</th>', ' | ', body)

        # Strip remaining HTML tags
        body = re.sub(r'<[^>]+>', '', body)

        # Decode HTML entities
        body = html_module.unescape(body)

        # Clean up whitespace
        lines = body.split('\n')
        cleaned = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            # Skip header lines
            if re.match(r'^AÑO\s+[XLIV]+\s+Núm\.\s*\d+$', line):
                continue
            if re.match(r'^\d+\s+de\s+\w+\s+de\s+\d{4}$', line):
                continue
            if line.startswith('I.- DISPOSICIONES') or line.startswith('II.- ') or line.startswith('III.- '):
                continue
            cleaned.append(line)

        text = '\n'.join(cleaned)
        # Normalize multiple blank lines
        text = re.sub(r'\n{3,}', '\n\n', text)
        return (title, text.strip())

    def _build_eli_id(self, doc_type: str, year: int, month: int,
                       day: int, number: str) -> str:
        """Build a unique ELI-based document ID."""
        return f"eli-es-cm-{doc_type}-{year}-{month:02d}-{day:02d}-{number}"

    def _build_eli_url(self, doc_type: str, year: int, month: int,
                        day: int, number: str) -> str:
        """Build the ELI URL for a document."""
        return f"https://docm.jccm.es/docm/eli/es-cm/{doc_type}/{year}/{month:02d}/{day:02d}/{number}"

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legislation from DOCM via ELI hierarchy (newest first)."""
        for doc_type in ELI_TYPES:
            type_name = ELI_TYPE_NAMES.get(doc_type, doc_type)
            logger.info(f"Processing type: {type_name} ({doc_type})")

            years = self._get_years_for_type(doc_type)
            if not years:
                logger.info(f"  No years found for type {doc_type}")
                continue

            # Process newest first
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
                            html_content = self._fetch_full_text_html(
                                doc_type, year, month, day, number
                            )
                            if not html_content:
                                logger.warning(
                                    f"  No HTML for {doc_type}/{year}/{month:02d}/{day:02d}/{number}"
                                )
                                continue

                            title, text = self._extract_text_from_html(html_content)
                            if not text or len(text) < 50:
                                logger.warning(
                                    f"  Insufficient text for {doc_type}/{year}/{month:02d}/{day:02d}/{number}: "
                                    f"{len(text) if text else 0} chars"
                                )
                                continue

                            yield {
                                "doc_type": doc_type,
                                "type_name": type_name,
                                "year": year,
                                "month": month,
                                "day": day,
                                "number": number,
                                "title": title,
                                "text": text,
                                "date": f"{year}-{month:02d}-{day:02d}",
                                "eli_url": self._build_eli_url(
                                    doc_type, year, month, day, number
                                ),
                            }

                logger.info(f"  Completed {type_name} year {year}")

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
                            html_content = self._fetch_full_text_html(
                                doc_type, year, month, day, number
                            )
                            if not html_content:
                                continue

                            title, text = self._extract_text_from_html(html_content)
                            if not text or len(text) < 50:
                                continue

                            yield {
                                "doc_type": doc_type,
                                "type_name": type_name,
                                "year": year,
                                "month": month,
                                "day": day,
                                "number": number,
                                "title": title,
                                "text": text,
                                "date": f"{year}-{month:02d}-{day:02d}",
                                "eli_url": self._build_eli_url(
                                    doc_type, year, month, day, number
                                ),
                            }

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
            "_source": "ES/CastillaLaMancha",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("eli_url", ""),
            "doc_type": doc_type,
            "type_name": raw.get("type_name", ""),
            "language": "es",
            "region": "Castilla-La Mancha",
            "country": "ES",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing DOCM Castilla-La Mancha connection...")

        print("\n1. Testing ELI base endpoint...")
        page = self._fetch_page(f"{BASE_URL}/eli/es-cm")
        if not page:
            print("   ERROR: Cannot reach ELI endpoint")
            return
        types = self._extract_eli_links(page, r'eli/es-cm/([a-z]+)"')
        print(f"   Found {len(types)} document types: {', '.join(types)}")

        print("\n2. Testing law type hierarchy...")
        years = self._get_years_for_type("l")
        print(f"   Laws available for {len(years)} years ({min(years) if years else '?'}-{max(years) if years else '?'})")

        print("\n3. Testing drill-down and HTML full text...")
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
                        html_content = self._fetch_full_text_html(
                            "l", test_year, test_month, test_day, test_num
                        )
                        title, text = self._extract_text_from_html(html_content)
                        print(f"   Doc: l/{test_year}/{test_month:02d}/{test_day:02d}/{test_num}")
                        print(f"   Title: {title[:100]}..." if len(title) > 100 else f"   Title: {title}")
                        print(f"   Text length: {len(text)} chars")
                        print(f"   First 200 chars: {text[:200]}...")
                    else:
                        print("   No documents found")
                else:
                    print("   No days found")
            else:
                print("   No months found")

        print("\nAll tests passed!")


def main():
    scraper = CastillaLaManchaScraper()

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
