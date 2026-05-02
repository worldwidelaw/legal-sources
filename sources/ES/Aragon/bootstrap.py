#!/usr/bin/env python3
"""
ES/Aragon -- Aragón Regional Legislation Data Fetcher

Fetches regional legislation from Aragón via the BOA (Boletín Oficial de Aragón)
BRSCGI search and document retrieval system.

Strategy:
  - Use CMD=VERLST to paginate through document listings (SECC-C=DISPOSICIONES).
  - Extract DOCN identifiers, titles, dates, sections, and issuers from listing HTML.
  - Use CMD=VERDOC&DOCN={id} to fetch full text of each document.
  - Parse "Texto completo:" section from document HTML for full body text.

Endpoints:
  - List: https://www.boa.aragon.es/cgi-bin/EBOA/BRSCGI?CMD=VERLST&BASE=BOLE&...
  - Doc:  https://www.boa.aragon.es/cgi-bin/EBOA/BRSCGI?CMD=VERDOC&BASE=BOLE&DOCN={id}

Data:
  - 90,000+ legislative dispositions since 1978
  - License: CC BY 4.0
  - Language: Spanish (es)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import time
import socket
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional

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
logger = logging.getLogger("legal-data-hunter.ES.aragon")

BASE_URL = "https://www.boa.aragon.es/cgi-bin/EBOA/BRSCGI"
PAGE_SIZE = 25


class AragonScraper(BaseScraper):
    """
    Scraper for ES/Aragon -- Aragón Regional Legislation (BOA).
    Country: ES
    URL: https://www.boa.aragon.es

    Data types: legislation
    Auth: none (CC BY 4.0)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml",
        })
        adapter = HTTPAdapter(max_retries=3)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _clean_html(self, text: str) -> str:
        """Strip HTML tags and clean whitespace."""
        if not text:
            return ""
        text = re.sub(r'<br\s*/?\s*>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = html.unescape(text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n[ \t]+', '\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _fetch_listing_page(self, start: int, count: int,
                            date_from: str = None, date_to: str = None) -> str:
        """Fetch a listing page of documents.

        Uses raw URL construction because BRSCGI requires specific parameter
        ordering (docs= must appear before SEC=).
        """
        end = start + count - 1
        url = (
            f"{BASE_URL}?CMD=VERLST&BASE=BOLE&docs={start}-{end}"
            f"&SEC=BUSQUEDA_AVANZADA&RNG={count}&SORT=-PUBL"
            f"&SEPARADOR=&SECC-C=DISPOSICIONES"
        )
        if date_from:
            url += f"&%40PUBL-GE={date_from}"
        if date_to:
            url += f"&%40PUBL-LE={date_to}"

        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=60)
        resp.raise_for_status()
        return resp.content.decode('latin-1', errors='replace')

    def _parse_listing(self, html_text: str) -> list:
        """Parse a listing page to extract document metadata."""
        results = []

        # Extract total count
        total_m = re.search(r'total de (\d+)', html_text)
        total = int(total_m.group(1)) if total_m else 0

        # Extract DOCN numbers from HTML comments
        docns = re.findall(r'DOCN=(\d+)', html_text)

        # Extract titles
        titles = re.findall(r'<p class="boatitulo">(.*?)</p>', html_text, re.DOTALL)

        # Extract dates
        dates = re.findall(r'<span class="negrita">(\d{2}/\d{2}/\d{4})</span>', html_text)

        # Extract sections
        sections = re.findall(r'<span class="boaseccion">\s*(.*?)\s*</span>', html_text, re.DOTALL)

        # Extract emitters
        emitters = re.findall(r'<span class="boaemisor">(.*?)</span>', html_text, re.DOTALL)

        # The sections list contains numbered entries ("1º") interspersed with section names
        # Filter to just the text sections
        section_names = [s.strip() for s in sections if not re.match(r'^\d+', s.strip())]

        for i, docn in enumerate(docns):
            title = ""
            if i < len(titles):
                title = re.sub(r'<[^>]+>', '', titles[i]).strip()
                title = re.sub(r'\s+', ' ', title)
                title = html.unescape(title)

            date = dates[i] if i < len(dates) else ""
            section = section_names[i] if i < len(section_names) else ""
            emitter = emitters[i].strip() if i < len(emitters) else ""
            emitter = html.unescape(emitter)

            results.append({
                "docn": docn,
                "title": title,
                "pub_date": date,
                "section": section,
                "emitter": emitter,
                "total": total,
            })

        return results

    def _fetch_full_text(self, docn: str) -> str:
        """Fetch full text for a document by DOCN."""
        url = (
            f"{BASE_URL}?CMD=VERDOC&BASE=BOLE"
            f"&SEC=BUSQUEDA_AVANZADA&SEPARADOR=&DOCN={docn}"
        )

        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=60)
        resp.raise_for_status()
        content = resp.content.decode('latin-1', errors='replace')

        # Find the main content div (id="leelo")
        leelo_start = content.find('id="leelo"')
        if leelo_start < 0:
            return ""

        rest = content[leelo_start:]

        # Extract text segments between script blocks
        segments = re.findall(r'(?:</script>)(.*?)(?:<script|<footer)', rest, re.DOTALL)

        # Find the longest segment — that's the body text
        best = ""
        for seg in segments:
            clean = self._clean_html(seg)
            if len(clean) > len(best):
                best = clean

        # Remove leading "Texto completo:" prefix if present
        best = re.sub(r'^Texto completo:\s*', '', best)

        return best

    def _convert_date(self, date_str: str) -> str:
        """Convert DD/MM/YYYY to ISO 8601 YYYY-MM-DD."""
        if not date_str:
            return ""
        try:
            parts = date_str.split("/")
            return f"{parts[2]}-{parts[1]}-{parts[0]}"
        except (IndexError, ValueError):
            return date_str

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legislative documents from BOA."""
        start = 1
        total = None

        while True:
            try:
                html_text = self._fetch_listing_page(start, PAGE_SIZE)
                entries = self._parse_listing(html_text)
            except Exception as e:
                logger.error(f"Error fetching listing at offset {start}: {e}")
                break

            if not entries:
                break

            if total is None:
                total = entries[0].get("total", 0)
                logger.info(f"Total documents in BOA DISPOSICIONES: {total}")

            for entry in entries:
                docn = entry["docn"]
                try:
                    text = self._fetch_full_text(docn)
                except Exception as e:
                    logger.warning(f"Error fetching DOCN {docn}: {e}")
                    continue

                if not text or len(text) < 100:
                    logger.debug(f"Skipping DOCN {docn}: text too short ({len(text)} chars)")
                    continue

                entry["text"] = text
                yield entry

            start += PAGE_SIZE
            if total and start > total:
                break

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents published since the given date."""
        date_from = since.strftime("%Y%m%d")
        date_to = datetime.now().strftime("%Y%m%d")

        start = 1
        total = None

        while True:
            try:
                html_text = self._fetch_listing_page(start, PAGE_SIZE,
                                                     date_from=date_from,
                                                     date_to=date_to)
                entries = self._parse_listing(html_text)
            except Exception as e:
                logger.error(f"Error fetching updates at offset {start}: {e}")
                break

            if not entries:
                break

            if total is None:
                total = entries[0].get("total", 0)
                logger.info(f"Documents since {since.date()}: {total}")

            for entry in entries:
                docn = entry["docn"]
                try:
                    text = self._fetch_full_text(docn)
                except Exception as e:
                    logger.warning(f"Error fetching DOCN {docn}: {e}")
                    continue

                if not text or len(text) < 100:
                    continue

                entry["text"] = text
                yield entry

            start += PAGE_SIZE
            if total and start > total:
                break

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        docn = raw.get("docn", "")
        title = raw.get("title", "")
        text = raw.get("text", "")
        pub_date = self._convert_date(raw.get("pub_date", ""))
        section = raw.get("section", "")
        emitter = raw.get("emitter", "")

        url = f"https://www.boa.aragon.es/cgi-bin/EBOA/BRSCGI?CMD=VERDOC&BASE=BOLE&SEC=BUSQUEDA_AVANZADA&SEPARADOR=&DOCN={docn}"

        return {
            "_id": f"BOA-{docn}",
            "_source": "ES/Aragon",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": pub_date,
            "url": url,
            "section": section,
            "emitter": emitter,
            "docn": docn,
            "language": "es",
            "region": "Aragón",
            "country": "ES",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing BOA Aragón connection...")

        print("\n1. Testing listing endpoint...")
        try:
            html_text = self._fetch_listing_page(1, 3)
            entries = self._parse_listing(html_text)
            print(f"   Found {len(entries)} entries (total: {entries[0]['total'] if entries else 0})")
            for e in entries[:3]:
                print(f"   DOCN={e['docn']}: {e['title'][:80]}...")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        print("\n2. Testing full text retrieval...")
        if entries:
            docn = entries[0]["docn"]
            try:
                text = self._fetch_full_text(docn)
                print(f"   DOCN {docn}: {len(text)} chars")
                print(f"   Preview: {text[:200]}...")
            except Exception as e:
                print(f"   ERROR: {e}")

        print("\n3. Testing date filtering...")
        try:
            html_text = self._fetch_listing_page(1, 3, date_from="20260101", date_to="20261231")
            entries = self._parse_listing(html_text)
            print(f"   2026 documents: {entries[0]['total'] if entries else 0}")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nAll tests passed!")


def main():
    scraper = AragonScraper()

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
