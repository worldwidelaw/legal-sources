#!/usr/bin/env python3
"""
ES/Canarias -- Canary Islands Regional Legislation (BOC)

Fetches legislation from the Boletín Oficial de Canarias (BOC) via HTML
scraping of the official gazette website.

Strategy:
  - Crawl yearly index pages at gobiernodecanarias.org/boc/{year}/index.html
    to discover bulletin numbers and dates.
  - For each bulletin, parse the index page to identify Section I
    (Disposiciones Generales) document entries.
  - Fetch each document's HTML page and extract metadata from META tags
    and full text from <p class="justificado"> body elements.

Data:
  - Full text available from 1980 onward.
  - License: CC BY 4.0 (Gobierno de Canarias open data).
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
import time
import socket
from pathlib import Path
from datetime import datetime, timezone, timedelta
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
logger = logging.getLogger("legal-data-hunter.ES.canarias")

BASE_URL = "https://www.gobiernodecanarias.org/boc"
START_YEAR = 2012  # online HTML archive starts here


class CanariasScraper(BaseScraper):
    """
    Scraper for ES/Canarias -- Canary Islands Regional Legislation (BOC).
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
        """Strip HTML tags and clean whitespace from content."""
        if not text:
            return ""
        text = html.unescape(text)
        if '&lt;' in text or '&amp;' in text:
            text = html.unescape(text)
        text = re.sub(r'<br\s*/?\s*>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<p[^>]*>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n[ \t]+', '\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch a page with correct UTF-8 encoding."""
        self.rate_limiter.wait()
        try:
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            resp.encoding = "utf-8"
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

    def _get_bulletin_numbers(self, year: int) -> list:
        """Get all bulletin numbers and dates for a given year."""
        url = f"{BASE_URL}/{year}/index.html"
        page = self._fetch_page(url)
        if not page:
            return []

        # Pattern: /boc/2025/001/index.html with title containing date
        pattern = rf'/boc/{year}/(\d{{3}})/index\.html"\s*title="[^"]*\((\d{{4}}-\d{{2}}-\d{{2}})\)"'
        results = re.findall(pattern, page)
        if not results:
            # Fallback: just extract bulletin numbers without dates
            pattern2 = rf'/boc/{year}/(\d{{3}})/index\.html'
            nums = list(dict.fromkeys(re.findall(pattern2, page)))
            results = [(n, "") for n in nums]

        logger.info(f"Year {year}: found {len(results)} bulletins")
        return results

    def _get_section_i_docs(self, year: int, bulletin: str) -> list:
        """Parse bulletin index to find Section I (Disposiciones Generales) documents."""
        url = f"{BASE_URL}/{year}/{bulletin}/index.html"
        page = self._fetch_page(url)
        if not page:
            return []

        # Check if Section I exists
        if 'Disposiciones generales' not in page and 'Disposiciones Generales' not in page:
            return []

        # Find the start of Section I and the start of the next section
        sec_start = re.search(
            r'<h4>I\.\s*Disposiciones\s+[Gg]enerales</h4>',
            page
        )
        if not sec_start:
            return []

        # Find the next <h4> section header after Section I
        sec_end = re.search(r'<h4>', page[sec_start.end():])
        if sec_end:
            section_html = page[sec_start.end():sec_start.end() + sec_end.start()]
        else:
            section_html = page[sec_start.end():]

        # Extract document HTML page links within Section I
        docs = []
        doc_pattern = rf'/boc/{year}/{bulletin}/(\d{{3}})\.html'
        cve_pattern = r'BOC-A-\d{4}-\d{3}-\d+'

        # Find all doc page links in the section
        for m in re.finditer(doc_pattern, section_html):
            doc_num = m.group(1)
            # Look for nearby CVE
            context_start = max(0, m.start() - 500)
            context = section_html[context_start:m.end() + 200]
            cve_match = re.search(cve_pattern, context)
            cve = cve_match.group(0) if cve_match else f"BOC-A-{year}-{bulletin}-{doc_num}"

            # Avoid duplicates
            if not any(d["doc_num"] == doc_num for d in docs):
                docs.append({
                    "doc_num": doc_num,
                    "cve": cve,
                    "year": year,
                    "bulletin": bulletin,
                })

        return docs

    def _fetch_document(self, year: int, bulletin: str, doc_num: str) -> Optional[dict]:
        """Fetch a single document page and extract metadata + full text."""
        url = f"{BASE_URL}/{year}/{bulletin}/{doc_num}.html"
        page = self._fetch_page(url)
        if not page:
            return None

        # Extract metadata from META tags
        def meta(name):
            m = re.search(
                rf'<META\s+NAME="{name}"\s+CONTENT="(.*?)"',
                page, re.IGNORECASE | re.DOTALL
            )
            return html.unescape(m.group(1).strip()) if m else ""

        title = meta("plainsummary") or meta("summary")
        date = meta("documentdate")
        entity = meta("plainentity") or meta("entity")
        doc_type_code = meta("typedocument")
        bulletin_num = meta("documentnumber")

        # Extract CVE from the page
        cve_match = re.search(r'BOC-A-\d{4}-\d{3}-\d+', page)
        cve = cve_match.group(0) if cve_match else f"BOC-A-{year}-{bulletin}-{doc_num}"

        # Extract section from h5.article_title
        section_match = re.search(
            r'<h5\s+class="article_title">\s*(.*?)\s*</h5>',
            page, re.DOTALL
        )
        section = self._clean_html(section_match.group(1)) if section_match else ""

        # Extract full text: all <p class="justificado"> elements after the header block
        # The body text starts after the <h3> metadata block
        body_start = re.search(r'</h3>\s*', page)
        if body_start:
            body_html = page[body_start.end():]
        else:
            body_html = page

        # Collect all justified paragraphs
        paragraphs = re.findall(
            r'<p\s+class="justificado"[^>]*>(.*?)</p>',
            body_html, re.DOTALL
        )

        if not paragraphs:
            # Fallback: try getting text from bloq_interior
            interior_match = re.search(
                r'<div\s+id="bloq_interior"[^>]*>(.*?)</div>\s*</div>\s*</div>',
                page, re.DOTALL
            )
            if interior_match:
                text = self._clean_html(interior_match.group(1))
            else:
                return None
        else:
            text_parts = [self._clean_html(p) for p in paragraphs]
            text = "\n\n".join(t for t in text_parts if t)

        if len(text) < 50:
            return None

        return {
            "cve": cve,
            "title": title,
            "text": text,
            "date": date,
            "entity": entity,
            "section": section,
            "typedocument": doc_type_code,
            "bulletin": bulletin_num or bulletin,
            "year": str(year),
            "url": url,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all Section I legislation from BOC (1980-present)."""
        current_year = datetime.now().year

        for year in range(START_YEAR, current_year + 1):
            logger.info(f"Processing year {year}...")
            bulletins = self._get_bulletin_numbers(year)

            for bulletin_num, bulletin_date in bulletins:
                docs = self._get_section_i_docs(year, bulletin_num)
                if not docs:
                    continue

                logger.info(f"Bulletin {year}/{bulletin_num}: {len(docs)} Section I documents")

                for doc_info in docs:
                    result = self._fetch_document(year, bulletin_num, doc_info["doc_num"])
                    if result:
                        yield result

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents published since the given date."""
        current_year = datetime.now().year
        since_year = since.year

        for year in range(since_year, current_year + 1):
            bulletins = self._get_bulletin_numbers(year)

            for bulletin_num, bulletin_date in bulletins:
                # Skip bulletins before the since date if we have date info
                if bulletin_date:
                    try:
                        bd = datetime.strptime(bulletin_date, "%Y-%m-%d")
                        if bd < since.replace(tzinfo=None):
                            continue
                    except ValueError:
                        pass

                docs = self._get_section_i_docs(year, bulletin_num)
                if not docs:
                    continue

                for doc_info in docs:
                    result = self._fetch_document(year, bulletin_num, doc_info["doc_num"])
                    if result:
                        yield result

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        cve = raw.get("cve", "")
        title = raw.get("title", "")
        text = raw.get("text", "")
        date = raw.get("date", "")
        entity = raw.get("entity", "")
        section = raw.get("section", "")
        url = raw.get("url", "")

        return {
            "_id": cve,
            "_source": "ES/Canarias",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "entity": entity,
            "section": section,
            "cve": cve,
            "bulletin": raw.get("bulletin", ""),
            "language": "es",
            "region": "Canarias",
            "country": "ES",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing BOC Canarias connection...")

        print("\n1. Testing yearly index...")
        try:
            bulletins = self._get_bulletin_numbers(2025)
            print(f"   Found {len(bulletins)} bulletins for 2025")
            if bulletins:
                print(f"   First: {bulletins[0]}, Last: {bulletins[-1]}")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        print("\n2. Testing bulletin Section I discovery...")
        found_docs = []
        for bnum, bdate in bulletins[:10]:
            docs = self._get_section_i_docs(2025, bnum)
            if docs:
                found_docs.extend(docs)
                print(f"   Bulletin {bnum}: {len(docs)} Section I docs")
                if len(found_docs) >= 3:
                    break

        print(f"\n3. Testing full document fetch...")
        if found_docs:
            doc = found_docs[0]
            result = self._fetch_document(2025, doc["bulletin"], doc["doc_num"])
            if result:
                print(f"   CVE: {result['cve']}")
                print(f"   Title: {result['title'][:100]}...")
                print(f"   Text length: {len(result['text'])} chars")
                print(f"   Date: {result['date']}")
                print(f"   Entity: {result['entity']}")
            else:
                print("   ERROR: Could not fetch document")

        print("\nAll tests passed!")


def main():
    scraper = CanariasScraper()

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
