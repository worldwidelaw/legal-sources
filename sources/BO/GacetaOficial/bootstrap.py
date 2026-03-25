#!/usr/bin/env python3
"""
BO/GacetaOficial -- Bolivia Official Gazette (via LexiVox)

Fetches Bolivian legislation from lexivox.org, an open legal database
with full text in HTML and GFDL licensing.

Strategy:
  - Search LexiVox by norm type to enumerate all norms
  - Paginate through results (15 per page, offset-based)
  - Fetch each norm's .xhtml page for full text + metadata
  - Extract body text from HTML, metadata from DCMI section

Data: 28,000+ norms (laws, decrees, resolutions, codes)
License: GFDL (GNU Free Documentation License)
Rate limit: 0.5 req/sec (2s between requests).

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List
from urllib.parse import urljoin, urlencode

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: beautifulsoup4 not installed. Run: pip3 install beautifulsoup4")
    sys.exit(1)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BO.GacetaOficial")

BASE_URL = "https://www.lexivox.org"
SEARCH_URL = BASE_URL + "/packages/lexml/buscar_normas.php"

# Norm types to fetch — covers all legislation categories
NORM_TYPES = [
    ("L", "Ley"),
    ("DS", "Decreto Supremo"),
    ("DP", "Decreto Presidencial"),
    ("RS", "Resolución Suprema"),
    ("CPE", "Constitución"),
    ("COD", "Código"),
]


class BOGacetaOficialScraper(BaseScraper):
    """
    Scraper for BO/GacetaOficial -- Bolivia Official Gazette via LexiVox.
    Country: BO
    URL: https://www.lexivox.org

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "es,en;q=0.5",
        })

    def _get_with_retry(self, url: str, max_retries: int = 3, timeout: int = 60) -> Optional[requests.Response]:
        """GET with retry logic."""
        for attempt in range(max_retries):
            try:
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 200:
                    return resp
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt+1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(3 * (attempt + 1))
        return None

    def _search_norms(self, date_min: str = "1800-01-01",
                      date_max: str = "2026-12-31", offset: int = 0) -> Optional[str]:
        """Fetch a search results page. Type filter doesn't work on LexiVox, so we fetch all."""
        params = {
            "tipo": "L",
            "con_jurisprudencia": "f",
            "fecha_promulgacion_min": date_min,
            "fecha_promulgacion_max": date_max,
            "lang": "es",
        }
        if offset > 0:
            params["sacb524current"] = str(offset)

        url = SEARCH_URL + "?" + urlencode(params)
        resp = self._get_with_retry(url)
        return resp.text if resp else None

    def _parse_search_results(self, html: str) -> List[str]:
        """Extract norm .xhtml URLs from a search results page."""
        soup = BeautifulSoup(html, 'html.parser')
        urls = []
        for link in soup.find_all('a', href=re.compile(r'/norms/BO-.*\.xhtml')):
            href = link.get('href', '')
            if href.startswith('/'):
                href = BASE_URL + href
            elif not href.startswith('http'):
                href = urljoin(BASE_URL + "/norms/", href)
            if href not in urls:
                urls.append(href)
        return urls

    def _get_total_results(self, html: str) -> int:
        """Extract total result count from search page."""
        match = re.search(r'(\d[\d.,]+)\s*(?:registros?|records?|resultados?)', html, re.I)
        if match:
            return int(match.group(1).replace('.', '').replace(',', ''))
        # Count pagination links to estimate
        soup = BeautifulSoup(html, 'html.parser')
        page_links = soup.find_all('a', href=re.compile(r'sacb524current=\d+'))
        if page_links:
            max_offset = 0
            for pl in page_links:
                m = re.search(r'sacb524current=(\d+)', pl.get('href', ''))
                if m:
                    max_offset = max(max_offset, int(m.group(1)))
            return max_offset + 15
        return 0

    def _fetch_norm_page(self, url: str) -> Optional[dict]:
        """Fetch a norm .xhtml page and extract text + metadata."""
        resp = self._get_with_retry(url)
        if not resp:
            return None

        soup = BeautifulSoup(resp.text, 'html.parser')

        # Extract norm ID from URL
        norm_id_match = re.search(r'/norms/(BO-[^.]+)\.xhtml', url)
        norm_id = norm_id_match.group(1) if norm_id_match else url.split('/')[-1].replace('.xhtml', '')

        # Extract title — skip "Contenido" and metadata headings
        title = norm_id
        for h1 in soup.find_all('h1'):
            h1_text = h1.get_text(strip=True)
            if h1_text and h1_text not in ('Contenido', 'Ficha Técnica (DCMI)',
                                            'Enlaces con otros documentos', 'Nota importante'):
                title = h1_text
                break

        # Extract metadata from DCMI/Ficha Tecnica section
        metadata = {}
        ficha = soup.find('table', class_=re.compile(r'dcmi|ficha', re.I))
        if not ficha:
            # Try finding by content
            for table in soup.find_all('table'):
                if table.find(string=re.compile(r'Ficha|DCMI|Norma', re.I)):
                    ficha = table
                    break

        if ficha:
            for row in ficha.find_all('tr'):
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    key = cells[0].get_text(strip=True).rstrip(':').lower()
                    val = cells[1].get_text(strip=True)
                    metadata[key] = val

        # Extract date
        date = metadata.get('fecha', '')
        if not date:
            date_match = re.search(r'(\d{4}-\d{2}-\d{2})', str(metadata))
            date = date_match.group(1) if date_match else None

        # Extract norm type
        norm_type = metadata.get('tipo', '')
        if not norm_type:
            type_match = re.match(r'BO-([A-Z]+)-', norm_id)
            norm_type = type_match.group(1) if type_match else ""

        # Extract body text from all norma divs (page splits content across multiple)
        norma_divs = soup.find_all('div', class_='norma')
        if norma_divs:
            # Skip first norma div (contains table of contents / navigation)
            body_divs = norma_divs[1:] if len(norma_divs) > 1 else norma_divs
            text = '\n\n'.join(d.get_text(separator='\n', strip=True) for d in body_divs)
        else:
            # Fallback: extract from margen div minus metadata
            margen = soup.find('div', class_='margen')
            if margen:
                for t in margen.find_all('table'):
                    t.decompose()
                for t in margen.find_all('div', class_='NOPRINT'):
                    t.decompose()
                text = margen.get_text(separator='\n', strip=True)
            else:
                body = soup.find('body')
                text = body.get_text(separator='\n', strip=True) if body else ""

        # Clean text
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'[ \t]{2,}', ' ', text)
        text = text.strip()

        # Remove "Ficha Técnica" and "Enlaces" sections from end if present
        for marker in ['Ficha Técnica', 'Enlaces con otros documentos', 'Nota importante']:
            idx = text.rfind(marker)
            if idx > len(text) // 2:
                text = text[:idx].strip()

        if len(text) < 50:
            return None

        return {
            "norm_id": norm_id,
            "title": title,
            "text": text,
            "date": date if date else None,
            "url": url,
            "norm_type": norm_type,
            "metadata": metadata,
        }

    def _enumerate_norms(self, max_results: int = 0) -> Generator[str, None, None]:
        """Enumerate norm URLs by paginating through search results."""
        logger.info("Enumerating all norms...")
        time.sleep(2)

        html = self._search_norms()
        if not html:
            logger.warning("No search results")
            return

        total = self._get_total_results(html)
        logger.info(f"Total: ~{total} results")

        seen = set()
        urls = self._parse_search_results(html)
        for u in urls:
            if u not in seen:
                seen.add(u)
                yield u

        if max_results and len(seen) >= max_results:
            return

        # Paginate
        offset = 16  # Page 2 starts at 16
        while offset < total:
            time.sleep(2)
            html = self._search_norms(offset=offset)
            if not html:
                break

            new_urls = self._parse_search_results(html)
            if not new_urls:
                break

            new_count = 0
            for u in new_urls:
                if u not in seen:
                    seen.add(u)
                    new_count += 1
                    yield u

            if new_count == 0:
                break

            if max_results and len(seen) >= max_results:
                break

            offset += 15
            if len(seen) % 100 == 0:
                logger.info(f"Enumerated {len(seen)} norms so far...")

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all norms with full text."""
        total = 0
        skipped = 0

        for url in self._enumerate_norms():
            time.sleep(2)
            record = self._fetch_norm_page(url)
            if not record:
                skipped += 1
                continue

            total += 1
            yield record

            if total % 50 == 0:
                logger.info(f"Progress: {total} docs ({skipped} skipped)")

        logger.info(f"Fetch complete: {total} docs, {skipped} skipped")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch norms published since a date."""
        date_min = since.strftime("%Y-%m-%d")
        date_max = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"Fetching updates since {date_min}")
        time.sleep(2)

        html = self._search_norms(date_min=date_min, date_max=date_max)
        if not html:
            return

        urls = self._parse_search_results(html)
        for url in urls:
            time.sleep(2)
            record = self._fetch_norm_page(url)
            if record:
                yield record

    def fetch_sample(self, count: int = 15) -> Generator[dict, None, None]:
        """Fetch sample norms from first pages of search results."""
        found = 0

        for url in self._enumerate_norms(max_results=count + 10):
            if found >= count:
                break

            time.sleep(2)
            record = self._fetch_norm_page(url)
            if not record:
                continue

            found += 1
            logger.info(
                f"Sample {found}/{count}: {record['norm_id']} "
                f"({len(record['text'])} chars) {record['title'][:60]}"
            )
            yield record

        logger.info(f"Sample complete: {found} records")

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw norm record to standard schema."""
        title = raw.get("title", raw["norm_id"])
        title = re.sub(r'\s+', ' ', title).strip()[:500]

        date = raw.get("date")
        if date and not re.match(r'^\d{4}-\d{2}-\d{2}$', str(date)):
            date_match = re.search(r'(\d{4}-\d{2}-\d{2})', str(date))
            date = date_match.group(1) if date_match else None

        return {
            "_id": f"BO-GacetaOficial-{raw['norm_id']}",
            "_source": "BO/GacetaOficial",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": date,
            "url": raw["url"],
            "norm_id": raw["norm_id"],
            "norm_type": raw.get("norm_type", ""),
        }

    def test_api(self) -> bool:
        """Test connectivity and text extraction."""
        logger.info("Testing LexiVox Bolivia...")

        # Test search
        html = self._search_norms("L")
        if not html:
            logger.error("Failed to fetch search results")
            return False

        urls = self._parse_search_results(html)
        total = self._get_total_results(html)
        logger.info(f"Search OK: {len(urls)} URLs on page 1, ~{total} total")

        if not urls:
            logger.error("No norm URLs found in search results")
            return False

        # Test norm page fetch
        time.sleep(2)
        record = self._fetch_norm_page(urls[0])
        if not record:
            logger.error(f"Failed to fetch norm page: {urls[0]}")
            return False

        logger.info(f"Norm page OK: {record['norm_id']} ({len(record['text'])} chars)")
        logger.info("All tests passed")
        return True


# -- CLI entry point ---------------------------------------------------------

if __name__ == "__main__":
    scraper = BOGacetaOficialScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample] [--count N]")
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
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in scraper.fetch_updates(since):
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1

        logger.info(f"Update complete: {saved} records saved")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
