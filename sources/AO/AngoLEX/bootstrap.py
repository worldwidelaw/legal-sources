#!/usr/bin/env python3
"""
AO/AngoLEX -- Angolan Legislation Portal

Scrapes Angolan legislation from angolex.com, a comprehensive portal covering
Constitution, codes, laws, presidential decrees, executive decrees, dispatches,
regulations, resolutions, and international agreements. ~2,378 entries.

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch 15+ sample records
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import re
import json
import logging
import hashlib
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AO.AngoLEX")

BASE_URL = "https://angolex.com/"
TODOS_URL = BASE_URL + "paginas/tabelas-legislacoes/todos.html"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/)",
    "Accept": "text/html, */*",
    "Accept-Language": "pt,en;q=0.5",
}

# Portuguese month names for date parsing
PT_MONTHS = {
    "janeiro": 1, "fevereiro": 2, "março": 3, "abril": 4,
    "maio": 5, "junho": 6, "julho": 7, "agosto": 8,
    "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12,
}

# Category listing pages and their subdirectory patterns (fallback if "todos" misses entries)
CATEGORY_PAGES = [
    "leis.html", "codigos.html", "decretos-legislativos-presidencial.html",
    "decretos-presidencial.html", "decretos-executivos.html",
    "decretos-executivos-conjuntos.html", "despachos.html",
    "regulamentos.html", "resolucoes.html", "avisos.html",
    "instrutivos.html", "acordos-internacionais.html",
]

# Map subdirectory to legislation type
SUBDIR_TYPES = {
    "leis": "Lei",
    "codigos": "Código",
    "decreto-presidencial": "Decreto Presidencial",
    "decreto-executivo": "Decreto Executivo",
    "decreto-executivo-conjunto": "Decreto Executivo Conjunto",
    "decreto-legislativo-presidencial": "Decreto Legislativo Presidencial",
    "despachos": "Despacho",
    "regulamentos": "Regulamento",
    "resolucoes": "Resolução",
    "avisos": "Aviso",
    "instrutivo": "Instrutivo",
    "acordos-internacionais": "Acordo Internacional",
    "deliberacoes": "Deliberação",
    "outros": "Outro",
    "postura": "Postura",
}


class AOAngoLEXScraper(BaseScraper):
    """Scraper for AO/AngoLEX - Angolan legislation portal."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = None

    def _get_session(self):
        if self.session is None:
            import requests
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry

            self.session = requests.Session()
            self.session.headers.update(_HEADERS)

            retry_strategy = Retry(
                total=3,
                backoff_factor=2,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET"],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self.session.mount("https://", adapter)
            self.session.mount("http://", adapter)
        return self.session

    def _fetch_page(self, url: str) -> str:
        """Fetch an HTML page. Returns content string."""
        self.rate_limiter.wait()
        sess = self._get_session()
        try:
            resp = sess.get(url, timeout=30)
            resp.raise_for_status()
            return resp.content.decode("utf-8", errors="replace")
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return ""

    def _parse_listing(self, html: str) -> list:
        """Parse a listing page to extract law entries.

        Returns list of dicts: {url, title, reference, subdir}
        """
        entries = []
        for match in re.finditer(
            r'<td[^>]*><a href="(\.\./([^/]+)/[^"]+\.html)">([^<]+)</a>\s*-?\s*([^<]*)</td>',
            html,
        ):
            rel_url = match.group(1)
            subdir = match.group(2)
            title = match.group(3).strip()
            reference = match.group(4).strip()

            # Convert relative URL to absolute
            abs_url = urljoin(
                BASE_URL + "paginas/tabelas-legislacoes/", rel_url
            )

            entries.append({
                "url": abs_url,
                "title": title,
                "reference": reference,
                "subdir": subdir,
            })
        return entries

    def _get_all_entries(self) -> list:
        """Get all law entries from the 'todos' listing page."""
        html = self._fetch_page(TODOS_URL)
        if not html:
            logger.error("Failed to fetch todos listing page")
            return []

        entries = self._parse_listing(html)
        logger.info(f"Parsed {len(entries)} entries from todos page")

        # Deduplicate by URL
        seen = set()
        unique = []
        for e in entries:
            if e["url"] not in seen:
                seen.add(e["url"])
                unique.append(e)

        logger.info(f"Unique entries: {len(unique)}")
        return unique

    def _extract_text(self, html: str) -> str:
        """Extract law full text from a detail page HTML.

        Finds content between the first CAPÍTULO/Artigo heading and the
        presidential/assembly signature at the end.
        """
        # Find body start: first substantive heading
        body_start = re.search(
            r'<h[4-6][^>]*>\s*(?:CAPÍTULO|TÍTULO|PREÂMBULO|Artigo\s+1|PARTE)',
            html,
        )
        if not body_start:
            # Fallback: find content after the H1 title
            h1 = re.search(r'</h1>', html)
            if h1:
                body_start = h1
            else:
                return ""

        content = html[body_start.start():]

        # Find content end: signature line
        end_patterns = [
            r'Presidente da República[^<]*(?:LOURENÇO|Santos|Neto)\.',
            r'Presidente da Assembleia Nacional[^<]*\.',
            r'O Presidente[^<]*\.',
            r'Publique-se\.',
        ]
        end_pos = len(content)
        for pat in end_patterns:
            m = re.search(pat, content, re.DOTALL)
            if m and m.end() < end_pos:
                end_pos = m.end()
                break

        if end_pos == len(content):
            # Fallback: cut at footer
            footer = re.search(r'Todos os direitos reservados', content)
            if footer:
                end_pos = footer.start()

        content = content[:end_pos]

        # Remove SUMÁRIO / table of contents section
        content = re.sub(
            r'<h\d[^>]*>\s*SUMÁRIO.*?</ol>', '', content, flags=re.DOTALL
        )

        # Strip HTML tags
        text = re.sub(r'<script[^>]*>.*?</script>', '', content, flags=re.DOTALL)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
        text = re.sub(r'&#\d+;?', '', text)
        text = re.sub(r'<[^>]+>', '\n', text)
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'&amp;', '&', text)
        text = re.sub(r'&[a-z]+;', ' ', text)
        text = re.sub(r'Início da Página', '', text)
        text = re.sub(r'OCULTAR|EXPANDIR', '', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _parse_date(self, reference: str) -> Optional[str]:
        """Extract date from a reference string like 'Lei n.º 1/25, de 12 de Março'.

        The year is usually embedded in the law number (e.g., n.º 2/24 → 2024)
        or given explicitly after the month (e.g., de 28 de Junho de 1888).
        """
        # Extract day and month
        m = re.search(
            r'de\s+(\d{1,2})\s+de\s+(\w+)(?:\s+de\s+(\d{4}))?',
            reference,
            re.IGNORECASE,
        )
        if not m:
            return None

        day = int(m.group(1))
        month_name = m.group(2).lower()
        month = PT_MONTHS.get(month_name)
        if not month:
            return None

        year = None

        # First: explicit year after month (e.g., "de 28 de Junho de 1888")
        if m.group(3):
            year = int(m.group(3))

        # Second: year from law number (e.g., "n.º 2/24" or "n.º 14/2019")
        if year is None:
            yr_match = re.search(r'n\.º\s*[\d\s]+/(\d{2,4})', reference)
            if yr_match:
                yr = int(yr_match.group(1))
                if yr >= 100:
                    year = yr
                elif yr <= 30:
                    year = 2000 + yr
                else:
                    year = 1900 + yr

        if year is None:
            return None

        try:
            return f"{year}-{month:02d}-{day:02d}"
        except (ValueError, OverflowError):
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all law records with full text."""
        entries = self._get_all_entries()
        if not entries:
            return

        yielded = 0
        for i, entry in enumerate(entries):
            html = self._fetch_page(entry["url"])
            if not html:
                continue

            text = self._extract_text(html)
            if not text or len(text) < 50:
                logger.warning(
                    f"Insufficient text ({len(text)} chars) from {entry['url']}"
                )
                continue

            yielded += 1
            if yielded % 50 == 0:
                logger.info(f"Progress: {yielded}/{len(entries)} records")

            yield {
                "url": entry["url"],
                "title": entry["title"],
                "reference": entry["reference"],
                "subdir": entry["subdir"],
                "text": text,
            }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Incremental updates — re-fetch all (static site has no date filtering)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw data into standardized record."""
        text = raw.get("text", "")
        if not text or len(text) < 50:
            return None

        title = raw.get("title", "")
        reference = raw.get("reference", "")
        url = raw.get("url", "")
        subdir = raw.get("subdir", "")

        full_title = f"{title} — {reference}" if reference else title
        date_str = self._parse_date(reference)

        # Determine legislation subtype
        leg_type = SUBDIR_TYPES.get(subdir, "Legislação")

        # Stable ID from URL slug
        slug = url.rstrip("/").split("/")[-1].replace(".html", "")
        doc_id = f"AO-ALX-{hashlib.md5(slug.encode()).hexdigest()[:10]}"

        return {
            "_id": doc_id,
            "_source": "AO/AngoLEX",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": full_title,
            "text": text,
            "date": date_str,
            "url": url,
            "reference": reference,
            "legislation_type": leg_type,
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = AOAngoLEXScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        import requests
        try:
            resp = requests.get(TODOS_URL, headers=_HEADERS, timeout=15)
            print(f"HTTP {resp.status_code}")
            print(f"Page size: {len(resp.text)} chars")
            entries = scraper._parse_listing(resp.text)
            print(f"Entries found: {len(entries)}")
            if entries:
                print(f"First: {entries[0]['title']}")
            print("Connection OK")
        except Exception as e:
            print(f"Connection FAILED: {e}")
            sys.exit(1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(f"\nBootstrap complete:")
        print(f"  Records fetched: {stats['records_fetched']}")
        if sample_mode:
            print(f"  Sample records saved: {stats.get('sample_records_saved', 0)}")
        else:
            print(f"  New: {stats['records_new']}")
            print(f"  Updated: {stats['records_updated']}")
            print(f"  Skipped: {stats['records_skipped']}")
        print(f"  Errors: {stats['errors']}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
