#!/usr/bin/env python3
"""
BR/ANATEL -- ANATEL Brazilian Telecom Regulatory Agency

Fetches telecom resolutions from ANATEL's legislation portal.

Strategy:
  - Scrape year-index pages to discover all resolution URLs
  - Fetch full text from individual Joomla HTML pages
  - Extract article body from <div class="item-page"> or similar

Data: ~765 resolutions (1997-2025). Vigente and Revogada.
License: Open data (public regulatory agency decisions).
Rate limit: 1 req/sec (self-imposed).

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py update --since DATE  # Fetch recent resolutions
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html.parser import HTMLParser

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BR.ANATEL")

BASE_URL = "https://informacoes.anatel.gov.br"
LEGISLATION_URL = f"{BASE_URL}/legislacao"
YEARS = list(range(1997, 2026))


class _HTMLTextExtractor(HTMLParser):
    """Strip HTML tags and extract plain text."""

    def __init__(self):
        super().__init__()
        self._parts = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "head"):
            self._skip = True
        if tag in ("br", "p", "div", "li", "tr", "h1", "h2", "h3", "h4", "h5", "h6"):
            self._parts.append("\n")

    def handle_endtag(self, tag):
        if tag in ("script", "style", "head"):
            self._skip = False
        if tag in ("p", "div", "li", "tr"):
            self._parts.append("\n")

    def handle_data(self, data):
        if not self._skip:
            self._parts.append(data)

    def get_text(self):
        return "".join(self._parts)


def strip_html(html_str: str) -> str:
    """Remove HTML tags and return clean plain text."""
    if not html_str:
        return ""
    extractor = _HTMLTextExtractor()
    try:
        extractor.feed(html_str)
        text = extractor.get_text()
    except Exception:
        text = re.sub(r"<[^>]+>", " ", html_str)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&quot;", '"', text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    lines = [line.strip() for line in text.split("\n")]
    text = "\n".join(lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class ANATELScraper(BaseScraper):
    """
    Scraper for BR/ANATEL -- Brazilian Telecom Regulatory Agency.
    Country: BR
    URL: https://informacoes.anatel.gov.br/legislacao/resolucoes

    Data types: legislation
    Auth: none (public data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html, application/xhtml+xml, */*",
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.5",
            },
            timeout=30,
        )

    # -- Year index scraping ---------------------------------------------------

    def _fetch_year_links(self, year: int) -> list[dict]:
        """Fetch all resolution links from a year index page."""
        url = f"{LEGISLATION_URL}/resolucoes/{year}"
        try:
            resp = self.client.get(url, timeout=30)
            if resp is None or resp.status_code != 200:
                logger.warning(f"Year {year} returned status {resp.status_code if resp else 'None'}")
                return []
            return self._parse_year_index(resp.text, year)
        except Exception as e:
            logger.warning(f"Year {year} index failed: {e}")
            return []

    @staticmethod
    def _parse_year_index(html: str, year: int) -> list[dict]:
        """Parse year index page to extract resolution links and metadata."""
        results = []

        # Look for links to individual resolutions
        # Pattern: /legislacao/resolucoes/YYYY/ID-resolucao-NNN or similar
        link_pattern = re.compile(
            r'href="(/legislacao/resolucoes/' + str(year) + r'/(\d+)-([^"]+))"[^>]*>([^<]+)',
            re.IGNORECASE
        )

        seen_ids = set()
        for match in link_pattern.finditer(html):
            path = match.group(1)
            article_id = int(match.group(2))
            alias = match.group(3)
            link_text = match.group(4).strip()

            if article_id in seen_ids:
                continue
            seen_ids.add(article_id)

            # Extract resolution number from alias or link text
            res_num_match = re.search(r'resoluc[aã]o-?(\d+)', alias, re.IGNORECASE)
            res_num = res_num_match.group(1) if res_num_match else ""

            results.append({
                "article_id": article_id,
                "alias": alias,
                "year": year,
                "path": path,
                "link_text": link_text,
                "res_num": res_num,
            })

        return results

    # -- Full text fetching ----------------------------------------------------

    def _fetch_full_text(self, path: str) -> Optional[tuple[str, str, Optional[str]]]:
        """Fetch full text from a resolution page.
        Returns (title, text, date_str) or None."""
        url = f"{BASE_URL}{path}"
        try:
            resp = self.client.get(url, timeout=30)
            if resp is None or resp.status_code != 200:
                return None
            return self._extract_article_content(resp.text)
        except Exception as e:
            logger.debug(f"Full text fetch failed for {path}: {e}")
            return None

    @staticmethod
    def _extract_article_content(html: str) -> Optional[tuple[str, str, Optional[str]]]:
        """Extract title, body text, and date from a Joomla article page."""
        # Extract title
        title_match = re.search(r'<h[12][^>]*class="[^"]*item-title[^"]*"[^>]*>(.*?)</h[12]>', html, re.DOTALL)
        if not title_match:
            title_match = re.search(r'<title[^>]*>(.*?)</title>', html, re.DOTALL)
        title = strip_html(title_match.group(1)).strip() if title_match else ""

        # Extract article body - try multiple selectors
        body_html = None

        # Try item-page div (Joomla standard)
        item_match = re.search(r'<div[^>]*class="[^"]*item-page[^"]*"[^>]*>(.*)', html, re.DOTALL)
        if item_match:
            body_html = item_match.group(1)

        # Try com-content-article
        if not body_html:
            content_match = re.search(
                r'<div[^>]*class="[^"]*com-content-article[^"]*"[^>]*>(.*)',
                html, re.DOTALL
            )
            if content_match:
                body_html = content_match.group(1)

        # Try article tag
        if not body_html:
            article_match = re.search(r'<article[^>]*>(.*?)</article>', html, re.DOTALL)
            if article_match:
                body_html = article_match.group(1)

        # Fallback: everything between main content markers
        if not body_html:
            main_match = re.search(r'<main[^>]*>(.*?)</main>', html, re.DOTALL)
            if main_match:
                body_html = main_match.group(1)

        if not body_html:
            return None

        text = strip_html(body_html)

        # Extract date - look for publication/creation date
        date_str = None
        months_pt = {
            "janeiro": "01", "fevereiro": "02", "março": "03", "marco": "03",
            "abril": "04", "maio": "05", "junho": "06", "julho": "07",
            "agosto": "08", "setembro": "09", "outubro": "10",
            "novembro": "11", "dezembro": "12",
        }
        months_pt_short = {
            "jan": "01", "fev": "02", "mar": "03", "abr": "04",
            "mai": "05", "jun": "06", "jul": "07", "ago": "08",
            "set": "09", "out": "10", "nov": "11", "dez": "12",
        }

        # Look for structured date in metadata
        date_match = re.search(r'"datePublished"\s*:\s*"(\d{4}-\d{2}-\d{2})', html)
        if date_match:
            date_str = date_match.group(1)

        # Look for "de DD de MES de YYYY" in title/text
        if not date_str:
            pattern = r'de\s+(\d{1,2})\s+de\s+(' + '|'.join(months_pt.keys()) + r')\s+de\s+(\d{4})'
            date_match = re.search(pattern, html.lower())
            if date_match:
                day = date_match.group(1).zfill(2)
                month = months_pt[date_match.group(2)]
                year = date_match.group(3)
                date_str = f"{year}-{month}-{day}"

        # Look for "Publicado: Dia, DD Mes YYYY"
        if not date_str:
            for month_name, month_num in months_pt_short.items():
                pattern = rf'Publicado:.*?(\d{{1,2}})\s+{month_name}\w*\s+(\d{{4}})'
                date_match = re.search(pattern, html, re.IGNORECASE)
                if date_match:
                    day = date_match.group(1).zfill(2)
                    year = date_match.group(2)
                    date_str = f"{year}-{month_num}-{day}"
                    break

        # Fallback: DD/MM/YYYY
        if not date_str:
            date_match = re.search(r'(?:Publicad[ao]|Data)\s*(?:em|:)\s*(\d{1,2})[/.](\d{1,2})[/.](\d{4})', html)
            if date_match:
                date_str = f"{date_match.group(3)}-{date_match.group(2).zfill(2)}-{date_match.group(1).zfill(2)}"

        if len(text) > 100:
            return (title, text, date_str)
        return None

    # -- Record building -------------------------------------------------------

    def _build_record(self, link_info: dict, title: str, text: str, date_str: Optional[str]) -> dict:
        """Build a raw record."""
        return {
            "article_id": link_info["article_id"],
            "alias": link_info["alias"],
            "year": link_info["year"],
            "path": link_info["path"],
            "res_num": link_info.get("res_num", ""),
            "link_text": link_info.get("link_text", ""),
            "title": title,
            "text": text,
            "date": date_str,
        }

    # -- Core scraper methods --------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all resolutions by scraping year index pages."""
        total_found = 0

        for year in sorted(YEARS, reverse=True):
            self.rate_limiter.wait()
            links = self._fetch_year_links(year)
            if not links:
                logger.info(f"Year {year}: no resolutions found")
                continue

            logger.info(f"Year {year}: {len(links)} resolutions found")

            for link_info in links:
                self.rate_limiter.wait()
                result = self._fetch_full_text(link_info["path"])
                if result:
                    title, text, date_str = result
                    if not date_str:
                        date_str = f"{year}-01-01"
                    record = self._build_record(link_info, title, text, date_str)
                    total_found += 1
                    if total_found % 50 == 0:
                        logger.info(f"Progress: {total_found} records")
                    yield record

        logger.info(f"Fetch complete: {total_found} records")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent resolutions."""
        since_year = since.year
        since_str = since.strftime("%Y-%m-%d")
        logger.info(f"Fetching updates since {since.date()}")

        found = 0
        for year in sorted(YEARS, reverse=True):
            if year < since_year:
                break

            self.rate_limiter.wait()
            links = self._fetch_year_links(year)
            if not links:
                continue

            for link_info in links:
                self.rate_limiter.wait()
                result = self._fetch_full_text(link_info["path"])
                if result:
                    title, text, date_str = result
                    if date_str and date_str < since_str:
                        continue
                    if not date_str:
                        date_str = f"{year}-01-01"
                    record = self._build_record(link_info, title, text, date_str)
                    found += 1
                    yield record

        logger.info(f"Update complete: {found} records")

    def fetch_sample(self, count: int = 15) -> Generator[dict, None, None]:
        """Fetch sample records for validation."""
        found = 0

        # Fetch from a few different years for diversity
        sample_years = [2025, 2024, 2020, 2015, 2010, 2005, 2000, 1998]

        for year in sample_years:
            if found >= count:
                break

            self.rate_limiter.wait()
            links = self._fetch_year_links(year)
            if not links:
                continue

            logger.info(f"Year {year}: {len(links)} resolutions")

            for link_info in links[:4]:  # Take up to 4 from each year
                if found >= count:
                    break

                self.rate_limiter.wait()
                result = self._fetch_full_text(link_info["path"])
                if result:
                    title, text, date_str = result
                    if not date_str:
                        date_str = f"{year}-01-01"
                    record = self._build_record(link_info, title, text, date_str)
                    found += 1
                    logger.info(
                        f"Sample {found}/{count}: {title[:60]}... "
                        f"({date_str}) - {len(text)} chars"
                    )
                    yield record

        logger.info(f"Sample complete: {found} records")

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record to standard schema."""
        title = raw.get("title") or raw.get("link_text") or f"Resolução {raw.get('res_num', raw['article_id'])}"

        return {
            "_id": f"BR-ANATEL-{raw['article_id']}",
            "_source": "BR/ANATEL",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": raw.get("date"),
            "url": f"{BASE_URL}{raw['path']}",
            "resolution_number": raw.get("res_num"),
            "year": raw.get("year"),
            "alias": raw.get("alias"),
        }

    def test_api(self) -> bool:
        """Test connectivity."""
        logger.info("Testing BR/ANATEL legislation portal...")

        # Test year index
        logger.info("Testing year index...")
        links = self._fetch_year_links(2024)
        if not links:
            logger.error("Year index not responding")
            return False
        logger.info(f"Year index OK: {len(links)} resolutions for 2024")

        # Test detail page
        logger.info("Testing detail page...")
        result = self._fetch_full_text(links[0]["path"])
        if result:
            title, text, date_str = result
            logger.info(f"Detail OK: {title[:60]}... - {len(text)} chars")
            return True
        else:
            logger.error("Detail page returned no text")
            return False


# -- CLI entry point ---------------------------------------------------------

if __name__ == "__main__":
    scraper = ANATELScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample] [--count N] [--since DATE]")
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
        gen = scraper.fetch_updates(since)

        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in gen:
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1

        logger.info(f"Update complete: {saved} records saved")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
