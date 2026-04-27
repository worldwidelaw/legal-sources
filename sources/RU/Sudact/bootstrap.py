#!/usr/bin/env python3
"""
RU/Sudact -- Russian Court Decisions Fetcher (sudact.ru)

Fetches case law from sudact.ru, the largest open database of Russian court
decisions. Covers general jurisdiction courts, arbitration courts, magistrate
courts, and the Supreme Court across all 85+ Russian federal subjects.

Strategy:
  - Bootstrap: Reads sitemap XML index to enumerate decision URLs, then
    fetches individual decision pages and extracts full text from HTML.
  - Update: Uses the AJAX search API to find decisions by date range.
  - Sample: Fetches 15 records from the sitemap for validation.

Data source: https://sudact.ru/
Sitemap: https://sudact.ru/sitemap.xml

Usage:
  python bootstrap.py bootstrap            # Full fetch (100K+ records)
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py update               # Fetch recent decisions
"""

import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from lxml import etree

import requests
from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.RU.Sudact")

SITEMAP_INDEX_URL = "https://sudact.ru/sitemap.xml"
SITEMAP_PART_URLS = [
    "https://sudact.ru/sitemap_part_0.xml.gz",
    "https://sudact.ru/sitemap_part_1.xml.gz",
]
SITEMAP_NS = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}

# Court type mapping from URL path
COURT_TYPE_MAP = {
    "regular": "general_jurisdiction",
    "arbitral": "arbitration",
    "magistrate": "magistrate",
    "vsrf": "supreme_court",
}

# Russian month names for date parsing
RUSSIAN_MONTHS = {
    "января": "01", "февраля": "02", "марта": "03",
    "апреля": "04", "мая": "05", "июня": "06",
    "июля": "07", "августа": "08", "сентября": "09",
    "октября": "10", "ноября": "11", "декабря": "12",
}


def parse_russian_date(text: str) -> Optional[str]:
    """Parse Russian date like '8 октября 2025 г.' to ISO format."""
    if not text:
        return None
    m = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})", text)
    if m:
        day, month_name, year = m.group(1), m.group(2), m.group(3)
        month = RUSSIAN_MONTHS.get(month_name.lower())
        if month:
            return f"{year}-{month}-{int(day):02d}"
    # Try DD.MM.YYYY format
    m2 = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", text)
    if m2:
        return f"{m2.group(3)}-{m2.group(2)}-{m2.group(1)}"
    return None


class SudactScraper(BaseScraper):
    """Scraper for sudact.ru Russian court decisions."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        })

    def _fetch_sitemap_urls(self, limit: int = 0) -> list:
        """Fetch decision URLs from sitemap XML files."""
        all_urls = []
        for sitemap_url in SITEMAP_PART_URLS:
            logger.info(f"Fetching sitemap: {sitemap_url}")
            try:
                r = self.session.get(sitemap_url, timeout=120)
                r.raise_for_status()
            except requests.RequestException as e:
                logger.warning(f"Failed to fetch sitemap {sitemap_url}: {e}")
                continue

            # Parse XML (may be raw XML despite .gz extension)
            try:
                root = etree.fromstring(r.content)
            except etree.XMLSyntaxError:
                # Try decompressing
                import gzip
                try:
                    data = gzip.decompress(r.content)
                    root = etree.fromstring(data)
                except Exception:
                    logger.warning(f"Cannot parse sitemap {sitemap_url}")
                    continue

            urls = [loc.text for loc in root.findall(".//s:url/s:loc", SITEMAP_NS)]
            # Filter to doc pages only
            urls = [u for u in urls if "/doc/" in u]
            all_urls.extend(urls)
            logger.info(f"  Found {len(urls)} decision URLs in {sitemap_url}")

            if limit and len(all_urls) >= limit:
                all_urls = all_urls[:limit]
                break

        logger.info(f"Total decision URLs from sitemaps: {len(all_urls)}")
        return all_urls

    def _extract_decision(self, url: str) -> Optional[dict]:
        """Fetch a decision page and extract structured data."""
        try:
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.debug(f"Failed to fetch {url}: {e}")
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        # Extract JSON-LD metadata
        json_ld = {}
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string)
                if data.get("@type") == "Article":
                    json_ld = data
                    break
            except (json.JSONDecodeError, TypeError):
                continue

        # Extract title from h1
        h1 = soup.find("h1")
        title = h1.get_text(strip=True) if h1 else ""

        # Extract court info from div.b-justice
        court_div = soup.find("div", class_="b-justice")
        court_name = ""
        case_category = ""
        uid = ""
        if court_div:
            # Court name is in the first link or text
            court_link = court_div.find("a")
            if court_link:
                court_name = court_link.get_text(strip=True)
            else:
                court_name = court_div.get_text(strip=True).split("-")[0].strip()

            # Category (Гражданское, Уголовное, etc.)
            cat_span = court_div.find("span", class_="b-doc-category")
            if cat_span:
                case_category = cat_span.get_text(strip=True).strip("- ")

            # UID
            uid_span = court_div.find("span", class_="b-doc-uid")
            if uid_span:
                uid_text = uid_span.get_text(strip=True)
                uid = uid_text.replace("УИД-", "").replace("УИД —", "").strip()

        # Fallback court name from JSON-LD
        if not court_name and json_ld.get("author", {}).get("name"):
            court_name = json_ld["author"]["name"]

        # Extract full text from the main content cell
        text = self._extract_text(soup)

        if not text or len(text) < 100:
            logger.debug(f"Insufficient text ({len(text) if text else 0} chars) at {url}")
            return None

        # Extract date from title or JSON-LD
        date = None
        if title:
            date = parse_russian_date(title)
        if not date and json_ld.get("dateModified"):
            date = json_ld["dateModified"][:10]

        # Extract case number from title
        case_number = ""
        m = re.search(r"по делу\s*№?\s*(.+?)$", title)
        if m:
            case_number = m.group(1).strip()

        # Determine court type from URL
        court_type = "unknown"
        for path_key, ct in COURT_TYPE_MAP.items():
            if f"/{path_key}/" in url:
                court_type = ct
                break

        # Extract doc ID from URL
        doc_id_match = re.search(r"/doc/([A-Za-z0-9]+)/?$", url)
        doc_id = doc_id_match.group(1) if doc_id_match else url

        return {
            "doc_id": doc_id,
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "court": court_name,
            "court_type": court_type,
            "case_number": case_number,
            "case_category": case_category,
            "uid": uid,
        }

    def _extract_text(self, soup: BeautifulSoup) -> str:
        """Extract the decision full text from the page HTML."""
        # The text is in td.h-col1 inside the content table
        cell = soup.find("td", class_="h-col1")
        if not cell:
            return ""

        # Remove non-text elements: scripts, styles, ads, navigation
        for tag in cell.find_all(["script", "style", "noscript", "iframe"]):
            tag.decompose()

        # Remove the right-side menu column and action buttons
        for cls in ["h-col2-rightmenu", "b-doc-actions", "b-doc-menu", "b-justice-menu"]:
            for el in cell.find_all(class_=cls):
                el.decompose()

        # Remove the "Документы по делу" section (related docs at the bottom)
        for el in cell.find_all("div", class_="b-case-docs"):
            el.decompose()

        # Remove the "Судебная практика по:" section at the bottom
        for el in cell.find_all("div", class_="b-doc-practice"):
            el.decompose()

        # Remove the metadata table at the bottom (Суд:, Истцы:, etc.)
        for br_tag in cell.find_all("br"):
            sibling_text = br_tag.next_sibling
            if sibling_text and isinstance(sibling_text, str) and "Суд:" in sibling_text:
                # Remove everything from this point
                parent = br_tag.parent
                if parent:
                    for sib in list(br_tag.next_siblings):
                        if hasattr(sib, "decompose"):
                            sib.decompose()
                        else:
                            sib.extract()
                    br_tag.decompose()

        # Get text, preserving paragraph breaks
        # Replace block elements with newlines
        for br in cell.find_all("br"):
            br.replace_with("\n")
        for p in cell.find_all("p"):
            p.insert_before("\n")
            p.insert_after("\n")
        for div in cell.find_all("div"):
            div.insert_before("\n")

        text = cell.get_text()

        # Clean up the text
        # Remove the h1 title (already extracted separately)
        h1 = soup.find("h1")
        if h1:
            h1_text = h1.get_text(strip=True)
            text = text.replace(h1_text, "", 1)

        # Clean whitespace
        lines = [line.strip() for line in text.split("\n")]
        lines = [line for line in lines if line]
        text = "\n".join(lines)

        # Remove ad markers
        text = re.sub(r"adfox_\d+", "", text)
        # Remove remaining JS artifacts
        text = re.sub(r"window\.Ya\.adfoxCode\.create[^;]+;", "", text)

        return text.strip()

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decisions from the sitemap."""
        urls = self._fetch_sitemap_urls()
        logger.info(f"Starting bootstrap: {len(urls)} decisions to fetch")

        for i, url in enumerate(urls):
            if i > 0 and i % 100 == 0:
                logger.info(f"Progress: {i}/{len(urls)} decisions fetched")

            decision = self._extract_decision(url)
            if decision:
                yield decision

            # Rate limit
            time.sleep(1.0)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent decisions using the sitemap (no incremental API)."""
        # Re-fetch sitemap and yield all (dedup handles filtering)
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw decision data into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 50:
            return None

        return {
            "_id": f"RU-Sudact-{raw['doc_id']}",
            "_source": "RU/Sudact",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "court": raw.get("court", ""),
            "court_type": raw.get("court_type", ""),
            "case_number": raw.get("case_number", ""),
            "case_category": raw.get("case_category", ""),
            "uid": raw.get("uid", ""),
            "language": "ru",
        }


# ── CLI entry point ─────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="RU/Sudact data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Sample mode: fetch only 15 records")
    parser.add_argument("--full", action="store_true",
                        help="Full mode: fetch all records")
    args = parser.parse_args()

    scraper = SudactScraper()

    if args.command == "bootstrap":
        if args.sample:
            logger.info("=== SAMPLE MODE: fetching 15 records ===")
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
        else:
            stats = scraper.bootstrap(sample_mode=not args.full)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        last_run = scraper.status.get("last_run")
        since = datetime.fromisoformat(last_run) if last_run else datetime(2020, 1, 1, tzinfo=timezone.utc)
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
