#!/usr/bin/env python3
"""
PR/LexJuris -- Puerto Rico Laws & Jurisprudence

Fetches Puerto Rico legislation with full text from lexjuris.com.

Strategy:
  - Discover the per-year law menus from the master index (lexleyes.htm).
    The path scheme changed several times (ley1997/lex1997menu.htm,
    Leyes2001/lex2001menu.htm, Leyes2024/lexl2024Menu.htm ...), so the
    menus are read off the index rather than templated.
  - Parse each menu for individual law links: lex[l]{YY|YYYY}{NNN}.htm
  - Fetch each law page and extract every Word "Section" div
    (pre-2022 pages use Section1/Section2, 2022+ use WordSection1).
  - Decode per-page: pages before ~2022 are windows-1252, later ones UTF-8.
  - No robots.txt restrictions; 2-second crawl delay for politeness.

Usage:
  python bootstrap.py bootstrap          # Fetch all legislation
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap-fast     # Same as bootstrap (VPS runner alias)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PR.LexJuris")

BASE_URL = "https://www.lexjuris.com/lexlex"
INDEX_URL = "https://www.lexjuris.com/lexleyes.htm"
FIRST_YEAR = 1997

REPLACEMENT_CHAR = "�"

# Word export wrappers holding the document body. Pre-2022 pages emit
# Section1/Section2/..., 2022+ pages emit WordSection1.
SECTION_CLASS_RE = re.compile(r"^(Word)?Section\d+$")

# Markers that begin the LexJuris site chrome appended after the law text.
FOOTER_MARKERS = (
    "Notas Importantes",
    "ADVERTENCIA",
    "Presione Aquí para regresar",
    "Presione Aqui para regresar",
    "LexJuris de Puerto Rico siempre",
    "-------------------",
)

# Spanish months for date parsing
SPANISH_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}


def decode_html(raw: bytes) -> str:
    """
    Decode a LexJuris page.

    Pages published before ~2022 are windows-1252 (and declare it in a meta
    tag); 2022+ pages are UTF-8 and declare nothing. Forcing UTF-8 on the
    older bytes replaced every accented character with U+FFFD (issue #1410),
    which is lossy and unrecoverable after the fact.

    UTF-8 is tried strictly first: cp1252 text with accents is almost never
    valid UTF-8, so a clean strict decode is reliable evidence of UTF-8.
    """
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        pass
    # cp1252 leaves a few byte values undefined; latin-1 is the safety net.
    try:
        return raw.decode("cp1252")
    except UnicodeDecodeError:
        return raw.decode("latin-1")


class LexJurisScraper(BaseScraper):
    """Scraper for PR/LexJuris -- Puerto Rico legislation."""

    def __init__(self, source_dir: Optional[str] = None):
        super().__init__(source_dir or str(Path(__file__).parent))
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/worldwidelaw/legal-sources)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "es-PR,es;q=0.9,en;q=0.5",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[str]:
        """HTTP GET with 2-second delay and retry. Returns decoded HTML."""
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return decode_html(resp.content)
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _discover_year_menus(self) -> List[Tuple[int, str]]:
        """
        Read the master index for per-year law menus.

        Returns [(year, menu_url), ...] newest first. Falls back to the modern
        URL template if the index is unreachable.
        """
        html = self._request(INDEX_URL)
        menus: Dict[int, str] = {}

        if html:
            soup = BeautifulSoup(html, "html.parser")
            for link in soup.find_all("a", href=True):
                href = link["href"]
                # Menu filenames: lex1997menu.htm / lex2001menu.htm / lexl2024Menu.htm
                m = re.search(r"lexl?(\d{4})menu\.html?$", href, re.IGNORECASE)
                if not m:
                    continue
                year = int(m.group(1))
                if FIRST_YEAR <= year <= datetime.now().year + 1:
                    menus.setdefault(year, urljoin(INDEX_URL, href))
        else:
            logger.warning("Index page unreachable, falling back to URL template")

        # Belt and braces: the index sometimes lags a newly opened year.
        for year in range(FIRST_YEAR, datetime.now().year + 2):
            menus.setdefault(year, f"{BASE_URL}/Leyes{year}/lexl{year}Menu.htm")

        return sorted(menus.items(), reverse=True)

    def _parse_menu_page(self, html: str, year: int, menu_url: str) -> List[Dict[str, Any]]:
        """Parse a year menu page for individual law links."""
        soup = BeautifulSoup(html, "html.parser")
        documents = []
        seen = set()

        # lex97001.htm (1997-1999), lex2000001.htm, lexl2024001.htm
        pattern = re.compile(
            rf"lexl?(?:{year}|{year % 100:02d})(\d{{3}})\.html?$", re.IGNORECASE
        )

        for link in soup.find_all("a", href=True):
            href = link["href"]
            filename = href.split("/")[-1].split("?")[0]
            match = pattern.match(filename)
            if not match:
                continue

            key = filename.lower()
            if key in seen:
                continue
            seen.add(key)

            documents.append({
                "url": urljoin(menu_url, href),
                "filename": filename,
                "law_number": int(match.group(1)),
                "year": year,
                "title_from_menu": link.get_text(strip=True),
            })

        documents.sort(key=lambda d: d["law_number"])
        return documents

    @staticmethod
    def _strip_footer(text: str) -> str:
        """
        Drop the LexJuris site chrome ("Notas Importantes", "ADVERTENCIA",
        navigation bar, copyright) appended after the law text.

        Only markers in the back of the document count, so an "ADVERTENCIA"
        heading inside a law body cannot truncate it.
        """
        floor = max(200, int(len(text) * 0.25))
        cuts = [
            idx for idx in (text.find(marker, floor) for marker in FOOTER_MARKERS)
            if idx != -1
        ]
        return text[:min(cuts)].rstrip() if cuts else text

    def _extract_full_text(self, html: str, law_year: Optional[int] = None) -> Dict[str, str]:
        """Extract full text and metadata from a law page."""
        soup = BeautifulSoup(html, "html.parser")
        result = {"text": "", "date": "", "title": ""}

        title_tag = soup.find("title")
        if title_tag:
            result["title"] = re.sub(r"\s+", " ", title_tag.get_text(strip=True))

        # Concatenate every Word section. Multi-section pages split the
        # preamble (ends at DECRÉTASE) from the articles, so taking only the
        # first one truncated the body (issue #1410).
        sections = soup.find_all(class_=SECTION_CLASS_RE)
        if sections:
            text = "\n".join(s.get_text(separator="\n", strip=True) for s in sections)
        else:
            container = soup.select_one("#content") or soup.find("body")
            text = container.get_text(separator="\n", strip=True) if container else ""

        text = self._strip_footer(text)

        # Clean up whitespace (Word exports are littered with hard CRs)
        text = text.replace("\r\n", "\n").replace("\r", " ").replace("\xa0", " ")
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)
        result["text"] = text.strip()

        # Extract the approval date: "de DD de MONTH de YYYY". Law bodies cite
        # the laws they amend, so only a date in the law's own year is trusted
        # — the first match on the page is often an amended law's date.
        for match in re.finditer(
            r"\bde\s+(\d{1,2})\s+de\s+([A-Za-zÁÉÍÓÚáéíóú]+)\s+de\s+(19\d{2}|20\d{2})",
            result["text"],
            re.IGNORECASE,
        ):
            day, year = int(match.group(1)), int(match.group(3))
            month = SPANISH_MONTHS.get(match.group(2).lower())
            if not month or not 1 <= day <= 31:
                continue
            if law_year is not None and year != law_year:
                continue
            result["date"] = f"{year:04d}-{month:02d}-{day:02d}"
            break

        return result

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        year = raw.get("year", 0)
        law_num = raw.get("law_number", 0)

        return {
            "_id": f"PR-Ley-{year}-{law_num:03d}",
            "_source": "PR/LexJuris",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "law_number": f"Ley Num. {law_num} de {year}",
            "url": raw.get("url", ""),
        }

    def _fetch_year(self, year: int, menu_url: str) -> Generator[Dict[str, Any], None, None]:
        """Yield every law of one year."""
        html = self._request(menu_url)
        if html is None:
            logger.info(f"No menu page for {year}, skipping")
            return

        docs = self._parse_menu_page(html, year, menu_url)
        if not docs:
            logger.info(f"No laws found for {year}")
            return

        logger.info(f"Year {year}: {len(docs)} laws found")

        for doc in docs:
            doc_html = self._request(doc["url"])
            if doc_html is None:
                logger.warning(f"Failed to fetch: Ley {doc['law_number']} de {year}")
                continue

            extracted = self._extract_full_text(doc_html, law_year=year)
            if len(extracted["text"]) < 200:
                logger.warning(
                    f"Insufficient text for Ley {doc['law_number']} de {year}: "
                    f"{len(extracted['text'])} chars"
                )
                continue
            if REPLACEMENT_CHAR in extracted["text"]:
                logger.warning(
                    f"Replacement chars in Ley {doc['law_number']} de {year} "
                    "- charset detection failed"
                )

            yield {
                "year": year,
                "law_number": doc["law_number"],
                "title": extracted["title"] or doc.get("title_from_menu", ""),
                "text": extracted["text"],
                "date": extracted["date"],
                "url": doc["url"],
            }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all legislation from every year menu, newest first."""
        count = 0
        for year, menu_url in self._discover_year_menus():
            for raw in self._fetch_year(year, menu_url):
                count += 1
                yield raw
        logger.info(f"Completed: {count} laws fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent legislation (current year only)."""
        year = datetime.now().year
        menus = dict(self._discover_year_menus())
        menu_url = menus.get(year, f"{BASE_URL}/Leyes{year}/lexl{year}Menu.htm")
        count = 0
        for raw in self._fetch_year(year, menu_url):
            count += 1
            yield raw
        logger.info(f"Updates: {count} laws fetched for {year}")

    def test(self) -> bool:
        """Quick connectivity test across both page generations."""
        menus = dict(self._discover_year_menus())
        logger.info(f"Discovered {len(menus)} year menus ({min(menus)}-{max(menus)})")

        # 2013 is windows-1252 + multi-section; the current year is UTF-8.
        for year in (2013, datetime.now().year):
            menu_url = menus.get(year)
            html = self._request(menu_url) if menu_url else None
            if not html:
                logger.error(f"Cannot reach LexJuris menu for {year}")
                return False

            docs = self._parse_menu_page(html, year, menu_url)
            if not docs:
                logger.error(f"No laws found on {year} menu page")
                return False

            doc_html = self._request(docs[0]["url"])
            if not doc_html:
                logger.error(f"Cannot fetch first law of {year}")
                return False

            extracted = self._extract_full_text(doc_html, law_year=year)
            logger.info(
                f"{year} OK: {len(docs)} laws | Ley {docs[0]['law_number']} "
                f"{len(extracted['text'])} chars | mojibake="
                f"{extracted['text'].count(REPLACEMENT_CHAR)}"
            )
            if len(extracted["text"]) < 200 or REPLACEMENT_CHAR in extracted["text"]:
                return False

        return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="PR/LexJuris data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = LexJurisScraper()

    if args.command == "test":
        sys.exit(0 if scraper.test() else 1)
    elif args.command in ("bootstrap", "bootstrap-fast"):
        scraper.bootstrap(sample_mode=args.sample, sample_size=15)
    elif args.command == "update":
        scraper.update()


if __name__ == "__main__":
    main()
