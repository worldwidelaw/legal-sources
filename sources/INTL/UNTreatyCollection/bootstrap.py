#!/usr/bin/env python3
"""
INTL/UNTreatyCollection -- UN Multilateral Treaty Collection (MTDSG)

Fetches metadata, declarations, reservations, and objections for 419
multilateral treaties deposited with the UN Secretary-General.

Strategy:
  - Scrape 29 chapter pages for treaty IDs
  - Fetch individual treaty XML from MTDSG endpoint
  - Extract structured metadata + textual content from XML
  - ~450 total HTTP requests (29 chapters + ~419 XML files)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import html
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import unquote

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
logger = logging.getLogger("legal-data-hunter.INTL.UNTreatyCollection")

CHAPTER_URL = "https://treaties.un.org/Pages/Treaties.aspx?id={chapter_id}&subid=A&clang=_en"
XML_URL = "https://treaties.un.org/doc/Publication/MTDSG/Volume%20I/Chapter%20{roman}/{treaty_id}.en.xml"

ROMAN = {
    1: "I", 2: "II", 3: "III", 4: "IV", 5: "V", 6: "VI", 7: "VII",
    8: "VIII", 9: "IX", 10: "X", 11: "XI", 12: "XII", 13: "XIII",
    14: "XIV", 15: "XV", 16: "XVI", 17: "XVII", 18: "XVIII", 19: "XIX",
    20: "XX", 21: "XXI", 22: "XXII", 23: "XXIII", 24: "XXIV", 25: "XXV",
    26: "XXVI", 27: "XXVII", 28: "XXVIII", 29: "XXIX",
}


class UNTreatyCollectionScraper(BaseScraper):
    """
    Scraper for INTL/UNTreatyCollection -- UN Multilateral Treaties.
    Country: INTL
    URL: https://treaties.un.org/

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    # ------------------------------------------------------------------
    # Phase 1: Discover treaty IDs from chapter pages
    # ------------------------------------------------------------------
    def _get_treaty_ids(self, max_chapters: Optional[int] = None) -> list[dict]:
        """Scrape chapter pages for all treaty IDs and titles."""
        treaties = []
        chapters = range(1, 30)
        if max_chapters:
            chapters = range(1, min(max_chapters + 1, 30))

        for chapter_id in chapters:
            url = CHAPTER_URL.format(chapter_id=chapter_id)
            logger.info("Fetching chapter %d", chapter_id)
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
            except requests.RequestException as e:
                logger.warning("Failed to fetch chapter %d: %s", chapter_id, e)
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            links = soup.select('a[href*="mtdsg_no"]')
            for a in links:
                href = a.get("href", "")
                m = re.search(r"mtdsg_no=([^&]+)", href)
                if m:
                    treaty_id = unquote(m.group(1))
                    title = a.get_text(strip=True)
                    treaties.append({
                        "chapter_id": chapter_id,
                        "treaty_id": treaty_id,
                        "title": title,
                    })

            logger.info("  Found %d treaties (total: %d)", len(links), len(treaties))
            time.sleep(1)

        return treaties

    # ------------------------------------------------------------------
    # Phase 2: Fetch and parse treaty XML
    # ------------------------------------------------------------------
    def _fetch_treaty_xml(self, chapter_id: int, treaty_id: str) -> Optional[ET.Element]:
        """Fetch the MTDSG XML for a treaty."""
        roman = ROMAN.get(chapter_id, str(chapter_id))
        url = XML_URL.format(roman=roman, treaty_id=treaty_id)
        try:
            resp = self.session.get(url, timeout=30)
            if resp.status_code != 200:
                logger.warning("XML not found for %s (HTTP %d)", treaty_id, resp.status_code)
                return None
            return ET.fromstring(resp.text)
        except (requests.RequestException, ET.ParseError) as e:
            logger.warning("Failed to fetch/parse XML for %s: %s", treaty_id, e)
            return None

    def _extract_text_from_elem(self, elem: Optional[ET.Element]) -> str:
        """Extract all text content from an XML element, stripping tags."""
        if elem is None:
            return ""
        # Use method="text" to get text content, then clean HTML entities/tags
        raw = ET.tostring(elem, encoding="unicode", method="text")
        raw = html.unescape(raw)
        # Strip any remaining HTML-like tags (e.g. <superscript>, <a>, <right>)
        raw = re.sub(r"<[^>]+>", "", raw)
        raw = re.sub(r"\n{3,}", "\n\n", raw)
        raw = re.sub(r"[ \t]+", " ", raw)
        return raw.strip()

    def _parse_treaty_xml(self, root: ET.Element, treaty_info: dict) -> dict:
        """Parse treaty XML into structured data."""
        treaty = root.find("Treaty")
        if treaty is None:
            return {}

        header = treaty.find("Header")
        ext = header.find("ExternalData") if header else None

        # Extract metadata
        title = ""
        conclusion = ""
        eif_text = ""
        parties_count = ""
        registration = ""

        if ext is not None:
            title_elem = ext.find("Titlesect")
            title = title_elem.text.strip() if title_elem is not None and title_elem.text else ""
            title = re.sub(r"<[^>]+>", "", title)  # Strip HTML tags in title
            concl_elem = ext.find("Conclusion")
            conclusion = concl_elem.text.strip() if concl_elem is not None and concl_elem.text else ""
            eif_elem = ext.find("EIF")
            if eif_elem is not None:
                lt = eif_elem.find("Labeltext")
                eif_text = lt.text.strip() if lt is not None and lt.text else ""
            status_elem = ext.find("Status")
            if status_elem is not None:
                p = status_elem.find("Parties")
                parties_count = p.text.strip() if p is not None and p.text else ""
            reg_elem = ext.find("Registration")
            registration = self._extract_text_from_elem(reg_elem)

        # Parse conclusion date
        date = self._parse_date(conclusion)

        # Extract participants table
        participants_text = self._extract_participants(treaty.find("Participants"))

        # Extract declarations, reservations, objections
        declarations = self._extract_text_from_elem(treaty.find("Declarations"))
        objections = self._extract_text_from_elem(treaty.find("Objections"))
        decl_article = self._extract_text_from_elem(treaty.find("DeclarationsUnderArticle"))
        notifications = self._extract_text_from_elem(treaty.find("Notifications"))
        territory = self._extract_text_from_elem(treaty.find("TerritorialApplications"))
        endnotes = self._extract_text_from_elem(treaty.find("EndNotes"))

        # Build full text from all textual sections
        text_parts = []
        if declarations:
            text_parts.append(f"DECLARATIONS AND RESERVATIONS\n\n{declarations}")
        if objections:
            text_parts.append(f"OBJECTIONS\n\n{objections}")
        if decl_article:
            text_parts.append(f"DECLARATIONS UNDER ARTICLE\n\n{decl_article}")
        if notifications:
            text_parts.append(f"NOTIFICATIONS\n\n{notifications}")
        if territory:
            text_parts.append(f"TERRITORIAL APPLICATIONS\n\n{territory}")
        if participants_text:
            text_parts.append(f"PARTICIPANTS\n\n{participants_text}")
        if endnotes:
            text_parts.append(f"NOTES\n\n{endnotes}")

        full_text = "\n\n---\n\n".join(text_parts)

        return {
            "title": title or treaty_info.get("title", ""),
            "conclusion": conclusion,
            "date": date,
            "entry_into_force": eif_text,
            "parties_count": parties_count,
            "registration": registration,
            "text": full_text,
            "chapter_id": treaty_info["chapter_id"],
            "treaty_id": treaty_info["treaty_id"],
        }

    def _parse_date(self, text: str) -> Optional[str]:
        """Extract ISO date from conclusion text like 'San Francisco, 26 June 1945'."""
        months = {
            "January": "01", "February": "02", "March": "03", "April": "04",
            "May": "05", "June": "06", "July": "07", "August": "08",
            "September": "09", "October": "10", "November": "11", "December": "12",
        }
        m = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})", text)
        if m:
            day = m.group(1).zfill(2)
            month = months.get(m.group(2), "01")
            year = m.group(3)
            return f"{year}-{month}-{day}"
        # Try just year
        m2 = re.search(r"(\d{4})", text)
        if m2:
            return f"{m2.group(1)}-01-01"
        return None

    def _extract_participants(self, participants_elem: Optional[ET.Element]) -> str:
        """Extract participant table as readable text."""
        if participants_elem is None:
            return ""
        lines = []
        for row in participants_elem.iter("Row"):
            entries = row.findall("Entry")
            if entries:
                vals = [self._extract_text_from_elem(e).strip() for e in entries]
                vals = [v for v in vals if v]
                if vals:
                    lines.append(" | ".join(vals))
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # BaseScraper interface
    # ------------------------------------------------------------------
    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all UN multilateral treaties. Yields as chapters are processed."""
        logger.info("Starting full fetch of UN Treaty Collection")

        for chapter_id in range(1, 30):
            url = CHAPTER_URL.format(chapter_id=chapter_id)
            logger.info("Fetching chapter %d", chapter_id)
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
            except requests.RequestException as e:
                logger.warning("Failed to fetch chapter %d: %s", chapter_id, e)
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            links = soup.select('a[href*="mtdsg_no"]')
            chapter_treaties = []
            for a in links:
                href = a.get("href", "")
                m = re.search(r"mtdsg_no=([^&]+)", href)
                if m:
                    treaty_id = unquote(m.group(1))
                    title = a.get_text(strip=True)
                    chapter_treaties.append({
                        "chapter_id": chapter_id,
                        "treaty_id": treaty_id,
                        "title": title,
                    })

            logger.info("  Found %d treaties in chapter %d", len(chapter_treaties), chapter_id)
            time.sleep(1)

            for treaty_info in chapter_treaties:
                root = self._fetch_treaty_xml(treaty_info["chapter_id"], treaty_info["treaty_id"])
                if root is None:
                    continue

                parsed = self._parse_treaty_xml(root, treaty_info)
                if not parsed:
                    continue

                record = {
                    "_id": f"UNTC-{treaty_info['treaty_id']}",
                    "_source": "INTL/UNTreatyCollection",
                    "_type": "legislation",
                    "_fetched_at": datetime.now(timezone.utc).isoformat(),
                    "title": re.sub(r"<[^>]+>", "", parsed["title"]),
                    "text": parsed["text"],
                    "date": parsed["date"],
                    "url": f"https://treaties.un.org/Pages/ViewDetails.aspx?src=TREATY&mtdsg_no={treaty_info['treaty_id']}&chapter={treaty_info['chapter_id']}&clang=_en",
                    "conclusion": parsed["conclusion"],
                    "entry_into_force": parsed["entry_into_force"],
                    "parties_count": parsed["parties_count"],
                    "registration": parsed["registration"],
                    "chapter_id": parsed["chapter_id"],
                    "treaty_id": parsed["treaty_id"],
                }

                if record["text"]:
                    yield record
                else:
                    logger.warning("No text content for treaty %s", treaty_info["treaty_id"])
                    yield record

                time.sleep(1)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch updates (re-fetch all - treaties change slowly)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record (already normalized during build)."""
        return raw


# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------
def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="INTL/UNTreatyCollection data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = UNTreatyCollectionScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            # Test chapter listing
            resp = scraper.session.get(
                CHAPTER_URL.format(chapter_id=1), timeout=30
            )
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            links = soup.select('a[href*="mtdsg_no"]')
            logger.info("OK: %d treaties in Chapter I", len(links))

            # Test XML endpoint
            root = scraper._fetch_treaty_xml(1, "I-1")
            if root is not None:
                parsed = scraper._parse_treaty_xml(root, {
                    "chapter_id": 1, "treaty_id": "I-1", "title": "test"
                })
                logger.info("XML parsed: %s (%d chars text)",
                            parsed.get("title", "?")[:60], len(parsed.get("text", "")))
            logger.info("Connectivity test passed!")
        except Exception as e:
            logger.error("Connectivity test failed: %s", e)
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info("Bootstrap complete: %s", json.dumps(stats, indent=2))

    elif args.command == "update":
        stats = scraper.update()
        logger.info("Update complete: %s", json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
