#!/usr/bin/env python3
"""
INTL/SADC-Tribunal -- SADC Tribunal Decisions

Fetches all 16 SADC Tribunal decisions (2007-2010) from SAFLII.
The Tribunal was suspended in 2010; no new decisions since then.

Strategy:
  - Scrape year index pages from SAFLII: /sa/cases/SADCT/{year}/
  - Fetch each HTML decision page for full text
  - One decision (2008/3) is PDF-only; skip if no PDF parser available

Usage:
  python bootstrap.py bootstrap          # Fetch all 16 decisions
  python bootstrap.py bootstrap --sample # Same (only 16 total)
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.SADC-Tribunal")

SAFLII_BASE = "https://www.saflii.org"
SADCT_BASE = f"{SAFLII_BASE}/sa/cases/SADCT"
YEARS = [2007, 2008, 2009, 2010]

# Known decisions with metadata (from SAFLII index pages)
KNOWN_DECISIONS = [
    {"year": 2007, "num": 1, "ext": "html", "title": "Mike Campbell (Pvt) Ltd and Others v Republic of Zimbabwe (2/07) [2007] SADCT 1", "date": "2007-12-13"},
    {"year": 2008, "num": 1, "ext": "html", "title": "Nixon Chirinda and Others v Mike Campbell (Pvt) Ltd and Others (09/08) [2008] SADCT 1", "date": "2008-09-17"},
    {"year": 2008, "num": 2, "ext": "html", "title": "Mike Campbell (Pvt) Ltd and Others v Republic of Zimbabwe (2/2007) [2008] SADCT 2", "date": "2008-11-28"},
    {"year": 2008, "num": 3, "ext": "pdf", "title": "Mtingwi v SADC Secretariat (1/2007) [2008] SADCT 3", "date": "2008-05-27"},
    {"year": 2009, "num": 1, "ext": "html", "title": "Campbell v Republic of Zimbabwe (03/2009) [2009] SADCT 1", "date": "2009-06-05"},
    {"year": 2009, "num": 2, "ext": "html", "title": "Zimbabwe Human Rights NGO Forum v Republic of Zimbabwe (05/2008) [2009] SADCT 2", "date": "2009-01-01"},
    {"year": 2009, "num": 3, "ext": "html", "title": "Tembani and Others v Republic of Zimbabwe (07/2008) [2009] SADCT 3", "date": "2009-08-14"},
    {"year": 2009, "num": 4, "ext": "html", "title": "United Peoples' Party of South Africa v SADC and Others (12/2008) [2009] SADCT 4", "date": "2009-08-14"},
    {"year": 2010, "num": 1, "ext": "html", "title": "Kanyama v SADC Secretariat (05/2009) [2010] SADCT 1", "date": "2010-01-29"},
    {"year": 2010, "num": 2, "ext": "html", "title": "Kethusegile-Juru v SADC Parliamentary Forum (02/2009) [2010] SADCT 2", "date": "2010-02-05"},
    {"year": 2010, "num": 3, "ext": "html", "title": "Mondlane v SADC Secretariat (07/2009) [2010] SADCT 3", "date": "2010-02-05"},
    {"year": 2010, "num": 4, "ext": "html", "title": "Swissbourgh Diamond Mines (Pty) Ltd v Kingdom of Lesotho (04/2009) [2010] SADCT 4", "date": "2010-06-11"},
    {"year": 2010, "num": 5, "ext": "html", "title": "United Republic of Tanzania v Cimexpan (Mauritius) Ltd and Others (01/2009) [2010] SADCT 5", "date": "2010-06-11"},
    {"year": 2010, "num": 6, "ext": "html", "title": "Bach's Transport (Pty) Ltd v Democratic Republic of Congo (14/2008) [2010] SADCT 6", "date": "2010-06-11"},
    {"year": 2010, "num": 7, "ext": "html", "title": "Kethusegile-Juru v SADC Parliamentary Forum (02/2009) [2010] SADCT 7", "date": "2010-06-11"},
    {"year": 2010, "num": 8, "ext": "html", "title": "Fick and Others v Republic of Zimbabwe (01/2010) [2010] SADCT 8", "date": "2010-07-16"},
]


class SADCTribunalScraper(BaseScraper):
    SOURCE_ID = "INTL/SADC-Tribunal"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
        })

    def _fetch_html(self, url: str) -> Optional[str]:
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=60)
                resp.raise_for_status()
                return resp.text
            except requests.RequestException as e:
                if attempt == 2:
                    logger.warning("Failed to fetch %s: %s", url, e)
                    return None
                time.sleep(2 * (attempt + 1))

    @staticmethod
    def _extract_judgment_text(html: str) -> str:
        """Extract the judgment text from a SAFLII decision page."""
        soup = BeautifulSoup(html, "html.parser")
        # SAFLII uses a div with class "copy" or the main content area
        content = soup.find("div", class_="copy")
        if not content:
            content = soup.find("article")
        if not content:
            # Fall back to body, remove nav/header/footer
            content = soup.find("body")
            if content:
                for tag in content.find_all(["nav", "header", "footer", "script", "style", "aside"]):
                    tag.decompose()
        if not content:
            return ""
        text = content.get_text(separator="\n")
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def test_connection(self) -> bool:
        try:
            html = self._fetch_html(f"{SADCT_BASE}/")
            if html and "SADCT" in html:
                logger.info("Connection OK: SAFLII SADC Tribunal index accessible")
                return True
            logger.error("Unexpected response from SAFLII")
            return False
        except Exception as e:
            logger.error("Connection failed: %s", e)
            return False

    def fetch_all(self) -> Generator[Dict, None, None]:
        logger.info("Fetching all %d SADC Tribunal decisions from SAFLII...", len(KNOWN_DECISIONS))

        for i, dec in enumerate(KNOWN_DECISIONS):
            url = f"{SADCT_BASE}/{dec['year']}/{dec['num']}.{dec['ext']}"
            logger.info("[%d/%d] %s", i + 1, len(KNOWN_DECISIONS), dec["title"][:70])

            if dec["ext"] == "pdf":
                logger.warning("Skipping PDF-only decision: %s", dec["title"])
                continue

            self.rate_limiter.wait()
            html = self._fetch_html(url)
            if not html:
                logger.warning("Failed to fetch: %s", url)
                continue

            text = self._extract_judgment_text(html)
            if not text:
                logger.warning("No text extracted from: %s", url)
                continue

            yield {
                "year": dec["year"],
                "num": dec["num"],
                "title": dec["title"],
                "date": dec["date"],
                "url": url,
                "text": text,
            }

    def fetch_updates(self, since: datetime) -> Generator[Dict, None, None]:
        # Tribunal suspended since 2010, no updates possible
        return
        yield

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"SADCT-{raw['year']}-{raw['num']}",
            "_source": "INTL/SADC-Tribunal",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw["date"],
            "url": raw["url"],
            "citation": f"[{raw['year']}] SADCT {raw['num']}",
        }

    def run_bootstrap(self, sample: bool = False):
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for raw in self.fetch_all():
            normalized = self.normalize(raw)
            fname = re.sub(r'[^\w\-.]', '_', f"{normalized['_id']}.json")
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info("  -> %d chars of text", len(normalized["text"]))

            if sample and count >= 15:
                break

        logger.info("Bootstrap complete: %d records saved", count)
        return count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="INTL/SADC-Tribunal Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = SADCTribunalScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        scraper.run_bootstrap(sample=args.sample)
    elif args.command == "update":
        logger.info("No updates: SADC Tribunal suspended since 2010")


if __name__ == "__main__":
    main()
