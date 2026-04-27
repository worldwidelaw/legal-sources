#!/usr/bin/env python3
"""
JE/JerseyLaw -- Jersey Legal Information Board Enacted Laws Fetcher

Fetches enacted legislation from jerseylaw.je. The site is SharePoint-based
with WAF-protected search API, but individual law pages are server-rendered
HTML with full text in a <div class="law"> container.

Strategy:
  - Enumerate enacted laws by URL pattern: L-{num:02d}-{year}.aspx
  - Years range from 1950 to present
  - For each year, increment num from 01 until 404 is returned
  - Extract full text from div.law element

Usage:
  python bootstrap.py bootstrap          # Fetch all enacted laws
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.JE.JerseyLaw")

BASE_URL = "https://www.jerseylaw.je"
ENACTED_URL = BASE_URL + "/laws/enacted/Pages/L-{num:02d}-{year}.aspx"
START_YEAR = 1950
MAX_LAWS_PER_YEAR = 60


class JerseyLawScraper(BaseScraper):
    """Scraper for JE/JerseyLaw -- Jersey enacted legislation."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        """HTTP GET with 1-second crawl delay and retry."""
        for attempt in range(3):
            try:
                time.sleep(1)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return None

    def _extract_law(self, html: str, url: str, law_code: str, year: int) -> Optional[Dict[str, Any]]:
        """Extract law metadata and full text from a law page."""
        soup = BeautifulSoup(html, "html.parser")

        # Extract title from <title> tag
        title_el = soup.find("title")
        title = ""
        if title_el:
            title = title_el.get_text(strip=True)
            # Remove site suffix like " - Jersey Law"
            title = re.sub(r"\s*[-–—]\s*Jersey\s*Law\s*$", "", title, flags=re.IGNORECASE)
            title = title.strip()

        # Extract full text from div.law
        law_div = soup.find("div", class_="law")
        if not law_div:
            logger.warning(f"No div.law found for {law_code}")
            return None

        # Remove script/style tags
        for tag in law_div.find_all(["script", "style"]):
            tag.decompose()

        text = law_div.get_text(separator="\n", strip=True)
        # Clean up excessive whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)
        text = text.strip()

        if len(text) < 50:
            logger.warning(f"Text too short for {law_code}: {len(text)} chars")
            return None

        return {
            "_id": f"JE/JerseyLaw/{law_code}",
            "_source": "JE/JerseyLaw",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": f"{year}-01-01",
            "url": url,
            "law_code": law_code,
            "year": year,
            "jurisdiction": "JE",
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all enacted laws from 1950 to present."""
        current_year = datetime.now().year
        total = 0

        for year in range(START_YEAR, current_year + 1):
            year_count = 0
            for num in range(1, MAX_LAWS_PER_YEAR + 1):
                law_code = f"L-{num:02d}-{year}"
                url = ENACTED_URL.format(num=num, year=year)
                resp = self._request(url)

                if resp is None:
                    # 404 or error — no more laws for this year
                    break

                if 'class="law"' not in resp.text:
                    break

                record = self._extract_law(resp.text, url, law_code, year)
                if record:
                    total += 1
                    year_count += 1
                    yield record

            if year_count > 0:
                logger.info(f"Year {year}: {year_count} laws fetched (total: {total})")

        logger.info(f"Completed: {total} enacted laws fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch laws from the current year only."""
        current_year = datetime.now().year
        for num in range(1, MAX_LAWS_PER_YEAR + 1):
            law_code = f"L-{num:02d}-{current_year}"
            url = ENACTED_URL.format(num=num, year=current_year)
            resp = self._request(url)
            if resp is None or 'class="law"' not in resp.text:
                break
            record = self._extract_law(resp.text, url, law_code, current_year)
            if record:
                yield record

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Records are already normalized during extraction."""
        return raw


def main():
    scraper = JerseyLawScraper()
    args = sys.argv[1:]

    if not args or args[0] == "test":
        logger.info("Testing connectivity to jerseylaw.je...")
        url = ENACTED_URL.format(num=1, year=2020)
        resp = scraper._request(url)
        if resp and resp.status_code == 200 and 'class="law"' in resp.text:
            logger.info("SUCCESS: Connected and found law content")
            record = scraper._extract_law(resp.text, url, "L-01-2020", 2020)
            if record:
                logger.info(f"Title: {record['title']}")
                logger.info(f"Text length: {len(record['text'])} chars")
                logger.info(f"Text preview: {record['text'][:200]}...")
            return 0
        else:
            logger.error("FAILED: Could not connect or find law content")
            return 1

    if args[0] == "bootstrap":
        sample_mode = "--sample" in args
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        sample_limit = 15 if sample_mode else float("inf")

        if sample_mode:
            # Sample from recent years for speed
            sample_years = [2025, 2024, 2023, 2022, 2021]
            for year in sample_years:
                if count >= sample_limit:
                    break
                for num in range(1, 6):
                    if count >= sample_limit:
                        break
                    law_code = f"L-{num:02d}-{year}"
                    url = ENACTED_URL.format(num=num, year=year)
                    resp = scraper._request(url)
                    if resp is None or 'class="law"' not in resp.text:
                        break
                    record = scraper._extract_law(resp.text, url, law_code, year)
                    if record:
                        count += 1
                        out = sample_dir / f"{law_code}.json"
                        with open(out, "w", encoding="utf-8") as f:
                            json.dump(record, f, ensure_ascii=False, indent=2)
                        logger.info(f"[{count}] {law_code}: {record['title'][:60]} ({len(record['text'])} chars)")
        else:
            for record in scraper.fetch_all():
                count += 1
                out = sample_dir / f"{record['law_code']}.json"
                with open(out, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                if count % 50 == 0:
                    logger.info(f"Progress: {count} laws saved")

        logger.info(f"Bootstrap complete: {count} laws saved to {sample_dir}")
        return 0

    print(f"Unknown command: {args[0]}")
    print("Usage: python bootstrap.py [test|bootstrap] [--sample]")
    return 1


if __name__ == "__main__":
    sys.exit(main() or 0)
