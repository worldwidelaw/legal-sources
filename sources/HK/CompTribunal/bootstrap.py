#!/usr/bin/env python3
"""
HK/CompTribunal — Hong Kong Competition Tribunal Judgments

Fetches judgments from the Competition Tribunal via the HK Judiciary's
Legal Reference System JSON API + HTML body endpoint.

Strategy:
  - Discovery: JSON year listing at ju-year-{code}L2.json
  - Case lists: JSON at ju-caselist-{code}{year}_L4_.json
  - Full text: HTML at ju_body.jsp?DIS={dis}
  - Court codes: CTEA (enforcement actions), CTA (standard actions)

Data:
  - ~51 judgments (2017–present)
  - No authentication required
  - Full text in HTML (clean extraction)

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 12+ sample records
  python bootstrap.py bootstrap --full     # Full fetch
  python bootstrap.py test                 # Quick connectivity test
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
logger = logging.getLogger("legal-data-hunter.HK.CompTribunal")

BASE_URL = "https://legalref.judiciary.hk/lrs/common/"
DATA_URL = BASE_URL + "data/"
BODY_URL = BASE_URL + "ju/ju_body.jsp"

COURT_CODES = ["CTEA", "CTA"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


class HKCompTribunalScraper(BaseScraper):
    """
    Scraper for HK/CompTribunal — Hong Kong Competition Tribunal.
    Country: HK
    URL: https://legalref.judiciary.hk/

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _get(self, url: str, timeout: int = 60) -> requests.Response:
        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp

    def _get_json(self, url: str) -> dict:
        resp = self._get(url)
        return resp.json()

    def _discover_cases(self) -> List[Dict[str, Any]]:
        """Discover all Competition Tribunal cases via JSON API."""
        all_cases = []
        seen_dis = set()

        for code in COURT_CODES:
            year_url = f"{DATA_URL}ju-year-{code}L2.json"
            try:
                year_data = self._get_json(year_url)
            except Exception as e:
                logger.warning(f"Failed to fetch years for {code}: {e}")
                continue

            years = [r["year"] for r in year_data.get("rows", [])]
            logger.info(f"{code}: found {len(years)} years: {years}")

            for year in years:
                caselist_url = f"{DATA_URL}ju-caselist-{code}{year}_L4_.json"
                try:
                    case_data = self._get_json(caselist_url)
                except Exception as e:
                    logger.warning(f"Failed to fetch {code}/{year}: {e}")
                    continue

                for row in case_data.get("rows", []):
                    dis = row.get("dis")
                    if not dis or dis in seen_dis:
                        continue
                    seen_dis.add(dis)
                    all_cases.append({
                        "dis": dis,
                        "court_code": code,
                        "case_number": row.get("caseno", ""),
                        "ncn": row.get("ncn", ""),
                        "date_str": row.get("date", ""),
                        "title": row.get("title", ""),
                        "lang": row.get("lang", "EN"),
                        "reportage": row.get("reportage", ""),
                        "year": year,
                    })

        logger.info(f"Discovery complete: {len(all_cases)} unique judgments")
        return all_cases

    def _fetch_judgment_text(self, dis: str) -> str:
        """Fetch full judgment text from ju_body.jsp."""
        url = f"{BODY_URL}?DIS={dis}"
        try:
            resp = self._get(url)
            soup = BeautifulSoup(resp.text, "html.parser")

            # Remove script and style elements
            for tag in soup.find_all(["script", "style", "noscript"]):
                tag.decompose()

            # Get text content
            text = soup.get_text(separator="\n")

            # Clean up whitespace
            lines = []
            for line in text.split("\n"):
                line = line.strip()
                if line:
                    lines.append(line)
            text = "\n".join(lines)

            return text.strip()
        except Exception as e:
            logger.warning(f"Failed to fetch judgment DIS={dis}: {e}")
            return ""

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date from DD/MM/YYYY format to ISO 8601."""
        if not date_str:
            return None
        try:
            dt = datetime.strptime(date_str.strip(), "%d/%m/%Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        cases = self._discover_cases()
        for case in cases:
            yield case

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        dis = raw["dis"]
        case_number = raw["case_number"]
        ncn = raw.get("ncn", "")

        text = self._fetch_judgment_text(dis)
        if not text or len(text) < 100:
            logger.warning(f"Insufficient text for {case_number} (DIS={dis}): {len(text)} chars")
            return None

        date_iso = self._parse_date(raw.get("date_str", ""))
        title = raw.get("title", "").strip()
        if not title:
            title = f"Competition Tribunal Judgment {ncn or case_number}"

        identifier = ncn.replace("[", "").replace("]", "").replace(" ", "-") if ncn else f"CT-DIS-{dis}"

        return {
            "_id": f"HK-CT-{identifier}",
            "_source": "HK/CompTribunal",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_iso,
            "url": f"https://legalref.judiciary.hk/lrs/common/ju/ju_frame.jsp?DIS={dis}",
            "case_number": case_number,
            "neutral_citation": ncn,
            "court": "Competition Tribunal",
            "court_code": raw.get("court_code", ""),
            "jurisdiction": "Hong Kong",
            "reportage": raw.get("reportage", ""),
            "language": raw.get("lang", "EN"),
        }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="HK/CompTribunal Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test", "status"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (12 records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap")
    args = parser.parse_args()

    scraper = HKCompTribunalScraper()

    if args.command == "test":
        print("Testing connectivity...")
        year_data = scraper._get_json(f"{DATA_URL}ju-year-CTEAL2.json")
        years = [r["year"] for r in year_data.get("rows", [])]
        print(f"OK — CTEA years: {years}")
        case_data = scraper._get_json(f"{DATA_URL}ju-caselist-CTEA{years[0]}_L4_.json")
        print(f"OK — {len(case_data.get('rows', []))} cases in {years[0]}")
        if case_data.get("rows"):
            dis = case_data["rows"][0]["dis"]
            text = scraper._fetch_judgment_text(dis)
            print(f"OK — Judgment DIS={dis}: {len(text)} chars")
        sys.exit(0)

    if args.command == "status":
        print(json.dumps(scraper.status, indent=2, default=str))
        sys.exit(0)

    if args.command == "bootstrap":
        sample_mode = args.sample or not args.full
        result = scraper.bootstrap(
            sample_mode=sample_mode,
            sample_size=12 if sample_mode else 999999,
        )
        print(json.dumps(result, indent=2, default=str))
